#include <seastar/core/future-util.hh>
#include <seastar/core/future.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/thread.hh>

#include "data_fragment.hh"
#include "merge_algorithm.hh"
#include "run_reader_service.hh"
#include "utils.hh"

MergeAlgorithm::MergeAlgorithm(unsigned initial_run_count,
                               std::size_t input_file_size,
                               std::size_t per_cpu_memory,
                               seastar::sstring const& output_filepath,
                               seastar::sstring const& temp_path)
  : mInitialRunCount(initial_run_count)
  , mInputFileSize(input_file_size)
  , mPerCpuMemory(per_cpu_memory)
  , mOutputFilepath(output_filepath)
  , mTempPath(temp_path)
{}

void
MergeAlgorithm::merge()
{
  // K-way merge sort constant
  // const unsigned K = seastar::smp::count - 1;
  const unsigned K = 31; // temporary hardcode for 4GiB test file

  // Maximum size of each individual run on the previous level
  std::size_t run_size =
    mPerCpuMemory; // aligned to be a multiple of `RECORD_SIZE`

  uint32_t lvl = 1u;
  uint32_t prev_lvl_run_count = mInitialRunCount;

  while (prev_lvl_run_count > 1) {
    task_logger.info("Start merge pass (level {})", lvl);

    // assign unprocessed initial run ids
    std::queue<uint32_t> unprocessed_ids;
    for (uint32_t i = 0; i != prev_lvl_run_count; ++i) {
      unprocessed_ids.push(i);
    }

    unsigned current_run_id = 0u;
    while (!unprocessed_ids.empty()) {
      // take at most K ids from unprocessed list and pass them to the merge
      // pass in the case where < K ids are available, utilize only a fraction
      // of CPUs accordingly
      std::vector<uint32_t> assigned_ids;
      assigned_ids.reserve(K);
      for (uint32_t i = 0u; i != K; ++i) {
        uint32_t id = unprocessed_ids.front();
        assigned_ids.push_back(id);
        unprocessed_ids.pop();
        if (unprocessed_ids.empty())
          break;
      }

      merge_pass(lvl, current_run_id++, assigned_ids, run_size);

      // get the reactor some time after intensive work
      if (seastar::need_preempt())
        seastar::thread::yield();
    }
    ++lvl;
    // increase each run size by a factor of K = (smp::count - 1)
    // since it is the constant in K-way merge algorithm
    run_size *= K;
    run_size = align_to_record_size(run_size);
    prev_lvl_run_count = round_up_int_div(mInputFileSize, run_size);
  }

  // move last produced run to `output_file` destination
  auto last_run_path = run_filename(mTempPath, lvl - 1, 0);
  task_logger.info(
    "Moving the last run file \"{}\" to the final destination \"{}\"",
    last_run_path,
    mOutputFilepath);
  seastar::rename_file(last_run_path, mOutputFilepath)
    .then([] {
      task_logger.info("Successfully moved file to the final destination");
    })
    .wait();
}

MergeAlgorithm::TempBufferWriter::TempBufferWriter(
  seastar::file& out,
  seastar::temporary_buffer<record_underlying_type> const& buf,
  unsigned lvl,
  unsigned run_id,
  uint64_t& outfile_write_pos,
  uint64_t& buf_write_pos)
  : mOutputFile(out)
  , mBuf(buf)
  , mLvl(lvl)
  , mRunId(run_id)
  , mOutFileWritePos(outfile_write_pos)
  , mBufWritePos(buf_write_pos)
{}

seastar::future<>
MergeAlgorithm::TempBufferWriter::write(std::size_t buf_size)
{
  return mOutputFile
    .dma_write<record_underlying_type>(mOutFileWritePos, mBuf.get(), buf_size)
    .then([this](std::size_t written) {
      mOutFileWritePos += written;
      mBufWritePos = 0u;
    })
    .then([this] { return mOutputFile.flush(); })
    .then([this, buf_size] {
      task_logger.info("Written {} bytes to the run <id {}, level {}>",
                       buf_size,
                       mRunId,
                       mLvl);
    });
}

void
MergeAlgorithm::merge_pass(unsigned lvl,
                           unsigned current_run_id,
                           std::vector<unsigned> const& assigned_ids,
                           std::size_t run_size)
{
  static constexpr size_t OUT_BUF_SIZE =
    align_to_record_size(32u * seastar::MB);

  std::size_t const readers_count = assigned_ids.size();

  std::vector<seastar::lw_shared_ptr<RunReaderService>> run_readers_vector;
  run_readers_vector.reserve(readers_count);

  auto const reader_shard_indices =
    boost::irange(static_cast<size_t>(0u), readers_count);

  seastar::temporary_buffer<record_underlying_type> out_buf(OUT_BUF_SIZE);
  uint64_t buf_write_pos = 0u;

  seastar::file output_file =
    seastar::open_file_dma(run_filename(mTempPath, lvl, current_run_id),
                           seastar::open_flags::create |
                             seastar::open_flags::truncate |
                             seastar::open_flags::wo)
      .get0();
  uint64_t outfile_write_pos = 0u;

  TempBufferWriter buf_writer(output_file,
                              out_buf,
                              lvl,
                              current_run_id,
                              outfile_write_pos,
                              buf_write_pos);

  // create a reader for each assigned id
  for (auto i : reader_shard_indices) {
    unsigned run_lvl = lvl - 1, run_id = assigned_ids[i];
    task_logger.info(
      "opening file for the run <id {}, level {}>", run_id, run_lvl);

    auto r = seastar::make_lw_shared<RunReaderService>(
      align_to_record_size(mPerCpuMemory / 31));
    r->open_run_file(run_filename(mTempPath, run_lvl, run_id)).wait();
    r->fetch_data().wait();
    run_readers_vector.push_back(std::move(r));
  }

  // get a record from each reader and push into pq
  for (auto const& reader_ptr : run_readers_vector) {
    mPq.push({ reader_ptr->current_record_in_fragment(), reader_ptr.get() });
  }

  while (run_readers_vector.size() > 1u) {
    auto min_element = mPq.top();

    std::copy(min_element.first,
              min_element.first + RECORD_SIZE,
              out_buf.get_write() + buf_write_pos);
    buf_write_pos += RECORD_SIZE;

    mPq.pop();

    min_element.second->advance_record_in_fragment();

    if (min_element.second->has_more()) {
      mPq.push({ min_element.second->current_record_in_fragment(),
                 min_element.second });
    } else {
      // if reader with `just extracted min element` is exhausted
      auto reader_to_erase = std::find_if(
        run_readers_vector.begin(),
        run_readers_vector.end(),
        [v = min_element.second](auto const& x) { return x.get() == v; });
      (*reader_to_erase)->remove_run_file().wait();
      // exclude it from the processing list
      run_readers_vector.erase(reader_to_erase);
    }

    // write back to file if the buffer is full
    if (buf_write_pos == OUT_BUF_SIZE) {
      buf_writer.write(OUT_BUF_SIZE).wait();
    }
  }

  // write back to file if there is anything left in the last reader
  auto const& last_reader = run_readers_vector.front();
  DataFragment const last_fragment = last_reader->data_fragment();
  record_underlying_type const *remaining_recs_ptr =
                                 last_reader->current_record_in_fragment(),
                               *fragment_end = last_fragment.mBeginPtr +
                                               last_fragment.mDataSize;
  while (remaining_recs_ptr != fragment_end) {
    std::copy(remaining_recs_ptr,
              remaining_recs_ptr + RECORD_SIZE,
              out_buf.get_write() + buf_write_pos);
    buf_write_pos += RECORD_SIZE;
    if (buf_write_pos == OUT_BUF_SIZE) {
      buf_writer.write(OUT_BUF_SIZE).wait();
    }
    remaining_recs_ptr += RECORD_SIZE;
  }
  if (buf_write_pos != 0u) {
    buf_writer.write(buf_write_pos).wait();
  }
  last_reader->remove_run_file().wait();

  output_file.close().wait();
}
