#include <seastar/core/future-util.hh>
#include <seastar/core/future.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/thread.hh>

#include "data_fragment.hh"
#include "merge_algorithm.hh"
#include "utils.hh"

MergeAlgorithm::MergeAlgorithm(
  unsigned initial_run_count,
  std::size_t input_file_size,
  std::size_t per_cpu_memory,
  seastar::sstring const& output_filepath,
  seastar::sstring const& temp_path,
  seastar::sharded<RunReaderService>& sharded_run_reader)
  : mInitialRunCount(initial_run_count)
  , mInputFileSize(input_file_size)
  , mPerCpuMemory(per_cpu_memory)
  , mOutputFilepath(output_filepath)
  , mTempPath(temp_path)
  , mRunReader(sharded_run_reader)
{}

void
MergeAlgorithm::merge()
{
  // K-way merge sort constant
  const unsigned K = seastar::smp::count - 1;

  // Maximum size of each individual run on the previous level
  std::size_t run_size =
    mPerCpuMemory; // aligned to be a multiple of `RECORD_SIZE`

  uint32_t lvl = 1u;
  uint32_t prev_lvl_run_count = mInitialRunCount;

  mRunReader.start(mPerCpuMemory).wait();

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
      task_logger.info(
        "writing {} bytes to the run {}/{}", buf_size, mLvl, mRunId);
    });
}

seastar::future<DataFragment>
MergeAlgorithm::fetch_and_get_data(unsigned shard_idx)
{
  return mRunReader
    .invoke_on(shard_idx, [](RunReaderService& r) { return r.fetch_data(); })
    .then([this, shard_idx] {
      return mRunReader.invoke_on(
        shard_idx, [](RunReaderService& r) { return r.data_fragment(); });
    });
}

void
MergeAlgorithm::pq_consume_fragment(DataFragment const& frag)
{
  for (uint64_t fragment_pos = 0u; fragment_pos < frag.mDataSize;
       fragment_pos += RECORD_SIZE) {
    mPq.push(frag.mBeginPtr + fragment_pos);
  }
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

  // shift indices by 1 since we want to skip zero-th shard from reading
  auto const reader_shard_indices =
    boost::irange(static_cast<size_t>(1u), readers_count + 1);

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

  bool nothing_to_fetch = false;

  // open assigned file ids
  seastar::parallel_for_each(
    reader_shard_indices,
    [this, &assigned_ids, lvl](unsigned shard_idx) {
      return mRunReader.invoke_on(shard_idx, [&](RunReaderService& r) {
        unsigned run_lvl = lvl - 1,
                 run_id = assigned_ids[seastar::engine().cpu_id() - 1];
        task_logger.info("opening file for the run {}/{}", run_lvl, run_id);
        return r.open_run_file(run_filename(mTempPath, run_lvl, run_id));
      });
    })
    .then([&] {
      // while there is something to process in at least one reader
      // and we've not hit the size limit for the newly generated run
      return seastar::do_until(
               [&nothing_to_fetch,
                &outfile_write_pos,
                run_size,
                readers_count] {
                 return nothing_to_fetch ||
                        (outfile_write_pos >= run_size * readers_count);
               },
               [&] {
                 return seastar::do_for_each(
                          reader_shard_indices,
                          [&](unsigned shard_idx) {
                            // fetch data and retrieve corresponding data
                            // fragment pointing to the available data on
                            // the shard
                            return fetch_and_get_data(shard_idx).then(
                              [this](DataFragment const& frag) {
                                // skip the reader if there is no data, it has
                                // definitely reached EOF
                                if (frag.mDataSize == 0)
                                  return seastar::make_ready_future<>();

                                // sort data locally on zero-th core
                                pq_consume_fragment(frag);

                                return seastar::make_ready_future<>();
                              });
                          })
                   .then([&] {
                     if (mPq.empty()) {
                       nothing_to_fetch = true;
                       return seastar::make_ready_future<>();
                     }

                     // flush priority queue to the output buffer and then
                     // to the file (in chunks)
                     return seastar::do_until(
                              [this] { return mPq.empty(); },
                              [&] {
                                record_underlying_type const* rec_ptr =
                                  mPq.top();
                                std::copy(rec_ptr,
                                          rec_ptr + RECORD_SIZE,
                                          out_buf.get_write() + buf_write_pos);
                                buf_write_pos += RECORD_SIZE;
                                mPq.pop();
                                // flush full output buffer contents to the
                                // output file
                                if (buf_write_pos == OUT_BUF_SIZE) {
                                  return buf_writer.write(OUT_BUF_SIZE);
                                }
                                return seastar::make_ready_future<>();
                              })
                       .then([&] {
                         // flush remaining part of the output buffer
                         // to the file
                         if (buf_write_pos != 0) {
                           return buf_writer.write(buf_write_pos);
                         }
                         return seastar::make_ready_future<>();
                       });
                   });
               })
        .then([&output_file] { return output_file.close(); })
        .then([this, reader_shard_indices] {
          // remove exhausted run files that participated in this merge
          // operation
          return seastar::parallel_for_each(
            reader_shard_indices, [this](unsigned shard_idx) {
              return mRunReader.invoke_on(shard_idx, [](RunReaderService& r) {
                return r.remove_run_file();
              });
            });
        });
    })
    .wait();
}
