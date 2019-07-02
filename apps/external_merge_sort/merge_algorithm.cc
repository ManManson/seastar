#include <seastar/core/future-util.hh>
#include <seastar/core/future.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/thread.hh>

#include "data_fragment.hh"
#include "merge_algorithm.hh"
#include "run_reader.hh"
#include "utils.hh"

MergeAlgorithm::MergeAlgorithm(std::size_t input_file_size,
                               seastar::sstring const& output_filepath,
                               seastar::sstring const& temp_path)
  : mInputFileSize(input_file_size)
  , mOutputFilepath(output_filepath)
  , mTempPath(temp_path)
{}

void
MergeAlgorithm::merge(std::size_t per_cpu_memory, unsigned initial_run_count)
{
  // Try to calculate optimal K constant for the K-way merge sort algorithm.
  //
  // Each MergePass instance manages K run readers. We are bounded by
  // `per_cpu_memory` limit per core. So each reader should occupy at most
  // `per_cpu_memory / K` fraction of the shard memory.
  //
  // On the one hand we want to minimize overall levels count. On the other hand
  // we don't want to have *too* small buffers and an enormous count of readers
  // per merge pass. So by default we put `K = <number of initial runs>` but
  // also check if the buffers for the individual readers are not getting small
  // (define this threshold as 4MiB).
  //
  // TODO: consider adjusting K in such a way that provides every core with
  // roughly equal amount of work?
  unsigned K = initial_run_count;
  if (align_to_record_size(per_cpu_memory / K) < 4u * seastar::MB) {
    K = align_to_record_size(per_cpu_memory / (4u * seastar::MB));
  }

  // Maximum size of each individual run on the previous level
  std::size_t run_size =
    per_cpu_memory; // aligned to be a multiple of `RECORD_SIZE`

  uint32_t lvl = 1u;
  uint32_t prev_lvl_run_count = initial_run_count;

  seastar::sharded<MergePass> merge_pass;
  merge_pass.start(K, per_cpu_memory, mTempPath).wait();

  while (prev_lvl_run_count > 1) {
    task_logger.info("Start merge pass (level {})", lvl);

    uint32_t prev_lvl_id = 0u;

    std::vector<seastar::future<>> merge_tasks;
    unsigned current_run_id = 0u, current_cpu_id = 0u;

    while (prev_lvl_id != prev_lvl_run_count) {
      // calculate run ids span for each merge pass to operate on
      unsigned last_id_to_assign =
        prev_lvl_id + K < prev_lvl_run_count ? K : prev_lvl_run_count;
      auto id_span = boost::irange<unsigned>(prev_lvl_id, last_id_to_assign);

      std::vector<uint32_t> assigned_ids;
      assigned_ids.reserve(K);
      assigned_ids.assign(id_span.begin(), id_span.end());

      prev_lvl_id = last_id_to_assign;

      // TODO: limit parallelism via `with_semaphore` (seastar::smp::count)
      // to ensure that no core takes more than one merge pass at a time.
      auto task = merge_pass.invoke_on(current_cpu_id, [=](auto& inst) {
        return inst.execute(lvl, current_run_id, assigned_ids);
      });
      current_cpu_id = (current_cpu_id + 1) % seastar::smp::count;
      ++current_run_id;

      merge_tasks.push_back(std::move(task));

      // get the reactor some time after intensive work
      if (seastar::need_preempt())
        seastar::thread::yield();
    }
    seastar::when_all(merge_tasks.begin(), merge_tasks.end())
      .discard_result()
      .wait();
    ++lvl;
    // increase each run size by a factor of K (used solely to calculate runs
    // count from the previous levels)
    run_size *= K;
    run_size = align_to_record_size(run_size);
    prev_lvl_run_count = round_up_int_div(mInputFileSize, run_size);
  }

  // move the last produced run to `output_file` destination
  auto last_run_path = run_filename(mTempPath, lvl - 1, 0);
  task_logger.info(
    "Moving the last run file \"{}\" to the final destination \"{}\"",
    last_run_path,
    mOutputFilepath);
  seastar::rename_file(last_run_path, mOutputFilepath)
    .then([] {
      task_logger.info("Successfully moved file to the final destination");
    })
    .then([&merge_pass] { return merge_pass.stop(); })
    .wait();
}

TempBufferWriter::TempBufferWriter(unsigned lvl, unsigned run_id)
  : mBuf(OUT_BUF_SIZE)
  , mLvl(lvl)
  , mRunId(run_id)
  , mOutFileWritePos(0u)
  , mBufWritePos(0u)
{}

seastar::future<>
TempBufferWriter::open_file(seastar::file fd)
{
  mOutputFile = std::move(fd);
  return seastar::make_ready_future<>();
}

seastar::future<>
TempBufferWriter::write()
{
  auto old_buf_write_pos = mBufWritePos;
  return mOutputFile
    .dma_write<record_underlying_type>(
      mOutFileWritePos, mBuf.get(), mBufWritePos)
    .then([this](std::size_t written) {
      mOutFileWritePos += written;
      mBufWritePos = 0u;
    })
    .then([this] { return mOutputFile.flush(); })
    .then([this, old_buf_write_pos] {
      task_logger.info("Written {} bytes to the run <id {}, level {}>",
                       old_buf_write_pos,
                       mRunId,
                       mLvl);
    });
}

bool
TempBufferWriter::is_full() const
{
  return mBufWritePos == OUT_BUF_SIZE;
}

bool
TempBufferWriter::is_empty() const
{
  return mBufWritePos == 0u;
}

void
TempBufferWriter::append_record(record_underlying_type const* rec_ptr)
{
  std::copy(rec_ptr, rec_ptr + RECORD_SIZE, mBuf.get_write() + mBufWritePos);
  mBufWritePos += RECORD_SIZE;
}

MergePass::MergePass(unsigned K,
                     std::size_t per_cpu_mem,
                     seastar::sstring temp_path)
  : mK(K)
  , mPerCpuMemory(per_cpu_mem)
  , mTempPath(std::move(temp_path))
{}

seastar::future<>
MergePass::stop()
{
  return seastar::make_ready_future<>();
}

seastar::future<>
MergePass::execute(unsigned lvl,
                   unsigned current_run_id,
                   std::vector<unsigned> assigned_ids)
{
  std::vector<seastar::lw_shared_ptr<RunReader>> run_readers;
  run_readers.reserve(assigned_ids.size());

  TempBufferWriter buf_writer(lvl, current_run_id);

  return seastar::do_with(
    std::move(run_readers),
    std::move(buf_writer),
    std::move(assigned_ids),
    [this, lvl, current_run_id](
      auto& run_readers, auto& buf_writer, auto& assigned_ids) {
      // open temporary file for the output run
      return seastar::open_file_dma(
               run_filename(mTempPath, lvl, current_run_id),
               seastar::open_flags::create | seastar::open_flags::truncate |
                 seastar::open_flags::wo)
        .then([&buf_writer](seastar::file out) {
          return buf_writer.open_file(std::move(out));
        })
        .then([this, &buf_writer, &run_readers, &assigned_ids, lvl] {
          // create a reader for each assigned id
          return seastar::do_for_each(
                   assigned_ids,
                   [this, &run_readers, lvl](unsigned run_id) {
                     unsigned run_lvl = lvl - 1;
                     task_logger.info(
                       "opening file for the run <id {}, level {}>",
                       run_id,
                       run_lvl);

                     auto r = seastar::make_lw_shared<RunReader>(
                       align_to_record_size(mPerCpuMemory / mK));

                     // and fetch some data for each reader
                     return seastar::do_with(
                       std::move(r),
                       [this, &run_readers, run_lvl, run_id](auto& reader_ptr) {
                         return reader_ptr
                           ->open_run_file(
                             run_filename(mTempPath, run_lvl, run_id))
                           .then(
                             [&reader_ptr] { return reader_ptr->fetch_data(); })
                           .then([&run_readers, &reader_ptr] {
                             run_readers.push_back(std::move(reader_ptr));
                           });
                       });
                   })
            .then([this, &run_readers] {
              // get a record from each reader and push into pq
              return seastar::do_for_each(
                run_readers, [this](auto const& reader_ptr) {
                  mPq.push(
                    { reader_ptr->current_record_in_fragment(), reader_ptr });
                });
            })
            .then([this, &run_readers, &buf_writer] {
              return seastar::do_until(
                       [&run_readers] { return run_readers.size() <= 1u; },
                       [this, &buf_writer, &run_readers] {
                         // extract values one by one from each reader and
                         // append to the buf_writer until there is just one
                         // reader left
                         return merge_step(buf_writer, run_readers);
                       })
                .then([this, &buf_writer, &run_readers] {
                  // flush remaining elements in the last reader from the local
                  // buffer to the output file
                  return finalize_merge(buf_writer, run_readers);
                });
            });
        });
    });
}

seastar::future<>
MergePass::merge_step(
  TempBufferWriter& buf_writer,
  std::vector<seastar::lw_shared_ptr<RunReader>>& run_readers)
{
  return seastar::async([this, &buf_writer, &run_readers] {
    // extract min element from the pq, and write it to
    // the buffer
    auto min_element = mPq.top();
    buf_writer.append_record(min_element.first);
    // write back to file if the buffer is full
    if (buf_writer.is_full())
      buf_writer.write().wait();

    mPq.pop();

    min_element.second->advance_record_in_fragment().wait();

    if (min_element.second->has_more()) {
      // refill pq with the new value from the reader
      // associated with the "previous" min element
      mPq.push({ min_element.second->current_record_in_fragment(),
                 min_element.second });
    } else {
      // if the reader associated with "previous" min
      // element is exhausted, remove its run file and
      // erase from the `run_readers` processing list
      min_element.second->remove_run_file().wait();

      auto reader_to_erase = std::find_if(
        run_readers.begin(),
        run_readers.end(),
        [v = min_element.second.get()](auto const& x) { return x.get() == v; });
      run_readers.erase(reader_to_erase);
    }
  });
}

seastar::future<>
MergePass::finalize_merge(
  TempBufferWriter& buf_writer,
  std::vector<seastar::lw_shared_ptr<RunReader>>& run_readers)
{
  // pop the remaining element from the queue since we are
  // going to flush the data in the last reader directly
  mPq.pop();

  auto const& last_reader = run_readers.front();
  DataFragment const last_fragment = last_reader->data_fragment();
  record_underlying_type const *remaining_recs_ptr =
                                 last_reader->current_record_in_fragment(),
                               *fragment_end = last_fragment.mBeginPtr +
                                               last_fragment.mDataSize;

  return seastar::do_until(
           [&remaining_recs_ptr, fragment_end] {
             return remaining_recs_ptr == fragment_end;
           },
           [&buf_writer, &remaining_recs_ptr] {
             buf_writer.append_record(remaining_recs_ptr);
             remaining_recs_ptr += RECORD_SIZE;
             if (buf_writer.is_full())
               return buf_writer.write();
             return seastar::make_ready_future<>();
           })
    .then([&buf_writer] {
      if (!buf_writer.is_empty())
        return buf_writer.write();
      return seastar::make_ready_future<>();
    })
    .then([&run_readers] { return run_readers.front()->remove_run_file(); });
}
