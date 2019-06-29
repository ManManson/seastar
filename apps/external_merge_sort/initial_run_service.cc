#include "initial_run_service.hh"

#include <seastar/core/file-types.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/future.hh>
#include <seastar/core/reactor.hh>

#include <algorithm>
#include <iterator>

#include "record_comparators.hh"
#include "utils.hh"

InitialRunService::InitialRunService(seastar::file_handle fd,
                                     std::size_t input_fd_size,
                                     size_t cpu_memory,
                                     seastar::sstring temp_path)
  : mInputFile(fd.to_file())
  // aligned to be a multiple of `RECORD_SIZE`
  , mAlignedCpuMemSize(cpu_memory)
  , mCurrentRunId(seastar::engine().cpu_id())
  , mRunCount(round_up_int_div(input_fd_size, mAlignedCpuMemSize))
  , mTempBuf(OUT_BUF_SIZE)
  , mTempPath(std::move(temp_path))
  , mInputFdSize(input_fd_size)
{}

seastar::future<>
InitialRunService::stop()
{
  return seastar::make_ready_future<>();
}

seastar::future<>
InitialRunService::start()
{
  return seastar::do_until(
    [this] { return mCurrentRunId >= mRunCount; },
    [this] {
      mWritePos = 0;

      std::size_t const default_run_end_pos =
        mCurrentRunId * mAlignedCpuMemSize + mAlignedCpuMemSize;

      std::size_t const buf_size = mInputFdSize >= default_run_end_pos
                                     ? mAlignedCpuMemSize
                                     : mInputFdSize % mAlignedCpuMemSize;

      task_logger.info("Initializing run {}. Reading {} bytes at the offset "
                       "{} from the input file.",
                       mCurrentRunId,
                       buf_size,
                       mCurrentRunId * mAlignedCpuMemSize);
      return mInputFile
        .dma_read<record_underlying_type>(mCurrentRunId * mAlignedCpuMemSize,
                                          buf_size)
        .then([this](auto buf) { return create_initial_run(std::move(buf)); })
        .handle_exception([](auto ex) {
          task_logger.error("Exception during creation of an initial run: {}",
                            ex);
        });
    });
}

size_t
InitialRunService::total_run_count() const
{
  return mRunCount;
}

seastar::future<>
InitialRunService::create_initial_run(
  seastar::temporary_buffer<record_underlying_type> buf)
{
  auto rptr_vec = sort_raw_data(buf);
  mInBufferSliceIt = rptr_vec.cbegin();
  return seastar::do_with(
           std::move(buf),
           std::move(rptr_vec),
           [this, &rptr_vec](auto& buf, auto& rptr_vec) {
             return seastar::open_file_dma(
                      run_filename(mTempPath, 0u, mCurrentRunId),
                      seastar::open_flags::create |
                        seastar::open_flags::truncate | seastar::open_flags::wo)
               .then([this, &rptr_vec](seastar::file output_run_fd) {
                 return seastar::do_with(
                   std::move(output_run_fd), [&](auto& output_run_fd) {
                     auto old_run_id = mCurrentRunId;
                     mCurrentRunId += seastar::smp::count;
                     // write sorted data to the
                     // output in a series of chunks
                     return seastar::do_until(
                              [this, &rptr_vec] {
                                return mInBufferSliceIt == rptr_vec.cend();
                              },
                              [this, &rptr_vec, &output_run_fd, old_run_id] {
                                auto actual_size = fill_output_buffer(rptr_vec);
                                return output_run_fd
                                  .dma_write(
                                    mWritePos, mTempBuf.get(), actual_size)
                                  .then([&, old_run_id](size_t s) {
                                    task_logger.info("Written {} "
                                                     "bytes to the run {}",
                                                     s,
                                                     old_run_id);
                                    mWritePos += s;
                                  });
                              })
                       .then([&output_run_fd] { return output_run_fd.flush(); })
                       .then(
                         [&output_run_fd] { return output_run_fd.close(); });
                   });
               });
           })
    .handle_exception([](auto ex) {
      task_logger.error("Exception during creation of an initial run: {}", ex);
    });
}

record_ptr_vector
InitialRunService::sort_raw_data(
  seastar::temporary_buffer<record_underlying_type> const& buf)
{
  record_ptr_vector result;
  result.reserve(buf.size() / RECORD_SIZE);
  for (auto pos = buf.begin(), end = buf.end(); pos != end; pos += RECORD_SIZE)
    result.push_back(&*pos);
  std::sort(result.begin(), result.end(), record_compare());
  return result;
}

size_t
InitialRunService::fill_output_buffer(record_ptr_vector const& v)
{
  static constexpr std::size_t MAX_RECORDS_TO_FILL = OUT_BUF_SIZE / RECORD_SIZE;

  record_underlying_type* write_ptr = mTempBuf.get_write();

  std::size_t slice_size;
  auto distance_to_end =
    static_cast<size_t>(std::distance(mInBufferSliceIt, v.cend()));
  record_ptr_vector::const_iterator end_it;
  if (distance_to_end < MAX_RECORDS_TO_FILL) {
    end_it = v.cend();
    slice_size = distance_to_end * RECORD_SIZE;
  } else {
    end_it = mInBufferSliceIt + MAX_RECORDS_TO_FILL;
    slice_size = MAX_RECORDS_TO_FILL * RECORD_SIZE;
  }
  while (mInBufferSliceIt != end_it) {
    std::copy(*mInBufferSliceIt, *mInBufferSliceIt + RECORD_SIZE, write_ptr);
    write_ptr += RECORD_SIZE;
    ++mInBufferSliceIt;
  }
  return slice_size;
}
