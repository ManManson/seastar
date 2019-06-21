#include <seastar/core/future.hh>
#include <seastar/core/reactor.hh>

#include "data_fragment.hh"
#include "run_reader_service.hh"
#include "utils.hh"

RunReaderService::RunReaderService(std::size_t mem)
  : mAlignedCpuMemSize(mem) // aligned to be a multiple of `RECORD_SIZE`
{}

seastar::future<>
RunReaderService::stop()
{
  if (mFd)
    return mFd.close();
  return seastar::make_ready_future<>();
}

seastar::future<>
RunReaderService::set_run_fd(seastar::sstring filepath, seastar::file fd)
{
  mFd = std::move(fd);
  mFilePath = std::move(filepath);
  mCurrentReadPos = 0u;
  return seastar::make_ready_future<>();
}

seastar::future<>
RunReaderService::open_run_file(seastar::sstring path)
{
  return seastar::do_with(std::move(path), [&](auto& path) {
    return seastar::open_file_dma(path, seastar::open_flags::ro)
      .then([this, &path](seastar::file fd) {
        return set_run_fd(path, std::move(fd));
      });
  });
}

seastar::future<>
RunReaderService::remove_run_file()
{
  task_logger.info("removing file \"{}\"", mFilePath);
  return mFd.close()
    .then([this] { return seastar::remove_file(mFilePath); })
    .then([this] { mFd = seastar::file(); });
}

seastar::future<>
RunReaderService::fetch_data()
{
  return mFd
    .dma_read<record_underlying_type>(mCurrentReadPos, mAlignedCpuMemSize)
    .then([this](seastar::temporary_buffer<record_underlying_type> buf) {
      mBuf = std::move(buf);
      mActualBufSize = mBuf.size();
      mCurrentReadPos += mActualBufSize;
    });
}

DataFragment
RunReaderService::data_fragment()
{
  return DataFragment{ mBuf.get(), mActualBufSize };
}
