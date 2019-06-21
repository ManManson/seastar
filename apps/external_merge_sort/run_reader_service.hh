#pragma once

#include <seastar/core/file.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/temporary_buffer.hh>

#include "utils.hh"

#include <stdint.h>
#include <cstddef>

namespace seastar
{

template<typename... T>
class future;

} // namespace seastar

class DataFragment;

///
/// \brief The RunReaderService class
/// Manages opening, disposing, reading and fetching portions of data from a run
///
class RunReaderService
{
  seastar::file mFd;
  seastar::sstring mFilePath;
  seastar::temporary_buffer<record_underlying_type> mBuf;
  uint64_t mCurrentReadPos = 0;
  std::size_t mActualBufSize = 0u;
  std::size_t mAlignedCpuMemSize;

public:
  RunReaderService(std::size_t mem);

  seastar::future<> stop();

  seastar::future<> set_run_fd(seastar::sstring filepath, seastar::file fd);
  seastar::future<> open_run_file(seastar::sstring path);
  seastar::future<> remove_run_file();

  seastar::future<> fetch_data();
  DataFragment data_fragment();
};
