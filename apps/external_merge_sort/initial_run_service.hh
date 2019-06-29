#pragma once

#include <seastar/core/file.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/temporary_buffer.hh>

#include <cstddef>
#include <stdint.h>

#include "utils.hh"

namespace seastar {

template<typename... T>
class future;

} // namespace seastar

///
/// \brief The InitialRunService class
/// sharded service that walks through the input and generates a series of
/// initial runs to be consumed in a later merge procedure.
///
class InitialRunService
{
  seastar::file mInputFile;
  std::size_t mAlignedCpuMemSize;
  uint32_t mCurrentRunId;
  uint32_t mRunCount;
  uint64_t mWritePos;

  seastar::temporary_buffer<record_underlying_type> mTempBuf;
  record_ptr_vector::const_iterator mInBufferSliceIt;

  seastar::sstring mTempPath;

  std::size_t mInputFdSize;

  static constexpr size_t OUT_BUF_SIZE =
    align_to_record_size(32u * seastar::MB);

public:
  InitialRunService(seastar::file_handle fd,
                    std::size_t input_fd_size,
                    size_t cpu_memory,
                    seastar::sstring temp_path);

  seastar::future<> stop();
  seastar::future<> start();

  size_t total_run_count() const;

private:
  seastar::future<> create_initial_run(
    seastar::temporary_buffer<record_underlying_type> buf);

  record_ptr_vector sort_raw_data(
    seastar::temporary_buffer<record_underlying_type> const& buf);

  size_t fill_output_buffer(record_ptr_vector const& v);
};
