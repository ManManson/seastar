#pragma once

#include <seastar/core/sharded.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/temporary_buffer.hh>

#include <cstddef>
#include <queue>
#include <stdint.h>
#include <vector>

#include "record_comparators.hh"
#include "run_reader_service.hh"
#include "utils.hh"

namespace seastar {
class file;

template<typename... T>
class future;
} // namespace seastar

class DataFragment;

class MergeAlgorithm
{
  using priority_queue_type =
    std::priority_queue<record_underlying_type const*,
                        std::vector<record_underlying_type const*>,
                        inverse_record_compare>;

  priority_queue_type mPq;

  unsigned mInitialRunCount;
  std::size_t mInputFileSize;
  std::size_t mPerCpuMemory;
  seastar::sstring mOutputFilepath;
  seastar::sstring mTempPath;
  seastar::sharded<RunReaderService>& mRunReader;

public:
  MergeAlgorithm(unsigned initial_run_count,
                 std::size_t input_file_size,
                 std::size_t per_cpu_memory,
                 seastar::sstring const& output_filepath,
                 seastar::sstring const& temp_path,
                 seastar::sharded<RunReaderService>& sharded_run_reader);

  // main routine that invokes the algorithm
  void merge();

private:
  ///
  /// \brief The TempBufferWriter class
  /// Simple wrapper around output file and buffer to aid in writing output
  /// buffer to the new run
  ///
  class TempBufferWriter
  {
    // output file handle to be populated from the internal buffer
    seastar::file& mOutputFile;
    // temporary buffer to support chunked output to the file
    // gets filled in `pq_consume_fragment` calls
    seastar::temporary_buffer<record_underlying_type> const& mBuf;
    // current merging pass level and run id for logging
    unsigned mLvl;
    unsigned mRunId;
    // output file write offset and buffer write
    uint64_t& mOutFileWritePos;
    // current buffer end position, gets zero'ed after successful flush to file
    uint64_t& mBufWritePos;

  public:
    TempBufferWriter(
      seastar::file& out,
      seastar::temporary_buffer<record_underlying_type> const& buf,
      unsigned lvl,
      unsigned run_id,
      uint64_t& outfile_write_pos,
      uint64_t& buf_write_pos);

    seastar::future<> write(std::size_t buf_size);
  };

  seastar::future<DataFragment> fetch_and_get_data(unsigned shard_idx);
  void pq_consume_fragment(DataFragment const& frag);
  // single merge pass (merge `assigned_ids` files to one new larger file)
  void merge_pass(unsigned lvl,
                  unsigned current_run_id,
                  std::vector<unsigned> const& assigned_ids,
                  std::size_t run_size);
};
