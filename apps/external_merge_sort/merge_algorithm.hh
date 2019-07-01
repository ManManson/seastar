#pragma once

#include <seastar/core/sharded.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/temporary_buffer.hh>

#include <cstddef>
#include <queue>
#include <stdint.h>
#include <vector>

#include "record_comparators.hh"
#include "utils.hh"

namespace seastar {
class file;

template<typename... T>
class future;
} // namespace seastar

class RunReaderService;
class DataFragment;

class MergeAlgorithm
{
  using priority_queue_type = std::priority_queue<priorq_element,
                                                  std::vector<priorq_element>,
                                                  inverse_record_compare>;

  priority_queue_type mPq;

  unsigned mInitialRunCount;
  std::size_t mInputFileSize;
  std::size_t mPerCpuMemory;
  seastar::sstring mOutputFilepath;
  seastar::sstring mTempPath;

public:
  MergeAlgorithm(unsigned initial_run_count,
                 std::size_t input_file_size,
                 std::size_t per_cpu_memory,
                 seastar::sstring const& output_filepath,
                 seastar::sstring const& temp_path);

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
    static constexpr std::size_t OUT_BUF_SIZE =
      align_to_record_size(32u * seastar::MB);

    // output file handle to be populated from the internal buffer
    seastar::file mOutputFile;
    // temporary buffer to support chunked output to the file
    seastar::temporary_buffer<record_underlying_type> mBuf;
    // current merging pass level and run id for logging
    unsigned mLvl;
    unsigned mRunId;
    // output file write offset and buffer write
    uint64_t mOutFileWritePos;
    // current buffer end position, gets zero'ed after successful flush to file
    uint64_t mBufWritePos;

  public:
    TempBufferWriter(seastar::file out, unsigned lvl, unsigned run_id);

    seastar::future<> write();

    void append_record(record_underlying_type const* rec_ptr);

    bool is_full() const;
    bool is_empty() const;
  };

  // single merge pass (merge `assigned_ids` files to one new larger file)
  void merge_pass(unsigned lvl,
                  unsigned current_run_id,
                  std::vector<unsigned> const& assigned_ids,
                  std::size_t run_size);
};
