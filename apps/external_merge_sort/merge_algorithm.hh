#pragma once

#include <seastar/core/sharded.hh>
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

class RunReader;
class DataFragment;

class MergeAlgorithm
{
  std::size_t mInputFileSize;
  seastar::sstring mOutputFilepath;
  seastar::sstring mTempPath;

public:
  MergeAlgorithm(std::size_t input_file_size,
                 seastar::sstring const& output_filepath,
                 seastar::sstring const& temp_path);

  // main routine that invokes the algorithm
  void merge(std::size_t per_cpu_memory, unsigned initial_run_count);

private:
  // single merge pass (merge `assigned_ids` files to one new larger file)
  seastar::future<> merge_pass(unsigned lvl,
                               unsigned current_run_id,
                               std::vector<unsigned> assigned_ids,
                               unsigned K);
};

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
  TempBufferWriter(unsigned lvl, unsigned run_id);

  seastar::future<> open_file(seastar::file fd);

  seastar::future<> write();

  void append_record(record_underlying_type const* rec_ptr);

  bool is_full() const;
  bool is_empty() const;
};

class MergePass
{
private:
  using priority_queue_type = std::priority_queue<priorq_element,
                                                  std::vector<priorq_element>,
                                                  inverse_record_compare>;

  priority_queue_type mPq;

  unsigned mK;
  std::size_t mPerCpuMemory;
  seastar::sstring mTempPath;

public:
  MergePass(unsigned K, std::size_t per_cpu_mem, seastar::sstring temp_path);

  seastar::future<> stop();

  seastar::future<> execute(unsigned lvl,
                            unsigned current_run_id,
                            std::vector<unsigned> assigned_ids);
};
