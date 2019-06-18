#include <seastar/core/app-template.hh>
#include <seastar/core/file-types.hh>
#include <seastar/core/file.hh>
#include <seastar/core/future.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/units.hh>
#include <seastar/util/log.hh>

#include <boost/range/irange.hpp>
#include <fmt/printf.h>

#include <algorithm>
#include <queue>
#include <vector>

namespace bpo = boost::program_options;

constexpr std::size_t RECORD_SIZE = 4 * seastar::KB;
using record_underlying_type = unsigned char;
using record_ptr_vector = std::vector<record_underlying_type const*>;

seastar::logger task_logger("external_merge_sort");

struct record_compare
{
  bool operator()(record_underlying_type const* lhs,
                  record_underlying_type const* rhs) const
  {
    return std::lexicographical_compare(
      lhs, lhs + RECORD_SIZE, rhs, rhs + RECORD_SIZE);
  }
};

struct inverse_record_compare
{
  bool operator()(record_underlying_type const* lhs,
                  record_underlying_type const* rhs) const
  {
    return std::lexicographical_compare(
      rhs, rhs + RECORD_SIZE, lhs, lhs + RECORD_SIZE);
  }
};

constexpr std::size_t
align_to_record_size(std::size_t s)
{
  return s / RECORD_SIZE * RECORD_SIZE;
}

record_ptr_vector
sort_raw_data(seastar::temporary_buffer<record_underlying_type> const& buf)
{
  record_ptr_vector result;
  result.reserve(buf.size() / RECORD_SIZE);
  for (auto pos = buf.begin(), end = buf.end(); pos != end; pos += RECORD_SIZE)
    result.push_back(&*pos);
  std::sort(result.begin(), result.end(), record_compare());
  return result;
}

seastar::sstring
partition_filename(unsigned level, unsigned id)
{
  seastar::sstring result = "/opt/test_data/part-lvl";
  result += seastar::to_sstring(level);
  result += "-";
  result += seastar::to_sstring(id);
  return result;
}

class PartitionerService
{
  seastar::file mInputFile;
  std::size_t mAlignedCpuMemSize;
  std::size_t mCurrentPartitionId;
  std::size_t mPartitionsCount;
  std::size_t mWritePos;

  seastar::temporary_buffer<record_underlying_type> mTempBuf;
  record_ptr_vector::const_iterator mInBufferSliceIt;

  static constexpr size_t OUT_BUF_SIZE = align_to_record_size(16 * seastar::MB);

public:
  PartitionerService(seastar::file_handle fd,
                     std::size_t input_fd_size,
                     size_t cpu_memory)
    : mInputFile(fd.to_file())
    // align to be a multiple of `record_size`
    , mAlignedCpuMemSize(align_to_record_size(cpu_memory))
    , mCurrentPartitionId(seastar::engine().cpu_id())
    , mPartitionsCount((input_fd_size + mAlignedCpuMemSize - 1) /
                       mAlignedCpuMemSize)
    , mTempBuf(OUT_BUF_SIZE)
  {}

  seastar::future<> stop() { return seastar::make_ready_future<>(); }

  seastar::future<> start()
  {
    return seastar::do_until(
      [this] { return mCurrentPartitionId >= mPartitionsCount; },
      [this] {
        mWritePos = 0;
        task_logger.info(
          "partition {}. reading {} bytes from input file. Offset {}",
          mCurrentPartitionId,
          mAlignedCpuMemSize,
          mCurrentPartitionId * mAlignedCpuMemSize);
        return mInputFile
          .dma_read<record_underlying_type>(
            mCurrentPartitionId * mAlignedCpuMemSize, mAlignedCpuMemSize)
          .then([this](auto buf) {
            return create_initial_partition(std::move(buf));
          });
      });
  }

  size_t total_partitions_count() const { return mPartitionsCount; }

private:
  seastar::future<> create_initial_partition(
    seastar::temporary_buffer<record_underlying_type> buf)
  {
    auto rptr_vec = sort_raw_data(buf);
    mInBufferSliceIt = rptr_vec.cbegin();
    return seastar::do_with(
      std::move(buf), std::move(rptr_vec), [&](auto& buf, auto& rptr_vec) {
        return seastar::open_file_dma(
                 partition_filename(0u, mCurrentPartitionId),
                 seastar::open_flags::create | seastar::open_flags::truncate |
                   seastar::open_flags::wo)
          .then([&](seastar::file output_partition_fd) {
            return seastar::do_with(
              std::move(output_partition_fd), [&](auto& output_partition_fd) {
                task_logger.info("writing to partition {}",
                                 mCurrentPartitionId);
                mCurrentPartitionId += seastar::smp::count;
                return seastar::do_until(
                  [&] { return mInBufferSliceIt == rptr_vec.cend(); },
                  [&] {
                    auto actual_size = fill_output_buffer(rptr_vec);
                    return output_partition_fd
                      .dma_write(mWritePos, mTempBuf.get(), actual_size)
                      .then([&](size_t s) {
                        task_logger.info("successfully written {} "
                                         "bytes to partition",
                                         s);
                        mWritePos += s;
                      });
                  });
              });
          });
      });
  }

  size_t fill_output_buffer(record_ptr_vector const& v)
  {
    static constexpr std::size_t MAX_RECORDS_TO_FILL =
      OUT_BUF_SIZE / RECORD_SIZE;

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
};

struct DataFragment
{
  const record_underlying_type* mBeginPtr; // points to an array of 4K records
  std::size_t mDataSize;                   // array size in bytes
};

class RunReaderService
{
  seastar::file mFd;
  seastar::temporary_buffer<record_underlying_type> mBuf;
  std::size_t mActualBufSize = 0u;
  std::size_t mCurrentReadPos = 0;
  std::size_t mAlignedCpuMemSize;

public:
  RunReaderService(std::size_t mem)
    : mAlignedCpuMemSize(
        align_to_record_size(mem)) // align to be a multiple of `record_size`
  {}

  seastar::future<> stop() const { return seastar::make_ready_future<>(); }

  seastar::future<> set_file(seastar::file fd)
  {
    mFd = std::move(fd);
    mCurrentReadPos = 0u;
    return seastar::make_ready_future<>();
  }

  seastar::future<> open_file(seastar::sstring const& path)
  {
    return seastar::open_file_dma(path, seastar::open_flags::ro)
      .then([&](seastar::file fd) { return set_file(std::move(fd)); });
  }

  seastar::future<std::size_t> fetch_data()
  {
    return seastar::async([&]() -> std::size_t {
      mBuf =
        mFd
          .dma_read<record_underlying_type>(mCurrentReadPos, mAlignedCpuMemSize)
          .get0();
      mActualBufSize = mBuf.size();
      mCurrentReadPos += mActualBufSize;
      return mActualBufSize;
    });
  }

  DataFragment data_fragment()
  {
    return DataFragment{ mBuf.get(), mActualBufSize };
  }
};

using priority_queue_type =
  std::priority_queue<record_underlying_type const*,
                      std::vector<record_underlying_type const*>,
                      inverse_record_compare>;

void
merge_pass(unsigned lvl,
           unsigned current_lvl_part_count,
           unsigned current_part_id,
           seastar::sharded<RunReaderService>& sharded_reader,
           priority_queue_type& priorq,
           std::vector<unsigned> const& assigned_ids,
           std::size_t part_size)
{
  static constexpr size_t OUT_BUF_SIZE = align_to_record_size(16 * seastar::MB);
  auto const reader_shard_indices =
    boost::irange(static_cast<size_t>(1u), assigned_ids.size() + 1);

  seastar::temporary_buffer<record_underlying_type> out_buf(OUT_BUF_SIZE);
  std::size_t buf_write_pos = 0u;

  seastar::file output_file =
    seastar::open_file_dma(partition_filename(lvl, current_part_id),
                           seastar::open_flags::create |
                             seastar::open_flags::truncate |
                             seastar::open_flags::wo)
      .get0();
  std::size_t outfile_write_pos = 0u;

  // open assigned file ids
  seastar::parallel_for_each(
    reader_shard_indices,
    [&](unsigned shard_idx) {
      return sharded_reader.invoke_on(shard_idx, [&](RunReaderService& r) {
        task_logger.info("opening partition on shard {}",
                         seastar::engine().cpu_id());
        return r.open_file(partition_filename(
          lvl - 1, assigned_ids[seastar::engine().cpu_id() - 1]));
      });
    })
    .get0();

  task_logger.info("successfully opened reading streams on reader shards");

  // while there is something to process in at least one reader
  while (outfile_write_pos <
         part_size * assigned_ids.size()) { // part_size * <readers_count>
    for (auto i : reader_shard_indices) {
      sharded_reader
        .invoke_on(i,
                   [&](RunReaderService& r) {
                     task_logger.info("fetching data on cpu_id {}",
                                      seastar::engine().cpu_id());
                     return r.fetch_data().then([](std::size_t) {
                       return seastar::make_ready_future<>();
                     });
                   })
        .get0();

      DataFragment fragment =
        sharded_reader
          .invoke_on(i, [](RunReaderService& r) { return r.data_fragment(); })
          .get0();

      if (fragment.mDataSize == 0)
        continue;

      // sort data locally on zero-th core
      for (std::size_t fragment_pos = 0u; fragment_pos < fragment.mDataSize;
           fragment_pos += RECORD_SIZE) {
        priorq.push(fragment.mBeginPtr + fragment_pos);
      }
      // yield to the reactor event loop for a while
      if (seastar::need_preempt())
        seastar::thread::yield();
    }

    if (priorq.empty())
      break;

    while (!priorq.empty()) {
      record_underlying_type const* rec_ptr = priorq.top();
      std::copy(
        rec_ptr, rec_ptr + RECORD_SIZE, out_buf.get_write() + buf_write_pos);
      buf_write_pos += RECORD_SIZE;
      priorq.pop();
      // flush full output buffer contents to the output file
      if (buf_write_pos == OUT_BUF_SIZE) {
        buf_write_pos = 0u;
        output_file
          .dma_write<record_underlying_type>(
            outfile_write_pos, out_buf.get(), OUT_BUF_SIZE)
          .get0();
        outfile_write_pos += OUT_BUF_SIZE;
      }
    }
    // flush remaining part of the output buffer to the file
    if (buf_write_pos != 0) {
      output_file
        .dma_write<record_underlying_type>(
          outfile_write_pos, out_buf.get(), buf_write_pos)
        .get0();
      outfile_write_pos += buf_write_pos;
      buf_write_pos = 0u;
    }

    // transfer control to reactor event loop to prevent reactor stalls
    if (seastar::need_preempt())
      seastar::thread::yield();
  }
}

void
merge_algorithm(unsigned initial_partition_count,
                std::size_t input_file_size,
                seastar::sharded<RunReaderService>& sharded_reader,
                std::size_t per_cpu_memory)
{
  const unsigned K = seastar::smp::count - 1;

  std::size_t part_size = align_to_record_size(per_cpu_memory);

  unsigned lvl = 1u;
  unsigned prev_lvl_partition_count = initial_partition_count;

  sharded_reader.start(per_cpu_memory).get0();

  priority_queue_type priorq;

  while (prev_lvl_partition_count > 1) {
    task_logger.info("invoking merge pass (level {})", lvl);

    // assign unprocessed initial partition ids
    std::queue<unsigned> unprocessed_ids;
    for (unsigned i = 0; i != prev_lvl_partition_count; ++i) {
      unprocessed_ids.push(i);
    }

    unsigned current_part_id = 0u;
    while (!unprocessed_ids.empty()) {
      // take at most K ids from unprocessed list and pass them to the merge
      // pass in the case where < K ids are available, utilize only a fraction
      // of CPUs accordingly
      std::vector<unsigned> assigned_ids;
      assigned_ids.reserve(K);
      for (unsigned i = 0u; i != K; ++i) {
        unsigned id = unprocessed_ids.front();
        assigned_ids.push_back(id);
        unprocessed_ids.pop();
        if (unprocessed_ids.empty())
          break;
      }

      merge_pass(lvl,
                 prev_lvl_partition_count,
                 current_part_id++,
                 sharded_reader,
                 priorq,
                 assigned_ids,
                 part_size);
    }
    ++lvl;
    // increase each partition size by a factor of K = (smp::count - 1)
    // since it is the constant in K-way merge algorithm
    part_size *= K;
    part_size = align_to_record_size(part_size);
    prev_lvl_partition_count = (input_file_size + part_size - 1) / part_size;
  }
}

int
main(int argc, char** argv)
{
  seastar::app_template app;
  auto opts_adder = app.add_options();
  opts_adder(
    "input",
    bpo::value<seastar::sstring>()->default_value("/opt/test_data/tf1"),
    "path to the large file to be sorted")(
    "output",
    bpo::value<seastar::sstring>()->default_value("/opt/test_data/output_tf"),
    "path to the sorted output file");

  seastar::sharded<PartitionerService> partitioner;
  seastar::sharded<RunReaderService> sharded_reader;
  return app.run(argc, argv, [&] {
    return seastar::async([&] {
      std::size_t const available_memory =
                          seastar::memory::stats().free_memory() / 2,
                        per_cpu_memory = available_memory / seastar::smp::count;
      fmt::print("Number of cores: {}\n"
                 "Total available memory: {} bytes ({} MB)\n"
                 "Memory available to each core: {} bytes ({} MB)\n",
                 seastar::smp::count,
                 available_memory,
                 available_memory / 1024 / 1024,
                 per_cpu_memory,
                 per_cpu_memory / 1024 / 1024);

      auto& opts = app.configuration();
      seastar::sstring const& input_filepath =
        opts["input"].as<seastar::sstring>();
      auto input_fd =
        seastar::open_file_dma(input_filepath, seastar::open_flags::ro).get0();
      std::size_t const input_size = input_fd.size().get0();

      partitioner.start(input_fd.dup(), input_size, per_cpu_memory).get0();
      seastar::engine().at_exit([&partitioner] { return partitioner.stop(); });

      // initial partitioning pass
      partitioner.invoke_on_all([](auto& p) { return p.start(); }).get0();
      input_fd.close().get0();

      seastar::engine().at_exit(
        [&sharded_reader] { return sharded_reader.stop(); });

      // invoke K-way merge sorting algorithm
      merge_algorithm(partitioner.local().total_partitions_count(),
                      input_size,
                      sharded_reader,
                      per_cpu_memory);
    });
  });
}
