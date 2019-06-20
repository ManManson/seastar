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

constexpr std::size_t
round_up_int_div(std::size_t num, std::size_t denom)
{
  return (num + denom - 1) / denom;
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
run_filename(seastar::sstring const& dst, unsigned level, unsigned id)
{
  seastar::sstring result = dst;
  result += "/run-lvl";
  result += seastar::to_sstring(level);
  result += "-";
  result += seastar::to_sstring(id);
  return result;
}

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

  static constexpr size_t OUT_BUF_SIZE =
    align_to_record_size(64u * seastar::MB);

public:
  InitialRunService(seastar::file_handle fd,
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
  {}

  seastar::future<> stop() { return seastar::make_ready_future<>(); }

  seastar::future<> start()
  {
    return seastar::do_until(
      [this] { return mCurrentRunId >= mRunCount; },
      [this] {
        mWritePos = 0;
        task_logger.info("Initializing run {}. Reading {} bytes at the offset "
                         "{} from the input file.",
                         mCurrentRunId,
                         mAlignedCpuMemSize,
                         mCurrentRunId * mAlignedCpuMemSize);
        return mInputFile
          .dma_read<record_underlying_type>(mCurrentRunId * mAlignedCpuMemSize,
                                            mAlignedCpuMemSize)
          .then(
            [this](auto buf) { return create_initial_run(std::move(buf)); });
      });
  }

  size_t total_run_count() const { return mRunCount; }

private:
  seastar::future<> create_initial_run(
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
                          seastar::open_flags::truncate |
                          seastar::open_flags::wo)
                 .then([this, &rptr_vec](seastar::file output_run_fd) {
                   return seastar::do_with(
                     std::move(output_run_fd), [&](auto& output_run_fd) {
                       auto old_run_id = mCurrentRunId;
                       mCurrentRunId += seastar::smp::count;
                       return seastar::do_until(
                                [this, &rptr_vec] {
                                  return mInBufferSliceIt == rptr_vec.cend();
                                },
                                [this, &rptr_vec, &output_run_fd, old_run_id] {
                                  auto actual_size =
                                    fill_output_buffer(rptr_vec);
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
                         .then(
                           [&output_run_fd] { return output_run_fd.flush(); })
                         .then(
                           [&output_run_fd] { return output_run_fd.close(); });
                     });
                 });
             })
      .handle_exception([](auto ex) {
        task_logger.error("Exception during creation of an initial run: {}",
                          ex);
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
  seastar::sstring mFilePath;
  seastar::temporary_buffer<record_underlying_type> mBuf;
  uint64_t mCurrentReadPos = 0;
  std::size_t mActualBufSize = 0u;
  std::size_t mAlignedCpuMemSize;

public:
  RunReaderService(std::size_t mem)
    : mAlignedCpuMemSize(mem) // aligned to be a multiple of `RECORD_SIZE`
  {}

  seastar::future<> stop()
  {
    if (mFd)
      return mFd.close();
    return seastar::make_ready_future<>();
  }

  seastar::future<> set_run_fd(seastar::sstring filepath, seastar::file fd)
  {
    mFd = std::move(fd);
    mFilePath = std::move(filepath);
    mCurrentReadPos = 0u;
    return seastar::make_ready_future<>();
  }

  seastar::future<> open_run_file(seastar::sstring path)
  {
    return seastar::do_with(std::move(path), [&](auto& path) {
      return seastar::open_file_dma(path, seastar::open_flags::ro)
        .then([this, &path](seastar::file fd) {
          return set_run_fd(path, std::move(fd));
        });
    });
  }

  seastar::future<> remove_run_file()
  {
    task_logger.info("removing file \"{}\"", mFilePath);
    return mFd.close()
      .then([this] { return seastar::remove_file(mFilePath); })
      .then([this] { mFd = seastar::file(); });
  }

  seastar::future<std::size_t> fetch_data()
  {
    return seastar::async([this]() -> std::size_t {
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
                 seastar::sharded<RunReaderService>& sharded_run_reader)
    : mInitialRunCount(initial_run_count)
    , mInputFileSize(input_file_size)
    , mPerCpuMemory(per_cpu_memory)
    , mOutputFilepath(output_filepath)
    , mTempPath(temp_path)
    , mRunReader(sharded_run_reader)
  {}

  void merge()
  {
    const unsigned K = seastar::smp::count - 1;

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

private:
  void merge_pass(unsigned lvl,
                  unsigned current_run_id,
                  std::vector<unsigned> const& assigned_ids,
                  std::size_t run_size)
  {
    static constexpr size_t OUT_BUF_SIZE =
      align_to_record_size(64u * seastar::MB);
    auto const reader_shard_indices =
      boost::irange(static_cast<size_t>(1u), assigned_ids.size() + 1);

    seastar::temporary_buffer<record_underlying_type> out_buf(OUT_BUF_SIZE);
    uint64_t buf_write_pos = 0u;

    seastar::file output_file =
      seastar::open_file_dma(run_filename(mTempPath, lvl, current_run_id),
                             seastar::open_flags::create |
                               seastar::open_flags::truncate |
                               seastar::open_flags::wo)
        .get0();
    uint64_t outfile_write_pos = 0u;

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
      .wait();

    // while there is something to process in at least one reader
    while (outfile_write_pos <
           run_size * assigned_ids.size()) { // run_size * <readers_count>
      for (auto shard_idx : reader_shard_indices) {
        DataFragment fragment =
          mRunReader
            .invoke_on(shard_idx,
                       [](RunReaderService& r) {
                         return r.fetch_data().then([](std::size_t) {
                           return seastar::make_ready_future<>();
                         });
                       })
            .then([this, shard_idx] {
              return mRunReader.invoke_on(shard_idx, [](RunReaderService& r) {
                return r.data_fragment();
              });
            })
            .get0();

        if (fragment.mDataSize == 0)
          continue;

        // sort data locally on zero-th core
        for (uint64_t fragment_pos = 0u; fragment_pos < fragment.mDataSize;
             fragment_pos += RECORD_SIZE) {
          mPq.push(fragment.mBeginPtr + fragment_pos);
        }
        // yield to the reactor event loop for a while
        if (seastar::need_preempt())
          seastar::thread::yield();
      }

      if (mPq.empty())
        break;

      while (!mPq.empty()) {
        record_underlying_type const* rec_ptr = mPq.top();
        std::copy(
          rec_ptr, rec_ptr + RECORD_SIZE, out_buf.get_write() + buf_write_pos);
        buf_write_pos += RECORD_SIZE;
        mPq.pop();
        // flush full output buffer contents to the output file
        if (buf_write_pos == OUT_BUF_SIZE) {
          write_buf(output_file,
                    out_buf,
                    OUT_BUF_SIZE,
                    outfile_write_pos,
                    buf_write_pos,
                    lvl,
                    current_run_id);
        }
      }
      // flush remaining part of the output buffer to the file
      if (buf_write_pos != 0) {
        write_buf(output_file,
                  out_buf,
                  buf_write_pos,
                  outfile_write_pos,
                  buf_write_pos,
                  lvl,
                  current_run_id);
      }

      // transfer control to reactor event loop to prevent reactor stalls
      if (seastar::need_preempt())
        seastar::thread::yield();
    }

    output_file.close().wait();

    // remove exhausted run files that participated in this merge operation
    seastar::parallel_for_each(
      reader_shard_indices,
      [this](unsigned shard_idx) {
        return mRunReader.invoke_on(
          shard_idx, [](RunReaderService& r) { return r.remove_run_file(); });
      })
      .wait();
  }

  void write_buf(seastar::file& output_file,
                 seastar::temporary_buffer<record_underlying_type> const& buf,
                 std::size_t buf_size,
                 uint64_t& outfile_write_pos,
                 uint64_t& buf_write_pos,
                 unsigned run_lvl,
                 unsigned run_id)
  {
    task_logger.info(
      "writing {} bytes to the run {}/{}", buf_size, run_lvl, run_id);
    output_file
      .dma_write<record_underlying_type>(outfile_write_pos, buf.get(), buf_size)
      .wait();
    output_file.flush().wait();
    outfile_write_pos += buf_size;
    buf_write_pos = 0u;
  }
};

int
main(int argc, char** argv)
{
  seastar::app_template app;
  auto opts_adder = app.add_options();
  opts_adder("input",
             bpo::value<seastar::sstring>(),
             "path to the large file to be sorted")(
    "output", bpo::value<seastar::sstring>(), "path to the sorted output file")(
    "tmp",
    bpo::value<seastar::sstring>()->default_value("/tmp"),
    "path to temp directory where intermediate partitions are stored");

  seastar::sharded<InitialRunService> init_run_srv;
  seastar::sharded<RunReaderService> sharded_reader_srv;
  return app.run(argc, argv, [&] {
    return seastar::async([&] {
      seastar::engine().at_exit(
        [&init_run_srv] { return init_run_srv.stop(); });
      seastar::engine().at_exit(
        [&sharded_reader_srv] { return sharded_reader_srv.stop(); });

      std::size_t const per_cpu_memory = seastar::memory::stats().free_memory();
      fmt::print("------\n"
                 "Number of cores: {}\n"
                 "Raw memory available to each core: {} bytes ({} MB)\n",
                 seastar::smp::count,
                 per_cpu_memory,
                 per_cpu_memory / seastar::MB);

      // On my machine Seastar reports that each core has 1GB free memory.
      // leave 64MB for temporary buffers and also reserve a little bit more
      // to be sure that we don't hit the limit while allocating supplementary
      // structures during regular operation of merging algorithm.
      float const cpu_mem_threshold = 0.8f;
      std::size_t const aligned_cpu_mem = align_to_record_size(
        static_cast<std::size_t>(cpu_mem_threshold * per_cpu_memory));
      fmt::print("Reserved per-cpu memory size: {} bytes ({} MB)\n"
                 "------\n",
                 aligned_cpu_mem,
                 aligned_cpu_mem / seastar::MB);

      auto& opts = app.configuration();
      seastar::sstring const &input_filepath =
                               opts["input"].as<seastar::sstring>(),
                             output_filepath =
                               opts["output"].as<seastar::sstring>(),
                             temp_path = opts["tmp"].as<seastar::sstring>();

      task_logger.info("Opening input file at \"{}\"", input_filepath);

      auto input_fd =
        seastar::open_file_dma(input_filepath, seastar::open_flags::ro).get0();
      std::size_t const input_size = input_fd.size().get0();

      if (input_size % RECORD_SIZE) {
        throw std::runtime_error(
          "Input file size should be a multiple of RECORD_SIZE (4096 bytes)");
      }

      task_logger.info("Successfully opened input file. File size: {} bytes",
                       input_size);

      seastar::engine()
        .file_exists(output_filepath)
        .then([output_filepath](bool exists) {
          // remove output file if it already exists
          if (exists) {
            task_logger.info("Output file \"{}\" already exists. Removing.",
                             output_filepath);
            return seastar::engine().remove_file(output_filepath);
          }
          return seastar::make_ready_future<>();
        })
        .then(
          [&init_run_srv, &input_fd, input_size, aligned_cpu_mem, temp_path] {
            // create initial runs
            return seastar::do_with(
              std::move(input_fd),
              [&init_run_srv,
               input_size,
               aligned_cpu_mem,
               temp_path = std::move(temp_path)](seastar::file& input_fd) {
                return init_run_srv
                  .start(input_fd.dup(), input_size, aligned_cpu_mem, temp_path)
                  .then([&init_run_srv] {
                    return init_run_srv.invoke_on_all(
                      [](auto& p) { return p.start(); });
                  })
                  .then([&input_fd] { return input_fd.close(); });
              });
          })
        .then([&, temp_path] {
          return seastar::async([&, temp_path = std::move(temp_path)] {
            // invoke K-way merge sorting algorithm
            MergeAlgorithm malgo(init_run_srv.local().total_run_count(),
                                 input_size,
                                 aligned_cpu_mem,
                                 output_filepath,
                                 temp_path,
                                 sharded_reader_srv);
            malgo.merge();
          });
        })
        .wait();
    });
  });
}
