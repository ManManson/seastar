#include <seastar/core/app-template.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/future.hh>
#include <seastar/core/thread.hh>
#include <seastar/util/std-compat.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/file.hh>
#include <seastar/core/file-types.hh>
#include <seastar/util/log.hh>
#include <seastar/core/units.hh>

#include <fmt/printf.h>
#include <boost/range/irange.hpp>
#include <boost/iterator/counting_iterator.hpp>

#include <algorithm>

#include <array>
#include <vector>
#include <set>
#include <queue>

namespace bpo = boost::program_options;

constexpr std::size_t RECORD_SIZE = 4 * seastar::KB;
using record_ptr_vector = std::vector<char const*>;

seastar::logger task_logger("external_merge_sort");

struct record_compare
{
    bool operator ()(char const* lhs, char const* rhs) const
    {
        return std::lexicographical_compare(lhs, lhs + RECORD_SIZE,
                                            rhs, rhs + RECORD_SIZE);
    }
};

struct inverse_record_compare
{
    bool operator ()(char const* lhs, char const* rhs) const
    {
        return std::lexicographical_compare(rhs, rhs + RECORD_SIZE,
                                            lhs, lhs + RECORD_SIZE);
    }
};

constexpr std::size_t align_to_record_size(std::size_t s)
{
    return s / RECORD_SIZE * RECORD_SIZE;
}

record_ptr_vector sort_raw_data(seastar::temporary_buffer<char> const& buf)
{
    record_ptr_vector result;
    result.reserve(buf.size() / RECORD_SIZE);
    for(auto pos = buf.begin(), end = buf.end(); pos != end; pos += RECORD_SIZE)
        result.push_back(&*pos);
    std::sort(result.begin(), result.end(), record_compare());
    return result;
}

class PartitionerService {
    seastar::file mInputFile;
    std::size_t mAlignedCpuMemSize;
    std::size_t mCurrentPartitionId;
    std::size_t mPartitionsCount;
    std::size_t mWritePos;

    seastar::temporary_buffer<char> mTempBuf;
    record_ptr_vector::const_iterator mInBufferSliceIt;

    static constexpr size_t OUT_BUF_SIZE = align_to_record_size(16 * seastar::MB);

public:

    PartitionerService(seastar::file_handle fd, std::size_t input_fd_size, size_t cpu_memory)
        : mInputFile(fd.to_file()),
          mAlignedCpuMemSize(align_to_record_size(cpu_memory)), // align to be a multiple of `record_size`
          mCurrentPartitionId(seastar::engine().cpu_id()),
          mPartitionsCount(input_fd_size / mAlignedCpuMemSize),
          mTempBuf(OUT_BUF_SIZE)
    {}

    seastar::future<> stop() {
        return seastar::make_ready_future<>();
    }

    seastar::future<> start() {
        return seastar::do_until([this] { return mCurrentPartitionId >= mPartitionsCount; }, [this] {
            mWritePos = 0;
            task_logger.info("partition {}. reading {} bytes from input file. Offset {}",
                             mCurrentPartitionId, mAlignedCpuMemSize, mCurrentPartitionId * mAlignedCpuMemSize);
            return mInputFile.dma_read<char>(mCurrentPartitionId * mAlignedCpuMemSize, mAlignedCpuMemSize)
                .then([this](auto buf) {
                    return create_initial_partition(std::move(buf));
                });
        });
    }

    size_t total_partitions_count() const
    {
        return mPartitionsCount;
    }

private:

    seastar::future<> create_initial_partition(seastar::temporary_buffer<char> buf)
    {
        auto rptr_vec = sort_raw_data(buf);
        mInBufferSliceIt = rptr_vec.cbegin();
        return seastar::do_with(std::move(buf), std::move(rptr_vec), [&] (auto& buf, auto& rptr_vec) {
            return seastar::open_file_dma("/opt/test_data/part" + seastar::to_sstring(mCurrentPartitionId),
                seastar::open_flags::create | seastar::open_flags::truncate | seastar::open_flags::wo)
                .then([&](seastar::file output_partition_fd) {
                    return seastar::do_with(std::move(output_partition_fd), [&](auto& output_partition_fd) {
                        task_logger.info("writing to partition {}", mCurrentPartitionId);
                        mCurrentPartitionId += seastar::smp::count;
                        return seastar::do_until([&] { return mInBufferSliceIt == rptr_vec.cend(); }, [&] {
                            auto actual_size = fill_output_buffer(rptr_vec);
                            return output_partition_fd.dma_write(mWritePos, mTempBuf.get(), actual_size)
                                    .then([&](size_t s) {
                                        task_logger.info("successfully written {} bytes to partition", s);
                                        mWritePos += s;
                                    });
                        });
                    });
                });
        });
    }


    size_t fill_output_buffer(record_ptr_vector const& v)
    {
        static constexpr std::size_t MAX_RECORDS_TO_FILL = OUT_BUF_SIZE / RECORD_SIZE;

        char* write_ptr = mTempBuf.get_write();

        std::size_t slice_size;
        auto distance_to_end = static_cast<size_t>(std::distance(mInBufferSliceIt, v.cend()));
        record_ptr_vector::const_iterator end_it;
        if(distance_to_end < MAX_RECORDS_TO_FILL)
        {
            end_it = v.cend();
            slice_size = distance_to_end * RECORD_SIZE;
        }
        else
        {
            end_it = mInBufferSliceIt + MAX_RECORDS_TO_FILL;
            slice_size = MAX_RECORDS_TO_FILL * RECORD_SIZE;
        }
        while(mInBufferSliceIt != end_it)
        {
            std::copy(*mInBufferSliceIt, *mInBufferSliceIt + RECORD_SIZE, write_ptr);
            write_ptr += RECORD_SIZE;
            ++mInBufferSliceIt;
        }
        return slice_size;
    }
};

struct DataFragment
{
    const char* mBeginPtr; // points to an array of 4K records
    std::size_t mDataSize; // array size in bytes
};

class InitialRunReader
{
    static constexpr size_t BUF_SIZE = align_to_record_size(16 * seastar::MB);

    seastar::file mFd;
    seastar::temporary_buffer<char> mBuf;
    std::size_t mActualBufSize = 0u;
    std::size_t mCurrentReadPos = 0;

public:

    InitialRunReader() = default;

    seastar::future<> stop() const {
        return seastar::make_ready_future<>();
    }

    seastar::future<> set_file(seastar::file fd)
    {
        mFd = std::move(fd);
        return seastar::make_ready_future<>();
    }

    seastar::future<> open_file(seastar::sstring const& path) {
        return seastar::open_file_dma(path, seastar::open_flags::ro).then([&] (seastar::file fd) {
            return set_file(std::move(fd));
        });
    }

    seastar::future<> fetch_data()
    {
        return seastar::async([&] {
            mBuf = mFd.dma_read<char>(mCurrentReadPos, BUF_SIZE).get0();
            mActualBufSize = mBuf.size();
            mCurrentReadPos += mActualBufSize;
        });
    }

    DataFragment get_next_fragment()
    {
        return DataFragment{mBuf.get(), mActualBufSize};
    }
};

void merge(unsigned initial_partition_count, seastar::sharded<InitialRunReader>& sharded_reader)
{
    static constexpr size_t OUT_BUF_SIZE = align_to_record_size(16 * seastar::MB);
    auto const reader_shard_indices = boost::irange(1u, seastar::smp::count);

    seastar::temporary_buffer<char> out_buf(OUT_BUF_SIZE);
    std::size_t buf_write_pos = 0u;

    seastar::file output_file = seastar::open_file_dma("/opt/test_data/sorted",
        seastar::open_flags::create | seastar::open_flags::truncate | seastar::open_flags::wo).get0();
    std::size_t outfile_write_pos = 0u;

    sharded_reader.start().get0();

    // assign unprocessed initial partition ids
    std::queue<unsigned> unprocessed_ids;
    for(unsigned i = 0; i != initial_partition_count; ++i) {
        unprocessed_ids.push(i);
    }

    // open first files and fetch data
    sharded_reader.invoke_on_others([&] (InitialRunReader& r) {
        task_logger.info("opening initial file on shard {}", seastar::engine().cpu_id());
        return r.open_file("/opt/test_data/part" + seastar::to_sstring(seastar::engine().cpu_id()));
    }).get0();
    sharded_reader.invoke_on_others([] (InitialRunReader& r) {
        task_logger.info("fetching data on cpu_id {}", seastar::engine().cpu_id());
        return r.fetch_data();
    }).get0();

    task_logger.info("successfully fetched initial data on all reader shards");

    std::priority_queue<char const*, std::vector<char const*>, inverse_record_compare> priorq;

    // while there is something to process
    while(!unprocessed_ids.empty()) {
        for(auto i : reader_shard_indices) {
            DataFragment fragment = sharded_reader.invoke_on(i, [](InitialRunReader& r) {
                return r.get_next_fragment();
            }).get0();
            // if there is no fragment, then it means that the file is over
            // and we need to open a new one and fetch data afterwards
            if(!fragment.mBeginPtr) {
                if(unprocessed_ids.empty())
                    break;
                // take partition id from unprocessed queue, assign it to the shard that needs to be updated
                unsigned part_id = unprocessed_ids.front();
                sharded_reader.invoke_on(i, [part_id](InitialRunReader& r) {
                    //r.set_next_file(part_id);
                    r.fetch_data();
                }).get0();
                unprocessed_ids.pop();
            }
            // sort data locally
            for(std::size_t fragment_pos = 0u; fragment_pos < fragment.mDataSize; fragment_pos += RECORD_SIZE) {
                priorq.push(fragment.mBeginPtr + fragment_pos);
            }
            if(seastar::need_preempt())
                seastar::thread::yield(); // yield to the reactor event loop for a while
        }

        while(!priorq.empty()) {
            char const* rec_ptr = priorq.top();
            std::copy(rec_ptr, rec_ptr + RECORD_SIZE, out_buf.get_write() + buf_write_pos);
            buf_write_pos += RECORD_SIZE;
            priorq.pop();
            // flush full output buffer contents to the output file
            if(buf_write_pos >= OUT_BUF_SIZE) {
                buf_write_pos = 0u;
                output_file.dma_write<char>(outfile_write_pos, out_buf.get(), OUT_BUF_SIZE).get0();
                outfile_write_pos += OUT_BUF_SIZE;
            }
        }
        // transfer control to reactor event loop again to prevent reactor stalls
        seastar::thread::yield();
    }
}

int main(int argc, char** argv) {
    seastar::app_template app;
    auto opts_adder = app.add_options();
    opts_adder("input", bpo::value<seastar::sstring>()->default_value("/opt/test_data/tf1"), "path to the large file to be sorted")
              ("output", bpo::value<seastar::sstring>()->default_value("/opt/test_data/output_tf"), "path to the sorted output file");

    seastar::sharded<PartitionerService> partitioner;
    seastar::sharded<InitialRunReader> sharded_reader;
    return app.run(argc, argv, [&] {
        return seastar::async([&] {
            std::size_t const available_memory = seastar::memory::stats().free_memory() / 2,
                per_cpu_memory = available_memory / seastar::smp::count;
            fmt::print("Number of cores: {}\n"
                       "Total available memory: {} bytes ({} MB)\n"
                       "Memory available to each core: {} bytes ({} MB)\n",
                       seastar::smp::count,
                       available_memory, available_memory / 1024 / 1024,
                       per_cpu_memory, per_cpu_memory / 1024 / 1024);

//            auto& opts = app.configuration();
//            seastar::sstring const& input_filepath = opts["input"].as<seastar::sstring>();
//            auto input_fd = seastar::open_file_dma(input_filepath, seastar::open_flags::ro).get0();
//            std::size_t const input_size = input_fd.size().get0();

//            partitioner.start(input_fd.dup(), input_size, per_cpu_memory).get0();
//            seastar::engine().at_exit([&partitioner] {
//                return partitioner.stop();
//            });

//            // initial partitioning pass
//            partitioner.invoke_on_all([] (auto& p) {
//                return p.start();
//            }).get0();
//            input_fd.close().get0();

            // invoke K-way merge sorting algorithm
            merge(137/*partitioner.local().total_partitions_count()*/, sharded_reader);
        });
    });
}
