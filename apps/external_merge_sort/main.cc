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

#include <algorithm>

#include <array>
#include <vector>

namespace bpo = boost::program_options;

constexpr std::size_t record_size = 4 * seastar::KB;
using record_ptr_vector = std::vector<char const*>;

seastar::logger task_logger("external_merge_sort");

struct record_compare
{
    bool operator ()(char const* lhs, char const* rhs) const
    {
        return std::lexicographical_compare(lhs, lhs + record_size,
                                            rhs, rhs + record_size);
    }
};

constexpr std::size_t align_to_record_size(std::size_t s)
{
    return s / record_size * record_size;
}

record_ptr_vector sort_raw_data(seastar::temporary_buffer<char> const& buf)
{
    record_ptr_vector result;
    result.reserve(buf.size() / record_size);
    for(auto pos = buf.begin(), end = buf.end(); pos != end; pos += record_size)
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
        static constexpr std::size_t MAX_RECORDS_TO_FILL = OUT_BUF_SIZE / record_size;

        char* write_ptr = mTempBuf.get_write();

        std::size_t slice_size;
        auto distance_to_end = static_cast<size_t>(std::distance(mInBufferSliceIt, v.cend()));
        record_ptr_vector::const_iterator end_it;
        if(distance_to_end < MAX_RECORDS_TO_FILL)
        {
            end_it = v.cend();
            slice_size = distance_to_end * record_size;
        }
        else
        {
            end_it = mInBufferSliceIt + MAX_RECORDS_TO_FILL;
            slice_size = MAX_RECORDS_TO_FILL * record_size;
        }
        while(mInBufferSliceIt != end_it)
        {
            std::copy(*mInBufferSliceIt, *mInBufferSliceIt + record_size, write_ptr);
            write_ptr += record_size;
            ++mInBufferSliceIt;
        }
        if(mInBufferSliceIt != v.cend())
            ++mInBufferSliceIt;

        return slice_size;
    }
};

int main(int argc, char** argv) {
    seastar::app_template app;
    auto opts_adder = app.add_options();
    opts_adder("input", bpo::value<seastar::sstring>()->default_value("/opt/test_data/tf1"), "path to the large file to be sorted")
              ("output", bpo::value<seastar::sstring>()->default_value("/opt/test_data/output_tf"), "path to the sorted output file");

    seastar::sharded<PartitionerService> partitioner;
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

            auto& opts = app.configuration();
            seastar::sstring const& input_filepath = opts["input"].as<seastar::sstring>();
            auto input_fd = seastar::open_file_dma(input_filepath, seastar::open_flags::ro).get0();
            std::size_t const input_size = input_fd.size().get0();

            partitioner.start(input_fd.dup(), input_size, per_cpu_memory).get0();
            seastar::engine().at_exit([&partitioner] {
                return partitioner.stop();
            });

            // initial partitioning pass
            partitioner.invoke_on_all([] (auto& p) {
                return p.start();
            }).get0();
            input_fd.close().get0();

            // repeatedly invoke merge passes until there is nothing to merge
        });
    });
}
