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
using record_type = std::array<char, record_size>;
using record_ptr_vector = std::vector<char const*>;

seastar::logger task_logger("external_merge_sort");

struct record_compare
{
    bool operator ()(record_type const& lhs, record_type const& rhs) const
    {
        return std::lexicographical_compare(lhs.data(), lhs.data() + record_size,
                                            rhs.data(), rhs.data() + record_size);
    }

    bool operator ()(record_type const* lhs, record_type const* rhs) const
    {
        return std::lexicographical_compare(lhs->data(), lhs->data() + record_size,
                                            rhs->data(), rhs->data() + record_size);
    }

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
    std::size_t mOutputRecOffset;
    std::size_t mWritePos;

    seastar::temporary_buffer<char> mTempBuf;

    static constexpr size_t OUT_BUF_SIZE = align_to_record_size(32 * seastar::MB);

public:

    PartitionerService(seastar::file_handle fd, std::size_t input_fd_size, size_t cpu_memory)
        : mInputFile(fd.to_file()),
          mAlignedCpuMemSize(align_to_record_size(cpu_memory)), // align to be a multiple of `record_size`
          mCurrentPartitionId(seastar::engine().cpu_id()),
          mPartitionsCount(input_fd_size / mAlignedCpuMemSize),
          mOutputRecOffset(0u),
          mTempBuf(OUT_BUF_SIZE)
    {}

    seastar::future<> stop() {
        return seastar::make_ready_future<>();
    }

    seastar::future<> start() {
        return seastar::do_until([this] { return mCurrentPartitionId >= mPartitionsCount; }, [this] {
            mWritePos = 0;
            mOutputRecOffset = 0;
            task_logger.info("reading {} bytes from input file. Offset {}", mAlignedCpuMemSize, mCurrentPartitionId * mAlignedCpuMemSize);
            return mInputFile.dma_read<char>(mCurrentPartitionId * mAlignedCpuMemSize, mAlignedCpuMemSize)
                .then([this](auto buf) {
                    task_logger.info("successfully read data");
                    return create_initial_partition(std::move(buf));
                });
        });
    }

private:

    seastar::future<> create_initial_partition(seastar::temporary_buffer<char> buf)
    {
        return seastar::do_with(std::move(buf), [&] (auto& buf) {
            auto record_ptr_vector = sort_raw_data(buf);
            return seastar::do_with(std::move(record_ptr_vector), [&] (auto& rptr_v) {
                return seastar::open_file_dma("/opt/test_data/part" + seastar::to_sstring(mCurrentPartitionId),
                    seastar::open_flags::create | seastar::open_flags::truncate | seastar::open_flags::wo)
                    .then([&](seastar::file output_partition_fd) {
                        return seastar::do_with(std::move(output_partition_fd), mCurrentPartitionId, [&](auto& output_partition_fd, auto& prev_partition_id) {
                            task_logger.info("writing to partition {}", prev_partition_id);
                            mCurrentPartitionId += seastar::smp::count;
                            return seastar::do_until([&] { return buf.size() / record_size < mOutputRecOffset; }, [&] {
                                return fill_output_buffer(rptr_v).then([&] {
                                    return output_partition_fd.dma_write(mWritePos, mTempBuf.get(), OUT_BUF_SIZE)
                                            .then([&](size_t s) {
                                                task_logger.info("successfully written {} bytes to partition {}", s, prev_partition_id);
                                                mWritePos += s;
                                            });
                                });
                            });
                        });
                });
            });
        });
    }


    seastar::future<> fill_output_buffer(record_ptr_vector const& v)
    {
        static constexpr std::size_t MAX_RECORDS_TO_FILL = OUT_BUF_SIZE / record_size;
        char* write_ptr = mTempBuf.get_write();
        auto it = v.cbegin() + mOutputRecOffset;
        record_ptr_vector::const_iterator end_it;
        if(v.size() - mOutputRecOffset < MAX_RECORDS_TO_FILL)
            end_it = v.end();
        else
            end_it = it + MAX_RECORDS_TO_FILL;
        while(it != end_it)
        {
            std::copy(*it, *it + record_size, write_ptr);
            write_ptr += record_size;
            ++it;
        }
        mOutputRecOffset += MAX_RECORDS_TO_FILL;
        return seastar::make_ready_future<>();
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
