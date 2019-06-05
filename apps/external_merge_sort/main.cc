#include <seastar/core/app-template.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/future.hh>
#include <seastar/core/thread.hh>
#include <seastar/util/std-compat.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/file.hh>
#include <seastar/core/file-types.hh>
#include <seastar/util/log.hh>

#include <fmt/printf.h>
#include <boost/range/irange.hpp>

#include <sys/uio.h>
#include <algorithm>

namespace bpo = boost::program_options;

constexpr std::size_t record_size = 1 << 12; // 4096 bytes
constexpr std::size_t records_per_partition = 10000; // Approx. 40MB

seastar::logger task_logger("external_merge_sort");

struct iovec_compare
{
    bool operator()(iovec const& lhs, iovec const& rhs) const
    {
       auto lhs_base = reinterpret_cast< unsigned char* >( lhs.iov_base ),
            rhs_base = reinterpret_cast< unsigned char* >( rhs.iov_base );
       return std::lexicographical_compare(lhs_base, lhs_base + record_size,
                                           rhs_base, rhs_base + record_size);
    }
};

std::vector< iovec > sort_raw_data(seastar::temporary_buffer<unsigned char> const& buf)
{
    std::vector< iovec > result;
    result.reserve(buf.size() / record_size);
    for(auto pos = buf.begin(), end = buf.end(); pos != end; pos += record_size)
        result.push_back(iovec{const_cast< unsigned char* >( &*pos ), record_size});
    std::sort(result.begin(), result.end(), iovec_compare());
    return result;
}

class PartitionerService {
    seastar::file mFile;
    std::uint32_t mCurrentPartitionId;
    std::uint64_t mPartitionCount;

    std::size_t mLeft;
    std::size_t mWritePos;

public:

    PartitionerService(seastar::file_handle fd)
        : mFile(fd.to_file()),
          mCurrentPartitionId(seastar::engine().cpu_id()),
          mPartitionCount(mFile.size().get0() / record_size / records_per_partition)
    {}

    seastar::future<> stop() { return seastar::make_ready_future<>(); }

    seastar::future<> start() {
        return seastar::do_until([this] { return mCurrentPartitionId >= mPartitionCount; }, [this] {
            mLeft = record_size * records_per_partition;
            mWritePos = 0;
            auto const bytes_to_read = record_size * records_per_partition;
            task_logger.info("reading {} bytes from input file. Offset {}", bytes_to_read, mCurrentPartitionId * bytes_to_read);
            return mFile.dma_read<unsigned char>(mCurrentPartitionId * bytes_to_read, bytes_to_read)
                .then([this](auto buf) {
                    return create_partition(std::move(buf));
                });
        });
    }

private:

    seastar::future<> create_partition(seastar::temporary_buffer<unsigned char> buf)
    {
        return seastar::do_with(std::move(buf), [&] (auto& buf) {
            return seastar::do_with(sort_raw_data(buf), [&](auto& sorted_buf) {
                return seastar::open_file_dma("/opt/test_data/partition" + seastar::to_sstring(mCurrentPartitionId),
                    seastar::open_flags::create | seastar::open_flags::truncate | seastar::open_flags::wo)
                    .then([&](seastar::file output_partition_fd) {
                        return seastar::do_with(std::move(output_partition_fd), mCurrentPartitionId, [&](auto& output_partition_fd, auto& prev_partition_id) {
                            task_logger.info("writing to partition {}", prev_partition_id);
                            mCurrentPartitionId += seastar::smp::count;
                            return seastar::do_until([this] { return mLeft == 0; }, [&] {
                                return output_partition_fd.dma_write(mWritePos, sorted_buf)
                                    .then([&](size_t s) {
                                        task_logger.info("successfully written {} bytes to partition {}", s, prev_partition_id);
                                        mWritePos += s;
                                        mLeft -= s;
                                        sorted_buf.erase(sorted_buf.begin(), sorted_buf.begin() + s / record_size);
                                        return seastar::make_ready_future<>();
                                });
                            });
                        });
                    });
            });
        });
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
            auto& opts = app.configuration();
            seastar::sstring const& input_filepath = opts["input"].as<seastar::sstring>();
            auto input_fd = seastar::open_file_dma(input_filepath, seastar::open_flags::ro).get0();

            std::uint64_t const partition_count = input_fd.size().get0() / record_size / records_per_partition;
            fmt::print("overall partitions count: {}\n", partition_count);

            partitioner.start(input_fd.dup()).get0();
            seastar::engine().at_exit([&partitioner] {
                return partitioner.stop();
            });
            // initial partitioning pass
            partitioner.invoke_on_all([] (auto& p) {
                return p.start();
            }).get0();
            input_fd.close().get0();
        });
    });
}
