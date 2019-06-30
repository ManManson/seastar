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

#include "data_fragment.hh"
#include "initial_run_service.hh"
#include "merge_algorithm.hh"
#include "record_comparators.hh"
#include "run_reader_service.hh"
#include "utils.hh"

namespace bpo = boost::program_options;

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
  return app.run(argc, argv, [&] {
    return seastar::async([&] {
      seastar::engine().at_exit(
        [&init_run_srv] { return init_run_srv.stop(); });

      std::size_t const per_cpu_memory = seastar::memory::stats().free_memory();
      fmt::print("------\n"
                 "Number of cores: {}\n"
                 "Raw memory available to each core: {} bytes ({} MB)\n",
                 seastar::smp::count,
                 per_cpu_memory,
                 per_cpu_memory / seastar::MB);

#ifdef SEASTAR_DEFAULT_ALLOCATOR
      // On my machine Seastar reports that each core has 1GB free memory.
      // leave 64MB for temporary buffers and also reserve a little bit more
      // to be sure that we don't hit the limit while allocating supplementary
      // structures during regular operation of merging algorithm.
      float const cpu_mem_threshold = 0.8f;
      std::size_t const aligned_cpu_mem = align_to_record_size(
        static_cast<std::size_t>(cpu_mem_threshold * per_cpu_memory));
#else
      // It turns out that Seastar's custom allocator doesn't like very large
      // contiguous allocations, so reserve only a small fraction of each core
      // memory for internal sorting and reading buffers. Could not determine
      // exact limits, so take a value that is almost guaranteed to be safe.
      std::size_t const aligned_cpu_mem =
        align_to_record_size(128u * seastar::MB);
#endif
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
                                 temp_path);
            malgo.merge();
          });
        })
        .wait();
    });
  });
}
