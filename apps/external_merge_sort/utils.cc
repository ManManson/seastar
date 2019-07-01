#include <seastar/core/print.hh>

#include "utils.hh"

seastar::logger task_logger("external_merge_sort");

seastar::sstring
run_filename(seastar::sstring const& dst, unsigned level, unsigned id)
{
  return fmt::format("{}/run-lvl{}-{}", dst, level, id);
}
