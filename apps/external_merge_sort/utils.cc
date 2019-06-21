#include "utils.hh"

seastar::logger task_logger("external_merge_sort");

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
