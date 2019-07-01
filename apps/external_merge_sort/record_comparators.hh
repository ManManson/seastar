#pragma once

#include <seastar/core/shared_ptr.hh>

#include <utility>

#include "utils.hh"

struct record_compare
{
  bool operator()(record_underlying_type const* lhs,
                  record_underlying_type const* rhs) const;
};

class RunReader;

using priorq_element =
  std::pair<record_underlying_type const*, seastar::lw_shared_ptr<RunReader>>;

struct inverse_record_compare
{
  bool operator()(priorq_element const& lhs, priorq_element const& rhs) const;
};
