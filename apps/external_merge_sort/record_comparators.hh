#pragma once

#include <utility>

#include "utils.hh"

namespace seastar {

template<typename T>
class lw_shared_ptr;

} // namespace seastar

struct record_compare
{
  bool operator()(record_underlying_type const* lhs,
                  record_underlying_type const* rhs) const;
};

class RunReader;

using priorq_element = std::pair<record_underlying_type const*,
                                 seastar::lw_shared_ptr<RunReader>>;

struct inverse_record_compare
{
  bool operator()(priorq_element const& lhs, priorq_element const& rhs) const;
};
