#pragma once

#include "utils.hh"

struct record_compare
{
  bool operator()(record_underlying_type const* lhs,
                  record_underlying_type const* rhs) const;
};

class RunReaderService;

struct inverse_record_compare
{
  bool operator()(
    std::pair<record_underlying_type const*, RunReaderService*> const& lhs,
    std::pair<record_underlying_type const*, RunReaderService*> const& rhs)
    const;
};
