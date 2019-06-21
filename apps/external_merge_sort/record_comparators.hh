#pragma once

#include "utils.hh"

struct record_compare
{
  bool operator()(record_underlying_type const* lhs,
                  record_underlying_type const* rhs) const;
};

struct inverse_record_compare
{
  bool operator()(record_underlying_type const* lhs,
                  record_underlying_type const* rhs) const;
};
