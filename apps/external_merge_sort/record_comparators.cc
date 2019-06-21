#include "record_comparators.hh"

bool
record_compare::operator()(record_underlying_type const* lhs,
                           record_underlying_type const* rhs) const
{
  return std::lexicographical_compare(
    lhs, lhs + RECORD_SIZE, rhs, rhs + RECORD_SIZE);
}

bool
inverse_record_compare::operator()(record_underlying_type const* lhs,
                                   record_underlying_type const* rhs) const
{
  return std::lexicographical_compare(
    rhs, rhs + RECORD_SIZE, lhs, lhs + RECORD_SIZE);
}
