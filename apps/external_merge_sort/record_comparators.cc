#include <seastar/core/shared_ptr.hh>

#include "record_comparators.hh"
#include "run_reader.hh"

bool
record_compare::operator()(record_underlying_type const* lhs,
                           record_underlying_type const* rhs) const
{
  return std::lexicographical_compare(
    lhs, lhs + RECORD_SIZE, rhs, rhs + RECORD_SIZE);
}

bool
inverse_record_compare::operator()(priorq_element const& lhs,
                                   priorq_element const& rhs) const
{
  return std::lexicographical_compare(
    rhs.first, rhs.first + RECORD_SIZE, lhs.first, lhs.first + RECORD_SIZE);
}
