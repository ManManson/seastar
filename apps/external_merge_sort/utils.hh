#pragma once

#include <cstddef>
#include <vector>

#include <seastar/core/units.hh>
#include <seastar/core/sstring.hh>
#include <seastar/util/log.hh>

constexpr std::size_t RECORD_SIZE = 4 * seastar::KB;
using record_underlying_type = unsigned char;
using record_ptr_vector = std::vector<record_underlying_type const*>;

extern seastar::logger task_logger;

// Align `s` to be a multiple of `RECORD_SIZE`
constexpr std::size_t
align_to_record_size(std::size_t s)
{
  return s / RECORD_SIZE * RECORD_SIZE;
}

// Integer division with rounding policy that rounds up
constexpr std::size_t
round_up_int_div(std::size_t num, std::size_t denom)
{
  return (num + denom - 1) / denom;
}

seastar::sstring
run_filename(seastar::sstring const& dst, unsigned level, unsigned id);
