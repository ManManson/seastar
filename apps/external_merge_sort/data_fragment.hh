#pragma once

#include "utils.hh"

#include <cstddef>

struct DataFragment
{
  const record_underlying_type* mBeginPtr; // points to an array of 4K records
  std::size_t mDataSize;                   // array size in bytes
};
