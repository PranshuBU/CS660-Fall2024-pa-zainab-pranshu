
#include <algorithm>
#include <cmath>
#include <db/ColumnStats.hpp>
#include <iostream> // For debugging
#include <numeric>
#include <stdexcept>
#include <vector>

namespace db {

ColumnStats::ColumnStats(unsigned buckets, int min, int max)
    : buckets(buckets), min(min), max(max), histogram(buckets, 0), count(0) {
  if (buckets == 0 || min >= max) {
    throw std::invalid_argument("Invalid bucket count or range");
  }
  bucketWidth = std::ceil(static_cast<double>(max - min + 1) / buckets); // Inclusive range
}

void ColumnStats::addValue(int v) {
  if (v < min || v > max) {
    return; // Ignore values outside range
  }
  size_t bucketIndex = static_cast<size_t>((v - min) / bucketWidth);
  bucketIndex = std::min(bucketIndex, histogram.size() - 1); // Clamp to valid range
  histogram[bucketIndex]++;
  count++;
}

size_t ColumnStats::estimateCardinality(PredicateOp op, int v) const {
  if (count == 0) {
    return 0; // No data
  }

  // Handle out-of-range values
  if (v < min) {
    return (op == PredicateOp::GT || op == PredicateOp::GE) ? count : 0;
  }
  if (v > max) {
    return (op == PredicateOp::LT || op == PredicateOp::LE) ? count : 0;
  }

  // Determine bucket information
  size_t bucketIndex = static_cast<size_t>((v - min) / bucketWidth);
  bucketIndex = std::min(bucketIndex, histogram.size() - 1); // Clamp to valid range
  double bucketHeight = static_cast<double>(histogram[bucketIndex]);
  int bucketStart = min + static_cast<int>(bucketIndex * bucketWidth);
  int bucketEnd = bucketStart + static_cast<int>(bucketWidth) - 1;
  double fraction = 0.0;

  // Estimate cardinality based on the operation
  switch (op) {
  case PredicateOp::EQ:
    if (bucketHeight == 0) {
      return 0; // No contribution from empty buckets
    }
    return static_cast<size_t>(bucketHeight / bucketWidth);

  case PredicateOp::NE:
    if (bucketHeight == 0) {
      return count; // All other values
    }
    return count - static_cast<size_t>(bucketHeight / bucketWidth);

  case PredicateOp::LT:
    fraction = (v > bucketStart) ? static_cast<double>(v - bucketStart) / bucketWidth : 0.0;
    return static_cast<size_t>(fraction * bucketHeight) +
           std::accumulate(histogram.begin(), histogram.begin() + bucketIndex, 0UL);

  case PredicateOp::LE:
    fraction = (v >= bucketStart) ? static_cast<double>(v - bucketStart + 1) / bucketWidth : 0.0;
    return static_cast<size_t>(fraction * bucketHeight) +
           std::accumulate(histogram.begin(), histogram.begin() + bucketIndex, 0UL);

  case PredicateOp::GT:
    fraction = (v < bucketEnd) ? static_cast<double>(bucketEnd - v) / bucketWidth : 0.0;
    return static_cast<size_t>(fraction * bucketHeight) +
           std::accumulate(histogram.begin() + bucketIndex + 1, histogram.end(), 0UL);

  // case PredicateOp::GE:
  //   fraction = (v <= bucketEnd) ? static_cast<double>(bucketEnd - v + 1) / bucketWidth : 0.0;
  //   return static_cast<size_t>(fraction * bucketHeight) +
  //          std::accumulate(histogram.begin() + bucketIndex, histogram.end(), 0UL);
  case PredicateOp::GE:
    if (bucketHeight == 0) {
      return std::accumulate(histogram.begin() + bucketIndex, histogram.end(), 0UL);
    }
    if (v <= bucketEnd) {
      fraction = static_cast<double>(bucketEnd - v + 1) / bucketWidth;
      fraction = std::clamp(fraction, 0.0, 1.0);
    } else {
      fraction = 0.0; // No contribution from this bucket
    }
    return static_cast<size_t>(fraction * bucketHeight) +
           std::accumulate(histogram.begin() + bucketIndex + 1, histogram.end(), 0UL);

  default:
    throw std::logic_error("Unsupported operation");
  }
}

} // namespace db
