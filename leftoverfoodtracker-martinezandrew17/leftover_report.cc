
#include "leftover_report.h"

#include <algorithm>
#include <string>


LeftoverReport::LeftoverReport(
    const std::vector<LeftoverRecord>& leftover_records)
    : leftover_records_(leftover_records) {}

void LeftoverReport::AddRecord(const LeftoverRecord& record) {
  leftover_records_.push_back(record);
}
