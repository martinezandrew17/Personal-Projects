// Please fill in below.
// Andrew Martinez
// CPSC 121L-13 (e.g. CPSC 121L-01)
// April 18, 2024
// martinezandrew2023@csu.fullerton.edu
// martinezandrew17

#include "leftover_tracker.h"

#include <algorithm>

#include "leftover_record.h"
#include "leftover_report.h"


LeftoverTracker::LeftoverTracker() {
  leftover_records_ = std::vector<LeftoverRecord>();
}

bool LeftoverTracker::AddRecord(const LeftoverRecord& record) {
  for (LeftoverRecord existing : leftover_records_) {
    if (existing.GetDate() == record.GetDate() &&
        existing.GetMeal() == record.GetMeal() &&
        existing.GetFoodName() == record.GetFoodName() &&
        existing.GetQuantityInOz() == record.GetQuantityInOz() &&
        existing.GetLeftoverReason() == record.GetLeftoverReason() &&
        existing.GetDisposalMechanism() == record.GetDisposalMechanism() &&
        existing.GetCost() == record.GetCost()) {
      return false;
    }
  }
  leftover_records_.push_back(record);
  return true;
}
const std::vector<LeftoverRecord>& LeftoverTracker::GetRecords() const {
  return leftover_records_;
}
LeftoverReport GetLeftoverReport() { return GetLeftoverReport(); }

bool LeftoverTracker::DeleteRecord(const LeftoverRecord& record) {
  auto new_end = std::remove_if(
      leftover_records_.begin(), leftover_records_.end(),
      [&record](const LeftoverRecord& existing) { return existing == record; });
  leftover_records_.erase(new_end, leftover_records_.end());

  return new_end != leftover_records_.end();
}

bool LeftoverTracker::recordExistsInVector(const LeftoverRecord& record) {
  for (const LeftoverRecord& existing : leftover_records_) {
    if (existing == record) {
      return true;
    }
  }
  return false;
}

LeftoverReport LeftoverTracker::GenerateReport() const {
  return LeftoverReport(leftover_records_);
}