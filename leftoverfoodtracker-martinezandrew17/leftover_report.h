#ifndef LEFTOVER_REPORT_H_
#define LEFTOVER_REPORT_H_

#include <string>
#include <unordered_map>
#include <vector>

#include "leftover_record.h"

class LeftoverReport {
 public:
  LeftoverReport(const std::vector<LeftoverRecord>& leftover_records);

  std::vector<std::string> MostCommonLeftover() const;
  std::vector<std::string> MostCostlyLeftoverProducingMeals() const;
  double TotalCostOfLeftovers() const;
  std::vector<std::string> MostCommonLeftoverReasons() const;
  std::vector<std::string> MostCommonDisposalMechanisms() const;
  std::vector<std::string> SuggestedLeftoverReductionStrategies() const;

  void AddRecord(const LeftoverRecord& record);

 private:
  std::vector<LeftoverRecord> leftover_records_;
  std::vector<std::string> most_common_leftover_;
  std::vector<std::string> most_costly_leftover_producing_meals_;
  double total_cost_of_leftovers_;
  std::vector<std::string> most_common_leftover_reasons_;
  std::vector<std::string> most_common_disposal_mechanism_;
  std::vector<std::string> suggested_strategies_to_reduce_leftovers_;
};

#endif
