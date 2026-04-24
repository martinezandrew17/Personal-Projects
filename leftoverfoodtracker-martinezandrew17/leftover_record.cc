// Please fill in below.
// Andrew Martinez
// CPSC 121L-13 (e.g. CPSC 121L-01)
// April 18, 2024
// martinezandrew2023@csu.fullerton.edu
// martinezandrew17

#include "leftover_record.h"

LeftoverRecord::LeftoverRecord()
    : date_(""),
      meal_(""),
      food_name_(""),
      qty_in_oz_(0.0),
      leftover_reason_(""),
      disposal_mechanism_(""),
      cost_(0.0) {}

std::string LeftoverRecord::GetDate() const { return date_; }

std::string LeftoverRecord::GetMeal() const { return meal_; }

std::string LeftoverRecord::GetFoodName() const { return food_name_; }

double LeftoverRecord::GetQuantityInOz() const { return qty_in_oz_; }

std::string LeftoverRecord::GetLeftoverReason() const {
  return leftover_reason_;
}

std::string LeftoverRecord::GetDisposalMechanism() const {
  return disposal_mechanism_;
}

double LeftoverRecord::GetCost() const { return cost_; }

void LeftoverRecord::SetDate(const std::string& date) { date_ = date; }

void LeftoverRecord::SetMeal(const std::string& meal) { meal_ = meal; }

void LeftoverRecord::SetFoodName(const std::string& food_name) {
  food_name_ = food_name;
}

void LeftoverRecord::SetQuantityInOz(double quantity) { qty_in_oz_ = quantity; }

void LeftoverRecord::SetLeftoverReason(const std::string& reason) {
  leftover_reason_ = reason;
}

void LeftoverRecord::SetDisposalMechanism(const std::string& mechanism) {
  disposal_mechanism_ = mechanism;
}

void LeftoverRecord::SetCost(double cost) { cost_ = cost; }

bool LeftoverRecord::operator==(const LeftoverRecord& other) const {
  return (date_ == other.date_ && meal_ == other.meal_ &&
          food_name_ == other.food_name_ && qty_in_oz_ == other.qty_in_oz_ &&
          leftover_reason_ == other.leftover_reason_ &&
          disposal_mechanism_ == other.disposal_mechanism_ &&
          cost_ == other.cost_);
}
