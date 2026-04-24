#ifndef LEFTOVER_RECORD_H_
#define LEFTOVER_RECORD_H_

#include <string>

class LeftoverRecord {
 public:
  LeftoverRecord();

  void SetDate(const std::string& date);
  void SetMeal(const std::string& meal);
  void SetFoodName(const std::string& food_name);
  void SetQuantityInOz(double qty_in_oz);
  void SetLeftoverReason(const std::string& leftover_reason);
  void SetDisposalMechanism(const std::string& disposal_mechanism);
  void SetCost(double cost);

  double cost;
  LeftoverRecord(double cost) : cost(cost) {}

  std::string meal() const { return meal_; }

  std::string disposal_mechanism() const { return disposal_mechanism_; }

  std::string reason() const { return reason_; }
  std::string food_item() const { return food_item_; }

  std::string GetDate() const;
  std::string GetMeal() const;
  std::string GetFoodName() const;
  double GetQuantityInOz() const;
  std::string GetLeftoverReason() const;
  std::string GetDisposalMechanism() const;
  double GetCost() const;

  bool operator==(const LeftoverRecord& other) const;

 private:
  std::string date_;
  std::string meal_;
  std::string food_name_;
  double qty_in_oz_;
  std::string leftover_reason_;
  std::string disposal_mechanism_;
  double cost_;
  std::string reason_;
  std::string food_item_;
};

#endif
