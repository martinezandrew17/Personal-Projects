#include "leftover_tracker_backend.h"

#include <filesystem>
#include <string>
#include <unordered_map>
#include <vector>

#include "leftover_record.h"
#include "leftover_report.h"
#include "leftover_tracker_backend.h"
#include "server_utils/rapidjson/document.h"      // rapidjson's DOM-style API
#include "server_utils/rapidjson/prettywriter.h"  // for stringify JSON
#include "server_utils/rapidjson/rapidjson.h"
#include "server_utils/rapidjson/stringbuffer.h"  // wrapper of C stream for prettywriter as output
#include "server_utils/rapidjson/writer.h"

// Util function to convert a LeftoverRecord class object into a serialized
// JSON object.
void SerializeLeftoverRecordToJSON(
    const LeftoverRecord& record,
    rapidjson::Writer<rapidjson::StringBuffer>* writer) {
  writer->StartObject();

  // Serialize Date
  writer->String("date_");
  std::string date;
  date = record.GetDate();
  writer->String(date.c_str());

  // Serialize Meal
  writer->String("meal_");
  std::string meal;
  meal = record.GetMeal();
  writer->String(meal.c_str());

  // Serialize Food Name
  writer->String("food_name_");
  std::string food_name;
  food_name = record.GetFoodName();
  writer->String(food_name.c_str());

  // Serialize Quantity In Ounces
  writer->String("qty_in_oz_");
  double qty_in_oz;
  qty_in_oz = record.GetQuantityInOz();
  writer->Double(qty_in_oz);

  // Serialize Leftover Reasons
  writer->String("leftover_reason_");
  std::string leftover_reason;
  leftover_reason = record.GetLeftoverReason();
  writer->String(leftover_reason.c_str());

  // Serialize Disposal Mechanism
  writer->String("disposal_mechanism_");
  std::string disposal_mechanism;
  disposal_mechanism = record.GetDisposalMechanism();
  writer->String(disposal_mechanism.c_str());

  // Serialize Cost
  writer->String("cost_");
  double cost;
  cost = record.GetCost();
  writer->Double(cost);

  writer->EndObject();
}

// Util function to convert a serialized JSON object in to a LeftoverRecord
// class object.
LeftoverRecord DeserializeLeftoverRecordFromJSON(
    const rapidjson::Value& json_obj) {
  LeftoverRecord record;

  if (json_obj.HasMember("date_") && json_obj["date_"].IsString()) {
    record.SetDate(json_obj["date_"].GetString());
  }

  if (json_obj.HasMember("meal_") && json_obj["meal_"].IsString()) {
    record.SetMeal(json_obj["meal_"].GetString());
  }

  if (json_obj.HasMember("food_name_") && json_obj["food_name_"].IsString()) {
    record.SetFoodName(json_obj["food_name_"].GetString());
  }

  if (json_obj.HasMember("qty_in_oz_") && json_obj["qty_in_oz_"].IsDouble()) {
    record.SetQuantityInOz(json_obj["qty_in_oz_"].GetDouble());
  }

  if (json_obj.HasMember("leftover_reason_") &&
      json_obj["leftover_reason_"].IsString()) {
    record.SetLeftoverReason(json_obj["leftover_reason_"].GetString());
  }

  if (json_obj.HasMember("disposal_mechanism_") &&
      json_obj["disposal_mechanism_"].IsString()) {
    record.SetDisposalMechanism(json_obj["disposal_mechanism_"].GetString());
  }

  if (json_obj.HasMember("cost_") && json_obj["cost_"].IsDouble()) {
    record.SetCost(json_obj["cost_"].GetDouble());
  }
  return record;
}

// Util function to convert a LeftoverRecord class object into a Crow JSON
// object.
crow::json::wvalue LeftoverRecordToCrowJSON(const LeftoverRecord& record) {
  crow::json::wvalue record_json({});

  record_json["date"] = record.GetDate();

  record_json["meal"] = record.GetMeal();

  record_json["food_name"] = record.GetFoodName();

  record_json["qty_in_oz"] = record.GetQuantityInOz();

  record_json["leftover_reason"] = record.GetLeftoverReason();

  record_json["disposal_mechanism"] = record.GetDisposalMechanism();

  record_json["cost"] = record.GetCost();

  return record_json;
}

// Util function to convert a LeftoverReport object into a serialized
// JSON object.
crow::json::wvalue LeftoverReportToCrowJSON(const LeftoverReport& report) {
  crow::json::wvalue report_json({});

  std::vector<std::string> most_common_disposal_mechanisms{};
  most_common_disposal_mechanisms = report.MostCommonDisposalMechanisms();

  std::vector<std::string> most_common_leftovers{};
  most_common_leftovers = report.MostCommonLeftover();

  std::vector<std::string> most_common_leftover_reasons{};
  most_common_leftover_reasons = report.MostCommonLeftoverReasons();

  std::vector<std::string> most_costly_leftover_producing_meals{};
  most_costly_leftover_producing_meals =
      report.MostCostlyLeftoverProducingMeals();

  std::vector<std::string> suggested_strategies_to_reduce_leftovers{};
  suggested_strategies_to_reduce_leftovers =
      report.SuggestedLeftoverReductionStrategies();

  double total_cost_over_leftover_{};
  total_cost_over_leftover_ = report.TotalCostOfLeftovers();

  return report_json;
}

// Util function to convert a crow query string into a LeftoverRecord class
// object.
LeftoverRecord QueryStringToLeftoverRecord(
    const crow::query_string& query_string) {
  LeftoverRecord record{};

  // TODO 1. Get the date from the query_string using query_string.get("date"),
  // and set it in the `record` object using the setter in LeftoverRecord
  // class.
  record.SetDate(query_string.get("date"));
  // TODO 2. Get the meal from the query_string using query_string.get("meal"),
  // and set it in the `record` object using the setter in LeftoverRecord
  // class.
  record.SetMeal(query_string.get("meal"));
  // TODO 3. Get the food name from the query_string using
  // query_string.get("food_name"), and set it in the `record` object using the
  // setter in LeftoverRecord class.
  record.SetFoodName(query_string.get("food_name"));
  // TODO 4. Get the quantity from the query_string using
  // std::stod(query_string.get("qty_in_oz")), and set it in the `record` object
  // using the setter in LeftoverRecord class.
  record.SetQuantityInOz(std::stod(query_string.get("qty_in_oz")));
  // TODO 5. Get the leftover reason from the query_string using
  // query_string.get("leftover_reason"), and set it in the `record` object
  // using the setter in LeftoverRecord class.
  record.SetLeftoverReason(query_string.get("leftover_reason"));
  // TODO 6. Get the disposal mechanism from the query_string using
  // query_string.get("disposal_mechanism"), and set it in the `record` object
  // using the setter in LeftoverRecord class.
  record.SetDisposalMechanism(query_string.get("disposal_mechanism"));
  // TODO 7. Get the cost from the query_string using
  // std::stod(query_string.get("cost")), and set it in the `record` object
  // using the setter in LeftoverRecord class.
  record.SetCost(std::stod(query_string.get("cost")));
  return record;
}

bool LeftoverTrackerBackend::LoadRecordsFromJSONFile() {
  std::ifstream records_file{leftover_records_filepath_};
  if (!records_file.is_open()) {
    std::filesystem::remove(leftover_records_filepath_);
    return false;
  }

  std::stringstream buffer;
  buffer << records_file.rdbuf();

  std::string s = buffer.str();
  rapidjson::Document doc;

  if (s.empty()) {
    std::filesystem::remove(leftover_records_filepath_);
    return false;
  }

  std::string validJson(s);

  bool doc_parse_error = doc.Parse(validJson.c_str()).HasParseError();
  if (doc_parse_error || !doc.IsArray()) {
    std::filesystem::remove(leftover_records_filepath_);
    return false;
  }

  for (rapidjson::Value::ConstValueIterator itr = doc.Begin(); itr != doc.End();
       ++itr) {
    LeftoverRecord record = DeserializeLeftoverRecordFromJSON(*itr);
    // TODO: Call the member function in the LeftoverTracker class to add a
    // `record
    tracker_.AddRecord(record);
  }

  records_file.close();
  return true;
}

bool LeftoverTrackerBackend::WriteRecordsToJSONFile() const {
  std::ofstream records_file{leftover_records_filepath_};
  // Get the records from leftover_tracker_, convert to JSON and
  // save to file.
  rapidjson::StringBuffer ss;
  rapidjson::Writer<rapidjson::StringBuffer> writer(ss);
  writer.StartArray();

  const std::vector<LeftoverRecord>& records = tracker_.GetRecords();
  // TODO: Call the member function in the LeftoverTracker class, on the
  // member object that you added in leftover_tracker.h, that returns all
  // the LeftoverRecord objects. Store the returned records in the vector
  // declared above. Also change the data type of the records vector to `const
  // std::vector<LettoverRecord>&`.

  for (LeftoverRecord record : records) {
    SerializeLeftoverRecordToJSON(record, &writer);
  }
  writer.EndArray();

  records_file << ss.GetString();

  records_file.flush();
  records_file.close();
  return true;
}

crow::json::wvalue LeftoverTrackerBackend::AddRecord(
    const crow::query_string& query_string) {
  LeftoverRecord record = QueryStringToLeftoverRecord(query_string);
  crow::json::wvalue status;

  bool add_result = tracker_.AddRecord(record);
  // TODO: Call the member function in the LeftoverTracker class, on the
  // member object that you added in leftover_tracker.h, that adds a
  // `record` and returns the status of the add operation as a bool. Store the
  // returned value in the bool declared above.
  status["success"] = add_result;

  return status;
}

crow::json::wvalue LeftoverTrackerBackend::DeleteRecord(
    const crow::query_string& query_string) {
  LeftoverRecord record = QueryStringToLeftoverRecord(query_string);
  bool delete_result = tracker_.DeleteRecord(record);
  // TODO: Call the member function in the LefrtoverTracker class, on the
  // member object that you added in leftovere_tracker.h, that deletes
  // `record` and returns the status of the delete operation as a bool. Store
  // the returned value in the bool declared above.
  crow::json::wvalue status;
  status["success"] = delete_result;

  return status;
}

crow::json::wvalue LeftoverTrackerBackend::GetRecords() const {
  const std::vector<LeftoverRecord> records = tracker_.GetRecords();
  // TODO: Call the member function in the LeftoverTracker class, on the
  // member object that you added in leftover_tracker.h, that returns all
  // the LeftoverRecord objects. Store the returned records in the vector
  // declared above. Also change the data type of the records vector to `const
  // std::vector<LeftoverRecord>&`.

  crow::json::wvalue records_json({});
  records_json["num_records"] = records.size();

  std::vector<crow::json::wvalue> record_json_vector;
  for (const LeftoverRecord& record : records) {
    record_json_vector.push_back(LeftoverRecordToCrowJSON(record));
  }
  records_json["record_array"] = std::move(record_json_vector);
  return records_json;
}

crow::json::wvalue LeftoverTrackerBackend::GetLeftoverReport() const {
  crow::json::wvalue report;
  const auto& records = tracker_.GetRecords();

  LeftoverReport leftoverReport(records);

  report["most_common_leftover_"] = leftoverReport.MostCommonLeftover();
  report["most_leftover_producing_meal_"] =
      leftoverReport.MostCostlyLeftoverProducingMeals();
  report["most_common_disposal_mechanism_"] =
      leftoverReport.MostCommonDisposalMechanisms();
  report["total_cost_of_leftover_"] = leftoverReport.TotalCostOfLeftovers();
  report["most_common_leftover_reason_"] =
      leftoverReport.MostCommonLeftoverReasons();
  report["suggested_strategies_to_reduce_leftover_"] =
      leftoverReport.SuggestedLeftoverReductionStrategies();

  return report;
}

std::vector<std::string> LeftoverReport::MostCommonLeftover() const {
  std::unordered_map<std::string, int> itemCounts;

  int maxCount = 0;
  for (const auto& record : leftover_records_) {
    itemCounts[record.GetFoodName()]++;
    maxCount = std::max(maxCount, itemCounts[record.GetFoodName()]);
  }

  std::vector<std::string> mostCommonItems;
  for (const auto& pair : itemCounts) {
    if (pair.second == maxCount) {
      mostCommonItems.push_back(pair.first);
    }
  }

  return mostCommonItems;
}

std::vector<std::string> LeftoverReport::MostCostlyLeftoverProducingMeals()
    const {
  std::unordered_map<std::string, double> mealCosts;

  for (const LeftoverRecord& record : leftover_records_) {
    mealCosts[record.meal()] += record.GetCost();
  }

  double maxCost = 0.0;
  for (const auto& pair : mealCosts) {
    maxCost = std::max(maxCost, pair.second);
  }

  std::vector<std::string> mostCostlyMeals;
  for (const auto& pair : mealCosts) {
    if (pair.second == maxCost) {
      mostCostlyMeals.push_back(pair.first);
    }
  }

  return mostCostlyMeals;
}

std::vector<std::string> LeftoverReport::MostCommonLeftoverReasons() const {
  std::unordered_map<std::string, int> reasonCounts;

  for (const LeftoverRecord& record : leftover_records_) {
    reasonCounts[record.GetLeftoverReason()]++;
  }

  int maxCount = 0;
  std::string mostCommonReason;
  for (const auto& pair : reasonCounts) {
    if (pair.second > maxCount) {
      maxCount = pair.second;
      mostCommonReason = pair.first;
    }
  }

  std::vector<std::string> mostCommonReasons;
  for (const auto& pair : reasonCounts) {
    if (pair.second == maxCount) {
      mostCommonReasons.push_back(pair.first);
    }
  }

  return mostCommonReasons;
}

std::vector<std::string> LeftoverReport::MostCommonDisposalMechanisms() const {
  std::unordered_map<std::string, int> disposalCounts;

  int maxCount = 0;
  for (const LeftoverRecord& record : leftover_records_) {
    disposalCounts[record.GetDisposalMechanism()]++;
    maxCount =
        std::max(maxCount, disposalCounts[record.GetDisposalMechanism()]);
  }

  std::vector<std::string> mostCommonDisposalMechanisms;
  for (const auto& pair : disposalCounts) {
    if (pair.second == maxCount) {
      mostCommonDisposalMechanisms.push_back(pair.first);
    }
  }

  return mostCommonDisposalMechanisms;
}

double LeftoverReport::TotalCostOfLeftovers() const {
  double total_cost = 0.0;
  for (const LeftoverRecord& record : leftover_records_) {
    total_cost += record.GetCost();
  }
  return total_cost;
}

std::vector<std::string> LeftoverReport::SuggestedLeftoverReductionStrategies()
    const {
  std::vector<std::string> suggestedStrategies;
  const std::vector<std::string>& CommonReasons = MostCommonLeftoverReasons();
  std::unordered_map<std::string, int> strategies;
  if (std::find(CommonReasons.begin(), CommonReasons.end(), "Expired") !=
      CommonReasons.end()) {
    suggestedStrategies.push_back("Donate before expiration");
  }
  if (std::find(CommonReasons.begin(), CommonReasons.end(),
                "Too much left overs") != CommonReasons.end()) {
    suggestedStrategies.push_back("Cook small servings");
  }
  if (CommonReasons.size() > 0 &&
      std::find(CommonReasons.begin(), CommonReasons.end(), "Expired") ==
          CommonReasons.end()) {
    suggestedStrategies.push_back("Recycle left overs");
  }
  if (std::find(CommonReasons.begin(), CommonReasons.end(),
                "Too much left overs") != CommonReasons.end() ||
      std::find(CommonReasons.begin(), CommonReasons.end(), "Tastes bad") !=
          CommonReasons.end()) {
    suggestedStrategies.push_back("Buy less food");
  }
  return suggestedStrategies;
}
