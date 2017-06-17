#include <string>
#include <iostream>
#include <map>
#include <vector>
#include <cassert>
#include "roaring.hh"
#include "../table.h"
#include "../query.h"

using namespace std;

void insertRow(Table *table, int64_t timestamp, string referrer, string endpoint, string gender, int responseTime) {
  table->fields["timestamp"]->addValue(GenericValueContainer(timestamp));
  table->fields["endpoint"]->addValue(GenericValueContainer(endpoint));
  table->fields["referrer"]->addValue(GenericValueContainer(referrer));
  table->fields["gender"]->addValue(GenericValueContainer(gender));
  table->fields["responseTime"]->addValue(GenericValueContainer(responseTime));
  table->incrementRecordCount();
}

void insertRows(Table *table) {
  for (int i = 0; i < 0; ++i) {
    insertRow(table, i + 1, "google", "/home", "male", rand());
  }
  insertRow(table, 1, "google", "/home", "male", rand());
  insertRow(table, 2, "facebook", "/home", "female", rand());
  insertRow(table, 3, "twitter", "/api", "female", rand());
  insertRow(table, 4, "-none-", "/api", "male", rand());
  insertRow(table, 5, "google", "/home", "male", rand());
  insertRow(table, 6, "bing", "/home", "male", rand());
  insertRow(table, 7, "google", "/home", "female", rand());
//  insertRow(table, 2, "/home", "female", rand());
//  insertRow(table, 2, "/home", "female", rand());
}

void runQuery(Table *table) {
  Query *query = new Query(table);
//  FilterExpr *endpointMustBeHome = new FilterExpr("endpoint", "=", "/home");
  GroupByExpr *groupByExprEndpoint = new GroupByExpr("endpoint");
//  GroupByExpr *groupByExprGender = new GroupByExpr("gender");
  GroupByExpr *groupByExprTimestamp3Secs = new GroupByExpr("dateSecondsGroup(timestamp, 3)");
  SelectExpr *selectExprEndpoint = new SelectExpr("endpoint");
//  SelectExpr *selectExprGender = new SelectExpr("gender");
  SelectExpr *selectExprCount = new SelectExpr("*", "count", "count");
//  SelectExpr *selectExprMinResponseTime = new SelectExpr("responseTime", "min", "min(responseTime)");
//  SelectExpr *selectExprMaxResponseTime = new SelectExpr("responseTime", "max", "max(responseTime)");
//  SelectExpr *selectExprSumResponseTime = new SelectExpr("responseTime", "sum", "sum(responseTime)");
  SelectExpr *selectExprAvgResponseTime = new SelectExpr("responseTime", "avg", "avg(responseTime)");
  SelectExpr *selectExprBy3Secs = new SelectExpr("timestamp", "dateSecondsGroup", "dateSecondsGroup(timestamp, 3)");
  OrderByExpr *orderByExprTimestamp = new OrderByExpr("dateSecondsGroup(timestamp, 3)");
  OrderByExpr *orderByExprCount = new OrderByExpr("count");
  selectExprBy3Secs->aggerationFuncArgs.push_back("3");
  query->isAggregationQuery = true;
//  query->filterExprs.push_back(endpointMustBeHome);
  query->groupByExprs.push_back(groupByExprTimestamp3Secs);
  query->groupByExprs.push_back(groupByExprEndpoint);
//  query->groupByExprs.push_back(groupByExprGender);
  query->selectExprs.push_back(selectExprBy3Secs);
  query->selectExprs.push_back(selectExprEndpoint);
//  query->selectExprs.push_back(selectExprGender);
  query->selectExprs.push_back(selectExprCount);
//  query->selectExprs.push_back(selectExprMinResponseTime);
//  query->selectExprs.push_back(selectExprMaxResponseTime);
//  query->selectExprs.push_back(selectExprSumResponseTime);
  query->selectExprs.push_back(selectExprAvgResponseTime);
  query->orderByExprs.push_back(orderByExprTimestamp);
  query->orderByExprs.push_back(orderByExprCount);

  // run query
  query->run();

  // print result rows
  query->printResultRows();
}

int main() {
  std::cout << "Hello, World!" << std::endl;

  const auto table = new Table();
  const auto fieldTimestamp = new Field("timestamp", FIELD_TYPE_TIMESTAMP);
  const auto fieldEndpoint = new Field("endpoint", FIELD_TYPE_STRING);
  const auto fieldGender = new Field("gender", FIELD_TYPE_STRING);
  const auto fieldReferrer = new Field("referrer", FIELD_TYPE_STRING);
  const auto fieldResponseTime = new Field("responseTime", FIELD_TYPE_INT);

  fieldEndpoint->setEncoding(FIELD_ENCODING_DICT);
  fieldGender->setEncoding(FIELD_ENCODING_DICT);
  fieldReferrer->setEncoding(FIELD_ENCODING_DICT);

  table->setField(fieldTimestamp);
  table->setField(fieldEndpoint);
  table->setField(fieldGender);
  table->setField(fieldReferrer);
  table->setField(fieldResponseTime);

  insertRows(table);

  runQuery(table);

  delete fieldTimestamp;
  delete fieldEndpoint;
  delete fieldResponseTime;
  delete table;

  return 0;
}
