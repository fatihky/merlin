#include <string>
#include <vector>
#include "roaring.hh"
#include "table.h"
#include "generic-value.h"
#include "utils.h"

#ifndef MERLIN_QUERY_H
#define MERLIN_QUERY_H

using namespace std;

class SelectExpr {
  public:
  string field;
  string display;
  string aggerationFunc;
  vector<string> aggerationFuncArgs;
  bool isAggerationSelect;
  // this is for dateSecondsGroup function currently
  map<string, Roaring *> groups;

  SelectExpr(string field_): field(field_), isAggerationSelect(false) {}
  SelectExpr(string field_, string aggerationFunc_): field(field_), aggerationFunc(aggerationFunc_), isAggerationSelect(true) {}
  SelectExpr(string field_, string aggerationFunc_, string display_): field(field_), aggerationFunc(aggerationFunc_), display(display_), isAggerationSelect(true) {}
};

class FilterExpr {
  public:
  string field;
  string op; // operator
  string val;
  FilterExpr(string field_, string op_, string val_): field(field_), op(op_), val(val_) {}
};

class GroupByExpr {
  public:
  string field;
  GroupByExpr(string field_): field(field_) {}
};

class OrderByExpr {
  public:
  string field;
  bool asc;

  OrderByExpr(string field_, bool asc_ = false): field(field_), asc(asc_) {}
};

class AggregationGroup {
  public:
  vector<string> keys;
  Roaring *bitmap;
  map<string, string> valueMap; // field > value

  AggregationGroup(AggregationGroup *group) {
    keys = group->keys;
    valueMap = group->valueMap;
    bitmap = new Roaring(*group->bitmap);
  }

  AggregationGroup(string field, string initialKey, Roaring *initialBitmap) {
    keys.push_back(initialKey);
    valueMap[field] = initialKey;
    bitmap = new Roaring(*initialBitmap);
  }

  AggregationGroup *clone(string field, string key, Roaring *bitmap) {
    auto clone = new AggregationGroup(this);
    clone->keys.push_back(key);
    clone->valueMap[field] = key;
    *clone->bitmap &= *bitmap;
    return clone;
  }

  ~AggregationGroup() {
    delete bitmap;
  }
};

class QueryResultRow {
  public:
  vector<GenericValueContainer *> values;

  ~QueryResultRow() {
    for (auto &&val : values) {
      delete val;
    }
  }
};

class QueryResult {
  public:
  vector<QueryResultRow *> rows;

  ~QueryResult() {
    for (auto &&row : rows) {
      delete row;
    }
  }
};

class Query {
  public:
  vector<SelectExpr *> selectExprs;
  vector<FilterExpr *> filterExprs;
  vector<GroupByExpr *> groupByExprs;
  vector<OrderByExpr *> orderByExprs;
  vector<AggregationGroup *> aggregationGroups;
  bool isAggregationQuery;
  QueryResult result;
  Roaring *initialBitmap;
  Table *table;
  bool debug;

  struct {
    int64_t filter_us;
    int64_t filter_ms;

    int64_t group_us;
    int64_t group_ms;

    int64_t order_us;
    int64_t order_ms;
  } stats;

  Query(Table *table_, bool debug_ = false) {
    table = table_;
    initialBitmap = nullptr;
    debug = debug_;
  }

  ~Query() {
    for (auto &&select : selectExprs) {
      delete select;
    }

    for (auto &&filterExpr : filterExprs) {
      delete filterExpr;
    }

    for (auto &&groupByExpr : groupByExprs) {
      delete groupByExpr;
    }

    for (auto &&orderByExpr : orderByExprs) {
      delete orderByExpr;
    }

    for (auto &&aggregationGroup : aggregationGroups) {
      delete aggregationGroup;
    }

    if (initialBitmap != nullptr) {
      delete initialBitmap;
    }
  }

  void applyFilters();
  void genAggrGroups();
  void genResultRows();
  void applyOrder();
  void printResultRows();
  void run();

  private:
  int findSelectFieldIndex(string field);
  SelectExpr *findSelectExprByDisplayValue(string displayValue);
};

void runQuery(Table *table);

#endif //MERLIN_QUERY_H
