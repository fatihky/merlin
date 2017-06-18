#include <string>
#include <vector>
#include "roaring/roaring.h"
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
  map<string, roaring_bitmap_t *> groups;

  SelectExpr(string field_): field(field_), isAggerationSelect(false) {}
  SelectExpr(string field_, string aggerationFunc_): field(field_), aggerationFunc(aggerationFunc_), isAggerationSelect(true) {}
  SelectExpr(string field_, string aggerationFunc_, string display_): field(field_), aggerationFunc(aggerationFunc_), display(display_), isAggerationSelect(true) {}
  ~SelectExpr() {
    for (auto &&it : groups) {
      delete it.second;
    }
  }
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
  roaring_bitmap_t *bitmap;
  map<string, string> valueMap; // field > value

  AggregationGroup(string field, string initialKey, roaring_bitmap_t *initialBitmap) {
    keys.push_back(initialKey);
    valueMap[field] = initialKey;
    bitmap = roaring_bitmap_copy(initialBitmap);
  }

  AggregationGroup(AggregationGroup *group, bool copyBitmap = true) {
    keys = group->keys;
    valueMap = group->valueMap;
    if (copyBitmap) {
      bitmap = roaring_bitmap_copy(group->bitmap);
    } else {
      bitmap = nullptr;
    }
  }

  AggregationGroup *clone(string field, string key, roaring_bitmap_t *bitmap) {
    auto clone = new AggregationGroup(this, false);
    clone->keys.push_back(key);
    clone->valueMap[field] = key;
    clone->bitmap = roaring_bitmap_and(this->bitmap, bitmap);
    return clone;
  }

  ~AggregationGroup() {
    roaring_bitmap_free(bitmap);
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
  int limit;
  bool isAggregationQuery;
  QueryResult result;
  roaring_bitmap_t *initialBitmap;
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
    limit = -1;
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
  void applyLimit();
  void printResultRows();
  void run();

  private:
  int findSelectFieldIndex(string field);
  SelectExpr *findSelectExprByDisplayValue(string displayValue);
};

void runQuery(Table *table);

#endif //MERLIN_QUERY_H
