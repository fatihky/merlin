#include <string>
#include <vector>
#include "deps/CRoaring/cpp/roaring.hh"
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
  bool isAggerationSelect;
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

class AggregationGroup {
  public:
  vector<string> keys;
  Roaring *bitmap;

  AggregationGroup(AggregationGroup *group) {
    keys = group->keys;
    bitmap = new Roaring(*group->bitmap);
  }

  AggregationGroup(string initialKey, Roaring *initialBitmap) {
    keys.push_back(initialKey);
    bitmap = new Roaring(*initialBitmap);
  }

  AggregationGroup *clone(string key, Roaring *bitmap) {
    auto clone = new AggregationGroup(this);
    clone->keys.push_back(key);
    *clone->bitmap &= *bitmap;
    return clone;
  }
};

class QueryResultRow {
  public:
  vector<GenericValueContainer *> values;
};

class QueryResult {
  public:
  vector<QueryResultRow *> rows;
};

class Query {
  public:
  vector<SelectExpr *> selectExprs;
  vector<FilterExpr *> filterExprs;
  vector<GroupByExpr *> groupByExprs;
  vector<AggregationGroup *> aggregationGroups;
  bool isAggregationQuery;
  QueryResult result;
  Roaring *initialBitmap;
  Roaring *filterResult;
  Table *table;

  Query(Table *table_, Roaring *roar) {
    table = table_;
    initialBitmap = roar;
  }

  void applyFilters();
  void genAggrGroups();
};

void runQuery(Table *table);

#endif //MERLIN_QUERY_H
