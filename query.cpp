#include <iostream>
#include "query.h"
#include "utils.h"

using namespace std;

void Query::applyFilters() {
  auto *result = initialBitmap;

  for (auto &&filter : filterExprs) {
    cout << filter->field << " " << filter->op << " " << filter->val << endl;
    auto bitmap = table->fields[filter->field]->getBitmap(filter->op, filter->val);
    cout << "field's bitmap: ";
    bitmap->printf();
    cout << endl;
    (*result) &= *bitmap;
    cout << "current bitmap: ";
    result->printf();
    cout << endl;
  }

  filterResult = result;
}

void Query::genAggrGroups() {
  vector<AggregationGroup *> result;
  for (auto &&groupByExpr : groupByExprs) {
    const auto field = table->fields[groupByExpr->field];
    // const auto keys = field->keys();
    if (result.size() == 0) {
      vector<AggregationGroup *> aggrGroupsField;
      const auto groups = field->genGroups(initialBitmap);
      for (auto &&group : groups) {
        const auto aggregationGroup = new AggregationGroup(field->name, group.first, group.second);
        aggrGroupsField.push_back(aggregationGroup);
        cout << "created group for... " << group.first << " bitmap: ";
        aggregationGroup->bitmap->printf();
        cout << endl;
      }
      result = aggrGroupsField;
      continue;
    }

    vector<AggregationGroup *> aggrGroupsField;
    for (auto &&groupByGroup : result) {
      cout << "generate new groups for key ";
      dumpStrVector(groupByGroup->keys);
      cout << endl;

      for (unsigned long i = 0, size = groupByGroup->keys.size(); i < size; ++i) {
        const auto currentBitmap = groupByGroup->bitmap;
        const auto groups = field->genGroups(currentBitmap);
        for (auto &&group : groups) {
          const auto aggregationGroup = groupByGroup->clone(field->name, group.first, group.second);
          aggrGroupsField.push_back(aggregationGroup);
          cout << "generated group for... " << group.first << endl;
          cout << "generated keys: ";
          dumpStrVector(aggregationGroup->keys);
          cout << " | bitmap: ";
          aggregationGroup->bitmap->printf();
          cout << endl;
        }
      }
    }

    result = aggrGroupsField;
  }

  aggregationGroups = result;
}

void Query::genResultRows() {
  if (!isAggregationQuery) {
    assert(0 && "non aggregation queries are not supported at the moment");
  }

  for (auto &&aggrGroup : aggregationGroups) {
    auto row = new QueryResultRow();

    for (auto &&selectExpr : selectExprs) {
      if (selectExpr->isAggerationSelect) {
        assert(0 && "aggregation selects not supported yet");
      }

      cout << "[" << aggrGroup->valueMap[selectExpr->field] << "] ";
      row->values.push_back(new GenericValueContainer(aggrGroup->valueMap[selectExpr->field]));
    }

    cout << endl;

    result.rows.push_back(row);
  }
}

void runQuery(Table *table) {
  auto roar = new Roaring(roaring_bitmap_from_range(0, 1000, 1));
  Query *query = new Query(table, roar);
  FilterExpr *endpointMustBeHome = new FilterExpr("endpoint", "=", "/home");
  GroupByExpr *groupByExprEndpoint = new GroupByExpr("endpoint");
  GroupByExpr *groupByExprGender = new GroupByExpr("gender");
  SelectExpr *selectExprEndpoint = new SelectExpr("endpoint");
  SelectExpr *selectExprGender = new SelectExpr("gender");
  query->filterExprs.push_back(endpointMustBeHome);
  query->groupByExprs.push_back(groupByExprEndpoint);
  query->groupByExprs.push_back(groupByExprGender);
  query->selectExprs.push_back(selectExprEndpoint);
  query->selectExprs.push_back(selectExprGender);
  // apply filters
  query->applyFilters();
  cout << "after filter, bitmap: ";
  query->initialBitmap->printf();
  cout << endl;
  // apply groups
  query->genAggrGroups();
  // generate rows
  query->genResultRows();
}
