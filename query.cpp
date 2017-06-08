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
        if (selectExpr->aggerationFunc != "count") {
          assert(0 && "only count aggregation function is supported at the moment");
        }

        if (selectExpr->field != "*") {
          assert(0 && "only '*' is supported for count()");
        }

        cout << "[" << aggrGroup->bitmap->cardinality() << "] ";
        row->values.push_back(new GenericValueContainer(aggrGroup->bitmap->cardinality()));
      } else {
        cout << "[" << aggrGroup->valueMap[selectExpr->field] << "] ";
        row->values.push_back(new GenericValueContainer(aggrGroup->valueMap[selectExpr->field]));
      }
    }

    cout << endl;

    result.rows.push_back(row);
  }
}

void Query::printResultRows() {
  const auto selectExprCount = selectExprs.size();

  for (auto i = 0UL, len = selectExprs.size(); i < len; i++) {
    const auto selectExpr = selectExprs[i];

    if (!selectExpr->display.empty()) {
      cout << selectExpr->display;
    } else if (!selectExpr->isAggerationSelect) {
      cout << selectExpr->field;
    } else {
      cout << selectExpr->aggerationFunc << "(" << selectExpr->field << ")";
    }

    if (i < len - 1) {
      cout << ", ";
    }
  }

  cout << endl;

  for (auto &&row : result.rows) {
    for (auto i = 0; i < selectExprCount; i++) {
      const auto value = row->values[i];

      if (value->type == FIELD_TYPE_STRING) {
        cout << value->getStrVal();
      } else if (value->type == FIELD_TYPE_INT) {
        cout << value->getIVal();
      } else if (value->type == FIELD_TYPE_BIGINT) {
        cout << value->getUInt64Val();
      } else {
        assert(0 && "unspported value type in result rows");
      }

      if (i < selectExprCount - 1) {
        cout << ", ";
      }
    }

    cout << endl;
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
  SelectExpr *selectExprCount = new SelectExpr("*", "count");
  query->filterExprs.push_back(endpointMustBeHome);
  query->groupByExprs.push_back(groupByExprEndpoint);
  query->groupByExprs.push_back(groupByExprGender);
  query->selectExprs.push_back(selectExprEndpoint);
  query->selectExprs.push_back(selectExprGender);
  query->selectExprs.push_back(selectExprCount);
  // apply filters
  query->applyFilters();
  cout << "after filter, bitmap: ";
  query->initialBitmap->printf();
  cout << endl;
  // apply groups
  query->genAggrGroups();
  // generate rows
  query->genResultRows();
  // print result rows
  query->printResultRows();
}
