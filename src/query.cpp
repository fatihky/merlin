#include <iostream>
#include <chrono>
#include "query.h"
#include "utils.h"

using namespace std;

void Query::applyFilters() {
  auto result = initialBitmap;

  for (auto &&filter : filterExprs) {
    if (debug) {
      cout << filter->field << " " << filter->op << " " << filter->val << endl;
    }
    auto bitmap = table->fields[filter->field]->getBitmap(filter->op, filter->val);
    if (bitmap == nullptr) {
      // this means no results found for this bitmap.
      // while we always doing AND on result bitmaps,
      // we could get empty bitmap
      // so I am resetting result bitmap and stopping loop
      roaring_bitmap_clear(&result->roaring);
      break;
    }

    if (debug) {
      cout << "field's bitmap's cardinality: " << bitmap->cardinality() << endl;
    }

    (*result) &= *bitmap;

    if (debug) {
      cout << "current bitmap's cardinality: " << result->cardinality() << endl;
    }
  }
}

SelectExpr *Query::findSelectExprByDisplayValue(string displayValue) {
  for (auto &&selectExpr : selectExprs) {
    if (selectExpr->display == displayValue) {
      return selectExpr;
    }
  }

  return nullptr;
}

static void clearAggrGroups(vector<AggregationGroup *> groups) {
  for (auto group : groups) {
    delete group;
  }
}

void Query::genAggrGroups() {
  vector<AggregationGroup *> result;
  for (auto &&groupByExpr : groupByExprs) {
    const auto isAggerationGroupBy = table->fields.count(groupByExpr->field) == 0;
    const auto aggrSelectExpr = findSelectExprByDisplayValue(groupByExpr->field);
    const auto field = isAggerationGroupBy
                       ? table->fields[aggrSelectExpr->field]
                       : table->fields[groupByExpr->field];

    // const auto keys = field->keys();
    if (result.size() == 0) {
      vector<AggregationGroup *> aggrGroupsField;
      const auto groups = isAggerationGroupBy
                          ? field->genGroups(initialBitmap, aggrSelectExpr->aggerationFunc, aggrSelectExpr->aggerationFuncArgs)
                          : field->genGroups(initialBitmap);
      for (auto &&group : groups) {
        const auto aggregationGroup = new AggregationGroup(field->name, group.first, group.second);
        aggrGroupsField.push_back(aggregationGroup);
        if (debug) {
          cout << "created group for... " << group.first << " bitmap size: " << aggregationGroup->bitmap->cardinality()
               << endl;
        }
      }
      result = aggrGroupsField;

      if (isAggerationGroupBy) {
        aggrSelectExpr->groups = std::move(groups);
      }
      continue;
    }

    vector<AggregationGroup *> aggrGroupsField;
    for (auto &&groupByGroup : result) {
      if (debug) {
        cout << endl << "generate new groups for key ";
        dumpStrVector(groupByGroup->keys);
        cout << endl;
      }

      for (unsigned long i = 0, size = groupByGroup->keys.size(); i < size; ++i) {
        const auto currentBitmap = groupByGroup->bitmap;
        const auto groups = isAggerationGroupBy
                            ? field->genGroups(currentBitmap, aggrSelectExpr->aggerationFunc, aggrSelectExpr->aggerationFuncArgs)
                            : field->genGroups(currentBitmap);
        for (auto &&group : groups) {
          const auto aggregationGroup = groupByGroup->clone(field->name, group.first, group.second);
          aggrGroupsField.push_back(aggregationGroup);

          if (debug) {
            cout << "generated group for... " << group.first << endl;
            cout << "generated keys: ";
            dumpStrVector(aggregationGroup->keys);
            cout << " | bitmap size: ";
            aggregationGroup->bitmap->cardinality();
            cout << endl;
          }
        }

        if (isAggerationGroupBy) {
          aggrSelectExpr->groups = std::move(groups);
        } else {
          for (auto &&it : groups) {
            delete it.second;
          }
        }
      }
    }

    clearAggrGroups(result);
    result = aggrGroupsField;
  }

  aggregationGroups = result;
}

void Query::genResultRows() {
  if (!isAggregationQuery) {
    throw std::runtime_error("non aggregation queries are not supported at the moment");
  }

  for (auto &&aggrGroup : aggregationGroups) {
    auto row = new QueryResultRow();

    for (auto &&selectExpr : selectExprs) {
      if (selectExpr->isAggerationSelect) {
        if (selectExpr->aggerationFunc == "count") {
          if (selectExpr->field != "*") {
            throw std::runtime_error("only '*' is supported for count()");
          }

          if (debug) {
            cout << "[" << aggrGroup->bitmap->cardinality() << "] ";
          }
          row->values.push_back(new GenericValueContainer(aggrGroup->bitmap->cardinality()));
        } else if (selectExpr->aggerationFunc == "min") {
          const auto field = table->fields[selectExpr->field];
          const auto min = field->aggrFuncMin(aggrGroup->bitmap);
          if (debug) {
            cout << "[" << min << "] ";
          }
          row->values.push_back(new GenericValueContainer(min));
        } else if (selectExpr->aggerationFunc == "max") {
          const auto field = table->fields[selectExpr->field];
          const auto max = field->aggrFuncMax(aggrGroup->bitmap);
          if (debug) {
            cout << "[" << max << "] ";
          }
          row->values.push_back(new GenericValueContainer(max));
        } else if (selectExpr->aggerationFunc == "sum") {
          const auto field = table->fields[selectExpr->field];
          const auto sum = field->aggrFuncSum(aggrGroup->bitmap);
          if (debug) {
            cout << "[" << sum << "] ";
          }
          row->values.push_back(new GenericValueContainer(sum));
        } else if (selectExpr->aggerationFunc == "avg" || selectExpr->aggerationFunc == "mean") {
          const auto field = table->fields[selectExpr->field];
          const auto sum = field->aggrFuncSum(aggrGroup->bitmap);
          const auto avg = sum / aggrGroup->bitmap->cardinality();
          if (debug) {
            cout << "[" << avg << "] ";
          }
          row->values.push_back(new GenericValueContainer(avg));
        } else if (selectExpr->aggerationFunc == "dateSecondsGroup") {
          if (debug) {
            cout << "[" << aggrGroup->valueMap[selectExpr->field] << "] ";
          }
          row->values.push_back(new GenericValueContainer(aggrGroup->valueMap[selectExpr->field]));
        } else {
          throw std::runtime_error("unknown aggregation function: " + selectExpr->aggerationFunc + " -- " + (selectExpr->aggerationFunc == "count" ? "true" : "false"));
        }
      } else {
        if (debug) {
          cout << "[" << aggrGroup->valueMap[selectExpr->field] << "] ";
        }
        row->values.push_back(new GenericValueContainer(aggrGroup->valueMap[selectExpr->field]));
      }
    }

    if (debug) {
      cout << endl;
    }

    result.rows.push_back(row);
  }
}

int Query::findSelectFieldIndex(string field) {
  int index = 0;

  for (auto &&selectExpr : selectExprs) {
    if (field == selectExpr->field) {
      return index;
    }

    if (field == selectExpr->display) {
      return index;
    }

    index++;
  }

  return -1;
}

void Query::applyOrder() {
  if (orderByExprs.size() == 0) {
    return;
  }
  const auto query = this;

  std::sort(result.rows.begin(), result.rows.end(), [&](QueryResultRow *row1, QueryResultRow *row2) {
    for (auto &&orderByExpr : query->orderByExprs) {
      const auto fieldIndex = query->findSelectFieldIndex(orderByExpr->field);
      if (fieldIndex == -1) {
        throw std::runtime_error("unknown field in order by: " + orderByExpr->field);
      }
      const auto val1 = row1->values[fieldIndex];
      const auto val2 = row2->values[fieldIndex];

      if (val1->type == FIELD_TYPE_INT) {
        int ival1 = val1->getIVal();
        int ival2 = val2->getIVal();

        if (ival1 > ival2) {
          return !orderByExpr->asc;
        }

        if (ival1 < ival2) {
          return orderByExpr->asc;
        }
      } else if (val1->type == FIELD_TYPE_BIGINT) {
        uint64_t ival1 = val1->getUInt64Val();
        uint64_t ival2 = val2->getUInt64Val();

        if (ival1 > ival2) {
          return !orderByExpr->asc;
        }

        if (ival1 < ival2) {
          return orderByExpr->asc;
        }
      } else if (val1->type == FIELD_TYPE_STRING) {
        const auto strVal1 = val1->getStrVal();
        const auto strVal2 = val2->getStrVal();
        const auto compareResult = strVal1.compare(strVal2);
        if (compareResult > 0) {
          return !orderByExpr->asc;
        } else if (compareResult < 0) {
          return orderByExpr->asc;
        }
      } else {
        throw std::runtime_error("unknown value type for order by");
      }
    }

    return false;
  });
}

void Query::applyLimit() {
  if (limit == -1 || limit >= result.rows.size()) {
    return;
  }

  for (auto it = result.rows.begin() + limit; it != result.rows.end(); it++) {
    delete *it;
  }

  result.rows.erase(result.rows.begin() + limit, result.rows.end());
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
        throw std::runtime_error("unspported value type in result rows");
      }

      if (i < selectExprCount - 1) {
        cout << ", ";
      }
    }

    cout << endl;
  }
}

void Query::run() {
  // prepare initial bitmap
  Roaring *roar;
  chrono::time_point<chrono::system_clock> start;
  chrono::duration<double> elapsed;
  long long duration;

  if (table->size > 0) {
    roar = new Roaring(roaring_bitmap_from_range(1, table->size + 1, 1));
  } else {
    roar = new Roaring();
  }
  initialBitmap = roar;

  // apply filters
  start = std::chrono::system_clock::now();
  applyFilters();
  elapsed = std::chrono::system_clock::now() - start;
  stats.filter_us = std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count();
  stats.filter_ms = std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count();

  // apply groups
  start = std::chrono::system_clock::now();
  genAggrGroups();
  elapsed = std::chrono::system_clock::now() - start;
  stats.group_us = std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count();
  stats.group_ms = std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count();

  // generate rows
  genResultRows();

  // apply order
  start = std::chrono::system_clock::now();
  applyOrder();
  elapsed = std::chrono::system_clock::now() - start;
  stats.order_us = std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count();
  stats.order_ms = std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count();

  // apply limit
  applyLimit();

  // reset initial bitmap
  initialBitmap = nullptr;
  delete roar;
}
