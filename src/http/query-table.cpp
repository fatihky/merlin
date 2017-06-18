#include <chrono>
#include "../http.h"
#include "../query.h"

void commandQueryTable(picojson::object &req, picojson::object &res) {
  Query *query = nullptr;
  picojson::array fields;
  string tableName;
  string err;
  bool debug = false;
  int limit = -1;
  picojson::array selectedFields;
  picojson::array resultRows;
  picojson::object queryStats;
  chrono::time_point<chrono::system_clock> start;
  chrono::duration<double> elapsed;
  long long milliseconds;
  long long microseconds;

  if (!req["name"].is<string>()) {
    return setError(res, "name prop is required");
  }

  if (!req["select"].is<picojson::array>()) {
    return setError(res, "select prop must be an array");
  }

  if (!req["filters"].is<picojson::array>()) {
    return setError(res, "filters prop must be an array");
  }

  if (!req["group_by"].is<picojson::array>()) {
    return setError(res, "group_by prop must be an array");
  }

  if (!req["order_by"].is<picojson::array>()) {
    return setError(res, "order_by prop must be an array");
  }

  if (req["debug"].is<bool>()) {
    debug = req["debug"].get<bool>();
  }

  if (req["limit"].is<double>()) {
    limit = (int) req["limit"].get<int64_t>();
  }

  tableName = req["name"].to_str();

  if (httpServer.tables.count(tableName) == 0) {
    return setError(res, "table not found");
  }

  Table *table = httpServer.tables[tableName];
  query = new Query(table, debug);

  if (limit != -1) {
    query->limit = limit;
  }

  for (auto &&row : req["select"].get<picojson::array>()) {
    if (!row.is<picojson::object>()) {
      err = "each select expression must be an object";
      goto error;
    }

    picojson::object& obj = row.get<picojson::object>();
    SelectExpr *selectExpr;
    string field;
    string aggrFunc;
    string display;

    if (!obj["field"].is<string>()) {
      err = "select: field prop is required in each filter obj.";
      goto error;
    }

    field = obj["field"].get<string>();

    if (field.empty()) {
      err = "select: field can not be empty";
      goto error;
    }

    selectExpr = new SelectExpr(field);

    if (obj["display"].is<string>()) {
      selectExpr->display = obj["display"].get<string>();
    }

    if (obj["aggr_func"].is<string>()) {
      selectExpr->aggerationFunc = obj["aggr_func"].get<string>();
      selectExpr->isAggerationSelect = true;
    }

    // add this information to selectedFields array
    if (!selectExpr->display.empty()) {
      selectedFields.push_back(picojson::value(selectExpr->display));
    } else if (!selectExpr->isAggerationSelect) {
      selectedFields.push_back(picojson::value(selectExpr->field));
    } else {
      selectedFields.push_back(picojson::value(selectExpr->aggerationFunc + "(" + selectExpr->field + ")"));
    }

    query->selectExprs.push_back(selectExpr);
  }

  for (auto &&row : req["filters"].get<picojson::array>()) {
    if (!row.is<picojson::object>()) {
      err = "each filter must be an object";
      goto error;
    }

    picojson::object& obj = row.get<picojson::object>();
    string field;
    string op;
    string value;

    if (!obj["field"].is<string>()) {
      err = "filters: field prop is required in each filter obj.";
      goto error;
    }

    field = obj["field"].get<string>();

    if (field.empty()) {
      err = "filters: field can not be empty";
      goto error;
    }

    if (!obj["operator"].is<string>()) {
      err = "filters: field prop is required in each filter obj.";
      goto error;
    }

    op = obj["operator"].get<string>();

    if (op.empty()) {
      err = "filters: operator can not be empty";
      goto error;
    }

    if (!obj["value"].is<string>()) {
      err = "filters: value prop is required in each filter obj.";
      goto error;
    }

    value = obj["value"].get<string>();

    if (value.empty()) {
      err = "filters: value can not be empty";
      goto error;
    }

    cout << "new filter: " << field << op << value << endl;
    query->filterExprs.push_back(new FilterExpr(field, op, value));
  }

  for (auto &&row : req["group_by"].get<picojson::array>()) {
    if (!row.is<picojson::object>()) {
      err = "each group by expression must be an object";
      goto error;
    }

    picojson::object& obj = row.get<picojson::object>();
    string field;

    if (!obj["field"].is<string>()) {
      err = "group by: field prop is required in each filter obj.";
      goto error;
    }

    field = obj["field"].get<string>();

    if (field.empty()) {
      err = "group by: field can not be empty";
      goto error;
    }

    query->groupByExprs.push_back(new GroupByExpr(field));
  }

  for (auto &&row : req["order_by"].get<picojson::array>()) {
    if (!row.is<picojson::object>()) {
      err = "each order by expression must be an object";
      goto error;
    }

    picojson::object& obj = row.get<picojson::object>();
    string field;

    if (!obj["field"].is<string>()) {
      err = "order by: field prop is required in each filter obj.";
      goto error;
    }

    field = obj["field"].get<string>();

    if (field.empty()) {
      err = "order by: field can not be empty";
      goto error;
    }

    auto orderByExpr = new OrderByExpr(field);

    if (obj["asc"].is<bool>()) {
      orderByExpr->asc = obj["asc"].get<bool>();
    }

    query->orderByExprs.push_back(orderByExpr);
  }

  start = std::chrono::system_clock::now();

  query->isAggregationQuery = true;
  query->run();

  elapsed = std::chrono::system_clock::now() - start;
  milliseconds = std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count();
  microseconds = std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count();

  for (auto &&row : query->result.rows) {
    picojson::array picoRow;

    for (auto &&value : row->values) {
      if (value->type == FIELD_TYPE_STRING) {
        picoRow.push_back(picojson::value(value->getStrVal()));
      } else if (value->type == FIELD_TYPE_TIMESTAMP) {
        picoRow.push_back(picojson::value(value->getInt64Val()));
      } else if (value->type == FIELD_TYPE_BIGINT) {
        picoRow.push_back(picojson::value((int64_t) value->getUInt64Val()));
      } else {
        err = "invalid result row value type: " + to_string(value->type);
        goto error;
      }
    }

    resultRows.push_back(picojson::value(picoRow));
  }

  if (req["query_stats_detailed"].is<bool>() && req["query_stats_detailed"].get<bool>() == true) {
    queryStats["filter_us"] = picojson::value(query->stats.filter_us);
    queryStats["filter_ms"] = picojson::value(query->stats.filter_ms);
    queryStats["group_us"] = picojson::value(query->stats.group_us);
    queryStats["group_ms"] = picojson::value(query->stats.group_ms);
    queryStats["order_us"] = picojson::value(query->stats.order_us);
    queryStats["order_ms"] = picojson::value(query->stats.order_ms);
    res["query_stats_detailed"] = picojson::value(queryStats);
  }

  res["elapsed_ms"] = picojson::value((int64_t) milliseconds);
  res["elapsed_us"] = picojson::value((int64_t) microseconds);
  res["columns"] = picojson::value(selectedFields);
  res["rows"] = picojson::value(resultRows);

  delete query;

  return;

  error:

  if (query) {
    delete query;
  }

  return setError(res, err);
}
