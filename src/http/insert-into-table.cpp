#include "../http.h"

void commandInsertIntoTable(picojson::object &req, picojson::object &res) {
  picojson::array fields;
  string tableName;
  string err;
  auto rows = req["rows"];

  if (!req["name"].is<string>()) {
    return setError(res, "name prop is required");
  }

  if (!rows.is<picojson::array>()) {
    return setError(res, "rows prop must be an array");
  }

  tableName = req["name"].to_str();

  if (httpServer.tables.count(tableName) == 0) {
    return setError(res, "table not found");
  }

  Table *table = httpServer.tables[tableName];

  for (auto &&row : rows.get<picojson::array>()) {
    if (!row.is<picojson::object>()) {
      err = "each row must be an object";
      goto error;
    }

    picojson::object& obj = row.get<picojson::object>();

    for (auto &&fieldIter : table->fields) {
      auto field = fieldIter.second;
      auto value = obj[field->name];
      if (value.is<double>()) {
        if (field->type == FIELD_TYPE_TIMESTAMP) {
          field->addValue(std::move(GenericValueContainer(value.get<int64_t>())));
        } else if (field->type == FIELD_TYPE_INT) {
          field->addValue(std::move(GenericValueContainer((int) value.get<int64_t>())));
        } else {
          err = "invalid value type for field: " + field->name;
          goto error;
        }
      } else if (value.is<string>()) {
        if (field->type == FIELD_TYPE_STRING) {
          field->addValue(std::move(GenericValueContainer(value.to_str())));
        } else {
          err = "invalid value type for field: " + field->name;
          goto error;
        }
      } else {
        err = "invalid value type for field: " + field->name;
        goto error;
      }
    }

    table->incrementRecordCount();

  }

  res["inserted"] = picojson::value(true);

  return;

  error:
  return setError(res, err);
}
