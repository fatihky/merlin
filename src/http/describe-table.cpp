#include "../http.h"

void commandDescribeTable(picojson::object &req, picojson::object &res) {
  picojson::array fields;
  string tableName;

  if (!req["name"].is<string>()) {
    return setError(res, "name prop is required");
  }

  tableName = req["name"].to_str();

  if (httpServer.tables.count(tableName) == 0) {
    return setError(res, "table not found");
  }

  const Table *table = httpServer.tables[tableName];

  for (auto &&iter : table->fields) {
    const auto field = iter.second;
    picojson::object obj;

    obj["name"] = picojson::value(field->name);
    obj["type"] = picojson::value(fieldTypeToStr[field->type]);
    obj["encoding"] = picojson::value(encodingTypeToStr[field->encoding]);

    fields.push_back(picojson::value(obj));
  }

  res["fields"] = picojson::value(fields);
}
