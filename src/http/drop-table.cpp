#include "../http.h"

void commandDropTable(picojson::object &req, picojson::object &res) {
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
  httpServer.tables.erase(tableName);

  delete table;

  res["dropped"] = picojson::value(true);
}
