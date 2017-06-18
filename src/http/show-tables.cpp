#include "../http.h"

void commandShowTables(picojson::object &req, picojson::object &res) {
  picojson::array tableNames;

  for (auto &&tableIter : httpServer.tables) {
    tableNames.push_back(picojson::value(tableIter.first));
  }

  res["tables"] = picojson::value(tableNames);
}
