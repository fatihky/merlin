#include "../http.h"

void commandPing(picojson::object &req, picojson::object &res) {
  res["pong"] = picojson::value(true);
}
