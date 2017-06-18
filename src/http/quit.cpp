#include "../http.h"

void commandQuit(picojson::object &req, picojson::object &res) {
  res["quit"] = picojson::value(true);
  httpServerStop();
}
