#include <iostream>
#include <map>
#include <netdb.h> // NI_MAXHOST, required by Simple-Web-Server
#include <netinet/ip.h> // ip_mreq, required by Simple-Web-Server
#include "table.h"
#include "query.h"
#include "server_http.hpp"
#include "http.h"

using namespace std;

map<string, int> fieldTypes = {
  {"timestamp", FIELD_TYPE_TIMESTAMP},
  {"string", FIELD_TYPE_STRING},
  {"int", FIELD_TYPE_INT}
};
map<string, int> encodingTypes = {
  {"dict", FIELD_ENCODING_DICT}
};
map<int, string> fieldTypeToStr = {
  {FIELD_TYPE_TIMESTAMP, "timestamp"},
  {FIELD_TYPE_STRING, "string"},
  {FIELD_TYPE_INT, "int"}
};
map<int, string> encodingTypeToStr = {
  {FIELD_ENCODING_NONE, "none"},
  {FIELD_ENCODING_DICT, "dict"},
  {FIELD_ENCODING_MULTI_VAL, "multi_val"}
};

typedef SimpleWeb::Server<SimpleWeb::HTTP> HttpServer;

class MerlinHttpServerResponse {
  public:

  std::shared_ptr<HttpServer::Response> response;

  MerlinHttpServerResponse(std::shared_ptr<HttpServer::Response> response_): response(response_) {}

  void end(const char *res, size_t length) {
    string contentType = "text/plain; charset=utf-8";

    if (length > 0 && res[0] == '{') {
      contentType = "application/json; charset=utf-8";
    }

    *response << "HTTP/1.1 200 OK\r\n"
              << "Content-Type: " << contentType << "\r\n"
              << "Content-Length: " << length << "\r\n\r\n"
              << res;
  }
};

// http request handler
static void httpHandler(shared_ptr<HttpServer::Response> response, shared_ptr<HttpServer::Request> request);

// request validator
static bool validateRequestBody(MerlinHttpServerResponse &res, picojson::object &doc);

// global server context
MerlinHttpServer httpServer = {
};
HttpServer server;

void httpServerInit() {
  httpServer.commandHandlers["ping"] = commandPing;
  httpServer.commandHandlers["quit"] = commandQuit;
  httpServer.commandHandlers["show_tables"] = commandShowTables;
  httpServer.commandHandlers["create_table"] = commandCreateTable;
  httpServer.commandHandlers["describe_table"] = commandDescribeTable;
  httpServer.commandHandlers["drop_table"] = commandDropTable;
  httpServer.commandHandlers["insert_into_table"] = commandInsertIntoTable;
  httpServer.commandHandlers["query_table"] = commandQueryTable;
  httpServer.commandHandlers["stats_table"] = commandTableStatistics;
}

void httpServerDeinit() {
  for (auto &&it : httpServer.tables) {
    auto table = it.second;
    delete table;
  }
}

void httpServerStop() {
  server.stop();
}

// helper function for handlers
void setError(picojson::object &res, string message) {
  res["stat"] = picojson::value("error");
  res["error_message"] = picojson::value(message);
}

void httpHandler(shared_ptr<HttpServer::Response> response, shared_ptr<HttpServer::Request> request) {
  auto res = MerlinHttpServerResponse(response);
  auto content = request->content.string();

  if (content.size() == 0) {
    return res.end("no request body found", 21);
  }

  picojson::value documentWrap;
  string parseErr;
  picojson::parse(documentWrap, content.c_str(), content.c_str() + content.size(), &parseErr);

  if (!parseErr.empty()) {
    return res.end("json is not valid", 16);
  }

  if (!documentWrap.is<picojson::object>()) {
    return res.end("a valid json object is required as request body", 47);
  }

  if (!validateRequestBody(res, documentWrap.get<picojson::object>())) {
    return;
  }

  picojson::object responseDocument;

  const string cmd = documentWrap.get("command").to_str();
  const auto handler = httpServer.commandHandlers[cmd];
  handler(documentWrap.get<picojson::object>(), responseDocument);

  const auto responseString = picojson::value(responseDocument).serialize();
  res.end(responseString.c_str(), responseString.size());
}

static bool validateRequestBody(MerlinHttpServerResponse &res, picojson::object &doc) {
  auto command = doc["command"];

  if (!command.is<string>()) {
    res.end("command property is required", 28);
    return false;
  }

  // check whether command is valid
  if (httpServer.commandHandlers.count(doc["command"].to_str()) == 0) {
    res.end("invalid command", 15);
    return false;
  }

  return true;
}

int main() {
  // start http server
  httpServerInit();

  cout << "starting http server on :3000" << endl;

  server.resource["/api/v1/command"]["POST"] = httpHandler;
  server.default_resource["POST"] = httpHandler;

  server.default_resource["GET"] = [](shared_ptr<HttpServer::Response> response, shared_ptr<HttpServer::Request> request) {
    string msg = "hello, merlin";
    *response << "HTTP/1.1 200 OK\r\n"
              << "Content-Type: text/plain\r\n"
              << "Content-Length: " << msg.length() << "\r\n\r\n"
              << msg;
  };

  server.config.port = 3000;
  server.start();

  httpServerDeinit();

  return 0;
}
