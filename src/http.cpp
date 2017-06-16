#include <iostream>
#include <map>
#include <uWS/uWS.h>
#include "picojson.h"
#include "server.h"

using namespace std;

// return values:
// true: send response document to the client
// false: do not send anything, handler already sent custom response
typedef bool (*CommandHandlerFunc) (uWS::HttpResponse *httpRes, uWS::HttpRequest &httpReq, picojson::object &req, picojson::object &res);

struct MerlinHttpServer {
  map<string, CommandHandlerFunc> commandHandlers;
};

// request validator
static bool validateRequestBody(uWS::HttpResponse *res, uWS::HttpRequest &req, picojson::object &doc);

// command handlers
static bool commandPing(uWS::HttpResponse *httpRes, uWS::HttpRequest &httpReq, picojson::object &req, picojson::object &res);
static bool commandShowTables(uWS::HttpResponse *httpRes, uWS::HttpRequest &httpReq, picojson::object &req, picojson::object &res);

// global server context
static MerlinHttpServer httpServer = {};

void httpServerInit() {
  httpServer.commandHandlers["ping"] = commandPing;
  httpServer.commandHandlers["show_tables"] = commandShowTables;
}

void httpHandler(uWS::HttpResponse *res, uWS::HttpRequest req, char *data, size_t length, size_t remainingBytes) {
  if (req.getMethod() != uWS::METHOD_POST) {
    return res->end("only post is supported", 22);
  }

  if (length == 0) {
    return res->end("no request body found", 21);
  }

  picojson::value documentWrap;
  string parseErr;
  picojson::parse(documentWrap, data, data + length, &parseErr);

  if (!parseErr.empty()) {
    return res->end("json is not valid", 16);
  }

  if (!documentWrap.is<picojson::object>()) {
    return res->end("a valid json object is required as request body", 47);
  }

  if (!validateRequestBody(res, req, documentWrap.get<picojson::object>())) {
    return;
  }

  picojson::object responseDocument;

  const string cmd = documentWrap.get("command").to_str();
  const auto handler = httpServer.commandHandlers[cmd];
  const auto sendResponseDocument = handler(res, req, documentWrap.get<picojson::object>(), responseDocument);

  if (sendResponseDocument) {
    const auto responseString = picojson::value(responseDocument).serialize();
    res->end(responseString.c_str(), responseString.size());
  }
}

static bool validateRequestBody(uWS::HttpResponse *res, uWS::HttpRequest &req, picojson::object &doc) {
  auto command = doc["command"];

  if (!command.is<string>()) {
    res->end("command property is required", 28);
    return false;
  }

  // check whether command is valid
  if (httpServer.commandHandlers.count(doc["command"].to_str()) == 0) {
    res->end("invalid command", 15);
    return false;
  }

  return true;
}

static bool commandPing(uWS::HttpResponse *httpRes, uWS::HttpRequest &httpReq, picojson::object &req, picojson::object &res) {
  res["pong"] = picojson::value(true);
  return true;
}

static bool commandShowTables(uWS::HttpResponse *httpRes, uWS::HttpRequest &httpReq, picojson::object &req, picojson::object &res) {
  picojson::array tableNames;

  for (auto &&tableIter : server.tables) {
    tableNames.push_back(picojson::value(tableIter.first));
  }

  res["tables"] = picojson::value(tableNames);

  return true;
}
