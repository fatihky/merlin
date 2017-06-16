#include <iostream>
#include <map>
#include <uWS/uWS.h>
#include "rapidjson/rapidjson.h"
#include "rapidjson/document.h"

using namespace std;

typedef void (*CommandHandlerFunc) (uWS::HttpResponse *res, uWS::HttpRequest &req, rapidjson::Document &doc);

struct MerlinHttpServer {
  map<string, CommandHandlerFunc> commandHandlers;
};

// request validator
static bool validateRequestBody(uWS::HttpResponse *res, uWS::HttpRequest &req, rapidjson::Document &doc);

// command handlers
static void commandPing(uWS::HttpResponse *res, uWS::HttpRequest &req, rapidjson::Document &doc);

// global server context
static MerlinHttpServer httpServer = {};

void httpServerInit() {
  httpServer.commandHandlers["ping"] = commandPing;
}

void httpHandler(uWS::HttpResponse *res, uWS::HttpRequest req, char *data, size_t length, size_t remainingBytes) {
  if (req.getMethod() != uWS::METHOD_POST) {
    return res->end("only post is supported", 22);
  }

  if (length == 0) {
    return res->end("no request body found", 21);
  }

  rapidjson::Document document;
  document.Parse(data, length);

  if (!document.IsObject()) {
    return res->end("a valid json object is required as request body", 47);
  }

  if (!validateRequestBody(res, req, document)) {
    return;
  }

  const auto cmd = document["command"].GetString();
  const auto handler = httpServer.commandHandlers[cmd];

  handler(res, req, document);
}

static bool validateRequestBody(uWS::HttpResponse *res, uWS::HttpRequest &req, rapidjson::Document &doc) {
  if (!doc["command"].IsString()) {
    res->end("command property is required", 28);
    return false;
  }

  // check whether command is valid
  if (httpServer.commandHandlers.count(doc["command"].GetString()) == 0) {
    res->end("invalid command", 15);
    return false;
  }

  return true;
}

static void commandPing(uWS::HttpResponse *res, uWS::HttpRequest &req, rapidjson::Document &doc) {
  res->end("{\"pong\": true}", 14);
}