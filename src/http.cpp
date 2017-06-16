#include <iostream>
#include <map>
#include <uWS/uWS.h>
#include "rapidjson/rapidjson.h"
#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"

using namespace std;

// return values:
// true: send response document to the client
// false: do not send anything, handler already sent custom response
typedef bool (*CommandHandlerFunc) (uWS::HttpResponse *httpRes, uWS::HttpRequest &httpReq, rapidjson::Document &req, rapidjson::Document &res);

struct MerlinHttpServer {
  map<string, CommandHandlerFunc> commandHandlers;
};

// request validator
static bool validateRequestBody(uWS::HttpResponse *res, uWS::HttpRequest &req, rapidjson::Document &doc);

// command handlers
static bool commandPing(uWS::HttpResponse *httpRes, uWS::HttpRequest &httpReq, rapidjson::Document &req, rapidjson::Document &res);

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

  rapidjson::Document responseDocument;
  responseDocument.SetObject();

  const auto cmd = document["command"].GetString();
  const auto handler = httpServer.commandHandlers[cmd];
  const auto sendResponseDocument = handler(res, req, document, responseDocument);

  if (sendResponseDocument) {
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    responseDocument.Accept(writer);
    res->end(buffer.GetString(), buffer.GetLength());
  }
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

static bool commandPing(uWS::HttpResponse *httpRes, uWS::HttpRequest &httpReq, rapidjson::Document &req, rapidjson::Document &res) {
  rapidjson::Document::AllocatorType& allocator = res.GetAllocator();
  res.AddMember("pong", true, allocator);
  return true;
}
