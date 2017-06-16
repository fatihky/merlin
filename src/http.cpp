#include <iostream>
#include <map>
#include <uWS/uWS.h>
#include "picojson.h"
#include "table.h"
#include "field.h"

using namespace std;

// return values:
// true: send response document to the client
// false: do not send anything, handler already sent custom response
typedef bool (*CommandHandlerFunc) (uWS::HttpResponse *httpRes, uWS::HttpRequest &httpReq, picojson::object &req, picojson::object &res);

struct MerlinHttpServer {
  map<string, CommandHandlerFunc> commandHandlers;
  map<string, Table *> tables;
};

// request validator
static bool validateRequestBody(uWS::HttpResponse *res, uWS::HttpRequest &req, picojson::object &doc);

// command handlers
static bool commandPing(uWS::HttpResponse *httpRes, uWS::HttpRequest &httpReq, picojson::object &req, picojson::object &res);
static bool commandShowTables(uWS::HttpResponse *httpRes, uWS::HttpRequest &httpReq, picojson::object &req, picojson::object &res);
static bool commandCreateTable(uWS::HttpResponse *httpRes, uWS::HttpRequest &httpReq, picojson::object &req, picojson::object &res);

// global server context
static MerlinHttpServer httpServer = {
};

map<string, int> fieldTypes = {
  {"timestamp", FIELD_TYPE_TIMESTAMP},
  {"string", FIELD_TYPE_STRING},
  {"int", FIELD_TYPE_INT}
};
map<string, int> encodingTypes = {
  {"dict", FIELD_ENCODING_DICT}
};

void httpServerInit() {
  httpServer.commandHandlers["ping"] = commandPing;
  httpServer.commandHandlers["show_tables"] = commandShowTables;
  httpServer.commandHandlers["create_table"] = commandCreateTable;
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

static bool setError(picojson::object &res, string message) {
  res["stat"] = picojson::value("error");
  res["error_message"] = picojson::value(message);
  return true;
}

static bool commandPing(uWS::HttpResponse *httpRes, uWS::HttpRequest &httpReq, picojson::object &req, picojson::object &res) {
  res["pong"] = picojson::value(true);
  return true;
}

static bool commandShowTables(uWS::HttpResponse *httpRes, uWS::HttpRequest &httpReq, picojson::object &req, picojson::object &res) {
  picojson::array tableNames;

  for (auto &&tableIter : httpServer.tables) {
    tableNames.push_back(picojson::value(tableIter.first));
  }

  res["tables"] = picojson::value(tableNames);

  return true;
}

static bool commandCreateTable(uWS::HttpResponse *httpRes, uWS::HttpRequest &httpReq, picojson::object &req, picojson::object &res) {
  picojson::array tableNames;
  string tableName;
  const auto fields = req["fields"];
  string err;
  const auto table = new Table();

  if (!req["name"].is<string>()) {
    err = "table name is required";
    goto error;
  }

  tableName = req["name"].to_str();

  if (httpServer.tables.count(tableName) == 1) {
    err = "table already exist";
    goto error;
  }

  if (!fields.is<picojson::array>()) {
    err = "fields prop is required.";
    goto error;
  }

  for (auto &&field : fields.get<picojson::array>()) {
    if (!field.is<picojson::object>()) {
      err = "field must be an object";
      goto error;
    }

    auto obj = field.get<picojson::object>();
    string name;
    string type;
    string encoding;

    if (!obj["name"].is<string>()) {
      err = "name prop required in field definitions";
      goto error;
    }

    name = obj["name"].to_str();

    if (!obj["type"].is<string>()) {
      err = "type prop required in field definitions";
      goto error;
    }

    type = obj["type"].to_str();

    if (fieldTypes.count(type) == 0) {
      err = "invalid field type: " + type;
      goto error;
    }

    if (obj["encoding"].is<string>()) {
      encoding = obj["encoding"].to_str();

      if (encodingTypes.count(encoding) == 0) {
        err = "invalid encoding type: " + encoding;
        goto error;
      }

      if (encoding == "dict" && type != "string") {
        err = "dict encoding only usable with string fields at the moment";
        goto error;
      }
    }

    const auto mField = new Field(name, fieldTypes[type]);

    if (!encoding.empty()) {
      mField->encoding = encodingTypes[encoding];
    }

    table->setField(mField);
  }

  httpServer.tables[tableName] = table;

  res["created"] = picojson::value(true);

  return true;

  error:
    return setError(res, err);
}

int main() {
  uWS::Hub h;

  // start http server
  httpServerInit();

  h.onHttpRequest(httpHandler);

  cout << "starting http server on :3000" << endl;

  h.listen(3000);
  h.run();

  return 0;
}
