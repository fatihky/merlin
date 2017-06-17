#include <iostream>
#include <map>
#include <uWS/uWS.h>
#include "table.h"
#include "field.h"

// picojson and roaring bitmap both declares this
// but picojson does does not check whether this
// constant is defined
#ifdef __STDC_FORMAT_MACROS
#  undef __STDC_FORMAT_MACROS
#endif
#define PICOJSON_USE_INT64
#include "picojson.h"

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
static bool commandDescribeTable(uWS::HttpResponse *httpRes, uWS::HttpRequest &httpReq, picojson::object &req, picojson::object &res);
static bool commandDropTable(uWS::HttpResponse *httpRes, uWS::HttpRequest &httpReq, picojson::object &req, picojson::object &res);
static bool commandInsertIntoTable(uWS::HttpResponse *httpRes, uWS::HttpRequest &httpReq, picojson::object &req, picojson::object &res);

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

void httpServerInit() {
  httpServer.commandHandlers["ping"] = commandPing;
  httpServer.commandHandlers["show_tables"] = commandShowTables;
  httpServer.commandHandlers["create_table"] = commandCreateTable;
  httpServer.commandHandlers["describe_table"] = commandDescribeTable;
  httpServer.commandHandlers["drop_table"] = commandDropTable;
  httpServer.commandHandlers["insert_into_table"] = commandInsertIntoTable;
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

static bool commandDescribeTable(uWS::HttpResponse *httpRes, uWS::HttpRequest &httpReq, picojson::object &req, picojson::object &res) {
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

  return true;
}

static bool commandDropTable(uWS::HttpResponse *httpRes, uWS::HttpRequest &httpReq, picojson::object &req, picojson::object &res) {
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

  return true;
}

static bool commandInsertIntoTable(uWS::HttpResponse *httpRes, uWS::HttpRequest &httpReq, picojson::object &req, picojson::object &res) {
  picojson::array fields;
  string tableName;
  string err;
  auto rows = req["rows"];

  if (!req["name"].is<string>()) {
    return setError(res, "name prop is required");
  }

  if (!rows.is<picojson::array>()) {
    return setError(res, "rows prop must be an array");
  }

  tableName = req["name"].to_str();

  if (httpServer.tables.count(tableName) == 0) {
    return setError(res, "table not found");
  }

  const Table *table = httpServer.tables[tableName];

  for (auto &&row : rows.get<picojson::array>()) {
    if (!row.is<picojson::object>()) {
      err = "each row must be an object";
      goto error;
    }

    picojson::object obj = row.get<picojson::object>();

    for (auto &&fieldIter : table->fields) {
      auto field = fieldIter.second;
      auto value = obj[field->name];
      if (value.is<double>()) {
        if (field->type == FIELD_TYPE_TIMESTAMP) {
          field->addValue(std::move(GenericValueContainer(value.get<int64_t>())));
        } else if (field->type == FIELD_TYPE_INT) {
          field->addValue(std::move(GenericValueContainer((int) value.get<int64_t>())));
        } else {
          err = "invalid value type for field: " + field->name;
          goto error;
        }
      } else if (value.is<string>()) {
        if (field->type == FIELD_TYPE_STRING) {
          field->addValue(std::move(GenericValueContainer(value.to_str())));
        } else {
          err = "invalid value type for field: " + field->name;
          goto error;
        }
      } else {
        err = "invalid value type for field: " + field->name;
        goto error;
      }
    }

    table->incrementRecordCount();

  }

  res["inserted"] = picojson::value(true);

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
