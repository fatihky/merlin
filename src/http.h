#include "table.h"

// picojson and roaring bitmap both declares this
// but picojson does does not check whether this
// constant is defined
#ifdef __STDC_FORMAT_MACROS
#  undef __STDC_FORMAT_MACROS
#endif
#define PICOJSON_USE_INT64
#include "picojson.h"

#ifndef MERLIN_HTTP_H
#define MERLIN_HTTP_H

using namespace std;

typedef void (*CommandHandlerFunc) (picojson::object &req, picojson::object &res);

struct MerlinHttpServer {
  map<string, CommandHandlerFunc> commandHandlers;
  map<string, Table *> tables;
};

extern MerlinHttpServer httpServer;
extern map<string, int> fieldTypes;
extern map<string, int> encodingTypes;
extern map<int, string> fieldTypeToStr;
extern map<int, string> encodingTypeToStr;

void httpServerStop();

// helper for command handlers
void setError(picojson::object &res, string message);

void commandPing(picojson::object &req, picojson::object &res);
void commandQuit(picojson::object &req, picojson::object &res);
void commandShowTables(picojson::object &req, picojson::object &res);
void commandCreateTable(picojson::object &req, picojson::object &res);
void commandDescribeTable(picojson::object &req, picojson::object &res);
void commandDropTable(picojson::object &req, picojson::object &res);
void commandInsertIntoTable(picojson::object &req, picojson::object &res);
void commandQueryTable(picojson::object &req, picojson::object &res);

#endif //MERLIN_HTTP_H
