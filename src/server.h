#include <map>
#include "table.h"

#ifndef MERLIN_SERVER_H
#define MERLIN_SERVER_H

using namespace std;

struct MerlinServer {
  map<string, Table *> tables;
};

extern MerlinServer server;

#endif //MERLIN_SERVER_H
