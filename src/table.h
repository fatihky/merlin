#include <string>
#include <vector>

#include "field.h"

#ifndef MERLIN_TABLE_H
#define MERLIN_TABLE_H

using namespace std;

class Table {
  public:
  map<string, Field *> fields;

  void setField(Field *field) {
    fields[field->name] = field;
  }
};

#endif //MERLIN_TABLE_H
