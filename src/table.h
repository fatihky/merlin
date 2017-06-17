#include <string>
#include <vector>

#include "field.h"

#ifndef MERLIN_TABLE_H
#define MERLIN_TABLE_H

using namespace std;

class Table {
  public:
  map<string, Field *> fields;
  size_t size; // record count

  void setField(Field *field) {
    fields[field->name] = field;
  }

  void incrementRecordCount() {
    size++;
  }
};

#endif //MERLIN_TABLE_H
