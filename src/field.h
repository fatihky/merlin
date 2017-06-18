//
// Created by Fatih Kaya on 6/1/17.
//

#include <string>
#include <vector>
#include <map>
#include <stdlib.h>
#include "roaring.hh"
#include "../deps/fastrange/fastrange.h"
#include "field-types.h"
#include "generic-value.h"

#ifndef MERLIN_FIELD_H
#define MERLIN_FIELD_H

using namespace std;

class Field;

class Field {
  public:
  string name;
  int type;
  int encoding;
  int size;

  struct {
    // FIELD_TYPE_TIMESTAMP
    vector<int64_t> timestamps;
    // FIELD_TYPE_INT
    vector<int> ivals;
    // FIELD_TYPE_BOOLEAN
    Roaring *bvals;
    // FIELD_TYPE_STRING
    struct {
      struct {
        // FIELD_ENCODING_NONE
        vector<string> arr;
      } raw;
      struct {
        // FIELD_ENCODING_DICT
        map<string, Roaring *> dict;
      } dict;
      struct {
        // FIELD_ENCODING_MULTI_VAL
        vector<vector<int>> valArrays;
        map<string, int> idMap;
        map<int, Roaring *> idToBitmap;
      } multi_val;
    } strval;

  } storage;

  Field(string name_, int type_): name(name_), type(type_) {
    encoding = FIELD_ENCODING_NONE;
    size = 0;
  }

  ~Field() {}

  void setEncoding(const int encoding_) {
    encoding = encoding_;
  }

  void addValue(const GenericValueContainer &genericValueContainer);

  Roaring *getBitmap(string op, string value);

  map<string, Roaring *> genGroups(Roaring *initialBitmap);

  map<string, Roaring *> genGroups(Roaring *initialBitmap, string func, vector<string> funcArgs);

  uint64_t aggrFuncMin(Roaring *bitmap);

  uint64_t aggrFuncMax(Roaring *bitmap);

  uint64_t aggrFuncSum(Roaring *bitmap);
};

#endif //MERLIN_FIELD_H
