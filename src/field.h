//
// Created by Fatih Kaya on 6/1/17.
//

#include <string>
#include <vector>
#include <map>
#include <stdlib.h>
#include "roaring/roaring.h"
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
    roaring_bitmap_t *bvals;
    // FIELD_TYPE_STRING
    struct {
      struct {
        // FIELD_ENCODING_NONE
        vector<string> arr;
      } raw;
      struct {
        // FIELD_ENCODING_DICT
        map<string, roaring_bitmap_t *> dict;
      } dict;
      struct {
        // FIELD_ENCODING_MULTI_VAL
        vector<vector<int>> valArrays;
        map<string, int> idMap;
        map<int, roaring_bitmap_t *> idToBitmap;
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

  roaring_bitmap_t *getBitmap(string op, string value);

  map<string, roaring_bitmap_t *> genGroups(roaring_bitmap_t *initialBitmap);

  map<string, roaring_bitmap_t *> genGroups(roaring_bitmap_t *initialBitmap, string func, vector<string> funcArgs);

  uint64_t aggrFuncMin(roaring_bitmap_t *bitmap);

  uint64_t aggrFuncMax(roaring_bitmap_t *bitmap);

  uint64_t aggrFuncSum(roaring_bitmap_t *bitmap);
};

#endif //MERLIN_FIELD_H
