//
// Created by Fatih Kaya on 6/1/17.
//

#include <string>
#include <vector>
#include <map>
#include "deps/CRoaring/cpp/roaring.hh"
#include "field-types.h"
#include "generic-value.h"

#ifndef MERLIN_FIELD_H
#define MERLIN_FIELD_H

using namespace std;

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

  void addValue(const GenericValueContainer &genericValueContainer) {
    assert(genericValueContainer.type === type);

    switch (type) {
      case FIELD_TYPE_TIMESTAMP: {
        storage.timestamps.push_back(genericValueContainer.getInt64Val());
      } break;
      case FIELD_TYPE_INT: {
        storage.ivals.push_back(genericValueContainer.getIVal());
      } break;
      case FIELD_TYPE_STRING: {
        storage.timestamps.push_back(genericValueContainer.getInt64Val());

        switch (encoding) {
          case FIELD_ENCODING_NONE: {
            storage.strval.raw.arr.push_back(genericValueContainer.getStrVal());
          } break;
          case FIELD_ENCODING_DICT: {
            string key = genericValueContainer.getStrVal();
            uint32_t index = (uint32_t) size + 1;

            if (storage.strval.dict.dict.count(key)) {
              storage.strval.dict.dict[key]->add(index);
            } else {
              storage.strval.dict.dict[key] = new Roaring();
              storage.strval.dict.dict[key]->add(index);
            }
          } break;

          default: assert(0 && "unknown string encoding");
        }
      } break;
      case FIELD_TYPE_BOOLEAN: {
        if (genericValueContainer.bVal) {
          storage.bvals->add((uint32_t) size);
        }
      } break;
      default: assert(0 && "unknown field type");
    }

    size++;
  }

  Roaring *getBitmap(string op, string value) {
    switch (type) {
      case FIELD_TYPE_STRING: {
        switch (encoding) {
          case FIELD_ENCODING_DICT: {
            if (op != "=") {
              assert(0 && "unsupported operator");
            }

            return storage.strval.dict.dict[value];
          };
          default: assert(0 && "unsupported string encoding");
        }
      }
      default: assert(0 && "unsupported field type");
    }

    return nullptr;
  }

  map<string, Roaring *> genGroups(Roaring *initialBitmap) {
    map<string, Roaring *> result;

    switch (type) {
      case FIELD_TYPE_STRING: {
        switch (encoding) {
          case FIELD_ENCODING_DICT: {
            for (auto &&val : storage.strval.dict.dict) {
              const auto bitmap = new Roaring((*val.second) & *initialBitmap);
              if (bitmap->cardinality() == 0) {
                continue;
              }
              result[val.first] = bitmap;
            }
          } break;
          default: assert(0 && "unsupported string encoding");
        }
      }
      default: assert(0 && "unsupported field type");
    }

    return result;
  };
};

#endif //MERLIN_FIELD_H