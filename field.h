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

class Field;

struct aggr_func_min_data {
  uint64_t min;
  Field *field;
};

struct aggr_func_max_data {
  uint64_t max;
  Field *field;
};

struct aggr_func_sum_data {
  uint64_t sum;
  Field *field;
};

static bool aggrFuncMinInt(uint32_t value, void *data);
static bool aggrFuncMaxInt(uint32_t value, void *data);
static bool aggrFuncSumInt(uint32_t value, void *data);

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
    assert(genericValueContainer.type == type);

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

          default: throw std::runtime_error("unknown string encoding");
        }
      } break;
      case FIELD_TYPE_BOOLEAN: {
        if (genericValueContainer.bVal) {
          storage.bvals->add((uint32_t) size);
        }
      } break;
      default: throw std::runtime_error("unknown field type");
    }

    size++;
  }

  Roaring *getBitmap(string op, string value) {
    switch (type) {
      case FIELD_TYPE_STRING: {
        switch (encoding) {
          case FIELD_ENCODING_DICT: {
            if (op != "=") {
              throw std::runtime_error("unsupported operator");
            }

            return storage.strval.dict.dict[value];
          };
          default: throw std::runtime_error("unsupported string encoding");
        }
      }
      default: throw std::runtime_error("unsupported field type");
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
          default: throw std::runtime_error("field \"" + name + "\": unsupported string encoding " + to_string(encoding) + " for group by.");
        }
      } break;
      default: throw std::runtime_error("field \"" + name + "\": unsupported field type " + to_string(type) + " for group by.");
    }

    return result;
  };

  uint64_t aggrFuncMin(Roaring *bitmap) {
    const auto field = this;
    uint64_t min = UINT64_MAX;
    const aggr_func_min_data ctx = {
      .min = UINT64_MAX,
      .field = this
    };

    if (type != FIELD_TYPE_INT) {
      throw std::runtime_error("only int fields supported by min()");
    }

    bitmap->iterate(aggrFuncMinInt, (void *) &ctx);

    return ctx.min;
  }

  uint64_t aggrFuncMax(Roaring *bitmap) {
    const auto field = this;
    const aggr_func_max_data ctx = {
      .max = 0,
      .field = this
    };

    if (type != FIELD_TYPE_INT) {
      throw std::runtime_error("only int fields supported by max()");
    }

    bitmap->iterate(aggrFuncMaxInt, (void *) &ctx);

    return ctx.max;
  }

  uint64_t aggrFuncSum(Roaring *bitmap) {
    const auto field = this;
    const aggr_func_sum_data ctx = {
      .sum = 0,
      .field = this
    };

    if (type != FIELD_TYPE_INT) {
      throw std::runtime_error("only int fields supported by sum()");
    }

    bitmap->iterate(aggrFuncSumInt, (void *) &ctx);

    return ctx.sum;
  }
};


static bool aggrFuncMinInt(uint32_t value, void *data) {
  const auto ctx = (aggr_func_min_data *) data;
  const auto ival = ctx->field->storage.ivals[value];

  if (ival < ctx->min) {
    ctx->min = (uint64_t) ival;
  }

  return true;
}

static bool aggrFuncMaxInt(uint32_t value, void *data) {
  const auto ctx = (aggr_func_max_data *) data;
  const auto ival = ctx->field->storage.ivals[value];

  if (ival > ctx->max) {
    ctx->max = (uint64_t) ival;
  }

  return true;
}

static bool aggrFuncSumInt(uint32_t value, void *data) {
  const auto ctx = (aggr_func_sum_data *) data;
  const auto ival = ctx->field->storage.ivals[value];

  ctx->sum += ival;

  return true;
}

#endif //MERLIN_FIELD_H
