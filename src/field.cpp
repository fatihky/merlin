#include "field.h"

struct aggr_func_date_seconds_group_data {
  int64_t seconds;
  map<int64_t, Roaring *> &result;
  Field *field;
};

static bool aggrFuncDateSecondsGroup(uint32_t value, void *data);

void Field::addValue(const GenericValueContainer &genericValueContainer) {
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

Roaring *Field::getBitmap(string op, string value) {
  switch (type) {
    case FIELD_TYPE_STRING: {
      switch (encoding) {
        case FIELD_ENCODING_DICT: {
          if (op != "=") {
            throw std::runtime_error("unsupported operator");
          }

          if (storage.strval.dict.dict.count(value) == 0) {
            return nullptr;
          }

          return storage.strval.dict.dict[value];
        };

        default: throw std::runtime_error("unsupported string encoding");
      }
    } break;
    default: throw std::runtime_error("unsupported field type");
  }

  return nullptr;
}

map<string, Roaring *> Field::genGroups(Roaring *initialBitmap) {
  map<string, Roaring *> result;

  switch (type) {
    case FIELD_TYPE_TIMESTAMP: {
      // generate groups for given seconds.
      // merge generated bitmaps with initial bitmap.
      // return result.

      // first get grouping seconds
      // iterate over initial bitmap's results' timestamps
    } break;
    case FIELD_TYPE_STRING: {
      switch (encoding) {
        case FIELD_ENCODING_DICT: {
          for (auto &&val : storage.strval.dict.dict) {
            const auto bitmap = new Roaring((*val.second) & *initialBitmap);
            if (bitmap->cardinality() == 0) {
              delete bitmap;
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

static bool aggrFuncDateSecondsGroup(uint32_t value, void *data) {
  const auto *ctx = (aggr_func_date_seconds_group_data *) data;
  const auto timestamp = ctx->field->storage.timestamps[value - 1];
  //  const int64_t group = timestamp - (int64_t) fastrange64((uint64_t) timestamp, (uint64_t) ctx->seconds);
  const int64_t group = timestamp - (timestamp % ctx->seconds);
  Roaring *roar = nullptr;

  if (ctx->result.count(group) == 1) {
    roar = ctx->result[group];
  } else {
    roar = new Roaring();
    ctx->result[group] = roar;
  }

  roar->add(value);
  return true;
}

map<string, Roaring *> Field::genGroups(Roaring *initialBitmap, string func, vector<string> funcArgs) {
  map<string, Roaring *> result;
  map<int64_t, Roaring *> intResult;

  if (type != FIELD_TYPE_TIMESTAMP) {
    throw std::runtime_error("field \"" + name + "\" has invalid field type " + to_string(type) + "aggr functions on select only supported for timestamp fields.");
  }

  if (func != "dateSecondsGroup") {
    throw std::runtime_error("only dateSecondsGroup function is supported at the moment");
  }

  const auto cStr = funcArgs[0].c_str();
  const int64_t secs = strtoll(cStr, nullptr, 10);
  aggr_func_date_seconds_group_data aggrResult = {
    .seconds = secs,
    .result = intResult,
    .field = this
  };

  initialBitmap->iterate(aggrFuncDateSecondsGroup, (void *) &aggrResult);

  for (auto &&timeStamp : aggrResult.result) {
    result[to_string(timeStamp.first)] = timeStamp.second;
  }

  return result;
}

uint64_t Field::aggrFuncMin(Roaring *bitmap) {
  int min = INT_MAX;

  if (type != FIELD_TYPE_INT) {
    throw std::runtime_error("only int fields supported by min()");
  }

  roaring_uint32_iterator_t *i = roaring_create_iterator(&bitmap->roaring);

  while(i->has_value) {
    auto curr = storage.ivals[i->current_value - 1];
    min = std::min(min, curr);
    roaring_advance_uint32_iterator(i);
  }

  roaring_free_uint32_iterator(i);

  return (uint64_t) min;
}

uint64_t Field::aggrFuncMax(Roaring *bitmap) {
  int max = INT_MIN;

  if (type != FIELD_TYPE_INT) {
    throw std::runtime_error("only int fields supported by max()");
  }

  roaring_uint32_iterator_t *i = roaring_create_iterator(&bitmap->roaring);

  while(i->has_value) {
    auto curr = storage.ivals[i->current_value - 1];
    max = std::max(max, curr);
    roaring_advance_uint32_iterator(i);
  }

  roaring_free_uint32_iterator(i);

  return (uint64_t) max;
}

uint64_t Field::aggrFuncSum(Roaring *bitmap) {
  uint64_t sum = 0;

  if (type != FIELD_TYPE_INT) {
    throw std::runtime_error("only int fields supported by sum()");
  }

  roaring_uint32_iterator_t *i = roaring_create_iterator(&bitmap->roaring);

  while(i->has_value) {
    sum += storage.ivals[i->current_value - 1];
    roaring_advance_uint32_iterator(i);
  }

  roaring_free_uint32_iterator(i);

  return sum;
}
