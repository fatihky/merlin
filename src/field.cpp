#include <iostream>
#include "roaring/containers/containers.h"
#include "roaring/containers/array.h"
#include "field.h"

using namespace std;

struct aggr_func_date_seconds_group_data {
  int64_t seconds;
  map<int64_t, roaring_bitmap_t *> &result;
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
            roaring_bitmap_add(storage.strval.dict.dict[key], index);
          } else {
            storage.strval.dict.dict[key] = roaring_bitmap_create();
            roaring_bitmap_add(storage.strval.dict.dict[key], index);
          }
        } break;

        default: throw std::runtime_error("unknown string encoding");
      }
    } break;
    case FIELD_TYPE_BOOLEAN: {
      if (genericValueContainer.bVal) {
        roaring_bitmap_add(storage.bvals, (uint32_t) size + 1);
      }
    } break;
    default: throw std::runtime_error("unknown field type");
  }

  size++;
}

roaring_bitmap_t *Field::getBitmap(string op, string value) {
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

map<string, roaring_bitmap_t *> Field::genGroups(roaring_bitmap_t *initialBitmap) {
  map<string, roaring_bitmap_t *> result;

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
            roaring_bitmap_t *bitmap;
            if (initialBitmap != nullptr) {
              bitmap = roaring_bitmap_and(val.second, initialBitmap);
            } else {
              bitmap = roaring_bitmap_copy(val.second);
            }
            if (roaring_bitmap_get_cardinality(bitmap) == 0) {
              roaring_bitmap_free(bitmap);
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

  return std::move(result);
};

static bool aggrFuncDateSecondsGroup(uint32_t value, void *data) {
  const auto *ctx = (aggr_func_date_seconds_group_data *) data;
  const auto timestamp = ctx->field->storage.timestamps[value - 1];
  //  const int64_t group = timestamp - (int64_t) fastrange64((uint64_t) timestamp, (uint64_t) ctx->seconds);
  const int64_t group = timestamp - (timestamp % ctx->seconds);
  roaring_bitmap_t *roar = nullptr;

  if (ctx->result.count(group) == 1) {
    roar = ctx->result[group];
  } else {
    roar = roaring_bitmap_create();
    ctx->result[group] = roar;
  }

  roaring_bitmap_add(roar, value);
  return true;
}

map<string, roaring_bitmap_t *> Field::genGroups(roaring_bitmap_t *initialBitmap, string func, vector<string> funcArgs) {
  map<string, roaring_bitmap_t *> result;
  map<int64_t, roaring_bitmap_t *> intResult;

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

  roaring_iterate(initialBitmap, aggrFuncDateSecondsGroup, (void *) &aggrResult);

  for (auto &&timeStamp : aggrResult.result) {
    result[to_string(timeStamp.first)] = timeStamp.second;
  }

  return std::move(result);
}

uint64_t Field::aggrFuncMin(roaring_bitmap_t *bitmap) {
  int min = INT_MAX;

  if (type != FIELD_TYPE_INT) {
    throw std::runtime_error("only int fields supported by min()");
  }

  roaring_uint32_iterator_t *i = roaring_create_iterator(bitmap);

  while(i->has_value) {
    auto curr = storage.ivals[i->current_value - 1];
    min = std::min(min, curr);
    roaring_advance_uint32_iterator(i);
  }

  roaring_free_uint32_iterator(i);

  return (uint64_t) min;
}

uint64_t Field::aggrFuncMax(roaring_bitmap_t *bitmap) {
  int max = INT_MIN;

  if (type != FIELD_TYPE_INT) {
    throw std::runtime_error("only int fields supported by max()");
  }

  roaring_uint32_iterator_t *i = roaring_create_iterator(bitmap);

  while(i->has_value) {
    auto curr = storage.ivals[i->current_value - 1];
    max = std::max(max, curr);
    roaring_advance_uint32_iterator(i);
  }

  roaring_free_uint32_iterator(i);

  return (uint64_t) max;
}

uint64_t Field::aggrFuncSum(roaring_bitmap_t *bitmap) {
  uint64_t sum = 0;

  if (type != FIELD_TYPE_INT) {
    throw std::runtime_error("only int fields supported by sum()");
  }

  roaring_uint32_iterator_t *i = roaring_create_iterator(bitmap);

  while(i->has_value) {
    sum += storage.ivals[i->current_value - 1];
    roaring_advance_uint32_iterator(i);
  }

  roaring_free_uint32_iterator(i);

  return sum;
}

int64_t Field::statUsedMemory() {
  switch (type) {
    case FIELD_TYPE_TIMESTAMP: {
      return sizeof(int64_t) * storage.timestamps.capacity();
    }
    case FIELD_TYPE_INT: {
      return sizeof(int) * storage.ivals.capacity();
    }
    case FIELD_TYPE_STRING: {
      switch (encoding) {
        case FIELD_ENCODING_DICT: {
          int sum = 0;
          for (auto &&it : storage.strval.dict.dict) {
            sum += it.first.size();
            // I don't know how much bytes taken by each record
            // so I am just adding 10
            sum += 10;
            // roaring bitmap does not give used memory.
            // I will try to calculate it by hand
            auto r = it.second;
            for (int i = 0; i < r->high_low_container.size; i++) {
              uint8_t typecode;
              void *cont = ra_get_container_at_index(&r->high_low_container, i, &typecode);
              switch (typecode) {
                case BITSET_CONTAINER_TYPE_CODE: {
                  sum += sizeof(bitset_container_t);
                  sum += sizeof(uint64_t) * BITSET_CONTAINER_SIZE_IN_WORDS;
                } break;
                case ARRAY_CONTAINER_TYPE_CODE: {
                  sum += sizeof(array_container_t);
                  sum += sizeof(uint16_t) * ((array_container_t *)cont)->capacity;
                } break;
                case RUN_CONTAINER_TYPE_CODE: {
                  sum += sizeof(run_container_t);
                  sum += sizeof(rle16_t) * ((run_container_t *)cont)->capacity;
                } break;
                default:
                  cout << "Field::statUsedMemory(): skipping unknown container type: " << typecode << endl;
              }
            }
          }
          return sum;
        }
        default:
          throw std::runtime_error("statUsedMemory not implemented for string field encoding " + to_string(encoding));
      }
    }
    default:
      throw std::runtime_error("statUsedMemory not implemented for field type " + to_string(type));
  }
}