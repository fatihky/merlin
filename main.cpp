#include <string>
#include <iostream>
#include <map>
#include <vector>
#include <cassert>
#include "deps/CRoaring/cpp/roaring.hh"

using namespace std;

const int FIELD_TYPE_TIMESTAMP = 1;
const int FIELD_TYPE_INT = 2;
const int FIELD_TYPE_STRING = 3;
const int FIELD_TYPE_BOOLEAN = 4;

const int FIELD_ENCODING_NONE = 1;
const int FIELD_ENCODING_DICT = 2;
const int FIELD_ENCODING_MULTI_VAL = 3;

class GenericValueContainer {
  public:
  int type;
  int64_t i64Val;
  int iVal;
  string strVal;
  bool bVal;
  GenericValueContainer(int64_t i64Val_): i64Val(i64Val_) { type = FIELD_TYPE_TIMESTAMP; }
  GenericValueContainer(int iVal_): iVal(iVal_) { type = FIELD_TYPE_INT; }
  GenericValueContainer(string strVal_): strVal(strVal_) { type = FIELD_TYPE_STRING; }
  GenericValueContainer(bool bVal_): bVal(bVal_) { type = FIELD_TYPE_BOOLEAN; }
  const inline int64_t getInt64Val () const { return i64Val; }
  const inline int getIVal() const { return iVal; }
  const inline string getStrVal() const { return strVal; }
  const inline bool getBVal() const { return bVal; }
};

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

            if (storage.strval.dict.dict.count(key)) {
              storage.strval.dict.dict[key]->add((uint32_t) size);
            } else {
              storage.strval.dict.dict[key] = new Roaring();
              storage.strval.dict.dict[key]->add((uint32_t) size);
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
    }

    size++;
  }
};

class Table {
  public:
  map<string, Field *> fields;

  void setField(Field *field) {
    fields[field->name] = field;
  }
};

void insertRow(Table *table, int timestamp, string endpoint, int responseTime) {
  table->fields["timestamp"]->addValue(GenericValueContainer((int64_t) timestamp));
  table->fields["endpoint"]->addValue(GenericValueContainer(endpoint));
  table->fields["responseTime"]->addValue(GenericValueContainer(responseTime));
}

void insertRows(Table *table) {
  for (int i = 0; i < 1; ++i) {
    insertRow(table, i + 1, "/home", rand());
  }
}

int main() {
  std::cout << "Hello, World!" << std::endl;

  const auto table = new Table();
  const auto fieldTimestamp = new Field("timestamp", FIELD_TYPE_TIMESTAMP);
  const auto fieldEndpoint = new Field("endpoint", FIELD_TYPE_STRING);
  const auto fieldResponseTime = new Field("responseTime", FIELD_TYPE_TIMESTAMP);

  fieldEndpoint->setEncoding(FIELD_ENCODING_DICT);

  table->setField(fieldTimestamp);
  table->setField(fieldEndpoint);
  table->setField(fieldResponseTime);

  insertRows(table);

  delete fieldTimestamp;
  delete fieldEndpoint;
  delete fieldResponseTime;
  delete table;

  return 0;
}
