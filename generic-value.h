//
// Created by Fatih Kaya on 6/1/17.
//

#include <string>
//#include "field.h"
#include "field-types.h"

#ifndef MERLIN_GENERIC_VALUE_H
#define MERLIN_GENERIC_VALUE_H

using namespace std;

class GenericValueContainer {
  public:
  int type;
  int64_t i64Val;
  uint64_t u64Val;
  int iVal;
  string strVal;
  bool bVal;
  GenericValueContainer(int64_t i64Val_): i64Val(i64Val_) { type = FIELD_TYPE_TIMESTAMP; }
  GenericValueContainer(uint64_t u64Val_): u64Val(u64Val_) { type = FIELD_TYPE_BIGINT; }
  GenericValueContainer(int iVal_): iVal(iVal_) { type = FIELD_TYPE_INT; }
  GenericValueContainer(string strVal_): strVal(strVal_) { type = FIELD_TYPE_STRING; }
  GenericValueContainer(bool bVal_): bVal(bVal_) { type = FIELD_TYPE_BOOLEAN; }
  const inline int64_t getInt64Val () const { return i64Val; }
  const inline uint64_t getUInt64Val () const { return u64Val; }
  const inline int getIVal() const { return iVal; }
  const inline string getStrVal() const { return strVal; }
  const inline bool getBVal() const { return bVal; }
};

#endif //MERLIN_GENERIC_VALUE_H
