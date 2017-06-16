#include <string>
#include <iostream>
#include <map>
#include <vector>
#include <cassert>
#include "roaring.hh"
#include "query.h"
#include "field.h"
#include "query.h"
#include "generic-value.h"
#include "table.h"
#include "query.h"

using namespace std;

void insertRow(Table *table, int64_t timestamp, string referrer, string endpoint, string gender, int responseTime) {
  table->fields["timestamp"]->addValue(GenericValueContainer(timestamp));
  table->fields["endpoint"]->addValue(GenericValueContainer(endpoint));
  table->fields["referrer"]->addValue(GenericValueContainer(referrer));
  table->fields["gender"]->addValue(GenericValueContainer(gender));
  table->fields["responseTime"]->addValue(GenericValueContainer(responseTime));
}

void insertRows(Table *table) {
  for (int i = 0; i < 0; ++i) {
    insertRow(table, i + 1, "google", "/home", "male", rand());
  }
  insertRow(table, 1, "google", "/home", "male", rand());
  insertRow(table, 2, "facebook", "/home", "female", rand());
  insertRow(table, 3, "twitter", "/api", "female", rand());
  insertRow(table, 4, "-none-", "/api", "male", rand());
  insertRow(table, 5, "google", "/home", "male", rand());
  insertRow(table, 6, "bing", "/home", "male", rand());
  insertRow(table, 7, "google", "/home", "female", rand());
//  insertRow(table, 2, "/home", "female", rand());
//  insertRow(table, 2, "/home", "female", rand());
}

int main() {
  std::cout << "Hello, World!" << std::endl;

  const auto table = new Table();
  const auto fieldTimestamp = new Field("timestamp", FIELD_TYPE_TIMESTAMP);
  const auto fieldEndpoint = new Field("endpoint", FIELD_TYPE_STRING);
  const auto fieldGender = new Field("gender", FIELD_TYPE_STRING);
  const auto fieldReferrer = new Field("referrer", FIELD_TYPE_STRING);
  const auto fieldResponseTime = new Field("responseTime", FIELD_TYPE_INT);

  fieldEndpoint->setEncoding(FIELD_ENCODING_DICT);
  fieldGender->setEncoding(FIELD_ENCODING_DICT);
  fieldReferrer->setEncoding(FIELD_ENCODING_DICT);

  table->setField(fieldTimestamp);
  table->setField(fieldEndpoint);
  table->setField(fieldGender);
  table->setField(fieldReferrer);
  table->setField(fieldResponseTime);

  insertRows(table);

  runQuery(table);

  delete fieldTimestamp;
  delete fieldEndpoint;
  delete fieldResponseTime;
  delete table;

  return 0;
}
