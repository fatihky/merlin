#include "../http.h"

void commandCreateTable(picojson::object &req, picojson::object &res) {
  picojson::array tableNames;
  string tableName;
  const auto fields = req["fields"];
  string err;
  const auto table = new Table();

  if (!req["name"].is<string>()) {
    err = "table name is required";
    goto error;
  }

  tableName = req["name"].to_str();

  if (httpServer.tables.count(tableName) == 1) {
    err = "table already exist";
    goto error;
  }

  if (!fields.is<picojson::array>()) {
    err = "fields prop is required.";
    goto error;
  }

  for (auto &&field : fields.get<picojson::array>()) {
    if (!field.is<picojson::object>()) {
      err = "field must be an object";
      goto error;
    }

    auto obj = field.get<picojson::object>();
    string name;
    string type;
    string encoding;

    if (!obj["name"].is<string>()) {
      err = "name prop required in field definitions";
      goto error;
    }

    name = obj["name"].to_str();

    if (!obj["type"].is<string>()) {
      err = "type prop required in field definitions";
      goto error;
    }

    type = obj["type"].to_str();

    if (fieldTypes.count(type) == 0) {
      err = "invalid field type: " + type;
      goto error;
    }

    if (obj["encoding"].is<string>()) {
      encoding = obj["encoding"].to_str();

      if (encodingTypes.count(encoding) == 0) {
        err = "invalid encoding type: " + encoding;
        goto error;
      }

      if (encoding == "dict" && type != "string") {
        err = "dict encoding only usable with string fields at the moment";
        goto error;
      }
    }

    const auto mField = new Field(name, fieldTypes[type]);

    if (!encoding.empty()) {
      mField->encoding = encodingTypes[encoding];
    }

    table->setField(mField);
  }

  httpServer.tables[tableName] = table;

  res["created"] = picojson::value(true);

  return;

  error:

  delete table;

  return setError(res, err);
}
