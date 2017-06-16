//
// Created by Fatih Kaya on 6/1/17.
//

#ifndef MERLIN_FIELD_TYPES_H
#define MERLIN_FIELD_TYPES_H

const int FIELD_TYPE_TIMESTAMP = 1;
const int FIELD_TYPE_INT = 2;
const int FIELD_TYPE_STRING = 3;
const int FIELD_TYPE_BOOLEAN = 4;
const int FIELD_TYPE_BIGINT = 5; // uint64

const int FIELD_ENCODING_NONE = 1;
const int FIELD_ENCODING_DICT = 2;
const int FIELD_ENCODING_MULTI_VAL = 3;

#endif //MERLIN_FIELD_TYPES_H
