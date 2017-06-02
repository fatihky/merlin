cmake_minimum_required(VERSION 3.7)
project(merlin)

set(CMAKE_CXX_STANDARD 11)

include_directories(deps/CRoaring/include deps/CRoaring/cpp)

add_subdirectory("deps/CRoaring")

set(SOURCE_FILES main.cpp query.cpp query.h generic-value.h field.h field-types.h table.h utils.h utils.cpp)
add_executable(merlin ${SOURCE_FILES})
target_link_libraries(merlin roaring)
