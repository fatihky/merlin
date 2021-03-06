cmake_minimum_required(VERSION 3.7)
project(merlin)

set(CMAKE_CXX_STANDARD 11)

# roaring bitmap include dirs
include_directories(deps/CRoaring/include deps/CRoaring/cpp)
include_directories(deps/CRoaring/include deps/CRoaring/include)
# picojson include dirs
include_directories(deps/picojson)
# Simple-Web-Server
include_directories(deps/Simple-Web-Server)

if (APPLE)
  # this dirs found with brew --prefix openssl
  include_directories(/usr/local/opt/openssl/include)
  link_directories(/usr/local/opt/openssl/lib)
endif()
if (UNIX)
  include_directories(/usr/local/include)
  link_directories(/usr/local/lib)
endif()

add_subdirectory("deps/CRoaring")

set(LIBS roaring ssl z boost_system pthread)
set(LIBRARY_SOURCES
  src/query.cpp
  src/query.h
  src/generic-value.h
  src/field.h
  src/field-types.h
  src/table.h
  src/utils.h
  src/utils.cpp
  src/http.h
  src/field.cpp)
set(HTTP_SERVER_SOURCES
  src/http.cpp
  src/http/ping.cpp
  src/http/quit.cpp
  src/http/create-table.cpp
  src/http/show-tables.cpp
  src/http/describe-table.cpp
  src/http/drop-table.cpp
  src/http/insert-into-table.cpp
  src/http/query-table.cpp
  src/http/stats-table.cpp)

add_library(merlin SHARED ${LIBRARY_SOURCES})
add_executable(merlin_http ${HTTP_SERVER_SOURCES})
add_executable(sample src/example/sample.cpp)
target_link_libraries(merlin ${LIBS})
target_link_libraries(merlin_http merlin ${LIBS})
target_link_libraries(sample merlin ${LIBS})
