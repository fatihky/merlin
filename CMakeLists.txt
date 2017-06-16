cmake_minimum_required(VERSION 3.7)
project(merlin)

set(CMAKE_CXX_STANDARD 11)

# roaring bitmap include dirs
include_directories(deps/CRoaring/include deps/CRoaring/cpp)
# rapidjson include dirs
include_directories(deps/rapidjson/include)

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

set(SOURCE_FILES src/main.cpp src/query.cpp src/query.h src/generic-value.h src/field.h src/field-types.h src/table.h src/utils.h src/utils.cpp src/http.cpp src/http.h src/server.h)
add_executable(merlin ${SOURCE_FILES})
target_link_libraries(merlin roaring)
target_link_libraries(merlin uWS)
target_link_libraries(merlin ssl)
target_link_libraries(merlin z)
target_link_libraries(merlin uv)
