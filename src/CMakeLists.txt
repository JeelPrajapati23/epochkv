add_library(resp_parser resp_parser.cpp)
target_include_directories(resp_parser PUBLIC ${PROJECT_SOURCE_DIR}/include)

add_library(hash_table hash_table.cpp)
target_include_directories(hash_table PUBLIC ${PROJECT_SOURCE_DIR}/include)

add_executable(echo_server echo_server.cpp)
target_include_directories(echo_server PRIVATE ${PROJECT_SOURCE_DIR}/include)
