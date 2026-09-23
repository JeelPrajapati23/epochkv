add_library(resp_parser resp_parser.cpp)
target_include_directories(resp_parser PUBLIC ${PROJECT_SOURCE_DIR}/include)

add_library(hash_table hash_table.cpp)
target_include_directories(hash_table PUBLIC ${PROJECT_SOURCE_DIR}/include)

add_library(reply reply.cpp)
target_include_directories(reply PUBLIC ${PROJECT_SOURCE_DIR}/include)

add_library(command_dispatcher command_dispatcher.cpp)
target_link_libraries(command_dispatcher PUBLIC hash_table PRIVATE reply)

add_executable(kv_server kv_server.cpp)
target_link_libraries(kv_server PRIVATE resp_parser command_dispatcher reply)
