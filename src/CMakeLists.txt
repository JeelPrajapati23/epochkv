add_library(resp_parser resp_parser.cpp)
target_include_directories(resp_parser PUBLIC ${PROJECT_SOURCE_DIR}/include)

# Header-only (templated on the value type).
add_library(hash_table INTERFACE)
target_include_directories(hash_table INTERFACE ${PROJECT_SOURCE_DIR}/include)

add_library(store store.cpp)
target_link_libraries(store PUBLIC hash_table)

add_library(reply reply.cpp)
target_include_directories(reply PUBLIC ${PROJECT_SOURCE_DIR}/include)

add_library(command_dispatcher command_dispatcher.cpp)
target_link_libraries(command_dispatcher PUBLIC store PRIVATE reply)

add_library(server server.cpp)
target_link_libraries(server PUBLIC resp_parser command_dispatcher PRIVATE reply)

add_executable(kv_server kv_server.cpp)
target_link_libraries(kv_server PRIVATE server)
