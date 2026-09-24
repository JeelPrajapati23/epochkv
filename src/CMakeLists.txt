find_package(Threads REQUIRED)

add_library(resp_parser resp_parser.cpp)
target_include_directories(resp_parser PUBLIC ${PROJECT_SOURCE_DIR}/include)

# Header-only (templated on the value type).
add_library(hash_table INTERFACE)
target_include_directories(hash_table INTERFACE ${PROJECT_SOURCE_DIR}/include)

add_library(cluster_slot cluster_slot.cpp)
target_include_directories(cluster_slot PUBLIC ${PROJECT_SOURCE_DIR}/include)

add_library(store store.cpp)
target_link_libraries(store PUBLIC hash_table cluster_slot)

add_library(reply reply.cpp)
target_include_directories(reply PUBLIC ${PROJECT_SOURCE_DIR}/include)

add_library(file_util file_util.cpp)
target_include_directories(file_util PUBLIC ${PROJECT_SOURCE_DIR}/include)

add_library(snapshot snapshot.cpp)
target_link_libraries(snapshot PUBLIC store PRIVATE file_util)

add_library(aof aof.cpp)
target_link_libraries(aof PUBLIC store PRIVATE snapshot resp_parser file_util Threads::Threads)

add_library(persistence persistence.cpp)
target_link_libraries(persistence PUBLIC aof store PRIVATE snapshot file_util)

add_library(repl_backlog repl_backlog.cpp)
target_include_directories(repl_backlog PUBLIC ${PROJECT_SOURCE_DIR}/include)

add_library(replication replication.cpp)
target_link_libraries(replication PUBLIC repl_backlog resp_parser PRIVATE store persistence reply file_util)

add_library(cluster_msg cluster_msg.cpp)
target_link_libraries(cluster_msg PUBLIC cluster_slot PRIVATE reply)

add_library(cluster cluster.cpp)
target_link_libraries(cluster PUBLIC cluster_msg resp_parser PRIVATE store replication reply file_util)

add_library(migrate migrate.cpp)
target_include_directories(migrate PUBLIC ${PROJECT_SOURCE_DIR}/include)

add_library(command_dispatcher command_dispatcher.cpp)
target_link_libraries(command_dispatcher PUBLIC store resp_parser PRIVATE persistence replication reply cluster migrate snapshot)

add_library(server server.cpp)
target_link_libraries(server PUBLIC resp_parser command_dispatcher PRIVATE replication reply)

add_executable(kv_server kv_server.cpp)
target_link_libraries(kv_server PRIVATE server persistence replication cluster file_util)
