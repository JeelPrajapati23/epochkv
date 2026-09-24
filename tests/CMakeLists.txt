add_executable(kv_store_tests
  test_sanity.cpp
  test_resp_parser.cpp
  test_hash_table.cpp
  test_reply.cpp
  test_command_dispatcher.cpp
  test_store.cpp
  test_snapshot.cpp
  test_persistence.cpp
  test_repl_backlog.cpp
  test_cluster.cpp)
target_link_libraries(kv_store_tests PRIVATE
  Catch2::Catch2WithMain resp_parser hash_table reply command_dispatcher store snapshot persistence
  repl_backlog replication cluster cluster_msg cluster_slot)
target_include_directories(kv_store_tests PRIVATE ${PROJECT_SOURCE_DIR}/include)

include(Catch)
catch_discover_tests(kv_store_tests)
