add_executable(kv_store_tests test_sanity.cpp)
target_link_libraries(kv_store_tests PRIVATE Catch2::Catch2WithMain)
target_include_directories(kv_store_tests PRIVATE ${PROJECT_SOURCE_DIR}/include)

include(Catch)
catch_discover_tests(kv_store_tests)
