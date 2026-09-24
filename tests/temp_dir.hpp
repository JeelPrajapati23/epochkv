#pragma once

#include <cstdlib>
#include <filesystem>
#include <stdexcept>
#include <string>

// A fresh directory under /tmp, removed with everything in it on destruction.
struct TempDir {
    std::string path;

    TempDir() {
        char pattern[] = "/tmp/kv_store_test_XXXXXX";
        if (mkdtemp(pattern) == nullptr) {
            throw std::runtime_error("mkdtemp failed");
        }
        path = pattern;
    }

    ~TempDir() {
        std::error_code ignored;
        std::filesystem::remove_all(path, ignored);
    }

    TempDir(const TempDir&) = delete;
    TempDir& operator=(const TempDir&) = delete;

    std::string file(const std::string& name) const { return path + "/" + name; }
};
