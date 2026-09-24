#include "file_util.hpp"

#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <cstdio>

namespace fileutil {

size_t write_all(int fd, const char* data, size_t len) {
    size_t written = 0;
    while (written < len) {
        ssize_t n = write(fd, data + written, len - written);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            break;
        }
        written += static_cast<size_t>(n);
    }
    return written;
}

bool fsync_dir(const std::string& dir) {
    int fd = open(dir.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (fd < 0) {
        return false;
    }
    bool ok = fsync(fd) == 0;
    close(fd);
    return ok;
}

bool durable_rename(const std::string& from, const std::string& to) {
    return std::rename(from.c_str(), to.c_str()) == 0 && fsync_dir(dirname(to));
}

std::string join(const std::string& dir, const std::string& name) {
    if (dir.empty()) {
        return name;
    }
    return dir.back() == '/' ? dir + name : dir + "/" + name;
}

std::string dirname(const std::string& path) {
    size_t slash = path.rfind('/');
    if (slash == std::string::npos) {
        return ".";
    }
    return slash == 0 ? "/" : path.substr(0, slash);
}

}  // namespace fileutil
