#pragma once

#include <cstddef>
#include <string>

// Small POSIX file helpers shared by the snapshot and AOF code.
namespace fileutil {

// write() until all `len` bytes are out, retrying on EINTR. Returns how many
// bytes were written; fewer than `len` means an error, with errno set.
size_t write_all(int fd, const char* data, size_t len);

// fsyncs a directory, which is what makes a create/rename/unlink inside it
// durable. fsyncing a file persists its contents but not its directory
// entry, so without this a rename can be lost on power failure.
bool fsync_dir(const std::string& dir);

// rename() + fsync_dir(): atomically and durably replaces `to` with `from`.
bool durable_rename(const std::string& from, const std::string& to);

std::string join(const std::string& dir, const std::string& name);
std::string dirname(const std::string& path);

}  // namespace fileutil
