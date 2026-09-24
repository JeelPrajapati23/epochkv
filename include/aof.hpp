#pragma once

#include <sys/types.h>

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "store.hpp"

enum class FsyncPolicy {
    kAlways,    // fsync before any reply goes out: an acknowledged write is on disk
    kEverySec,  // fsync once a second on a background thread: lose at most ~1-2s
    kNo,        // never fsync; the kernel flushes dirty pages on its own (~30s)
};

// Append-only file in the multi-part layout Redis 7 uses, kept in its own
// directory:
//
//   appendonly.aof.manifest        which files make up the AOF, in replay order
//   appendonly.aof.<n>.base.snap   snapshot of the dataset as of the last rewrite
//   appendonly.aof.<n>.incr.aof    RESP-encoded write commands since then
//
// Loading = load the base, then replay each incr. A rewrite (compaction)
// only has to produce a new base: writes that arrive while it runs go to a
// fresh incr file, so nothing has to be buffered in memory and merged in
// afterwards (the pre-Redis-7 design).
class AppendOnlyFile {
public:
    using Args = std::vector<std::string>;
    using Replay = std::function<void(const Args&)>;

    AppendOnlyFile(std::string dir, FsyncPolicy policy);
    ~AppendOnlyFile();

    AppendOnlyFile(const AppendOnlyFile&) = delete;
    AppendOnlyFile& operator=(const AppendOnlyFile&) = delete;

    // True if a manifest exists, i.e. there is an AOF to load.
    bool exists() const;

    // Loads the base into `store` and feeds every logged command to `replay`.
    // A command cut off at the very end of the last incr (crash mid-write) is
    // truncated away with a warning; damage anywhere else is an error, since
    // skipping it would silently drop or reorder writes. On success the last
    // incr stays open for appending.
    bool load(Store& store, const Replay& replay, std::string& error);

    // Starts a fresh AOF whose base is the current contents of `store`.
    bool create(const Store& store, std::string& error);

    // Buffers one command. Nothing reaches the file until flush().
    void feed(const Args& argv);

    // Writes the buffer to the current incr file and fsyncs per the policy.
    // The server calls this once per event-loop iteration, before sending
    // any replies, so a client never sees OK for a write the AOF doesn't
    // have. On a write error the unwritten data is kept for a retry.
    bool flush();
    // flush() plus a synchronous fsync whatever the policy, e.g. for shutdown.
    bool flush_and_sync();

    // Rewrite, step 1 (parent, just before fork): route new writes to a new
    // incr file and record it in the manifest. The forked child then writes
    // a snapshot to temp_base_path(child_pid).
    bool begin_rewrite(std::string& error);
    // Step 2, after the child exits: on success install its snapshot as the
    // new base and delete the files it supersedes; on failure just discard
    // it (the manifest then lists several incrs, which replay fine).
    bool finish_rewrite(bool child_ok, const std::string& temp_base, std::string& error);
    std::string temp_base_path(pid_t child_pid) const;

    // False after a failed write or background fsync, until one succeeds.
    bool healthy() const;
    std::string last_error() const;

    // Bytes on disk (base + incrs), and the same figure right after the last
    // rewrite or load; auto-rewrite compares the two.
    uint64_t current_size() const { return base_bytes_ + older_incr_bytes_ + incr_bytes_; }
    uint64_t size_after_rewrite() const { return size_after_rewrite_; }

private:
    struct FileEntry {
        std::string name;
        uint64_t seq;
        bool is_base;
    };
    class FsyncWorker;

    std::string path(const std::string& name) const;
    bool read_manifest(std::string& error);
    bool persist_manifest(std::string& error);
    bool replay_incr(const std::string& file, bool is_last, const Replay& replay, uint64_t& valid_bytes,
                     std::string& error);

    std::string dir_;
    FsyncPolicy policy_;
    std::unique_ptr<FsyncWorker> fsync_worker_;

    std::optional<FileEntry> base_;
    std::vector<FileEntry> incrs_;  // the last one is the file being appended to
    uint64_t next_seq_ = 1;

    int fd_ = -1;  // current incr, opened O_APPEND
    std::string buf_;
    int write_errno_ = 0;
    bool unsynced_ = false;
    std::chrono::steady_clock::time_point last_fsync_{};

    uint64_t base_bytes_ = 0;
    uint64_t older_incr_bytes_ = 0;  // incrs before the current one
    uint64_t incr_bytes_ = 0;        // current incr
    uint64_t size_after_rewrite_ = 0;
};
