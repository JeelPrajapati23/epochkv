#pragma once

#include <sys/types.h>

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "aof.hpp"
#include "store.hpp"

// "Snapshot if at least `changes` writes happened in the last `seconds`".
struct SavePoint {
    int64_t seconds;
    uint64_t changes;
};

// Ties the two persistence mechanisms to the running server:
//
//  - Snapshots: SAVE writes one synchronously; BGSAVE and the save points
//    fork() a child that writes the child's copy-on-write view of memory
//    while the parent keeps serving clients.
//  - AOF: every change to the dataset is logged as a command (propagate())
//    and written out before replies are sent (before_sleep()). BGREWRITEAOF
//    and auto-rewrite compact it, again in a forked child.
//
// At most one child runs at a time. Everything here runs on the event-loop
// thread; the only other thread is the AOF's background fsync.
class Persistence {
public:
    using Args = std::vector<std::string>;
    using Replay = AppendOnlyFile::Replay;

    struct Options {
        std::string dir = ".";
        std::string dbfilename = "dump.snap";
        // Redis's defaults: after 1h if >=1 change, 5min if >=100, 1min if >=10000.
        std::vector<SavePoint> save_points{{3600, 1}, {300, 100}, {60, 10000}};
        bool appendonly = false;
        FsyncPolicy appendfsync = FsyncPolicy::kEverySec;
        // Rewrite once the AOF has grown by this % since the last rewrite
        // and is at least min_size. 0 disables automatic rewrites.
        uint64_t auto_aof_rewrite_percentage = 100;
        uint64_t auto_aof_rewrite_min_size = 64 * 1024 * 1024;
    };

    Persistence(Store& store, Options options);
    ~Persistence();

    Persistence(const Persistence&) = delete;
    Persistence& operator=(const Persistence&) = delete;

    // Startup: rebuild the dataset from the AOF if appendonly is on,
    // otherwise from the snapshot. Commands logged in the AOF are re-executed
    // through `replay`. Returns false if the data on disk can't be trusted.
    bool load(const Replay& replay, std::string& error);
    bool loading() const { return loading_; }

    // Records one change to the dataset, as a command that reproduces it.
    void propagate(const Args& argv);

    // Once per event-loop iteration, before replies are sent.
    void before_sleep();
    // Periodic: reap a finished child, trigger save points / auto-rewrite.
    void cron();
    // Stops any child, then makes everything durable: final AOF fsync, and a
    // final snapshot if save points are configured (Redis does the same).
    bool shutdown();

    // Backends for SAVE, BGSAVE, BGREWRITEAOF and LASTSAVE. Errors come back
    // ready to send as a RESP error ("ERR ...").
    bool save(std::string& error);
    bool bgsave(std::string& error);
    enum class RewriteStart { kStarted, kScheduled, kError };
    RewriteStart bgrewriteaof(std::string& error);
    int64_t lastsave() const { return last_save_unix_; }
    bool child_running() const { return child_pid_ >= 0; }

    // Non-empty while writes must be refused because they can't be
    // persisted (a failed background save, or a failing AOF). Acknowledging
    // writes that will be lost on restart would be worse than refusing them.
    std::string write_error() const;

private:
    enum class ChildKind { kNone, kSnapshot, kAofRewrite };
    using SteadyTime = std::chrono::steady_clock::time_point;

    bool load_snapshot(std::string& error);
    bool start_child(ChildKind kind, std::string& error);
    void reap_child();
    void finish_child(bool ok);
    void kill_child();
    std::string child_temp_path(ChildKind kind, pid_t pid) const;
    bool save_point_reached(SteadyTime now) const;
    bool aof_needs_rewrite(SteadyTime now) const;
    std::string snapshot_path() const;

    Store& store_;
    Options options_;
    std::unique_ptr<AppendOnlyFile> aof_;
    bool loading_ = false;
    bool aof_was_healthy_ = true;

    uint64_t dirty_ = 0;          // changes since the last successful snapshot
    uint64_t dirty_at_fork_ = 0;  // dirty_ when the running snapshot child forked
    int64_t last_save_unix_ = 0;  // seconds, for LASTSAVE
    SteadyTime last_save_{};      // for save-point intervals, immune to clock changes
    bool last_bgsave_ok_ = true;
    SteadyTime last_bgsave_try_{};
    bool last_rewrite_ok_ = true;
    SteadyTime last_rewrite_try_{};
    bool rewrite_scheduled_ = false;

    pid_t child_pid_ = -1;
    ChildKind child_kind_ = ChildKind::kNone;
    SteadyTime child_start_{};
    int64_t child_start_unix_ = 0;
};
