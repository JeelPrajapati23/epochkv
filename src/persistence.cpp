#include "persistence.hpp"

#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <iostream>

#include "file_util.hpp"
#include "snapshot.hpp"

namespace {

// After a failed background save/rewrite, wait this long before an
// automatic retry, so a full disk doesn't turn into a fork() every 100ms.
constexpr std::chrono::seconds kRetryDelay{5};

int64_t unix_seconds() {
    return static_cast<int64_t>(std::time(nullptr));
}

}  // namespace

Persistence::Persistence(Store& store, Options options) : store_(store), options_(std::move(options)) {}

Persistence::~Persistence() {
    kill_child();  // never leave an orphan writing into our directory
}

std::string Persistence::snapshot_path() const {
    return fileutil::join(options_.dir, options_.dbfilename);
}

std::string Persistence::child_temp_path(ChildKind kind, pid_t pid) const {
    if (kind == ChildKind::kAofRewrite) {
        return aof_->temp_base_path(pid);
    }
    return fileutil::join(options_.dir, "temp-" + std::to_string(pid) + ".snap");
}

bool Persistence::load(const Replay& replay, std::string& error) {
    loading_ = true;
    bool ok;
    if (options_.appendonly) {
        aof_ = std::make_unique<AppendOnlyFile>(fileutil::join(options_.dir, "appendonlydir"), options_.appendfsync);
        if (aof_->exists()) {
            ok = aof_->load(store_, replay, error);
        } else {
            // First start with AOF on: seed it from the snapshot, if there is
            // one, so turning AOF on doesn't silently start from empty.
            ok = load_snapshot(error) && aof_->create(store_, error);
        }
    } else {
        ok = load_snapshot(error);
    }
    loading_ = false;
    last_save_ = std::chrono::steady_clock::now();
    last_save_unix_ = unix_seconds();
    return ok;
}

bool Persistence::load_snapshot(std::string& error) {
    return snapshot::load(snapshot_path(), store_, error) != snapshot::LoadResult::kError;
}

void Persistence::propagate(const Args& argv) {
    if (loading_) {
        return;  // replaying the AOF must not append to it
    }
    ++dirty_;
    if (aof_) {
        aof_->feed(argv);
    }
}

void Persistence::before_sleep() {
    if (!aof_) {
        return;
    }
    bool ok = aof_->flush();
    if (!ok && options_.appendfsync == FsyncPolicy::kAlways) {
        // The replies for these writes are queued but unsent, and 'always'
        // promised they'd be on disk first. We can't keep that promise and
        // can't un-execute the commands, so stop rather than acknowledge.
        std::cerr << "fatal: can't write the AOF with appendfsync always (" << aof_->last_error()
                  << "); exiting\n";
        std::exit(1);
    }
    bool healthy = ok && aof_->healthy();
    if (healthy != aof_was_healthy_) {
        if (healthy) {
            std::cerr << "AOF writes are succeeding again; accepting writes\n";
        } else {
            std::cerr << "error writing the AOF (" << aof_->last_error() << "); refusing writes until it recovers\n";
        }
        aof_was_healthy_ = healthy;
    }
}

void Persistence::cron() {
    reap_child();
    if (child_pid_ >= 0) {
        return;
    }
    auto now = std::chrono::steady_clock::now();
    std::string error;
    if (rewrite_scheduled_ || aof_needs_rewrite(now)) {
        rewrite_scheduled_ = false;
        if (!start_child(ChildKind::kAofRewrite, error)) {
            std::cerr << "can't start AOF rewrite: " << error << "\n";
        }
    } else if (save_point_reached(now)) {
        std::cout << dirty_ << " changes since the last save; saving in the background\n";
        if (!start_child(ChildKind::kSnapshot, error)) {
            std::cerr << "can't start background save: " << error << "\n";
        }
    }
}

bool Persistence::save_point_reached(SteadyTime now) const {
    if (!last_bgsave_ok_ && now - last_bgsave_try_ < kRetryDelay) {
        return false;
    }
    auto since_save = std::chrono::duration_cast<std::chrono::seconds>(now - last_save_).count();
    for (const SavePoint& sp : options_.save_points) {
        if (dirty_ >= sp.changes && since_save >= sp.seconds) {
            return true;
        }
    }
    return false;
}

bool Persistence::aof_needs_rewrite(SteadyTime now) const {
    if (!aof_ || options_.auto_aof_rewrite_percentage == 0) {
        return false;
    }
    if (!last_rewrite_ok_ && now - last_rewrite_try_ < kRetryDelay) {
        return false;
    }
    uint64_t size = aof_->current_size();
    uint64_t base = std::max<uint64_t>(aof_->size_after_rewrite(), 1);
    return size >= options_.auto_aof_rewrite_min_size && size > base &&
           (size - base) * 100 / base >= options_.auto_aof_rewrite_percentage;
}

bool Persistence::start_child(ChildKind kind, std::string& error) {
    auto now = std::chrono::steady_clock::now();
    if (kind == ChildKind::kSnapshot) {
        last_bgsave_try_ = now;
    } else {
        last_rewrite_try_ = now;
        if (!aof_->begin_rewrite(error)) {
            last_rewrite_ok_ = false;
            return false;
        }
    }

    // fork() gives the child a copy-on-write view of memory frozen at this
    // instant: a consistent point-in-time snapshot with no locking, while the
    // parent carries on. Pages are only physically copied when the parent
    // writes to them, so the cost is proportional to the write rate during
    // the save, not the dataset size (worst case: 2x memory).
    pid_t pid = fork();
    if (pid < 0) {
        error = std::string("fork: ") + std::strerror(errno);
        (kind == ChildKind::kSnapshot ? last_bgsave_ok_ : last_rewrite_ok_) = false;
        return false;
    }
    if (pid == 0) {
        bool ok = snapshot::write_file(store_, child_temp_path(kind, getpid()));
        // _exit, not exit: no atexit handlers or static destructors, and no
        // second flush of stdio buffers inherited from the parent.
        _exit(ok ? 0 : 1);
    }

    child_pid_ = pid;
    child_kind_ = kind;
    child_start_ = now;
    child_start_unix_ = unix_seconds();
    dirty_at_fork_ = dirty_;
    std::cout << (kind == ChildKind::kSnapshot ? "background save" : "AOF rewrite") << " started by pid " << pid
              << "\n";
    return true;
}

void Persistence::reap_child() {
    if (child_pid_ < 0) {
        return;
    }
    int status = 0;
    pid_t r = waitpid(child_pid_, &status, WNOHANG);
    if (r == 0 || (r < 0 && errno == EINTR)) {
        return;  // still running
    }
    finish_child(r > 0 && WIFEXITED(status) && WEXITSTATUS(status) == 0);
}

void Persistence::finish_child(bool ok) {
    std::string temp = child_temp_path(child_kind_, child_pid_);
    bool snapshot_done = child_kind_ == ChildKind::kSnapshot;
    if (snapshot_done) {
        // The child already fsynced the file; the rename is the commit point.
        if (ok && fileutil::durable_rename(temp, snapshot_path())) {
            // Writes made after the fork aren't in this snapshot: only the
            // ones before it stop counting toward the next save point.
            dirty_ -= dirty_at_fork_;
            last_save_ = child_start_;
            last_save_unix_ = child_start_unix_;
            last_bgsave_ok_ = true;
            std::cout << "background save finished\n";
        } else {
            unlink(temp.c_str());
            last_bgsave_ok_ = false;
            ok = false;
            std::cerr << "background save failed\n";
        }
    } else {
        std::string error;
        last_rewrite_ok_ = aof_->finish_rewrite(ok, temp, error);
        if (last_rewrite_ok_) {
            std::cout << "AOF rewrite finished\n";
        } else {
            std::cerr << "AOF rewrite failed: " << error << "\n";
        }
    }
    child_pid_ = -1;
    child_kind_ = ChildKind::kNone;
    if (snapshot_done && snapshot_listener_) {
        snapshot_listener_(ok);
    }
}

void Persistence::kill_child() {
    if (child_pid_ < 0) {
        return;
    }
    // SIGUSR1 rather than SIGTERM: the child inherited the parent's blocked
    // SIGINT/SIGTERM (see Server::start), and its exit is treated as a
    // failure either way.
    kill(child_pid_, SIGUSR1);
    while (waitpid(child_pid_, nullptr, 0) < 0 && errno == EINTR) {
    }
    finish_child(false);
}

bool Persistence::replace_dataset(const std::string& snapshot_file, std::string& error) {
    kill_child();  // whatever it's writing describes the old dataset
    loading_ = true;
    store_.clear();
    bool ok = snapshot::load(snapshot_file, store_, error) == snapshot::LoadResult::kOk;
    loading_ = false;
    if (!ok) {
        unlink(snapshot_file.c_str());
        return false;
    }
    // The received file is a valid snapshot of exactly this dataset: keep
    // it as ours instead of writing the same bytes again.
    if (fileutil::durable_rename(snapshot_file, snapshot_path())) {
        dirty_ = 0;
        last_save_ = std::chrono::steady_clock::now();
        last_save_unix_ = unix_seconds();
        last_bgsave_ok_ = true;
    } else {
        std::cerr << "can't install the received snapshot: " << std::strerror(errno) << "\n";
        unlink(snapshot_file.c_str());
    }
    // An AOF left describing the old dataset would resurrect it on restart.
    if (aof_ && !aof_->reset(store_, error)) {
        error = "restarting the AOF: " + error;
        return false;
    }
    return true;
}

bool Persistence::shutdown() {
    kill_child();
    bool ok = true;
    if (aof_ && !aof_->flush_and_sync()) {
        std::cerr << "error flushing the AOF on shutdown: " << aof_->last_error() << "\n";
        ok = false;
    }
    if (!options_.save_points.empty()) {
        std::string error;
        if (save(error)) {
            std::cout << "snapshot saved on shutdown\n";
        } else {
            std::cerr << error << "\n";
            ok = false;
        }
    }
    return ok;
}

bool Persistence::save(std::string& error) {
    if (child_kind_ == ChildKind::kSnapshot) {
        error = "ERR Background save already in progress";
        return false;
    }
    if (!snapshot::save(store_, snapshot_path())) {
        error = "ERR error saving the snapshot: " + std::string(std::strerror(errno));
        return false;
    }
    dirty_ = 0;
    last_save_ = std::chrono::steady_clock::now();
    last_save_unix_ = unix_seconds();
    last_bgsave_ok_ = true;
    return true;
}

bool Persistence::bgsave(std::string& error) {
    if (child_pid_ >= 0) {
        error = child_kind_ == ChildKind::kSnapshot
                    ? "ERR Background save already in progress"
                    : "ERR Another child process is active (AOF rewrite): can't BGSAVE right now";
        return false;
    }
    if (!start_child(ChildKind::kSnapshot, error)) {
        error = "ERR " + error;
        return false;
    }
    return true;
}

Persistence::RewriteStart Persistence::bgrewriteaof(std::string& error) {
    if (!aof_) {
        error = "ERR AOF is disabled (start the server with --appendonly yes)";
        return RewriteStart::kError;
    }
    if (child_kind_ == ChildKind::kAofRewrite) {
        error = "ERR Background append only file rewriting already in progress";
        return RewriteStart::kError;
    }
    if (child_pid_ >= 0) {
        rewrite_scheduled_ = true;  // starts from cron() once the snapshot child exits
        return RewriteStart::kScheduled;
    }
    if (!start_child(ChildKind::kAofRewrite, error)) {
        error = "ERR " + error;
        return RewriteStart::kError;
    }
    return RewriteStart::kStarted;
}

std::string Persistence::write_error() const {
    if (!options_.save_points.empty() && !last_bgsave_ok_) {
        return "MISCONF Snapshotting is configured but the last background save failed; "
               "writes are refused until a save succeeds. Check the server log.";
    }
    if (aof_ && !aof_->healthy()) {
        return "MISCONF Errors writing to the AOF file: " + aof_->last_error();
    }
    return "";
}
