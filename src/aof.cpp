#include "aof.hpp"

#include <fcntl.h>
#include <pthread.h>
#include <sys/stat.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <csignal>
#include <condition_variable>
#include <cstring>
#include <fstream>
#include <iostream>
#include <mutex>
#include <sstream>
#include <thread>

#include "file_util.hpp"
#include "resp_parser.hpp"
#include "snapshot.hpp"

namespace {

constexpr const char* kManifestName = "appendonly.aof.manifest";
constexpr std::chrono::seconds kEverySecInterval{1};
// RespParser erases each parsed command from the front of its buffer, so
// that cost scales with how much is buffered: feed it small chunks.
constexpr size_t kLoadChunkSize = 4096;

std::string base_name(uint64_t seq) {
    return "appendonly.aof." + std::to_string(seq) + ".base.snap";
}

std::string incr_name(uint64_t seq) {
    return "appendonly.aof." + std::to_string(seq) + ".incr.aof";
}

// Same RESP array encoding clients send, so loading reuses the network parser.
void append_command(std::string& out, const std::vector<std::string>& argv) {
    out += '*';
    out += std::to_string(argv.size());
    out += "\r\n";
    for (const std::string& arg : argv) {
        out += '$';
        out += std::to_string(arg.size());
        out += "\r\n";
        out += arg;
        out += "\r\n";
    }
}

uint64_t file_size(const std::string& path) {
    struct stat st;
    return stat(path.c_str(), &st) == 0 ? static_cast<uint64_t>(st.st_size) : 0;
}

}  // namespace

// everysec's fsync runs here, off the event loop: fsync can block for
// hundreds of milliseconds on a busy disk, and every client would stall
// with it. At most one fsync is in flight; the loop skips a round rather
// than queueing up behind a slow disk.
class AppendOnlyFile::FsyncWorker {
public:
    // The thread must never receive signals: the server handles SIGINT/SIGTERM
    // through a signalfd, which only works if *every* thread blocks them — a
    // process-directed signal goes to any thread that doesn't, and the
    // default action kills the whole process. A new thread inherits its
    // creator's mask, so block everything around the spawn (setting the mask
    // inside the thread would leave a window before it runs).
    FsyncWorker() {
        sigset_t all;
        sigset_t previous;
        sigfillset(&all);
        pthread_sigmask(SIG_BLOCK, &all, &previous);
        thread_ = std::thread([this] { run(); });
        pthread_sigmask(SIG_SETMASK, &previous, nullptr);
    }

    ~FsyncWorker() {
        {
            std::lock_guard<std::mutex> lock(mu_);
            stop_ = true;
        }
        cv_.notify_all();
        thread_.join();
    }

    // Starts an fsync of `fd` unless one is already running.
    bool try_submit(int fd) {
        std::lock_guard<std::mutex> lock(mu_);
        if (fd_ >= 0) {
            return false;
        }
        fd_ = fd;
        cv_.notify_all();
        return true;
    }

    // Blocks until no fsync is running — required before closing the fd.
    void wait_idle() {
        std::unique_lock<std::mutex> lock(mu_);
        cv_.wait(lock, [this] { return fd_ < 0; });
    }

    int last_errno() const { return last_errno_.load(); }

private:
    void run() {
        std::unique_lock<std::mutex> lock(mu_);
        while (true) {
            cv_.wait(lock, [this] { return stop_ || fd_ >= 0; });
            if (fd_ < 0) {
                return;  // stopping, nothing pending
            }
            int fd = fd_;
            lock.unlock();
            int rc = fdatasync(fd);
            last_errno_ = rc == 0 ? 0 : errno;
            lock.lock();
            fd_ = -1;
            cv_.notify_all();
        }
    }

    std::mutex mu_;
    std::condition_variable cv_;
    int fd_ = -1;
    bool stop_ = false;
    std::atomic<int> last_errno_{0};
    std::thread thread_;
};

AppendOnlyFile::AppendOnlyFile(std::string dir, FsyncPolicy policy) : dir_(std::move(dir)), policy_(policy) {
    if (policy_ == FsyncPolicy::kEverySec) {
        fsync_worker_ = std::make_unique<FsyncWorker>();
    }
}

AppendOnlyFile::~AppendOnlyFile() {
    fsync_worker_.reset();  // joins, so no fsync is still using fd_
    if (fd_ >= 0) {
        close(fd_);
    }
}

std::string AppendOnlyFile::path(const std::string& name) const {
    return fileutil::join(dir_, name);
}

std::string AppendOnlyFile::temp_base_path(pid_t child_pid) const {
    return path("temp-rewrite-" + std::to_string(child_pid) + ".snap");
}

bool AppendOnlyFile::exists() const {
    return access(path(kManifestName).c_str(), F_OK) == 0;
}

// One line per file:  file <name> seq <n> type <b|i>  (Redis's manifest format).
bool AppendOnlyFile::read_manifest(std::string& error) {
    std::ifstream in(path(kManifestName));
    if (!in) {
        error = path(kManifestName) + ": " + std::strerror(errno);
        return false;
    }
    std::string line;
    while (std::getline(in, line)) {
        if (line.empty()) {
            continue;
        }
        std::istringstream fields(line);
        std::string file_kw, name, seq_kw, type_kw, type;
        uint64_t seq;
        if (!(fields >> file_kw >> name >> seq_kw >> seq >> type_kw >> type) || file_kw != "file" ||
            seq_kw != "seq" || type_kw != "type" || (type != "b" && type != "i") ||
            name.find('/') != std::string::npos) {
            error = "malformed AOF manifest line: " + line;
            return false;
        }
        FileEntry entry{name, seq, type == "b"};
        if (entry.is_base) {
            if (base_ || !incrs_.empty()) {
                error = "AOF manifest: the base file must be listed once, first";
                return false;
            }
            base_ = entry;
        } else {
            incrs_.push_back(entry);
        }
        next_seq_ = std::max(next_seq_, seq + 1);
    }
    if (incrs_.empty()) {
        error = "AOF manifest lists no incr file";
        return false;
    }
    return true;
}

// Write to a temp file, fsync, rename over the old manifest, fsync the
// directory: readers see either the old manifest or the new one, never half.
bool AppendOnlyFile::persist_manifest(std::string& error) {
    std::string content;
    auto add = [&](const FileEntry& f) {
        content += "file " + f.name + " seq " + std::to_string(f.seq) + " type " + (f.is_base ? "b" : "i") + "\n";
    };
    if (base_) {
        add(*base_);
    }
    for (const FileEntry& f : incrs_) {
        add(f);
    }

    std::string temp = path(std::string(kManifestName) + ".tmp");
    int fd = open(temp.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    bool ok = fd >= 0 && fileutil::write_all(fd, content.data(), content.size()) == content.size() &&
              fsync(fd) == 0;
    if (fd >= 0) {
        int saved_errno = errno;
        close(fd);
        errno = saved_errno;
    }
    ok = ok && fileutil::durable_rename(temp, path(kManifestName));
    if (!ok) {
        error = "writing the AOF manifest: " + std::string(std::strerror(errno));
        unlink(temp.c_str());
    }
    return ok;
}

bool AppendOnlyFile::create(const Store& store, std::string& error) {
    if (mkdir(dir_.c_str(), 0755) < 0 && errno != EEXIST) {
        error = dir_ + ": " + std::strerror(errno);
        return false;
    }
    FileEntry base{base_name(next_seq_), next_seq_, true};
    FileEntry incr{incr_name(next_seq_ + 1), next_seq_ + 1, false};

    if (!snapshot::save(store, path(base.name))) {
        error = "writing the AOF base: " + std::string(std::strerror(errno));
        return false;
    }
    int fd = open(path(incr.name).c_str(), O_WRONLY | O_APPEND | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    if (fd < 0) {
        error = path(incr.name) + ": " + std::strerror(errno);
        return false;
    }
    base_ = base;
    incrs_ = {incr};
    if (!persist_manifest(error)) {
        close(fd);
        return false;
    }
    next_seq_ += 2;
    fd_ = fd;
    base_bytes_ = file_size(path(base.name));
    older_incr_bytes_ = incr_bytes_ = 0;
    size_after_rewrite_ = current_size();
    return true;
}

bool AppendOnlyFile::load(Store& store, const Replay& replay, std::string& error) {
    if (!read_manifest(error)) {
        return false;
    }
    if (base_) {
        std::string base_path = path(base_->name);
        switch (snapshot::load(base_path, store, error)) {
            case snapshot::LoadResult::kOk:
                break;
            case snapshot::LoadResult::kNotFound:
                error = base_path + ": listed in the manifest but missing";
                return false;
            case snapshot::LoadResult::kError:
                return false;
        }
        base_bytes_ = file_size(base_path);
    }
    for (size_t i = 0; i < incrs_.size(); ++i) {
        bool is_last = i + 1 == incrs_.size();
        uint64_t bytes;
        if (!replay_incr(incrs_[i].name, is_last, replay, bytes, error)) {
            return false;
        }
        (is_last ? incr_bytes_ : older_incr_bytes_) += bytes;
    }

    fd_ = open(path(incrs_.back().name).c_str(), O_WRONLY | O_APPEND | O_CLOEXEC);
    if (fd_ < 0) {
        error = path(incrs_.back().name) + ": " + std::strerror(errno);
        return false;
    }
    size_after_rewrite_ = current_size();
    return true;
}

bool AppendOnlyFile::replay_incr(const std::string& file, bool is_last, const Replay& replay,
                                 uint64_t& valid_bytes, std::string& error) {
    std::string file_path = path(file);
    int fd = open(file_path.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd < 0) {
        error = file_path + ": " + std::strerror(errno);
        return false;
    }

    RespParser parser;
    uint64_t fed = 0;
    valid_bytes = 0;  // length of the prefix made of complete commands
    char buf[kLoadChunkSize];
    bool ok = true;
    while (ok) {
        ssize_t n = read(fd, buf, sizeof(buf));
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            error = file_path + ": " + std::strerror(errno);
            ok = false;
            break;
        }
        if (n == 0) {
            break;
        }
        parser.feed(buf, static_cast<size_t>(n));
        fed += static_cast<uint64_t>(n);
        while (true) {
            RespParser::ParseResult result = parser.try_parse_command();
            if (result.status == RespParser::Status::kIncomplete) {
                break;
            }
            if (result.status == RespParser::Status::kProtocolError) {
                error = file_path + ": corrupt command at byte offset " + std::to_string(valid_bytes);
                ok = false;
                break;
            }
            valid_bytes = fed - parser.buffered_bytes();
            replay(result.command);
        }
    }
    close(fd);
    if (!ok) {
        return false;
    }

    if (parser.buffered_bytes() > 0) {
        // Only the file being appended to at the time of a crash can end in a
        // half-written command. Anywhere else it means real damage.
        if (!is_last) {
            error = file_path + ": truncated command at byte offset " + std::to_string(valid_bytes);
            return false;
        }
        std::cerr << "warning: " << file_path << " ends in an incomplete command (crash mid-write?); "
                  << "truncating " << (fed - valid_bytes) << " bytes at offset " << valid_bytes << "\n";
        if (truncate(file_path.c_str(), static_cast<off_t>(valid_bytes)) < 0) {
            error = file_path + ": truncate: " + std::strerror(errno);
            return false;
        }
    }
    return true;
}

void AppendOnlyFile::feed(const Args& argv) {
    append_command(buf_, argv);
}

bool AppendOnlyFile::flush() {
    if (!buf_.empty()) {
        size_t written = fileutil::write_all(fd_, buf_.data(), buf_.size());
        if (written < buf_.size()) {
            write_errno_ = errno;
            // Don't leave half a command in the file: the retry would append
            // after it and replay would misparse from there on. Cut the
            // partial write back off; if even that fails, keep it and retry
            // only the remainder, so the file stays a clean prefix of the log.
            if (written > 0 && ftruncate(fd_, static_cast<off_t>(incr_bytes_)) < 0) {
                incr_bytes_ += written;
                buf_.erase(0, written);
            }
            return false;
        }
        incr_bytes_ += written;
        buf_.clear();
        write_errno_ = 0;
        unsynced_ = true;

        if (policy_ == FsyncPolicy::kAlways) {
            // One fsync covers every write command from this loop iteration,
            // across all clients (group commit).
            if (fdatasync(fd_) < 0) {
                write_errno_ = errno;
                return false;
            }
            unsynced_ = false;
        }
    }

    if (policy_ == FsyncPolicy::kEverySec && unsynced_) {
        auto now = std::chrono::steady_clock::now();
        if (now - last_fsync_ >= kEverySecInterval && fsync_worker_->try_submit(fd_)) {
            unsynced_ = false;
            last_fsync_ = now;
        }
    }
    return true;
}

bool AppendOnlyFile::flush_and_sync() {
    if (!flush()) {
        return false;
    }
    if (fsync_worker_) {
        fsync_worker_->wait_idle();
    }
    if (fdatasync(fd_) < 0) {
        write_errno_ = errno;
        return false;
    }
    unsynced_ = false;
    return true;
}

bool AppendOnlyFile::begin_rewrite(std::string& error) {
    // The old incr must be fully on disk before anything lands in the new
    // one; otherwise a crash could keep a later write and lose an earlier one.
    if (!flush_and_sync()) {
        error = "flushing the AOF before rewrite: " + last_error();
        return false;
    }
    FileEntry incr{incr_name(next_seq_), next_seq_, false};
    int fd = open(path(incr.name).c_str(), O_WRONLY | O_APPEND | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    if (fd < 0) {
        error = path(incr.name) + ": " + std::strerror(errno);
        return false;
    }
    // Recorded in the manifest *before* any write goes to it, so a crash
    // during the rewrite still replays the writes made after the fork.
    incrs_.push_back(incr);
    if (!persist_manifest(error)) {
        incrs_.pop_back();
        close(fd);
        unlink(path(incr.name).c_str());
        return false;
    }
    ++next_seq_;
    close(fd_);
    fd_ = fd;
    older_incr_bytes_ += incr_bytes_;
    incr_bytes_ = 0;
    return true;
}

bool AppendOnlyFile::finish_rewrite(bool child_ok, const std::string& temp_base, std::string& error) {
    if (!child_ok) {
        unlink(temp_base.c_str());
        error = "rewrite child failed";
        return false;
    }
    FileEntry new_base{base_name(next_seq_), next_seq_, true};
    std::string new_base_path = path(new_base.name);
    if (std::rename(temp_base.c_str(), new_base_path.c_str()) < 0) {
        error = "installing the new AOF base: " + std::string(std::strerror(errno));
        unlink(temp_base.c_str());
        return false;
    }

    // The child forked right after begin_rewrite() switched incr files, so
    // its snapshot covers the old base and every incr but the current one.
    std::optional<FileEntry> old_base = base_;
    std::vector<FileEntry> old_incrs = incrs_;
    base_ = new_base;
    incrs_ = {old_incrs.back()};
    if (!persist_manifest(error)) {
        base_ = old_base;
        incrs_ = old_incrs;
        unlink(new_base_path.c_str());
        return false;
    }
    ++next_seq_;

    // Only now that the manifest no longer references them. A crash before
    // these unlinks just leaves orphaned files that nothing loads.
    if (old_base) {
        unlink(path(old_base->name).c_str());
    }
    for (size_t i = 0; i + 1 < old_incrs.size(); ++i) {
        unlink(path(old_incrs[i].name).c_str());
    }
    fileutil::fsync_dir(dir_);

    base_bytes_ = file_size(new_base_path);
    older_incr_bytes_ = 0;
    size_after_rewrite_ = current_size();
    return true;
}

bool AppendOnlyFile::healthy() const {
    return write_errno_ == 0 && (!fsync_worker_ || fsync_worker_->last_errno() == 0);
}

std::string AppendOnlyFile::last_error() const {
    int err = write_errno_ != 0 ? write_errno_ : (fsync_worker_ ? fsync_worker_->last_errno() : 0);
    return err != 0 ? std::strerror(err) : "no error";
}
