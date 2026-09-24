#include "snapshot.hpp"

#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <vector>

#include "file_util.hpp"

namespace snapshot {
namespace {

constexpr char kMagic[] = {'K', 'V', 'S', 'N', 'A', 'P'};
constexpr uint8_t kVersion = 1;
constexpr uint8_t kOpString = 0x00;
constexpr uint8_t kOpExpireMs = 0xFC;
constexpr uint8_t kOpEof = 0xFF;
constexpr size_t kIoBufferSize = 64 * 1024;
constexpr size_t kMaxVarintBytes = 10;  // ceil(64 / 7)

// CRC-32 (IEEE 802.3, reflected polynomial 0xEDB88320), zlib-compatible:
// crc32_update(crc32_update(0, a), b) == crc32_update(0, a + b), so the
// checksum can be computed while streaming. It catches torn writes and bit
// rot, not deliberate tampering.
uint32_t crc32_update(uint32_t crc, const char* data, size_t len) {
    static const std::array<uint32_t, 256> table = [] {
        std::array<uint32_t, 256> t{};
        for (uint32_t i = 0; i < 256; ++i) {
            uint32_t c = i;
            for (int k = 0; k < 8; ++k) {
                c = (c & 1) ? 0xEDB88320u ^ (c >> 1) : c >> 1;
            }
            t[i] = c;
        }
        return t;
    }();
    crc = ~crc;
    for (size_t i = 0; i < len; ++i) {
        crc = table[(crc ^ static_cast<uint8_t>(data[i])) & 0xFF] ^ (crc >> 8);
    }
    return ~crc;
}

// Buffers output into 64KB write()s and checksums everything that passes
// through. After the first I/O error it stops writing and just reports it.
class Writer {
public:
    explicit Writer(int fd) : fd_(fd) { buf_.reserve(kIoBufferSize); }

    void put(const char* data, size_t len) {
        crc_ = crc32_update(crc_, data, len);
        buf_.append(data, len);
        if (buf_.size() >= kIoBufferSize) {
            flush();
        }
    }

    void put_byte(uint8_t b) {
        char c = static_cast<char>(b);
        put(&c, 1);
    }

    void put_varint(uint64_t v) {
        char tmp[kMaxVarintBytes];
        size_t n = 0;
        do {
            uint8_t byte = v & 0x7F;
            v >>= 7;
            tmp[n++] = static_cast<char>(v != 0 ? byte | 0x80 : byte);
        } while (v != 0);
        put(tmp, n);
    }

    void put_fixed(uint64_t v, size_t bytes) {
        char tmp[8];
        for (size_t i = 0; i < bytes; ++i) {
            tmp[i] = static_cast<char>((v >> (8 * i)) & 0xFF);
        }
        put(tmp, bytes);
    }

    void put_string(const std::string& s) {
        put_varint(s.size());
        put(s.data(), s.size());
    }

    bool flush() {
        if (ok_ && fileutil::write_all(fd_, buf_.data(), buf_.size()) != buf_.size()) {
            ok_ = false;
        }
        buf_.clear();
        return ok_;
    }

    uint32_t crc() const { return crc_; }

private:
    int fd_;
    std::string buf_;
    uint32_t crc_ = 0;
    bool ok_ = true;
};

// Buffered reader that checksums every byte it hands out.
class Reader {
public:
    explicit Reader(int fd) : fd_(fd), buf_(kIoBufferSize) {}

    // False if the file ends (or read() fails) before `len` bytes.
    bool get(char* out, size_t len) {
        while (len > 0) {
            if (pos_ == len_ && !fill()) {
                return false;
            }
            size_t n = std::min(len, len_ - pos_);
            std::memcpy(out, buf_.data() + pos_, n);
            crc_ = crc32_update(crc_, buf_.data() + pos_, n);
            pos_ += n;
            out += n;
            len -= n;
        }
        return true;
    }

    bool get_byte(uint8_t& b) {
        char c;
        if (!get(&c, 1)) {
            return false;
        }
        b = static_cast<uint8_t>(c);
        return true;
    }

    bool get_varint(uint64_t& v) {
        v = 0;
        for (size_t i = 0; i < kMaxVarintBytes; ++i) {
            uint8_t byte;
            if (!get_byte(byte)) {
                return false;
            }
            v |= static_cast<uint64_t>(byte & 0x7F) << (7 * i);
            if ((byte & 0x80) == 0) {
                return true;
            }
        }
        return false;  // no terminating byte within 10 bytes: corrupt
    }

    bool get_fixed(uint64_t& v, size_t bytes) {
        char tmp[8];
        if (!get(tmp, bytes)) {
            return false;
        }
        v = 0;
        for (size_t i = 0; i < bytes; ++i) {
            v |= static_cast<uint64_t>(static_cast<uint8_t>(tmp[i])) << (8 * i);
        }
        return true;
    }

    // Grows `s` chunk by chunk instead of resizing to the declared length up
    // front: a corrupt length like 2^60 then fails as "truncated" once the
    // file runs out, instead of attempting a giant allocation.
    bool get_string(std::string& s) {
        uint64_t len;
        if (!get_varint(len)) {
            return false;
        }
        s.clear();
        while (len > 0) {
            size_t n = static_cast<size_t>(std::min<uint64_t>(len, kIoBufferSize));
            size_t old_size = s.size();
            s.resize(old_size + n);
            if (!get(&s[old_size], n)) {
                return false;
            }
            len -= n;
        }
        return true;
    }

    bool at_eof() { return pos_ == len_ && !fill(); }
    uint32_t crc() const { return crc_; }
    int error() const { return errno_; }

private:
    bool fill() {
        ssize_t n;
        do {
            n = read(fd_, buf_.data(), buf_.size());
        } while (n < 0 && errno == EINTR);
        if (n < 0) {
            errno_ = errno;
        }
        if (n <= 0) {
            return false;
        }
        len_ = static_cast<size_t>(n);
        pos_ = 0;
        return true;
    }

    int fd_;
    std::vector<char> buf_;
    size_t pos_ = 0;
    size_t len_ = 0;
    uint32_t crc_ = 0;
    int errno_ = 0;
};

bool load_from(Reader& r, Store& store, std::string& error) {
    auto fail = [&](const char* what) {
        error = r.error() != 0 ? std::strerror(r.error()) : what;
        return false;
    };

    char magic[sizeof(kMagic)];
    if (!r.get(magic, sizeof(magic)) || std::memcmp(magic, kMagic, sizeof(kMagic)) != 0) {
        return fail("not a snapshot file (bad magic)");
    }
    uint8_t version;
    if (!r.get_byte(version)) {
        return fail("truncated");
    }
    if (version != kVersion) {
        error = "unsupported snapshot version " + std::to_string(version);
        return false;
    }

    const int64_t now = store.now_ms();
    while (true) {
        uint8_t op;
        if (!r.get_byte(op)) {
            return fail("truncated");
        }
        int64_t expire_ms = -1;
        if (op == kOpExpireMs) {
            uint64_t raw;
            if (!r.get_fixed(raw, 8) || !r.get_byte(op)) {
                return fail("truncated");
            }
            expire_ms = static_cast<int64_t>(raw);
            if (expire_ms < 0) {
                return fail("corrupt expire time");
            }
        }
        if (op == kOpEof && expire_ms == -1) {
            break;
        }
        if (op != kOpString) {
            return fail("corrupt: unknown record type");
        }
        std::string key;
        std::string value;
        if (!r.get_string(key) || !r.get_string(value)) {
            return fail("truncated");
        }
        if (expire_ms >= 0 && expire_ms <= now) {
            continue;  // expired while the server was down
        }
        store.set(key, std::move(value));
        if (expire_ms >= 0) {
            store.set_expire(key, expire_ms);
        }
    }

    const uint32_t expected = r.crc();  // covers everything through the EOF marker
    uint64_t stored;
    if (!r.get_fixed(stored, 4)) {
        return fail("truncated");
    }
    if (stored != expected) {
        return fail("checksum mismatch");
    }
    if (!r.at_eof()) {
        return fail("unexpected data after the end of the snapshot");
    }
    return true;
}

}  // namespace

bool write_file(const Store& store, const std::string& path) {
    int fd = open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    if (fd < 0) {
        return false;
    }
    Writer w(fd);
    w.put(kMagic, sizeof(kMagic));
    w.put_byte(kVersion);

    const int64_t now = store.now_ms();
    store.for_each([&](const std::string& key, const std::string& value, int64_t expire_ms) {
        if (expire_ms >= 0) {
            if (expire_ms <= now) {
                return;
            }
            w.put_byte(kOpExpireMs);
            w.put_fixed(static_cast<uint64_t>(expire_ms), 8);
        }
        w.put_byte(kOpString);
        w.put_string(key);
        w.put_string(value);
    });
    w.put_byte(kOpEof);
    w.put_fixed(w.crc(), 4);

    // fsync before the caller renames the file into place: otherwise the
    // rename can reach disk before the data, and a crash leaves an empty or
    // partial file under the final name.
    bool ok = w.flush() && fsync(fd) == 0;
    int saved_errno = errno;
    ok = close(fd) == 0 && ok;
    if (!ok && saved_errno != 0) {
        errno = saved_errno;
    }
    return ok;
}

bool save(const Store& store, const std::string& path) {
    std::string temp = fileutil::join(fileutil::dirname(path), "temp-" + std::to_string(getpid()) + ".snap");
    if (!write_file(store, temp) || !fileutil::durable_rename(temp, path)) {
        int saved_errno = errno;
        unlink(temp.c_str());
        errno = saved_errno;
        return false;
    }
    return true;
}

LoadResult load(const std::string& path, Store& store, std::string& error) {
    int fd = open(path.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd < 0) {
        if (errno == ENOENT) {
            return LoadResult::kNotFound;
        }
        error = path + ": " + std::strerror(errno);
        return LoadResult::kError;
    }
    Reader reader(fd);
    std::string reason;
    bool ok = load_from(reader, store, reason);
    close(fd);
    if (!ok) {
        error = path + ": " + reason;
        return LoadResult::kError;
    }
    return LoadResult::kOk;
}

}  // namespace snapshot
