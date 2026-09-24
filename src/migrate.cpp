#include "migrate.hpp"

#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>

namespace migrate {
namespace {

using Clock = std::chrono::steady_clock;

// Waits for `events` on fd until the deadline. False on timeout or error.
bool wait_for(int fd, short events, Clock::time_point deadline) {
    while (true) {
        auto left = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - Clock::now()).count();
        if (left <= 0) {
            return false;
        }
        pollfd p{fd, events, 0};
        int rc = poll(&p, 1, static_cast<int>(left));
        if (rc < 0 && errno == EINTR) {
            continue;
        }
        return rc > 0 && (p.revents & (events | POLLHUP | POLLERR)) != 0;
    }
}

int connect_with_deadline(const std::string& host, uint16_t port, Clock::time_point deadline) {
    addrinfo hints{};
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    addrinfo* res = nullptr;
    if (getaddrinfo(host.c_str(), std::to_string(port).c_str(), &hints, &res) != 0) {
        return -1;
    }
    int fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    bool ok = fd >= 0 && (connect(fd, res->ai_addr, res->ai_addrlen) == 0 || errno == EINPROGRESS);
    freeaddrinfo(res);
    int err = 0;
    socklen_t len = sizeof(err);
    ok = ok && wait_for(fd, POLLOUT, deadline) && getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len) == 0 && err == 0;
    if (!ok) {
        if (fd >= 0) {
            close(fd);
        }
        return -1;
    }
    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    return fd;
}

}  // namespace

bool exchange(const std::string& host, uint16_t port, const std::string& request, size_t expected,
              std::chrono::milliseconds timeout, std::vector<std::string>& replies, std::string& error) {
    auto deadline = Clock::now() + timeout;
    int fd = connect_with_deadline(host, port, deadline);
    if (fd < 0) {
        error = "IOERR error or timeout connecting to the client";
        return false;
    }

    bool ok = true;
    size_t sent = 0;
    while (ok && sent < request.size()) {
        ssize_t n = send(fd, request.data() + sent, request.size() - sent, MSG_NOSIGNAL);
        if (n > 0) {
            sent += static_cast<size_t>(n);
        } else if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            ok = wait_for(fd, POLLOUT, deadline);
        } else if (!(n < 0 && errno == EINTR)) {
            ok = false;
        }
    }

    std::string buf;
    replies.clear();
    while (ok && replies.size() < expected) {
        size_t eol = buf.find("\r\n");
        if (eol != std::string::npos) {
            replies.push_back(buf.substr(0, eol));
            buf.erase(0, eol + 2);
            continue;
        }
        char chunk[4096];
        ssize_t n = read(fd, chunk, sizeof(chunk));
        if (n > 0) {
            buf.append(chunk, static_cast<size_t>(n));
        } else if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            ok = wait_for(fd, POLLIN, deadline);
        } else if (!(n < 0 && errno == EINTR)) {
            ok = false;  // closed before answering everything
        }
    }
    close(fd);
    if (!ok) {
        error = "IOERR error or timeout reading to target instance";
    }
    return ok;
}

}  // namespace migrate
