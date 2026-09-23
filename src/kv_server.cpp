#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstdio>
#include <iostream>
#include <string>

#include "command_dispatcher.hpp"
#include "hash_table.hpp"
#include "reply.hpp"
#include "resp_parser.hpp"

namespace {

constexpr int kPort = 6380;
constexpr int kBacklog = 128;
constexpr size_t kReadChunkSize = 16 * 1024;
// Upper bound on bytes buffered for a single not-yet-complete command
// (Redis's equivalent is client-query-buffer-limit, default 1GB).
constexpr size_t kMaxQueryBufferBytes = 64 * 1024 * 1024;

// write() may accept fewer bytes than asked (partial write) or be interrupted
// by a signal before writing anything (EINTR); loop until everything is sent.
// MSG_NOSIGNAL: if the client has already closed, return EPIPE instead of
// raising SIGPIPE, whose default action would kill the whole server.
bool send_all(int fd, const std::string& data) {
    size_t sent = 0;
    while (sent < data.size()) {
        ssize_t n = send(fd, data.data() + sent, data.size() - sent, MSG_NOSIGNAL);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            std::perror("send");
            return false;
        }
        sent += static_cast<size_t>(n);
    }
    return true;
}

// Serves one client until it disconnects or violates the protocol.
void serve_client(int client_fd, CommandDispatcher& dispatcher) {
    RespParser parser;
    std::string out;
    char buf[kReadChunkSize];

    while (true) {
        ssize_t n = read(client_fd, buf, sizeof(buf));
        if (n == 0) {
            return;  // client closed the connection
        }
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            std::perror("read");
            return;
        }
        parser.feed(buf, static_cast<size_t>(n));

        // One read() may carry several pipelined commands; run them all and
        // send the replies together in a single write.
        bool close_after_reply = false;
        while (true) {
            RespParser::ParseResult result = parser.try_parse_command();
            if (result.status == RespParser::Status::kIncomplete) {
                break;
            }
            if (result.status == RespParser::Status::kProtocolError) {
                // Once framing is broken we can't tell where the next command
                // starts, so nothing after this point can be trusted.
                reply::error(out, "ERR Protocol error");
                close_after_reply = true;
                break;
            }
            dispatcher.dispatch(result.command, out);
        }

        if (!close_after_reply && parser.buffered_bytes() > kMaxQueryBufferBytes) {
            reply::error(out, "ERR Protocol error: query buffer limit exceeded");
            close_after_reply = true;
        }

        if (!out.empty()) {
            if (!send_all(client_fd, out)) {
                return;
            }
            out.clear();
        }
        if (close_after_reply) {
            return;
        }
    }
}

}  // namespace

int main() {
    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) {
        std::perror("socket");
        return 1;
    }

    int opt = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(kPort);

    if (bind(listen_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        std::perror("bind");
        close(listen_fd);
        return 1;
    }

    if (listen(listen_fd, kBacklog) < 0) {
        std::perror("listen");
        close(listen_fd);
        return 1;
    }

    std::cout << "kv_server listening on port " << kPort << "\n";

    HashTable store;
    CommandDispatcher dispatcher(store);

    // Still one client at a time: while one is being served, others wait in
    // the listen backlog. The data outlives each connection, so a second
    // client sees what the first one SET.
    while (true) {
        sockaddr_in client_addr{};
        socklen_t client_len = sizeof(client_addr);
        int client_fd = accept(listen_fd, reinterpret_cast<sockaddr*>(&client_addr), &client_len);
        if (client_fd < 0) {
            if (errno == EINTR) {
                continue;
            }
            std::perror("accept");
            continue;
        }

        char client_ip[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &client_addr.sin_addr, client_ip, sizeof(client_ip));
        std::cout << "client connected: " << client_ip << ":" << ntohs(client_addr.sin_port) << "\n";

        serve_client(client_fd, dispatcher);

        std::cout << "client disconnected\n";
        close(client_fd);
    }
}
