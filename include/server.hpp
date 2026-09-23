#pragma once

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "command_dispatcher.hpp"
#include "resp_parser.hpp"

// Single-threaded, level-triggered epoll event loop. One thread multiplexes
// every client: sockets are non-blocking, and the kernel tells us which ones
// are ready so we never park inside read()/write() on a single client.
//
// Owns: the listening socket, the epoll instance, a signalfd for SIGINT/SIGTERM
// (graceful shutdown), and per-client Connection state.
class Server {
public:
    Server(std::string bind_addr, uint16_t port, CommandDispatcher& dispatcher);
    ~Server();

    Server(const Server&) = delete;
    Server& operator=(const Server&) = delete;

    // Creates the listening socket, epoll instance and signalfd. Returns
    // false (after logging the failing syscall) if any setup step fails.
    bool start();

    // Runs `task` roughly every `interval` on the event-loop thread (Redis's
    // serverCron). Call before run().
    void set_cron(std::chrono::milliseconds interval, std::function<void()> task);

    // Runs the event loop until SIGINT/SIGTERM arrives.
    void run();

private:
    struct Connection {
        explicit Connection(int fd) : fd(fd) {}

        int fd;
        RespParser parser;
        std::string out;                  // encoded replies not yet sent
        bool write_interest = false;      // currently registered for EPOLLOUT
        bool close_after_flush = false;   // protocol error: send the error, then close
        bool closing = false;             // queued for close at end of this loop iteration
    };

    int cron_timeout_ms() const;
    void run_cron_if_due();

    void accept_clients();
    void handle_readable(Connection& conn);
    void flush(Connection& conn);
    void set_write_interest(Connection& conn, bool enabled);
    void mark_closing(Connection& conn);
    void close_pending();
    void close_all();

    std::string bind_addr_;
    uint16_t port_;
    CommandDispatcher& dispatcher_;

    int listen_fd_ = -1;
    int epoll_fd_ = -1;
    int signal_fd_ = -1;

    // steady_clock, not the wall clock: an NTP adjustment or manual clock
    // change must not make the timer fire in a burst or stall for hours.
    std::chrono::milliseconds cron_interval_{0};
    std::function<void()> cron_task_;
    std::chrono::steady_clock::time_point next_cron_;

    std::unordered_map<int, std::unique_ptr<Connection>> conns_;
    std::vector<int> pending_close_;
};
