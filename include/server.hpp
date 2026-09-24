#pragma once

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "client.hpp"
#include "command_dispatcher.hpp"

class Replication;

// Single-threaded, level-triggered epoll event loop. One thread multiplexes
// every client: sockets are non-blocking, and the kernel tells us which ones
// are ready so we never park inside read()/write() on a single client.
//
// Owns: the listening socket, the epoll instance, a signalfd for SIGINT/SIGTERM
// (graceful shutdown), and every Client. Also hosts sockets it doesn't own
// (watch()), such as a replica's in-progress handshake with its master.
class Server : public ClientHost {
public:
    Server(std::string bind_addr, uint16_t port, CommandDispatcher& dispatcher);
    ~Server() override;

    Server(const Server&) = delete;
    Server& operator=(const Server&) = delete;

    // Optional; required before a replica's master link or a replica
    // attached to us can exist.
    void set_replication(Replication* replication) { replication_ = replication; }

    // Creates the listening socket, epoll instance and signalfd. Returns
    // false (after logging the failing syscall) if any setup step fails.
    bool start();

    // Runs `task` roughly every `interval` on the event-loop thread (Redis's
    // serverCron). Call before run().
    void set_cron(std::chrono::milliseconds interval, std::function<void()> task);

    // Runs `hook` once per loop iteration, after commands have executed but
    // before any of their replies are written (Redis's beforeSleep). The
    // AOF flush lives here: a reply must never leave before the write it
    // acknowledges has been handed to the AOF.
    void set_before_sleep(std::function<void()> hook);

    // Runs the event loop until SIGINT/SIGTERM arrives.
    void run();

    // ClientHost.
    void queue_write(Client& c) override;
    void close_client(Client& c) override { mark_closing(c); }
    void process_buffered(Client& c) override;
    bool watch(int fd, bool want_write, std::function<void()> on_ready) override;
    void unwatch(int fd) override;
    Client& adopt_master(int fd, const std::string& pending) override;

private:
    int cron_timeout_ms() const;
    void run_cron_if_due();

    Client& add_client(int fd);
    void accept_clients();
    void handle_readable(Client& conn);
    void process_input(Client& conn);
    void flush_pending_writes();
    void flush(Client& conn);
    bool refill_snapshot(Client& conn);
    void set_write_interest(Client& conn, bool enabled);
    void mark_closing(Client& conn);
    void close_pending();
    void close_all();

    std::string bind_addr_;
    uint16_t port_;
    CommandDispatcher& dispatcher_;
    Replication* replication_ = nullptr;

    int listen_fd_ = -1;
    int epoll_fd_ = -1;
    int signal_fd_ = -1;

    // steady_clock, not the wall clock: an NTP adjustment or manual clock
    // change must not make the timer fire in a burst or stall for hours.
    std::chrono::milliseconds cron_interval_{0};
    std::function<void()> cron_task_;
    std::chrono::steady_clock::time_point next_cron_;
    std::function<void()> before_sleep_;

    uint64_t next_client_id_ = 1;
    std::unordered_map<int, std::unique_ptr<Client>> conns_;
    std::unordered_map<int, std::function<void()>> watchers_;
    std::vector<int> pending_writes_;  // clients with fresh replies to send this iteration
    std::vector<int> pending_close_;
};
