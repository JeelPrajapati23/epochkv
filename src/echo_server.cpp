#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstdio>
#include <cstring>
#include <iostream>

constexpr int PORT = 6380;
constexpr int BACKLOG = 1;
constexpr size_t BUFFER_SIZE = 4096;

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
    addr.sin_port = htons(PORT);

    if (bind(listen_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        std::perror("bind");
        close(listen_fd);
        return 1;
    }

    if (listen(listen_fd, BACKLOG) < 0) {
        std::perror("listen");
        close(listen_fd);
        return 1;
    }

    std::cout << "echo_server listening on port " << PORT << "\n";

    sockaddr_in client_addr{};
    socklen_t client_len = sizeof(client_addr);
    int client_fd = accept(listen_fd, reinterpret_cast<sockaddr*>(&client_addr), &client_len);
    if (client_fd < 0) {
        std::perror("accept");
        close(listen_fd);
        return 1;
    }

    char client_ip[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &client_addr.sin_addr, client_ip, sizeof(client_ip));
    std::cout << "client connected: " << client_ip << ":" << ntohs(client_addr.sin_port) << "\n";

    char buffer[BUFFER_SIZE];
    ssize_t bytes_read;
    while ((bytes_read = read(client_fd, buffer, sizeof(buffer))) > 0) {
        ssize_t bytes_written = write(client_fd, buffer, static_cast<size_t>(bytes_read));
        if (bytes_written < 0) {
            std::perror("write");
            break;
        }
    }

    if (bytes_read < 0) {
        std::perror("read");
    }

    std::cout << "client disconnected\n";

    close(client_fd);
    close(listen_fd);
    return 0;
}
