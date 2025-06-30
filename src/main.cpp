#include "TCPManager.h"

namespace {
std::function<void(int)> shutdown_handler;
void signal_handler(int signal) { shutdown_handler(signal); }
} // namespace

int main(int argc, char *argv[]) {
    // Disable output buffering
    std::cout << std::unitbuf;
    std::cerr << std::unitbuf;

    try {
        TCPManager tcp_manager;
        tcp_manager.createSocketAndListen();

        shutdown_handler = [&tcp_manager](int signal) {
            std::cout << "Caught signal " << signal << '\n';
            tcp_manager.~TCPManager();
            exit(1);
        };

        signal(SIGINT, signal_handler);
        Fd client_fd = tcp_manager.acceptConnections();

        while (true) {
            try {
                KafkaApis kafka_apis(client_fd, tcp_manager);

                tcp_manager.readBufferFromClientFd(
                    client_fd,
                    [&kafka_apis](const char *buf, const size_t buf_size) {
                        kafka_apis.classifyRequest(buf, buf_size);
                    });
            } catch (const std::exception &e) {
                std::cerr << "Error handling client: " << e.what() << '\n';
            }
        }
    } catch (const std::exception &e) {
        std::cerr << "Error: " << e.what() << '\n';
        return 1;
    }

    return 0;
}