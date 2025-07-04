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
            tcp_manager.shutdown();
            exit(0);
        };

        signal(SIGINT, signal_handler);
        
        // Run the multi-threaded server
        tcp_manager.runServer();
        
    } catch (const std::exception &e) {
        std::cerr << "Error: " << e.what() << '\n';
        return 1;
    }

    return 0;
}