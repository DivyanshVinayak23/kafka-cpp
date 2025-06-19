#include <cstdlib>
#include <cstring>
#include <iostream>
#include <netdb.h>
#include <string>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/types.h>

int main(int argc, char* argv[]) {
    // Disable output buffering
    std::cout << std::unitbuf;
    std::cerr << std::unitbuf;

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        std::cerr << "Failed to create server socket: " << std::endl;
        return 1;
    }

    // Since the tester restarts your program quite often, setting SO_REUSEADDR
    // ensures that we don't run into 'Address already in use' errors
    int reuse = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
        close(server_fd);
        std::cerr << "setsockopt failed: " << std::endl;
        return 1;
    }

    struct sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(9092);

    if (bind(server_fd, reinterpret_cast<struct sockaddr*>(&server_addr), sizeof(server_addr)) != 0) {
        close(server_fd);
        std::cerr << "Failed to bind to port 9092" << std::endl;
        return 1;
    }

    int connection_backlog = 5;
    if (listen(server_fd, connection_backlog) != 0) {
        close(server_fd);
        std::cerr << "listen failed" << std::endl;
        return 1;
    }

    std::cout << "Waiting for a client to connect...\n";

    struct sockaddr_in client_addr{};
    socklen_t client_addr_len = sizeof(client_addr);

    // You can use print statements as follows for debugging, they'll be visible when running tests.
    std::cerr << "Logs from your program will appear here!\n";
    
    // Uncomment this block to pass the first stage
    // 
    int client_fd = accept(server_fd, reinterpret_cast<struct sockaddr*>(&client_addr), &client_addr_len);
    std::cout << "Client connected\n";
    uint32_t message_size_net;
    if(read(client_fd, &message_size_net,4) != 4)
    {
        std::cerr << "Failed to read message size" << std::endl;
        close(client_fd);
        close(server_fd);
        return 1;
    }
    unsigned char header[10];
    if(read(client_fd, header, 10) != 10)
    {
        std::cerr << "Failed to read the header" << std::endl;
        close(client_fd);
        close(server_fd);
        return 1;
    }
    
    uint32_t message_size = ntohl(message_size_net);
    int remaining_bytes = message_size - 10;
    while(remaining_bytes > 0)
    {
        char buffer[1024];
        int to_read = remaining_bytes < 256 ? remaining_bytes : 256;
        int n = read(client_fd,buffer,to_read);
        if(n <= 0)
        {
            break;
        }
        remaining_bytes -= n;
    }
    
    uint32_t correlation_id;
    memcpy(&correlation_id, header + 4, 4);
    uint16_t api_key , api_version;
    memcpy(&api_key, header, 2);
    memcpy(&api_version, header + 2, 2);

    int16_t error_code = 0;
    if(api_key == 18)
    {
        if(api_version > 4)
        {
            error_code = 35;
        }

    }

    uint32_t response_message_size = htonl(6);
    int16_t error_code_net = htons(error_code);
    write(client_fd, &response_message_size, 4);
    write(client_fd, &correlation_id,4);
    write(client_fd, &error_code_net, 2);
    
    close(client_fd);

    close(server_fd);
    return 0;
}