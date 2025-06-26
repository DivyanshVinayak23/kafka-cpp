#include <cstdlib>
#include <cstring>
#include <iostream>
#include <netdb.h>
#include <string>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <errno.h>


struct RequestHeader{
    uint16_t request_api_key;
    uint16_t request_api_version;
    uint32_t correlation_id;
    uint16_t client_id_length;
};

struct api_version{
    int16_t api_key;
    int16_t min_version;
    int16_t max_version;
    char tag_buffer;
};

struct api_versions{
    int8_t size;
    api_version *array;
};


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
    
    // Handle multiple requests from the same client
    while (true) {
        uint32_t size;
        uint16_t error_code;
        RequestHeader request_header;
        
        // Read request size
        ssize_t bytes_read = read(client_fd, &size, sizeof(size));
        if (bytes_read <= 0) {
            // Client disconnected or error occurred
            std::cerr << "Client disconnected or read error" << std::endl;
            break;
        }
        
        // Read request header
        bytes_read = read(client_fd, &request_header, sizeof(request_header));
        if (bytes_read <= 0) {
            std::cerr << "Failed to read request header" << std::endl;
            break;
        }
        
        // Convert network byte order to host byte order
        request_header.request_api_key = ntohs(request_header.request_api_key);
        request_header.request_api_version = ntohs(request_header.request_api_version);
        request_header.client_id_length = ntohs(request_header.client_id_length);
        
        // Read client ID
        char *client_id = new char[request_header.client_id_length];
        bytes_read = read(client_fd, client_id, request_header.client_id_length);
        if (bytes_read <= 0) {
            delete[] client_id;
            std::cerr << "Failed to read client ID" << std::endl;
            break;
        }
        
        // Convert size to host byte order
        size = ntohl(size);
        
        // Read request body
        char *body = new char[size - sizeof(request_header) - request_header.client_id_length];
        bytes_read = read(client_fd, body, size - sizeof(request_header) - request_header.client_id_length);
        if (bytes_read <= 0) {
            delete[] client_id;
            delete[] body;
            std::cerr << "Failed to read request body" << std::endl;
            break;
        }
        
        // Prepare response
        api_versions content;
        content.size = 1;  // Changed from 2 to 1 since we only have one API version
        content.array = new api_version[content.size];
        content.array[0].api_key = htons(18);  // Use htons for network byte order
        content.array[0].min_version = htons(0);
        content.array[0].max_version = htons(4);
        content.array[0].tag_buffer = 0;
        uint32_t throttle_time_ms = 0;
        int8_t tag = 0;
        uint32_t res_size;
        
        if (request_header.request_api_version > 4) {
            error_code = htons(35);
            res_size = htonl(sizeof(request_header.correlation_id) + sizeof(error_code));
            write(client_fd, &res_size, sizeof(res_size));
            write(client_fd, &request_header.correlation_id, sizeof(request_header.correlation_id));
            write(client_fd, &error_code, sizeof(error_code));
        } else {
            error_code = 0;
            // Calculate response size: correlation_id + error_code + api_versions_size + api_versions_array + throttle_time_ms + tag
            uint32_t response_size = sizeof(request_header.correlation_id) + sizeof(error_code) + 
                                   sizeof(content.size) + (content.size * sizeof(api_version)) + 
                                   sizeof(throttle_time_ms) + sizeof(tag);
            res_size = htonl(response_size);
            write(client_fd, &res_size, sizeof(res_size));
            write(client_fd, &request_header.correlation_id, sizeof(request_header.correlation_id));
            write(client_fd, &error_code, sizeof(error_code));
            write(client_fd, &content.size, sizeof(content.size));
            write(client_fd, content.array, content.size * sizeof(api_version));
            write(client_fd, &throttle_time_ms, sizeof(throttle_time_ms));
            write(client_fd, &tag, sizeof(tag));
        }
        
        // Clean up memory for this request
        delete[] client_id;
        delete[] body;
        delete[] content.array;
    }
    
    close(client_fd);
    close(server_fd);
    return 0;
}