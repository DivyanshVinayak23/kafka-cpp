#include <cstdlib>
#include <cstring>
#include <iostream>
#include <netdb.h>
#include <string>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/types.h>


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
    
    int client_fd = accept(server_fd, reinterpret_cast<struct sockaddr*>(&client_addr), &client_addr_len);
    std::cout << "Client connected\n";
    
    while(true){
        uint32_t size;
        auto received_size = recv(client_fd, &size, sizeof(size), 0);
        if(received_size <= 0){
            std::cerr << "Error receiving size or client disconnected" << std::endl;
            break;
        }
        if(received_size != sizeof(size)){
            std::cerr << "Error receiving size: " << std::endl;
            break;
        }

        // Convert size from network byte order
        size = ntohl(size);
        
        RequestHeader request_header;
        auto header_size = recv(client_fd, &request_header, sizeof(request_header), 0);
        if(header_size != sizeof(request_header)){
            std::cerr << "Error receiving request header" << std::endl;
            break;
        }

        // Convert header fields from network byte order
        request_header.request_api_key = ntohs(request_header.request_api_key);
        request_header.request_api_version = ntohs(request_header.request_api_version);
        // Keep correlation_id in network byte order for response
        uint32_t correlation_id_host = ntohl(request_header.correlation_id);
        request_header.client_id_length = ntohs(request_header.client_id_length);
        
        // Read client ID
        char *client_id = nullptr;
        if(request_header.client_id_length > 0) {
            client_id = new char[request_header.client_id_length];
            auto client_id_size = recv(client_fd, client_id, request_header.client_id_length, 0);
            if(client_id_size != request_header.client_id_length){
                std::cerr << "Error receiving client ID" << std::endl;
                delete[] client_id;
                break;
            }
        }
        
        // Read remaining body
        uint32_t body_size = size - sizeof(request_header) - request_header.client_id_length;
        char *body = nullptr;
        if(body_size > 0) {
            body = new char[body_size];
            auto body_received = recv(client_fd, body, body_size, 0);
            if(body_received != body_size){
                std::cerr << "Error receiving body" << std::endl;
                delete[] body;
                if(client_id) delete[] client_id;
                break;
            }
        }
        
        // Prepare response
        uint16_t error_code = 0;
        uint32_t res_size;
        
        if (request_header.request_api_version > 4) {
            error_code = 35; // UNSUPPORTED_VERSION
            res_size = sizeof(request_header.correlation_id) + sizeof(error_code);
            
            // Send error response
            uint32_t res_size_network = htonl(res_size);
            write(client_fd, &res_size_network, sizeof(res_size_network));
            write(client_fd, &request_header.correlation_id, sizeof(request_header.correlation_id));
            write(client_fd, &error_code, sizeof(error_code));
        } else {
            // Create API versions response
            api_versions content;
            content.size = 1; // Only one API version entry
            content.array = new api_version[content.size];
            content.array[0].api_key = 18; // API_VERSIONS
            content.array[0].min_version = 0;
            content.array[0].max_version = 4;
            content.array[0].tag_buffer = 0;
            
            uint32_t throttle_time_ms = 0;
            int8_t tag = 0;
            
            // Calculate response size: correlation_id + error_code + api_versions_size + api_versions_array + throttle_time_ms + tag
            res_size = sizeof(request_header.correlation_id) + sizeof(error_code) + 
                      sizeof(content.size) + (content.size * sizeof(api_version)) + 
                      sizeof(throttle_time_ms) + sizeof(tag);
            
            // Send response
            uint32_t res_size_network = htonl(res_size);
            write(client_fd, &res_size_network, sizeof(res_size_network));
            write(client_fd, &request_header.correlation_id, sizeof(request_header.correlation_id));
            write(client_fd, &error_code, sizeof(error_code));
            write(client_fd, &content.size, sizeof(content.size));
            write(client_fd, content.array, content.size * sizeof(api_version));
            write(client_fd, &throttle_time_ms, sizeof(throttle_time_ms));
            write(client_fd, &tag, sizeof(tag));
            
            delete[] content.array;
        }
        
        // Clean up
        if(client_id) delete[] client_id;
        if(body) delete[] body;
    }
    
    close(client_fd);
    close(server_fd);
    return 0;
}