#include <arpa/inet.h>
#include <cstring>
#include <iostream>
#include <netdb.h>
#include <sstream>
#include <string>
#include <unistd.h>

struct RequestHeader {
    uint16_t request_api_key;
    uint16_t request_api_version;
    uint32_t correlation_id;
};

struct api_version {
    int16_t api_key;
    int16_t min_version;
    int16_t max_version;
};

// Helper: write varint (Kafka compact encoding)
void write_varint(std::ostream& os, uint32_t value) {
    do {
        uint8_t byte = value & 0x7F;
        value >>= 7;
        if (value != 0)
            byte |= 0x80;
        os.put(byte);
    } while (value != 0);
}

// Helper: read varint (Kafka compact encoding)
uint32_t read_varint(int fd) {
    uint32_t value = 0;
    int shift = 0;
    uint8_t byte;
    do {
        read(fd, &byte, 1);
        value |= (byte & 0x7F) << shift;
        shift += 7;
    } while ((byte & 0x80) != 0);
    return value;
}

int main() {
    std::cout << std::unitbuf;
    std::cerr << std::unitbuf;

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        std::cerr << "Failed to create socket\n";
        return 1;
    }

    int reuse = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

    sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(9092);

    if (bind(server_fd, (sockaddr*)&server_addr, sizeof(server_addr)) != 0) {
        std::cerr << "Bind failed\n";
        return 1;
    }

    listen(server_fd, 5);
    std::cerr << "Logs from your program will appear here!\n";

    sockaddr_in client_addr{};
    socklen_t client_addr_len = sizeof(client_addr);
    int client_fd = accept(server_fd, (sockaddr*)&client_addr, &client_addr_len);
    std::cout << "Client connected\n";

    while (true) {
        uint32_t size;
        int bytes = read(client_fd, &size, sizeof(size));
        if (bytes <= 0) break;

        size = ntohl(size);
        RequestHeader header;
        bytes = read(client_fd, &header, sizeof(header));
        if (bytes <= 0) break;

        header.request_api_key = ntohs(header.request_api_key);
        header.request_api_version = ntohs(header.request_api_version);
        header.correlation_id = ntohl(header.correlation_id);

        // Read client ID based on API version
        uint32_t client_id_length = 0;
        char* client_id = nullptr;
        
        if (header.request_api_version >= 3) {
            client_id_length = read_varint(client_fd);
            if (client_id_length > 0) {
                client_id = new char[client_id_length];
                read(client_fd, client_id, client_id_length);
            }
        } else {
            uint16_t len;
            read(client_fd, &len, sizeof(len));
            client_id_length = ntohs(len);
            if (client_id_length > 0) {
                client_id = new char[client_id_length];
                read(client_fd, client_id, client_id_length);
            }
        }

        // Calculate remaining bytes to read
        size_t header_size = sizeof(RequestHeader);
        if (header.request_api_version < 3) {
            header_size += sizeof(uint16_t);
        }
        uint32_t remaining = size - header_size - client_id_length;
        
        char* body = nullptr;
        if (remaining > 0) {
            body = new char[remaining];
            read(client_fd, body, remaining);
        }

        // Build response - MUST include all expected APIs
        const int NUM_APIS = 3;
        api_version api_versions[NUM_APIS] = {
            {htons(0),  htons(0), htons(0)},  // Produce API
            {htons(1),  htons(0), htons(0)},  // Fetch API
            {htons(18), htons(0), htons(4)}   // ApiVersions API
        };

        std::stringstream response_body;
        uint32_t net_corr_id = htonl(header.correlation_id);

        if (header.request_api_version >= 3) {
            // Throttle time
            int32_t throttle = htonl(0);
            response_body.write((char*)&throttle, sizeof(throttle));

            // Error code
            uint16_t error_code = htons(0);
            response_body.write((char*)&error_code, sizeof(error_code));

            // Compact array size
            write_varint(response_body, NUM_APIS);

            for (int i = 0; i < NUM_APIS; i++) {
                response_body.write((char*)&api_versions[i].api_key, sizeof(int16_t));
                response_body.write((char*)&api_versions[i].min_version, sizeof(int16_t));
                response_body.write((char*)&api_versions[i].max_version, sizeof(int16_t));
                
                // Tagged fields for each ApiVersion element
                write_varint(response_body, 0);
            }

            // Array tagged fields
            write_varint(response_body, 0);

            // Top-level tagged fields
            write_varint(response_body, 0);
        } else {
            // Legacy encoding for version < 3
            uint16_t error_code = htons(0);
            response_body.write((char*)&error_code, sizeof(error_code));

            int32_t api_count = htonl(NUM_APIS);
            response_body.write((char*)&api_count, sizeof(api_count));

            for (int i = 0; i < NUM_APIS; i++) {
                response_body.write((char*)&api_versions[i].api_key, sizeof(int16_t));
                response_body.write((char*)&api_versions[i].min_version, sizeof(int16_t));
                response_body.write((char*)&api_versions[i].max_version, sizeof(int16_t));
            }
        }

        // Prepare full response (frame size + correlation_id + response body)
        std::string body_str = response_body.str();
        uint32_t frame_size = sizeof(net_corr_id) + body_str.size();
        uint32_t net_frame_size = htonl(frame_size);

        // Send response
        write(client_fd, &net_frame_size, sizeof(net_frame_size));
        write(client_fd, &net_corr_id, sizeof(net_corr_id));
        write(client_fd, body_str.data(), body_str.size());

        if (client_id) delete[] client_id;
        if (body) delete[] body;
    }

    close(client_fd);
    close(server_fd);
    return 0;
}