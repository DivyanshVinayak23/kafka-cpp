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
    uint16_t client_id_length;
};

struct api_version {
    int16_t api_key;
    int16_t min_version;
    int16_t max_version;
};

struct api_versions {
    int32_t size;
    api_version* array;
};

// Helper: write Kafka-style compact unsigned varint
void write_varint(std::ostream& os, uint32_t value) {
    while (true) {
        uint8_t byte = value & 0x7F;
        value >>= 7;
        if (value == 0) {
            os.put(byte);
            break;
        } else {
            os.put(byte | 0x80);
        }
    }
}

// Helper: write int16 little-endian (Kafka expects this)
void write_int16_le(std::ostream& os, int16_t value) {
    os.put(static_cast<char>(value & 0xFF));
    os.put(static_cast<char>((value >> 8) & 0xFF));
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
        header.client_id_length = ntohs(header.client_id_length);

        char* client_id = nullptr;
        if (header.client_id_length > 0) {
            client_id = new char[header.client_id_length];
            read(client_fd, client_id, header.client_id_length);
        }

        uint32_t remaining = size - sizeof(RequestHeader) - header.client_id_length;
        char* body = nullptr;
        if (remaining > 0) {
            body = new char[remaining];
            read(client_fd, body, remaining);
        }

        // Build response
        api_versions content;
        content.size = 1;
        content.array = new api_version[content.size];
        content.array[0].api_key = 18;  // ApiVersions
        content.array[0].min_version = 0;
        content.array[0].max_version = 4;

        std::stringstream response;
        uint32_t net_corr_id = htonl(header.correlation_id);
        response.write((char*)&net_corr_id, sizeof(net_corr_id));

        if (header.request_api_version >= 3) {
            // Version 3+ response format

            // 1. throttle_time_ms
            int32_t throttle = 0;
            response.write((char*)&throttle, sizeof(throttle));  // already little-endian

            // 2. Compact array of api_versions
            write_varint(response, content.size + 1);  // compact array length

            for (int i = 0; i < content.size; i++) {
                write_int16_le(response, content.array[i].api_key);
                write_int16_le(response, content.array[i].min_version);
                write_int16_le(response, content.array[i].max_version);
            }

            // 3. Empty tagged fields
            write_varint(response, 0);  // compact varint = 0
        } else {
            // Legacy response format (version < 3)
            int32_t api_count = htonl(content.size);
            response.write((char*)&api_count, sizeof(api_count));

            for (int i = 0; i < content.size; i++) {
                int16_t k = htons(content.array[i].api_key);
                int16_t min = htons(content.array[i].min_version);
                int16_t max = htons(content.array[i].max_version);
                response.write((char*)&k, sizeof(k));
                response.write((char*)&min, sizeof(min));
                response.write((char*)&max, sizeof(max));
            }
        }

        std::string res = response.str();
        uint32_t net_size = htonl(res.size());
        write(client_fd, &net_size, sizeof(net_size));
        write(client_fd, res.data(), res.size());

        delete[] content.array;
        if (client_id) delete[] client_id;
        if (body) delete[] body;
    }

    close(client_fd);
    close(server_fd);
    return 0;
}
