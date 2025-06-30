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

// Helper: write Kafka-style compact unsigned varint (LE base‑128)
void write_varint(std::ostream& os, uint32_t val) {
    while (true) {
        uint8_t b = val & 0x7F;
        val >>= 7;
        if (val) {
            os.put(b | 0x80);
        } else {
            os.put(b);
            break;
        }
    }
}

int main() {
    std::cerr << std::unitbuf;
    std::cout << std::unitbuf;

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &(int){1}, sizeof(int));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(9092);
    addr.sin_addr.s_addr = INADDR_ANY;
    bind(server_fd, (sockaddr*)&addr, sizeof(addr));
    listen(server_fd, 5);
    std::cerr << "Logs from your program will appear here!\n";

    sockaddr_in client_addr{};
    socklen_t client_len = sizeof(client_addr);
    int client_fd = accept(server_fd, (sockaddr*)&client_addr, &client_len);
    std::cout << "Client connected\n";

    while (true) {
        uint32_t msg_size_net;
        if (read(client_fd, &msg_size_net, 4) != 4) break;
        uint32_t msg_size = ntohl(msg_size_net);

        RequestHeader hdr;
        if (read(client_fd, &hdr, sizeof(hdr)) != sizeof(hdr)) break;
        hdr.request_api_key     = ntohs(hdr.request_api_key);
        hdr.request_api_version = ntohs(hdr.request_api_version);
        hdr.correlation_id      = ntohl(hdr.correlation_id);
        hdr.client_id_length    = ntohs(hdr.client_id_length);

        if (hdr.client_id_length) {
            delete[] new char[hdr.client_id_length];
            read(client_fd, new char[hdr.client_id_length], hdr.client_id_length);
        }
        uint32_t body_len = msg_size - sizeof(hdr) - hdr.client_id_length;
        if (body_len) {
            delete[] new char[body_len];
            read(client_fd, new char[body_len], body_len);
        }

        // Prepare ApiVersions response
        api_versions resp;
        resp.size = 1;
        resp.array = new api_version[1];
        resp.array[0].api_key     = 18;
        resp.array[0].min_version = 0;
        resp.array[0].max_version = 4;

        std::stringstream ss;
        // 1) correlation_id
        uint32_t corr_net = htonl(hdr.correlation_id);
        ss.write((char*)&corr_net, 4);

        if (hdr.request_api_version >= 3) {
            // 2) throttle_time_ms
            int32_t throttle_net = htonl(0);
            ss.write((char*)&throttle_net, 4);

            // 3) compact array length = N+1
            write_varint(ss, resp.size + 1);

            // 4) each api_version entry, **network byte order**
            for (int i = 0; i < resp.size; i++) {
                int16_t k_net   = htons(resp.array[i].api_key);
                int16_t min_net = htons(resp.array[i].min_version);
                int16_t max_net = htons(resp.array[i].max_version);
                ss.write((char*)&k_net,   2);
                ss.write((char*)&min_net, 2);
                ss.write((char*)&max_net, 2);
            }

            // 5) empty tagged fields
            write_varint(ss, 0);
        } else {
            // legacy < v3
            int32_t cnt_net = htonl(resp.size);
            ss.write((char*)&cnt_net, 4);
            for (int i = 0; i < resp.size; i++) {
                int16_t k_net   = htons(resp.array[i].api_key);
                int16_t min_net = htons(resp.array[i].min_version);
                int16_t max_net = htons(resp.array[i].max_version);
                ss.write((char*)&k_net,   2);
                ss.write((char*)&min_net, 2);
                ss.write((char*)&max_net, 2);
            }
        }

        // Write size + payload
        std::string payload = ss.str();
        uint32_t out_size = htonl((uint32_t)payload.size());
        write(client_fd, &out_size, 4);
        write(client_fd, payload.data(), payload.size());

        delete[] resp.array;
    }

    close(client_fd);
    close(server_fd);
    return 0;
}
