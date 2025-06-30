#include <arpa/inet.h>
#include <iostream>
#include <string>
#include <vector>
#include <memory>
#include <stdexcept>
#include <netdb.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <signal.h>
#include <functional>
#include <unistd.h>
#include <cstring>
#include <cstdint>

struct Fd {
    explicit Fd(int _fd) : fd(_fd) {}
    Fd() = default;
    ~Fd() {
        if (fd >= 0) {
            close(fd);
        }
    }

    // Delete copy operations
    Fd(const Fd &) = delete;
    Fd &operator=(const Fd &) = delete;

    // Add move operations
    Fd(Fd &&other) noexcept : fd(other.fd) { other.fd = -1; }
    Fd &operator=(Fd &&other) noexcept {
        if (this != &other) {
            if (fd >= 0) {
                close(fd);
            }
            fd = other.fd;
            other.fd = -1;
        }
        return *this;
    }

    void setFd(int _fd) { fd = _fd; }
    int getFd() const { return fd; }
    operator int() const { return fd; }

  private:
    int fd = -1;
};

#pragma pack(push, 1)
struct Header {
    uint32_t message_size{};
};

struct NullableString {
    std::string value = "";

    static NullableString fromBuffer(const char *buffer, size_t buffer_size) {
        NullableString result;
        if (buffer_size >= 2) {
            int16_t length = ntohs(*reinterpret_cast<const int16_t*>(buffer));
            if (length > 0 && buffer_size >= 2 + length) {
                result.value = std::string(buffer + 2, length);
            }
        }
        return result;
    }
    
    std::string_view toString() const {
        return value;
    }
};

struct TaggedFields {
    uint8_t fieldCount = 0;

    std::string toString() const {
        return "TaggedFields{fieldCount=" + std::to_string(fieldCount) + "}";
    }
};

struct RequestHeader : Header {
    constexpr static uint32_t MIN_HEADER_SIZE = 14;

    int16_t request_api_key{};
    int16_t request_api_version{};
    int32_t correlation_id{};
    NullableString client_id{};

    static RequestHeader fromBuffer(const char *buffer, size_t buffer_size) {
        RequestHeader header;
        header.fromBufferLocal(buffer, buffer_size);
        return header;
    }
    
    void fromBufferLocal(const char *buffer, size_t buffer_size) {
        if (buffer_size < MIN_HEADER_SIZE) {
            throw std::runtime_error("Buffer too small for RequestHeader");
        }
        
        message_size = ntohl(*reinterpret_cast<const uint32_t*>(buffer));
        request_api_key = ntohs(*reinterpret_cast<const int16_t*>(buffer + 4));
        request_api_version = ntohs(*reinterpret_cast<const int16_t*>(buffer + 6));
        correlation_id = ntohl(*reinterpret_cast<const int32_t*>(buffer + 8));
        
        int16_t client_id_length = ntohs(*reinterpret_cast<const int16_t*>(buffer + 12));
        if (client_id_length > 0 && buffer_size >= MIN_HEADER_SIZE + client_id_length) {
            client_id.value = std::string(buffer + MIN_HEADER_SIZE, client_id_length);
        }
    }
    
    std::string toString() const {
        return "RequestHeader{api_key=" + std::to_string(request_api_key) + 
               ", api_version=" + std::to_string(request_api_version) + 
               ", correlation_id=" + std::to_string(correlation_id) + 
               ", client_id=" + client_id.toString() + "}";
    }
};

struct ApiVersionsRequestMessage : RequestHeader {
    static ApiVersionsRequestMessage fromBuffer(const char *buffer, size_t buffer_size) {
        ApiVersionsRequestMessage msg;
        msg.fromBufferLocal(buffer, buffer_size);
        return msg;
    }
    
    std::string toString() const {
        return "ApiVersionsRequestMessage{" + RequestHeader::toString() + "}";
    }
};

constexpr size_t MAX_BUFFER_SIZE = 1024;

struct ResponseHeader : Header {
    int32_t correlation_id{};
};

struct ApiVersionsResponseMessage : ResponseHeader {
    int16_t error_code{};
    uint8_t api_keys_count{};
    int16_t api_key{};
    int16_t min_version{};
    int16_t max_version{};

    TaggedFields tagged_fields{};
    int32_t throttle_time = 0;
    TaggedFields tagged_fields2{};

    std::string toBuffer() const {
        std::string buffer;
        
        // Calculate total size
        uint32_t total_size = sizeof(correlation_id) + sizeof(error_code) + 
                             sizeof(throttle_time) + sizeof(api_keys_count) +
                             sizeof(api_key) + sizeof(min_version) + sizeof(max_version) +
                             sizeof(tagged_fields.fieldCount) + sizeof(tagged_fields2.fieldCount);
        
        // Message size (4 bytes)
        uint32_t network_size = htonl(total_size);
        buffer.append(reinterpret_cast<const char*>(&network_size), sizeof(network_size));
        
        // Correlation ID (4 bytes)
        int32_t network_correlation = htonl(correlation_id);
        buffer.append(reinterpret_cast<const char*>(&network_correlation), sizeof(network_correlation));
        
        // Error code (2 bytes)
        int16_t network_error = htons(error_code);
        buffer.append(reinterpret_cast<const char*>(&network_error), sizeof(network_error));
        
        // Throttle time (4 bytes)
        int32_t network_throttle = htonl(throttle_time);
        buffer.append(reinterpret_cast<const char*>(&network_throttle), sizeof(network_throttle));
        
        // API keys count (1 byte)
        buffer.append(reinterpret_cast<const char*>(&api_keys_count), sizeof(api_keys_count));
        
        // API key (2 bytes)
        int16_t network_api_key = htons(api_key);
        buffer.append(reinterpret_cast<const char*>(&network_api_key), sizeof(network_api_key));
        
        // Min version (2 bytes)
        int16_t network_min_version = htons(min_version);
        buffer.append(reinterpret_cast<const char*>(&network_min_version), sizeof(network_min_version));
        
        // Max version (2 bytes)
        int16_t network_max_version = htons(max_version);
        buffer.append(reinterpret_cast<const char*>(&network_max_version), sizeof(network_max_version));
        
        // Tagged fields (1 byte)
        buffer.append(reinterpret_cast<const char*>(&tagged_fields.fieldCount), sizeof(tagged_fields.fieldCount));
        
        // Tagged fields 2 (1 byte)
        buffer.append(reinterpret_cast<const char*>(&tagged_fields2.fieldCount), sizeof(tagged_fields2.fieldCount));
        
        return buffer;
    }
    
    std::string toString() const {
        return "ApiVersionsResponseMessage{correlation_id=" + std::to_string(correlation_id) + 
               ", error_code=" + std::to_string(error_code) + 
               ", api_keys_count=" + std::to_string(api_keys_count) + 
               ", api_key=" + std::to_string(api_key) + 
               ", min_version=" + std::to_string(min_version) + 
               ", max_version=" + std::to_string(max_version) + "}";
    }
};

#pragma pack(pop)

struct TCPManager {
    TCPManager() = default;

    static struct sockaddr_in getSocketAddr() {
        struct sockaddr_in server_addr {
            .sin_family = AF_INET, .sin_port = htons(9092),
        };
        server_addr.sin_addr.s_addr = INADDR_ANY;
        return server_addr;
    }

    void createSocketAndListen() {
        server_fd.setFd(socket(AF_INET, SOCK_STREAM, 0));
        if (server_fd.getFd() < 0) {
            throw std::runtime_error("Failed to create server socket");
        }

        // Since the tester restarts your program quite often, setting SO_REUSEADDR
        // ensures that we don't run into 'Address already in use' errors
        int reuse = 1;
        if (setsockopt(server_fd.getFd(), SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
            throw std::runtime_error("setsockopt failed");
        }

        struct sockaddr_in server_addr = getSocketAddr();

        if (bind(server_fd.getFd(), reinterpret_cast<struct sockaddr*>(&server_addr), sizeof(server_addr)) != 0) {
            throw std::runtime_error("Failed to bind to port 9092");
        }

        int connection_backlog = 5;
        if (listen(server_fd.getFd(), connection_backlog) != 0) {
            throw std::runtime_error("listen failed");
        }

        std::cout << "Waiting for a client to connect...\n";
    }
    
    Fd acceptConnections() const {
        struct sockaddr_in client_addr{};
        socklen_t client_addr_len = sizeof(client_addr);

        Fd client_fd(accept(server_fd.getFd(), reinterpret_cast<struct sockaddr*>(&client_addr), &client_addr_len));
        if (client_fd.getFd() < 0) {
            throw std::runtime_error("accept failed");
        }
        
        std::cout << "Client connected\n";
        return client_fd;
    }

    void writeBufferOnClientFd(const Fd &client_fd, const auto &response_message) const {
        std::string buffer = response_message.toBuffer();
        ssize_t written = write(client_fd.getFd(), buffer.data(), buffer.size());
        if (written != static_cast<ssize_t>(buffer.size())) {
            throw std::runtime_error("Failed to write complete response");
        }
    }

    void readBufferFromClientFd(const Fd &client_fd, const std::function<void(const char *, const size_t)> &func) const {
        char buffer[MAX_BUFFER_SIZE];
        ssize_t received_bytes = read(client_fd.getFd(), buffer, sizeof(buffer));
        
        if (received_bytes <= 0) {
            throw std::runtime_error("Connection closed or read error");
        }
        
        func(buffer, static_cast<size_t>(received_bytes));
    }

  private:
    Fd server_fd;
};

struct KafkaApis {
    KafkaApis(const Fd &_client_fd, const TCPManager &_tcp_manager) 
        : client_fd(_client_fd), tcp_manager(_tcp_manager) {}
    ~KafkaApis() = default;

    static constexpr uint32_t UNSUPPORTED_VERSION = 35;
    static constexpr uint16_t API_VERSIONS_REQUEST = 18;

    void classifyRequest(const char *buf, const size_t buf_size) const {
        if (buf_size < RequestHeader::MIN_HEADER_SIZE) {
            throw std::runtime_error("Buffer too small for request classification");
        }
        
        int16_t api_key = ntohs(*reinterpret_cast<const int16_t*>(buf + 4));
        
        if (api_key == API_VERSIONS_REQUEST) {
            checkApiVersions(buf, buf_size);
        } else {
            throw std::runtime_error("Unsupported API key: " + std::to_string(api_key));
        }
    }
    
    void checkApiVersions(const char *buf, const size_t buf_size) const {
        ApiVersionsRequestMessage request = ApiVersionsRequestMessage::fromBuffer(buf, buf_size);
        
        ApiVersionsResponseMessage response;
        response.correlation_id = request.correlation_id;
        
        if (request.request_api_version > 4) {
            // Unsupported version error
            response.error_code = UNSUPPORTED_VERSION;
            response.api_keys_count = 0;
        } else {
            // Success response
            response.error_code = 0;
            response.api_keys_count = 1;
            response.api_key = API_VERSIONS_REQUEST;
            response.min_version = 0;
            response.max_version = 4;
        }
        
        tcp_manager.writeBufferOnClientFd(client_fd, response);
    }

  private:
    const Fd &client_fd;
    const TCPManager &tcp_manager;
};

namespace {
std::function<void(int)> shutdown_handler;
void signal_handler(int signal) { shutdown_handler(signal); }
} // namespace

int main(int argc, char *argv[]) {
    // Disable output buffering
    std::cout << std::unitbuf;
    std::cerr << std::unitbuf;

    // You can use print statements as follows for debugging, they'll be visible when running tests.
    std::cerr << "Logs from your program will appear here!\n";

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
                break;
            }
        }
    } catch (const std::exception &e) {
        std::cerr << "Error: " << e.what() << '\n';
        return 1;
    }

    return 0;
}