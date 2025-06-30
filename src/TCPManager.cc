#include "TCPManager.h"

#include <netinet/tcp.h>
#include <unistd.h>

Fd &Fd::operator=(Fd &&other) noexcept {
    if (this != &other) {
        if (fd >= 0)
            close(fd);
        fd = other.fd;
        other.fd = -1;
    }
    return *this;
}

Fd::~Fd() {
    if (fd != -1) {
        std::cout << "Closing file descriptor " << fd << "\n";
        close(fd);
    } else {
        std::cout << "File descriptor already closed" << fd << "\n ";
    }
}

void hexdump(const void *data, size_t size) {
    const unsigned char *bytes = static_cast<const unsigned char *>(data);

    for (size_t i = 0; i < size; ++i) {
        std::cout << std::hex << std::setw(2) << std::setfill('0')
                  << static_cast<int>(bytes[i]) << " ";

        if ((i + 1) % 16 == 0) {
            std::cout << std::endl;
        }
    }

    std::cout << std::endl;
}

NullableString NullableString::fromBuffer(const char *buffer,
                                          size_t buffer_size) {
    if (buffer_size < sizeof(uint16_t)) [[unlikely]] {
        throw std::runtime_error("Buffer size is too small");
    }

    NullableString nullable_string;
    uint16_t length = ntohs(*reinterpret_cast<const uint16_t *>(buffer));

    if (length == -1) {
        return nullable_string;
    }

    nullable_string.value = std::string(buffer + sizeof(uint16_t), length);
    return nullable_string;
}

std::string_view NullableString::toString() const { return value; }

std::string TaggedFields::toString() const {
    return "TaggedFields{fieldCount=" + std::to_string(fieldCount) + "}";
}

void RequestHeader::fromBufferLocal(const char *buffer, size_t buffer_size) {
    if (buffer_size < RequestHeader::MIN_HEADER_SIZE) {
        throw std::runtime_error("Buffer size is too small");
    }

#pragma GCC diagnostic ignored "-Winvalid-offsetof"

#define READL(field)                                                           \
    field = ntohl(*reinterpret_cast<const decltype(RequestHeader::field) *>(   \
        buffer + offsetof(RequestHeader, field)));
#define READS(field)                                                           \
    field = ntohs(*reinterpret_cast<const decltype(RequestHeader::field) *>(   \
        buffer + offsetof(RequestHeader, field)));

    READL(message_size);
    READS(request_api_key);
    READS(request_api_version);
    READL(corellation_id);

    client_id = NullableString::fromBuffer(
        buffer + offsetof(RequestHeader, client_id),
        buffer_size - offsetof(RequestHeader, client_id));

#undef READL
#undef READS

#pragma diagnostic(pop)
}

RequestHeader RequestHeader::fromBuffer(const char *buffer,
                                        size_t buffer_size) {
    RequestHeader request_header;
    request_header.fromBufferLocal(buffer, buffer_size);
    return request_header;
}

ApiVersionsRequestMessage
ApiVersionsRequestMessage::fromBuffer(const char *buffer, size_t buffer_size) {
    if (buffer_size < ApiVersionsRequestMessage::MIN_HEADER_SIZE) {
        throw std::runtime_error("Buffer size is too small");
    }

    ApiVersionsRequestMessage api_versions_request_message;
    api_versions_request_message.fromBufferLocal(buffer, buffer_size);
    return api_versions_request_message;
}

std::string RequestHeader::toString() const {
    return "RequestHeader{message_size=" + std::to_string(message_size) +
           ", request_api_key=" + std::to_string(request_api_key) +
           ", request_api_version=" + std::to_string(request_api_version) +
           ", corellation_id=" + std::to_string(corellation_id) +
           ", client_id=" + std::string(client_id.toString()) + "}";
}

std::string ApiVersionsRequestMessage::toString() const {
    return "ApiVersionsRequestMessage{" + RequestHeader::toString() + "}";
}

std::string ApiVersionsResponseMessage::toBuffer() const {
    char buffer[sizeof(ApiVersionsResponseMessage)]{};

#pragma GCC diagnostic ignored "-Winvalid-offsetof"

#define FILL_BUFFERL(field)                                                    \
    *reinterpret_cast<decltype(field) *>(                                      \
        buffer + offsetof(ApiVersionsResponseMessage, field)) = htonl(field)

#define FILL_BUFFERS(field)                                                    \
    *reinterpret_cast<decltype(field) *>(                                      \
        buffer + offsetof(ApiVersionsResponseMessage, field)) = htons(field)

    *reinterpret_cast<decltype(message_size) *>(
        buffer + __builtin_offsetof(ApiVersionsResponseMessage, message_size)) =
        htonl(message_size - sizeof(message_size));

    FILL_BUFFERL(corellation_id);
    FILL_BUFFERS(error_code);

    *reinterpret_cast<uint8_t *>(
        buffer + offsetof(ApiVersionsResponseMessage, api_keys_count)) =
        api_keys_count;

    FILL_BUFFERS(api_key);
    FILL_BUFFERS(min_version);
    FILL_BUFFERS(max_version);

#undef FILL_BUFFERL
#undef FILL_BUFFERS

#pragma diagnostic(pop)

    return std::string(buffer, sizeof(buffer));
}

std::string ApiVersionsResponseMessage::toString() const {
    return "ApiVersionsResponseMessage{message_size=" +
           std::to_string(message_size) +
           ", corellation_id=" + std::to_string(corellation_id) +
           ", error_code=" + std::to_string(error_code) +
           ", api_key=" + std::to_string(api_key) +
           ", api_keys_count=" + std::to_string(api_keys_count) +
           ", min_version=" + std::to_string(min_version) +
           ", max_version=" + std::to_string(max_version) +
           ", throttle_time=" + std::to_string(throttle_time) +
           ", tagged_fields=" + tagged_fields.toString() +
           ", tagged_fields2=" + tagged_fields2.toString() + "}";
}

void TCPManager::createSocketAndListen() {
    server_fd.setFd(socket(AF_INET, SOCK_STREAM, 0));
    if (server_fd < 0) {
        perror("socket failed: ");
        throw std::runtime_error("Failed to create server socket: ");
    }

    // Since the tester restarts your program quite often, setting SO_REUSEADDR
    // ensures that we don't run into 'Address already in use' errors
    int reuse = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) <
        0) {
        perror("setsockopt failed: ");
        close(server_fd);
        throw std::runtime_error("setsockopt failed: ");
    }

    struct sockaddr_in server_addr = getSocketAddr();

    if (bind(server_fd, reinterpret_cast<struct sockaddr *>(&server_addr),
             sizeof(server_addr)) != 0) {
        perror("bind failed: ");
        close(server_fd);
        throw std::runtime_error("Failed to bind to port 9092");
    }

    std::cout << "Waiting for a client to connect...\n";

    int connection_backlog = 5;
    if (listen(server_fd, connection_backlog) != 0) {
        perror("listen failed: ");
        close(server_fd);
        throw std::runtime_error("listen failed");
    }

    // You can use print statements as follows for debugging, they'll be visible
    // when running tests.
    std::cerr << "Logs from your program will appear here!\n";
}

Fd TCPManager::acceptConnections() const {
    struct sockaddr_in client_addr {};
    socklen_t client_addr_len = sizeof(client_addr);

    struct sockaddr *addr = reinterpret_cast<struct sockaddr *>(&client_addr);
    Fd client_fd(accept(server_fd, addr, &client_addr_len));

    if (client_fd < 0) {
        perror("accept failed: ");
        throw std::runtime_error("Failed to accept connection: ");
    }

    std::cout << "Client connected\n";
    return client_fd;
}

void TCPManager::writeBufferOnClientFd(const Fd &client_fd,
                                       const auto &response_message) const {

    std::cout << "Sending msg to client: " << response_message.toString()
              << "\n";

    std::string buffer = response_message.toBuffer();

    // Write message Length
    if (send(client_fd, buffer.data(), sizeof(uint32_t), 0) !=
        sizeof(uint32_t)) {
        perror("send failed: ");
        throw std::runtime_error("Failed to send msgLen to client: ");
    }

    if (send(client_fd, buffer.data() + 4, buffer.size() - 4, 0) !=
        buffer.size() - 4) {
        perror("send failed: ");
        throw std::runtime_error("Failed to send msgLen to client: ");
    }

    std::cout << "Message sent to client: " << buffer.size() << " bytes\n";

    // Just flush the write buffer
    int optval = 1;
    setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &optval, sizeof(optval));
}

void TCPManager::readBufferFromClientFd(
    const Fd &client_fd,
    const std::function<void(const char *, const size_t)> &func) const {
    char buffer[MAX_BUFFER_SIZE];
    size_t bytes_received = recv(client_fd, buffer, MAX_BUFFER_SIZE, 0);

    if (bytes_received < 0) {
        perror("recv failed: ");
        throw std::runtime_error("Failed to read from client: ");
    }

    if (bytes_received == 0) {
        std::cout << "Client disconnected\n";
        return;
    }

    std::cout << "Received " << bytes_received << " bytes from client\n";
    func(buffer, bytes_received);
}

KafkaApis::KafkaApis(const Fd &_client_fd, const TCPManager &_tcp_manager)
    : client_fd(_client_fd), tcp_manager(_tcp_manager) {}

void KafkaApis::classifyRequest(const char *buf, const size_t buf_size) const {
    RequestHeader request_header = RequestHeader::fromBuffer(buf, buf_size);

    switch (request_header.request_api_key) {
    case API_VERSIONS_REQUEST:
        checkApiVersions(buf, buf_size);
        break;
    default:
        std::cout << "Unsupported API key: " << request_header.request_api_key
                  << "\n";
    }
}

void KafkaApis::checkApiVersions(const char *buf, const size_t buf_size) const {
    ApiVersionsRequestMessage request_message =
        ApiVersionsRequestMessage::fromBuffer(buf, buf_size);

    std::cout << "Received API Versions Request: " << request_message.toString()
              << "\n";

    ApiVersionsResponseMessage api_versions_response_message;
    api_versions_response_message.message_size =
        sizeof(ApiVersionsResponseMessage);
    api_versions_response_message.corellation_id =
        request_message.corellation_id;

    if (request_message.request_api_version < 0 ||
        request_message.request_api_version > 4) {
        api_versions_response_message.error_code = UNSUPPORTED_VERSION;
        std::cout << "Unsupported version: "
                  << request_message.request_api_version << "\n";
    } else {
        std::cout << "Supported version: "
                  << request_message.request_api_version << "\n";
        api_versions_response_message.api_keys_count = 2;
        api_versions_response_message.api_key = API_VERSIONS_REQUEST;
        api_versions_response_message.min_version = 3;
        api_versions_response_message.max_version = 4;
    }

    tcp_manager.writeBufferOnClientFd(client_fd, api_versions_response_message);
}