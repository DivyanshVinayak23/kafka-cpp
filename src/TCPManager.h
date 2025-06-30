#include <arpa/inet.h>
#include <bits/stdc++.h>
#include <netdb.h>
#include <sys/socket.h>
#include <sys/types.h>

struct Fd {
    explicit Fd(int _fd) : fd(_fd) {}
    Fd() = default;
    ~Fd();

    // Delete copy operations
    Fd(const Fd &) = delete;
    Fd &operator=(const Fd &) = delete;

    // Add move operations
    Fd(Fd &&other) noexcept : fd(other.fd) { other.fd = -1; }
    Fd &operator=(Fd &&other) noexcept;

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

    static NullableString fromBuffer(const char *buffer, size_t buffer_size);
    std::string_view toString() const;
};

struct TaggedFields {
    uint8_t fieldCount = 0;

    std::string toString() const;
};

struct RequestHeader : Header {
    constexpr static uint32_t MIN_HEADER_SIZE = 14;

    int16_t request_api_key{};
    int16_t request_api_version{};
    int32_t corellation_id{};
    NullableString client_id{};

    static RequestHeader fromBuffer(const char *buffer, size_t buffer_size);
    void fromBufferLocal(const char *buffer, size_t buffer_size);
    std::string toString() const;
};

struct ApiVersionsRequestMessage : RequestHeader {
    static ApiVersionsRequestMessage fromBuffer(const char *buffer,
                                                size_t buffer_size);
    std::string toString() const;
};

constexpr size_t MAX_BUFFER_SIZE = 1024;

struct ResponseHeader : Header {
    int32_t corellation_id{};
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

    std::string toBuffer() const;
    std::string toString() const;
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

    void createSocketAndListen();
    Fd acceptConnections() const;

    void writeBufferOnClientFd(const Fd &client_fd,
                               const auto &response_message) const;

    void readBufferFromClientFd(
        const Fd &client_fd,
        const std::function<void(const char *, const size_t)> &func) const;

  private:
    Fd server_fd;
};

struct KafkaApis {
    KafkaApis(const Fd &_client_fd, const TCPManager &_tcp_manager);
    ~KafkaApis() = default;

    static constexpr uint32_t UNSUPPORTED_VERSION = 35;
    static constexpr uint16_t API_VERSIONS_REQUEST = 18;

    void classifyRequest(const char *buf, const size_t buf_size) const;
    void checkApiVersions(const char *buf, const size_t buf_size) const;

  private:
    const Fd &client_fd;
    const TCPManager &tcp_manager;
};