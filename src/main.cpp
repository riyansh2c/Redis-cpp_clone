#include <iostream>
#include <string>
#include <vector>
#include <sstream>
#include <thread>
#include <mutex>
#include <algorithm>
#include <cstring>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

// Global state configuration
struct ServerConfig {
    int port = 6379;
    std::string role = "master";
    std::string master_host = "";
    int master_port = 0;
    std::string master_replid = "8371b4ed115d9771a04d4a1636540b76fc1a1425";
    int master_repl_offset = 0;
};

ServerConfig g_config;
std::mutex g_config_mutex;

// Raw 88-byte hex string representing a valid empty RDB file
const std::string EMPTY_RDB_HEX = "524544495330303131fe00ff52a2e19d6b240179";

// Convert hex string into raw binary string
std::string hex_to_bytes(const std::string& hex) {
    std::string bytes;
    for (size_t i = 0; i < hex.length(); i += 2) {
        std::string byteString = hex.substr(i, 2);
        char byte = (char) strtol(byteString.c_str(), nullptr, 16);
        bytes.push_back(byte);
    }
    return bytes;
}

// Convert input text to uppercase for command parsing
std::string to_upper(std::string str) {
    std::transform(str.begin(), str.end(), str.begin(), ::toupper);
    return str;
}

// Parse RESP Array strings into vector tokens
std::vector<std::string> parse_resp_array(const std::string& buffer) {
    std::vector<std::string> tokens;
    if (buffer.empty() || buffer[0] != '*') return tokens;

    std::stringstream ss(buffer);
    std::string line;
    
    // Read array length header (*<num_elements>)
    std::getline(ss, line);
    
    while (std::getline(ss, line)) {
        if (line.empty()) continue;
        if (line[0] == '$') {
            // Read bulk string content line
            if (std::getline(ss, line)) {
                if (!line.empty() && line.back() == '\r') line.pop_back();
                tokens.push_back(line);
            }
        }
    }
    return tokens;
}

// Execute commands and return RESP response strings
std::string execute_command(const std::vector<std::string>& tokens, int client_fd) {
    if (tokens.empty()) return "";

    std::string cmd = to_upper(tokens[0]);

    if (cmd == "PING") {
        return "+PONG\r\n";
    } 
    else if (cmd == "ECHO" && tokens.size() > 1) {
        return "$" + std::to_string(tokens[1].length()) + "\r\n" + tokens[1] + "\r\n";
    }
    else if (cmd == "INFO") {
        std::string section = (tokens.size() > 1) ? to_upper(tokens[1]) : "";
        
        std::lock_guard<std::mutex> lock(g_config_mutex);
        std::string info_body = "# Replication\r\n";
        info_body += "role:" + g_config.role + "\r\n";
        info_body += "connected_replicas:0\r\n";
        info_body += "master_replid:" + g_config.master_replid + "\r\n";
        info_body += "master_repl_offset:" + std::to_string(g_config.master_repl_offset) + "\r\n";

        return "$" + std::to_string(info_body.length()) + "\r\n" + info_body + "\r\n";
    }
    else if (cmd == "REPLCONF") {
        return "+OK\r\n";
    }
    else if (cmd == "PSYNC") {
        std::string replid;
        {
            std::lock_guard<std::mutex> lock(g_config_mutex);
            replid = g_config.master_replid;
        }

        // 1. Return +FULLRESYNC header
        std::string resync_msg = "+FULLRESYNC " + replid + " 0\r\n";
        send(client_fd, resync_msg.c_str(), resync_msg.length(), 0);

        // 2. Decode and transmit empty RDB snapshot payload
        std::string rdb_bytes = hex_to_bytes(EMPTY_RDB_HEX);
        std::string rdb_header = "$" + std::to_string(rdb_bytes.length()) + "\r\n";

        send(client_fd, rdb_header.c_str(), rdb_header.length(), 0);
        send(client_fd, rdb_bytes.data(), rdb_bytes.length(), 0);

        // Already responded over socket, return empty string to avoid duplicate send
        return "";
    }

    return "-ERR unknown command\r\n";
}

// Background thread for replica handshake initialization
void initiate_handshake(const std::string& master_host, int master_port, int replica_port) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        std::cerr << "Failed to create socket for replication handshake\n";
        return;
    }

    struct sockaddr_in master_addr{};
    master_addr.sin_family = AF_INET;
    master_addr.sin_port = htons(master_port);
    inet_pton(AF_INET, master_host.c_str(), &master_addr.sin_addr);

    if (connect(sock, (struct sockaddr*)&master_addr, sizeof(master_addr)) < 0) {
        std::cerr << "Failed to connect to master for handshake\n";
        close(sock);
        return;
    }

    char buffer[1024];

    // Handshake Step 1: Send PING
    std::string ping_cmd = "*1\r\n$4\r\nPING\r\n";
    send(sock, ping_cmd.c_str(), ping_cmd.length(), 0);
    recv(sock, buffer, sizeof(buffer), 0);

    // Handshake Step 2a: Send REPLCONF listening-port
    std::string port_str = std::to_string(replica_port);
    std::string replconf1 = "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$" +
                            std::to_string(port_str.length()) + "\r\n" + port_str + "\r\n";
    send(sock, replconf1.c_str(), replconf1.length(), 0);
    recv(sock, buffer, sizeof(buffer), 0);

    // Handshake Step 2b: Send REPLCONF capa psync2
    std::string replconf2 = "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n";
    send(sock, replconf2.c_str(), replconf2.length(), 0);
    recv(sock, buffer, sizeof(buffer), 0);

    // Handshake Step 3: Send PSYNC ? -1
    std::string psync_cmd = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";
    send(sock, psync_cmd.c_str(), psync_cmd.length(), 0);
    recv(sock, buffer, sizeof(buffer), 0);

    close(sock);
}

// Handle client connection lifecycle
void handle_client(int client_fd) {
    char buffer[1024];
    while (true) {
        memset(buffer, 0, sizeof(buffer));
        ssize_t bytes_received = recv(client_fd, buffer, sizeof(buffer) - 1, 0);
        if (bytes_received <= 0) break;

        std::vector<std::string> tokens = parse_resp_array(std::string(buffer, bytes_received));
        std::string response = execute_command(tokens, client_fd);

        if (!response.empty()) {
            send(client_fd, response.c_str(), response.length(), 0);
        }
    }
    close(client_fd);
}

int main(int argc, char* argv[]) {
    // Parse CLI arguments (--port, --replicaof)
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--port" && i + 1 < argc) {
            g_config.port = std::stoi(argv[++i]);
        } else if (arg == "--replicaof" && i + 1 < argc) {
            g_config.role = "replica";
            std::string replica_arg = argv[++i];
            std::stringstream ss(replica_arg);
            ss >> g_config.master_host >> g_config.master_port;
        }
    }

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    int reuse = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

    struct sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(g_config.port);

    if (bind(server_fd, (struct sockaddr*)&server_addr, sizeof(server_addr)) != 0) {
        std::cerr << "Failed to bind to port " << g_config.port << "\n";
        return 1;
    }

    if (listen(server_fd, 5) != 0) {
        std::cerr << "Failed to listen on socket\n";
        return 1;
    }

    std::cout << "Redis TCP Server (" << g_config.role << ") listening on port " << g_config.port << "...\n";

    // Launch handshake background thread if configured as replica
    if (g_config.role == "replica") {
        std::thread(initiate_handshake, g_config.master_host, g_config.master_port, g_config.port).detach();
    }

    while (true) {
        struct sockaddr_in client_addr{};
        socklen_t client_len = sizeof(client_addr);
        int client_fd = accept(server_fd, (struct sockaddr*)&client_addr, &client_len);
        if (client_fd >= 0) {
            std::thread(handle_client, client_fd).detach();
        }
    }

    close(server_fd);
    return 0;
}