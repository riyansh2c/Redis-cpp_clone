#include <iostream>
#include <string>
#include <vector>
#include <sstream>
#include <unordered_map>
#include <unordered_set>
#include <chrono>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <algorithm>
#include <cstdint>
#include <cstring>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <unistd.h>

// Replication & Server Configuration
std::string server_role = "master";
int server_port = 6379;
std::string master_host = "";
int master_port = 0;

// Client Transaction and Watch State
struct ClientState {
    int client_fd;
    bool in_multi = false;
    bool transaction_dirty = false;
    std::vector<std::vector<std::string>> queued_commands;
    std::unordered_set<std::string> watched_keys;
};

// Stream Storage Data Structures
struct StreamEntry {
    std::string id;
    std::vector<std::pair<std::string, std::string>> kv_pairs;
};

// Global Memory Stores and Registries
std::unordered_map<std::string, std::string> kv_store;
std::unordered_map<std::string, std::chrono::time_point<std::chrono::steady_clock>> expiry_store;
std::unordered_map<std::string, std::vector<StreamEntry>> stream_store;

inline std::unordered_map<std::string, std::unordered_set<int>> watched_keys_registry;
inline std::unordered_map<int, ClientState> clients_registry;

std::mutex db_mutex;
std::condition_variable stream_cv;

// Helper: Parse stream IDs (e.g., "1000-1" or "*")
void parse_stream_id(const std::string& id_str, uint64_t& ms, uint64_t& seq, bool& auto_seq) {
    auto_seq = false;
    size_t dash = id_str.find('-');
    if (dash != std::string::npos) {
        ms = std::stoull(id_str.substr(0, dash));
        std::string seq_part = id_str.substr(dash + 1);
        if (seq_part == "*") {
            auto_seq = true;
            seq = 0;
        } else {
            seq = std::stoull(seq_part);
        }
    } else {
        ms = std::stoull(id_str);
        seq = 0;
    }
}

// Invalidate watched keys across all watching clients
void touchKey(const std::string& key) {
    auto it = watched_keys_registry.find(key);
    if (it != watched_keys_registry.end()) {
        for (int fd : it->second) {
            if (clients_registry.count(fd)) {
                clients_registry[fd].transaction_dirty = true;
            }
        }
    }
}

// Clean up watched key subscriptions for a given client
void cleanupClientWatches(ClientState& client) {
    for (const std::string& key : client.watched_keys) {
        auto it = watched_keys_registry.find(key);
        if (it != watched_keys_registry.end()) {
            it->second.erase(client.client_fd);
            if (it->second.empty()) {
                watched_keys_registry.erase(it);
            }
        }
    }
    client.watched_keys.clear();
}

std::string handleWatch(ClientState& client, const std::vector<std::string>& args) {
    if (args.size() < 2) {
        return "-ERR wrong number of arguments for 'watch' command\r\n";
    }
    if (client.in_multi) {
        return "-ERR WATCH inside MULTI is not allowed\r\n";
    }
    for (size_t i = 1; i < args.size(); ++i) {
        const std::string& key = args[i];
        client.watched_keys.insert(key);
        watched_keys_registry[key].insert(client.client_fd);
    }
    return "+OK\r\n";
}

std::string handleUnwatch(ClientState& client) {
    cleanupClientWatches(client);
    return "+OK\r\n";
}

// Primary execution router for standard commands
std::string execute_command(const std::vector<std::string>& tokens) {
    if (tokens.empty()) return "";

    std::string command = tokens[0];
    std::transform(command.begin(), command.end(), command.begin(), ::toupper);

    if (command == "PING") {
        return "+PONG\r\n";
    }
    else if (command == "ECHO" && tokens.size() >= 2) {
        return "$" + std::to_string(tokens[1].length()) + "\r\n" + tokens[1] + "\r\n";
    }
    else if (command == "INFO") {
        std::string section = (tokens.size() >= 2) ? tokens[1] : "default";
        std::transform(section.begin(), section.end(), section.begin(), ::toupper);

        std::string info_body = "";
        info_body += "# Replication\r\n";
        info_body += "role:" + server_role + "\r\n";
        info_body += "connected_replicas:0\r\n";
        info_body += "master_replid:8371b4ed115d9771a04d4a1636540b76fc1a1425\r\n";
        info_body += "master_repl_offset:0\r\n";

        return "$" + std::to_string(info_body.length()) + "\r\n" + info_body + "\r\n";
    }
    else if (command == "REPLCONF") {
        return "+OK\r\n";
    }
    else if (command == "PSYNC") {
        return "+FULLRESYNC 8371b4ed115d9771a04d4a1636540b76fc1a1425 0\r\n";
    }
    else if (command == "SET" && tokens.size() >= 3) {
        std::lock_guard<std::mutex> lock(db_mutex);
        kv_store[tokens[1]] = tokens[2];
        if (tokens.size() >= 5) {
            std::string opt = tokens[3];
            std::transform(opt.begin(), opt.end(), opt.begin(), ::toupper);
            if (opt == "PX") {
                int px = std::stoi(tokens[4]);
                expiry_store[tokens[1]] = std::chrono::steady_clock::now() + std::chrono::milliseconds(px);
            }
        }
        touchKey(tokens[1]);
        return "+OK\r\n";
    }
    else if (command == "GET" && tokens.size() >= 2) {
        std::lock_guard<std::mutex> lock(db_mutex);
        std::string key = tokens[1];
        if (expiry_store.count(key) && std::chrono::steady_clock::now() > expiry_store[key]) {
            kv_store.erase(key);
            expiry_store.erase(key);
        }
        if (kv_store.count(key)) {
            std::string val = kv_store[key];
            return "$" + std::to_string(val.length()) + "\r\n" + val + "\r\n";
        } else {
            return "$-1\r\n";
        }
    }
    else if (command == "INCR" && tokens.size() >= 2) {
        std::lock_guard<std::mutex> lock(db_mutex);
        std::string key = tokens[1];
        int current_val = 0;

        if (kv_store.count(key)) {
            try {
                current_val = std::stoi(kv_store[key]);
            } catch (...) {
                return "-ERR value is not an integer or out of range\r\n";
            }
        }

        current_val++;
        kv_store[key] = std::to_string(current_val);
        touchKey(key);
        return ":" + std::to_string(current_val) + "\r\n";
    }
    else if (command == "TYPE" && tokens.size() >= 2) {
        std::lock_guard<std::mutex> lock(db_mutex);
        std::string key = tokens[1];

        if (expiry_store.count(key) && std::chrono::steady_clock::now() > expiry_store[key]) {
            kv_store.erase(key);
            expiry_store.erase(key);
        }

        if (kv_store.count(key)) {
            return "+string\r\n";
        } 
        else if (stream_store.count(key) && !stream_store[key].empty()) {
            return "+stream\r\n";
        } 
        else {
            return "+none\r\n";
        }
    }
    else if (command == "XADD" && tokens.size() >= 5) {
        std::string key = tokens[1];
        std::string id_str = tokens[2];

        uint64_t ms = 0, seq = 0;
        bool auto_seq = false;

        if (id_str == "*") {
            ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::system_clock::now().time_since_epoch()).count();
            auto_seq = true;
        } else {
            parse_stream_id(id_str, ms, seq, auto_seq);
        }

        {
            std::lock_guard<std::mutex> lock(db_mutex);
            auto& stream = stream_store[key];

            if (id_str == "*" || auto_seq) {
                uint64_t top_ms = 0, top_seq = 0;
                if (!stream.empty()) {
                    bool dummy;
                    parse_stream_id(stream.back().id, top_ms, top_seq, dummy);
                }
                if (ms == top_ms && !stream.empty()) {
                    seq = top_seq + 1;
                } else if (ms == 0) {
                    seq = 1;
                } else {
                    seq = 0;
                }
                id_str = std::to_string(ms) + "-" + std::to_string(seq);
            }

            if (ms == 0 && seq == 0) {
                return "-ERR The ID specified in XADD must be greater than 0-0\r\n";
            }

            if (!stream.empty()) {
                uint64_t top_ms = 0, top_seq = 0;
                bool dummy;
                parse_stream_id(stream.back().id, top_ms, top_seq, dummy);

                if (ms < top_ms || (ms == top_ms && seq <= top_seq)) {
                    return "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n";
                }
            }

            StreamEntry entry;
            entry.id = id_str;
            for (size_t i = 3; i < tokens.size(); i += 2) {
                if (i + 1 < tokens.size()) {
                    entry.kv_pairs.push_back({tokens[i], tokens[i + 1]});
                }
            }

            stream.push_back(entry);
            touchKey(key);
        }

        stream_cv.notify_all();
        return "$" + std::to_string(id_str.length()) + "\r\n" + id_str + "\r\n";
    }
    else if (command == "XRANGE" && tokens.size() >= 4) {
        std::string key = tokens[1];
        std::string start_str = tokens[2];
        std::string end_str = tokens[3];

        uint64_t start_ms = 0, start_seq = 0;
        uint64_t end_ms = UINT64_MAX, end_seq = UINT64_MAX;

        if (start_str != "-") {
            size_t dash = start_str.find('-');
            if (dash != std::string::npos) {
                start_ms = std::stoull(start_str.substr(0, dash));
                start_seq = std::stoull(start_str.substr(dash + 1));
            } else {
                start_ms = std::stoull(start_str);
                start_seq = 0;
            }
        }

        if (end_str != "+") {
            size_t dash = end_str.find('-');
            if (dash != std::string::npos) {
                end_ms = std::stoull(end_str.substr(0, dash));
                end_seq = std::stoull(end_str.substr(dash + 1));
            } else {
                end_ms = std::stoull(end_str);
                end_seq = UINT64_MAX;
            }
        }

        std::string response = "";
        size_t count = 0;

        {
            std::lock_guard<std::mutex> lock(db_mutex);
            if (stream_store.count(key)) {
                for (const auto& entry : stream_store[key]) {
                    uint64_t entry_ms = 0, entry_seq = 0;
                    bool dummy;
                    parse_stream_id(entry.id, entry_ms, entry_seq, dummy);

                    bool ge_start = (entry_ms > start_ms) || (entry_ms == start_ms && entry_seq >= start_seq);
                    bool le_end = (entry_ms < end_ms) || (entry_ms == end_ms && entry_seq <= end_seq);

                    if (ge_start && le_end) {
                        count++;
                        response += "*2\r\n";
                        response += "$" + std::to_string(entry.id.length()) + "\r\n" + entry.id + "\r\n";
                        response += "*" + std::to_string(entry.kv_pairs.size() * 2) + "\r\n";
                        for (const auto& kv : entry.kv_pairs) {
                            response += "$" + std::to_string(kv.first.length()) + "\r\n" + kv.first + "\r\n";
                            response += "$" + std::to_string(kv.second.length()) + "\r\n" + kv.second + "\r\n";
                        }
                    }
                }
            }
        }

        return "*" + std::to_string(count) + "\r\n" + response;
    }

    return "-ERR unknown command\r\n";
}

std::string handleExec(ClientState& client) {
    if (!client.in_multi) {
        return "-ERR EXEC without MULTI\r\n";
    }

    if (client.transaction_dirty) {
        client.queued_commands.clear();
        client.in_multi = false;
        client.transaction_dirty = false;
        cleanupClientWatches(client);
        return "*-1\r\n";
    }

    std::string response = "*" + std::to_string(client.queued_commands.size()) + "\r\n";
    for (const auto& queued_tokens : client.queued_commands) {
        response += execute_command(queued_tokens);
    }

    client.queued_commands.clear();
    client.in_multi = false;
    cleanupClientWatches(client);

    return response;
}

std::string handleDiscard(ClientState& client) {
    if (!client.in_multi) {
        return "-ERR DISCARD without MULTI\r\n";
    }

    client.queued_commands.clear();
    client.in_multi = false;
    client.transaction_dirty = false;
    cleanupClientWatches(client);

    return "+OK\r\n";
}

void handle_client(int client_fd) {
    {
        std::lock_guard<std::mutex> lock(db_mutex);
        clients_registry[client_fd] = ClientState{client_fd};
    }

    char buffer[4096];

    while (true) {
        ssize_t bytes_received = recv(client_fd, buffer, sizeof(buffer) - 1, 0);
        if (bytes_received <= 0) break;
        buffer[bytes_received] = '\0';

        std::stringstream ss(buffer);
        std::string line;
        std::vector<std::string> tokens;

        if (std::getline(ss, line) && !line.empty() && line[0] == '*') {
            int num_tokens = std::stoi(line.substr(1));
            for (int i = 0; i < num_tokens; ++i) {
                std::getline(ss, line); // $len
                std::getline(ss, line); // token text
                if (!line.empty() && line.back() == '\r') line.pop_back();
                tokens.push_back(line);
            }
        }

        if (tokens.empty()) continue;

        std::string command = tokens[0];
        std::transform(command.begin(), command.end(), command.begin(), ::toupper);

        db_mutex.lock();
        ClientState& client = clients_registry[client_fd];
        db_mutex.unlock();

        std::string res;

        if (command == "WATCH") {
            std::lock_guard<std::mutex> lock(db_mutex);
            res = handleWatch(client, tokens);
        }
        else if (command == "UNWATCH") {
            std::lock_guard<std::mutex> lock(db_mutex);
            res = handleUnwatch(client);
        }
        else if (command == "MULTI") {
            std::lock_guard<std::mutex> lock(db_mutex);
            if (client.in_multi) {
                res = "-ERR MULTI calls can not be nested\r\n";
            } else {
                client.in_multi = true;
                client.queued_commands.clear();
                res = "+OK\r\n";
            }
        }
        else if (command == "EXEC") {
            res = handleExec(client);
        }
        else if (command == "DISCARD") {
            std::lock_guard<std::mutex> lock(db_mutex);
            res = handleDiscard(client);
        }
        else if (client.in_multi) {
            std::lock_guard<std::mutex> lock(db_mutex);
            client.queued_commands.push_back(tokens);
            res = "+QUEUED\r\n";
        }
        else {
            res = execute_command(tokens);
        }

        if (!res.empty()) {
            send(client_fd, res.c_str(), res.length(), 0);
        }
    }

    {
        std::lock_guard<std::mutex> lock(db_mutex);
        cleanupClientWatches(clients_registry[client_fd]);
        clients_registry.erase(client_fd);
    }
    close(client_fd);
}

void initiate_handshake() {
    int master_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (master_fd < 0) return;

    sockaddr_in master_addr{};
    master_addr.sin_family = AF_INET;
    master_addr.sin_port = htons(master_port);
    inet_pton(AF_INET, master_host.c_str(), &master_addr.sin_addr);

    if (connect(master_fd, (struct sockaddr*)&master_addr, sizeof(master_addr)) < 0) {
        std::cerr << "Failed to connect to master at " << master_host << ":" << master_port << std::endl;
        close(master_fd);
        return;
    }

    char buf[512];

    // 1. Send PING
    std::string ping_cmd = "*1\r\n$4\r\nPING\r\n";
    send(master_fd, ping_cmd.c_str(), ping_cmd.length(), 0);
    recv(master_fd, buf, sizeof(buf), 0);

    // 2. Send REPLCONF listening-port
    std::string port_str = std::to_string(server_port);
    std::string replconf1 = "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$" + 
                            std::to_string(port_str.length()) + "\r\n" + port_str + "\r\n";
    send(master_fd, replconf1.c_str(), replconf1.length(), 0);
    recv(master_fd, buf, sizeof(buf), 0);

    // 3. Send REPLCONF capa psync2
    std::string replconf2 = "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n";
    send(master_fd, replconf2.c_str(), replconf2.length(), 0);
    recv(master_fd, buf, sizeof(buf), 0);

    // 4. Send PSYNC ? -1
    std::string psync = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";
    send(master_fd, psync.c_str(), psync.length(), 0);
    recv(master_fd, buf, sizeof(buf), 0);

    std::thread(handle_client, master_fd).detach();
}

int main(int argc, char* argv[]) {
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--port" && i + 1 < argc) {
            server_port = std::stoi(argv[++i]);
        } else if (arg == "--replicaof" && i + 1 < argc) {
            server_role = "replica";
            std::string replica_arg = argv[++i];

            std::stringstream ss(replica_arg);
            if (ss >> master_host) {
                if (!(ss >> master_port) && i + 1 < argc) {
                    master_port = std::stoi(argv[++i]);
                }
            }
        }
    }

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) return 1;

    int reuse = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

    sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(server_port);

    if (bind(server_fd, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) return 1;
    if (listen(server_fd, 5) < 0) return 1;

    std::cout << "Redis TCP Server (" << server_role << ") listening on port " << server_port << "..." << std::endl;

    if (server_role == "replica") {
        std::thread(initiate_handshake).detach();
    }

    while (true) {
        sockaddr_in client_addr{};
        socklen_t client_len = sizeof(client_addr);
        int client_fd = accept(server_fd, (struct sockaddr*)&client_addr, &client_len);
        if (client_fd >= 0) {
            std::thread(handle_client, client_fd).detach();
        }
    }

    close(server_fd);
    return 0;
}