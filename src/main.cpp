#include <iostream>
#include <string>
#include <vector>
#include <unordered_map>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <chrono>
#include <algorithm>
#include <cstring>
#include <cstdint>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>

// Struct for Stream Entries
struct StreamEntry {
    std::string id;
    std::vector<std::pair<std::string, std::string>> kv_pairs;
};

// Global Stream Store: maps stream key -> list of entries
std::unordered_map<std::string, std::vector<StreamEntry>> stream_store;

// Struct for String values supporting PX expiration
struct Value {
    std::string data;
    std::chrono::time_point<std::chrono::steady_clock> expiry;
    bool has_expiry = false;
};

// Global Thread-Safe In-Memory Stores
std::mutex db_mutex;
std::condition_variable cv;

std::unordered_map<std::string, Value> kv_store;
std::unordered_map<std::string, std::vector<std::string>> list_store;
std::unordered_map<std::string, std::unordered_map<std::string, std::string>> hash_store;

// Helper: RESP Protocol Array Parser
std::vector<std::string> parse_resp(const std::string& buffer) {
    std::vector<std::string> tokens;
    if (buffer.empty() || buffer[0] != '*') return tokens;

    size_t pos = 0;
    size_t line_end = buffer.find("\r\n", pos);
    if (line_end == std::string::npos) return tokens;

    int num_elements = std::stoi(buffer.substr(1, line_end - 1));
    pos = line_end + 2;

    for (int i = 0; i < num_elements; ++i) {
        if (pos >= buffer.size() || buffer[pos] != '$') break;
        line_end = buffer.find("\r\n", pos);
        if (line_end == std::string::npos) break;

        int str_len = std::stoi(buffer.substr(pos + 1, line_end - pos - 1));
        pos = line_end + 2;

        tokens.push_back(buffer.substr(pos, str_len));
        pos += str_len + 2;
    }
    return tokens;
}

// Helper: Parse Stream IDs ("ms-seq" or "ms-*")
bool parse_stream_id(const std::string& id_str, uint64_t& ms, uint64_t& seq, bool& auto_seq) {
    size_t dash_pos = id_str.find('-');
    if (dash_pos == std::string::npos) return false;

    try {
        ms = std::stoull(id_str.substr(0, dash_pos));
        std::string seq_part = id_str.substr(dash_pos + 1);

        if (seq_part == "*") {
            auto_seq = true;
            seq = 0;
        } else {
            auto_seq = false;
            seq = std::stoull(seq_part);
        }
    } catch (...) {
        return false;
    }
    return true;
}

// Client Connection Handler
void handle_client(int client_fd) {
    char buffer[1024];

    while (true) {
        ssize_t bytes_read = read(client_fd, buffer, sizeof(buffer) - 1);
        if (bytes_read <= 0) break;

        buffer[bytes_read] = '\0';
        std::vector<std::string> tokens = parse_resp(std::string(buffer, bytes_read));
        if (tokens.empty()) continue;

        std::string command = tokens[0];
        std::transform(command.begin(), command.end(), command.begin(), ::toupper);

        // --- PING ---
        if (command == "PING") {
            send(client_fd, "+PONG\r\n", 7, 0);
        }
        // --- ECHO ---
        else if (command == "ECHO" && tokens.size() >= 2) {
            std::string res = "$" + std::to_string(tokens[1].length()) + "\r\n" + tokens[1] + "\r\n";
            send(client_fd, res.c_str(), res.length(), 0);
        }
        // --- SET key value [PX milliseconds] ---
        else if (command == "SET" && tokens.size() >= 3) {
            std::string key = tokens[1];
            Value val;
            val.data = tokens[2];

            if (tokens.size() >= 5) {
                std::string opt = tokens[3];
                std::transform(opt.begin(), opt.end(), opt.begin(), ::toupper);
                if (opt == "PX") {
                    long long ms = std::stoll(tokens[4]);
                    val.expiry = std::chrono::steady_clock::now() + std::chrono::milliseconds(ms);
                    val.has_expiry = true;
                }
            }

            {
                std::lock_guard<std::mutex> lock(db_mutex);
                kv_store[key] = val;
            }
            send(client_fd, "+OK\r\n", 5, 0);
        }
        // --- GET key ---
        else if (command == "GET" && tokens.size() >= 2) {
            std::string key = tokens[1];
            std::string res = "$-1\r\n";

            {
                std::lock_guard<std::mutex> lock(db_mutex);
                if (kv_store.count(key)) {
                    auto& val = kv_store[key];
                    if (val.has_expiry && std::chrono::steady_clock::now() >= val.expiry) {
                        kv_store.erase(key);
                    } else {
                        res = "$" + std::to_string(val.data.length()) + "\r\n" + val.data + "\r\n";
                    }
                }
            }
            send(client_fd, res.c_str(), res.length(), 0);
        }
        // --- RPUSH key element [element ...] ---
        else if (command == "RPUSH" && tokens.size() >= 3) {
            std::string key = tokens[1];
            size_t new_size = 0;

            {
                std::lock_guard<std::mutex> lock(db_mutex);
                for (size_t i = 2; i < tokens.size(); ++i) {
                    list_store[key].push_back(tokens[i]);
                }
                new_size = list_store[key].size();
            }
            cv.notify_all();

            std::string res = ":" + std::to_string(new_size) + "\r\n";
            send(client_fd, res.c_str(), res.length(), 0);
        }
        // --- LPUSH key element [element ...] ---
        else if (command == "LPUSH" && tokens.size() >= 3) {
            std::string key = tokens[1];
            size_t new_size = 0;

            {
                std::lock_guard<std::mutex> lock(db_mutex);
                for (size_t i = 2; i < tokens.size(); ++i) {
                    list_store[key].insert(list_store[key].begin(), tokens[i]);
                }
                new_size = list_store[key].size();
            }
            cv.notify_all();

            std::string res = ":" + std::to_string(new_size) + "\r\n";
            send(client_fd, res.c_str(), res.length(), 0);
        }
        // --- LPOP key ---
        else if (command == "LPOP" && tokens.size() >= 2) {
            std::string key = tokens[1];
            std::string res = "$-1\r\n";

            {
                std::lock_guard<std::mutex> lock(db_mutex);
                if (list_store.count(key) && !list_store[key].empty()) {
                    std::string val = list_store[key].front();
                    list_store[key].erase(list_store[key].begin());
                    res = "$" + std::to_string(val.length()) + "\r\n" + val + "\r\n";
                }
            }
            send(client_fd, res.c_str(), res.length(), 0);
        }
        // --- BLPOP key timeout ---
        else if (command == "BLPOP" && tokens.size() >= 3) {
            std::string key = tokens[1];
            int timeout_sec = std::stoi(tokens[2]);
            std::string popped_val = "";

            std::unique_lock<std::mutex> lock(db_mutex);

            auto list_has_data = [&]() {
                return list_store.count(key) && !list_store[key].empty();
            };

            bool acquired = true;
            if (!list_has_data()) {
                if (timeout_sec == 0) {
                    cv.wait(lock, list_has_data);
                } else {
                    acquired = cv.wait_for(lock, std::chrono::seconds(timeout_sec), list_has_data);
                }
            }

            if (acquired && list_has_data()) {
                popped_val = list_store[key].front();
                list_store[key].erase(list_store[key].begin());

                std::string res = "*2\r\n$" + std::to_string(key.length()) + "\r\n" + key + "\r\n$" +
                                  std::to_string(popped_val.length()) + "\r\n" + popped_val + "\r\n";
                send(client_fd, res.c_str(), res.length(), 0);
            } else {
                send(client_fd, "*-1\r\n", 5, 0);
            }
        }
        // --- LRANGE key start stop ---
        else if (command == "LRANGE" && tokens.size() >= 4) {
            std::string key = tokens[1];
            int start = std::stoi(tokens[2]);
            int stop = std::stoi(tokens[3]);
            std::vector<std::string> result;

            {
                std::lock_guard<std::mutex> lock(db_mutex);
                if (list_store.count(key)) {
                    const auto& lst = list_store[key];
                    int n = lst.size();

                    if (start < 0) start += n;
                    if (stop < 0) stop += n;

                    start = std::max(0, start);
                    stop = std::min(n - 1, stop);

                    for (int i = start; i <= stop && i < n; ++i) {
                        result.push_back(lst[i]);
                    }
                }
            }

            std::string res = "*" + std::to_string(result.size()) + "\r\n";
            for (const auto& item : result) {
                res += "$" + std::to_string(item.length()) + "\r\n" + item + "\r\n";
            }
            send(client_fd, res.c_str(), res.length(), 0);
        }
        // --- HSET key field value ---
        else if (command == "HSET" && tokens.size() >= 4) {
            std::string key = tokens[1];
            std::string field = tokens[2];
            std::string val = tokens[3];

            {
                std::lock_guard<std::mutex> lock(db_mutex);
                hash_store[key][field] = val;
            }
            send(client_fd, ":1\r\n", 4, 0);
        }
        // --- HGET key field ---
        else if (command == "HGET" && tokens.size() >= 3) {
            std::string key = tokens[1];
            std::string field = tokens[2];
            std::string res = "$-1\r\n";

            {
                std::lock_guard<std::mutex> lock(db_mutex);
                if (hash_store.count(key) && hash_store[key].count(field)) {
                    std::string val = hash_store[key][field];
                    res = "$" + std::to_string(val.length()) + "\r\n" + val + "\r\n";
                }
            }
            send(client_fd, res.c_str(), res.length(), 0);
        }
        // --- HGETALL key ---
        else if (command == "HGETALL" && tokens.size() >= 2) {
            std::string key = tokens[1];
            std::vector<std::string> pairs;

            {
                std::lock_guard<std::mutex> lock(db_mutex);
                if (hash_store.count(key)) {
                    for (const auto& [f, v] : hash_store[key]) {
                        pairs.push_back(f);
                        pairs.push_back(v);
                    }
                }
            }

            std::string res = "*" + std::to_string(pairs.size()) + "\r\n";
            for (const auto& item : pairs) {
                res += "$" + std::to_string(item.length()) + "\r\n" + item + "\r\n";
            }
            send(client_fd, res.c_str(), res.length(), 0);
        }
        // --- XADD key ID field value [field value ...] ---
        else if (command == "XADD" && tokens.size() >= 5) {
            std::string key = tokens[1];
            std::string explicit_id = tokens[2];

            std::vector<std::pair<std::string, std::string>> kv_pairs;
            for (size_t i = 3; i + 1 < tokens.size(); i += 2) {
                kv_pairs.push_back({tokens[i], tokens[i + 1]});
            }

            uint64_t req_ms = 0, req_seq = 0;
            bool auto_seq = false;

            if (explicit_id == "*") {
                req_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch()).count();
                auto_seq = true;
            } else if (!parse_stream_id(explicit_id, req_ms, req_seq, auto_seq)) {
                send(client_fd, "-ERR Invalid stream ID format\r\n", 31, 0);
                continue;
            }

            {
                std::lock_guard<std::mutex> lock(db_mutex);
                auto& stream = stream_store[key];

                if (!stream.empty()) {
                    uint64_t top_ms = 0, top_seq = 0;
                    bool dummy;
                    parse_stream_id(stream.back().id, top_ms, top_seq, dummy);

                    if (auto_seq) {
                        if (req_ms == top_ms) {
                            req_seq = top_seq + 1;
                        } else if (req_ms < top_ms) {
                            req_ms = top_ms;
                            req_seq = top_seq + 1;
                        } else {
                            req_seq = (req_ms == 0) ? 1 : 0;
                        }
                    }

                    if (req_ms < top_ms || (req_ms == top_ms && req_seq <= top_seq)) {
                        std::string err = "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n";
                        send(client_fd, err.c_str(), err.length(), 0);
                        continue;
                    }
                } else {
                    if (auto_seq) {
                        req_seq = (req_ms == 0) ? 1 : 0;
                    }
                }

                if (req_ms == 0 && req_seq == 0) {
                    std::string err = "-ERR The ID specified in XADD must be greater than 0-0\r\n";
                    send(client_fd, err.c_str(), err.length(), 0);
                    continue;
                }

                std::string final_id = std::to_string(req_ms) + "-" + std::to_string(req_seq);
                stream.push_back({final_id, kv_pairs});

                std::string res = "$" + std::to_string(final_id.length()) + "\r\n" + final_id + "\r\n";
                send(client_fd, res.c_str(), res.length(), 0);
            }
        }
    }

    close(client_fd);
}

int main() {
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        std::cerr << "Failed to create socket" << std::endl;
        return 1;
    }

    int reuse = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

    sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(6379);

    if (bind(server_fd, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        std::cerr << "Failed to bind to port 6379" << std::endl;
        return 1;
    }

    if (listen(server_fd, 5) < 0) {
        std::cerr << "Failed to listen on socket" << std::endl;
        return 1;
    }

    std::cout << "Redis TCP Server listening on port 6379..." << std::endl;

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