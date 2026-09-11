#include <iostream>
#include <string>
#include <vector>
#include <sstream>
#include <unordered_map>
#include <chrono>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <algorithm>
#include <cstdint>
#include <cstring>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>

struct StreamEntry {
    std::string id;
    std::vector<std::pair<std::string, std::string>> kv_pairs;
};

std::unordered_map<std::string, std::string> kv_store;
std::unordered_map<std::string, std::chrono::time_point<std::chrono::steady_clock>> expiry_store;
std::unordered_map<std::string, std::vector<StreamEntry>> stream_store;

std::mutex db_mutex;
std::condition_variable stream_cv;

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

// Helper function to execute commands and format RESP responses
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
        return ":" + std::to_string(current_val) + "\r\n";
    }
    else if (command == "TYPE" && tokens.size() >= 2) {
        std::lock_guard<std::mutex> lock(db_mutex);
        std::string key = tokens[1];

        // Clean up expired key if necessary
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
    else if (command == "XREAD" && tokens.size() >= 4) {
        int block_ms = -1;
        size_t streams_idx = 0;

        for (size_t i = 1; i < tokens.size(); ++i) {
            std::string arg = tokens[i];
            std::transform(arg.begin(), arg.end(), arg.begin(), ::toupper);
            if (arg == "BLOCK" && i + 1 < tokens.size()) {
                block_ms = std::stoi(tokens[i + 1]);
            } else if (arg == "STREAMS") {
                streams_idx = i;
                break;
            }
        }

        if (streams_idx != 0 && streams_idx < tokens.size() - 1) {
            size_t num_args = tokens.size() - (streams_idx + 1);
            size_t num_streams = num_args / 2;

            std::vector<std::string> keys;
            std::vector<std::string> start_ids;

            for (size_t i = 0; i < num_streams; ++i) {
                keys.push_back(tokens[streams_idx + 1 + i]);
                std::string sid = tokens[streams_idx + 1 + num_streams + i];

                if (sid == "$") {
                    std::lock_guard<std::mutex> lock(db_mutex);
                    std::string k = keys.back();
                    if (stream_store.count(k) && !stream_store[k].empty()) {
                        sid = stream_store[k].back().id;
                    } else {
                        sid = "0-0";
                    }
                }
                start_ids.push_back(sid);
            }

            auto fetch_entries = [&](std::string& response, size_t& matched_streams) -> bool {
                response = "";
                matched_streams = 0;
                for (size_t i = 0; i < num_streams; ++i) {
                    std::string key = keys[i];
                    std::string start_str = start_ids[i];

                    uint64_t start_ms = 0, start_seq = 0;
                    bool dummy;
                    parse_stream_id(start_str, start_ms, start_seq, dummy);

                    std::string stream_entries_resp = "";
                    size_t entry_count = 0;

                    if (stream_store.count(key)) {
                        for (const auto& entry : stream_store[key]) {
                            uint64_t entry_ms = 0, entry_seq = 0;
                            parse_stream_id(entry.id, entry_ms, entry_seq, dummy);

                            bool gt_start = (entry_ms > start_ms) || (entry_ms == start_ms && entry_seq > start_seq);

                            if (gt_start) {
                                entry_count++;
                                stream_entries_resp += "*2\r\n";
                                stream_entries_resp += "$" + std::to_string(entry.id.length()) + "\r\n" + entry.id + "\r\n";
                                stream_entries_resp += "*" + std::to_string(entry.kv_pairs.size() * 2) + "\r\n";
                                for (const auto& kv : entry.kv_pairs) {
                                    stream_entries_resp += "$" + std::to_string(kv.first.length()) + "\r\n" + kv.first + "\r\n";
                                    stream_entries_resp += "$" + std::to_string(kv.second.length()) + "\r\n" + kv.second + "\r\n";
                                }
                            }
                        }
                    }

                    if (entry_count > 0) {
                        matched_streams++;
                        response += "*2\r\n";
                        response += "$" + std::to_string(key.length()) + "\r\n" + key + "\r\n";
                        response += "*" + std::to_string(entry_count) + "\r\n" + stream_entries_resp;
                    }
                }
                return matched_streams > 0;
            };

            std::unique_lock<std::mutex> lock(db_mutex);
            std::string response;
            size_t matched_streams = 0;

            bool has_data = fetch_entries(response, matched_streams);

            if (!has_data && block_ms >= 0) {
                if (block_ms == 0) {
                    stream_cv.wait(lock, [&]() { return fetch_entries(response, matched_streams); });
                } else {
                    stream_cv.wait_for(lock, std::chrono::milliseconds(block_ms), [&]() {
                        return fetch_entries(response, matched_streams);
                    });
                }
            }

            if (matched_streams > 0) {
                return "*" + std::to_string(matched_streams) + "\r\n" + response;
            } else {
                return "$-1\r\n";
            }
        }
    }

    return "-ERR unknown command\r\n";
}

void handle_client(int client_fd) {
    char buffer[4096];
    bool in_transaction = false;
    std::vector<std::vector<std::string>> command_queue;

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

        if (command == "MULTI") {
            if (in_transaction) {
                std::string err = "-ERR MULTI calls can not be nested\r\n";
                send(client_fd, err.c_str(), err.length(), 0);
            } else {
                in_transaction = true;
                command_queue.clear();
                std::string res = "+OK\r\n";
                send(client_fd, res.c_str(), res.length(), 0);
            }
        }
        else if (command == "EXEC") {
            if (!in_transaction) {
                std::string err = "-ERR EXEC without MULTI\r\n";
                send(client_fd, err.c_str(), err.length(), 0);
            } else {
                in_transaction = false;
                std::string res = "*" + std::to_string(command_queue.size()) + "\r\n";
                for (const auto& queued_tokens : command_queue) {
                    res += execute_command(queued_tokens);
                }
                command_queue.clear();
                send(client_fd, res.c_str(), res.length(), 0);
            }
        }
        else if (command == "DISCARD") {
            if (!in_transaction) {
                std::string err = "-ERR DISCARD without MULTI\r\n";
                send(client_fd, err.c_str(), err.length(), 0);
            } else {
                in_transaction = false;
                command_queue.clear();
                std::string res = "+OK\r\n";
                send(client_fd, res.c_str(), res.length(), 0);
            }
        }
        else if (in_transaction) {
            command_queue.push_back(tokens);
            std::string res = "+QUEUED\r\n";
            send(client_fd, res.c_str(), res.length(), 0);
        }
        else {
            std::string res = execute_command(tokens);
            send(client_fd, res.c_str(), res.length(), 0);
        }
    }
    close(client_fd);
}

int main() {
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) return 1;

    int reuse = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

    sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(6379);

    if (bind(server_fd, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) return 1;
    if (listen(server_fd, 5) < 0) return 1;

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