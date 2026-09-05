#include <iostream>
#include <cstdlib>
#include <string>
#include <cstring>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <thread>
#include <algorithm>
#include <unordered_map>
#include <mutex>
#include <vector>
#include <chrono>

struct ValueEntry {
    std::string value;
    std::chrono::steady_clock::time_point expiry;
    bool has_expiry = false;
};

// Key-Value String Store
std::unordered_map<std::string, ValueEntry> kv_store;

// List Store
std::unordered_map<std::string, std::vector<std::string>> list_store;

// Global mutex to protect both stores
std::mutex db_mutex;

std::vector<std::string> parse_resp_array(const std::string& request) {
  std::vector<std::string> tokens;
  size_t pos = 0;

  if (request.empty() || request[0] != '*') return tokens;

  pos = request.find("\r\n", pos);
  if (pos == std::string::npos) return tokens;
  pos += 2;

  while (pos < request.length()) {
    if (request[pos] == '$') {
      size_t len_end = request.find("\r\n", pos);
      if (len_end == std::string::npos) break;
      
      int len = std::stoi(request.substr(pos + 1, len_end - (pos + 1)));
      pos = len_end + 2;

      if (pos + len <= request.length()) {
        tokens.push_back(request.substr(pos, len));
        pos += len + 2;
      } else {
        break;
      }
    } else {
      break;
    }
  }
  return tokens;
}

void handle_client(int client_fd) {
  char buffer[1024];
  while (true) {
    std::memset(buffer, 0, sizeof(buffer));
    ssize_t bytes_received = read(client_fd, buffer, sizeof(buffer) - 1);
    if (bytes_received <= 0) break;

    std::string request(buffer, bytes_received);
    std::vector<std::string> tokens = parse_resp_array(request);

    if (tokens.empty()) {
      std::string response = "+PONG\r\n";
      send(client_fd, response.c_str(), response.length(), 0);
      continue;
    }

    std::string command = tokens[0];
    std::transform(command.begin(), command.end(), command.begin(), ::toupper);

    if (command == "PING") {
      std::string response = "+PONG\r\n";
      send(client_fd, response.c_str(), response.length(), 0);
    } 
    else if (command == "ECHO" && tokens.size() >= 2) {
      std::string msg = tokens[1];
      std::string response = "$" + std::to_string(msg.length()) + "\r\n" + msg + "\r\n";
      send(client_fd, response.c_str(), response.length(), 0);
    } 
    else if (command == "SET" && tokens.size() >= 3) {
      std::string key = tokens[1];
      std::string val = tokens[2];

      ValueEntry entry;
      entry.value = val;

      if (tokens.size() >= 5) {
        std::string opt = tokens[3];
        std::transform(opt.begin(), opt.end(), opt.begin(), ::toupper);
        if (opt == "PX") {
          int px_ms = std::stoi(tokens[4]);
          entry.expiry = std::chrono::steady_clock::now() + std::chrono::milliseconds(px_ms);
          entry.has_expiry = true;
        }
      }

      {
        std::lock_guard<std::mutex> lock(db_mutex);
        kv_store[key] = entry;
      }

      std::string response = "+OK\r\n";
      send(client_fd, response.c_str(), response.length(), 0);
    } 
    else if (command == "GET" && tokens.size() >= 2) {
      std::string key = tokens[1];
      std::string response;

      {
        std::lock_guard<std::mutex> lock(db_mutex);
        auto it = kv_store.find(key);
        if (it == kv_store.end()) {
          response = "$-1\r\n";
        } else {
          if (it->second.has_expiry && std::chrono::steady_clock::now() >= it->second.expiry) {
            kv_store.erase(it);
            response = "$-1\r\n";
          } else {
            std::string val = it->second.value;
            response = "$" + std::to_string(val.length()) + "\r\n" + val + "\r\n";
          }
        }
      }
      send(client_fd, response.c_str(), response.length(), 0);
    }
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

      // RESP Integer reply: :<count>\r\n
      std::string response = ":" + std::to_string(new_size) + "\r\n";
      send(client_fd, response.c_str(), response.length(), 0);
    }
    else if (command == "LRANGE" && tokens.size() >= 4) {
      std::string key = tokens[1];
      int start = std::stoi(tokens[2]);
      int stop = std::stoi(tokens[3]);
      std::string response;

      {
        std::lock_guard<std::mutex> lock(db_mutex);
        auto it = list_store.find(key);

        if (it == list_store.end()) {
          response = "*0\r\n"; // Empty RESP Array
        } else {
          const auto& vec = it->second;
          int n = static_cast<int>(vec.size());

          // Handle negative indexing
          if (start < 0) start = n + start;
          if (stop < 0) stop = n + stop;

          // Clamp bounds
          if (start < 0) start = 0;
          if (stop >= n) stop = n - 1;

          if (start > stop || start >= n) {
            response = "*0\r\n";
          } else {
            int count = stop - start + 1;
            response = "*" + std::to_string(count) + "\r\n";
            for (int i = start; i <= stop; ++i) {
              response += "$" + std::to_string(vec[i].length()) + "\r\n" + vec[i] + "\r\n";
            }
          }
        }
      }
      send(client_fd, response.c_str(), response.length(), 0);
    }
  }
  close(client_fd);
}

int main(int argc, char **argv) {
  std::cout << std::unitbuf;
  std::cerr << std::unitbuf;

  int server_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (server_fd < 0) return 1;

  int reuse = 1;
  setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

  struct sockaddr_in server_addr{};
  server_addr.sin_family = AF_INET;
  server_addr.sin_addr.s_addr = INADDR_ANY;
  server_addr.sin_port = htons(6379);

  if (bind(server_fd, (struct sockaddr *) &server_addr, sizeof(server_addr)) != 0) return 1;
  if (listen(server_fd, 5) != 0) return 1;

  std::cout << "Redis-compatible server active on port 6379...\n";

  while (true) {
    struct sockaddr_in client_addr;
    int client_addr_len = sizeof(client_addr);
    int client_fd = accept(server_fd, (struct sockaddr *) &client_addr, (socklen_t *) &client_addr_len);
    if (client_fd < 0) continue;
    std::thread(handle_client, client_fd).detach();
  }

  close(server_fd);
  return 0;
}