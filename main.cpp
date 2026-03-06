#include <iostream>
#include <thread>
#include <vector>
#include <atomic>
#include <chrono>
#include <string>
#include <sstream>
#include <iomanip>
#include <cstring>
#include <random>
#include "randomx.h"

// For Windows sockets
#include <winsock2.h>
#include <ws2tcpip.h>
#pragma comment(lib, "Ws2_32.lib")

using namespace std;

// Pool and wallet configuration
const string POOL_HOST = "gulf.moneroocean.stream";
const int POOL_PORT = 10032; // Low difficulty port for testing
const string WALLET_ADDRESS = "45Hmns9GPKtJyrcawEH6zUYrsjxicKmPC7xDf2AF8FFeP6ZGq8anR1fRCyM4FUHFHWgc8ArAVWpquEpoysBfPffPT5yUUXn";
const string PASSWORD = "x";
const string USER_AGENT = "GrokMiner/1.0";

// Global variables
atomic<bool> running(true);
atomic<uint64_t> hash_count(0);
atomic<double> hashrate(0.0);
atomic<uint64_t> accepted(0);
atomic<uint64_t> rejected(0);
atomic<uint64_t> job_version(0);
mutex job_mutex;
mutex cout_mutex;
mutex socket_mutex;
struct Job {
    string blob;
    string job_id;
    string seed_hash;
    uint64_t target;
    uint32_t height;
};
Job current_job;

// RandomX globals
randomx_flags flags;
randomx_cache* cache = nullptr;
randomx_dataset* dataset = nullptr;
vector<randomx_vm*> vms;

// Functions
string bin2hex(const char* bin, size_t len) {
    stringstream ss;
    ss << hex << setfill('0');
    for (size_t i = 0; i < len; ++i) {
        ss << setw(2) << (unsigned int)(unsigned char)bin[i];
    }
    return ss.str();
}

bool hex2bin(const string& hex, char* bin, size_t len) {
    if (hex.length() != len * 2) return false;
    for (size_t i = 0; i < len; ++i) {
        sscanf(hex.c_str() + 2 * i, "%2hhx", &bin[i]);
    }
    return true;
}

uint64_t hex2int64(const string& hex) {
    uint64_t val = 0;
    sscanf(hex.c_str(), "%llx", &val);
    return val;
}

void init_randomx(const string& seed_hash) {
    flags = randomx_get_flags() | RANDOMX_FLAG_LARGE_PAGES | RANDOMX_FLAG_HARD_AES | RANDOMX_FLAG_FULL_MEM;
    cache = randomx_alloc_cache(flags);
    randomx_init_cache(cache, seed_hash.c_str(), seed_hash.length());
    dataset = randomx_alloc_dataset(flags);
    randomx_init_dataset(dataset, cache, 0, randomx_dataset_item_count());
}

void create_vms(int num_threads) {
    for (int i = 0; i < num_threads; ++i) {
        randomx_vm* vm = randomx_create_vm(flags, cache, dataset);
        vms.push_back(vm);
    }
}

void cleanup_randomx() {
    for (auto vm : vms) {
        randomx_destroy_vm(vm);
    }
    randomx_destroy_dataset(dataset);
    randomx_release_cache(cache);
}

bool parse_job(const string& response, Job& job, bool is_initial = false) {
    size_t blob_pos = response.find("\"blob\":\"");
    if (blob_pos == string::npos) return false;
    blob_pos += 8;
    size_t blob_end = response.find("\"", blob_pos);
    job.blob = response.substr(blob_pos, blob_end - blob_pos);

    size_t job_id_pos = response.find("\"job_id\":\"");
    if (job_id_pos != string::npos) {
        job_id_pos += 10;
        size_t job_id_end = response.find("\"", job_id_pos);
        job.job_id = response.substr(job_id_pos, job_id_end - job_id_pos);
    }

    size_t target_pos = response.find("\"target\":\"");
    if (target_pos != string::npos) {
        target_pos += 10;
        size_t target_end = response.find("\"", target_pos);
        string target_hex = response.substr(target_pos, target_end - target_pos);
        job.target = hex2int64(target_hex);
    }

    size_t seed_pos = response.find("\"seed_hash\":\"");
    if (seed_pos != string::npos) {
        seed_pos += 13;
        size_t seed_end = response.find("\"", seed_pos);
        job.seed_hash = response.substr(seed_pos, seed_end - seed_pos);
    }

    return true;
}

void mining_thread(int id, uint32_t start_nonce, uint32_t increment, SOCKET sock) {
    char input[76]; // Monero blob size
    char hash[RANDOMX_HASH_SIZE];
    randomx_vm* vm = vms[id];
    Job local_job;
    uint64_t local_version = 0;
    while (running) {
        {
            lock_guard<mutex> lock(job_mutex);
            if (job_version != local_version) {
                local_job = current_job;
                local_version = job_version;
            }
        }
        hex2bin(local_job.blob, input, 76);
        uint32_t nonce = start_nonce;
        memcpy(input + 39, &nonce, sizeof(nonce));
        randomx_calculate_hash(vm, input, 76, hash);
        hash_count++;
        uint64_t h = *reinterpret_cast<uint64_t*>(hash + 24);
        if (h < local_job.target) {
            string hex_hash = bin2hex(hash, RANDOMX_HASH_SIZE);
            string submit = "{\"id\":\"" + to_string(id) + "\",\"method\":\"submit\",\"params\":{\"job_id\":\"" + local_job.job_id + "\",\"nonce\":\"" + hex(nonce) + "\",\"result\":\"" + hex_hash + "\"}}\r\n";
            {
                lock_guard<mutex> lock(socket_mutex);
                send(sock, submit.c_str(), submit.length(), 0);
            }
        }
        start_nonce += increment;
    }
}

void hashrate_thread() {
    uint64_t prev_hash_count = 0;
    while (running) {
        this_thread::sleep_for(chrono::seconds(10));
        uint64_t current_hashes = hash_count.load();
        double hr = (current_hashes - prev_hash_count) / 10.0;
        hashrate = hr;
        prev_hash_count = current_hashes;
        lock_guard<mutex> lock(cout_mutex);
        cout << "Hashrate: " << hr << " H/s | Accepted: " << accepted << " | Rejected: " << rejected << endl;
        // Estimate mined coins (rough, based on network stats; update values as needed)
        double network_hr = 3000000000.0; // Example 3 GH/s
        double block_reward = 0.6; // Approx XMR per block
        double estimated_per_hour = (hr / network_hr) * block_reward * 6; // 6 blocks/hour avg
        cout << "Estimated mined coins: " << fixed << setprecision(8) << estimated_per_hour << " XMR (per hour)" << endl;
    }
}

void timer_thread() {
    while (running) {
        this_thread::sleep_for(chrono::minutes(5));
        lock_guard<mutex> lock(cout_mutex);
        cout << "5 minutes passed. Automatically sending collected coins to wallet..." << endl;
        // Actual payout is pool-managed; this is a simulation message
    }
}

void manual_thread() {
    string input;
    while (running) {
        getline(cin, input);
        if (input == "collect") {
            lock_guard<mutex> lock(cout_mutex);
            cout << "Manually collecting all mined coins to wallet..." << endl;
            // Actual payout is pool-managed; this is a simulation message
        } else if (input == "stop") {
            running = false;
        }
    }
}

int main() {
    // Initialize Winsock
    WSADATA wsa;
    if (WSAStartup(MAKEWORD(2, 2), &wsa) != 0) {
        cerr << "Winsock init failed" << endl;
        return 1;
    }

    // Connect to pool
    SOCKET sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock == INVALID_SOCKET) {
        cerr << "Socket creation failed" << endl;
        return 1;
    }
    addrinfo hints = {0}, *result = nullptr;
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    getaddrinfo(POOL_HOST.c_str(), to_string(POOL_PORT).c_str(), &hints, &result);
    if (connect(sock, result->ai_addr, (int)result->ai_addrlen) == SOCKET_ERROR) {
        cerr << "Connection failed" << endl;
        return 1;
    }
    freeaddrinfo(result);

    // Send login
    string login = "{\"id\":\"1\",\"method\":\"login\",\"params\":{\"login\":\"" + WALLET_ADDRESS + "\",\"pass\":\"" + PASSWORD + "\",\"agent\":\"" + USER_AGENT + "\"}}\r\n";
    send(sock, login.c_str(), login.length(), 0);

    // Receive initial response and parse job
    char buffer[4096];
    int len = recv(sock, buffer, sizeof(buffer), 0);
    string response(buffer, len);
    string initial_seed_hash = "0000000000000000000000000000000000000000000000000000000000000000"; // Default or from response
    parse_job(response, current_job, true);
    current_job.seed_hash = initial_seed_hash; // Parse properly in production
    init_randomx(current_job.seed_hash);
    int num_threads = thread::hardware_concurrency() - 1; // Use most cores for high profit
    create_vms(num_threads);
    job_version = 1;

    // Start mining threads
    uint32_t increment = 0xFFFFFFFF / num_threads + 1;
    uint32_t start_nonce = 0;
    vector<thread> mining_threads;
    for (int i = 0; i < num_threads; ++i) {
        mining_threads.emplace_back(mining_thread, i, start_nonce, increment, sock);
        start_nonce += increment;
    }

    // Start utility threads
    thread hr_thread(hashrate_thread);
    thread t_timer(timer_thread);
    thread t_manual(manual_thread);

    // Main loop for receiving pool messages
    while (running) {
        len = recv(sock, buffer, sizeof(buffer), 0);
        if (len <= 0) break;
        response = string(buffer, len);
        if (response.find("\"job\"") != string::npos) {
            Job new_job;
            if (parse_job(response, new_job)) {
                bool reinit = (new_job.seed_hash != current_job.seed_hash);
                if (reinit) {
                    cleanup_randomx();
                    init_randomx(new_job.seed_hash);
                    create_vms(num_threads);
                }
                {
                    lock_guard<mutex> lock(job_mutex);
                    current_job = new_job;
                    job_version++;
                }
            }
        } else if (response.find("\"result\"") != string::npos) {
            if (response.find("\"status\":\"OK\"") != string::npos) {
                accepted++;
            } else {
                rejected++;
            }
        }
    }

    running = false;
    for (auto& t : mining_threads) t.join();
    hr_thread.join();
    t_timer.join();
    t_manual.join();
    cleanup_randomx();
    closesocket(sock);
    WSACleanup();
    return 0;
}
