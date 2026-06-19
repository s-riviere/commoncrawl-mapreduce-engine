// =============================================================================
// DESCRIPTION
// =============================================================================
// MapReduce worker – C++ implementation.
// Drop-in replacement for src/map_reduce/worker.py.
// Connects to the master, executes MAP and REDUCE tasks, emits WORKER_TIMING
// lines in the same format as the Python worker so amdahl_bench.py can parse
// them without modification.
//
// Build:
//   g++ -std=c++17 -O2 -o worker src/cpp/worker.cpp -lz
//
// Usage:
//   ./worker -h HOST -p PORT -i INPUT_DIR -o OUTPUT_DIR -l LOCAL_MAP_DIR
//
// Arguments:
//   -h HOST           Master hostname or IP
//   -p PORT           Master listening port
//   -i INPUT_DIR      Shared NFS input directory (split files)
//   -o OUTPUT_DIR     Shared NFS output directory (reduce results)
//   -l LOCAL_MAP_DIR  Local scratch directory for MAP intermediate partitions
// =============================================================================

#include <algorithm>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>
#include <zlib.h>

// POSIX networking
#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

namespace fs = std::filesystem;

// ── Timing ────────────────────────────────────────────────────────────────────

static double now_s() {
    using Clock = std::chrono::steady_clock;
    using Secs  = std::chrono::duration<double>;
    return Secs(Clock::now().time_since_epoch()).count();
}

// ── Logging ───────────────────────────────────────────────────────────────────

static void log(const std::string& level, const std::string& msg) {
    std::time_t t = std::time(nullptr);
    char buf[32];
    std::strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", std::localtime(&t));
    std::cout << "[" << buf << "] [WORKER-CPP] [" << level << "] " << msg << "\n";
    std::cout.flush();
}

// ── Minimal JSON (handles only the master message shapes) ────────────────────

struct JsonVal {
    enum class Kind { Str, Int, StrArr } kind;
    std::string              s;
    int64_t                  i   = 0;
    std::vector<std::string> arr;

    JsonVal() : kind(Kind::Str) {}
    explicit JsonVal(std::string v)              : kind(Kind::Str),    s(std::move(v)) {}
    explicit JsonVal(int64_t v)                  : kind(Kind::Int),    i(v) {}
    explicit JsonVal(std::vector<std::string> v) : kind(Kind::StrArr), arr(std::move(v)) {}

    const std::string&              as_str() const { return s; }
    int64_t                         as_int() const { return i; }
    const std::vector<std::string>& as_arr() const { return arr; }
};

class JsonParser {
    const std::string& src;
    size_t pos = 0;

    void skip_ws() {
        while (pos < src.size() && std::isspace((unsigned char)src[pos])) ++pos;
    }

    std::string parse_string() {
        if (pos >= src.size() || src[pos] != '"')
            throw std::runtime_error("Expected '\"' at " + std::to_string(pos));
        ++pos;
        std::string r;
        while (pos < src.size() && src[pos] != '"') {
            if (src[pos] == '\\' && pos + 1 < src.size()) { ++pos; }
            r += src[pos++];
        }
        if (pos >= src.size()) throw std::runtime_error("Unterminated string");
        ++pos;
        return r;
    }

    int64_t parse_int() {
        bool neg = (pos < src.size() && src[pos] == '-');
        if (neg) ++pos;
        int64_t v = 0;
        if (pos >= src.size() || !std::isdigit((unsigned char)src[pos]))
            throw std::runtime_error("Expected digit at " + std::to_string(pos));
        while (pos < src.size() && std::isdigit((unsigned char)src[pos]))
            v = v * 10 + (src[pos++] - '0');
        return neg ? -v : v;
    }

    std::vector<std::string> parse_str_array() {
        if (pos >= src.size() || src[pos] != '[')
            throw std::runtime_error("Expected '[' at " + std::to_string(pos));
        ++pos;
        std::vector<std::string> r;
        skip_ws();
        while (pos < src.size() && src[pos] != ']') {
            skip_ws();
            if (src[pos] == '"') r.push_back(parse_string());
            skip_ws();
            if (pos < src.size() && src[pos] == ',') ++pos;
        }
        if (pos < src.size()) ++pos;  // closing ]
        return r;
    }

public:
    explicit JsonParser(const std::string& s) : src(s) {}

    std::unordered_map<std::string, JsonVal> parse_object() {
        skip_ws();
        if (pos >= src.size() || src[pos] != '{')
            throw std::runtime_error("Expected '{' at " + std::to_string(pos));
        ++pos;
        std::unordered_map<std::string, JsonVal> obj;
        skip_ws();
        while (pos < src.size() && src[pos] != '}') {
            skip_ws();
            std::string key = parse_string();
            skip_ws();
            if (pos >= src.size() || src[pos] != ':')
                throw std::runtime_error("Expected ':' at " + std::to_string(pos));
            ++pos;
            skip_ws();
            JsonVal val;
            if (src[pos] == '"') {
                val = JsonVal(parse_string());
            } else if (src[pos] == '[') {
                val = JsonVal(parse_str_array());
            } else if (std::isdigit((unsigned char)src[pos]) || src[pos] == '-') {
                val = JsonVal(parse_int());
            } else {
                throw std::runtime_error(std::string("Unexpected char '") +
                                         src[pos] + "' at " + std::to_string(pos));
            }
            obj.emplace(std::move(key), std::move(val));
            skip_ws();
            if (pos < src.size() && src[pos] == ',') ++pos;
            skip_ws();
        }
        if (pos < src.size()) ++pos;  // closing }
        return obj;
    }
};

static std::unordered_map<std::string, JsonVal> parse_json(const std::string& s) {
    JsonParser p(s);
    return p.parse_object();
}

// ── TCP helpers ───────────────────────────────────────────────────────────────

static int tcp_connect(const std::string& host, int port) {
    struct addrinfo hints{}, *res = nullptr;
    hints.ai_family   = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    int rc = getaddrinfo(host.c_str(), std::to_string(port).c_str(), &hints, &res);
    if (rc != 0)
        throw std::runtime_error("getaddrinfo: " + std::string(gai_strerror(rc)));
    int fd = -1;
    for (auto r = res; r != nullptr; r = r->ai_next) {
        fd = socket(r->ai_family, r->ai_socktype, r->ai_protocol);
        if (fd < 0) continue;
        if (connect(fd, r->ai_addr, r->ai_addrlen) == 0) break;
        close(fd); fd = -1;
    }
    freeaddrinfo(res);
    if (fd < 0)
        throw std::runtime_error("Cannot connect to " + host + ":" + std::to_string(port));
    return fd;
}

static void send_line(int fd, const std::string& json) {
    std::string msg = json + "\n";
    size_t sent = 0;
    while (sent < msg.size()) {
        ssize_t n = send(fd, msg.c_str() + sent, msg.size() - sent, 0);
        if (n <= 0) throw std::runtime_error("send() failed: " + std::string(std::strerror(errno)));
        sent += (size_t)n;
    }
}

// ── Worker ────────────────────────────────────────────────────────────────────

class Worker {
    std::string master_host;
    int         master_port;
    fs::path    input_dir;
    fs::path    output_dir;
    fs::path    local_map_dir;

    int         sock_fd  = -1;
    std::string recv_buf;
    double      t_clean  = 0.0;

    // Read one '\n'-terminated line from the master socket.
    std::string recv_line() {
        while (true) {
            auto nl = recv_buf.find('\n');
            if (nl != std::string::npos) {
                std::string line = recv_buf.substr(0, nl);
                recv_buf         = recv_buf.substr(nl + 1);
                return line;
            }
            char   tmp[4096];
            ssize_t n = recv(sock_fd, tmp, sizeof(tmp), 0);
            if (n <= 0) throw std::runtime_error("Master connection closed");
            recv_buf.append(tmp, (size_t)n);
        }
    }

    void clean_local_dir() {
        log("INFO", "Cleaning local map directory: " + local_map_dir.string());
        double t0 = now_s();
        if (fs::exists(local_map_dir)) fs::remove_all(local_map_dir);
        fs::create_directories(local_map_dir);
        t_clean = now_s() - t0;
        std::ostringstream ss;
        ss << std::fixed << std::setprecision(2) << t_clean;
        log("INFO", "Local map directory ready: " + local_map_dir.string() + " (" + ss.str() + "s)");
    }

    // ── MAP ───────────────────────────────────────────────────────────────────

    void execute_map(const std::unordered_map<std::string, JsonVal>& task) {
        int split_id   = (int)task.at("split_id").as_int();
        int n_reducers = (int)task.at("n_reducers").as_int();

        // Build input file path: commoncrawl-NNNN.txt
        std::ostringstream fname_ss;
        fname_ss << "commoncrawl-" << std::setw(4) << std::setfill('0') << split_id << ".txt";
        fs::path file_path = input_dir / fname_ss.str();

        log("INFO", "MAP start split=" + std::to_string(split_id) +
                    " reducers=" + std::to_string(n_reducers) +
                    " input=" + file_path.string());

        if (!fs::exists(file_path)) {
            log("ERROR", "Input split missing: " + file_path.string());
            return;
        }

        fs::create_directories(local_map_dir);

        // Open all partition files
        double t_io_open_start = now_s();
        std::vector<std::ofstream> parts((size_t)n_reducers);
        for (int r = 0; r < n_reducers; ++r) {
            fs::path pp = local_map_dir / ("partition_" + std::to_string(r) + ".txt");
            parts[(size_t)r].open(pp, std::ios::app);
            if (!parts[(size_t)r])
                throw std::runtime_error("Cannot open partition file: " + pp.string());
        }
        (void)(now_s() - t_io_open_start);  // tracked but not emitted (consistent with Python)

        // Read entire input file
        double t_io_read_start = now_s();
        std::vector<std::string> lines;
        {
            std::ifstream fin(file_path);
            std::string line;
            while (std::getline(fin, line)) lines.push_back(std::move(line));
        }
        double t_io_read = now_s() - t_io_read_start;

        // Compute: word-count + CRC32 partition
        double t_compute_start = now_s();
        for (size_t li = 0; li < lines.size(); ++li) {
            std::istringstream iss(lines[li]);
            std::string word;
            while (iss >> word) {
                // Mirror Python: word.isalnum()
                bool alnum = !word.empty();
                for (unsigned char c : word)
                    if (!std::isalnum(c)) { alnum = false; break; }
                if (!alnum) continue;

                // Lowercase
                std::transform(word.begin(), word.end(), word.begin(),
                               [](unsigned char c) { return (char)std::tolower(c); });

                // CRC32 partition (mirrors Python: zlib.crc32(key.encode()) % n_reducers)
                uint32_t crc = crc32(0L, reinterpret_cast<const Bytef*>(word.c_str()),
                                     (uInt)word.size());
                int rid = (int)(crc % (uint32_t)n_reducers);
                parts[(size_t)rid] << word << "\t1\n";
            }
            if (li % 50000 == 0)
                for (auto& f : parts) f.flush();
        }
        double t_compute = now_s() - t_compute_start;

        // Close files
        double t_io_write_start = now_s();
        for (auto& f : parts) f.close();
        double t_io_write = now_s() - t_io_write_start;

        log("INFO", "MAP done split=" + std::to_string(split_id) +
                    " t_clean=" + std::to_string(t_clean) + "s" +
                    " t_io_read=" + std::to_string(t_io_read) + "s" +
                    " t_compute=" + std::to_string(t_compute) + "s" +
                    " t_io_write=" + std::to_string(t_io_write) + "s");

        // Emit timing line – same format as Python worker
        std::printf(
            "WORKER_TIMING: {\"phase\":\"MAP\",\"split_id\":%d,"
            "\"t_clean\":%.3f,\"t_io_read\":%.3f,\"t_compute\":%.3f,\"t_io_write\":%.3f}\n",
            split_id, t_clean, t_io_read, t_compute, t_io_write);
        std::fflush(stdout);
    }

    // ── REDUCE ────────────────────────────────────────────────────────────────

    void execute_reduce(const std::unordered_map<std::string, JsonVal>& task) {
        int         reducer_id  = (int)task.at("reducer_id").as_int();
        const auto& map_workers = task.at("map_workers").as_arr();

        log("INFO", "REDUCE start reducer=" + std::to_string(reducer_id) +
                    " map_workers=" + std::to_string(map_workers.size()));

        std::unordered_map<std::string, int64_t> counts;
        const std::string ssh_opts =
            "-o StrictHostKeyChecking=no -o BatchMode=yes -o LogLevel=ERROR";

        double t_shuffle = 0.0, t_compute = 0.0;

        for (const auto& raw_ip : map_workers) {
            // Strip IPv4-mapped IPv6 prefix (::ffff:a.b.c.d → a.b.c.d)
            std::string worker_ip = raw_ip;
            if (worker_ip.substr(0, 7) == "::ffff:")
                worker_ip = worker_ip.substr(7);

            fs::path remote_part =
                local_map_dir / ("partition_" + std::to_string(reducer_id) + ".txt");
            std::string cmd =
                "ssh " + ssh_opts + " " + worker_ip + " 'cat " + remote_part.string() + "'";

            double t0 = now_s();
            FILE* fp = popen(cmd.c_str(), "r");
            if (!fp) {
                log("ERROR", "popen failed for worker " + worker_ip);
                continue;
            }

            std::vector<std::string> raw_lines;
            char buf[8192];
            while (std::fgets(buf, (int)sizeof(buf), fp))
                raw_lines.emplace_back(buf);
            pclose(fp);
            t_shuffle += now_s() - t0;

            double tc = now_s();
            for (const auto& line : raw_lines) {
                auto tab = line.find('\t');
                if (tab == std::string::npos) continue;
                std::string key     = line.substr(0, tab);
                std::string val_str = line.substr(tab + 1);
                val_str.erase(val_str.find_last_not_of(" \t\r\n") + 1);  // rtrim
                if (!key.empty() && !val_str.empty())
                    counts[key] += std::stoll(val_str);
            }
            t_compute += now_s() - tc;
        }

        // Sort descending by count, write output
        fs::create_directories(output_dir);
        fs::path out_path = output_dir / ("part-" + std::to_string(reducer_id) + ".txt");

        double t_io_write_start = now_s();
        {
            std::vector<std::pair<std::string, int64_t>> sorted_counts(counts.begin(),
                                                                        counts.end());
            std::sort(sorted_counts.begin(), sorted_counts.end(),
                      [](const auto& a, const auto& b) { return a.second > b.second; });
            std::ofstream fout(out_path);
            for (const auto& [k, v] : sorted_counts)
                fout << k << "\t" << v << "\n";
        }
        double t_io_write = now_s() - t_io_write_start;

        log("INFO", "REDUCE done reducer=" + std::to_string(reducer_id) +
                    " t_shuffle=" + std::to_string(t_shuffle) + "s" +
                    " t_compute=" + std::to_string(t_compute) + "s" +
                    " t_io_write=" + std::to_string(t_io_write) + "s");

        std::printf(
            "WORKER_TIMING: {\"phase\":\"REDUCE\",\"reducer_id\":%d,"
            "\"t_shuffle\":%.3f,\"t_compute\":%.3f,\"t_io_write\":%.3f}\n",
            reducer_id, t_shuffle, t_compute, t_io_write);
        std::fflush(stdout);
    }

public:
    Worker(std::string host, int port,
           std::string in_dir, std::string out_dir, std::string local_dir)
        : master_host(std::move(host))
        , master_port(port)
        , input_dir(in_dir)
        , output_dir(out_dir)
        , local_map_dir(local_dir) {}

    void run() {
        sock_fd = tcp_connect(master_host, master_port);
        log("INFO",
            "Connected to master " + master_host + ":" + std::to_string(master_port));

        clean_local_dir();

        while (true) {
            send_line(sock_fd, "{\"status\":\"READY_FOR_TASK\"}");

            std::string line;
            try {
                line = recv_line();
            } catch (const std::exception& e) {
                log("WARN", std::string("Master connection closed: ") + e.what());
                break;
            }

            std::unordered_map<std::string, JsonVal> task;
            try {
                task = parse_json(line);
            } catch (const std::exception& e) {
                log("ERROR",
                    std::string("JSON parse error: ") + e.what() + " | line: " + line);
                break;
            }

            std::string task_type =
                task.count("type") ? task.at("type").as_str() : "";
            log("INFO", "Received task type=" + task_type);

            if (task_type == "MAP") {
                execute_map(task);
                send_line(sock_fd, "{\"status\":\"TASK_FINISHED\"}");
                try { recv_line(); } catch (...) {
                    log("WARN", "Master closed connection before ACK after MAP");
                    break;
                }
            } else if (task_type == "REDUCE") {
                execute_reduce(task);
                send_line(sock_fd, "{\"status\":\"TASK_FINISHED\"}");
                try { recv_line(); } catch (...) {
                    log("WARN", "Master closed connection before ACK after REDUCE");
                    break;
                }
            } else if (task_type == "WAIT") {
                sleep(2);
                recv_buf.clear();  // discard any stale fragments
            } else {
                log("WARN", "Unknown task type received: " + task_type);
            }
        }

        close(sock_fd);
    }
};

// ── Utility: expand leading '~' ───────────────────────────────────────────────

static std::string expand_home(std::string path) {
    if (!path.empty() && path[0] == '~') {
        const char* home = std::getenv("HOME");
        if (home) path = std::string(home) + path.substr(1);
    }
    return path;
}

// ── main ──────────────────────────────────────────────────────────────────────

static void usage(const char* prog) {
    std::cerr
        << "Usage: " << prog
        << " -h HOST -p PORT -i INPUT_DIR -o OUTPUT_DIR -l LOCAL_MAP_DIR\n"
        << "  -h HOST           Master hostname or IP\n"
        << "  -p PORT           Master listening port\n"
        << "  -i INPUT_DIR      Shared input directory containing split files\n"
        << "  -o OUTPUT_DIR     Shared output directory for reduce results\n"
        << "  -l LOCAL_MAP_DIR  Local directory for MAP intermediate partitions\n";
}

int main(int argc, char** argv) {
    std::string host, input_dir, output_dir, local_map_dir;
    int  port = -1;
    int  opt;

    while ((opt = getopt(argc, argv, "h:p:i:o:l:")) != -1) {
        switch (opt) {
            case 'h': host         = optarg; break;
            case 'p': port         = std::stoi(optarg); break;
            case 'i': input_dir    = optarg; break;
            case 'o': output_dir   = optarg; break;
            case 'l': local_map_dir = optarg; break;
            default:  usage(argv[0]); return 1;
        }
    }

    if (host.empty() || port < 0 ||
        input_dir.empty() || output_dir.empty() || local_map_dir.empty()) {
        std::cerr << "Error: all arguments are required.\n";
        usage(argv[0]);
        return 1;
    }

    input_dir     = expand_home(input_dir);
    output_dir    = expand_home(output_dir);
    local_map_dir = expand_home(local_map_dir);

    try {
        Worker w(host, port, input_dir, output_dir, local_map_dir);
        w.run();
    } catch (const std::exception& e) {
        std::cerr << "[WORKER-CPP] [FATAL] " << e.what() << "\n";
        return 1;
    }
    return 0;
}
