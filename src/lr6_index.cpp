#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <limits>
#include <string>
#include <vector>

using u8  = uint8_t;
using u16 = uint16_t;
using u32 = uint32_t;
using u64 = uint64_t;

static void die(const std::string& msg) {
    std::cerr << "ERROR: " << msg << "\n";
    std::exit(1);
}

static inline bool is_space(char c) {
    return c==' ' || c=='\t' || c=='\r' || c=='\n' || c=='\f' || c=='\v';
}

static std::string to_lower_ascii(std::string s) {
    for (char& ch : s) {
        unsigned char u = (unsigned char)ch;
        if (u >= 'A' && u <= 'Z') ch = char(u - 'A' + 'a');
    }
    return s;
}

struct TokenPair {
    std::string term;
    u32 doc;
};

struct DictEntry {
    std::string term;
    u32 df;
    u64 postings_off;
};

static bool parse_tokens_line(const std::string& line, u32& docId, std::string& token) {

    size_t i = 0;
    while (i < line.size() && is_space(line[i])) i++;
    if (i >= line.size()) return false;


    u64 v = 0;
    bool any = false;
    while (i < line.size() && std::isdigit((unsigned char)line[i])) {
        any = true;
        u64 d = (u64)(line[i] - '0');
        if (v > (std::numeric_limits<u64>::max() - d) / 10) return false;
        v = v * 10 + d;
        i++;
    }
    if (!any) return false;


    if (i >= line.size() || !is_space(line[i])) return false;
    while (i < line.size() && is_space(line[i])) i++;
    if (i >= line.size()) return false;

    size_t start = i;
    while (i < line.size() && !is_space(line[i])) i++;
    token = line.substr(start, i - start);

    docId = (u32)v;
    return !token.empty();
}

static int hexval(char c) {
    if (c >= '0' && c <= '9') return (c - '0');
    if (c >= 'a' && c <= 'f') return 10 + (c - 'a');
    if (c >= 'A' && c <= 'F') return 10 + (c - 'A');
    return -1;
}


static std::string percent_decode(const std::string& s) {
    std::string out;
    out.reserve(s.size());
    for (size_t i = 0; i < s.size(); ) {
        char c = s[i];
        if (c == '%' && i + 2 < s.size()) {
            int h1 = hexval(s[i+1]);
            int h2 = hexval(s[i+2]);
            if (h1 >= 0 && h2 >= 0) {
                char b = (char)((h1 << 4) | h2);
                out.push_back(b);
                i += 3;
                continue;
            }
        }
        if (c == '+') { 
            out.push_back(' ');
            i++;
            continue;
        }
        out.push_back(c);
        i++;
    }
    return out;
}

static std::string title_from_url_norm(const std::string& url) {
    std::string tail = url;
    const std::string key = "/wiki/";
    size_t p = url.find(key);
    if (p != std::string::npos) tail = url.substr(p + key.size());
    else {
        size_t s = url.find_last_of('/');
        if (s != std::string::npos && s + 1 < url.size()) tail = url.substr(s + 1);
    }

    for (char& c : tail) if (c == '_') c = ' ';


    return percent_decode(tail);
}

static std::vector<std::string> extract_url_norms_from_json(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    if (!in) die("Cannot open JSON: " + path);

    std::string text((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    std::vector<std::string> urls;

    const std::string needle = "\"url_norm\"";
    size_t pos = 0;
    while (true) {
        size_t k = text.find(needle, pos);
        if (k == std::string::npos) break;
        size_t c = text.find(':', k + needle.size());
        if (c == std::string::npos) break;


        size_t q1 = text.find('"', c + 1);
        if (q1 == std::string::npos) break;

        std::string val;
        val.reserve(128);
        size_t i = q1 + 1;
        while (i < text.size()) {
            char ch = text[i];
            if (ch == '\\' && i + 1 < text.size()) {
                char nxt = text[i+1];
                
                if (nxt == '"' || nxt == '\\' || nxt == '/') { val.push_back(nxt); i += 2; continue; }
                if (nxt == 'n') { val.push_back('\n'); i += 2; continue; }
                if (nxt == 't') { val.push_back('\t'); i += 2; continue; }
                if (nxt == 'r') { val.push_back('\r'); i += 2; continue; }

                val.push_back(ch);
                i++;
                continue;
            }
            if (ch == '"') {
                break;
            }
            val.push_back(ch);
            i++;
        }
        urls.push_back(val);
        pos = i + 1;
    }

    return urls;
}

struct SectionInfo {
    u32 type = 0;
    u32 flags = 0;
    u64 offset = 0;
    u64 size = 0;
};

static void write_u16(std::ofstream& out, u16 v) { out.write((char*)&v, sizeof(v)); }
static void write_u32(std::ofstream& out, u32 v) { out.write((char*)&v, sizeof(v)); }
static void write_u64(std::ofstream& out, u64 v) { out.write((char*)&v, sizeof(v)); }
static void write_f64(std::ofstream& out, double v) { out.write((char*)&v, sizeof(v)); }

int main(int argc, char** argv) {
    if (argc < 3) {
        std::cerr <<
            "Usage:\n"
            "  " << argv[0] << " <tokens.txt> <index.bin> [ir_lr2.documents.json]\n\n"
            "Examples:\n"
            "  " << argv[0] << " tokens.txt index.bin ir_lr2.documents.json\n"
            "  " << argv[0] << " tokens.txt index.bin\n";
        return 1;
    }

    const std::string tokens_path = argv[1];
    const std::string out_path    = argv[2];
    const bool has_json = (argc >= 4);
    const std::string json_path = has_json ? argv[3] : "";

    auto t0 = std::chrono::high_resolution_clock::now();


    std::ifstream tin(tokens_path);
    if (!tin) die("Cannot open tokens file: " + tokens_path);

    std::vector<TokenPair> pairs;
    pairs.reserve(1 << 20);

    std::string line;
    u32 max_doc = 0;
    u64 total_tokens = 0;
    u64 sum_term_len = 0;

    while (std::getline(tin, line)) {
        u32 docId = 0;
        std::string tok;
        if (!parse_tokens_line(line, docId, tok)) continue;

        tok = to_lower_ascii(tok);
        pairs.push_back({tok, docId});

        if (docId > max_doc) max_doc = docId;
        total_tokens++;
        sum_term_len += tok.size();
    }

    if (pairs.empty()) die("No tokens parsed from " + tokens_path);

    u32 docs_count = max_doc + 1;

    std::vector<std::string> urls;
    if (has_json) {
        urls = extract_url_norms_from_json(json_path);
        if (urls.empty()) {
            std::cerr << "WARN: no url_norm found in JSON, will use placeholders.\n";
        }
    }

    std::vector<std::string> fwd_url(docs_count), fwd_title(docs_count);

    for (u32 d = 0; d < docs_count; d++) {
        if (!urls.empty() && d < (u32)urls.size()) {
            fwd_url[d] = urls[d];
            fwd_title[d] = title_from_url_norm(urls[d]);
            if (fwd_title[d].empty()) fwd_title[d] = "Document " + std::to_string(d);
        } else {
            fwd_url[d] = "";
            fwd_title[d] = "Document " + std::to_string(d);
        }
    }

    std::sort(pairs.begin(), pairs.end(),
        [](const TokenPair& a, const TokenPair& b) {
            if (a.term < b.term) return true;
            if (a.term > b.term) return false;
            return a.doc < b.doc;
        }
    );


    std::vector<DictEntry> dict;
    dict.reserve(100000);

    std::vector<u32> postings_blob;
    postings_blob.reserve(pairs.size());

    u32 unique_terms = 0;

    size_t i = 0;
    while (i < pairs.size()) {
        const std::string& term = pairs[i].term;
        u64 postings_off = (u64)postings_blob.size() * sizeof(u32);

        u32 last_doc = std::numeric_limits<u32>::max();
        u32 df = 0;

        while (i < pairs.size() && pairs[i].term == term) {
            u32 d = pairs[i].doc;
            if (d != last_doc) {
                postings_blob.push_back(d);
                last_doc = d;
                df++;
            }
            i++;
        }

        dict.push_back({term, df, postings_off});
        unique_terms++;
    }

    double avg_term_len = (unique_terms > 0) ? (double)sum_term_len / (double)total_tokens : 0.0;

    auto t1 = std::chrono::high_resolution_clock::now();
    double build_ms = std::chrono::duration<double, std::milli>(t1 - t0).count();


    std::ofstream out(out_path, std::ios::binary);
    if (!out) die("Cannot open output file: " + out_path);


    char magic[4] = {'I','R','I','X'};
    out.write(magic, 4);
    write_u32(out, 1);
    write_u32(out, 0);
    write_u64(out, 0);

    std::vector<SectionInfo> sections;

    auto mark_section = [&](u32 type, u32 flags, u64 start_off) {
        SectionInfo s;
        s.type = type;
        s.flags = flags;
        s.offset = start_off;
        s.size = 0;
        sections.push_back(s);
    };

    auto cur_off = [&]() -> u64 {
        return (u64)out.tellp();
    };


    {
        u64 start = cur_off();
        mark_section(4, 0, start);

        write_u32(out, docs_count);
        write_u64(out, total_tokens);
        write_u32(out, unique_terms);
        write_f64(out, avg_term_len);
        write_f64(out, build_ms);

        sections.back().size = cur_off() - start;
    }


    {
        u64 start = cur_off();
        mark_section(1, 0, start);

        write_u32(out, (u32)dict.size());
        for (const auto& e : dict) {
            if (e.term.size() > 65535) die("Term too long (>65535 bytes): " + e.term);
            write_u16(out, (u16)e.term.size());
            out.write(e.term.data(), (std::streamsize)e.term.size());
            write_u32(out, e.df);
            write_u64(out, e.postings_off);
        }

        sections.back().size = cur_off() - start;
    }


    {
        u64 start = cur_off();
        mark_section(2, 0, start);


        if (!postings_blob.empty()) {
            out.write((const char*)postings_blob.data(),
                      (std::streamsize)(postings_blob.size() * sizeof(u32)));
        }

        sections.back().size = cur_off() - start;
    }


    {
        u64 start = cur_off();
        mark_section(3, 0, start);

        write_u32(out, docs_count);
        for (u32 d = 0; d < docs_count; d++) {
            const std::string& url = fwd_url[d];
            const std::string& ttl = fwd_title[d];

            write_u32(out, (u32)url.size());
            if (!url.empty()) out.write(url.data(), (std::streamsize)url.size());

            write_u32(out, (u32)ttl.size());
            if (!ttl.empty()) out.write(ttl.data(), (std::streamsize)ttl.size());
        }

        sections.back().size = cur_off() - start;
    }

    u64 table_off = cur_off();
    for (const auto& s : sections) {
        write_u32(out, s.type);
        write_u32(out, s.flags);
        write_u64(out, s.offset);
        write_u64(out, s.size);
    }

    out.seekp(4 + 4, std::ios::beg);
    write_u32(out, (u32)sections.size());
    write_u64(out, table_off);

    out.close();


    double tokens_per_ms = (build_ms > 0.0) ? (double)total_tokens / build_ms : 0.0;

    std::cout << "OK: wrote " << out_path << "\n";
    std::cout << "Docs: " << docs_count << "\n";
    std::cout << "Total tokens: " << total_tokens << "\n";
    std::cout << "Unique terms: " << unique_terms << "\n";
    std::cout << "Avg token(term) length (bytes): " << avg_term_len << "\n";
    std::cout << "Indexing time (ms): " << build_ms << "\n";
    std::cout << "Tokens per ms: " << tokens_per_ms << " (~" << (tokens_per_ms * 1000.0) << " tokens/s)\n";

    std::cout << "Time per document (ms/doc): " << (build_ms / (double)docs_count) << "\n";

    return 0;
}
