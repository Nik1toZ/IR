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

struct SectionInfo {
    u32 type = 0;
    u32 flags = 0;
    u64 offset = 0;
    u64 size = 0;
};

static u16 read_u16(std::ifstream& in) {
    u16 v;
    in.read((char*)&v, sizeof(v));
    if (!in) die("read_u16 failed");
    return v;
}
static u32 read_u32(std::ifstream& in) {
    u32 v;
    in.read((char*)&v, sizeof(v));
    if (!in) die("read_u32 failed");
    return v;
}
static u64 read_u64(std::ifstream& in) {
    u64 v;
    in.read((char*)&v, sizeof(v));
    if (!in) die("read_u64 failed");
    return v;
}
static double read_f64(std::ifstream& in) {
    double v;
    in.read((char*)&v, sizeof(v));
    if (!in) die("read_f64 failed");
    return v;
}

struct DictEntry {
    std::string term;
    u32 df = 0;
    u64 postings_off = 0; 
};

struct DocInfo {
    std::string url;
    std::string title;
};

struct Index {
    u32 docs_count = 0;
    std::vector<DictEntry> dict;
    std::vector<u32> postings;
    std::vector<DocInfo> docs; 

    u64 postings_section_offset = 0;
    u64 postings_section_size = 0;
};

static bool find_section(const std::vector<SectionInfo>& secs, u32 type, SectionInfo& out) {
    for (const auto& s : secs) if (s.type == type) { out = s; return true; }
    return false;
}

static Index load_index(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    if (!in) die("Cannot open index: " + path);

    char magic[4];
    in.read(magic, 4);
    if (!in) die("Cannot read magic");
    if (!(magic[0]=='I' && magic[1]=='R' && magic[2]=='I' && magic[3]=='X')) {
        die("Bad magic, expected IRIX");
    }

    u32 version = read_u32(in);
    if (version != 1) die("Unsupported version (expected 1)");

    u32 section_count = read_u32(in);
    u64 section_table_off = read_u64(in);

    in.seekg((std::streamoff)section_table_off, std::ios::beg);
    if (!in) die("seekg to section_table_off failed");

    std::vector<SectionInfo> secs;
    secs.reserve(section_count);
    for (u32 i = 0; i < section_count; i++) {
        SectionInfo s;
        s.type   = read_u32(in);
        s.flags  = read_u32(in);
        s.offset = read_u64(in);
        s.size   = read_u64(in);
        secs.push_back(s);
    }

    SectionInfo meta, dictS, postS, fwdS;
    if (!find_section(secs, 4, meta))  die("META section(type=4) not found");
    if (!find_section(secs, 1, dictS)) die("DICT section(type=1) not found");
    if (!find_section(secs, 2, postS)) die("POSTINGS section(type=2) not found");
    if (!find_section(secs, 3, fwdS))  die("FORWARD section(type=3) not found");

    Index idx;
    idx.postings_section_offset = postS.offset;
    idx.postings_section_size   = postS.size;


    in.seekg((std::streamoff)meta.offset, std::ios::beg);
    if (!in) die("seekg to META failed");
    idx.docs_count = read_u32(in);
    (void)read_u64(in);
    (void)read_u32(in);
    (void)read_f64(in);
    (void)read_f64(in);


    in.seekg((std::streamoff)dictS.offset, std::ios::beg);
    if (!in) die("seekg to DICT failed");
    u32 term_count = read_u32(in);
    idx.dict.reserve(term_count);

    for (u32 i = 0; i < term_count; i++) {
        u16 len = read_u16(in);
        std::string term;
        term.resize(len);
        if (len) in.read(&term[0], (std::streamsize)len);
        if (!in) die("DICT: failed reading term bytes");

        u32 df = read_u32(in);
        u64 off = read_u64(in);

        idx.dict.push_back({term, df, off});
    }

    if (postS.size % sizeof(u32) != 0) die("POSTINGS size is not multiple of 4");
    u64 n_u32 = postS.size / sizeof(u32);

    idx.postings.resize((size_t)n_u32);
    in.seekg((std::streamoff)postS.offset, std::ios::beg);
    if (!in) die("seekg to POSTINGS failed");

    if (n_u32) {
        in.read((char*)idx.postings.data(), (std::streamsize)(n_u32 * sizeof(u32)));
        if (!in) die("POSTINGS: failed reading blob");
    }


    in.seekg((std::streamoff)fwdS.offset, std::ios::beg);
    if (!in) die("seekg to FORWARD failed");
    u32 docs_count2 = read_u32(in);
    if (docs_count2 != idx.docs_count) die("FORWARD docs_count differs from META docs_count");

    idx.docs.resize(idx.docs_count);
    for (u32 d = 0; d < idx.docs_count; d++) {
        u32 url_len = read_u32(in);
        std::string url; url.resize(url_len);
        if (url_len) in.read(&url[0], (std::streamsize)url_len);
        if (!in) die("FORWARD: failed reading url");

        u32 ttl_len = read_u32(in);
        std::string ttl; ttl.resize(ttl_len);
        if (ttl_len) in.read(&ttl[0], (std::streamsize)ttl_len);
        if (!in) die("FORWARD: failed reading title");

        idx.docs[d].url = std::move(url);
        idx.docs[d].title = std::move(ttl);
    }

    for (size_t i = 1; i < idx.dict.size(); i++) {
        if (!(idx.dict[i-1].term <= idx.dict[i].term)) die("DICT is not sorted by term");
    }

    return idx;
}

static std::vector<u32> postings_for_term(const Index& idx, const std::string& term_lower) {
    auto it = std::lower_bound(idx.dict.begin(), idx.dict.end(), term_lower,
        [](const DictEntry& e, const std::string& t){ return e.term < t; });

    if (it == idx.dict.end() || it->term != term_lower) return {};

    const u32 df = it->df;
    const u64 off_bytes = it->postings_off;

    if (off_bytes % sizeof(u32) != 0) die("postings_off not aligned");
    u64 off_u32 = off_bytes / sizeof(u32);

    if (off_u32 + df > idx.postings.size()) die("postings_off/df out of range");

    std::vector<u32> out;
    out.reserve(df);
    for (u32 i = 0; i < df; i++) out.push_back(idx.postings[(size_t)off_u32 + i]);
    return out;
}

static std::vector<u32> make_universe(u32 docs_count) {
    std::vector<u32> u;
    u.reserve(docs_count);
    for (u32 i = 0; i < docs_count; i++) u.push_back(i);
    return u;
}


static std::vector<u32> op_and(const std::vector<u32>& a, const std::vector<u32>& b) {
    std::vector<u32> out;
    out.reserve(std::min(a.size(), b.size()));
    size_t i = 0, j = 0;
    while (i < a.size() && j < b.size()) {
        u32 x = a[i], y = b[j];
        if (x == y) { out.push_back(x); i++; j++; }
        else if (x < y) i++;
        else j++;
    }
    return out;
}

static std::vector<u32> op_or(const std::vector<u32>& a, const std::vector<u32>& b) {
    std::vector<u32> out;
    out.reserve(a.size() + b.size());
    size_t i = 0, j = 0;
    while (i < a.size() && j < b.size()) {
        u32 x = a[i], y = b[j];
        if (x == y) { out.push_back(x); i++; j++; }
        else if (x < y) { out.push_back(x); i++; }
        else { out.push_back(y); j++; }
    }
    while (i < a.size()) out.push_back(a[i++]);
    while (j < b.size()) out.push_back(b[j++]);
    return out;
}

static std::vector<u32> op_not(const std::vector<u32>& universe, const std::vector<u32>& a) {
    std::vector<u32> out;
    out.reserve(universe.size() > a.size() ? (universe.size() - a.size()) : 0);
    size_t i = 0, j = 0;
    while (i < universe.size() && j < a.size()) {
        u32 x = universe[i], y = a[j];
        if (x == y) { i++; j++; }
        else if (x < y) { out.push_back(x); i++; }
        else { j++; }
    }
    while (i < universe.size()) out.push_back(universe[i++]);
    return out;
}



enum class TokType { TERM, AND, OR, NOT, LPAREN, RPAREN };

struct Tok {
    TokType type;
    std::string text; 
};

static bool is_term_char(unsigned char c) {
    if (is_space((char)c)) return false;
    if (c=='&' || c=='|' || c=='!' || c=='(' || c==')') return false;
    return true;
}

static std::vector<Tok> tokenize_query(const std::string& line_raw) {
    std::vector<Tok> toks;
    size_t i = 0;

    auto push_term = [&](const std::string& s) {
        if (!s.empty()) toks.push_back({TokType::TERM, to_lower_ascii(s)});
    };

    while (i < line_raw.size()) {
        if (is_space(line_raw[i])) { i++; continue; }

        char c = line_raw[i];
        if (c == '(') { toks.push_back({TokType::LPAREN, ""}); i++; continue; }
        if (c == ')') { toks.push_back({TokType::RPAREN, ""}); i++; continue; }
        if (c == '!') { toks.push_back({TokType::NOT, ""}); i++; continue; }

        if (c == '&') {
            if (i + 1 < line_raw.size() && line_raw[i+1] == '&') i += 2;
            else i += 1;
            toks.push_back({TokType::AND, ""});
            continue;
        }
        if (c == '|') {
            if (i + 1 < line_raw.size() && line_raw[i+1] == '|') i += 2;
            else i += 1;
            toks.push_back({TokType::OR, ""});
            continue;
        }

        size_t start = i;
        while (i < line_raw.size() && is_term_char((unsigned char)line_raw[i])) i++;
        push_term(line_raw.substr(start, i - start));
    }

    return toks;
}

static bool is_operand_like(TokType t) {
    return (t == TokType::TERM || t == TokType::RPAREN);
}

static std::vector<Tok> insert_implicit_and(const std::vector<Tok>& in) {
    std::vector<Tok> out;
    out.reserve(in.size() * 2);

    for (size_t i = 0; i < in.size(); i++) {
        const Tok& cur = in[i];
        if (!out.empty()) {
            TokType prevT = out.back().type;
            TokType curT  = cur.type;
            bool need =
                is_operand_like(prevT) &&
                (curT == TokType::TERM || curT == TokType::LPAREN || curT == TokType::NOT);
            if (need) out.push_back({TokType::AND, ""});
        }
        out.push_back(cur);
    }
    return out;
}

static int precedence(TokType t) {
    if (t == TokType::NOT) return 3;
    if (t == TokType::AND) return 2;
    if (t == TokType::OR)  return 1;
    return 0;
}
static bool is_right_assoc(TokType t) {
    return (t == TokType::NOT);
}

static bool to_rpn(const std::vector<Tok>& toks, std::vector<Tok>& rpn, std::string& err) {
    rpn.clear();
    std::vector<Tok> opstack;
    int par = 0;

    for (const Tok& tk : toks) {
        if (tk.type == TokType::TERM) {
            rpn.push_back(tk);
            continue;
        }
        if (tk.type == TokType::LPAREN) {
            opstack.push_back(tk);
            par++;
            continue;
        }
        if (tk.type == TokType::RPAREN) {
            par--;
            if (par < 0) { err = "Unmatched ')'"; return false; }
            while (!opstack.empty() && opstack.back().type != TokType::LPAREN) {
                rpn.push_back(opstack.back());
                opstack.pop_back();
            }
            if (opstack.empty()) { err = "Unmatched ')'"; return false; }
            opstack.pop_back();
            continue;
        }

        if (tk.type == TokType::NOT || tk.type == TokType::AND || tk.type == TokType::OR) {
            int p = precedence(tk.type);
            while (!opstack.empty()) {
                TokType topT = opstack.back().type;
                if (topT == TokType::LPAREN) break;
                int p2 = precedence(topT);
                if (p2 > p || (p2 == p && !is_right_assoc(tk.type))) {
                    rpn.push_back(opstack.back());
                    opstack.pop_back();
                } else break;
            }
            opstack.push_back(tk);
            continue;
        }

        err = "Unknown token";
        return false;
    }

    if (par != 0) { err = "Unmatched '('"; return false; }

    while (!opstack.empty()) {
        if (opstack.back().type == TokType::LPAREN) { err = "Unmatched '('"; return false; }
        rpn.push_back(opstack.back());
        opstack.pop_back();
    }
    return true;
}

static bool eval_rpn(const Index& idx, const std::vector<u32>& universe,
                     const std::vector<Tok>& rpn,
                     std::vector<u32>& out, std::string& err) {
    std::vector< std::vector<u32> > st;

    for (const Tok& tk : rpn) {
        if (tk.type == TokType::TERM) {
            st.push_back(postings_for_term(idx, tk.text));
            continue;
        }
        if (tk.type == TokType::NOT) {
            if (st.empty()) { err = "NOT without operand"; return false; }
            auto a = std::move(st.back()); st.pop_back();
            st.push_back(op_not(universe, a));
            continue;
        }
        if (tk.type == TokType::AND || tk.type == TokType::OR) {
            if (st.size() < 2) { err = "Binary operator without 2 operands"; return false; }
            auto b = std::move(st.back()); st.pop_back();
            auto a = std::move(st.back()); st.pop_back();
            st.push_back(tk.type == TokType::AND ? op_and(a, b) : op_or(a, b));
            continue;
        }
        err = "Unexpected token in RPN";
        return false;
    }

    if (st.size() != 1) { err = "Bad expression"; return false; }
    out = std::move(st.back());
    return true;
}

static bool run_query(const Index& idx, const std::vector<u32>& universe,
                      const std::string& qline,
                      std::vector<u32>& result,
                      std::string& err) {
    auto toks0 = tokenize_query(qline);
    auto toks  = insert_implicit_and(toks0);

    bool has_term = false;
    for (const auto& t : toks) if (t.type == TokType::TERM) { has_term = true; break; }
    if (!has_term) { result.clear(); return true; }

    std::vector<Tok> rpn;
    if (!to_rpn(toks, rpn, err)) return false;
    if (!eval_rpn(idx, universe, rpn, result, err)) return false;
    return true;
}


struct SlowItem {
    double ms = 0.0;
    size_t line_no = 0;
    std::string query;
    size_t hits = 0;
};

static void usage(const char* argv0) {
    std::cerr <<
        "Usage:\n"
        "  " << argv0 << " <index.bin> [--k N] [--top N] [--only-docid] [--no-results]\n"
        "                      [--report report.txt] [--topres N]\n\n"
        "stdin: queries (one per line)\n"
        "stdout: results per doc (default: docId\\tTitle\\tURL)\n"
        "stderr: top slow queries\n\n"
        "Examples:\n"
        "  " << argv0 << " index.bin < queries.txt > out.tsv\n"
        "  " << argv0 << " index.bin --report report.txt < queries.txt > out.tsv\n";
}

int main(int argc, char** argv) {
    if (argc < 2) {
        usage(argv[0]);
        return 1;
    }

    std::string index_path = argv[1];

    size_t k_limit = 0;
    size_t topN = 10;
    bool only_docid = false;
    bool no_results = false;

    std::string report_path;
    size_t report_topres = 50;

    for (int i = 2; i < argc; i++) {
        std::string a = argv[i];
        if (a == "--k") {
            if (i + 1 >= argc) die("--k requires number");
            k_limit = (size_t)std::stoull(argv[++i]);
        } else if (a == "--top") {
            if (i + 1 >= argc) die("--top requires number");
            topN = (size_t)std::stoull(argv[++i]);
        } else if (a == "--only-docid") {
            only_docid = true;
        } else if (a == "--no-results") {
            no_results = true;
        } else if (a == "--report") {
            if (i + 1 >= argc) die("--report requires path");
            report_path = argv[++i];
        } else if (a == "--topres") {
            if (i + 1 >= argc) die("--topres requires number");
            report_topres = (size_t)std::stoull(argv[++i]);
        } else if (a == "-h" || a == "--help") {
            usage(argv[0]);
            return 0;
        } else {
            die("Unknown arg: " + a);
        }
    }

    Index idx = load_index(index_path);
    std::vector<u32> universe = make_universe(idx.docs_count);

    std::ofstream rep;
    if (!report_path.empty()) {
        rep.open(report_path, std::ios::out | std::ios::binary);
        if (!rep) die("Cannot open report file: " + report_path);
    }

    std::vector<SlowItem> slows;
    slows.reserve(256);

    std::string line;
    size_t line_no = 0;

    while (std::getline(std::cin, line)) {
        line_no++;

        bool allspace = true;
        for (char c : line) if (!is_space(c)) { allspace = false; break; }
        if (allspace) continue;

        auto t0 = std::chrono::high_resolution_clock::now();

        std::vector<u32> res;
        std::string err;
        bool ok = run_query(idx, universe, line, res, err);

        auto t1 = std::chrono::high_resolution_clock::now();
        double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();

        if (!ok) {
            std::cerr << "WARN: line " << line_no << ": parse/eval error: " << err
                      << " | query: " << line << "\n";
            slows.push_back({ms, line_no, line, 0});

            if (rep) {
                rep << "QUERY\t" << line << "\n";
                rep << "HITS\t0\n";
                rep << "ERROR\t" << err << "\n\n";
            }
            continue;
        }

        slows.push_back({ms, line_no, line, res.size()});


        if (rep) {
            rep << "QUERY\t" << line << "\n";
            rep << "HITS\t" << res.size() << "\n";
            size_t cnt = 0;
            for (u32 docId : res) {
                if (docId >= idx.docs.size()) continue;
                const auto& di = idx.docs[docId];
                rep << di.title << "\t" << di.url << "\n";
                cnt++;
                if (cnt >= report_topres) break;
            }
            rep << "\n";
        }

        if (!no_results) {
            size_t printed = 0;
            for (u32 docId : res) {
                if (k_limit && printed >= k_limit) break;
                if (docId >= idx.docs.size()) continue;

                if (only_docid) {
                    std::cout << docId << "\n";
                } else {
                    const auto& di = idx.docs[docId];
                    std::cout << docId << "\t" << di.title << "\t" << di.url << "\n";
                }
                printed++;
            }
        }
    }


    if (!slows.empty()) {
        std::sort(slows.begin(), slows.end(), [](const SlowItem& a, const SlowItem& b) {
            return a.ms > b.ms;
        });

        size_t n = std::min(topN, slows.size());
        std::cerr << "---- TOP " << n << " slowest queries ----\n";
        std::cerr << "rank\tms\tline\thits\tquery\n";
        for (size_t i = 0; i < n; i++) {
            const auto& s = slows[i];
            std::cerr << (i+1) << "\t" << s.ms << "\t" << s.line_no << "\t" << s.hits << "\t" << s.query << "\n";
        }
        std::cerr << "--------------------------------\n";
    }

    return 0;
}
