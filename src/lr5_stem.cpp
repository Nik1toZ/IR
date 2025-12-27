#include <algorithm>
#include <cctype>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <limits>
#include <sstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include <cstring>

using std::string;

static inline bool is_space(char c) {
    return c==' ' || c=='\t' || c=='\r' || c=='\n' || c=='\f' || c=='\v';
}

static inline string ltrim(string s) {
    size_t i = 0;
    while (i < s.size() && is_space(s[i])) i++;
    s.erase(0, i);
    return s;
}
static inline string rtrim(string s) {
    size_t i = s.size();
    while (i > 0 && is_space(s[i-1])) i--;
    s.erase(i);
    return s;
}
static inline string trim(string s) { return rtrim(ltrim(std::move(s))); }

static inline string to_lower_ascii(string s) {
    for (char &c : s) c = (char)std::tolower((unsigned char)c);
    return s;
}


static string normalize_token_bytes(const string& in) {
    string s;
    s.reserve(in.size());
    for (unsigned char uc : in) {
        if ((uc >= 'a' && uc <= 'z') || (uc >= 'A' && uc <= 'Z') ||
            (uc >= '0' && uc <= '9') || uc == '_' || uc >= 128) {
            s.push_back((char)uc);
        } else {

        }
    }

    s = to_lower_ascii(s);
    return s;
}

static inline bool looks_ascii_word(const string& s) {
    bool has_alpha = false;
    for (unsigned char uc : s) {
        if (uc >= 128) return false;
        if (std::isalpha(uc)) has_alpha = true;
    }
    return has_alpha;
}

static inline bool looks_cyrillic_utf8(const string& s) {

    for (unsigned char uc : s) if (uc >= 128) return true;
    return false;
}


static string stem_en_light(string w) {
    if (w.size() < 4) return w;

    auto ends_with = [&](const char* suf) {
        size_t ls = std::strlen(suf);
        return (w.size() > ls + 1) && (w.compare(w.size()-ls, ls, suf) == 0);
    };
    auto cut = [&](size_t n) { w.resize(w.size() - n); };


    if (ends_with("ing")) cut(3);
    else if (ends_with("ed")) cut(2);
    else if (ends_with("ly")) cut(2);
    else if (ends_with("es")) cut(2);
    else if (ends_with("s"))  cut(1);


    if (w.size() < 3) return w;
    return w;
}


static string stem_ru_light(string w) {
    if (w.size() < 8) return w;

    auto ends_with = [&](const char* suf) {
        size_t ls = std::strlen(suf);
        return (w.size() > ls + 4) && (w.compare(w.size()-ls, ls, suf) == 0);
    };
    auto cut = [&](size_t nbytes) { w.resize(w.size() - nbytes); };

    static const char* suf[] = {
        "иями","ями","ами","иями","иям","ием","иях",
        "ого","ему","ыми","ими","ее","ое","ая","яя",
        "ов","ев","ей","ам","ям","ах","ях","ом","ем",
        "ы","и","а","я","о","е","у","ю"
    };

    for (auto sfx : suf) {
        if (ends_with(sfx)) {
            cut(std::strlen(sfx));
            break;
        }
    }

    if (w.size() < 6) return w;
    return w;
}

static string stem_term(string term, bool enable_stem) {
    term = normalize_token_bytes(term);
    if (term.size() < 2) return "";

    if (!enable_stem) return term;

    if (looks_ascii_word(term)) {
        return stem_en_light(term);
    }
    if (looks_cyrillic_utf8(term)) {

        return stem_ru_light(term);
    }
    return term;
}


using DocId = int;
using TFMap = std::unordered_map<DocId, int>;
using Index = std::unordered_map<string, TFMap>;

struct SearchConfig {
    string tokens_path = "tokens.txt";
    int topk = 10;
    bool enable_stem = true;
    double exact_bonus = 0.5; 
};

struct CorpusIndex {
    Index stem_index;
    Index exact_index;
    std::unordered_set<DocId> all_docs;
};

static bool parse_doc_token_line(const string& line, DocId& doc, string& token) {
    std::istringstream iss(line);
    if (!(iss >> doc)) return false;
    if (!(iss >> token)) return false;
    return true;
}

static CorpusIndex build_index_from_tokens(const SearchConfig& cfg) {
    CorpusIndex ci;

    std::ifstream in(cfg.tokens_path);
    if (!in) {
        std::cerr << "ERROR: cannot open tokens file: " << cfg.tokens_path << "\n";
        std::exit(1);
    }

    string line;
    long long lines = 0;
    long long kept = 0;

    while (std::getline(in, line)) {
        lines++;
        line = trim(line);
        if (line.empty()) continue;

        DocId doc;
        string tok;
        if (!parse_doc_token_line(line, doc, tok)) continue;

        string exact = normalize_token_bytes(tok);
        if (exact.size() < 2) continue;

        if (exact.size() > 64) continue;

        string stem = stem_term(exact, cfg.enable_stem);

        ci.all_docs.insert(doc);
        ci.exact_index[exact][doc] += 1;
        ci.stem_index[stem][doc] += 1;

        kept++;
    }

    std::cerr << "Index built: docs=" << ci.all_docs.size()
              << ", lines=" << lines
              << ", kept=" << kept
              << ", stem_terms=" << ci.stem_index.size()
              << ", exact_terms=" << ci.exact_index.size()
              << "\n";
    return ci;
}

static inline double tf_weight(int tf) {
    return 1.0 + std::log((double)tf);
}
static inline double idf_weight(int N, int df) {
    return std::log((double)(N + 1) / (double)(df + 1)) + 1.0;
}

static std::vector<string> split_query_into_terms(const string& q) {
    
    std::vector<string> out;
    string cur;
    for (char c : q) {
        if (is_space(c)) {
            if (!cur.empty()) { out.push_back(cur); cur.clear(); }
        } else {
            cur.push_back(c);
        }
    }
    if (!cur.empty()) out.push_back(cur);
    return out;
}

struct Hit {
    DocId doc;
    double score;
};

static std::vector<Hit> search_query(
    const CorpusIndex& ci,
    const SearchConfig& cfg,
    const string& query_text
) {
    const int N = (int)ci.all_docs.size();
    if (N == 0) return {};

    
    auto raw_terms = split_query_into_terms(query_text);

    std::vector<string> q_exact;
    std::vector<string> q_stem;

    q_exact.reserve(raw_terms.size());
    q_stem.reserve(raw_terms.size());

    for (const auto& t : raw_terms) {
        string ex = normalize_token_bytes(t);
        if (ex.size() < 2) continue;
        if (ex.size() > 64) continue;

        q_exact.push_back(ex);
        q_stem.push_back(stem_term(ex, cfg.enable_stem));
    }

    
    std::unordered_set<DocId> candidates;
    candidates.reserve(4096);

    for (const auto& st : q_stem) {
        auto it = ci.stem_index.find(st);
        if (it == ci.stem_index.end()) continue;
        for (const auto& kv : it->second) candidates.insert(kv.first);
    }

    
    std::unordered_map<DocId, double> score;
    score.reserve(candidates.size() * 2 + 1);

    
    for (size_t i = 0; i < q_stem.size(); i++) {
        const auto& st = q_stem[i];
        auto it = ci.stem_index.find(st);
        if (it == ci.stem_index.end()) continue;

        int df = (int)it->second.size();
        double idf = idf_weight(N, df);

        for (const auto& kv : it->second) {
            DocId d = kv.first;
            int tf = kv.second;
            score[d] += tf_weight(tf) * idf;
        }
    }

    
    if (cfg.exact_bonus != 0.0) {
        for (const auto& ex : q_exact) {
            auto it = ci.exact_index.find(ex);
            if (it == ci.exact_index.end()) continue;

            
            for (const auto& kv : it->second) {
                DocId d = kv.first;
                if (score.find(d) != score.end()) {
                    score[d] += cfg.exact_bonus;
                }
            }
        }
    }

    
    std::vector<Hit> hits;
    hits.reserve(score.size());
    for (const auto& kv : score) hits.push_back({kv.first, kv.second});

    std::sort(hits.begin(), hits.end(), [](const Hit& a, const Hit& b) {
        if (a.score != b.score) return a.score > b.score;
        return a.doc < b.doc;
    });

    if ((int)hits.size() > cfg.topk) hits.resize(cfg.topk);
    return hits;
}

static void print_hits(const std::vector<Hit>& hits) {
    if (hits.empty()) {
        std::cout << "(no results)\n";
        return;
    }
    for (size_t i = 0; i < hits.size(); i++) {
        std::cout << (i+1) << ". doc=" << hits[i].doc << "\tscore=" << hits[i].score << "\n";
    }
}

static void usage(const char* argv0) {
    std::cerr
        << "Usage:\n"
        << "  " << argv0 << " --tokens tokens.txt [--topk 10] [--bonus 0.5] [--no-stem] [\"query text\"]\n"
        << "  " << argv0 << " --tokens tokens.txt --compare queries.txt [--out compare.tsv] [--topk 10] [--bonus 0.5]\n"
        << "\n"
        << "Examples:\n"
        << "  " << argv0 << " --tokens tokens.txt\n"
        << "  " << argv0 << " --tokens tokens.txt \"футболист забил гол\"\n"
        << "  " << argv0 << " --tokens tokens.txt --compare queries.txt --out compare.tsv\n";
}

static bool file_exists(const string& path) {
    std::ifstream f(path);
    return (bool)f;
}

int main(int argc, char** argv) {
    SearchConfig cfg;
    string query_arg;
    bool compare_mode = false;
    string compare_path;
    string out_path = "compare.tsv";

    for (int i = 1; i < argc; i++) {
        string a = argv[i];
        if (a == "--tokens" && i+1 < argc) {
            cfg.tokens_path = argv[++i];
        } else if (a == "--topk" && i+1 < argc) {
            cfg.topk = std::max(1, std::atoi(argv[++i]));
        } else if (a == "--bonus" && i+1 < argc) {
            cfg.exact_bonus = std::atof(argv[++i]);
        } else if (a == "--no-stem") {
            cfg.enable_stem = false;
        } else if (a == "--compare" && i+1 < argc) {
            compare_mode = true;
            compare_path = argv[++i];
        } else if (a == "--out" && i+1 < argc) {
            out_path = argv[++i];
        } else if (a == "--help" || a == "-h") {
            usage(argv[0]);
            return 0;
        } else {
            
            if (!query_arg.empty()) query_arg.push_back(' ');
            query_arg += a;
        }
    }

    if (!file_exists(cfg.tokens_path)) {
        std::cerr << "ERROR: tokens file not found: " << cfg.tokens_path << "\n";
        std::cerr << "Tip: run from the directory where tokens.txt is located, or pass --tokens path/to/tokens.txt\n";
        return 1;
    }

    
    CorpusIndex ci = build_index_from_tokens(cfg);

    
    if (compare_mode) {
        if (compare_path.empty() || !file_exists(compare_path)) {
            std::cerr << "ERROR: compare queries file not found: " << compare_path << "\n";
            return 1;
        }

        std::ifstream qf(compare_path);
        std::ofstream out(out_path);
        if (!out) {
            std::cerr << "ERROR: cannot open output file: " << out_path << "\n";
            return 1;
        }

        out << "query\tmode\trank\tdoc\tscore\n";

        string qline;
        while (std::getline(qf, qline)) {
            qline = trim(qline);
            if (qline.empty()) continue;

            
            {
                SearchConfig c0 = cfg;
                c0.enable_stem = false;
                auto hits0 = search_query(ci, c0, qline);
                for (size_t r = 0; r < hits0.size(); r++) {
                    out << qline << "\tno_stem\t" << (r+1) << "\t" << hits0[r].doc << "\t" << hits0[r].score << "\n";
                }
            }
            
            {
                SearchConfig c1 = cfg;
                c1.enable_stem = true;
                auto hits1 = search_query(ci, c1, qline);
                for (size_t r = 0; r < hits1.size(); r++) {
                    out << qline << "\tstem\t" << (r+1) << "\t" << hits1[r].doc << "\t" << hits1[r].score << "\n";
                }
            }
        }

        std::cerr << "OK: wrote " << out_path << "\n";
        return 0;
    }

    
    if (!query_arg.empty()) {
        auto hits = search_query(ci, cfg, query_arg);
        print_hits(hits);
        return 0;
    }

    
    std::cerr
        << "Interactive search.\n"
        << "Tokens: " << cfg.tokens_path << "\n"
        << "Stem: " << (cfg.enable_stem ? "ON" : "OFF")
        << ", exact_bonus=" << cfg.exact_bonus
        << ", topk=" << cfg.topk << "\n"
        << "Type query and press Enter. Empty line or :q to quit.\n";

    while (true) {
        std::cout << "> " << std::flush;
        string q;
        if (!std::getline(std::cin, q)) break;
        q = trim(q);
        if (q.empty() || q == ":q" || q == "quit" || q == "exit") break;

        auto hits = search_query(ci, cfg, q);
        print_hits(hits);
    }

    return 0;
}
