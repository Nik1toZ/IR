#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <cstring>
#include <string>
#include <chrono>

static void die(const char* msg) {
    std::fprintf(stderr, "ERROR: %s\n", msg);
    std::exit(1);
}

static bool read_file_all(const char* path, std::string& out) {
    out.clear();
    FILE* f = std::fopen(path, "rb");
    if (!f) return false;
    std::fseek(f, 0, SEEK_END);
    long sz = std::ftell(f);
    if (sz < 0) { std::fclose(f); return false; }
    std::fseek(f, 0, SEEK_SET);
    out.resize((size_t)sz);
    if (sz > 0) {
        size_t got = std::fread(&out[0], 1, (size_t)sz, f);
        if (got != (size_t)sz) { std::fclose(f); return false; }
    }
    std::fclose(f);
    return true;
}

static inline bool is_ws(char c) {
    return c==' ' || c=='\n' || c=='\r' || c=='\t';
}



static inline bool is_cont(uint8_t b) { return (b & 0xC0u) == 0x80u; }

static uint32_t utf8_next(const std::string& s, size_t& pos) {
    if (pos >= s.size()) return 0;
    uint8_t b0 = (uint8_t)s[pos++];

    if (b0 < 0x80u) return b0;

    if ((b0 & 0xE0u) == 0xC0u) {
        if (pos >= s.size()) return 0xFFFDu;
        uint8_t b1 = (uint8_t)s[pos++];
        if (!is_cont(b1)) return 0xFFFDu;
        uint32_t cp = ((uint32_t)(b0 & 0x1Fu) << 6) | (uint32_t)(b1 & 0x3Fu);
        if (cp < 0x80u) return 0xFFFDu;
        return cp;
    }

    if ((b0 & 0xF0u) == 0xE0u) {
        if (pos + 1 >= s.size()) { pos = s.size(); return 0xFFFDu; }
        uint8_t b1 = (uint8_t)s[pos++];
        uint8_t b2 = (uint8_t)s[pos++];
        if (!is_cont(b1) || !is_cont(b2)) return 0xFFFDu;
        uint32_t cp = ((uint32_t)(b0 & 0x0Fu) << 12) | ((uint32_t)(b1 & 0x3Fu) << 6) | (uint32_t)(b2 & 0x3Fu);
        if (cp < 0x800u) return 0xFFFDu;
        if (cp >= 0xD800u && cp <= 0xDFFFu) return 0xFFFDu;
        return cp;
    }

    if ((b0 & 0xF8u) == 0xF0u) {
        if (pos + 2 >= s.size()) { pos = s.size(); return 0xFFFDu; }
        uint8_t b1 = (uint8_t)s[pos++];
        uint8_t b2 = (uint8_t)s[pos++];
        uint8_t b3 = (uint8_t)s[pos++];
        if (!is_cont(b1) || !is_cont(b2) || !is_cont(b3)) return 0xFFFDu;
        uint32_t cp = ((uint32_t)(b0 & 0x07u) << 18) | ((uint32_t)(b1 & 0x3Fu) << 12)
                    | ((uint32_t)(b2 & 0x3Fu) << 6) | (uint32_t)(b3 & 0x3Fu);
        if (cp < 0x10000u || cp > 0x10FFFFu) return 0xFFFDu;
        return cp;
    }

    return 0xFFFDu;
}

static inline bool is_digit(uint32_t cp) { return (cp >= '0' && cp <= '9'); }
static inline bool is_latin(uint32_t cp) { return (cp >= 'A' && cp <= 'Z') || (cp >= 'a' && cp <= 'z'); }
static inline bool is_cyrillic(uint32_t cp) { return (cp >= 0x0400u && cp <= 0x04FFu) || (cp >= 0x0500u && cp <= 0x052Fu); }

static inline bool is_combining_mark(uint32_t cp) {
    return (cp >= 0x0300u && cp <= 0x036Fu)  
        || (cp >= 0x1AB0u && cp <= 0x1AFFu)
        || (cp >= 0x1DC0u && cp <= 0x1DFFu)
        || (cp >= 0x20D0u && cp <= 0x20FFu)
        || (cp >= 0xFE20u && cp <= 0xFE2Fu);
}

static inline bool is_token_base(uint32_t cp) {
    return is_digit(cp) || is_latin(cp) || is_cyrillic(cp);
}



struct Stats {
    uint64_t docs_with_field = 0;
    uint64_t tokens = 0;
    uint64_t token_chars = 0;
    uint64_t text_bytes = 0;
};

static void flush_token(FILE* out, bool with_docid, uint64_t docid,
                        const std::string& token, uint64_t token_len_base,
                        Stats& st) {
    st.tokens++;
    st.token_chars += token_len_base;
    if (out) {
        if (with_docid) std::fprintf(out, "%llu\t", (unsigned long long)docid);
        std::fwrite(token.data(), 1, token.size(), out);
        std::fputc('\n', out);
    }
}



static void tokenize_text_utf8_emit(const std::string& text, Stats& st,
                                    FILE* out, bool with_docid, uint64_t docid) {
    st.text_bytes += (uint64_t)text.size();

    bool in_tok = false;
    bool last_was_hyphen = false;
    uint64_t cur_len_base = 0;
    std::string token;
    token.reserve(32);

    for (size_t pos = 0; pos < text.size(); ) {
        size_t cp_start = pos;
        uint32_t cp = utf8_next(text, pos);

        if (is_token_base(cp)) {
            if (!in_tok) {
                in_tok = true;
                last_was_hyphen = false;
                cur_len_base = 0;
                token.clear();
            }
            token.append(text.data() + cp_start, pos - cp_start);
            cur_len_base++;
            last_was_hyphen = false;
            continue;
        }

        
        if (in_tok && is_combining_mark(cp)) {
            token.append(text.data() + cp_start, pos - cp_start);
            continue;
        }

        if (cp == (uint32_t)'-') {
            if (in_tok && !last_was_hyphen) {
                size_t p2 = pos;
                if (p2 < text.size()) {
                    uint32_t nextcp = utf8_next(text, p2);
                    if (is_token_base(nextcp)) {
                        token.push_back('-');
                        last_was_hyphen = true;
                        continue;
                    }
                }
            }
        }

        if (in_tok) {
            flush_token(out, with_docid, docid, token, cur_len_base, st);
            in_tok = false;
            last_was_hyphen = false;
            cur_len_base = 0;
            token.clear();
        }
    }

    if (in_tok) {
        flush_token(out, with_docid, docid, token, cur_len_base, st);
    }
}



static int hexval(char c) {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return 10 + (c - 'a');
    if (c >= 'A' && c <= 'F') return 10 + (c - 'A');
    return -1;
}

static void append_utf8(std::string& out, uint32_t cp) {
    if (cp <= 0x7Fu) {
        out.push_back((char)cp);
    } else if (cp <= 0x7FFu) {
        out.push_back((char)(0xC0u | (cp >> 6)));
        out.push_back((char)(0x80u | (cp & 0x3Fu)));
    } else if (cp <= 0xFFFFu) {
        out.push_back((char)(0xE0u | (cp >> 12)));
        out.push_back((char)(0x80u | ((cp >> 6) & 0x3Fu)));
        out.push_back((char)(0x80u | (cp & 0x3Fu)));
    } else {
        out.push_back((char)(0xF0u | (cp >> 18)));
        out.push_back((char)(0x80u | ((cp >> 12) & 0x3Fu)));
        out.push_back((char)(0x80u | ((cp >> 6) & 0x3Fu)));
        out.push_back((char)(0x80u | (cp & 0x3Fu)));
    }
}

static bool parse_json_string_relaxed(const std::string& s, size_t& i, std::string& out) {
    out.clear();
    if (i >= s.size() || s[i] != '"') return false;
    i++;
    while (i < s.size()) {
        char c = s[i++];
        if (c == '"') return true;
        if (c == '\\') {
            if (i >= s.size()) return false;
            char e = s[i++];
            switch (e) {
                case '"': out.push_back('"'); break;
                case '\\': out.push_back('\\'); break;
                case '/': out.push_back('/'); break;
                case 'b': out.push_back('\b'); break;
                case 'f': out.push_back('\f'); break;
                case 'n': out.push_back('\n'); break;
                case 'r': out.push_back('\r'); break;
                case 't': out.push_back('\t'); break;
                case 'u': {
                    if (i + 3 >= s.size()) return false;
                    int v1 = hexval(s[i]), v2 = hexval(s[i+1]), v3 = hexval(s[i+2]), v4 = hexval(s[i+3]);
                    if (v1<0 || v2<0 || v3<0 || v4<0) return false;
                    uint32_t u = (uint32_t)((v1<<12) | (v2<<8) | (v3<<4) | v4);
                    i += 4;

                    if (u >= 0xD800u && u <= 0xDBFFu) {
                        if (i + 5 < s.size() && s[i] == '\\' && s[i+1] == 'u') {
                            i += 2;
                            int w1 = hexval(s[i]), w2 = hexval(s[i+1]), w3 = hexval(s[i+2]), w4 = hexval(s[i+3]);
                            if (w1<0 || w2<0 || w3<0 || w4<0) { append_utf8(out, 0xFFFDu); i += 4; break; }
                            uint32_t l = (uint32_t)((w1<<12) | (w2<<8) | (w3<<4) | w4);
                            i += 4;
                            if (l >= 0xDC00u && l <= 0xDFFFu) {
                                uint32_t cp = 0x10000u + (((u - 0xD800u) << 10) | (l - 0xDC00u));
                                append_utf8(out, cp);
                            } else append_utf8(out, 0xFFFDu);
                        } else append_utf8(out, 0xFFFDu);
                    } else if (u >= 0xDC00u && u <= 0xDFFFu) {
                        append_utf8(out, 0xFFFDu);
                    } else {
                        append_utf8(out, u);
                    }
                } break;
                default:
                    return false;
            }
        } else {
            out.push_back(c);
        }
    }
    return false;
}



static void process_json_in_memory(const std::string& json,
                                   const std::string& field,
                                   int log_every,
                                   FILE* out,
                                   bool with_docid,
                                   Stats& st) {
    std::string key, val;
    uint64_t docid = 0;
    auto t0 = std::chrono::high_resolution_clock::now();

    size_t i = 0;
    while (i < json.size()) {
        if (json[i] != '"') { i++; continue; }

        size_t save = i;
        if (!parse_json_string_relaxed(json, i, key)) { i = save + 1; continue; }

        while (i < json.size() && is_ws(json[i])) i++;
        if (i >= json.size() || json[i] != ':') continue;
        i++;
        while (i < json.size() && is_ws(json[i])) i++;

        if (key == field && i < json.size() && json[i] == '"') {
            size_t vpos = i;
            if (!parse_json_string_relaxed(json, i, val)) { i = vpos + 1; continue; }

            st.docs_with_field++;
            tokenize_text_utf8_emit(val, st, out, with_docid, docid);
            docid++;

            if (log_every > 0 && (st.docs_with_field % (uint64_t)log_every) == 0) {
                auto t1 = std::chrono::high_resolution_clock::now();
                double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
                double kb = (double)st.text_bytes / 1024.0;
                double sec = ms / 1000.0;
                double kbps = (sec > 0.0) ? (kb / sec) : 0.0;
                double avglen = (st.tokens > 0) ? ((double)st.token_chars / (double)st.tokens) : 0.0;

                std::printf("progress\tdocs=%llu\tkb=%.3f\ttime_ms=%.3f\tkbps=%.3f\ttokens=%llu\tavg_len=%.3f\n",
                    (unsigned long long)st.docs_with_field, kb, ms, kbps,
                    (unsigned long long)st.tokens, avglen);
            }
        }
    }
}



static const char* arg_value(int& i, int argc, char** argv) {
    if (i + 1 >= argc) die("Отсутствует значение аргумента");
    return argv[++i];
}

int main(int argc, char** argv) {
    const char* json_path = nullptr;
    std::string field = "parsed_text";
    int log_every = 0;

    const char* emit_path = nullptr;
    bool with_docid = false;

    for (int i = 1; i < argc; ++i) {
        if (std::strcmp(argv[i], "--json") == 0) json_path = arg_value(i, argc, argv);
        else if (std::strcmp(argv[i], "--field") == 0) field = arg_value(i, argc, argv);
        else if (std::strcmp(argv[i], "--log_every") == 0) { log_every = std::atoi(arg_value(i, argc, argv)); if (log_every < 0) log_every = 0; }
        else if (std::strcmp(argv[i], "--emit_tokens") == 0) emit_path = arg_value(i, argc, argv);
        else if (std::strcmp(argv[i], "--with_docid") == 0) with_docid = (std::atoi(arg_value(i, argc, argv)) != 0);
        else if (std::strcmp(argv[i], "--help") == 0 || std::strcmp(argv[i], "-h") == 0) {
            std::printf("Usage:\n  %s --json <file.json> [--field name] [--log_every N] [--emit_tokens file] [--with_docid 0|1]\n", argv[0]);
            return 0;
        } else {
            std::fprintf(stderr, "Unknown arg: %s\n", argv[i]);
            return 1;
        }
    }

    if (!json_path) die("Не задан --json <file>");

    std::string json;
    if (!read_file_all(json_path, json)) die("Не удалось прочитать JSON");

    FILE* out = nullptr;
    if (emit_path) {
        out = std::fopen(emit_path, "wb");
        if (!out) die("Не удалось открыть файл для токенов");
    }

    Stats st;
    auto t0 = std::chrono::high_resolution_clock::now();
    process_json_in_memory(json, field, log_every, out, with_docid, st);
    auto t1 = std::chrono::high_resolution_clock::now();

    if (out) std::fclose(out);

    double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
    double avglen = (st.tokens > 0) ? ((double)st.token_chars / (double)st.tokens) : 0.0;
    double kb = (double)st.text_bytes / 1024.0;
    double sec = ms / 1000.0;
    double kbps = (sec > 0.0) ? (kb / sec) : 0.0;
    double ms_per_kb = (kb > 0.0) ? (ms / kb) : 0.0;

    std::printf("\n=== TOKENIZATION SUMMARY ===\n");
    std::printf("field:\t\t\t%s\n", field.c_str());
    std::printf("docs_with_field:\t%llu\n", (unsigned long long)st.docs_with_field);
    std::printf("input_text_kb:\t\t%.3f\n", kb);
    std::printf("tokens:\t\t\t%llu\n", (unsigned long long)st.tokens);
    std::printf("avg_token_len:\t\t%.3f (без учёта диакритики)\n", avglen);
    std::printf("time_ms:\t\t%.3f\n", ms);
    std::printf("speed:\t\t\t%.3f KB/s\n", kbps);
    std::printf("time_per_kb:\t\t%.6f ms/KB\n", ms_per_kb);

    if (emit_path) {
        std::printf("tokens_saved_to:\t%s\n", emit_path);
        std::printf("with_docid:\t\t%d\n", with_docid ? 1 : 0);
    }

    return 0;
}
