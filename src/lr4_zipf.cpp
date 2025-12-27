#include <algorithm>
#include <cctype>
#include <cmath>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <string>
#include <unordered_map>
#include <vector>

static inline std::string trim(const std::string& s) {
    size_t a = 0, b = s.size();
    while (a < b && (s[a] == '\r' || s[a] == '\n' || std::isspace((unsigned char)s[a]))) a++;
    while (b > a && (s[b - 1] == '\r' || s[b - 1] == '\n' || std::isspace((unsigned char)s[b - 1]))) b--;
    return s.substr(a, b - a);
}

static inline void to_lower_inplace(std::string& s) {
    for (char &ch : s) {
        unsigned char c = (unsigned char)ch;
        if (c < 128) ch = (char)std::tolower(c);
    }
}

static double median(std::vector<double> v) {
    if (v.empty()) return 0.0;
    std::sort(v.begin(), v.end());
    size_t n = v.size();
    if (n % 2) return v[n/2];
    return 0.5 * (v[n/2 - 1] + v[n/2]);
}

int main(int argc, char** argv) {
    std::string in_path = "tokens.txt";
    std::string out_tsv = "zipf.tsv";
    std::string out_sum = "zipf_summary.txt";


    if (argc >= 2) in_path = argv[1];
    if (argc >= 3) out_tsv  = argv[2];

    std::ifstream in(in_path);
    if (!in) {
        std::cerr << "Не могу открыть " << in_path << "\n";
        return 1;
    }

    std::unordered_map<std::string, long long> freq;
    freq.reserve(1 << 20);

    long long total_tokens = 0;
    std::string line;
    while (std::getline(in, line)) {
        std::string tok = trim(line);
        if (tok.empty()) continue;
        to_lower_inplace(tok);
        freq[tok]++;
        total_tokens++;
    }
    in.close();

    if (freq.empty()) {
        std::cerr << "Пустой словарь: нет токенов.\n";
        return 2;
    }

    std::vector<long long> f;
    f.reserve(freq.size());
    for (auto &kv : freq) f.push_back(kv.second);
    std::sort(f.begin(), f.end(), std::greater<long long>());

    const long long V = (long long)f.size(); 

    long long r1 = std::max<long long>(10, V / 100);
    long long r2 = std::max<long long>(r1 + 10, V / 2);

    double sum_x = 0, sum_y = 0, sum_xx = 0, sum_xy = 0;
    long long n = 0;
    for (long long r = r1; r <= r2; r++) {
        long long fr = f[(size_t)r - 1];
        if (fr <= 0) continue;
        double x = std::log((double)r);
        double y = std::log((double)fr);
        sum_x  += x;
        sum_y  += y;
        sum_xx += x * x;
        sum_xy += x * y;
        n++;
    }

    double slope = 0.0;
    if (n >= 2) {
        double denom = (n * sum_xx - sum_x * sum_x);
        if (std::abs(denom) > 1e-12) {
            slope = (n * sum_xy - sum_x * sum_y) / denom;
        }
    }
    double s = -slope;


    if (!(s > 0.1 && s < 3.0)) s = 1.0;

    std::vector<double> cands;
    cands.reserve((size_t)std::max<long long>(0, r2 - r1 + 1));
    for (long long r = r1; r <= r2; r++) {
        long long fr = f[(size_t)r - 1];
        if (fr <= 0) continue;
        cands.push_back((double)fr * std::pow((double)r, s));
    }
    double C = cands.empty() ? (double)f[0] : median(cands);

    std::ofstream out(out_tsv);
    if (!out) {
        std::cerr << "Не могу создать " << out_tsv << "\n";
        return 3;
    }

    out << "# rank\tfreq\tzipf_fit\n";
    out << std::fixed << std::setprecision(6);

    for (long long r = 1; r <= V; r++) {
        double fit = C / std::pow((double)r, s);
        out << r << "\t" << f[(size_t)r - 1] << "\t" << fit << "\n";
    }
    out.close();


    std::ofstream sum(out_sum);
    if (sum) {
        sum << "Вход: " << in_path << "\n";
        sum << "Всего токенов N = " << total_tokens << "\n";
        sum << "Размер словаря V = " << V << "\n";
        sum << "Top-1 частота f(1) = " << f[0] << "\n";
        sum << "Оценка Zipf: f(r) ~= C / r^s\n";
        sum << "s = " << std::setprecision(6) << s << "\n";
        sum << "C = " << std::setprecision(6) << C << "\n";
        sum << "Диапазон оценки (r1..r2): " << r1 << ".." << r2 << "\n";
    }

    std::cout << "OK: wrote " << out_tsv << "\n";
    std::cout << "Summary: zipf_summary.txt\n";
    return 0;
}
