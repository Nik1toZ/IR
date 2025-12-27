import os
import re
import json
import time
import heapq

import requests

API = "https://ru.wikipedia.org/w/api.php"

OUT_DIR = "corpus_raw"
STATE_DIR = os.path.join(OUT_DIR, "_state_html_v3")

RAW_DIR = os.path.join(OUT_DIR, "wiki_html_docs")
META_PATH = os.path.join(OUT_DIR, "wiki_html_metadata.jsonl")
STATS_PATH = os.path.join(OUT_DIR, "wiki_html_stats.json")
SEEN_PAGES_PATH = os.path.join(STATE_DIR, "seen_pages.json")
SEEN_CATS_PATH = os.path.join(STATE_DIR, "seen_categories.json")
QUEUE_PATH = os.path.join(STATE_DIR, "category_queue.json")

SEED_CATEGORIES = [
    "Категория:Футбольные клубы",
    "Категория:Футболисты",
    "Категория:Футбольные соревнования",
    "Категория:Футбольные стадионы",
    "Категория:Футбольные тренеры",
]

MAX_DEPTH = 4
TARGET_DOCS = 30000

MIN_WORDS = 1500
REQUEST_SLEEP = 0.15
TIMEOUT = 30
USER_AGENT = "IR-LR1-CorpusHTMLBot/3.0 (edu; contact: none)"


RE_YEAR_CAT = re.compile(r"Категория:\s*\d{3,4}\s*год\s+в\s+футболе", re.IGNORECASE)

BANNED_PATTERNS = [
    r"^Категория:Википедия:",
    r"^Категория:Портал:",
    r"по годам",
    r"год в футболе",
    r"по алфавиту",
    r"незавершённые статьи",
]
LOW_PATTERNS = [
    r"\bсписки\b",              
    r"по городам",
    r"по странам",
    r"по частям света",
    r"владельцы футбольных клубов",
]

BANNED_RX = [re.compile(p, re.IGNORECASE) for p in BANNED_PATTERNS]
LOW_RX = [re.compile(p, re.IGNORECASE) for p in LOW_PATTERNS]

def cat_priority(cat_title: str) -> int:
    if not cat_title:
        return 999
    ct = cat_title.strip()
    if RE_YEAR_CAT.fullmatch(ct):
        return 999
    for rx in BANNED_RX:
        if rx.search(ct):
            return 999
    for rx in LOW_RX:
        if rx.search(ct):
            return 2
    return 0


RE_WS = re.compile(r"\s+")

def normalize_ws(t: str) -> str:
    return RE_WS.sub(" ", (t or "")).strip()

def word_count(text: str) -> int:
    if not text:
        return 0
    tokens = re.findall(r"[A-Za-zА-Яа-яЁё0-9]+(?:-[A-Za-zА-Яа-яЁё0-9]+)?", text)
    return len(tokens)


def safe_mkdir(path: str):
    os.makedirs(path, exist_ok=True)

def load_json(path: str, default):
    if not os.path.exists(path):
        return default
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def save_json(path: str, obj):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)

def api_get(params: dict) -> dict:
    headers = {"User-Agent": USER_AGENT}
    r = requests.get(API, params=params, headers=headers, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()


def category_members(category_title: str, cmtype: str, continue_token: str | None):
    params = {
        "action": "query",
        "format": "json",
        "list": "categorymembers",
        "cmtitle": category_title,
        "cmnamespace": 0 if cmtype == "page" else 14,
        "cmlimit": 500,
    }
    if continue_token:
        params["cmcontinue"] = continue_token
    data = api_get(params)
    members = data.get("query", {}).get("categorymembers", [])
    cont = data.get("continue", {}).get("cmcontinue")
    return members, cont

def fetch_extract_plain(pageid: int) -> dict | None:
    params = {
        "action": "query",
        "format": "json",
        "prop": "extracts",
        "pageids": str(pageid),
        "explaintext": 1,
        "exsectionformat": "plain",
        "exlimit": 1,
        "formatversion": 2,
    }
    data = api_get(params)
    pages = data.get("query", {}).get("pages", [])
    if not pages:
        return None
    p = pages[0]
    return {"pageid": p.get("pageid"), "title": p.get("title"), "extract": p.get("extract") or ""}

def fetch_page_html(pageid: int) -> dict | None:
    params = {
        "action": "parse",
        "format": "json",
        "pageid": str(pageid),
        "prop": "text|revid|displaytitle",
        "formatversion": 2
    }
    data = api_get(params)
    parse = data.get("parse")
    if not parse:
        return None
    html = parse.get("text", "")
    return {
        "source": "ru.wikipedia.org",
        "pageid": parse.get("pageid"),
        "title": parse.get("title") or parse.get("displaytitle"),
        "revid": parse.get("revid"),
        "html": html
    }


def main():
    safe_mkdir(OUT_DIR)
    safe_mkdir(STATE_DIR)
    safe_mkdir(RAW_DIR)

    seen_pages = set(load_json(SEEN_PAGES_PATH, []))
    seen_cats = set(load_json(SEEN_CATS_PATH, []))

    
    q_list = load_json(QUEUE_PATH, None)
    heap = []
    if q_list is None:
        for c in SEED_CATEGORIES:
            pr = cat_priority(c)
            if pr < 999:
                heapq.heappush(heap, (pr, 0, c))
    else:
        for item in q_list:
            heapq.heappush(heap, (item["pr"], item["depth"], item["cat"]))

    stats = load_json(STATS_PATH, {
        "downloaded_docs": 0,
        "accepted_docs": 0,
        "skipped_short_before_html": 0,
        "skipped_banned_category": 0,
        "seen_pages": 0,
        "seen_categories": 0,
        "raw_bytes_total": 0,
        "accepted_raw_bytes_total": 0,
        "accepted_words_total": 0,
        "accepted_text_bytes_total": 0,
        "seed_categories": SEED_CATEGORIES,
        "max_depth": MAX_DEPTH,
        "min_words": MIN_WORDS,
        "format": "html",
        "strategy": "extract_filter_then_html",
        "queue": "priority"
    })

    accepted_docs = stats.get("accepted_docs", 0)

    with open(META_PATH, "a", encoding="utf-8") as meta_f:
        while heap and accepted_docs < TARGET_DOCS:
            pr, depth, cat = heapq.heappop(heap)

            if cat in seen_cats:
                continue
            seen_cats.add(cat)

            if depth > MAX_DEPTH:
                continue

            if pr >= 999:
                stats["skipped_banned_category"] += 1
                continue

            print(f"[CAT pr={pr} depth={depth}] {cat}")

            
            cont = None
            while True:
                members, cont = category_members(cat, "subcat", cont)
                for m in members:
                    sub = m.get("title")
                    if not sub:
                        continue
                    if sub in seen_cats:
                        continue
                    sub_pr = cat_priority(sub)
                    if sub_pr >= 999:
                        continue
                    if depth + 1 <= MAX_DEPTH:
                        heapq.heappush(heap, (sub_pr, depth + 1, sub))
                if not cont:
                    break
                time.sleep(REQUEST_SLEEP)

            
            cont = None
            while True:
                members, cont = category_members(cat, "page", cont)
                for m in members:
                    pageid = m.get("pageid")
                    if not pageid or pageid in seen_pages:
                        continue

                    seen_pages.add(pageid)

                    try:
                        ex = fetch_extract_plain(pageid)
                    except Exception as e:
                        print(f"  ! extract error pageid={pageid}: {e}")
                        time.sleep(REQUEST_SLEEP)
                        continue

                    if not ex:
                        time.sleep(REQUEST_SLEEP)
                        continue

                    extract_text = normalize_ws(ex.get("extract", ""))
                    wc = word_count(extract_text)
                    text_bytes = len(extract_text.encode("utf-8"))

                    if wc < MIN_WORDS:
                        stats["skipped_short_before_html"] += 1
                        time.sleep(REQUEST_SLEEP)
                        continue

                    try:
                        doc = fetch_page_html(pageid)
                    except Exception as e:
                        print(f"  ! html fetch error pageid={pageid}: {e}")
                        time.sleep(REQUEST_SLEEP)
                        continue

                    if not doc or not doc.get("html"):
                        time.sleep(REQUEST_SLEEP)
                        continue

                    raw_html = doc["html"]
                    raw_bytes = len(raw_html.encode("utf-8"))

                    raw_path = os.path.join(RAW_DIR, f"{pageid}.html")
                    with open(raw_path, "w", encoding="utf-8") as f:
                        f.write(raw_html)

                    stats["downloaded_docs"] += 1
                    stats["raw_bytes_total"] += raw_bytes
                    stats["accepted_docs"] += 1
                    accepted_docs = stats["accepted_docs"]
                    stats["accepted_raw_bytes_total"] += raw_bytes
                    stats["accepted_words_total"] += wc
                    stats["accepted_text_bytes_total"] += text_bytes


                    meta = {
                        "source": doc["source"],
                        "pageid": doc["pageid"],
                        "title": doc["title"],
                        "revid": doc["revid"],
                        "raw_path": raw_path.replace("\\", "/"),
                        "approx_words": wc,
                        "category_depth": depth,
                        "category_title": cat,
                        "category_priority": pr,
                    }
                    meta_f.write(json.dumps(meta, ensure_ascii=False) + "\n")
                    meta_f.flush()

                    if accepted_docs % 200 == 0:
                        print(f"  accepted={accepted_docs} short_before_html={stats['skipped_short_before_html']}")

                    time.sleep(REQUEST_SLEEP)
                    if accepted_docs >= TARGET_DOCS:
                        break

                if accepted_docs >= TARGET_DOCS or not cont:
                    break
                time.sleep(REQUEST_SLEEP)

            
            stats["seen_pages"] = len(seen_pages)
            stats["seen_categories"] = len(seen_cats)
            save_json(SEEN_PAGES_PATH, list(seen_pages))
            save_json(SEEN_CATS_PATH, list(seen_cats))
            save_json(QUEUE_PATH, [{"pr": a, "depth": b, "cat": c} for (a, b, c) in heap])
            save_json(STATS_PATH, stats)

    if stats["accepted_docs"] > 0:
        stats["avg_raw_bytes_accepted"] = stats["accepted_raw_bytes_total"] / stats["accepted_docs"]
        stats["avg_words_accepted"] = stats["accepted_words_total"] / stats["accepted_docs"]
        stats["avg_text_bytes_accepted"] = stats["accepted_text_bytes_total"] / stats["accepted_docs"]
    save_json(STATS_PATH, stats)

    print("\nDONE")
    print(json.dumps(stats, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
