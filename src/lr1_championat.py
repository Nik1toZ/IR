import os
import re
import json
import time
import hashlib
from urllib.parse import urljoin, urlparse
from datetime import datetime
import html as ihtml
import requests
from bs4 import BeautifulSoup
from bs4 import XMLParsedAsHTMLWarning
import warnings
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

OUT_DIR = "corpus_raw"
STATE_DIR = os.path.join(OUT_DIR, "_state_championat_longreads_v5")

RAW_DIR = os.path.join(OUT_DIR, "championat_longreads_html")
META_PATH = os.path.join(OUT_DIR, "championat_longreads_metadata.jsonl")
STATS_PATH = os.path.join(OUT_DIR, "championat_longreads_stats.json")

SEEN_URLS_PATH = os.path.join(STATE_DIR, "seen_urls.json")
LISTING_POS_PATH = os.path.join(STATE_DIR, "listing_pos.json")
ARTICLE_QUEUE_PATH = os.path.join(STATE_DIR, "article_queue.json")

USER_AGENT = "IR-LR1-ChampionatLongreadsBot/5.0 (edu; contact: none)"
TIMEOUT = 30
REQUEST_SLEEP = 0.35

TARGET_DOCS = 30000
MIN_WORDS = 900

LIST_PAGES_FROM = 1
LIST_PAGES_TO = 60

ARTICLES_PER_LISTING = 15

LISTING_URLS = [
    f"https://www.championat.com/articles/football/{i}.html"
    for i in range(LIST_PAGES_FROM, LIST_PAGES_TO + 1)
]

RE_TOKEN_WORD = re.compile(r"[A-Za-zА-Яа-яЁё0-9]+(?:-[A-Za-zА-Яа-яЁё0-9]+)?")

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

def http_get(url: str) -> str:
    headers = {
        "User-Agent": USER_AGENT,
        "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.7",
    }
    r = requests.get(url, headers=headers, timeout=TIMEOUT)
    r.raise_for_status()
    return r.text

def norm_url(u: str) -> str:
    p = urlparse(u)
    return p._replace(fragment="").geturl()

def url_to_id(url: str) -> str:
    return hashlib.sha1(url.encode("utf-8")).hexdigest()[:16]

def is_article_longread_url(u: str) -> bool:
    p = urlparse(u)
    if "championat.com" not in p.netloc:
        return False
    path = p.path
    if "/football/article-" not in path:
        return False
    if not path.endswith(".html"):
        return False
    if any(x in u for x in ["/rss/", "/video/", "/photo/", "/auth/", "/social/", "/forum/"]):
        return False
    return True

def extract_links_from_listing(html: str, base_url: str) -> list[str]:
    soup = BeautifulSoup(html, "lxml")
    urls = set()
    for a in soup.select("a[href]"):
        href = a.get("href")
        if not href:
            continue
        u = norm_url(urljoin(base_url, href))
        if is_article_longread_url(u):
            urls.add(u)
    return sorted(urls)

def extract_text_blocks(soup: BeautifulSoup) -> str:
    containers = []

    art = soup.find("article")
    if art:
        containers.append(art)

    ab = soup.find(attrs={"itemprop": "articleBody"})
    if ab:
        containers.append(ab)

    for cls in ["article__content", "content", "article-content", "text"]:
        for d in soup.select(f".{cls}"):
            containers.append(d)

    main = soup.find("main")
    if main:
        containers.append(main)

    if not containers:
        containers = [soup]

    paras = []
    seen = set()
    for c in containers:
        for p in c.find_all("p"):
            txt = p.get_text(" ", strip=True)
            if not txt or len(txt) < 40:
                continue
            if txt in seen:
                continue
            seen.add(txt)
            paras.append(txt)

    return "\n".join(paras).strip()

def extract_article_meta(html: str, url: str) -> dict:
    soup = BeautifulSoup(html, "lxml")

    title = ""
    h1 = soup.find("h1")
    if h1:
        title = h1.get_text(" ", strip=True)
    if not title:
        t = soup.find("title")
        title = t.get_text(" ", strip=True) if t else ""

    date = ""

    time_tag = soup.find("time")
    if time_tag:
        dt = time_tag.get("datetime") or time_tag.get("content") or ""
        dt = dt.strip()
        if dt:
            date = dt

    if not date:
        candidates = [
            ("meta", {"property": "article:published_time"}),
            ("meta", {"name": "article:published_time"}),
            ("meta", {"property": "og:pubdate"}),
            ("meta", {"name": "pubdate"}),
            ("meta", {"itemprop": "datePublished"}),
            ("meta", {"name": "date"}),
        ]
        for tag, attrs in candidates:
            m = soup.find(tag, attrs=attrs)
            if m and m.get("content"):
                date = m["content"].strip()
                if date:
                    break

    if not date:
        for s in soup.find_all("script", attrs={"type": "application/ld+json"}):
            try:
                raw = s.string or s.get_text() or ""
                raw = raw.strip()
                if not raw:
                    continue
                raw = ihtml.unescape(raw)
                data = json.loads(raw)

                items = data if isinstance(data, list) else [data]
                for it in items:
                    if isinstance(it, dict) and "datePublished" in it:
                        v = str(it.get("datePublished") or "").strip()
                        if v:
                            date = v
                            break
                if date:
                    break
            except Exception:
                continue

    date = (date or "").strip()


    text = extract_text_blocks(soup)
    wc = len(RE_TOKEN_WORD.findall(text))

    return {"url": url, "title": title.strip(), "date": date, "text": text, "approx_words": wc}

def save_state(seen_urls, listing_pos, article_queue, stats):
    save_json(SEEN_URLS_PATH, list(seen_urls))
    save_json(LISTING_POS_PATH, listing_pos)
    save_json(ARTICLE_QUEUE_PATH, article_queue)
    if stats["accepted_articles"] > 0:
        stats["avg_raw_bytes_accepted"] = stats["accepted_raw_bytes_total"] / stats["accepted_articles"]
        stats["avg_words_accepted"] = stats["accepted_words_total"] / stats["accepted_articles"]
        stats["avg_text_bytes_accepted"] = stats["accepted_text_bytes_total"] / stats["accepted_articles"]
    save_json(STATS_PATH, stats)

def main():
    safe_mkdir(OUT_DIR)
    safe_mkdir(STATE_DIR)
    safe_mkdir(RAW_DIR)

    seen_urls = set(load_json(SEEN_URLS_PATH, []))
    listing_pos = load_json(LISTING_POS_PATH, 0)
    article_queue = load_json(ARTICLE_QUEUE_PATH, [])

    stats = load_json(STATS_PATH, {
        "downloaded_pages": 0,
        "downloaded_articles": 0,
        "accepted_articles": 0,
        "skipped_short": 0,
        "raw_bytes_total": 0,
        "accepted_raw_bytes_total": 0,
        "accepted_words_total": 0,
        "accepted_text_bytes_total": 0,
        "min_words": MIN_WORDS,
        "target_docs": TARGET_DOCS,
        "list_pages_from": LIST_PAGES_FROM,
        "list_pages_to": LIST_PAGES_TO,
        "source": "championat.com",
        "format": "html",
        "mode": "football_articles_paginated_longreads_interleaved",
        "listing_pos": listing_pos
    })

    accepted = stats.get("accepted_articles", 0)

    with open(META_PATH, "a", encoding="utf-8") as meta_f:
        while accepted < TARGET_DOCS:
            
            if listing_pos < len(LISTING_URLS):
                lurl = LISTING_URLS[listing_pos]
                print(f"[LIST {listing_pos+1}/{len(LISTING_URLS)}] {lurl}")

                try:
                    html = http_get(lurl)
                except Exception as e:
                    print(f"  ! listing error: {e}")
                    listing_pos += 1
                    stats["listing_pos"] = listing_pos
                    save_state(seen_urls, listing_pos, article_queue, stats)
                    time.sleep(REQUEST_SLEEP)
                    continue

                stats["downloaded_pages"] += 1
                stats["raw_bytes_total"] += len(html.encode("utf-8"))

                links = extract_links_from_listing(html, lurl)
                added = 0
                for link in links:
                    if link in seen_urls:
                        continue
                    seen_urls.add(link)
                    article_queue.append(link)
                    added += 1

                print(f"  found_links={len(links)} added={added} article_queue={len(article_queue)}")

                listing_pos += 1
                stats["listing_pos"] = listing_pos

                save_state(seen_urls, listing_pos, article_queue, stats)
                time.sleep(REQUEST_SLEEP)

            
            processed_now = 0
            while processed_now < ARTICLES_PER_LISTING and accepted < TARGET_DOCS and article_queue:
                url = article_queue.pop(0)

                if not is_article_longread_url(url):
                    continue

                try:
                    html = http_get(url)
                except Exception as e:
                    print(f"  ! article error: {e}")
                    time.sleep(REQUEST_SLEEP)
                    continue

                stats["downloaded_articles"] += 1
                raw_bytes = len(html.encode("utf-8"))
                stats["raw_bytes_total"] += raw_bytes

                meta = extract_article_meta(html, url)
                wc = meta["approx_words"]
                text_bytes = len((meta.get("text") or "").encode("utf-8"))
                
                if wc < MIN_WORDS:
                    stats["skipped_short"] += 1
                    processed_now += 1
                    
                    if stats["downloaded_articles"] % 10 == 0:
                        print(f"  [SKIP] words={wc} title={meta['title'][:70]!r}")
                    time.sleep(REQUEST_SLEEP)
                    continue

                doc_id = url_to_id(url)
                raw_path = os.path.join(RAW_DIR, f"{doc_id}.html")
                with open(raw_path, "w", encoding="utf-8") as f:
                    f.write(html)

                accepted += 1
                stats["accepted_articles"] = accepted
                stats["accepted_raw_bytes_total"] += raw_bytes
                stats["accepted_words_total"] += wc
                stats["accepted_text_bytes_total"] += text_bytes

                meta_out = {
                    "source": "championat.com",
                    "url": url,
                    "doc_id": doc_id,
                    "raw_path": raw_path.replace("\\", "/"),
                    "title": meta["title"],
                    "date": meta["date"],
                    "approx_words": wc,
                }
                meta_f.write(json.dumps(meta_out, ensure_ascii=False) + "\n")
                meta_f.flush()

                processed_now += 1
                print(f"  [ACCEPT {accepted}] words={wc} title={meta['title'][:70]!r}")

                
                if accepted % 10 == 0:
                    save_state(seen_urls, listing_pos, article_queue, stats)

                time.sleep(REQUEST_SLEEP)

            
            if listing_pos >= len(LISTING_URLS) and not article_queue:
                break

            
            save_state(seen_urls, listing_pos, article_queue, stats)

    save_state(seen_urls, listing_pos, article_queue, stats)

    print("\nDONE")
    print(json.dumps(load_json(STATS_PATH, stats), ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
