import sys
import re
import time
import signal
import queue
import hashlib
import threading
import logging
from logging.handlers import RotatingFileHandler
from dataclasses import dataclass
from typing import Optional, Dict, Any, Tuple, List
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode, quote


import yaml
import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient, ReturnDocument


RE_TOKEN_WORD = re.compile(r"[A-Za-zА-Яа-яЁё0-9]+(?:-[A-Za-zА-Яа-яЁё0-9]+)?")
TRACKING_KEYS_PREFIX = ("utm_",)
TRACKING_KEYS_EXACT = {"gclid", "yclid", "fbclid", "mc_cid", "mc_eid"}

STOP_EVENT = threading.Event()


LOG = logging.getLogger("robot")
SRC_LOG: Dict[str, logging.Logger] = {}



def unix_ts() -> int:
    return int(time.time())

def normalize_ws(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()

def word_count(s: str) -> int:
    return len(RE_TOKEN_WORD.findall(s or ""))

def sha256_hex(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8", errors="ignore")).hexdigest()

def sleep_with_stop(seconds: float) -> None:
    end = time.time() + seconds
    while time.time() < end:
        if STOP_EVENT.is_set():
            return
        time.sleep(min(0.05, end - time.time()))

def normalize_url(url: str) -> str:
    p = urlparse(url)
    scheme = p.scheme.lower() if p.scheme else "https"
    netloc = (p.netloc or "").lower()
    path = p.path or "/"

    fragment = ""

    q = []
    for k, v in parse_qsl(p.query, keep_blank_values=True):
        kl = k.lower()
        if kl.startswith(TRACKING_KEYS_PREFIX) or kl in TRACKING_KEYS_EXACT:
            continue
        q.append((k, v))
    q.sort(key=lambda kv: (kv[0], kv[1]))
    query = urlencode(q, doseq=True)

    path = re.sub(r"/{2,}", "/", path)

    if path != "/" and path.endswith("/") and not path.endswith(".html/"):
        path = path[:-1]

    return urlunparse((scheme, netloc, path, p.params, query, fragment))

def safe_source(source: str) -> str:
    return re.sub(r"[^a-z0-9_]+", "_", (source or "").lower()).strip("_") or "source"


class RateLimiter:
    def __init__(self, delay_seconds: float):
        self.delay = max(0.0, float(delay_seconds))
        self._lock = threading.Lock()
        self._next_time = 0.0

    def wait(self):
        if self.delay <= 0:
            return
        with self._lock:
            now = time.time()
            if now < self._next_time:
                sleep_with_stop(self._next_time - now)
            self._next_time = max(self._next_time, time.time()) + self.delay


@dataclass
class MongoCfg:
    uri: str
    database: str
    documents_collection: str = "documents"
    tasks_collection: str = "tasks"

class MongoStore:
    def __init__(self, cfg: MongoCfg):
        self.client = MongoClient(cfg.uri)
        self.db = self.client[cfg.database]
        self.docs = self.db[cfg.documents_collection]
        self.tasks = self.db[cfg.tasks_collection]
        self._ensure_indexes()

    def _ensure_indexes(self):
        self.docs.create_index([("source", 1), ("url_norm", 1)], unique=True)
        self.docs.create_index([("source", 1), ("fetched_at", -1)])
        self.docs.create_index([("content_hash", 1)])

        self.tasks.create_index([("source", 1), ("url_norm", 1)], unique=True)
        self.tasks.create_index([("state", 1), ("next_fetch_at", 1), ("locked_until", 1)])
        self.tasks.create_index([("source", 1), ("state", 1), ("next_fetch_at", 1), ("locked_until", 1)])

    def upsert_task(self, source: str, url_norm: str, next_fetch_at: int, priority: int = 0, meta: Optional[Dict[str, Any]] = None) -> None:
        meta = meta or {}
        now = unix_ts()
        self.tasks.update_one(
            {"source": source, "url_norm": url_norm},
            {
                "$setOnInsert": {"created_at": now, "retries": 0, "state": "queued", "locked_until": 0, "locked_by": ""},
                "$set": {"source": source, "url_norm": url_norm, "priority": int(priority), "meta": meta},
                "$min": {"next_fetch_at": int(next_fetch_at)},
            },
            upsert=True,
        )

    def claim_task(self, source: str, worker_id: str, lock_ttl: int) -> Optional[Dict[str, Any]]:
        now = unix_ts()
        lock_until = now + int(lock_ttl)
        return self.tasks.find_one_and_update(
            filter={
                "source": source,
                "state": {"$in": ["queued", "error"]},
                "next_fetch_at": {"$lte": now},
                "locked_until": {"$lte": now},
            },
            update={"$set": {"state": "fetching", "locked_until": lock_until, "locked_by": worker_id, "started_at": now}},
            sort=[("priority", -1), ("next_fetch_at", 1), ("created_at", 1)],
            return_document=ReturnDocument.AFTER,
        )

    def mark_task_done(self, source: str, url_norm: str, next_fetch_at: int, note: str = "") -> None:
        now = unix_ts()
        self.tasks.update_one(
            {"source": source, "url_norm": url_norm},
            {"$set": {"state": "done", "locked_until": 0, "locked_by": "", "finished_at": now, "next_fetch_at": int(next_fetch_at), "note": note}},
        )

    def mark_task_skipped(self, source: str, url_norm: str, next_fetch_at: int, reason: str) -> None:
        now = unix_ts()
        self.tasks.update_one(
            {"source": source, "url_norm": url_norm},
            {"$set": {"state": "done", "locked_until": 0, "locked_by": "", "finished_at": now, "next_fetch_at": int(next_fetch_at), "note": f"skipped: {reason}"}},
        )

    def mark_task_error(self, source: str, url_norm: str, next_fetch_at: int, err: str, inc_retry: bool = True) -> None:
        now = unix_ts()
        upd = {
            "$set": {
                "state": "error",
                "locked_until": 0,
                "locked_by": "",
                "finished_at": now,
                "next_fetch_at": int(next_fetch_at),
                "last_error": err[:5000],
            }
        }
        if inc_retry:
            upd["$inc"] = {"retries": 1}
        self.tasks.update_one({"source": source, "url_norm": url_norm}, upd)


    def get_document_hash(self, source: str, url_norm: str) -> Optional[str]:
        d = self.docs.find_one({"source": source, "url_norm": url_norm}, {"content_hash": 1})
        return d.get("content_hash") if d else None

    def upsert_document(
    self,
    source: str,
    url_norm: str,
    raw_html: str,
    parsed_text: str,
    fetched_at: int,
    content_hash: str,
    http_etag: Optional[str] = None,
    http_last_modified: Optional[str] = None,
    status_code: Optional[int] = None,
    changed: bool = True,
    word_count: Optional[int] = None,
) -> None:
        base_set = {
            "source": source,
            "url_norm": url_norm,
            "fetched_at": int(fetched_at),
            "http_etag": http_etag,
            "http_last_modified": http_last_modified,
            "status_code": status_code,
        }
        if word_count is not None:
            base_set["word_count"] = int(word_count)
        if changed:
            base_set.update({
                "raw_html": raw_html,
                "parsed_text": parsed_text,
                "content_hash": content_hash,
                "updated_at": int(fetched_at),
            })
            upd = {"$set": base_set, "$setOnInsert": {"created_at": int(fetched_at)}}
        else:
            upd = {"$set": base_set, "$setOnInsert": {"created_at": int(fetched_at)}}

        self.docs.update_one({"source": source, "url_norm": url_norm}, upd, upsert=True)

    
    def stats_tasks(self) -> List[Dict[str, Any]]:
        return list(self.tasks.aggregate([
            {"$group": {"_id": {"source": "$source", "state": "$state"}, "n": {"$sum": 1}}},
            {"$sort": {"n": -1}}
        ]))

    def stats_docs(self) -> List[Dict[str, Any]]:
        return list(self.docs.aggregate([
            {"$group": {"_id": "$source", "n": {"$sum": 1}}},
            {"$sort": {"n": -1}}
        ]))



@dataclass
class FetchCfg:
    user_agent: str
    timeout_seconds: float = 25.0
    max_retries: int = 5

class Fetcher:
    def __init__(self, cfg: FetchCfg):
        self.cfg = cfg
        self.sess = requests.Session()
        self.sess.headers.update({"User-Agent": cfg.user_agent})

    def get(self, url: str, headers: Optional[Dict[str, str]] = None) -> requests.Response:
        last_exc = None
        for attempt in range(self.cfg.max_retries):
            if STOP_EVENT.is_set():
                raise RuntimeError("stopped")
            try:
                return self.sess.get(url, headers=headers or {}, timeout=self.cfg.timeout_seconds)
            except Exception as e:
                last_exc = e
                sleep_with_stop(min(5.0, 0.5 * (2 ** attempt)))
        raise last_exc


def parse_wiki_html(raw_html: str) -> str:
    soup = BeautifulSoup(raw_html, "lxml")
    root = soup.select_one("div.mw-parser-output") or soup.select_one("div#mw-content-text") or soup
    for sel in [
        "table", "div.navbox", "div.infobox", "div.reflist", "div.mw-editsection", "sup.reference",
        "span.mw-editsection", "div#toc", "div.thumb", "ol.references", "ul.gallery"
    ]:
        for x in root.select(sel):
            x.decompose()

    parts = []
    for tag in root.find_all(["p", "h2", "h3", "li"]):
        txt = normalize_ws(tag.get_text(" ", strip=True))
        if not txt or len(txt) < 40:
            continue
        parts.append(txt)

    out, seen = [], set()
    for p in parts:
        key = p.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(p)
    return "\n".join(out).strip()


def parse_championat_html(raw_html: str) -> str:
    soup = BeautifulSoup(raw_html, "lxml")
    article = soup.select_one("article") or soup.select_one('[itemprop="articleBody"]') or soup.select_one("main") or soup
    for sel in ["script", "style", "noscript", "header", "footer", "form", "aside"]:
        for x in article.select(sel):
            x.decompose()

    parts = []
    for tag in article.find_all(["p", "h1", "h2", "h3", "li"]):
        txt = normalize_ws(tag.get_text(" ", strip=True))
        if not txt or len(txt) < 40:
            continue
        parts.append(txt)

    out, seen = [], set()
    for p in parts:
        key = p.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(p)
    return "\n".join(out).strip()

CHAMP_HOSTS = ("www.championat.com", "championat.com")

def champ_full_url(href: str) -> Optional[str]:
    if not href:
        return None
    href = href.strip()
    if href.startswith("//"):
        href = "https:" + href
    if href.startswith("/"):
        href = "https://www.championat.com" + href
    if not (href.startswith("http://") or href.startswith("https://")):
        return None
    return href

def is_championat_listing_page(raw_html: str) -> bool:
    try:
        soup = BeautifulSoup(raw_html, "lxml")
        html_tag = soup.find("html")
        if html_tag:
            cls = " ".join(html_tag.get("class") or [])
            if "articles-list-page" in cls:
                return True
        
        if soup.select_one(".articles-list") or soup.select_one("[data-qa='articles-list']"):
            return True
    except Exception:
        pass
    return False

def extract_championat_article_links(listing_html: str) -> List[str]:
    out = []
    try:
        soup = BeautifulSoup(listing_html, "lxml")
        for a in soup.find_all("a", href=True):
            full = champ_full_url(a["href"])
            if not full:
                continue
            p = urlparse(full)
            if p.netloc not in CHAMP_HOSTS:
                continue

            path = p.path or ""
            if not path.startswith("/articles/football/"):
                continue
            if not path.endswith(".html"):
                continue

            
            seg = [s for s in path.split("/") if s]
            
            if len(seg) < 4:
                
                continue

            last = seg[-1]
            prev = seg[-2]

            m = re.fullmatch(r"(\d+)\.html", last)
            if not m:
                continue
            num = int(m.group(1))

            if num <= 200:

                continue


            if prev.startswith("_"):
                continue

            out.append(normalize_url(full))
    except Exception:
        return []

    uniq = []
    seen = set()
    for u in out:
        if u not in seen:
            seen.add(u)
            uniq.append(u)
    return uniq


def get_canonical_url(raw_html: str, base_url: str) -> Optional[str]:
    try:
        soup = BeautifulSoup(raw_html, "lxml")
        link = soup.find("link", rel=lambda v: v and "canonical" in v)
        if link and link.get("href"):
            href = link["href"].strip()
            if href.startswith("//"):
                href = "https:" + href
            if href.startswith("/"):
                p = urlparse(base_url)
                href = f"{p.scheme}://{p.netloc}{href}"
            return href
    except Exception:
        return None
    return None


def parse_sportsru_html(raw_html: str) -> str:
    soup = BeautifulSoup(raw_html, "lxml")
    root = soup.select_one("article") or soup.select_one("main") or soup
    for sel in ["script", "style", "noscript", "header", "footer", "form", "aside"]:
        for x in root.select(sel):
            x.decompose()

    parts = []
    for tag in root.find_all(["p", "h1", "h2", "h3", "li"]):
        txt = normalize_ws(tag.get_text(" ", strip=True))
        if not txt or len(txt) < 30:
            continue
        low = txt.lower()
        if "подпис" in low and "телег" in low:
            continue
        parts.append(txt)

    out, seen = [], set()
    for p in parts:
        key = p.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(p)
    return "\n".join(out).strip()


WIKI_API = "https://ru.wikipedia.org/w/api.php"

def wiki_make_page_url(title: str) -> str:
    t = title.replace(" ", "_")
    return "https://ru.wikipedia.org/wiki/" + quote(t, safe=":/()%_,-")

def wiki_fetch_category_members(fetcher: Fetcher, category_title: str, cmtype: str, limit: int = 5000) -> List[Dict[str, Any]]:
    members = []
    cont = None
    while not STOP_EVENT.is_set():
        params = {
            "action": "query",
            "format": "json",
            "list": "categorymembers",
            "cmtitle": category_title,
            "cmlimit": 500,
            "cmtype": cmtype,
        }
        if cont:
            params["cmcontinue"] = cont

        rr = fetcher.sess.get(WIKI_API, params=params, timeout=fetcher.cfg.timeout_seconds)
        rr.raise_for_status()
        data = rr.json()
        cms = data.get("query", {}).get("categorymembers", [])
        members.extend(cms)
        cont = data.get("continue", {}).get("cmcontinue")
        if not cont or len(members) >= limit:
            break
    return members

def wiki_fetch_page_html(fetcher: Fetcher, title: str) -> str:
    params = {
        "action": "parse",
        "format": "json",
        "page": title,
        "prop": "text",
        "disablelimitreport": 1,
        "disableeditsection": 1,
    }
    rr = fetcher.sess.get(WIKI_API, params=params, timeout=fetcher.cfg.timeout_seconds)
    rr.raise_for_status()
    data = rr.json()
    html = data.get("parse", {}).get("text", {}).get("*", "")
    return f"<!doctype html><html><head><meta charset='utf-8'></head><body>{html}</body></html>"


def discover_wiki(store, fetcher, rate, seed_categories, max_depth, max_pages, enqueue_priority):
    LOG.info("[wiki][discover] start")
    seen_cat = set()
    q = queue.Queue()

    for c in seed_categories:
        if not c.startswith("Категория:"):
            c = "Категория:" + c
        q.put((0, c))

    pages_enqueued = 0

    while not STOP_EVENT.is_set():
        try:
            depth, cat = q.get(timeout=0.2)
        except queue.Empty:
            break

        if cat in seen_cat:
            continue
        seen_cat.add(cat)
        if depth > max_depth:
            continue

        rate.wait()
        try:
            subcats = wiki_fetch_category_members(fetcher, cat, cmtype="subcat", limit=5000)
        except Exception as e:
            LOG.warning("[wiki][discover] subcat error cat=%s err=%s", cat, e)
            continue

        for sc in subcats:
            title = sc.get("title")
            if title and title not in seen_cat:
                q.put((depth + 1, title))

        rate.wait()
        try:
            pages = wiki_fetch_category_members(fetcher, cat, cmtype="page", limit=5000)
        except Exception as e:
            LOG.warning("[wiki][discover] pages error cat=%s err=%s", cat, e)
            continue

        for p in pages:
            title = p.get("title")
            if not title:
                continue
            url_norm = normalize_url(wiki_make_page_url(title))
            store.upsert_task("wiki", url_norm, next_fetch_at=unix_ts(), priority=enqueue_priority, meta={"title": title})
            pages_enqueued += 1
            if pages_enqueued % 500 == 0:
                LOG.info("[wiki][discover] enqueued=%d", pages_enqueued)
            if pages_enqueued >= max_pages:
                LOG.info("[wiki][discover] reached max_pages=%d", max_pages)
                return

    LOG.info("[wiki][discover] done enqueued=%d cats=%d", pages_enqueued, len(seen_cat))


def discover_championat(store, fetcher, rate, page_from, page_to, enqueue_priority):
    LOG.info("[championat][discover] start")
    pages_ok = 0
    links_enqueued = 0
    consecutive_404 = 0

    RE_ARTICLE = re.compile(r"^https?://(www\.)?championat\.com/football/article-\d+.*\.html$", re.I)

    for i in range(int(page_from), int(page_to) + 1):
        if STOP_EVENT.is_set():
            break

        listing_url = f"https://www.championat.com/articles/football/{i}.html"

        rate.wait()
        try:
            r = fetcher.get(listing_url)
        except Exception as e:
            LOG.warning("[championat][discover] listing error url=%s err=%s", listing_url, e)
            continue

        if r.status_code == 404:
            consecutive_404 += 1
            LOG.warning("[championat][discover] listing 404 url=%s (consecutive=%d)", listing_url, consecutive_404)
            if consecutive_404 >= 3:
                LOG.info("[championat][discover] stop: too many 404 in a row")
                break
            continue

        consecutive_404 = 0

        if r.status_code != 200:
            LOG.warning("[championat][discover] listing bad url=%s status=%s", listing_url, r.status_code)
            continue

        html = r.text or ""
        soup = BeautifulSoup(html, "lxml")

        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if not href:
                continue


            if href.startswith("/"):
                href = "https://www.championat.com" + href

            if not RE_ARTICLE.match(href):
                continue

            url_norm = normalize_url(href)
            store.upsert_task(
                "championat",
                url_norm,
                next_fetch_at=unix_ts(),
                priority=enqueue_priority,
                meta={"listing": listing_url},
            )
            links_enqueued += 1

        pages_ok += 1
        if pages_ok % 5 == 0:
            LOG.info("[championat][discover] listing_pages_ok=%d enqueued_articles=%d", pages_ok, links_enqueued)

    LOG.info("[championat][discover] done listing_pages_ok=%d enqueued_articles=%d", pages_ok, links_enqueued)


def discover_sportsru(store, fetcher, rate, page_from, page_to, enqueue_priority):
    LOG.info("[sportsru][discover] start")
    pages = 0
    links_enqueued = 0

    for i in range(int(page_from), int(page_to) + 1):
        if STOP_EVENT.is_set():
            break

        listing_url = "https://m.sports.ru/football/blogs/" if i == 1 else f"https://m.sports.ru/football/blogs/page{i}/"

        rate.wait()
        try:
            r = fetcher.get(listing_url)
            if r.status_code != 200:
                LOG.warning("[sportsru][discover] listing bad url=%s status=%s", listing_url, r.status_code)
                continue
            html = r.text
        except Exception as e:
            LOG.warning("[sportsru][discover] listing error url=%s err=%s", listing_url, e)
            continue

        soup = BeautifulSoup(html, "lxml")
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if not href:
                continue

            if href.startswith("/"):
                full = "https://m.sports.ru" + href
            else:
                full = href

            if ("sports.ru" not in full) and ("m.sports.ru" not in full):
                continue

            if "/football/blogs/" not in full:
                continue

            url_norm = normalize_url(full)
            store.upsert_task("sportsru", url_norm, next_fetch_at=unix_ts(), priority=enqueue_priority, meta={"listing": listing_url})
            links_enqueued += 1

        pages += 1
        if pages % 10 == 0:
            LOG.info("[sportsru][discover] listing_pages=%d enqueued_links=%d", pages, links_enqueued)

    LOG.info("[sportsru][discover] done listing_pages=%d enqueued_links=%d", pages, links_enqueued)



@dataclass
class LogicCfg:
    delay_seconds: float
    lock_ttl_seconds: int
    recrawl_seconds: int
    max_retries: int
    user_agent: str
    worker_threads_per_source: int = 1
    retry_backoff_base_seconds: int = 30
    retry_backoff_max_seconds: int = 3600
    timeout_seconds: float = 25.0
    progress_log_seconds: int = 60

@dataclass
class SourceCfg:
    enabled: bool = True
    min_words: int = 900

@dataclass
class WikiCfg(SourceCfg):
    seed_categories: List[str] = None
    max_depth: int = 4
    discovery_max_pages: int = 20000

@dataclass
class ChampionatCfg(SourceCfg):
    listing_pages_from: int = 1
    listing_pages_to: int = 60

@dataclass
class SportsRuCfg(SourceCfg):
    listing_pages_from: int = 1
    listing_pages_to: int = 80

def compute_retry_delay_seconds(retries: int, base: int, cap: int) -> int:
    return int(min(cap, base * (2 ** max(0, retries))))

def fetch_and_parse(source: str, url_norm: str, meta: Dict[str, Any], fetcher: Fetcher) -> Tuple[str, str, Optional[str], Optional[str], int, str]:
    headers = {}
    if meta:
        if meta.get("http_etag"):
            headers["If-None-Match"] = meta["http_etag"]
        if meta.get("http_last_modified"):
            headers["If-Modified-Since"] = meta["http_last_modified"]

    r = fetcher.get(url_norm, headers=headers)
    status = int(r.status_code)

    if status == 304:
        return "", "", r.headers.get("ETag"), r.headers.get("Last-Modified"), status, url_norm

    raw_html = r.text or ""
    etag = r.headers.get("ETag")
    last_mod = r.headers.get("Last-Modified")

    eff_url_norm = url_norm
    if source == "sportsru":
        canon = get_canonical_url(raw_html, base_url=url_norm)
        if canon:
            eff_url_norm = normalize_url(canon)

    if source == "wiki":
        title = (meta or {}).get("title")
        if title:
            raw_html = wiki_fetch_page_html(fetcher, title)
        parsed = parse_wiki_html(raw_html)
    elif source == "championat":
        parsed = parse_championat_html(raw_html)
    elif source == "sportsru":
        parsed = parse_sportsru_html(raw_html)
    else:
        parsed = ""

    return raw_html, parsed, etag, last_mod, status, eff_url_norm


def worker_loop(store: MongoStore, source: str, logic: LogicCfg, source_cfg: SourceCfg, fetcher: Fetcher, rate: RateLimiter, worker_id: str):
    slog = SRC_LOG.get(source, LOG)
    slog.info("[worker] start id=%s", worker_id)

    while not STOP_EVENT.is_set():
        task = store.claim_task(source=source, worker_id=worker_id, lock_ttl=logic.lock_ttl_seconds)
        if not task:
            sleep_with_stop(0.2)
            continue

        url_norm = task["url_norm"]
        retries = int(task.get("retries", 0))
        meta = task.get("meta") or {}
        rate.wait()

        try:
            fetched_at = unix_ts()
            raw_html, parsed_text, etag, last_mod, status, effective_url_norm = fetch_and_parse(source, url_norm, meta, fetcher)

            if effective_url_norm != url_norm:
                store.upsert_task(source, effective_url_norm, next_fetch_at=unix_ts(), priority=int(task.get("priority", 0)), meta=meta)
                store.mark_task_done(source, url_norm, next_fetch_at=fetched_at + logic.recrawl_seconds, note=f"alias->canonical {effective_url_norm}")
                slog.info("[alias] %s -> %s", url_norm, effective_url_norm)
                continue

            if status == 304:
                store.upsert_document(source, url_norm, "", "", fetched_at, "", etag, last_mod, status, changed=False, word_count=None)
                store.mark_task_done(source, url_norm, next_fetch_at=fetched_at + logic.recrawl_seconds, note="304 not modified")
                slog.info("[ok] %s status=304 not_modified", url_norm)
                continue

            wc = word_count(parsed_text)
            if wc < int(getattr(source_cfg, "min_words", 0) or 0):
                store.mark_task_skipped(source, url_norm, next_fetch_at=fetched_at + logic.recrawl_seconds, reason=f"too_short words={wc}")
                slog.info("[skip] %s status=%s words=%d (min=%d)", url_norm, status, wc, int(getattr(source_cfg, "min_words", 0) or 0))
                continue

            new_hash = sha256_hex(parsed_text)
            old_hash = store.get_document_hash(source, url_norm)
            changed = (old_hash != new_hash)

            store.upsert_document(source, url_norm, raw_html, parsed_text, fetched_at, new_hash, etag, last_mod, status, changed=changed, word_count=wc)
            note = "updated" if changed else "same_hash"
            store.mark_task_done(source, url_norm, next_fetch_at=fetched_at + logic.recrawl_seconds, note=note)

            slog.info("[ok] %s status=%s words=%d %s", url_norm, status, wc, note)

        except Exception as e:
            delay = compute_retry_delay_seconds(retries, logic.retry_backoff_base_seconds, logic.retry_backoff_max_seconds)
            next_time = unix_ts() + delay
            store.mark_task_error(source, url_norm, next_fetch_at=next_time, err=str(e), inc_retry=True)
            slog.warning("[err] %s retry_in=%ds err=%s", url_norm, delay, e)

            if retries + 1 >= logic.max_retries:
                store.mark_task_error(source, url_norm, next_fetch_at=unix_ts() + logic.retry_backoff_max_seconds, err=f"max_retries reached: {e}", inc_retry=False)
                slog.warning("[err] %s max_retries reached -> delayed", url_norm)

    slog.info("[worker] stop id=%s", worker_id)



def progress_loop(store: MongoStore, interval_sec: int):
    while not STOP_EVENT.is_set():
        sleep_with_stop(max(5, int(interval_sec)))
        if STOP_EVENT.is_set():
            break
        try:
            tstats = store.stats_tasks()
            dstats = store.stats_docs()


            parts_t = []
            for row in tstats[:12]:
                src = row["_id"]["source"]
                st = row["_id"]["state"]
                n = row["n"]
                parts_t.append(f"{src}:{st}={n}")
            parts_d = [f"{row['_id']}={row['n']}" for row in dstats[:10]]

            LOG.info("[stats] tasks: %s", " | ".join(parts_t) if parts_t else "no_data")
            LOG.info("[stats] docs:  %s", " | ".join(parts_d) if parts_d else "no_data")
        except Exception as e:
            LOG.warning("[stats] error: %s", e)


def setup_logging(log_dir: str, level: str = "INFO", max_mb: int = 20, backups: int = 5):
    log_dir = log_dir or "logs"
    level_name = (level or "INFO").upper()
    lvl = getattr(logging, level_name, logging.INFO)


    logging.getLogger().setLevel(lvl)

    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")


    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(lvl)
    ch.setFormatter(fmt)
    logging.getLogger().addHandler(ch)


    import os
    os.makedirs(log_dir, exist_ok=True)
    fh = RotatingFileHandler(
        filename=os.path.join(log_dir, "robot.log"),
        maxBytes=max_mb * 1024 * 1024,
        backupCount=backups,
        encoding="utf-8",
    )
    fh.setLevel(lvl)
    fh.setFormatter(fmt)
    logging.getLogger().addHandler(fh)

    
    global LOG
    LOG = logging.getLogger("robot")

    def make_source_logger(source: str):
        sname = safe_source(source)
        lg = logging.getLogger(f"src.{sname}")
        lg.setLevel(lvl)
        sh = RotatingFileHandler(
            filename=os.path.join(log_dir, f"{sname}.log"),
            maxBytes=max_mb * 1024 * 1024,
            backupCount=backups,
            encoding="utf-8",
        )
        sh.setLevel(lvl)
        sh.setFormatter(fmt)
        lg.addHandler(sh)
        return lg

    for src in ["wiki", "championat", "sportsru"]:
        SRC_LOG[src] = make_source_logger(src)

    LOG.info("logging initialized dir=%s level=%s", log_dir, level_name)


def load_config(path: str):
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}

    db = cfg.get("db") or {}
    logic = cfg.get("logic") or {}
    sources = cfg.get("sources") or {}
    logs = cfg.get("logs") or {}

    mongo_cfg = MongoCfg(
        uri=str(db.get("uri", "mongodb://localhost:27017")),
        database=str(db.get("database", "ir_lab2")),
        documents_collection=str(db.get("documents_collection", "documents")),
        tasks_collection=str(db.get("tasks_collection", "tasks")),
    )

    logic_cfg = LogicCfg(
        delay_seconds=float(logic.get("delay_seconds", 0.35)),
        lock_ttl_seconds=int(logic.get("lock_ttl_seconds", 120)),
        recrawl_seconds=int(logic.get("recrawl_seconds", 24 * 3600)),
        max_retries=int(logic.get("max_retries", 5)),
        user_agent=str(logic.get("user_agent", "IR-LR2-Bot/1.0 (edu)")),
        worker_threads_per_source=int(logic.get("worker_threads_per_source", 1)),
        retry_backoff_base_seconds=int(logic.get("retry_backoff_base_seconds", 30)),
        retry_backoff_max_seconds=int(logic.get("retry_backoff_max_seconds", 3600)),
        timeout_seconds=float(logic.get("timeout_seconds", 25.0)),
        progress_log_seconds=int(logic.get("progress_log_seconds", 60)),
    )

    w = sources.get("wiki") or {}
    c = sources.get("championat") or {}
    s = sources.get("sportsru") or {}

    wiki_cfg = WikiCfg(
        enabled=bool(w.get("enabled", True)),
        min_words=int(w.get("min_words", 1500)),
        seed_categories=list(w.get("seed_categories", ["Категория:Футбол"])),
        max_depth=int(w.get("max_depth", 4)),
        discovery_max_pages=int(w.get("discovery_max_pages", 20000)),
    )

    champ_cfg = ChampionatCfg(
        enabled=bool(c.get("enabled", True)),
        min_words=int(c.get("min_words", 900)),
        listing_pages_from=int(c.get("listing_pages_from", 1)),
        listing_pages_to=int(c.get("listing_pages_to", 60)),
    )

    sports_cfg = SportsRuCfg(
        enabled=bool(s.get("enabled", True)),
        min_words=int(s.get("min_words", 900)),
        listing_pages_from=int(s.get("listing_pages_from", 1)),
        listing_pages_to=int(s.get("listing_pages_to", 80)),
    )

    log_cfg = {
        "dir": str(logs.get("dir", "logs")),
        "level": str(logs.get("level", "INFO")),
        "max_mb": int(logs.get("max_mb", 20)),
        "backups": int(logs.get("backups", 5)),
    }

    enabled_sources = []
    if wiki_cfg.enabled:
        enabled_sources.append("wiki")
    if champ_cfg.enabled:
        enabled_sources.append("championat")
    if sports_cfg.enabled:
        enabled_sources.append("sportsru")

    return cfg, mongo_cfg, logic_cfg, wiki_cfg, champ_cfg, sports_cfg, enabled_sources, log_cfg


def install_signal_handlers():
    def _handler(sig, frame):
        if not STOP_EVENT.is_set():
            LOG.warning("stop requested (signal=%s)", sig)
            STOP_EVENT.set()
    signal.signal(signal.SIGINT, _handler)
    try:
        signal.signal(signal.SIGTERM, _handler)
    except Exception:
        pass

def main():
    if len(sys.argv) != 2:
        print("Usage: python lr2_robot.py /path/to/config.yaml")
        sys.exit(2)

    cfg_path = sys.argv[1]
    raw_cfg, mongo_cfg, logic_cfg, wiki_cfg, champ_cfg, sports_cfg, enabled_sources, log_cfg = load_config(cfg_path)
    setup_logging(log_cfg["dir"], log_cfg["level"], log_cfg["max_mb"], log_cfg["backups"])
    install_signal_handlers()

    store = MongoStore(mongo_cfg)

    fetcher = Fetcher(FetchCfg(
        user_agent=logic_cfg.user_agent,
        timeout_seconds=float(logic_cfg.timeout_seconds),
        max_retries=int(max(1, logic_cfg.max_retries)),
    ))

    rates = {src: RateLimiter(logic_cfg.delay_seconds) for src in ["wiki", "championat", "sportsru"]}

    
    discover_threads = []
    if wiki_cfg.enabled:
        discover_threads.append(threading.Thread(
            target=discover_wiki,
            name="discover_wiki",
            args=(store, fetcher, rates["wiki"], wiki_cfg.seed_categories, wiki_cfg.max_depth, wiki_cfg.discovery_max_pages, 10),
            daemon=True,
        ))
    if champ_cfg.enabled:
        discover_threads.append(threading.Thread(
            target=discover_championat,
            name="discover_championat",
            args=(store, fetcher, rates["championat"], champ_cfg.listing_pages_from, champ_cfg.listing_pages_to, 5),
            daemon=True,
        ))
    if sports_cfg.enabled:
        discover_threads.append(threading.Thread(
            target=discover_sportsru,
            name="discover_sportsru",
            args=(store, fetcher, rates["sportsru"], sports_cfg.listing_pages_from, sports_cfg.listing_pages_to, 5),
            daemon=True,
        ))

    for t in discover_threads:
        t.start()


    worker_threads = []
    wid = 0

    def spawn_workers(src: str, scfg: SourceCfg):
        nonlocal wid
        for _ in range(int(logic_cfg.worker_threads_per_source)):
            wid += 1
            worker_id = f"{src}-w{wid}"
            thr = threading.Thread(
                target=worker_loop,
                name=f"worker_{worker_id}",
                args=(store, src, logic_cfg, scfg, fetcher, rates[src], worker_id),
                daemon=True,
            )
            worker_threads.append(thr)
            thr.start()

    if wiki_cfg.enabled:
        spawn_workers("wiki", wiki_cfg)
    if champ_cfg.enabled:
        spawn_workers("championat", champ_cfg)
    if sports_cfg.enabled:
        spawn_workers("sportsru", sports_cfg)

    
    tstat = threading.Thread(target=progress_loop, name="progress", args=(store, logic_cfg.progress_log_seconds), daemon=True)
    tstat.start()

    LOG.info("running sources=%s mongo_db=%s", ",".join(enabled_sources), mongo_cfg.database)
    LOG.info("Ctrl+C to stop")

    try:
        while not STOP_EVENT.is_set():
            time.sleep(0.5)
    finally:
        STOP_EVENT.set()
        sleep_with_stop(0.5)
        LOG.info("stopped")

if __name__ == "__main__":
    main()
