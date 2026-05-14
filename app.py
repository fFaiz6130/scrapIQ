"""
ScrapIQ - Smart Web Scraper & Data Extractor
Flask backend with SQLite persistence and scraping utilities.

This module centralizes URL validation, HTTP fetching with size limits,
structured HTML parsing, persistence, and CSV export.
"""

from __future__ import annotations

import json
import logging
import os
import re
import socket
import sqlite3
from collections import Counter
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from io import BytesIO
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup
from flask import (
    Flask,
    abort,
    flash,
    redirect,
    render_template,
    request,
    send_file,
    url_for,
)

# -----------------------------------------------------------------------------
# App configuration
# -----------------------------------------------------------------------------

app = Flask(__name__)
# Prefer a stable env secret in production; default is fine for local college demos.
app.config["SECRET_KEY"] = os.environ.get("SCRAPIQ_SECRET_KEY", "scrapiq-dev-change-me")
app.secret_key = app.config["SECRET_KEY"]
# Avoid oversized form bodies (URL field only; keeps accidental huge POSTs safe).
app.config["MAX_CONTENT_LENGTH"] = 32 * 1024

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("scrapiq")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATABASE = os.path.join(BASE_DIR, "database.db")
# SQLite table name (matches coursework / SQL expectations)
HISTORY_TABLE = "history"
HISTORY_LIST_LIMIT = 100

# Limits to keep UI, memory, and DB responsive
MAX_PARAGRAPHS_STORE = 80
MAX_PARAGRAPHS_DISPLAY = 40
MAX_LINKS_STORE = 300
MAX_LINKS_DISPLAY = 150
MAX_IMAGES_STORE = 200
MAX_IMAGES_DISPLAY = 80
REQUEST_TIMEOUT = 12
MAX_RESPONSE_BYTES = 2_500_000
MAX_URL_LENGTH = 2048
DUPLICATE_WINDOW = timedelta(minutes=3)

# Simple English stopwords for keyword extraction
STOPWORDS = {
    "the", "a", "an", "and", "or", "but", "in", "on", "at", "to", "for",
    "of", "as", "is", "was", "are", "were", "been", "be", "have", "has",
    "had", "do", "does", "did", "will", "would", "could", "should", "may",
    "might", "must", "shall", "can", "this", "that", "these", "those", "i",
    "you", "he", "she", "it", "we", "they", "what", "which", "who", "whom",
    "with", "from", "by", "about", "into", "through", "during", "before",
    "after", "above", "below", "between", "under", "again", "further", "then",
    "once", "here", "there", "when", "where", "why", "how", "all", "each",
    "both", "few", "more", "most", "other", "some", "such", "no", "nor", "not",
    "only", "own", "same", "so", "than", "too", "very", "just", "also", "now",
    "your", "our", "their", "its", "if", "because", "while", "any", "my",
}

# Custom scrape: user-selected fields (checkbox values must match these keys).
CUSTOM_FIELD_KEYS = frozenset(
    {
        "product_name",
        "images",
        "price",
        "brand",
        "reviews",
        "description",
        "similar_products",
        "links",
    }
)
SCRAPIQ_CATEGORIES = ("Fashion", "Shoes", "Electronics", "Furniture")

# Dangerous or unsupported URL schemes (case-insensitive).
_BLOCKED_SCHEMES = re.compile(
    r"^\s*(javascript|data|file|vbscript|about|blob|mailto|tel|ftp)\s*:",
    re.I,
)

# IPv4 literal pattern for host validation.
_IPV4 = re.compile(r"^(\d{1,3}\.){3}\d{1,3}$")


def get_db_connection() -> sqlite3.Connection:
    """
    Open a new SQLite connection to database.db (file is auto-created).
    Uses WAL + busy timeout to reduce 'database is locked' errors on Windows.
    """
    conn = sqlite3.connect(
        DATABASE,
        timeout=30.0,
        check_same_thread=False,
        isolation_level="",
    )
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA foreign_keys=ON")
    except sqlite3.Error:
        pass
    return conn


@contextmanager
def db_session():
    """
    Yield a connection with explicit commit/rollback and guaranteed close.
    Prefer this over raw connect() in routes so commits are never skipped.
    """
    conn = get_db_connection()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return {r["name"] for r in rows}


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (name,),
    ).fetchone()
    return row is not None


def _migrate_legacy_scrapes(conn: sqlite3.Connection) -> None:
    """Move rows from legacy table `scrapes` into `history`, then drop `scrapes`."""
    if not _table_exists(conn, "scrapes"):
        return
    try:
        cols = _table_columns(conn, "scrapes")
        if "data_json" not in cols or "url" not in cols:
            return
        legacy = conn.execute("SELECT * FROM scrapes").fetchall()
        fallback_ts = (
            datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        )
        for row in legacy:
            scraped = row["scraped_at"] if "scraped_at" in row.keys() else fallback_ts
            if "created_at" in row.keys() and row["created_at"]:
                created = row["created_at"]
            else:
                created = scraped
            meta = row["meta_description"] if "meta_description" in row.keys() else None
            title = row["title"] if "title" in row.keys() else None
            payload = row["data_json"]
            conn.execute(
                f"""
                INSERT INTO {HISTORY_TABLE} (url, title, created_at, scraped_at, meta_description, data_json)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (row["url"], title, created, scraped, meta, payload),
            )
        conn.execute("DROP TABLE scrapes")
        if legacy:
            logger.info("Migrated %s rows from legacy 'scrapes' into '%s'.", len(legacy), HISTORY_TABLE)
    except sqlite3.Error as exc:
        logger.warning("Legacy migration skipped: %s", exc)


def init_db() -> None:
    """
    Create the `history` table and indexes, migrate legacy `scrapes` if present.
    Required columns: id, url, title, created_at (+ scraped_at, meta, JSON for ScrapIQ features).
    """
    with db_session() as conn:
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {HISTORY_TABLE} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT NOT NULL,
                title TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                scraped_at TEXT NOT NULL,
                meta_description TEXT,
                data_json TEXT NOT NULL
            )
            """
        )
        cols = _table_columns(conn, HISTORY_TABLE)
        if "scraped_at" not in cols:
            conn.execute(
                f"ALTER TABLE {HISTORY_TABLE} ADD COLUMN scraped_at TEXT NOT NULL DEFAULT (datetime('now'))"
            )
        if "meta_description" not in cols:
            conn.execute(f"ALTER TABLE {HISTORY_TABLE} ADD COLUMN meta_description TEXT")
        if "data_json" not in cols:
            conn.execute(
                f"ALTER TABLE {HISTORY_TABLE} ADD COLUMN data_json TEXT NOT NULL DEFAULT '{{}}'"
            )
        conn.execute(
            f"CREATE INDEX IF NOT EXISTS idx_history_scraped_at ON {HISTORY_TABLE} (scraped_at DESC)"
        )
        conn.execute(f"CREATE INDEX IF NOT EXISTS idx_history_url ON {HISTORY_TABLE} (url)")
        _migrate_legacy_scrapes(conn)


def sanitize_url_input(raw: str) -> str:
    """Strip whitespace/control chars and cap length (defense in depth)."""
    s = (raw or "").strip()
    s = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]", "", s)
    if len(s) > MAX_URL_LENGTH:
        s = s[:MAX_URL_LENGTH]
    return s


def _hostname_valid(host: str | None) -> bool:
    """Permissive host check: supports IDN / punycode and common LAN hosts for coursework."""
    if not host:
        return False
    h = str(host).strip().lower().rstrip(".")
    if not h or len(h) > 253 or ".." in h or h.startswith("."):
        return False
    if h in {"localhost", "127.0.0.1", "::1"}:
        return True
    if _IPV4.match(h):
        parts = [int(x) for x in h.split(".")]
        return all(0 <= p <= 255 for p in parts)
    if "." not in h:
        return False
    return True


def normalize_url(url: str) -> str:
    """
    Validate and normalize user URL input.
    - Rejects empty input
    - Strips dangerous characters
    - Auto-prefixes https:// when missing
    - Allows only http/https destinations with a plausible hostname
    """
    url = sanitize_url_input(url)
    if not url:
        raise ValueError("Please enter a website URL.")
    if _BLOCKED_SCHEMES.search(url):
        raise ValueError("That URL scheme is not supported. Use http:// or https:// web pages only.")
    if not re.match(r"^https?://", url, re.I):
        url = "https://" + url
    parsed = urlparse(url)
    if parsed.scheme.lower() not in {"http", "https"}:
        raise ValueError("Only http and https URLs are supported.")
    if not parsed.netloc:
        raise ValueError("Invalid URL: missing website host (domain).")
    host = parsed.hostname
    if host is None or not _hostname_valid(host):
        raise ValueError("Invalid URL: host name does not look like a public website address.")
    return parsed.geturl()


def _http_error_message(status: int) -> str:
    if status == 404:
        return "Page not found (404). Check the URL path or try the site homepage."
    if status == 403:
        return "Access forbidden (403). This website may block automated scraping."
    if status == 401:
        return "Unauthorized (401). This page requires authentication."
    if status == 429:
        return "Too many requests (429). Try again in a few minutes."
    if 500 <= status <= 599:
        return f"Server error ({status}). The website had a problem — try again later."
    if 400 <= status <= 499:
        return f"Could not retrieve the page (HTTP {status})."
    return f"Unexpected HTTP status {status}."


def fetch_page(url: str) -> tuple[str, BeautifulSoup, bool]:
    """
    Fetch URL with browser-like headers, enforce download size cap, return soup.
    Returns (final_url, soup, truncated_flag).
    """
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 ScrapIQ/1.0 (+education)"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }
    resp: requests.Response | None = None
    truncated = False
    try:
        resp = requests.get(
            url,
            headers=headers,
            timeout=REQUEST_TIMEOUT,
            allow_redirects=True,
            stream=True,
        )
    except requests.exceptions.SSLError as exc:
        raise ValueError(
            "SSL certificate error. Try http:// if the site supports it, or a different URL."
        ) from exc
    except requests.exceptions.Timeout as exc:
        raise ValueError(
            "Request timed out. The site may be slow, down, or blocking automated requests."
        ) from exc
    except requests.exceptions.TooManyRedirects as exc:
        raise ValueError("Too many redirects — the URL may be misconfigured.") from exc
    except requests.exceptions.ConnectionError as exc:
        raise ValueError("Could not connect. Check the URL, firewall, or your network connection.") from exc
    except requests.exceptions.RequestException as exc:
        raise ValueError("The request could not be completed. Please try another URL.") from exc

    try:
        if resp is None:
            raise ValueError("No response from server.")

        if resp.status_code == 403:
            raise ValueError(_http_error_message(403))
        if resp.status_code == 404:
            raise ValueError(_http_error_message(404))
        if resp.status_code >= 400:
            raise ValueError(_http_error_message(resp.status_code))

        content = b""
        for chunk in resp.iter_content(chunk_size=65536):
            if not chunk:
                continue
            content += chunk
            if len(content) > MAX_RESPONSE_BYTES:
                truncated = True
                break

        charset = resp.encoding or getattr(resp, "apparent_encoding", None) or "utf-8"
        try:
            html = content.decode(charset, errors="replace")
        except LookupError:
            html = content.decode("utf-8", errors="replace")

        try:
            soup = BeautifulSoup(html, "lxml")
        except Exception as exc:
            raise ValueError("Could not parse the page HTML (unexpected structure).") from exc

        return resp.url, soup, truncated
    finally:
        if resp is not None:
            try:
                resp.close()
            except Exception:
                pass


def scrape_title(soup: BeautifulSoup) -> str:
    if soup.title and soup.title.string:
        return soup.title.string.strip()
    og = soup.find("meta", property="og:title")
    if og and og.get("content"):
        return str(og["content"]).strip()
    return ""


def scrape_meta(soup: BeautifulSoup) -> str:
    desc = soup.find("meta", attrs={"name": "description"})
    if desc and desc.get("content"):
        return str(desc["content"]).strip()
    og = soup.find("meta", property="og:description")
    if og and og.get("content"):
        return str(og["content"]).strip()
    return ""


def scrape_headings(soup: BeautifulSoup) -> dict[str, list[str]]:
    def texts(selector: str) -> list[str]:
        return [h.get_text(strip=True) for h in soup.select(selector) if h.get_text(strip=True)]

    return {"h1": texts("h1"), "h2": texts("h2"), "h3": texts("h3")}


def scrape_paragraphs(soup: BeautifulSoup) -> list[str]:
    paras: list[str] = []
    for p in soup.find_all("p"):
        t = p.get_text(separator=" ", strip=True)
        if len(t) > 2:
            paras.append(t)
        if len(paras) >= MAX_PARAGRAPHS_STORE:
            break
    return paras


def scrape_links(soup: BeautifulSoup, base_url: str) -> list[dict[str, str]]:
    links: list[dict[str, str]] = []
    for a in soup.find_all("a", href=True):
        href = str(a["href"]).strip()
        if not href or href.startswith("#"):
            continue
        full = urljoin(base_url, href)
        text = a.get_text(separator=" ", strip=True) or "(no text)"
        links.append({"href": full, "text": text[:200]})
        if len(links) >= MAX_LINKS_STORE:
            break
    return links


def scrape_images(soup: BeautifulSoup, base_url: str) -> list[dict[str, str]]:
    images: list[dict[str, str]] = []
    for img in soup.find_all("img", src=True):
        src = str(img["src"]).strip()
        if not src:
            continue
        full = urljoin(base_url, src)
        alt = (img.get("alt") or "").strip()
        images.append({"src": full, "alt": alt[:120]})
        if len(images) >= MAX_IMAGES_STORE:
            break
    return images


EMAIL_PATTERN = re.compile(
    r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}",
    re.MULTILINE,
)
PHONE_PATTERN = re.compile(
    r"(?:\+?1[-.\s]?)?(?:\(?\d{3}\)?[-.\s]?)?\d{3}[-.\s]?\d{4}"
    r"|\+?\d{1,3}[-.\s]?\d{2,4}[-.\s]?\d{2,4}[-.\s]?\d{2,4}",
    re.MULTILINE,
)


def scrape_emails(text: str) -> list[str]:
    found = set(EMAIL_PATTERN.findall(text or ""))
    return sorted(found)


def scrape_phone_numbers(text: str) -> list[str]:
    raw = PHONE_PATTERN.findall(text or "")
    cleaned: list[str] = []
    seen: set[str] = set()
    for p in raw:
        digits = re.sub(r"\D", "", p)
        if len(digits) < 7:
            continue
        if p not in seen:
            seen.add(p)
            cleaned.append(p.strip())
    return cleaned[:50]


def top_keywords(text: str, limit: int = 25) -> list[dict[str, int]]:
    words = re.findall(r"[a-zA-Z]{3,}", (text or "").lower())
    filtered = [w for w in words if w not in STOPWORDS]
    counts = Counter(filtered)
    return [{"word": w, "count": c} for w, c in counts.most_common(limit)]


def seo_score(data: dict) -> dict:
    """Basic on-page SEO checklist and a simple 0-100 score."""
    title = (data.get("title") or "").strip()
    meta = (data.get("meta_description") or "").strip()
    h1s = data.get("headings", {}).get("h1") or []
    checks: list[dict[str, object]] = []

    def add(ok: bool, label: str, detail: str) -> None:
        checks.append({"ok": ok, "label": label, "detail": detail})

    score = 0
    tlen = len(title)
    if 30 <= tlen <= 60:
        score += 20
        add(True, "Title length", f"Good ({tlen} chars).")
    elif tlen > 0:
        score += 10
        add(False, "Title length", f"Adjust to ~30-60 chars (currently {tlen}).")
    else:
        add(False, "Title", "Missing page title.")

    mlen = len(meta)
    if 120 <= mlen <= 160:
        score += 20
        add(True, "Meta description", f"Good ({mlen} chars).")
    elif mlen > 0:
        score += 10
        add(False, "Meta description", f"Target ~120-160 chars (currently {mlen}).")
    else:
        add(False, "Meta description", "Missing or empty.")

    if len(h1s) == 1:
        score += 20
        add(True, "H1 usage", "Single H1 found.")
    elif len(h1s) == 0:
        add(False, "H1 usage", "No H1 heading found.")
    else:
        score += 10
        add(False, "H1 usage", f"Multiple H1s ({len(h1s)}); prefer one.")

    paras = data.get("paragraphs") or []
    word_count = sum(len(re.findall(r"\w+", p)) for p in paras)
    if word_count >= 300:
        score += 15
        add(True, "Content depth", f"~{word_count} words in sampled paragraphs.")
    elif word_count > 0:
        score += 8
        add(False, "Content depth", f"Thin content (~{word_count} words in sample).")
    else:
        add(False, "Content depth", "Little or no paragraph text extracted.")

    links = data.get("links") or []
    internal = 0
    base_host = urlparse(data.get("final_url") or data.get("url") or "").netloc
    for L in links:
        try:
            if urlparse(L.get("href", "")).netloc == base_host:
                internal += 1
        except (ValueError, TypeError):
            continue
    if internal >= 5:
        score += 10
        add(True, "Internal links", f"{internal} internal links in sample.")
    elif internal > 0:
        score += 5
        add(False, "Internal links", f"Only {internal} internal links in sample.")
    else:
        add(False, "Internal links", "Few or no internal links detected in sample.")

    imgs = data.get("images") or []
    with_alt = sum(1 for i in imgs if (i.get("alt") or "").strip())
    if imgs and with_alt / max(len(imgs), 1) >= 0.5:
        score += 15
        add(True, "Image alt text", f"{with_alt}/{len(imgs)} images have alt text.")
    elif imgs:
        score += 7
        add(False, "Image alt text", "Add descriptive alt text to more images.")
    else:
        score += 5
        add(True, "Images", "No images in sample (neutral).")

    score = min(100, score)
    return {"score": score, "checks": checks}


def build_plain_text(data: dict) -> str:
    """Flatten scrape data for copy-to-clipboard (plain text only)."""
    if data.get("scrape_mode") == "custom":
        lines = [
            f"URL: {data.get('url', '')}",
            f"Final URL: {data.get('final_url', '')}",
            f"Category: {data.get('category', '')}",
            f"Selected fields: {', '.join(data.get('selected_fields') or [])}",
            "",
        ]
        ce = data.get("custom_extract") or {}
        if ce.get("product_names"):
            lines.append("Product names:")
            for n in ce["product_names"]:
                lines.append(f"  - {n}")
            lines.append("")
        if ce.get("prices"):
            lines.append("Prices:")
            for p in ce["prices"]:
                lines.append(f"  - {p}")
            lines.append("")
        if ce.get("brands"):
            lines.append("Brands:")
            for b in ce["brands"]:
                lines.append(f"  - {b}")
            lines.append("")
        if ce.get("reviews"):
            lines.append("Reviews / ratings:")
            for r in ce["reviews"]:
                lines.append(f"  - {r}")
            lines.append("")
        if ce.get("descriptions"):
            lines.append("Descriptions:")
            for d in ce["descriptions"]:
                lines.append(d)
            lines.append("")
        if ce.get("images"):
            lines.append("Images:")
            for im in ce["images"][:40]:
                if isinstance(im, dict):
                    lines.append(f"  {im.get('src')} | {im.get('alt', '')}")
            lines.append("")
        if ce.get("similar_items"):
            lines.append("Similar / related items:")
            for it in ce["similar_items"]:
                lines.append(f"  {it.get('name')} | {it.get('image')} | {it.get('href')}")
            lines.append("")
        if ce.get("links"):
            lines.append("Links:")
            for L in ce["links"][:60]:
                lines.append(f"  {L.get('href')} | {L.get('text')}")
        return "\n".join(lines)

    lines = [
        f"URL: {data.get('url', '')}",
        f"Title: {data.get('title', '')}",
        f"Meta: {data.get('meta_description', '')}",
        "",
        "Headings:",
    ]
    h = data.get("headings") or {}
    for level in ("h1", "h2", "h3"):
        for t in h.get(level, []):
            lines.append(f"  [{level.upper()}] {t}")
    lines.append("")
    lines.append("Paragraphs:")
    for p in (data.get("paragraphs") or [])[:30]:
        lines.append(p)
    lines.append("")
    lines.append("Links:")
    for L in (data.get("links") or [])[:50]:
        lines.append(f"  {L.get('href')} | {L.get('text')}")
    lines.append("")
    lines.append("Images:")
    for im in (data.get("images") or [])[:30]:
        lines.append(f"  {im.get('src')} | {im.get('alt')}")
    lines.append("")
    lines.append("Emails: " + ", ".join(data.get("emails") or []))
    lines.append("Phones: " + ", ".join(data.get("phones") or []))
    return "\n".join(lines)


def export_csv_bytes(data: dict) -> BytesIO:
    """Build a multi-section CSV using pandas for a clean download."""
    if data.get("scrape_mode") == "custom":
        rows: list[dict[str, str]] = []

        def add_row(section: str, name: str, value: str) -> None:
            rows.append({"section": section, "name": name, "value": value})

        add_row("summary", "url", data.get("url", ""))
        add_row("summary", "final_url", data.get("final_url", ""))
        add_row("summary", "category", str(data.get("category", "")))
        add_row("summary", "selected_fields", ",".join(data.get("selected_fields") or []))
        ce = data.get("custom_extract") or {}
        for n in ce.get("product_names") or []:
            add_row("product_name", "value", str(n))
        for p in ce.get("prices") or []:
            add_row("price", "value", str(p))
        for b in ce.get("brands") or []:
            add_row("brand", "value", str(b))
        for r in ce.get("reviews") or []:
            add_row("review", "value", str(r))
        for d in ce.get("descriptions") or []:
            add_row("description", "text", str(d))
        for im in ce.get("images") or []:
            if isinstance(im, dict):
                add_row("image", "src", str(im.get("src", "")))
                add_row("image", "alt", str(im.get("alt", "")))
        for it in ce.get("similar_items") or []:
            add_row("similar", "name", str(it.get("name", "")))
            add_row("similar", "image", str(it.get("image", "")))
            add_row("similar", "href", str(it.get("href", "")))
        for L in ce.get("links") or []:
            add_row("link", "text", str(L.get("text", "")))
            add_row("link", "href", str(L.get("href", "")))
        df = pd.DataFrame(rows)
        buf = BytesIO()
        df.to_csv(buf, index=False)
        buf.seek(0)
        return buf

    rows: list[dict[str, str]] = []

    def add_row(section: str, name: str, value: str) -> None:
        rows.append({"section": section, "name": name, "value": value})

    add_row("summary", "url", data.get("url", ""))
    add_row("summary", "final_url", data.get("final_url", ""))
    add_row("summary", "title", data.get("title", ""))
    add_row("summary", "meta_description", data.get("meta_description", ""))
    add_row("summary", "seo_score", str((data.get("seo") or {}).get("score", "")))

    for level in ("h1", "h2", "h3"):
        for t in (data.get("headings") or {}).get(level, []):
            add_row("heading", level, t)

    for p in data.get("paragraphs") or []:
        add_row("paragraph", "text", p)

    for L in data.get("links") or []:
        add_row("link", "anchor_text", L.get("text", ""))
        add_row("link", "href", L.get("href", ""))

    for im in data.get("images") or []:
        add_row("image", "alt", im.get("alt", ""))
        add_row("image", "src", im.get("src", ""))

    for e in data.get("emails") or []:
        add_row("email", "address", e)

    for ph in data.get("phones") or []:
        add_row("phone", "number", ph)

    for kw in data.get("keywords") or []:
        add_row("keyword", kw.get("word", ""), str(kw.get("count", "")))

    df = pd.DataFrame(rows)
    buf = BytesIO()
    df.to_csv(buf, index=False)
    buf.seek(0)
    return buf


# --- Custom / product-style scraping (heuristic; works best with schema.org / common shops) ---


def _walk_json_for_products(obj) -> list[dict]:
    """Collect dict nodes that declare @type Product (handles @graph and nested lists)."""
    found: list[dict] = []
    if isinstance(obj, dict):
        t = obj.get("@type")
        types = t if isinstance(t, list) else ([t] if t else [])
        if "Product" in types:
            found.append(obj)
        for v in obj.values():
            found.extend(_walk_json_for_products(v))
    elif isinstance(obj, list):
        for x in obj:
            found.extend(_walk_json_for_products(x))
    return found


def _products_from_json_ld(soup: BeautifulSoup) -> list[dict]:
    products: list[dict] = []
    for script in soup.find_all("script", attrs={"type": True}):
        t = (script.get("type") or "").lower()
        if "ld+json" not in t:
            continue
        raw = script.string or script.get_text() or ""
        if not raw.strip():
            continue
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            continue
        products.extend(_walk_json_for_products(data))
    return products


def _as_list(x) -> list:
    if x is None:
        return []
    if isinstance(x, list):
        return x
    return [x]


def _extract_ld_product_fields(prods: list[dict]) -> dict[str, list]:
    names, prices, brands, reviews, descs, imgs = [], [], [], [], [], []
    for p in prods:
        if (n := p.get("name")) and isinstance(n, str):
            names.append(n.strip())
        off = p.get("offers") or {}
        if isinstance(off, list) and off:
            off = off[0]
        if isinstance(off, dict):
            pr = off.get("price") or off.get("lowPrice") or off.get("highPrice")
            cur = off.get("priceCurrency", "")
            if pr is not None:
                prices.append(f"{pr} {cur}".strip())
        b = p.get("brand")
        if isinstance(b, dict) and b.get("name"):
            brands.append(str(b["name"]).strip())
        elif isinstance(b, str):
            brands.append(b.strip())
        agg = p.get("aggregateRating") or {}
        if isinstance(agg, dict):
            rc = agg.get("ratingValue")
            cnt = agg.get("reviewCount") or agg.get("ratingCount")
            if rc is not None or cnt is not None:
                reviews.append(f"rating={rc} count={cnt}".strip())
        if (d := p.get("description")) and isinstance(d, str) and len(d) > 20:
            descs.append(d.strip()[:2000])
        for im in _as_list(p.get("image")):
            if isinstance(im, str):
                imgs.append({"src": im, "alt": ""})
            elif isinstance(im, dict) and im.get("url"):
                imgs.append({"src": str(im["url"]), "alt": str(im.get("caption") or "")})
    return {
        "product_names": names,
        "prices": prices,
        "brands": brands,
        "reviews": reviews,
        "descriptions": descs,
        "images": imgs,
    }


_PRICE_RE = re.compile(
    r"(?:\$|€|£)\s*[\d,]+(?:\.\d{2})?|[\d,]+(?:\.\d{2})?\s*(?:USD|EUR|GBP|usd|eur)",
    re.I,
)


def _extract_prices_heuristic(soup: BeautifulSoup) -> list[str]:
    found: list[str] = []
    for el in soup.find_all(attrs={"itemprop": re.compile(r"price", re.I)}):
        t = el.get("content") or el.get_text(strip=True)
        if t and len(t) < 80:
            found.append(t)
    for el in soup.find_all(class_=re.compile(r"price|cost|amount", re.I)):
        t = el.get_text(separator=" ", strip=True)
        if t and 3 < len(t) < 120:
            found.append(t)
    body = soup.get_text(" ", strip=True)
    for m in _PRICE_RE.findall(body):
        if m not in found:
            found.append(m)
        if len(found) >= 40:
            break
    return list(dict.fromkeys(found))[:40]


def _extract_reviews_heuristic(soup: BeautifulSoup) -> list[str]:
    out: list[str] = []
    for meta in soup.find_all("meta"):
        n = (meta.get("name") or meta.get("property") or "").lower()
        if "review" in n and meta.get("content"):
            out.append(meta["content"].strip()[:500])
    for el in soup.find_all(attrs={"itemprop": re.compile(r"review", re.I)}):
        t = el.get_text(" ", strip=True)
        if t and len(t) > 5:
            out.append(t[:500])
    return list(dict.fromkeys(out))[:30]


def _is_related_like_section(tag) -> bool:
    if not getattr(tag, "name", None) or tag.name not in (
        "div",
        "section",
        "aside",
        "ul",
        "ol",
        "article",
    ):
        return False
    classes = tag.get("class") or []
    cid = (tag.get("id") or "").lower()
    parts = [cid] + [str(c).lower() for c in classes if isinstance(c, str)]
    blob = " ".join(parts)
    keys = ("related", "similar", "recommend", "also-like", "cross-sell", "upsell", "you-may")
    return any(k in blob for k in keys)


def _extract_similar_blocks(
    soup: BeautifulSoup,
    base_url: str,
    category: str,
    fashion_same: bool,
    fashion_similar: bool,
) -> list[dict[str, str]]:
    """Heuristic related / similar product cards (name, image, link)."""
    items: list[dict[str, str]] = []
    seen: set[str] = set()
    fashion_kw = (
        "dress",
        "gown",
        "skirt",
        "top",
        "shirt",
        "blouse",
        "jacket",
        "coat",
        "pants",
        "jeans",
        "shoe",
        "sneaker",
        "heel",
        "boot",
        "bag",
        "watch",
        "jewelry",
    )

    def matches_fashion(blob: str, strict: bool) -> bool:
        b = blob.lower()
        if strict:
            return "dress" in b or "gown" in b
        return any(k in b for k in fashion_kw)

    for sec in soup.find_all(_is_related_like_section):
        for a in sec.find_all("a", href=True):
                href = urljoin(base_url, str(a["href"]).strip())
                if not href.startswith(("http://", "https://")) or href in seen:
                    continue
                name = a.get_text(" ", strip=True)[:200]
                if len(name) < 2:
                    continue
                img = a.find("img") or (a.parent and a.parent.find("img"))
                src = ""
                alt = ""
                if img and img.get("src"):
                    src = urljoin(base_url, str(img["src"]).strip())
                    alt = (img.get("alt") or "").strip()[:200]
                blob = f"{name} {alt}"
                if category == "Fashion" and (fashion_same or fashion_similar):
                    if fashion_same and not matches_fashion(blob, strict=True):
                        continue
                    if fashion_similar and not fashion_same and not matches_fashion(blob, strict=False):
                        continue
                seen.add(href)
                items.append({"name": name, "image": src, "href": href})
                if len(items) >= 36:
                    return items
    return items


def extract_custom_product_data(
    soup: BeautifulSoup,
    base_url: str,
    fields: set[str],
    category: str,
    fashion_same: bool,
    fashion_similar: bool,
) -> dict[str, list]:
    """Build raw buckets; caller filters to requested fields."""
    ld = _products_from_json_ld(soup)
    ld_fields = _extract_ld_product_fields(ld)

    names = list(ld_fields["product_names"])
    if not names:
        t = scrape_title(soup)
        if t:
            names.append(t)
        for h in soup.find_all(["h1", "h2"], limit=3):
            tx = h.get_text(strip=True)
            if 10 < len(tx) < 200:
                names.append(tx)
    names = list(dict.fromkeys(names))[:25]

    imgs = ld_fields["images"][:80]
    if not imgs:
        imgs = scrape_images(soup, base_url)[:80]

    prices = ld_fields["prices"] or _extract_prices_heuristic(soup)
    brands = list(dict.fromkeys(ld_fields["brands"]))[:20]
    reviews = ld_fields["reviews"] or _extract_reviews_heuristic(soup)
    descs = ld_fields["descriptions"]
    if not descs:
        m = scrape_meta(soup)
        if m:
            descs.append(m)
        for p in soup.find_all("p", limit=8):
            t = p.get_text(" ", strip=True)
            if len(t) > 80:
                descs.append(t[:1500])
    descs = list(dict.fromkeys(descs))[:15]

    eff_fields = set(fields)
    if category == "Fashion" and (fashion_same or fashion_similar):
        eff_fields.add("similar_products")

    similar: list[dict[str, str]] = []
    if "similar_products" in eff_fields:
        similar = _extract_similar_blocks(
            soup, base_url, category, fashion_same, fashion_similar
        )

    links = scrape_links(soup, base_url)[:200] if eff_fields & {"links"} else []

    buckets = {
        "product_names": names,
        "images": imgs,
        "prices": prices,
        "brands": brands,
        "reviews": reviews,
        "descriptions": descs,
        "similar_items": similar,
        "links": links,
    }
    out: dict[str, list] = {}
    key_map = {
        "product_name": "product_names",
        "images": "images",
        "price": "prices",
        "brand": "brands",
        "reviews": "reviews",
        "description": "descriptions",
        "similar_products": "similar_items",
        "links": "links",
    }
    for f in eff_fields:
        k = key_map.get(f)
        if k and k in buckets:
            out[k] = buckets[k]
    return out


def run_scrape_custom(
    url: str,
    fields: set[str],
    category: str,
    fashion_same: bool,
    fashion_similar: bool,
) -> dict:
    """User-directed scrape: only selected buckets are populated."""
    final_url, soup, truncated = fetch_page(url)
    custom = extract_custom_product_data(
        soup, final_url, fields, category, fashion_same, fashion_similar
    )

    names = custom.get("product_names") or []
    descs = custom.get("descriptions") or []
    title = names[0] if names else scrape_title(soup)
    meta = (descs[0][:500] if descs else "") or scrape_meta(soup)

    thin_for_seo = {
        "title": title,
        "meta_description": meta,
        "headings": {"h1": names[:1], "h2": [], "h3": []},
        "paragraphs": descs[:5],
        "links": custom.get("links", []),
        "images": custom.get("images", []),
    }
    seo = seo_score(thin_for_seo)

    n_links = len(custom.get("links", []))
    n_imgs = len(custom.get("images", []))
    n_sim = len(custom.get("similar_items", []))
    n_names = len(custom.get("product_names", []))

    data: dict = {
        "scrape_mode": "custom",
        "category": category,
        "selected_fields": sorted(fields),
        "fashion_same_dress": fashion_same,
        "fashion_similar_dress": fashion_similar,
        "url": url,
        "final_url": final_url,
        "title": title,
        "meta_description": meta,
        "custom_extract": custom,
        "response_truncated": truncated,
        "headings": {"h1": [], "h2": [], "h3": []},
        "paragraphs": [],
        "paragraphs_total": 0,
        "links": [],
        "links_total": 0,
        "images": [],
        "images_total": 0,
        "emails": [],
        "phones": [],
        "keywords": top_keywords(" ".join(names + descs)),
        "seo": seo,
        "counts": {
            "links": n_links,
            "images": n_imgs,
            "headings": n_names + n_sim,
            "emails": 0,
            "phones": 0,
            "paragraphs": len(descs),
        },
    }
    data["plain_text"] = build_plain_text(data)
    data["custom_stats"] = {
        "selections": len(fields),
        "data_points": sum(len(v) for v in custom.values() if isinstance(v, list)),
    }
    return data


def run_scrape(url: str) -> dict:
    """Run the full extraction pipeline and assemble the result payload."""
    final_url, soup, truncated = fetch_page(url)
    title = scrape_title(soup)
    meta = scrape_meta(soup)
    headings = scrape_headings(soup)
    paragraphs = scrape_paragraphs(soup)
    links = scrape_links(soup, final_url)
    images = scrape_images(soup, final_url)

    body_text = soup.get_text(separator="\n", strip=True)
    emails = scrape_emails(body_text)
    phones = scrape_phone_numbers(body_text)

    kw_source = " ".join(
        [title, meta]
        + headings["h1"]
        + headings["h2"]
        + paragraphs[:20]
    )
    keywords = top_keywords(kw_source)

    data: dict = {
        "url": url,
        "final_url": final_url,
        "title": title,
        "meta_description": meta,
        "headings": headings,
        "paragraphs": paragraphs,
        "paragraphs_total": len(paragraphs),
        "links": links,
        "links_total": len(links),
        "images": images,
        "images_total": len(images),
        "emails": emails,
        "phones": phones,
        "keywords": keywords,
        "response_truncated": truncated,
    }

    all_headings = headings["h1"] + headings["h2"] + headings["h3"]
    data["counts"] = {
        "links": data["links_total"],
        "images": data["images_total"],
        "headings": len(all_headings),
        "emails": len(emails),
        "phones": len(phones),
        "paragraphs": data["paragraphs_total"],
    }

    data["seo"] = seo_score(data)
    data["plain_text"] = build_plain_text(data)
    data["scrape_mode"] = "full"
    return data


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def find_recent_duplicate_id(conn: sqlite3.Connection, canonical_url: str) -> int | None:
    """If the same final URL was scraped recently, return its id to avoid spamming history."""
    row = conn.execute(
        f"""
        SELECT id, scraped_at FROM {HISTORY_TABLE}
        WHERE url = ?
        ORDER BY id DESC
        LIMIT 1
        """,
        (canonical_url,),
    ).fetchone()
    if not row:
        return None
    raw_ts = row["scraped_at"] or ""
    try:
        ts = raw_ts.replace("Z", "+00:00")
        last = datetime.fromisoformat(ts)
        if last.tzinfo is None:
            last = last.replace(tzinfo=timezone.utc)
    except ValueError:
        return None
    if datetime.now(timezone.utc) - last <= DUPLICATE_WINDOW:
        return int(row["id"])
    return None


def save_scrape(data: dict) -> tuple[int, bool]:
    """
    Persist scrape JSON into `history`. Returns (row_id, refreshed_existing).

    If the same canonical URL was scraped within DUPLICATE_WINDOW, the existing
    row is updated in place (fresh data, one history line — avoids spam entries).
    """
    canonical = data.get("final_url") or data.get("url")
    scraped_at = _now_iso()
    payload = json.dumps(data, ensure_ascii=False)

    try:
        with db_session() as conn:
            dup_id = find_recent_duplicate_id(conn, canonical)
            if dup_id is not None:
                conn.execute(
                    f"""
                    UPDATE {HISTORY_TABLE}
                    SET title = ?, meta_description = ?, scraped_at = ?, data_json = ?
                    WHERE id = ?
                    """,
                    (
                        data.get("title"),
                        data.get("meta_description"),
                        scraped_at,
                        payload,
                        dup_id,
                    ),
                )
                return dup_id, True

            cur = conn.execute(
                f"""
                INSERT INTO {HISTORY_TABLE} (url, title, scraped_at, meta_description, data_json)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    canonical,
                    data.get("title"),
                    scraped_at,
                    data.get("meta_description"),
                    payload,
                ),
            )
            return int(cur.lastrowid), False
    except sqlite3.Error as exc:
        logger.exception("SQLite error while saving scrape")
        raise ValueError(
            "Could not save to the database. Close other programs using database.db "
            "or check that the project folder is writable."
        ) from exc


def repair_loaded_payload(data: dict) -> dict:
    """Ensure legacy rows have fields newer templates expect."""
    if data.get("scrape_mode") == "custom":
        data.setdefault("custom_extract", {})
        ce = data["custom_extract"]
        for k in (
            "product_names",
            "images",
            "prices",
            "brands",
            "reviews",
            "descriptions",
            "similar_items",
            "links",
        ):
            if k not in ce or not isinstance(ce.get(k), list):
                ce[k] = []
        data.setdefault("selected_fields", [])
        data.setdefault("category", "")
        data.setdefault("fashion_same_dress", False)
        data.setdefault("fashion_similar_dress", False)
        data.setdefault("headings", {"h1": [], "h2": [], "h3": []})
        data.setdefault("paragraphs", [])
        data.setdefault("links", [])
        data.setdefault("images", [])
        data.setdefault("emails", [])
        data.setdefault("phones", [])
        data.setdefault("keywords", [])
        if "seo" not in data or not isinstance(data.get("seo"), dict):
            data["seo"] = {"score": 0, "checks": []}
        if "plain_text" not in data:
            data["plain_text"] = build_plain_text(data)
        if "response_truncated" not in data:
            data["response_truncated"] = False
        n_links = len(ce.get("links", []))
        n_imgs = len(ce.get("images", []))
        n_sim = len(ce.get("similar_items", []))
        n_names = len(ce.get("product_names", []))
        n_desc = len(ce.get("descriptions", []))
        data["counts"] = {
            "links": n_links,
            "images": n_imgs,
            "headings": n_names + n_sim,
            "emails": 0,
            "phones": 0,
            "paragraphs": n_desc,
        }
        if "custom_stats" not in data:
            data["custom_stats"] = {
                "selections": len(data.get("selected_fields") or []),
                "data_points": sum(len(ce[k]) for k in ce if isinstance(ce.get(k), list)),
            }
        return data

    if "headings" not in data or not isinstance(data.get("headings"), dict):
        data["headings"] = {"h1": [], "h2": [], "h3": []}
    for key in ("paragraphs", "links", "images", "emails", "phones", "keywords"):
        if key not in data or not isinstance(data.get(key), list):
            data[key] = []
    if "counts" not in data or not isinstance(data.get("counts"), dict):
        h = data["headings"]
        emails = data.get("emails") or []
        data["counts"] = {
            "links": int(data.get("links_total") or len(data.get("links") or [])),
            "images": int(data.get("images_total") or len(data.get("images") or [])),
            "headings": len((h.get("h1") or [])) + len((h.get("h2") or [])) + len((h.get("h3") or [])),
            "emails": len(emails),
            "phones": len(data.get("phones") or []),
            "paragraphs": int(data.get("paragraphs_total") or len(data.get("paragraphs") or [])),
        }
    if "seo" not in data or not isinstance(data.get("seo"), dict):
        data["seo"] = seo_score(data)
    else:
        seo = data["seo"]
        if "checks" not in seo or not isinstance(seo.get("checks"), list):
            seo["checks"] = []
        try:
            raw = int(seo.get("score", 0))
        except (TypeError, ValueError):
            raw = 0
        seo["score"] = max(0, min(100, raw))
    if "plain_text" not in data:
        data["plain_text"] = build_plain_text(data)
    if "response_truncated" not in data:
        data["response_truncated"] = False
    data.setdefault("scrape_mode", "full")
    return data


def load_scrape(scrape_id: int) -> dict | None:
    try:
        with db_session() as conn:
            row = conn.execute(
                f"SELECT * FROM {HISTORY_TABLE} WHERE id = ?", (scrape_id,)
            ).fetchone()
    except sqlite3.Error:
        logger.exception("SQLite error while loading scrape id=%s", scrape_id)
        return None
    if not row:
        return None
    try:
        data = json.loads(row["data_json"])
    except json.JSONDecodeError:
        logger.error("Corrupt JSON for scrape id %s", scrape_id)
        return None
    data = repair_loaded_payload(data)
    data["id"] = row["id"]
    data["scraped_at"] = row["scraped_at"]
    try:
        data["created_at"] = row["created_at"]
    except (KeyError, IndexError):
        data["created_at"] = row["scraped_at"]
    return data


def _escape_like(term: str) -> str:
    """Escape SQL LIKE wildcards in user search input."""
    return term.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/about")
def about():
    return render_template("about.html")


@app.route("/scrape", methods=["POST"])
def scrape_route():
    raw = request.form.get("url", "")
    try:
        url = normalize_url(raw)
    except ValueError as e:
        flash(str(e), "danger")
        return redirect(url_for("index"))

    fields_raw = request.form.getlist("scrape_fields")
    fields = {f for f in fields_raw if f in CUSTOM_FIELD_KEYS}
    category = (request.form.get("category") or "").strip()
    if category not in SCRAPIQ_CATEGORIES:
        category = "Electronics"
    fashion_same = request.form.get("fashion_same_dress") == "on"
    fashion_similar = request.form.get("fashion_similar_dress") == "on"

    try:
        if fields:
            data = run_scrape_custom(url, fields, category, fashion_same, fashion_similar)
        else:
            data = run_scrape(url)
        try:
            scrape_id, refreshed = save_scrape(data)
        except ValueError as save_err:
            flash(str(save_err), "danger")
            return redirect(url_for("index"))
        if refreshed:
            flash(
                "Recent scrape for this URL was refreshed (same history entry) so your list stays tidy.",
                "info",
            )
        if data.get("response_truncated"):
            flash(
                "Note: the downloaded HTML was very large, so only the first portion was analyzed for speed.",
                "warning",
            )
        return redirect(url_for("result", scrape_id=scrape_id))
    except ValueError as e:
        flash(str(e), "danger")
        return redirect(url_for("index"))
    except Exception:
        logger.exception("Unexpected scrape failure for url=%s", url)
        flash(
            "An unexpected error occurred while scraping. Please try again or use a different URL.",
            "danger",
        )
        return redirect(url_for("index"))


@app.route("/result/<int:scrape_id>")
def result(scrape_id: int):
    data = load_scrape(scrape_id)
    if not data:
        abort(404)
    return render_template("result.html", data=data)


@app.route("/history")
def history():
    q_raw = (request.args.get("q") or "").strip()
    q_lower = q_raw.lower()
    q_esc = _escape_like(q_lower)
    rows: list[sqlite3.Row] = []

    try:
        with db_session() as conn:
            if q_lower:
                rows = conn.execute(
                    f"""
                    SELECT id, url, title, scraped_at, created_at
                    FROM {HISTORY_TABLE}
                    WHERE LOWER(url) LIKE ? ESCAPE '\\'
                       OR LOWER(COALESCE(title, '')) LIKE ? ESCAPE '\\'
                    ORDER BY scraped_at DESC
                    LIMIT ?
                    """,
                    (f"%{q_esc}%", f"%{q_esc}%", HISTORY_LIST_LIMIT),
                ).fetchall()
            else:
                rows = conn.execute(
                    f"""
                    SELECT id, url, title, scraped_at, created_at
                    FROM {HISTORY_TABLE}
                    ORDER BY scraped_at DESC
                    LIMIT ?
                    """,
                    (HISTORY_LIST_LIMIT,),
                ).fetchall()
    except sqlite3.Error:
        logger.exception("SQLite error on history page")
        flash("History could not be loaded. Try closing other apps using database.db or restart the server.", "danger")

    return render_template("history.html", rows=rows, q=q_raw)


@app.post("/history/clear")
def history_clear_all():
    """Remove every row from the history table."""
    try:
        with db_session() as conn:
            conn.execute(f"DELETE FROM {HISTORY_TABLE}")
    except sqlite3.Error:
        logger.exception("SQLite error clearing history")
        flash("Could not clear history (database error).", "danger")
        return redirect(url_for("history"))
    flash("All history entries were deleted.", "success")
    return redirect(url_for("history"))


@app.post("/history/delete/<int:entry_id>")
def history_delete_one(entry_id: int):
    """Delete a single history row by id."""
    try:
        with db_session() as conn:
            cur = conn.execute(f"DELETE FROM {HISTORY_TABLE} WHERE id = ?", (entry_id,))
            deleted = cur.rowcount
    except sqlite3.Error:
        logger.exception("SQLite error deleting history id=%s", entry_id)
        flash("Could not delete that entry (database error).", "danger")
        return redirect(url_for("history"))
    if deleted:
        flash("History entry removed.", "success")
    else:
        flash("No matching history entry was found.", "warning")
    return redirect(url_for("history"))


@app.route("/export/<int:scrape_id>")
def export_csv(scrape_id: int):
    data = load_scrape(scrape_id)
    if not data:
        abort(404)
    buf = export_csv_bytes(data)
    filename = f"scrapiq_export_{scrape_id}.csv"
    return send_file(
        buf,
        mimetype="text/csv",
        as_attachment=True,
        download_name=filename,
    )


@app.context_processor
def inject_globals():
    return {
        "current_year": datetime.now(timezone.utc).year,
        "DISPLAY_PARAS": MAX_PARAGRAPHS_DISPLAY,
        "DISPLAY_LINKS": MAX_LINKS_DISPLAY,
        "DISPLAY_IMAGES": MAX_IMAGES_DISPLAY,
        "HISTORY_LIST_LIMIT": HISTORY_LIST_LIMIT,
    }


@app.route("/health")
def health():
    return {"status": "ok", "app": "ScrapIQ"}


init_db()


def _detect_lan_ipv4() -> str | None:
    """
    Best-effort IPv4 address of this PC on the local LAN (same Wi-Fi as your phone).
    Uses the interface that would handle outbound traffic; no data is sent.
    """
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.settimeout(0.8)
            s.connect(("8.8.8.8", 53))
            ip = s.getsockname()[0]
            if ip and not ip.startswith("127."):
                return ip
    except OSError:
        pass
    try:
        for info in socket.getaddrinfo(socket.gethostname(), None, socket.AF_INET):
            addr = info[4][0]
            if addr and not addr.startswith("127."):
                return addr
    except OSError:
        pass
    return None


def _print_startup_urls(port: int) -> None:
    """Print where to open ScrapIQ on this PC and on other devices on the same Wi-Fi."""
    lan = _detect_lan_ipv4()
    print("\n" + "=" * 58)
    print("  ScrapIQ — Smart Web Scraper & Data Extractor")
    print("  Listening on all interfaces (0.0.0.0) — use the URLs below.")
    print()
    print("  Open on this PC:")
    print(f"    http://127.0.0.1:{port}/")
    print(f"    http://localhost:{port}/")
    print()
    if lan:
        print("  Open on phone / other device (same Wi-Fi):")
        print(f"    http://{lan}:{port}/")
    else:
        print("  Open on phone (same Wi-Fi):")
        print("    (Could not auto-detect LAN IP — use your PC's Wi-Fi IPv4 from")
        print("     Windows Settings → Network → Wi-Fi → your network → Properties.)")
    print()
    print("  Tip: If the phone cannot load the page, allow Python in Windows Firewall")
    print("  for Private networks (see Windows Security → Firewall).")
    print("=" * 58 + "\n")


def _run_app() -> None:
    """Start Flask so the site works on this PC and on other devices on the same LAN."""
    try:
        port = int(os.environ.get("SCRAPIQ_PORT", "5000"))
    except ValueError:
        port = 5000

    _print_startup_urls(port)
    try:
        app.run(host="0.0.0.0", port=port, debug=True)
    except OSError as exc:
        err = str(exc).lower()
        if port == 5000 and ("10048" in str(exc) or "address already in use" in err or "in use" in err):
            alt = 5001
            print(f"Port {port} is in use — switching to port {alt}.")
            print("(Set SCRAPIQ_PORT=5001 to use this port every time.)\n")
            _print_startup_urls(alt)
            app.run(host="0.0.0.0", port=alt, debug=True)
        else:
            raise


if __name__ == "__main__":
    _run_app()
