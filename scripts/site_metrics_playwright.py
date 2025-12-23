import gzip
import re
import sys
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse, urldefrag

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError


DEFAULT_SITEMAP_CANDIDATES = [
    "/sitemap.xml",
    "/sitemap_index.xml",
    "/sitemap-index.xml",
    "/sitemap.xml.gz",
]


@dataclass
class Result:
    website: str
    total_pages: int
    average_words: float
    id: str


def normalize_base_url(url: str) -> str:
    url = url.strip()
    if not url:
        raise ValueError("website is leeg")
    if not url.startswith("http://") and not url.startswith("https://"):
        url = "https://" + url
    parsed = urlparse(url)
    if not parsed.netloc:
        raise ValueError("website url is ongeldig")
    return f"{parsed.scheme}://{parsed.netloc}/"


def same_host(a: str, b: str) -> bool:
    return urlparse(a).netloc.lower() == urlparse(b).netloc.lower()


def is_probably_cloudflare_block(html_or_text: str) -> bool:
    t = (html_or_text or "").lower()
    return (
        "attention required" in t
        or "cloudflare" in t
        or "please enable cookies" in t
        or "sorry, you have been blocked" in t
    )


def parse_robots_for_sitemaps(robots_text: str) -> List[str]:
    sitemaps = []
    for line in robots_text.splitlines():
        line = line.strip()
        if not line:
            continue
        if line.lower().startswith("sitemap:"):
            parts = line.split(":", 1)
            if len(parts) == 2:
                sm = parts[1].strip()
                if sm:
                    sitemaps.append(sm)
    return sitemaps


def strip_ns(tag: str) -> str:
    return tag.split("}", 1)[-1] if "}" in tag else tag


def parse_sitemap_xml(xml_bytes: bytes) -> Tuple[List[str], List[str]]:
    urls: List[str] = []
    children: List[str] = []
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError:
        return urls, children

    root_tag = strip_ns(root.tag).lower()

    if root_tag == "urlset":
        for url_el in root.findall(".//{*}url"):
            loc_el = url_el.find("{*}loc")
            if loc_el is not None and loc_el.text:
                urls.append(loc_el.text.strip())
    elif root_tag == "sitemapindex":
        for sm_el in root.findall(".//{*}sitemap"):
            loc_el = sm_el.find("{*}loc")
            if loc_el is not None and loc_el.text:
                children.append(loc_el.text.strip())

    return urls, children


def fetch_bytes_with_request(context, url: str, timeout_ms: int = 25000) -> Optional[bytes]:
    try:
        resp = context.request.get(url, timeout=timeout_ms)
        if not resp:
            return None
        if resp.status != 200:
            return None
        body = resp.body()
        if not body:
            return None
        return body
    except Exception:
        return None


def fetch_text_with_request(context, url: str, timeout_ms: int = 20000) -> Optional[str]:
    try:
        resp = context.request.get(url, timeout=timeout_ms)
        if not resp:
            return None
        if resp.status != 200:
            return None
        txt = resp.text()
        if not txt:
            return None
        return txt
    except Exception:
        return None


def open_homepage_for_cookies(page, base_url: str) -> None:
    try:
        page.goto(base_url, wait_until="domcontentloaded", timeout=30000)
        time.sleep(1.0)
    except PlaywrightTimeoutError:
        return
    except Exception:
        return


def try_discover_sitemaps(context, base_url: str) -> List[str]:
    discovered: List[str] = []

    robots_url = urljoin(base_url, "robots.txt")
    robots_text = fetch_text_with_request(context, robots_url)
    if robots_text and not is_probably_cloudflare_block(robots_text):
        discovered.extend(parse_robots_for_sitemaps(robots_text))

    for path in DEFAULT_SITEMAP_CANDIDATES:
        discovered.append(urljoin(base_url, path.lstrip("/")))

    seen = set()
    unique = []
    for u in discovered:
        if u not in seen:
            seen.add(u)
            unique.append(u)
    return unique


def fetch_all_sitemap_urls(context, sitemap_url: str, max_children: int = 50) -> List[str]:
    raw = fetch_bytes_with_request(context, sitemap_url, timeout_ms=30000)
    if not raw:
        return []

    content = raw
    if sitemap_url.endswith(".gz"):
        try:
            content = gzip.decompress(content)
        except Exception:
            return []

    if is_probably_cloudflare_block(content.decode("utf-8", errors="ignore")):
        return []

    urls, children = parse_sitemap_xml(content)
    if urls:
        return urls

    all_urls: List[str] = []
    for child in children[:max_children]:
        time.sleep(0.2)
        all_urls.extend(fetch_all_sitemap_urls(context, child, max_children=max_children))
    return all_urls


def discover_urls_from_sitemaps(context, base_url: str) -> List[str]:
    candidates = try_discover_sitemaps(context, base_url)
    for sm in candidates:
        urls = fetch_all_sitemap_urls(context, sm)
        if urls:
            cleaned = []
            seen = set()
            for u in urls:
                u = u.strip()
                if not u:
                    continue
                if u not in seen:
                    seen.add(u)
                    cleaned.append(u)
            return cleaned
    return []


def extract_internal_links(html: str, page_url: str, base_url: str) -> List[str]:
    soup = BeautifulSoup(html, "lxml")
    links = []
    for a in soup.find_all("a", href=True):
        href = (a.get("href") or "").strip()
        if not href:
            continue
        absolute = urljoin(page_url, href)
        absolute, _ = urldefrag(absolute)
        if not same_host(absolute, base_url):
            continue
        parsed = urlparse(absolute)
        if parsed.scheme not in ["http", "https"]:
            continue
        links.append(absolute)
    return links


def fallback_crawl_internal(page, base_url: str, limit: int = 2000) -> List[str]:
    queue: List[str] = [base_url]
    visited: Set[str] = set()

    while queue and len(visited) < limit:
        url = queue.pop(0)
        if url in visited:
            continue
        visited.add(url)

        try:
            page.goto(url, wait_until="domcontentloaded", timeout=30000)
            time.sleep(0.4)
            html = page.content() or ""
        except Exception:
            continue

        if is_probably_cloudflare_block(html):
            continue

        found = extract_internal_links(html, url, base_url)
        for u in found:
            if u not in visited:
                queue.append(u)

    return list(visited)


def word_count_from_html(html: str) -> int:
    soup = BeautifulSoup(html, "lxml")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    text = soup.get_text(separator=" ")
    text = re.sub(r"\s+", " ", text).strip()
    if not text:
        return 0

    words = re.findall(r"\b[\wÀ-ÿ]+\b", text)
    return len(words)


def compute_average_words(page, urls: List[str], sample_size: int = 10) -> float:
    sample = urls[:sample_size]
    counts = []

    for u in sample:
        try:
            page.goto(u, wait_until="domcontentloaded", timeout=30000)
            time.sleep(0.4)
            html = page.content() or ""
        except Exception:
            continue

        if is_probably_cloudflare_block(html):
            continue

        wc = word_count_from_html(html)
        if wc > 0:
            counts.append(wc)

    if not counts:
        return 0.0
    return sum(counts) / float(len(counts))


def main() -> int:
    if len(sys.argv) < 3:
        print("Gebruik: python scripts/site_metrics_playwright.py <id> <website_url>", file=sys.stderr)
        return 2

    given_id = sys.argv[1].strip()
    given_website = sys.argv[2].strip()
    base_url = normalize_base_url(given_website)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            locale="nl-NL",
            extra_http_headers={
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "nl-NL,nl;q=0.9,en;q=0.8",
            },
        )
        page = context.new_page()

        open_homepage_for_cookies(page, base_url)

        urls = discover_urls_from_sitemaps(context, base_url)
        if not urls:
            urls = fallback_crawl_internal(page, base_url)

        urls = [u for u in urls if same_host(u, base_url)]
        urls = sorted(set(urls))

        total_pages = len(urls)
        average_words = compute_average_words(page, urls, sample_size=10)

        browser.close()

    result = Result(
        website=given_website,
        total_pages=total_pages,
        average_words=round(average_words, 2),
        id=given_id,
    )

    print(f"website={result.website}")
    print(f"totalPages={result.total_pages}")
    print(f"averageWords={result.average_words}")
    print(f"id={result.id}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
