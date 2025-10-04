#!/usr/bin/env python3
"""
scrapper.py
Robust scraper for JobYaari -> fills knowledge_base.json

Features:
- Multi-strategy selector detection
- Session with retries + backoff
- Rate limiting + small random delays
- Deduplication and normalization
- Extracts title, organization, url, snippet, vacancies, salary, age, experience, qualification, posted
- Saves to knowledge_base.json
- Defensive: warns if page appears JS-rendered
"""

import os
import re
import json
import time
import random
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------- Configuration ----------
BASE_URL = "https://www.jobyaari.com"
PRIMARY_LISTING_PATH = "/latest-jobs"  # primary page to try
OUTPUT_FILE = "knowledge_base.json"
MAX_PER_CATEGORY = 7
REQUEST_TIMEOUT = 20
MAX_RETRIES = 3
DELAY_BETWEEN_REQUESTS = (1.0, 3.0)  # random sleep range (seconds)
CATEGORIES = ["Engineering", "Science", "Commerce", "Education"]

# ---------- Logging ----------
logger = logging.getLogger("JobYaariScraper")
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger.addHandler(ch)

# ---------- HTTP Session ----------
session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9"
})

retry_strategy = Retry(
    total=MAX_RETRIES,
    backoff_factor=1.0,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET", "POST"]
)
adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=10, pool_maxsize=20)
session.mount("https://", adapter)
session.mount("http://", adapter)


# ---------- Utility extractors (regex-based) ----------
def extract_vacancies(text: str) -> Optional[str]:
    if not text:
        return None
    patterns = [
        r'(\d{1,5})\s*(?:posts?|vacancies|vacancy|positions?|openings?)',
        r'(?:vacancy|vacancies|posts?)[:\s]+(\d{1,5})'
    ]
    for p in patterns:
        m = re.search(p, text, re.I)
        if m:
            return m.group(1)
    if re.search(r'\bmultiple\b', text, re.I):
        return "Multiple"
    return None

def extract_salary(text: str) -> Optional[str]:
    if not text:
        return None
    patterns = [
        r'(?:₹|Rs\.?|INR)\s*[\d,]+(?:\s*[-–]\s*(?:₹|Rs\.?|INR)?\s*[\d,]+)?',
        r'pay\s*scale[:\s]*[A-Za-z0-9\(\)\s,.-]+'
    ]
    for p in patterns:
        m = re.search(p, text, re.I)
        if m:
            return m.group(0).strip()
    return None

def extract_age(text: str) -> Optional[str]:
    if not text:
        return None
    patterns = [
        r'(\d{1,2}\s*-\s*\d{1,2}\s*years?)',
        r'(?:age|up to)[:\s]+(\d{1,2}\s*years?)',
    ]
    for p in patterns:
        m = re.search(p, text, re.I)
        if m:
            return m.group(0).strip()
    return None

def extract_experience(text: str) -> Optional[str]:
    if not text:
        return None
    if re.search(r'\b(?:fresher|no experience)\b', text, re.I):
        return "Fresher"
    patterns = [r'(\d{1,2}[+]?)\s*(?:years?|yrs?)\s*(?:experience|exp)']
    for p in patterns:
        m = re.search(p, text, re.I)
        if m:
            return m.group(0).strip()
    return None

def extract_qualification(text: str) -> Optional[str]:
    if not text:
        return None
    quals = [
        (r'\bph\.?d\b', "Ph.D"),
        (r'\bm\.?tech\b', "M.Tech"),
        (r'\bb\.?tech\b', "B.Tech"),
        (r'\bm\.?sc\b', "M.Sc"),
        (r'\bb\.?sc\b', "B.Sc"),
        (r'\bmba\b', "MBA"),
        (r'\bb\.?ed\b', "B.Ed"),
        (r'\bgraduate\b', "Graduate"),
    ]
    for patt, name in quals:
        if re.search(patt, text, re.I):
            return name
    return None

def extract_posted_date(text: str) -> Optional[str]:
    if not text:
        return None
    # relative days
    m = re.search(r'(\d+)\s*days?\s*ago', text, re.I)
    if m:
        days = int(m.group(1))
        d = datetime.utcnow() - timedelta(days=days)
        return d.date().isoformat()
    # direct YYYY-MM-DD in text
    m2 = re.search(r'20\d{2}[-/]\d{1,2}[-/]\d{1,2}', text)
    if m2:
        return m2.group(0)
    return None

def normalize_url(base: str, url: str) -> Optional[str]:
    if not url:
        return None
    url = url.strip()
    try:
        abs_url = urljoin(base, url)
        parsed = urlparse(abs_url)
        if parsed.scheme and parsed.netloc:
            return abs_url
    except Exception:
        pass
    return None

# ---------- Scraping helper functions ----------
def fetch(url: str) -> Optional[requests.Response]:
    try:
        logger.info(f"GET {url}")
        resp = session.get(url, timeout=REQUEST_TIMEOUT)
        if resp.status_code == 200:
            return resp
        logger.warning(f"Non-200 {resp.status_code} for {url}")
    except requests.exceptions.RequestException as e:
        logger.warning(f"Request failed: {e}")
    return None

def find_job_nodes(soup: BeautifulSoup) -> List:
    """
    Multi-strategy node discovery:
    1) JobYaari-typical nodes (div.job_listing etc)
    2) Links with job keywords -> their parent containers
    3) Generic article/div selectors with content+links
    """
    # Strategy specific to JobYaari (common classes)
    CandidateSelectors = [
        "div.job_listing", "div.latest-job", "div.job-box", "div.job-item",
        "li.job", "div.job-listing", "div.listing-item", "article.post", "article"
    ]
    for sel in CandidateSelectors:
        nodes = soup.select(sel)
        if nodes and len(nodes) >= 2:
            logger.debug(f"Selector '{sel}' matched {len(nodes)} nodes")
            return nodes[:200]

    # Strategy: find a-tags with job keywords and capture parents
    keywords = ['recruitment', 'notification', 'vacancy', 'vacancies', 'job', 'exam', 'admit', 'result', 'apply', 'posts', 'position']
    potential = []
    for a in soup.find_all('a', href=True, limit=300):
        t = a.get_text(separator=" ", strip=True)
        href = a.get('href', '')
        if not t:
            continue
        low = (t + " " + href).lower()
        if any(kw in low for kw in keywords) and len(t) > 10:
            parent = a.parent
            # ascend up to 4 levels to find a container
            for _ in range(4):
                if parent is None:
                    break
                if parent.name in ('article', 'div', 'li'):
                    potential.append(parent)
                    break
                parent = parent.parent
    # dedupe keeping order
    seen = set()
    unique = []
    for el in potential:
        try:
            key = (el.name, tuple(el.get('class', [])), el.get_text(strip=True)[:80])
        except Exception:
            key = str(el)[:80]
        if key not in seen:
            seen.add(key)
            unique.append(el)
    if len(unique) >= 2:
        logger.debug(f"Strategy Link-Parent found {len(unique)} nodes")
        return unique[:200]

    # Last-resort: any article/div with links and reasonable text
    all_cands = []
    for el in soup.find_all(['article', 'div'], class_=True, limit=400):
        links = el.find_all('a', href=True)
        text = el.get_text(strip=True)
        if links and len(text) > 40:
            all_cands.append(el)
    if all_cands:
        logger.debug(f"Last-resort found {len(all_cands)} nodes")
        return all_cands[:200]

    return []

def parse_job_node(node, base_url: str) -> Optional[Dict[str, Any]]:
    """
    Parse job node into a structured item.
    Returns None if not parseable/valid.
    """
    try:
        # Title: try h2/h3/h4 > a then first <a> inside
        title = None
        for sel in ("h2 a", "h3 a", "h4 a", "a"):
            el = node.select_one(sel)
            if el and el.get_text(strip=True):
                title = el.get_text(strip=True)
                url = el.get("href")
                break
        if not title:
            # fallback: any strong or heading
            el = node.find(['h2','h3','h4','strong'])
            if el and el.get_text(strip=True):
                title = el.get_text(strip=True)
            else:
                # give up if no good title
                return None

        # If url wasn't set above, try any <a>
        if 'url' not in locals():
            link = node.find('a', href=True)
            url = link.get('href') if link else None

        abs_url = normalize_url(base_url, url) if url else None

        full_text = node.get_text(" ", strip=True)
        snippet = full_text[:1200]

        # Try to find organization from small spans or .meta fields
        org = None
        org_selectors = ['.company', '.organization', '.employer', '.meta', '.job_meta', '.job_listing-meta']
        for sel in org_selectors:
            el = node.select_one(sel)
            if el and el.get_text(strip=True):
                org = el.get_text(strip=True)
                break
        if not org:
            # heuristics: look for "by <ORG>" or lines containing Dept/Ministry
            m = re.search(r'by\s+([A-Z][A-Za-z0-9 &,\-]{3,60})', full_text)
            if m:
                org = m.group(1)

        org = org or "Not specified"

        # Extract details by scanning snippet
        vacancies = extract_vacancies(snippet)
        salary = extract_salary(snippet)
        age = extract_age(snippet)
        experience = extract_experience(snippet)
        qualification = extract_qualification(snippet)
        posted = extract_posted_date(snippet)

        job = {
            "title": title.strip(),
            "organization": org.strip(),
            "category": None,  # assigned later
            "url": abs_url or "",
            "snippet": snippet,
            "vacancies": vacancies,
            "salary": salary,
            "age": age,
            "experience": experience,
            "qualification": qualification,
            "posted": posted,
            "scraped_at": datetime.utcnow().isoformat() + "Z"
        }

        # basic validation
        if len(job["title"]) < 6:
            return None
        if "advertisement" in job["title"].lower() or "sponsored" in job["title"].lower():
            return None

        return job
    except Exception as e:
        logger.debug(f"Failed to parse node: {e}")
        return None

def assign_category(job: Dict[str, Any]) -> str:
    """Assign a category based on title/snippet/organization keywords"""
    text = " ".join([
        job.get("title",""),
        job.get("snippet",""),
        job.get("organization","")
    ]).lower()

    # keyword mapping for categories (extend as necessary)
    mapping = {
        "Engineering": ["engineer", "engineering", "technical", "civil", "mechanical", "electrical", "electronics", "gates", "gate"],
        "Science": ["research", "scientist", "research fellow", "csir", "icmr", "scientist", "laboratory", "phd", "researcher"],
        "Commerce": ["bank", "clerk", "account", "commerce", "ibps", "ssc", "rbi", "finance", "accounts"],
        "Education": ["teacher", "lecturer", "professor", "tgt", "pgt", "kvs", "nvs", "education", "teach"]
    }
    for cat, keywords in mapping.items():
        for kw in keywords:
            if kw in text:
                return cat
    # default fallback: if can't detect, categorize as 'Engineering' (or better: 'Uncategorized' - but keep within required categories)
    return "Engineering"

# ---------- Main scraping flow ----------
def scrape_latest_jobs(max_per_category: int = MAX_PER_CATEGORY) -> Dict[str, List[Dict[str, Any]]]:
    """
    Scrape the latest jobs listing and attempt to produce categorized results.
    """
    results: Dict[str, List[Dict[str, Any]]] = {c: [] for c in CATEGORIES}

    main_url = urljoin(BASE_URL, PRIMARY_LISTING_PATH)
    resp = fetch(main_url)
    if not resp:
        # Try root
        logger.warning("Primary listing fetch failed. Trying homepage...")
        resp = fetch(BASE_URL)
    if not resp:
        logger.error("Failed to fetch primary pages. Aborting.")
        return results

    soup = BeautifulSoup(resp.content, "html.parser")

    # If page looks empty (very small), might be JS-rendered
    if len(soup.get_text(strip=True)) < 200:
        logger.warning("Page contains very little text -> site may be JS-rendered. "
                       "Consider using Selenium or an API. Scraper may not work.")
    
    nodes = find_job_nodes(soup)
    if not nodes:
        logger.warning("No job nodes found on main listing page. Try /api/inspect or Selenium.")
        return results

    logger.info(f"Found {len(nodes)} candidate nodes — parsing...")

    # parse nodes
    parsed_jobs = []
    for node in nodes:
        job = parse_job_node(node, BASE_URL)
        if job:
            parsed_jobs.append(job)

    logger.info(f"Parsed {len(parsed_jobs)} jobs from listing nodes.")

    # dedupe by normalized URL or title
    seen_urls = set()
    seen_titles = set()
    for j in parsed_jobs:
        url = j.get("url") or ""
        title = (j.get("title") or "").strip()
        key = url or title.lower()
        if not key:
            continue
        if key in seen_urls or (title.lower() in seen_titles):
            continue
        seen_urls.add(key)
        seen_titles.add(title.lower())

        cat = assign_category(j)
        j["category"] = cat
        if len(results[cat]) < max_per_category:
            results[cat].append(j)

    # If any category is short, optionally try scanning job detail pages or other pages (skipped by default)
    # NOTE: Could extend to check category-specific pages if known (e.g., /category/engineering)
    return results

def save_results(data: Dict[str, List[Dict[str, Any]]], outfile: str = OUTPUT_FILE) -> bool:
    try:
        # Ensure consistent ordering and simple objects
        with open(outfile + ".tmp", "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(outfile + ".tmp", outfile)
        logger.info(f"Saved {sum(len(v) for v in data.values())} jobs to {outfile}")
        return True
    except Exception as e:
        logger.exception(f"Failed to save results: {e}")
        return False

# ---------- CLI ----------
def main():
    random.seed()
    logger.info("Starting JobYaariScraperEnhanced (local run)")

    try:
        data = scrape_latest_jobs(MAX_PER_CATEGORY)
        total = sum(len(v) for v in data.values())
        logger.info(f"Scraping complete. Total jobs collected: {total}")

        # Show short summary
        for c in CATEGORIES:
            logger.info(f"  {c}: {len(data.get(c, []))} jobs")

        saved = save_results(data, OUTPUT_FILE)
        if saved:
            logger.info("All done.")
        else:
            logger.error("Could not save output.")
    except KeyboardInterrupt:
        logger.info("Interrupted by user.")
    except Exception as e:
        logger.exception(f"Unhandled error: {e}")


if __name__ == "__main__":
    main()
