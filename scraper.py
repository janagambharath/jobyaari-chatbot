#!/usr/bin/env python3
"""
scrapper.py
Robust scraper for JobYaari -> fills knowledge_base.json

Features:
- Multi-strategy selector detection
- Qualification normalization -> Graduate/Postgraduate/Doctorate/Other
- Categorization (Engineering/Science/Commerce/Education or Uncategorized)
- Optional Selenium fallback controlled by USE_SELENIUM env var
- Deduplication, rate-limiting, atomic save
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

# Optional selenium
USE_SELENIUM = os.environ.get("USE_SELENIUM", "0") == "1"
if USE_SELENIUM:
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
    except Exception:
        USE_SELENIUM = False

# ---------- Configuration ----------
BASE_URL = os.environ.get("JOBYAARI_BASE_URL", "https://www.jobyaari.com")
PRIMARY_LISTING_PATH = os.environ.get("JOBYAARI_LISTING_PATH", "/latest-jobs")
OUTPUT_FILE = os.environ.get("KNOWLEDGE_BASE_FILE", "knowledge_base.json")
MAX_PER_CATEGORY = int(os.environ.get("MAX_PER_CATEGORY", "7"))
REQUEST_TIMEOUT = 20
MAX_RETRIES = 3
DELAY_BETWEEN_REQUESTS = (0.8, 2.0)
CATEGORIES = ["Engineering", "Science", "Commerce", "Education", "Uncategorized"]

# ---------- Logging ----------
logger = logging.getLogger("JobYaariScraper")
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
if not logger.handlers:
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
    allowed_methods=["GET"]
)
adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=10, pool_maxsize=20)
session.mount("https://", adapter)
session.mount("http://", adapter)


# ---------- Extractors ----------
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

def normalize_qualification_to_level(qualification: Optional[str]) -> str:
    if not qualification:
        return "Not specified"
    q = qualification.lower()
    if re.search(r'\b(ph\.?d|phd|doctor)\b', q):
        return "Doctorate"
    if re.search(r'\b(m\.?tech|m\.?sc|mba|master|mtech|msc)\b', q):
        return "Postgraduate"
    if re.search(r'\b(b\.?tech|b\.?sc|b\.?ed|graduate|bachelor|ba|bcom)\b', q):
        return "Graduate"
    if re.search(r'\b(diploma|certificate)\b', q):
        return "Diploma/Certificate"
    return "Other"

def extract_posted_date(text: str) -> Optional[str]:
    if not text:
        return None
    m = re.search(r'(\d+)\s*days?\s*ago', text, re.I)
    if m:
        days = int(m.group(1))
        d = datetime.utcnow() - timedelta(days=days)
        return d.date().isoformat()
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

# ---------- HTTP helpers ----------
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

def render_with_selenium(url: str, wait_seconds: int = 3) -> Optional[str]:
    if not USE_SELENIUM:
        return None
    try:
        opts = Options()
        opts.headless = True
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        driver = webdriver.Chrome(options=opts)
        driver.get(url)
        time.sleep(wait_seconds)
        html = driver.page_source
        driver.quit()
        return html
    except Exception as e:
        logger.warning(f"Selenium render failed: {e}")
        try:
            driver.quit()
        except Exception:
            pass
        return None

# ---------- Node discovery & parsing ----------
def find_job_nodes(soup: BeautifulSoup) -> List:
    CandidateSelectors = [
        "div.job_listing", "div.latest-job", "div.job-box", "div.job-item",
        "li.job", "div.job-listing", "div.listing-item", "article.post", "article"
    ]
    for sel in CandidateSelectors:
        nodes = soup.select(sel)
        if nodes and len(nodes) >= 2:
            logger.debug(f"Selector '{sel}' matched {len(nodes)} nodes")
            return nodes[:200]

    keywords = ['recruitment', 'notification', 'vacancy', 'vacancies', 'job', 'exam', 'admit', 'result', 'apply', 'posts', 'position']
    potential = []
    for a in soup.find_all('a', href=True, limit=400):
        t = a.get_text(separator=" ", strip=True)
        href = a.get('href', '')
        if not t:
            continue
        low = (t + " " + href).lower()
        if any(kw in low for kw in keywords) and len(t) > 10:
            parent = a.parent
            for _ in range(4):
                if parent is None:
                    break
                if parent.name in ('article', 'div', 'li'):
                    potential.append(parent)
                    break
                parent = parent.parent
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
        return unique[:200]

    all_cands = []
    for el in soup.find_all(['article', 'div'], class_=True, limit=400):
        links = el.find_all('a', href=True)
        text = el.get_text(strip=True)
        if links and len(text) > 40:
            all_cands.append(el)
    if all_cands:
        return all_cands[:200]

    return []

def parse_job_node(node, base_url: str) -> Optional[Dict[str, Any]]:
    try:
        title = None
        url = None
        for sel in ("h2 a", "h3 a", "h4 a", "a"):
            el = node.select_one(sel)
            if el and el.get_text(strip=True):
                title = el.get_text(strip=True)
                url = el.get("href")
                break
        if not title:
            el = node.find(['h2','h3','h4','strong'])
            if el and el.get_text(strip=True):
                title = el.get_text(strip=True)
            else:
                return None

        if not url:
            link = node.find('a', href=True)
            url = link.get('href') if link else None
        abs_url = normalize_url(base_url, url) if url else ""

        full_text = node.get_text(" ", strip=True)
        snippet = full_text[:1200]

        org = None
        org_selectors = ['.company', '.organization', '.employer', '.meta', '.job_meta', '.job_listing-meta']
        for sel in org_selectors:
            el = node.select_one(sel)
            if el and el.get_text(strip=True):
                org = el.get_text(strip=True)
                break
        if not org:
            m = re.search(r'by\s+([A-Z][A-Za-z0-9 &,\-]{3,60})', full_text)
            if m:
                org = m.group(1)
        org = org or "Not specified"

        vacancies = extract_vacancies(snippet)
        salary = extract_salary(snippet)
        age = extract_age(snippet)
        experience = extract_experience(snippet)
        qualification_raw = extract_qualification(snippet)
        qualification = normalize_qualification_to_level(qualification_raw)
        posted = extract_posted_date(snippet)

        job = {
            "title": title.strip(),
            "organization": org.strip(),
            "category": None,
            "url": abs_url,
            "snippet": snippet,
            "vacancies": vacancies,
            "salary": salary,
            "age": age,
            "experience": experience,
            "qualification_raw": qualification_raw,
            "qualification": qualification,
            "posted": posted,
            "scraped_at": datetime.utcnow().isoformat() + "Z"
        }

        if len(job["title"]) < 6:
            return None
        if "advertisement" in job["title"].lower() or "sponsored" in job["title"].lower():
            return None

        return job
    except Exception as e:
        logger.debug(f"Failed to parse node: {e}")
        return None

def assign_category(job: Dict[str, Any]) -> str:
    text = " ".join([job.get("title",""), job.get("snippet",""), job.get("organization","")]).lower()
    mapping = {
        "Engineering": ["engineer", "engineering", "technical", "civil", "mechanical", "electrical", "electronics", "gate", "gates"],
        "Science": ["research", "scientist", "research fellow", "csir", "icmr", "laboratory", "phd", "researcher"],
        "Commerce": ["bank", "clerk", "account", "commerce", "ibps", "ssc", "rbi", "finance", "accounts"],
        "Education": ["teacher", "lecturer", "professor", "tgt", "pgt", "kvs", "nvs", "education", "teach"]
    }
    for cat, keywords in mapping.items():
        for kw in keywords:
            if kw in text:
                return cat
    logger.info(f"Uncategorized: {job.get('title')[:80]}")
    return "Uncategorized"

# ---------- Main ----------
def scrape_latest_jobs(max_per_category: int = MAX_PER_CATEGORY) -> Dict[str, List[Dict[str, Any]]]:
    results: Dict[str, List[Dict[str, Any]]] = {c: [] for c in CATEGORIES}
    main_url = urljoin(BASE_URL, PRIMARY_LISTING_PATH)
    resp = fetch(main_url)
    if not resp:
        logger.warning("Primary listing fetch failed - trying homepage")
        resp = fetch(BASE_URL)
    if not resp:
        logger.error("Failed to fetch listing pages")
        return results

    soup = BeautifulSoup(resp.content, "html.parser")
    if len(soup.get_text(strip=True)) < 200 and USE_SELENIUM:
        logger.info("Page seems JS-rendered; rendering with Selenium")
        html = render_with_selenium(main_url)
        if html:
            soup = BeautifulSoup(html, "html.parser")

    nodes = find_job_nodes(soup)
    if not nodes:
        logger.warning("No candidate nodes found on listing page")
        return results

    parsed_jobs = []
    for node in nodes:
        job = parse_job_node(node, BASE_URL)
        if job:
            parsed_jobs.append(job)
        time.sleep(random.uniform(*DELAY_BETWEEN_REQUESTS))

    # dedupe & categorize
    seen_keys = set()
    for j in parsed_jobs:
        key = (j.get("url") or "").strip() or j.get("title","").strip().lower()
        if not key or key in seen_keys:
            continue
        seen_keys.add(key)
        cat = assign_category(j)
        j["category"] = cat
        if len(results.get(cat, [])) < max_per_category:
            results.setdefault(cat, []).append(j)

    # Ensure each category exists
    for c in CATEGORIES:
        results.setdefault(c, [])

    return results

def save_results(data: Dict[str, List[Dict[str, Any]]], outfile: str = OUTPUT_FILE) -> bool:
    try:
        tmp = outfile + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp, outfile)
        logger.info(f"Saved {sum(len(v) for v in data.values())} jobs to {outfile}")
        return True
    except Exception as e:
        logger.exception(f"Failed to save results: {e}")
        return False

def main():
    random.seed()
    logger.info("Starting scrapper.py")
    data = scrape_latest_jobs(MAX_PER_CATEGORY)
    total = sum(len(v) for v in data.values())
    logger.info(f"Collected {total} jobs")
    for c in CATEGORIES:
        logger.info(f" {c}: {len(data.get(c, []))}")
    saved = save_results(data, OUTPUT_FILE)
    if saved:
        logger.info("Done")
    else:
        logger.error("Save failed")

if __name__ == "__main__":
    main()
