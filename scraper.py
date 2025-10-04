"""
Enhanced Production-Grade Scraper for JobYaari.com (or similar job sites)

Features:
- Robust requests.Session with retries & backoff
- Multiple CSS selector fallbacks
- URL-based deduplication
- Reasonable defaults: max_jobs per category, polite delay between requests
- Structured job dict output
- Logging for better observability
- Simple, safe parsing helpers (salary, vacancies, age, experience, posted_date)
- NOTE: Always check and respect robots.txt and site terms before scraping
"""

import re
import json
import time
import logging
from datetime import datetime, timedelta
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger("JobYaariScraper")
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger.addHandler(ch)


class JobYaariScraperEnhanced:
    def __init__(self, base_url="https://www.jobyaari.com", timeout=15, max_retries=3, backoff_factor=0.5):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Referer": "https://www.google.com/"
        })

        # Configure robust retry strategy on the session.
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=backoff_factor,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(["GET", "POST"])
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    # -------------------------------
    # Networking helpers
    # -------------------------------
    def fetch_page(self, url):
        try:
            resp = self.session.get(url, timeout=self.timeout)
            resp.raise_for_status()
            return resp
        except requests.exceptions.RequestException as e:
            logger.warning(f"[fetch_page] Failed to fetch {url}: {e}")
            return None

    # -------------------------------
    # Public scraping API
    # -------------------------------
    def scrape_category(self, category, category_url=None, max_jobs=7):
        """
        Scrape jobs for a single category.
        - category: friendly category name (e.g., "Engineering")
        - category_url: optional explicit URL (if None, builds /category/<slug>)
        - max_jobs: max jobs to return
        """
        slug = self._slugify(category)
        if category_url:
            url = category_url
        else:
            url = f"{self.base_url}/category/{slug}"

        logger.info(f"[scrape_category] Fetching category '{category}' => {url}")
        resp = self.fetch_page(url)
        if not resp:
            logger.error(f"[scrape_category] Could not fetch category page: {url}")
            return []

        soup = BeautifulSoup(resp.content, "html.parser")

        # Candidate selectors covering many WP/listing themes
        selectors = [
            "article.post", "div.job-listing-item", "li.job", "div.job-listing", "div.card-body",
            "div[class*='job-'], div[class*='job_'], article[class*='job']",
            ".entry-list .entry, .loop-item"
        ]

        job_nodes = []
        for sel in selectors:
            nodes = soup.select(sel)
            if nodes:
                job_nodes = nodes
                logger.debug(f"[scrape_category] Selected {len(nodes)} nodes with selector '{sel}'")
                break

        # Fallback: any article or link blocks
        if not job_nodes:
            job_nodes = soup.select("article, li, div")
            logger.debug(f"[scrape_category] Fallback selected {len(job_nodes)} generic nodes")

        jobs = []
        seen_urls = set()
        for node in job_nodes:
            if len(jobs) >= max_jobs:
                break
            try:
                job = self._parse_job_node(node, category)
                if not job:
                    continue
                # normalize URL and dedupe
                job_url = job.get("url", "").strip()
                if job_url:
                    job_url = self._normalize_url(job_url)
                else:
                    job_url = None

                # prefer canonical absolute url
                if job_url and job_url in seen_urls:
                    continue
                if job_url:
                    seen_urls.add(job_url)

                # final validation
                if self._is_valid_job(job):
                    job["url"] = job_url or job.get("url") or self.base_url
                    jobs.append(job)
                    logger.info(f"  [+] {job['title']} — {job['organization']} ({job['url']})")
            except Exception as e:
                logger.exception(f"[scrape_category] error parsing node: {e}")
                continue

        logger.info(f"[scrape_category] Found {len(jobs)} jobs in '{category}' (max {max_jobs}).")
        return jobs

    def scrape_all_categories(self, categories=None, max_jobs=7, delay=2):
        """
        Scrape a list of categories. Returns dict: {category: [jobs...]}
        - delay: seconds to wait between category requests (politeness)
        """
        if categories is None:
            categories = ["Engineering", "Science", "Commerce", "Education"]

        results = {}
        for cat in categories:
            results[cat] = self.scrape_category(cat, max_jobs=max_jobs)
            time.sleep(delay)
        return results

    # -------------------------------
    # Node parsing
    # -------------------------------
    def _parse_job_node(self, node, category):
        """Attempts to build a structured job dict from an HTML node."""
        title = self._extract_text(node, ["h2 a", "h2", "h3 a", "h3", ".job-title", ".entry-title", "a.title"])
        if not title:
            return None

        url = self._extract_url_from_node(node) or self._extract_attr(node, "a[href]")
        # organization/company
        organization = self._extract_text(node, [".company-name", ".org", ".organization", ".company", ".meta .company"]) or "Not specified"

        snippet = node.get_text(separator=" ", strip=True)

        job = {
            "title": title.strip(),
            "organization": organization.strip(),
            "category": category,
            "url": urljoin(self.base_url, url) if url else None,
            "snippet": snippet[:800],
            "vacancies": self._extract_vacancies(snippet),
            "salary": self._extract_salary(snippet),
            "age": self._extract_age(snippet),
            "experience": self._extract_experience(snippet),
            "qualification": self._extract_qualification(snippet),
            "location": self._extract_location(snippet),
            "posted": self._extract_posted_date(snippet),
            "scraped_at": datetime.utcnow().isoformat() + "Z"
        }
        return job

    # -------------------------------
    # Small extractor helpers
    # -------------------------------
    def _extract_text(self, node, selectors):
        for sel in selectors:
            el = node.select_one(sel)
            if el:
                return el.get_text(strip=True)
        return None

    def _extract_attr(self, node, selector_with_attr):
        # e.g., "a[href]" or "img[src]"
        m = re.match(r"(.+)\[([a-zA-Z_-]+)\]$", selector_with_attr)
        if not m:
            return None
        sel, attr = m.group(1).strip(), m.group(2).strip()
        el = node.select_one(sel)
        if el and el.has_attr(attr):
            return el[attr]
        return None

    def _extract_url_from_node(self, node):
        # Prefer explicit link inside title
        title_link = node.select_one("h2 a, h3 a, a.title, .job-title a, .entry-title a")
        if title_link and title_link.has_attr("href"):
            return title_link["href"]
        # fallback first anchor
        a = node.find("a", href=True)
        if a:
            return a["href"]
        return None

    def _normalize_url(self, url):
        if not url:
            return None
        # absolute
        return urljoin(self.base_url, url.strip())

    def _extract_vacancies(self, text):
        text = text or ""
        # common patterns: "2 posts", "3 vacancies", "vacancy: 10"
        m = re.search(r'(\d{1,3})\s*(?:posts?|vacancies|vacancy|positions?)', text, re.I)
        if m:
            return int(m.group(1))
        # sometimes "Multiple" or "Not specified"
        return None

    def _extract_salary(self, text):
        text = text or ""
        # capture ranges like "Rs. 50,000 - 60,000" or "₹40,000"
        m = re.search(r'((?:₹|Rs\.?|INR)\s*[\d,]+(?:\s*-\s*(?:₹|Rs\.?|INR)?\s*[\d,]+)?)', text, re.I)
        if m:
            return m.group(1).strip()
        return None

    def _extract_age(self, text):
        text = text or ""
        m = re.search(r'(\d{1,2}\s*-\s*\d{1,2}\s*years|\d{1,2}\s*years)', text, re.I)
        if m:
            return m.group(1).strip()
        return None

    def _extract_experience(self, text):
        text = text or ""
        if re.search(r'\bfresher\b', text, re.I):
            return "Fresher"
        # "2 years", "2+ years", "minimum 3 years"
        m = re.search(r'(?:minimum|at least)?\s*(\d{1,2}(?:\+)?\s*(?:years?|yrs?))', text, re.I)
        if m:
            return m.group(1).strip()
        return None

    def _extract_qualification(self, text):
        text = text or ""
        quals = ["ph\.?d", "m\.?tech", "b\.?tech", "m\.?e", "b\.?e", "m\.?sc", "b\.?sc", "mba", "mca", "m\.?com", "b\.?com", "b\.?ed", "m\.?ed", "diploma", "graduate", "postgraduate"]
        for q in quals:
            if re.search(r'\b' + q + r'\b', text, re.I):
                # return normalized uppercase-ish form
                return re.sub(r'\.?', '', q).upper().replace("\\", "")
        return None

    def _extract_location(self, text):
        text = text or ""
        # naive: look for word followed by "— City" or "at City" patterns (imperfect but useful)
        m = re.search(r'(?:Location[:\s]*|at\s+|in\s+)([A-Z][A-Za-z &,-]{2,40})', text)
        if m:
            return m.group(1).strip()
        return None

    def _extract_posted_date(self, text):
        text = text or ""
        # patterns: "Posted 3 days ago", "Posted on 02-08-2025", "02 Aug 2025"
        m = re.search(r'posted\s*(?:on)?\s*[:\-]?\s*(\d{1,2}[\/\-\s][A-Za-z0-9]{1,}\s*\d{2,4}|\d{1,2}\s*days?\s*ago|\d+\s*hours?\s*ago)', text, re.I)
        if m:
            posted_raw = m.group(1).strip()
            # handle "X days ago"
            days = re.match(r'(\d+)\s*days?\s*ago', posted_raw, re.I)
            if days:
                return (datetime.utcnow() - timedelta(days=int(days.group(1)))).date().isoformat()
            hours = re.match(r'(\d+)\s*hours?\s*ago', posted_raw, re.I)
            if hours:
                return (datetime.utcnow() - timedelta(hours=int(hours.group(1)))).isoformat()
            # try parse common formats (DD-MM-YYYY, DD MMM YYYY, etc.)
            for fmt in ("%d-%m-%Y", "%d/%m/%Y", "%d %b %Y", "%d %B %Y", "%Y-%m-%d"):
                try:
                    dt = datetime.strptime(posted_raw, fmt)
                    return dt.date().isoformat()
                except Exception:
                    continue
            # fallback to raw string
            return posted_raw
        # fallback None => unknown
        return None

    # -------------------------------
    # Utilities
    # -------------------------------
    def _is_valid_job(self, job):
        return bool(job.get("title")) and (job.get("url") or len(job.get("title", "")) > 5)

    def _slugify(self, text):
        s = re.sub(r'[^a-z0-9]+', '-', text.lower())
        return s.strip('-')

# -------------------------------
# CLI / quick test
# -------------------------------
if __name__ == "__main__":
    scraper = JobYaariScraperEnhanced()
    cats = ["Engineering", "Science", "Commerce", "Education"]
    data = scraper.scrape_all_categories(categories=cats, max_jobs=7, delay=2)

    total = sum(len(v) for v in data.values())
    logger.info(f"Scraping complete. Total jobs: {total}")

    # Print samples
    for cat, jobs in data.items():
        logger.info(f"Category: {cat} — {len(jobs)} jobs")
        for j in jobs[:2]:
            logger.info(f"  - {j['title'][:60]} | {j.get('organization')} | {j.get('url')}")

    # Save to JSON
    with open("knowledge_base.json", "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    logger.info("Saved results to knowledge_base.json")
