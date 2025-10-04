"""
Production-Grade JobYaari Scraper with Enhanced Network Resilience

Features:
- Circuit breaker pattern for failing endpoints
- Exponential backoff with jitter
- Connection pooling and keep-alive
- Comprehensive error handling
- Graceful degradation
- Request rate limiting
"""

import re
import json
import time
import random
import logging
from datetime import datetime, timedelta
from urllib.parse import urljoin, urlparse
from typing import Dict, List, Optional, Any

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger("JobYaariScraper")
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger.addHandler(ch)


class CircuitBreaker:
    """Simple circuit breaker to avoid hammering failing endpoints"""
    
    def __init__(self, failure_threshold=3, timeout=60):
        self.failure_count = 0
        self.failure_threshold = failure_threshold
        self.timeout = timeout
        self.last_failure_time = None
        self.state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN
    
    def call_failed(self):
        self.failure_count += 1
        self.last_failure_time = time.time()
        
        if self.failure_count >= self.failure_threshold:
            self.state = "OPEN"
            logger.warning(f"Circuit breaker OPEN after {self.failure_count} failures")
    
    def call_succeeded(self):
        self.failure_count = 0
        self.state = "CLOSED"
    
    def can_attempt(self) -> bool:
        if self.state == "CLOSED":
            return True
        
        if self.state == "OPEN":
            if time.time() - self.last_failure_time > self.timeout:
                self.state = "HALF_OPEN"
                logger.info("Circuit breaker HALF_OPEN - attempting recovery")
                return True
            return False
        
        # HALF_OPEN
        return True


class JobYaariScraperEnhanced:
    """Production-ready scraper with network resilience"""
    
    def __init__(self, base_url="https://www.jobyaari.com", timeout=20, max_retries=3, backoff_factor=1.0):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.circuit_breaker = CircuitBreaker(failure_threshold=3, timeout=120)
        
        # Enhanced session configuration
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Cache-Control": "max-age=0"
        })
        
        # Configure retry strategy with exponential backoff
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST", "HEAD"],
            raise_on_status=False  # Don't raise on retry exhaustion
        )
        
        # Use connection pooling
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=10,
            pool_maxsize=20,
            pool_block=False
        )
        
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
        
        # Request tracking
        self.request_count = 0
        self.failed_requests = 0
    
    def fetch_page(self, url: str, retry_count=0) -> Optional[requests.Response]:
        """Fetch page with circuit breaker and exponential backoff"""
        
        # Check circuit breaker
        if not self.circuit_breaker.can_attempt():
            logger.warning(f"Circuit breaker OPEN - skipping {url}")
            return None
        
        try:
            # Add jitter to prevent thundering herd
            if retry_count > 0:
                jitter = random.uniform(0, 0.5)
                sleep_time = (2 ** retry_count) + jitter
                logger.debug(f"Retry {retry_count} - sleeping {sleep_time:.2f}s")
                time.sleep(sleep_time)
            
            # Make request
            self.request_count += 1
            logger.info(f"Fetching: {url} (attempt {retry_count + 1})")
            
            resp = self.session.get(
                url,
                timeout=self.timeout,
                allow_redirects=True,
                verify=True  # SSL verification
            )
            
            # Check response
            if resp.status_code == 200:
                self.circuit_breaker.call_succeeded()
                return resp
            
            elif resp.status_code == 429:  # Rate limited
                logger.warning(f"Rate limited (429) on {url}")
                retry_after = int(resp.headers.get("Retry-After", 10))
                time.sleep(retry_after)
                if retry_count < 2:
                    return self.fetch_page(url, retry_count + 1)
            
            elif resp.status_code >= 500:  # Server error
                logger.warning(f"Server error {resp.status_code} on {url}")
                if retry_count < 2:
                    return self.fetch_page(url, retry_count + 1)
            
            else:
                logger.warning(f"HTTP {resp.status_code} on {url}")
            
            self.circuit_breaker.call_failed()
            self.failed_requests += 1
            return None
        
        except requests.exceptions.Timeout as e:
            logger.warning(f"Timeout on {url}: {e}")
            self.circuit_breaker.call_failed()
            self.failed_requests += 1
            if retry_count < 2:
                return self.fetch_page(url, retry_count + 1)
            return None
        
        except requests.exceptions.ConnectionError as e:
            logger.warning(f"Connection error on {url}: {e}")
            self.circuit_breaker.call_failed()
            self.failed_requests += 1
            if retry_count < 1:  # Fewer retries on connection errors
                return self.fetch_page(url, retry_count + 1)
            return None
        
        except requests.exceptions.RequestException as e:
            logger.warning(f"Request error on {url}: {e}")
            self.circuit_breaker.call_failed()
            self.failed_requests += 1
            return None
        
        except Exception as e:
            logger.exception(f"Unexpected error fetching {url}: {e}")
            self.circuit_breaker.call_failed()
            self.failed_requests += 1
            return None
    
    def scrape_category(self, category: str, category_url: Optional[str] = None, max_jobs: int = 7) -> List[Dict]:
        """Scrape a single category with robust error handling"""
        
        slug = self._slugify(category)
        url = category_url or f"{self.base_url}/category/{slug}"
        
        logger.info(f"Scraping category: {category} => {url}")
        
        # Fetch page
        resp = self.fetch_page(url)
        if not resp or not resp.content:
            logger.error(f"Failed to fetch category page: {url}")
            return []
        
        # Parse HTML
        try:
            soup = BeautifulSoup(resp.content, "html.parser")
        except Exception as e:
            logger.exception(f"HTML parsing error for {url}: {e}")
            return []
        
        # Try multiple selectors
        job_nodes = self._find_job_nodes(soup)
        
        if not job_nodes:
            logger.warning(f"No job nodes found for {category}")
            return []
        
        logger.info(f"Found {len(job_nodes)} potential job nodes")
        
        # Parse jobs
        jobs = []
        seen_urls = set()
        
        for idx, node in enumerate(job_nodes):
            if len(jobs) >= max_jobs:
                break
            
            try:
                job = self._parse_job_node(node, category)
                
                if not job or not self._is_valid_job(job):
                    continue
                
                # Deduplicate by URL
                job_url = self._normalize_url(job.get("url", ""))
                if job_url and job_url in seen_urls:
                    continue
                
                if job_url:
                    seen_urls.add(job_url)
                    job["url"] = job_url
                
                jobs.append(job)
                logger.info(f"  ✓ [{len(jobs)}] {job['title'][:60]} - {job['organization']}")
            
            except Exception as e:
                logger.exception(f"Error parsing job node {idx}: {e}")
                continue
        
        logger.info(f"Successfully scraped {len(jobs)} jobs for {category}")
        return jobs
    
    def scrape_all_categories(self, categories: Optional[List[str]] = None, max_jobs: int = 7, delay: int = 3) -> Dict[str, List[Dict]]:
        """Scrape all categories with rate limiting"""
        
        if categories is None:
            categories = ["Engineering", "Science", "Commerce", "Education"]
        
        results = {}
        
        for idx, cat in enumerate(categories):
            logger.info(f"\n{'='*60}")
            logger.info(f"Category {idx + 1}/{len(categories)}: {cat}")
            logger.info(f"{'='*60}")
            
            try:
                results[cat] = self.scrape_category(cat, max_jobs=max_jobs)
            except Exception as e:
                logger.exception(f"Failed to scrape category {cat}: {e}")
                results[cat] = []
            
            # Polite delay between categories
            if idx < len(categories) - 1:
                logger.info(f"Waiting {delay}s before next category...")
                time.sleep(delay)
        
        # Summary
        total = sum(len(v) for v in results.values())
        logger.info(f"\n{'='*60}")
        logger.info(f"Scraping Complete!")
        logger.info(f"Total requests: {self.request_count}")
        logger.info(f"Failed requests: {self.failed_requests}")
        logger.info(f"Total jobs: {total}")
        logger.info(f"{'='*60}\n")
        
        return results
    
    def _find_job_nodes(self, soup: BeautifulSoup) -> List:
        """Find job nodes using multiple selector strategies"""
        
        # Priority selector list (most specific to least specific)
        selectors = [
            "article.post",
            "div.job-listing",
            "div.job-item",
            "li.job",
            "div[class*='job-']",
            "article[class*='job']",
            ".entry-list .entry",
            "div.card",
            "article",
            "li"
        ]
        
        for selector in selectors:
            try:
                nodes = soup.select(selector)
                if nodes and len(nodes) > 2:  # Need at least 3 nodes
                    logger.debug(f"Using selector: {selector} ({len(nodes)} nodes)")
                    return nodes[:50]  # Limit to first 50
            except Exception as e:
                logger.debug(f"Selector {selector} failed: {e}")
                continue
        
        logger.warning("No suitable selector found - using fallback")
        return soup.select("div, article, li")[:50]
    
    def _parse_job_node(self, node, category: str) -> Optional[Dict]:
        """Parse a single job node into structured data"""
        
        # Extract title (required)
        title = self._extract_text(node, [
            "h2 a", "h2", "h3 a", "h3", "h4 a", "h4",
            ".job-title", ".title", ".entry-title",
            "a.post-title", ".post-title"
        ])
        
        if not title or len(title) < 5:
            return None
        
        # Extract URL
        url = self._extract_url_from_node(node)
        
        # Extract organization
        organization = self._extract_text(node, [
            ".company", ".company-name", ".organization",
            ".org", ".employer", ".meta .company",
            "span.company", "div.company"
        ]) or "Not specified"
        
        # Get full text for pattern matching
        snippet = node.get_text(separator=" ", strip=True)
        
        # Build job dictionary
        job = {
            "title": title.strip(),
            "organization": organization.strip(),
            "category": category,
            "url": urljoin(self.base_url, url) if url else self.base_url,
            "snippet": snippet[:1000],
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
    
    # -------------------------
    # Extraction Helpers
    # -------------------------
    
    def _extract_text(self, node, selectors: List[str]) -> Optional[str]:
        """Extract text from first matching selector"""
        for sel in selectors:
            try:
                el = node.select_one(sel)
                if el:
                    text = el.get_text(strip=True)
                    if text:
                        return text
            except Exception:
                continue
        return None
    
    def _extract_url_from_node(self, node) -> Optional[str]:
        """Extract job URL from node"""
        
        # Try title link first
        for sel in ["h2 a", "h3 a", "h4 a", ".title a", ".job-title a", "a.post-title"]:
            try:
                link = node.select_one(sel)
                if link and link.get("href"):
                    return link["href"]
            except Exception:
                continue
        
        # Fallback to first link
        try:
            link = node.find("a", href=True)
            if link:
                return link["href"]
        except Exception:
            pass
        
        return None
    
    def _normalize_url(self, url: str) -> Optional[str]:
        """Normalize URL to absolute form"""
        if not url:
            return None
        
        try:
            url = url.strip()
            # Make absolute
            abs_url = urljoin(self.base_url, url)
            # Parse and validate
            parsed = urlparse(abs_url)
            if parsed.scheme and parsed.netloc:
                return abs_url
        except Exception as e:
            logger.debug(f"URL normalization failed for {url}: {e}")
        
        return None
    
    def _extract_vacancies(self, text: str) -> Optional[str]:
        """Extract vacancy count"""
        if not text:
            return None
        
        # Pattern: "50 posts", "3 vacancies", etc.
        patterns = [
            r'(\d{1,4})\s*(?:posts?|vacancies|vacancy|positions?|openings?)',
            r'(?:vacancy|vacancies|posts?)[:\s]+(\d{1,4})'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.I)
            if match:
                return match.group(1)
        
        # Check for "Multiple"
        if re.search(r'\bmultiple\b', text, re.I):
            return "Multiple"
        
        return None
    
    def _extract_salary(self, text: str) -> Optional[str]:
        """Extract salary information"""
        if not text:
            return None
        
        # Patterns for Indian currency
        patterns = [
            r'(?:₹|Rs\.?|INR)\s*[\d,]+(?:\s*-\s*(?:₹|Rs\.?|INR)?\s*[\d,]+)?(?:/month|/annum)?',
            r'[\d,]+\s*-\s*[\d,]+\s*(?:per month|pm|monthly)'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.I)
            if match:
                return match.group(0).strip()
        
        return None
    
    def _extract_age(self, text: str) -> Optional[str]:
        """Extract age limit"""
        if not text:
            return None
        
        patterns = [
            r'(\d{1,2}\s*-\s*\d{1,2}\s*years?)',
            r'(?:age|up to)[:\s]+(\d{1,2}\s*years?)',
            r'(\d{1,2})\s*to\s*(\d{1,2})\s*years?'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.I)
            if match:
                return match.group(0).strip()
        
        return None
    
    def _extract_experience(self, text: str) -> Optional[str]:
        """Extract experience requirement"""
        if not text:
            return None
        
        # Check for fresher
        if re.search(r'\b(?:fresher|no experience|0 years?)\b', text, re.I):
            return "Fresher"
        
        # Experience patterns
        patterns = [
            r'(?:minimum|at least|min\.?)[:\s]*(\d{1,2}[+]?\s*(?:years?|yrs?))',
            r'(\d{1,2}[+]?)\s*(?:years?|yrs?)\s*(?:of\s*)?(?:experience|exp\.?)'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.I)
            if match:
                return match.group(1).strip()
        
        return None
    
    def _extract_qualification(self, text: str) -> Optional[str]:
        """Extract qualification requirement"""
        if not text:
            return None
        
        # Common Indian qualifications
        qualifications = [
            (r'\bph\.?d\b', "Ph.D"),
            (r'\bm\.?tech\b', "M.Tech"),
            (r'\bb\.?tech\b', "B.Tech"),
            (r'\bm\.?e\.?\b', "M.E"),
            (r'\bb\.?e\.?\b', "B.E"),
            (r'\bm\.?sc\b', "M.Sc"),
            (r'\bb\.?sc\b', "B.Sc"),
            (r'\bmba\b', "MBA"),
            (r'\bmca\b', "MCA"),
            (r'\bb\.?ed\b', "B.Ed"),
            (r'\bm\.?ed\b', "M.Ed"),
            (r'\bm\.?com\b', "M.Com"),
            (r'\bb\.?com\b', "B.Com"),
            (r'\bgraduate\b', "Graduate"),
            (r'\bpostgraduate\b', "Postgraduate"),
            (r'\bdiploma\b', "Diploma")
        ]
        
        for pattern, qual_name in qualifications:
            if re.search(pattern, text, re.I):
                return qual_name
        
        return None
    
    def _extract_location(self, text: str) -> Optional[str]:
        """Extract job location"""
        if not text:
            return None
        
        patterns = [
            r'(?:Location|Place)[:\s]+([A-Z][a-zA-Z\s,]{2,40})',
            r'(?:at|in)\s+([A-Z][a-zA-Z\s]{2,30}),?\s+(?:India|IN)'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                return match.group(1).strip()
        
        return None
    
    def _extract_posted_date(self, text: str) -> Optional[str]:
        """Extract posting date"""
        if not text:
            return None
        
        # "X days ago" pattern
        days_match = re.search(r'(\d+)\s*days?\s*ago', text, re.I)
        if days_match:
            days = int(days_match.group(1))
            date = datetime.utcnow() - timedelta(days=days)
            return date.date().isoformat()
        
        # "X hours ago" pattern
        hours_match = re.search(r'(\d+)\s*hours?\s*ago', text, re.I)
        if hours_match:
            hours = int(hours_match.group(1))
            date = datetime.utcnow() - timedelta(hours=hours)
            return date.date().isoformat()
        
        # Date patterns
        date_match = re.search(
            r'(?:posted\s*(?:on)?)[:\s]*(\d{1,2}[-/]\d{1,2}[-/]\d{2,4})',
            text, re.I
        )
        if date_match:
            date_str = date_match.group(1)
            # Try to parse
            for fmt in ["%d-%m-%Y", "%d/%m/%Y", "%d-%m-%y", "%d/%m/%y"]:
                try:
                    dt = datetime.strptime(date_str, fmt)
                    return dt.date().isoformat()
                except Exception:
                    continue
            return date_str
        
        return None
    
    # -------------------------
    # Validation
    # -------------------------
    
    def _is_valid_job(self, job: Optional[Dict]) -> bool:
        """Validate job data"""
        if not job:
            return False
        
        title = job.get("title", "")
        url = job.get("url", "")
        
        # Must have title
        if not title or len(title) < 5:
            return False
        
        # Filter out common non-job patterns
        spam_patterns = [
            r'\badvertisement\b',
            r'\bsponsored\b',
            r'\bpromotion\b',
            r'^share\b',
            r'^follow\b'
        ]
        
        for pattern in spam_patterns:
            if re.search(pattern, title, re.I):
                return False
        
        return True
    
    def _slugify(self, text: str) -> str:
        """Convert text to URL slug"""
        text = text.lower()
        text = re.sub(r'[^a-z0-9]+', '-', text)
        return text.strip('-')


# -------------------------
# CLI Test
# -------------------------
if __name__ == "__main__":
    logger.info("Starting JobYaari Scraper Test")
    
    scraper = JobYaariScraperEnhanced(timeout=20, max_retries=3)
    
    categories = ["Engineering", "Science", "Commerce", "Education"]
    
    try:
        data = scraper.scrape_all_categories(
            categories=categories,
            max_jobs=7,
            delay=3
        )
        
        # Display results
        total = sum(len(v) for v in data.values())
        print(f"\n{'='*60}")
        print(f"SCRAPING RESULTS")
        print(f"{'='*60}")
        print(f"Total Jobs: {total}")
        print(f"{'='*60}\n")
        
        for cat, jobs in data.items():
            print(f"\n{cat}: {len(jobs)} jobs")
            print("-" * 60)
            for idx, job in enumerate(jobs, 1):
                print(f"{idx}. {job['title'][:70]}")
                print(f"   Org: {job['organization']}")
                print(f"   URL: {job['url']}")
                if job.get('salary'):
                    print(f"   Salary: {job['salary']}")
                print()
        
        # Save to file
        output_file = "knowledge_base.json"
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        print(f"\n✓ Saved to {output_file}")
    
    except KeyboardInterrupt:
        print("\n\nScraping interrupted by user")
    except Exception as e:
        logger.exception(f"Scraping failed: {e}")
