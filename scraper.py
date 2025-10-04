"""
FIXED JobYaari Scraper - Better selector detection
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
        self.state = "CLOSED"
    
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
        
        return True


class JobYaariScraperEnhanced:
    """Fixed scraper with improved selector detection"""
    
    def __init__(self, base_url="https://www.jobyaari.com", timeout=20, max_retries=3, backoff_factor=1.0):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.circuit_breaker = CircuitBreaker(failure_threshold=3, timeout=120)
        
        # Enhanced session
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
        })
        
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
            raise_on_status=False
        )
        
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=10,
            pool_maxsize=20,
        )
        
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
        
        self.request_count = 0
        self.failed_requests = 0
    
    def fetch_page(self, url: str, retry_count=0) -> Optional[requests.Response]:
        """Fetch page with circuit breaker"""
        
        if not self.circuit_breaker.can_attempt():
            logger.warning(f"Circuit breaker OPEN - skipping {url}")
            return None
        
        try:
            if retry_count > 0:
                jitter = random.uniform(0, 0.5)
                sleep_time = (2 ** retry_count) + jitter
                time.sleep(sleep_time)
            
            self.request_count += 1
            logger.info(f"Fetching: {url} (attempt {retry_count + 1})")
            
            resp = self.session.get(url, timeout=self.timeout, allow_redirects=True)
            
            if resp.status_code == 200:
                self.circuit_breaker.call_succeeded()
                return resp
            elif resp.status_code == 429:
                logger.warning(f"Rate limited (429)")
                time.sleep(10)
                if retry_count < 2:
                    return self.fetch_page(url, retry_count + 1)
            elif resp.status_code >= 500:
                logger.warning(f"Server error {resp.status_code}")
                if retry_count < 2:
                    return self.fetch_page(url, retry_count + 1)
            
            self.circuit_breaker.call_failed()
            self.failed_requests += 1
            return None
        
        except Exception as e:
            logger.warning(f"Error fetching {url}: {e}")
            self.circuit_breaker.call_failed()
            self.failed_requests += 1
            if retry_count < 1:
                return self.fetch_page(url, retry_count + 1)
            return None
    
    def scrape_category(self, category: str, category_url: Optional[str] = None, max_jobs: int = 7) -> List[Dict]:
        """Scrape a single category"""
        
        slug = self._slugify(category)
        url = category_url or f"{self.base_url}/category/{slug}"
        
        logger.info(f"Scraping category: {category} => {url}")
        
        resp = self.fetch_page(url)
        if not resp or not resp.content:
            logger.error(f"Failed to fetch: {url}")
            return []
        
        try:
            soup = BeautifulSoup(resp.content, "html.parser")
        except Exception as e:
            logger.exception(f"HTML parsing error: {e}")
            return []
        
        # IMPROVED: Try to find job nodes intelligently
        job_nodes = self._find_job_nodes_smart(soup)
        
        if not job_nodes:
            logger.warning(f"No job nodes found for {category}")
            return []
        
        logger.info(f"Found {len(job_nodes)} potential job nodes")
        
        jobs = []
        seen_urls = set()
        
        for idx, node in enumerate(job_nodes):
            if len(jobs) >= max_jobs:
                break
            
            try:
                job = self._parse_job_node(node, category)
                
                if not job or not self._is_valid_job(job):
                    continue
                
                job_url = self._normalize_url(job.get("url", ""))
                if job_url and job_url in seen_urls:
                    continue
                
                if job_url:
                    seen_urls.add(job_url)
                    job["url"] = job_url
                
                jobs.append(job)
                logger.info(f"  ✓ [{len(jobs)}] {job['title'][:60]}")
            
            except Exception as e:
                logger.debug(f"Error parsing job node {idx}: {e}")
                continue
        
        logger.info(f"Successfully scraped {len(jobs)} jobs for {category}")
        return jobs
    
    def scrape_all_categories(self, categories: Optional[List[str]] = None, max_jobs: int = 7, delay: int = 3) -> Dict[str, List[Dict]]:
        """Scrape all categories"""
        
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
                logger.exception(f"Failed to scrape {cat}: {e}")
                results[cat] = []
            
            if idx < len(categories) - 1:
                logger.info(f"Waiting {delay}s...")
                time.sleep(delay)
        
        total = sum(len(v) for v in results.values())
        logger.info(f"\n{'='*60}")
        logger.info(f"Scraping Complete!")
        logger.info(f"Total requests: {self.request_count}")
        logger.info(f"Failed requests: {self.failed_requests}")
        logger.info(f"Total jobs: {total}")
        logger.info(f"{'='*60}\n")
        
        return results
    
    def _find_job_nodes_smart(self, soup: BeautifulSoup) -> List:
        """IMPROVED: Intelligently find job nodes"""
        
        # Strategy 1: Look for links with job-related keywords
        logger.debug("Strategy 1: Finding links with job keywords...")
        
        all_links = soup.find_all('a', href=True, limit=100)
        potential_containers = []
        
        for link in all_links:
            text = link.get_text(strip=True)
            href = link.get('href', '')
            
            # Check if this looks like a job
            if len(text) > 15:  # Must have substantial text
                keywords = ['recruitment', 'notification', 'vacancy', 'job', 'exam', 
                           'admit', 'result', 'apply', '2024', '2025', 'posts', 'positions']
                
                if any(kw in text.lower() or kw in href.lower() for kw in keywords):
                    # Get the parent container
                    parent = link.parent
                    for _ in range(3):  # Go up max 3 levels
                        if parent and parent.name in ['article', 'div', 'li']:
                            potential_containers.append(parent)
                            break
                        parent = parent.parent if parent else None
        
        # Remove duplicates
        unique_containers = list(dict.fromkeys(potential_containers))
        
        if len(unique_containers) >= 3:
            logger.debug(f"Strategy 1: Found {len(unique_containers)} containers")
            return unique_containers[:50]
        
        # Strategy 2: Try common selectors
        logger.debug("Strategy 1 failed. Trying Strategy 2: Common selectors...")
        
        selectors = [
            "article",
            "article.post",
            "div.post",
            "li.post",
            "div.entry",
            "article.entry",
            "div[class*='post']",
            "div[class*='entry']",
            "article[class*='post']",
        ]
        
        for selector in selectors:
            try:
                nodes = soup.select(selector)
                if len(nodes) >= 3:
                    logger.debug(f"Strategy 2: Using selector '{selector}' ({len(nodes)} nodes)")
                    return nodes[:50]
            except Exception:
                continue
        
        # Strategy 3: Last resort - all articles and divs with classes
        logger.debug("Strategy 2 failed. Strategy 3: All articles/divs...")
        
        all_elements = soup.find_all(['article', 'div'], class_=True, limit=100)
        
        # Filter to likely job containers (have links + text)
        likely_jobs = []
        for elem in all_elements:
            links = elem.find_all('a', href=True)
            text = elem.get_text(strip=True)
            
            if len(links) > 0 and len(text) > 30:
                likely_jobs.append(elem)
        
        if likely_jobs:
            logger.debug(f"Strategy 3: Found {len(likely_jobs)} likely containers")
            return likely_jobs[:50]
        
        logger.warning("All strategies failed - no job nodes found")
        return []
    
    def _parse_job_node(self, node, category: str) -> Optional[Dict]:
        """Parse a job node"""
        
        # Extract title - IMPROVED
        title = None
        
        # Try to find the main link
        title_selectors = ["h2 a", "h3 a", "h4 a", "a"]
        for sel in title_selectors:
            elem = node.select_one(sel)
            if elem:
                title = elem.get_text(strip=True)
                if len(title) > 15:  # Must be substantial
                    break
        
        if not title or len(title) < 10:
            return None
        
        # Extract URL
        url = self._extract_url_from_node(node)
        
        # Extract organization
        organization = "Not specified"
        org_keywords = ['organization', 'company', 'employer', 'dept', 'ministry']
        full_text = node.get_text(separator=" ", strip=True)
        
        # Try to find org in text
        for line in full_text.split('\n'):
            line = line.strip()
            if any(kw in line.lower() for kw in org_keywords) and len(line) < 100:
                organization = line
                break
        
        # Get snippet
        snippet = full_text[:1000]
        
        # Build job
        job = {
            "title": title.strip(),
            "organization": organization.strip(),
            "category": category,
            "url": urljoin(self.base_url, url) if url else self.base_url,
            "snippet": snippet,
            "vacancies": self._extract_vacancies(snippet),
            "salary": self._extract_salary(snippet),
            "age": self._extract_age(snippet),
            "experience": self._extract_experience(snippet),
            "qualification": self._extract_qualification(snippet),
            "posted": self._extract_posted_date(snippet),
            "scraped_at": datetime.utcnow().isoformat() + "Z"
        }
        
        return job
    
    def _extract_url_from_node(self, node) -> Optional[str]:
        """Extract URL from node"""
        
        for sel in ["h2 a", "h3 a", "h4 a", "a"]:
            try:
                link = node.select_one(sel)
                if link and link.get("href"):
                    return link["href"]
            except Exception:
                continue
        
        return None
    
    def _normalize_url(self, url: str) -> Optional[str]:
        """Normalize URL"""
        if not url:
            return None
        
        try:
            url = url.strip()
            abs_url = urljoin(self.base_url, url)
            parsed = urlparse(abs_url)
            if parsed.scheme and parsed.netloc:
                return abs_url
        except Exception:
            pass
        
        return None
    
    def _extract_vacancies(self, text: str) -> Optional[str]:
        """Extract vacancies"""
        if not text:
            return None
        
        patterns = [
            r'(\d{1,5})\s*(?:posts?|vacancies|vacancy|positions?|openings?)',
            r'(?:vacancy|vacancies|posts?)[:\s]+(\d{1,5})'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.I)
            if match:
                return match.group(1)
        
        if re.search(r'\bmultiple\b', text, re.I):
            return "Multiple"
        
        return None
    
    def _extract_salary(self, text: str) -> Optional[str]:
        """Extract salary"""
        if not text:
            return None
        
        patterns = [
            r'(?:₹|Rs\.?|INR)\s*[\d,]+(?:\s*-\s*(?:₹|Rs\.?|INR)?\s*[\d,]+)?',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.I)
            if match:
                return match.group(0).strip()
        
        return None
    
    def _extract_age(self, text: str) -> Optional[str]:
        """Extract age"""
        if not text:
            return None
        
        patterns = [
            r'(\d{1,2}\s*-\s*\d{1,2}\s*years?)',
            r'(?:age|up to)[:\s]+(\d{1,2}\s*years?)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.I)
            if match:
                return match.group(0).strip()
        
        return None
    
    def _extract_experience(self, text: str) -> Optional[str]:
        """Extract experience"""
        if not text:
            return None
        
        if re.search(r'\b(?:fresher|no experience)\b', text, re.I):
            return "Fresher"
        
        patterns = [
            r'(\d{1,2}[+]?)\s*(?:years?|yrs?)\s*(?:experience|exp)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.I)
            if match:
                return match.group(0).strip()
        
        return None
    
    def _extract_qualification(self, text: str) -> Optional[str]:
        """Extract qualification"""
        if not text:
            return None
        
        qualifications = [
            (r'\bph\.?d\b', "Ph.D"),
            (r'\bm\.?tech\b', "M.Tech"),
            (r'\bb\.?tech\b', "B.Tech"),
            (r'\bm\.?sc\b', "M.Sc"),
            (r'\bb\.?sc\b', "B.Sc"),
            (r'\bmba\b', "MBA"),
            (r'\bb\.?ed\b', "B.Ed"),
            (r'\bgraduate\b', "Graduate"),
        ]
        
        for pattern, qual_name in qualifications:
            if re.search(pattern, text, re.I):
                return qual_name
        
        return None
    
    def _extract_posted_date(self, text: str) -> Optional[str]:
        """Extract posting date"""
        if not text:
            return None
        
        # Check for relative dates
        days_match = re.search(r'(\d+)\s*days?\s*ago', text, re.I)
        if days_match:
            days = int(days_match.group(1))
            date = datetime.utcnow() - timedelta(days=days)
            return date.date().isoformat()
        
        return None
    
    def _is_valid_job(self, job: Optional[Dict]) -> bool:
        """Validate job"""
        if not job:
            return False
        
        title = job.get("title", "")
        
        if not title or len(title) < 10:
            return False
        
        # Filter spam
        spam_patterns = [
            r'\badvertisement\b',
            r'\bsponsored\b',
            r'^share\b',
            r'^follow\b',
            r'^subscribe\b',
        ]
        
        for pattern in spam_patterns:
            if re.search(pattern, title, re.I):
                return False
        
        return True
    
    def _slugify(self, text: str) -> str:
        """Convert to URL slug"""
        text = text.lower()
        text = re.sub(r'[^a-z0-9]+', '-', text)
        return text.strip('-')


# CLI Test
if __name__ == "__main__":
    logger.info("Starting JobYaari Scraper")
    
    scraper = JobYaariScraperEnhanced(timeout=20, max_retries=3)
    
    try:
        data = scraper.scrape_all_categories(
            categories=["Engineering", "Science", "Commerce", "Education"],
            max_jobs=7,
            delay=3
        )
        
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
                print()
        
        # Save
        with open("knowledge_base.json", "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        print(f"\n✓ Saved to knowledge_base.json")
    
    except KeyboardInterrupt:
        print("\nInterrupted")
    except Exception as e:
        logger.exception(f"Failed: {e}")
