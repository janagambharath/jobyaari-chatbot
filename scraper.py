"""
Production-Grade Scraper for JobYaari.com
- Scrapes direct category pages for higher accuracy.
- Uses specific HTML selectors based on site structure.
- Enhanced data extraction with advanced regex.
- Resilient with retry logic and robust error handling.
"""
import requests
from bs4 import BeautifulSoup
import json
import re
from datetime import datetime
import time
from urllib.parse import urljoin

class JobYaariScraper:
    def __init__(self):
        self.base_url = "https://www.jobyaari.com"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Referer': 'https://www.google.com/'
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)

    def fetch_page(self, url, retries=3, backoff_factor=2):
        """Fetches a web page with retry logic."""
        for i in range(retries):
            try:
                response = self.session.get(url, timeout=15)
                if response.status_code == 200:
                    return response
                print(f"  [Request] Status {response.status_code} for {url}. Retrying...")
            except requests.exceptions.RequestException as e:
                print(f"  [Request] Error fetching {url}: {e}. Retrying...")
            time.sleep(backoff_factor * (2 ** i))
        return None

    def scrape_category(self, category):
        """Scrapes jobs from a specific category page."""
        category_slug = category.lower().replace(' ', '-')
        category_url = f"{self.base_url}/category/{category_slug}"
        
        print(f"\n{'='*70}\n[Scraping] Category: {category} | URL: {category_url}\n{'='*70}")
        
        response = self.fetch_page(category_url)
        if not response:
            print(f"❌ Failed to fetch page for {category}.")
            return []

        soup = BeautifulSoup(response.content, 'html.parser')
        jobs = []
        
        # This selector is based on the provided screenshot for job cards
        job_elements = soup.select('article.post, div.job-listing-item, div.card-body')

        if not job_elements:
            print(f"⚠️ No job elements found for '{category}' with primary selectors. Trying fallback.")
            job_elements = soup.select('div[class*="job"], article[class*="post"]')

        print(f"Found {len(job_elements)} potential job elements.")

        for element in job_elements:
            try:
                job_data = self.extract_job_details(element, category)
                if job_data and self.is_valid_job(job_data):
                    jobs.append(job_data)
                    print(f"  ✓ Extracted: {job_data['title'][:60]}")
            except Exception as e:
                print(f"  - Error parsing an element: {e}")
                continue
        
        # Remove duplicates
        unique_jobs = list({job['title']: job for job in jobs}.values())
        print(f"✅ Found {len(unique_jobs)} unique jobs for {category}.")
        return unique_jobs[:7] # Limit to 7 jobs per category

    def extract_job_details(self, element, category):
        """Extracts job details from a single HTML element."""
        title = self.extract_text(element, ['h2 a', 'h3 a', '.job-title', '.entry-title'])
        if not title:
            return None
        
        organization = self.extract_text(element, ['.company-name', '.organization', 'a[href*="/company/"]']) or "Not specified"
        full_text = element.get_text(separator=' ', strip=True)

        return {
            "title": title,
            "organization": organization,
            "vacancies": self.extract_vacancies(full_text),
            "salary": self.extract_salary(full_text),
            "age": self.extract_age(full_text),
            "experience": self.extract_experience(full_text),
            "qualification": self.extract_qualification(full_text),

            "category": category,
            "url": self.extract_url(element),
            "scraped_at": datetime.now().isoformat()
        }

    def extract_text(self, element, selectors):
        """Helper to extract text using a list of selectors."""
        for selector in selectors:
            node = element.select_one(selector)
            if node:
                return node.get_text(strip=True)
        return None
        
    def extract_url(self, element):
        """Extracts the primary URL from a job element."""
        link_node = element.find('a', href=True)
        if link_node and link_node['href']:
            return urljoin(self.base_url, link_node['href'])
        return self.base_url

    def extract_vacancies(self, text):
        match = re.search(r'(\d+)\s*(?:posts?|vacancies)', text, re.I)
        return match.group(1) if match else "Multiple"

    def extract_salary(self, text):
        match = re.search(r'(?:₹|Rs\.?)\s*([\d,]+(?:\s*-\s*[\d,]+)?)', text, re.I)
        return match.group(0) if match else "As per norms"

    def extract_age(self, text):
        match = re.search(r'(\d+\s*-\s*\d+\s*years|\d+\s*years)', text, re.I)
        return match.group(1) if match else "As per rules"

    def extract_experience(self, text):
        if 'fresher' in text.lower():
            return "Fresher"
        match = re.search(r'(\d+(?:\+)?\s*years?)', text, re.I)
        return match.group(1) if match else "Not Specified"
        
    def extract_qualification(self, text):
        quals = ['Ph.D', 'M.Tech', 'B.Tech', 'M.E.', 'B.E.', 'M.Sc', 'B.Sc', 'MBA', 'M.Com', 'B.Com', 'M.Ed', 'B.Ed', 'Postgraduate', 'Graduate', 'Diploma']
        text_lower = text.lower()
        for qual in quals:
            if qual.lower().replace('.', r'\.?') in text_lower:
                return qual
        return "Graduate"

    def is_valid_job(self, job_data):
        """Basic validation for a scraped job."""
        return len(job_data['title']) > 10 and job_data['organization'] != "Not specified"

    def scrape_all_categories(self):
        """Scrapes all defined categories and returns the data."""
        categories = ['Engineering', 'Science', 'Commerce', 'Education']
        results = {}
        for category in categories:
            results[category] = self.scrape_category(category)
            time.sleep(2)  # Be respectful to the server
        return results

if __name__ == '__main__':
    print("🚀 Starting Production-Grade Scraper Test...")
    scraper = JobYaariScraper()
    all_data = scraper.scrape_all_categories()
    
    print("\n\n--- SCRAPING SUMMARY ---")
    total_scraped = sum(len(jobs) for jobs in all_data.values())
    print(f"Total Unique Jobs Scraped: {total_scraped}")
    for category, jobs in all_data.items():
        print(f"\n[{category}] - {len(jobs)} jobs found:")
        for job in jobs[:2]: # Print sample
            print(f"  - {job['title']} at {job['organization']}")
            
    with open('knowledge_base.json', 'w', encoding='utf-8') as f:
        json.dump(all_data, f, indent=2, ensure_ascii=False)
    print("\n✅ Data saved to knowledge_base.json")
