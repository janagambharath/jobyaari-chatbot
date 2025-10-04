"""
Advanced web scraper for JobYaari.com
This script extracts job data from different categories
"""

import requests
from bs4 import BeautifulSoup
import json
import re
from datetime import datetime

class JobYaariScraper:
    def __init__(self):
        self.base_url = "https://www.jobyaari.com"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
        }
        
    def scrape_category(self, category_name):
        """Scrape jobs for a specific category"""
        try:
            # Try different URL patterns
            urls_to_try = [
                f"{self.base_url}/category/{category_name.lower()}",
                f"{self.base_url}/{category_name.lower()}",
                f"{self.base_url}/tag/{category_name.lower()}",
            ]
            
            for url in urls_to_try:
                try:
                    response = requests.get(url, headers=self.headers, timeout=10)
                    if response.status_code == 200:
                        return self._parse_jobs(response.text, category_name)
                except:
                    continue
                    
            # If all fail, scrape main page
            response = requests.get(self.base_url, headers=self.headers, timeout=10)
            return self._parse_jobs(response.text, category_name, filter_category=True)
            
        except Exception as e:
            print(f"Error scraping {category_name}: {str(e)}")
            return []
    
    def _parse_jobs(self, html_content, category_name, filter_category=False):
        """Parse job listings from HTML"""
        soup = BeautifulSoup(html_content, 'html.parser')
        jobs = []
        
        # Find all potential job containers
        job_elements = soup.find_all(['article', 'div'], 
                                     class_=re.compile(r'post|job|card|entry|item', re.I))
        
        for element in job_elements:
            try:
                job_data = self._extract_job_data(element, category_name)
                
                # Filter by category if needed
                if filter_category:
                    text = element.get_text().lower()
                    if category_name.lower() not in text:
                        continue
                
                if job_data and job_data['title'] != "N/A":
                    jobs.append(job_data)
                    
                if len(jobs) >= 15:  # Limit per category
                    break
                    
            except Exception as e:
                continue
                
        return jobs
    
    def _extract_job_data(self, element, category):
        """Extract job details from an element"""
        # Title extraction
        title_elem = element.find(['h1', 'h2', 'h3', 'h4', 'a'], 
                                  class_=re.compile(r'title|heading|entry-title', re.I))
        if not title_elem:
            title_elem = element.find(['h1', 'h2', 'h3', 'h4'])
        
        title = title_elem.get_text(strip=True) if title_elem else "N/A"
        
        # Get all text for pattern matching
        full_text = element.get_text()
        
        # Organization extraction
        organization = self._extract_organization(full_text)
        
        # Vacancies extraction
        vacancies = self._extract_vacancies(full_text)
        
        # Salary extraction
        salary = self._extract_salary(full_text)
        
        # Age extraction
        age = self._extract_age(full_text)
        
        # Experience extraction
        experience = self._extract_experience(full_text)
        
        # Qualification extraction
        qualification = self._extract_qualification(full_text)
        
        # URL extraction
        link_elem = element.find('a', href=True)
        url = link_elem['href'] if link_elem else ""
        if url and not url.startswith('http'):
            url = self.base_url + url
        
        return {
            "title": title,
            "organization": organization,
            "vacancies": vacancies,
            "salary": salary,
            "age": age,
            "experience": experience,
            "qualification": qualification,
            "category": category,
            "url": url,
            "scraped_at": datetime.now().isoformat()
        }
    
    def _extract_organization(self, text):
        """Extract organization name"""
        patterns = [
            r'(?:by|in|at)\s+([A-Z][A-Za-z\s&]+(?:Ltd|Limited|Inc|Corporation|Department|Ministry|Board|Commission|University|Institute|College|Bank|Railway|Police)?)',
            r'([A-Z]{2,}(?:\s+[A-Z]{2,})*)',  # Acronyms like UPSC, SSC, etc.
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                org = match.group(1).strip()
                if len(org) > 2 and len(org) < 100:
                    return org
        
        return "Various Organizations"
    
    def _extract_vacancies(self, text):
        """Extract number of vacancies"""
        patterns = [
            r'(\d+)\s*(?:vacancy|vacancies|posts?|positions?)',
            r'(?:vacancy|vacancies|posts?)[\s:]+(\d+)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.I)
            if match:
                return match.group(1)
        
        return "Multiple"
    
    def _extract_salary(self, text):
        """Extract salary information"""
        patterns = [
            r'(?:Rs\.?|₹)\s*([\d,]+-[\d,]+)',
            r'(?:Rs\.?|₹)\s*([\d,]+)',
            r'(\d+)\s*(?:to|-)?\s*(\d+)\s*(?:per month|pm|/-)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.I)
            if match:
                return match.group(0)
        
        return "As per norms"
    
    def _extract_age(self, text):
        """Extract age limit"""
        patterns = [
            r'(\d+)\s*(?:-|to)\s*(\d+)\s*years?',
            r'(?:age|upto?)\s*(\d+)\s*years?',
            r'between\s*(\d+)\s*(?:and|to)\s*(\d+)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.I)
            if match:
                if len(match.groups()) > 1 and match.group(2):
                    return f"{match.group(1)}-{match.group(2)} years"
                return f"Up to {match.group(1)} years"
        
        return "As per rules"
    
    def _extract_experience(self, text):
        """Extract experience requirement"""
        patterns = [
            r'(\d+)\s*(?:\+)?\s*years?\s*(?:of\s*)?(?:experience|exp)',
            r'(?:experience|exp)[\s:]+(\d+)\s*years?',
            r'fresher',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.I)
            if match:
                if 'fresher' in match.group(0).lower():
                    return "Fresher"
                return f"{match.group(1)} years"
        
        return "Fresher/Experienced"
    
    def _extract_qualification(self, text):
        """Extract qualification requirement"""
        qualifications = {
            'B.Tech': r'B\.?Tech|Bachelor\s+of\s+Technology',
            'M.Tech': r'M\.?Tech|Master\s+of\s+Technology',
            'B.E.': r'B\.?E\.?|Bachelor\s+of\s+Engineering',
            'M.E.': r'M\.?E\.?|Master\s+of\s+Engineering',
            'B.Sc': r'B\.?Sc|Bachelor\s+of\s+Science',
            'M.Sc': r'M\.?Sc|Master\s+of\s+Science',
            'B.Com': r'B\.?Com|Bachelor\s+of\s+Commerce',
            'M.Com': r'M\.?Com|Master\s+of\s+Commerce',
            'MBA': r'MBA|Master\s+of\s+Business\s+Administration',
            'B.Ed': r'B\.?Ed|Bachelor\s+of\s+Education',
            'M.Ed': r'M\.?Ed|Master\s+of\s+Education',
            'Ph.D': r'Ph\.?D|Doctorate',
            'Diploma': r'Diploma',
            'Graduate': r'Graduate|Graduation',
            'Postgraduate': r'Postgraduate|Post\s+Graduate|PG',
        }
        
        for qual, pattern in qualifications.items():
            if re.search(pattern, text, re.I):
                return qual
        
        return "Graduate"
    
    def scrape_all_categories(self):
        """Scrape all categories"""
        categories = ['Engineering', 'Science', 'Commerce', 'Education']
        results = {}
        
        for category in categories:
            print(f"Scraping {category}...")
            jobs = self.scrape_category(category)
            results[category] = jobs
            print(f"Found {len(jobs)} jobs in {category}")
        
        return results
    
    def save_to_json(self, data, filename='knowledge_base.json'):
        """Save scraped data to JSON file"""
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print(f"Data saved to {filename}")

# Example usage
if __name__ == "__main__":
    scraper = JobYaariScraper()
    data = scraper.scrape_all_categories()
    scraper.save_to_json(data)
    
    # Print summary
    total = sum(len(jobs) for jobs in data.values())
    print(f"\nTotal jobs scraped: {total}")
    for category, jobs in data.items():
        print(f"{category}: {len(jobs)} jobs")