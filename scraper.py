"""
Enhanced JobYaari.com Scraper - Gets Real-time Data
This scraper is specifically designed for JobYaari.com's actual structure
"""

import requests
from bs4 import BeautifulSoup
import json
import re
from datetime import datetime
import time

class JobYaariScraper:
    def __init__(self):
        self.base_url = "https://www.jobyaari.com"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Referer': 'https://www.google.com/'
        }
        
    def scrape_homepage_by_category(self, category_filter):
        """Scrape from homepage and filter by category keywords"""
        try:
            print(f"Scraping homepage for {category_filter}...")
            response = requests.get(self.base_url, headers=self.headers, timeout=15)
            
            if response.status_code != 200:
                print(f"Failed to access homepage: {response.status_code}")
                return []
            
            soup = BeautifulSoup(response.content, 'html.parser')
            jobs = []
            
            # Try multiple selectors for job posts
            selectors = [
                'article.post',
                'div.post',
                'article',
                'div[class*="post"]',
                'div[class*="card"]',
                'div[class*="job"]'
            ]
            
            job_elements = []
            for selector in selectors:
                found = soup.select(selector)
                if found:
                    job_elements = found
                    print(f"Found {len(found)} elements with selector: {selector}")
                    break
            
            if not job_elements:
                print("No job elements found with any selector")
                return []
            
            # Process each job element
            for element in job_elements[:50]:  # Process more to filter later
                try:
                    text_content = element.get_text().lower()
                    
                    # Filter by category keywords
                    category_keywords = {
                        'Engineering': ['engineering', 'engineer', 'technical', 'iit', 'nit', 'gate', 'isro', 'drdo', 'ntpc', 'bhel'],
                        'Science': ['science', 'scientist', 'research', 'csir', 'net', 'laboratory', 'physics', 'chemistry', 'biology'],
                        'Commerce': ['bank', 'sbi', 'ibps', 'clerk', 'po', 'finance', 'accountant', 'ssc', 'cgl', 'commerce'],
                        'Education': ['teacher', 'professor', 'lecturer', 'education', 'kvs', 'nvs', 'ugc', 'teaching', 'school']
                    }
                    
                    # Check if this job matches the category
                    matches_category = False
                    if category_filter in category_keywords:
                        for keyword in category_keywords[category_filter]:
                            if keyword in text_content:
                                matches_category = True
                                break
                    
                    if not matches_category:
                        continue
                    
                    # Extract job data
                    job_data = self.extract_job_details(element, category_filter)
                    if job_data and job_data['title'] != "N/A" and len(job_data['title']) > 5:
                        jobs.append(job_data)
                        print(f"✓ Extracted: {job_data['title'][:50]}")
                        
                    if len(jobs) >= 5:  # Get 5 jobs per category
                        break
                        
                except Exception as e:
                    continue
            
            print(f"Total {category_filter} jobs found: {len(jobs)}")
            return jobs
            
        except Exception as e:
            print(f"Error scraping {category_filter}: {str(e)}")
            return []
    
    def extract_job_details(self, element, category):
        """Extract detailed job information from element"""
        
        # Get full text
        full_text = element.get_text(separator=' ', strip=True)
        
        # Extract title - try multiple methods
        title = "N/A"
        title_selectors = ['h1', 'h2', 'h3', 'h4', '.entry-title', '.post-title', 'a']
        for selector in title_selectors:
            title_elem = element.select_one(selector)
            if title_elem:
                title = title_elem.get_text(strip=True)
                if len(title) > 5:
                    break
        
        # Extract organization
        organization = self.extract_organization(full_text)
        
        # Extract vacancies
        vacancies = self.extract_vacancies(full_text)
        
        # Extract salary
        salary = self.extract_salary(full_text)
        
        # Extract age limit
        age = self.extract_age(full_text)
        
        # Extract experience
        experience = self.extract_experience(full_text)
        
        # Extract qualification
        qualification = self.extract_qualification(full_text)
        
        # Extract URL
        url = ""
        link = element.find('a', href=True)
        if link:
            url = link['href']
            if not url.startswith('http'):
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
    
    def extract_organization(self, text):
        """Extract organization name using patterns"""
        patterns = [
            r'(?:Organization|Department|Ministry|Board|Commission|Corporation|Limited|Ltd|Bank|Railway|University|Institute|College)[\s:]+([A-Z][A-Za-z\s&]+)',
            r'([A-Z]{2,}(?:\s+[A-Z]{2,})*)\s+(?:Recruitment|Notification|Exam)',
            r'(IIT|NIT|IIM|AIIMS|UPSC|SSC|IBPS|SBI|RBI|ISRO|DRDO|NTPC|BHEL|ONGC|GAIL|KVS|NVS)\b',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.I)
            if match:
                org = match.group(1).strip()
                if 2 < len(org) < 100:
                    return org
        
        return "Various Organizations"
    
    def extract_vacancies(self, text):
        """Extract vacancy count"""
        patterns = [
            r'(\d+)\s*(?:Vacancy|Vacancies|Posts?|Positions?|Openings?)',
            r'(?:Total|Number of)\s*(?:Vacancy|Vacancies|Posts?)[\s:]+(\d+)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.I)
            if match:
                num = match.group(1)
                if int(num) > 0:
                    return num
        
        return "Multiple"
    
    def extract_salary(self, text):
        """Extract salary information"""
        patterns = [
            r'(?:Salary|Pay Scale|Pay)[\s:]*(?:Rs\.?|₹)\s*([\d,]+-[\d,]+)',
            r'(?:Rs\.?|₹)\s*([\d,]+)\s*(?:-|to)\s*(?:Rs\.?|₹)?\s*([\d,]+)',
            r'(?:Rs\.?|₹)\s*([\d,]+)\s*(?:per month|PM|/-)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.I)
            if match:
                return match.group(0)
        
        return "As per norms"
    
    def extract_age(self, text):
        """Extract age limit"""
        patterns = [
            r'Age Limit[\s:]*(\d+)\s*(?:-|to)\s*(\d+)\s*years?',
            r'(\d+)\s*(?:-|to)\s*(\d+)\s*years?',
            r'(?:Maximum|Max|Upto?)\s*Age[\s:]*(\d+)\s*years?',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.I)
            if match:
                if len(match.groups()) >= 2 and match.group(2):
                    age1, age2 = match.group(1), match.group(2)
                    if 18 <= int(age1) <= 100 and 18 <= int(age2) <= 100:
                        return f"{age1}-{age2} years"
                elif match.group(1):
                    age = match.group(1)
                    if 18 <= int(age) <= 100:
                        return f"Up to {age} years"
        
        return "As per rules"
    
    def extract_experience(self, text):
        """Extract experience requirement"""
        if re.search(r'\bfresher\b', text, re.I):
            return "Fresher"
        
        patterns = [
            r'(\d+)\s*(?:\+)?\s*years?\s*(?:of\s*)?(?:Experience|Exp)',
            r'(?:Experience|Exp)[\s:]*(\d+)\s*years?',
            r'Minimum\s*(\d+)\s*years?\s*experience',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.I)
            if match:
                years = match.group(1)
                if 0 < int(years) < 50:
                    return f"{years} years"
        
        return "Fresher/Experienced"
    
    def extract_qualification(self, text):
        """Extract qualification requirement"""
        qualifications = {
            'Ph.D': r'Ph\.?D|Doctorate',
            'M.Tech': r'M\.?Tech|Master\s+of\s+Technology',
            'B.Tech': r'B\.?Tech|Bachelor\s+of\s+Technology',
            'M.E.': r'M\.?E\.?|Master\s+of\s+Engineering',
            'B.E.': r'B\.?E\.?|Bachelor\s+of\s+Engineering',
            'M.Sc': r'M\.?Sc|Master\s+of\s+Science',
            'B.Sc': r'B\.?Sc|Bachelor\s+of\s+Science',
            'MBA': r'MBA|Master\s+of\s+Business',
            'M.Com': r'M\.?Com|Master\s+of\s+Commerce',
            'B.Com': r'B\.?Com|Bachelor\s+of\s+Commerce',
            'M.Ed': r'M\.?Ed|Master\s+of\s+Education',
            'B.Ed': r'B\.?Ed|Bachelor\s+of\s+Education',
            'Postgraduate': r'Post\s*Graduate|PG',
            'Graduate': r'Graduate|Graduation|Degree',
            'Diploma': r'Diploma',
        }
        
        for qual, pattern in qualifications.items():
            if re.search(pattern, text, re.I):
                return qual
        
        return "Graduate"
    
    def scrape_all_categories(self):
        """Scrape all four categories"""
        categories = ['Engineering', 'Science', 'Commerce', 'Education']
        results = {}
        
        for category in categories:
            print(f"\n{'='*60}")
            print(f"Scraping {category}...")
            print(f"{'='*60}")
            jobs = self.scrape_homepage_by_category(category)
            results[category] = jobs
            time.sleep(2)  # Be respectful to the server
        
        return results
    
    def save_to_json(self, data, filename='knowledge_base.json'):
        """Save data to JSON file"""
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print(f"\n✓ Data saved to {filename}")

# Test the scraper
if __name__ == "__main__":
    print("JobYaari.com Real-time Scraper")
    print("="*60)
    
    scraper = JobYaariScraper()
    
    print("\nStarting scraping process...")
    data = scraper.scrape_all_categories()
    
    # Print summary
    print("\n" + "="*60)
    print("SCRAPING SUMMARY")
    print("="*60)
    total = 0
    for category, jobs in data.items():
        count = len(jobs)
        total += count
        print(f"{category}: {count} jobs")
        if jobs:
            print(f"  Sample: {jobs[0]['title'][:60]}...")
    
    print(f"\nTotal jobs scraped: {total}")
    
    # Save to file
    scraper.save_to_json(data)
    
    print("\n✓ Scraping completed successfully!")
