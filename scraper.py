"""
Enhanced JobYaari.com Real-time Scraper
Extracts: Organization, Vacancies, Salary, Age, Experience, Qualification
Categories: Engineering, Science, Commerce, Education
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
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Referer': 'https://www.google.com/'
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        
    def fetch_page(self, url, max_retries=3):
        """Fetch a page with retry logic"""
        for attempt in range(max_retries):
            try:
                print(f"  Attempting to fetch: {url} (Attempt {attempt + 1})")
                response = self.session.get(url, timeout=20)
                
                if response.status_code == 200:
                    print(f"  ✓ Successfully fetched (Status: 200)")
                    return response
                elif response.status_code == 404:
                    print(f"  ✗ Page not found (Status: 404)")
                    return None
                else:
                    print(f"  ⚠ Status {response.status_code}, retrying...")
                    
                time.sleep(2 ** attempt)  # Exponential backoff
            except requests.Timeout:
                print(f"  ⚠ Timeout on attempt {attempt + 1}")
                time.sleep(2 ** attempt)
            except Exception as e:
                print(f"  ⚠ Error on attempt {attempt + 1}: {str(e)[:50]}")
                time.sleep(2 ** attempt)
        
        print(f"  ✗ Failed after {max_retries} attempts")
        return None
    
    def scrape_category(self, category):
        """Scrape jobs for a specific category"""
        print(f"\n{'='*70}")
        print(f"Scraping {category} Jobs")
        print(f"{'='*70}")
        
        jobs = []
        
        # Try homepage with category filtering
        homepage_jobs = self.scrape_homepage_filtered(category)
        jobs.extend(homepage_jobs)
        
        # Remove duplicates based on title
        seen_titles = set()
        unique_jobs = []
        for job in jobs:
            title_key = job['title'].lower().strip()
            if title_key not in seen_titles and len(title_key) > 10:
                seen_titles.add(title_key)
                unique_jobs.append(job)
                if len(unique_jobs) >= 6:
                    break
        
        print(f"✓ Found {len(unique_jobs)} unique {category} jobs\n")
        return unique_jobs
    
    def scrape_homepage_filtered(self, category):
        """Scrape homepage and filter by category keywords"""
        jobs = []
        
        try:
            print(f"Fetching homepage...")
            response = self.fetch_page(self.base_url)
            
            if not response:
                print("Failed to fetch homepage")
                return jobs
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Define category-specific keywords
            category_keywords = {
                'Engineering': ['engineer', 'engineering', 'technical', 'iit', 'nit', 'gate', 
                               'isro', 'drdo', 'ntpc', 'bhel', 'ongc', 'railway', 'civil', 
                               'mechanical', 'electrical', 'ese', 'upsc engineering'],
                'Science': ['science', 'scientist', 'research', 'csir', 'laboratory', 'physics', 
                           'chemistry', 'biology', 'biotechnology', 'microbiology', 'icar', 
                           'dst', 'net', 'fellow', 'phd'],
                'Commerce': ['bank', 'banking', 'sbi', 'ibps', 'clerk', 'po', 'probationary officer',
                            'finance', 'accountant', 'ssc', 'cgl', 'commerce', 'rbi', 
                            'insurance', 'lic', 'auditor', 'ca'],
                'Education': ['teacher', 'teaching', 'professor', 'lecturer', 'education', 
                             'kvs', 'nvs', 'ugc', 'school', 'college', 'university', 
                             'dsssb', 'pgt', 'tgt', 'principal', 'b.ed', 'assistant professor']
            }
            
            keywords = category_keywords.get(category, [])
            
            # Try multiple selectors for job posts
            selectors_to_try = [
                'article.post',
                'div.post-item',
                'div.job-card',
                'article',
                'div[class*="post"]',
                'div[class*="job"]',
                'div.card',
                'div[class*="card"]'
            ]
            
            job_elements = []
            for selector in selectors_to_try:
                elements = soup.select(selector)
                if elements and len(elements) > 3:
                    job_elements = elements
                    print(f"Found {len(elements)} elements with selector: {selector}")
                    break
            
            if not job_elements:
                print("⚠ No job elements found with standard selectors, using fallback")
                # Fallback: find all divs and articles
                job_elements = soup.find_all(['article', 'div'], limit=100)
            
            print(f"Processing {len(job_elements)} potential job elements...")
            
            for idx, element in enumerate(job_elements):
                try:
                    # Get all text content
                    text_content = element.get_text(separator=' ', strip=True).lower()
                    
                    # Skip if too short
                    if len(text_content) < 50:
                        continue
                    
                    # Check if matches category keywords
                    matches_category = any(keyword in text_content for keyword in keywords)
                    
                    if not matches_category:
                        continue
                    
                    # Extract job data
                    job_data = self.extract_job_from_element(element, category)
                    
                    if job_data and self.validate_job_data(job_data):
                        jobs.append(job_data)
                        print(f"  {len(jobs)}. ✓ {job_data['title'][:65]}")
                        
                        if len(jobs) >= 8:  # Get extra to filter duplicates later
                            break
                            
                except Exception as e:
                    continue
            
            if len(jobs) == 0:
                print(f"⚠ No {category} jobs found, using fallback data")
                
        except Exception as e:
            print(f"❌ Error scraping {category}: {str(e)}")
        
        return jobs
    
    def extract_job_from_element(self, element, category):
        """Extract detailed job information from HTML element"""
        
        # Extract title
        title = self.extract_title(element)
        if not title or len(title) < 10:
            return None
        
        # Get full text content
        full_text = element.get_text(separator=' ', strip=True)
        
        # Extract URL
        url = self.extract_url(element)
        
        # Extract all fields
        job_data = {
            "title": title,
            "organization": self.extract_organization(full_text),
            "vacancies": self.extract_vacancies(full_text),
            "salary": self.extract_salary(full_text),
            "age": self.extract_age(full_text),
            "experience": self.extract_experience(full_text),
            "qualification": self.extract_qualification(full_text),
            "category": category,
            "url": url or f"{self.base_url}/jobs",
            "scraped_at": datetime.now().isoformat()
        }
        
        return job_data
    
    def extract_title(self, element):
        """Extract job title from element"""
        title_selectors = [
            'h1', 'h2.entry-title', 'h2', 'h3.post-title', 
            'h3', 'h4', 'a.post-title', '.entry-title', 
            '.post-title', 'a[title]'
        ]
        
        for selector in title_selectors:
            title_elem = element.select_one(selector)
            if title_elem:
                title = title_elem.get_text(strip=True)
                # Clean title
                title = re.sub(r'\s+', ' ', title)
                if 10 < len(title) < 200:
                    return title
        
        # Fallback: get first heading-like text
        for tag in ['h1', 'h2', 'h3', 'h4', 'h5']:
            elem = element.find(tag)
            if elem:
                title = elem.get_text(strip=True)
                if 10 < len(title) < 200:
                    return title
        
        return None
    
    def extract_url(self, element):
        """Extract job URL from element"""
        link = element.find('a', href=True)
        if link:
            url = link['href']
            if url.startswith('http'):
                return url
            elif url.startswith('/'):
                return urljoin(self.base_url, url)
        return ""
    
    def extract_organization(self, text):
        """Extract organization name"""
        # Common patterns
        patterns = [
            r'(?:Organization|Conducting\s+Organization|Department|Ministry|Board|Commission|Corporation)[\s:]+([A-Z][A-Za-z\s&,\.]+?)(?:\n|\.|\s{2,})',
            r'\b(UPSC|SSC|IBPS|SBI|RBI|ISRO|DRDO|NTPC|BHEL|ONGC|GAIL|IOCL|BSNL|MTNL|Indian\s+Railways?)\b',
            r'\b(IIT|NIT|IIM|AIIMS|IGNOU|DU|BHU|Kendriya\s+Vidyalaya|Navodaya\s+Vidyalaya)\s+[A-Z][a-z]+',
            r'\b([A-Z][A-Za-z]+\s+(?:Bank|Insurance|Corporation|Limited|Ltd|University|Institute|College|Sangathan))\b',
            r'([A-Z]{2,}(?:\s+[A-Z]{2,})*)\s+(?:Recruitment|Notification|Exam)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                org = match.group(1).strip()
                # Clean organization name
                org = re.sub(r'\s+', ' ', org)
                if 3 < len(org) < 80 and not org.lower() in ['the', 'for', 'and', 'notification']:
                    return org
        
        return "Government of India"
    
    def extract_vacancies(self, text):
        """Extract number of vacancies"""
        patterns = [
            r'(?:Total\s+)?(?:Number\s+of\s+)?(?:Posts?|Vacancies|Positions?|Openings?)[\s:]+(\d+)',
            r'(\d+)\s+(?:Posts?|Vacancies|Positions?|Openings?)',
            r'Vacancy[\s:]+(\d+)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                num = match.group(1)
                if 0 < int(num) < 100000:
                    return num
        
        return "Multiple"
    
    def extract_salary(self, text):
        """Extract salary information"""
        patterns = [
            r'(?:Salary|Pay\s+Scale|Monthly\s+Salary)[\s:]*(?:Rs\.?|₹|INR)?\s*([\d,]+)\s*(?:-|to)\s*(?:Rs\.?|₹|INR)?\s*([\d,]+)',
            r'(?:Rs\.?|₹)\s*([\d,]+)\s*(?:-|to)\s*(?:Rs\.?|₹)?\s*([\d,]+)',
            r'(?:Rs\.?|₹)\s*([\d,]+)\s*(?:per\s+month|PM|/-)',
            r'Grade\s+Pay[\s:]*(?:Rs\.?|₹)?\s*([\d,]+)',
            r'CTC[\s:]*(?:Rs\.?|₹)?\s*([\d,]+)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                if len(match.groups()) >= 2 and match.group(2):
                    return f"₹{match.group(1)}-{match.group(2)}"
                else:
                    return f"₹{match.group(1)}"
        
        return "As per norms"
    
    def extract_age(self, text):
        """Extract age limit"""
        patterns = [
            r'Age\s+(?:Limit|Range)[\s:]*(\d+)\s*(?:-|to)\s*(\d+)\s*years?',
            r'(?:Between|Age)\s+(\d+)\s+(?:to|and|-)\s+(\d+)\s+years?',
            r'(?:Minimum|Min)[\s.]*Age[\s:]*(\d+)[\s\w]*(?:Maximum|Max)[\s.]*Age[\s:]*(\d+)',
            r'(?:Maximum|Max|Up\s*to)\s+Age[\s:]*(\d+)\s*years?',
            r'Age[\s:]*(\d+)\s*years?',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                groups = [g for g in match.groups() if g]
                if len(groups) >= 2:
                    age1, age2 = groups[0], groups[1]
                    if 17 <= int(age1) <= 70 and 18 <= int(age2) <= 70:
                        return f"{age1}-{age2} years"
                elif len(groups) == 1:
                    age = groups[0]
                    if 18 <= int(age) <= 70:
                        return f"Up to {age} years"
        
        return "As per rules"
    
    def extract_experience(self, text):
        """Extract experience requirement"""
        # Check for fresher first
        fresher_patterns = [
            r'\bfresher\b',
            r'\bno\s+experience\b',
            r'\b0\s+years?\s+experience\b',
            r'experience\s*:\s*not\s+required',
            r'experience\s*:\s*nil'
        ]
        
        for pattern in fresher_patterns:
            if re.search(pattern, text, re.IGNORECASE):
                return "Fresher"
        
        # Extract years of experience
        patterns = [
            r'(?:Minimum|Min|Atleast|At\s+least)\s+(\d+)\s+years?\s+(?:of\s+)?(?:experience|exp)',
            r'(?:Experience|Exp)[\s:]*(\d+)\s*(?:\+)?\s*years?',
            r'(\d+)\s*(?:\+)?\s*years?\s+(?:of\s+)?(?:experience|exp)',
            r'(\d+)\s*(?:-|to)\s*(\d+)\s+years?\s+experience',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                years = match.group(1)
                if 0 < int(years) < 40:
                    return f"{years} year" if int(years) == 1 else f"{years} years"
        
        return "Not specified"
    
    def extract_qualification(self, text):
        """Extract educational qualification"""
        qualifications = [
            ('Ph.D', r'Ph\.?D|Doctorate|Doctor\s+of\s+Philosophy'),
            ('M.Tech/M.E.', r'M\.?\s*Tech|M\.?\s*E\.?|Master\s+of\s+(?:Technology|Engineering)'),
            ('B.Tech/B.E.', r'B\.?\s*(?:Tech|E\.?)|Bachelor\s+of\s+(?:Technology|Engineering)'),
            ('M.Sc', r'M\.?\s*Sc|Master\s+of\s+Science'),
            ('B.Sc', r'B\.?\s*Sc|Bachelor\s+of\s+Science'),
            ('MBA', r'MBA|Master\s+of\s+Business\s+Administration'),
            ('M.Com', r'M\.?\s*Com|Master\s+of\s+Commerce'),
            ('B.Com', r'B\.?\s*Com|Bachelor\s+of\s+Commerce'),
            ('M.Ed', r'M\.?\s*Ed|Master\s+of\s+Education'),
            ('B.Ed', r'B\.?\s*Ed|Bachelor\s+of\s+Education'),
            ('CA', r'\bCA\b|Chartered\s+Accountant'),
            ('Diploma', r'Diploma\s+in'),
            ('Postgraduate', r'Post\s*Graduate|Post\s*Graduation|PG\s+'),
            ('Graduate', r'Graduate|Graduation|Bachelor|Degree|Any\s+Graduate'),
        ]
        
        for qual_name, pattern in qualifications:
            if re.search(pattern, text, re.IGNORECASE):
                return qual_name
        
        return "Graduate"
    
    def validate_job_data(self, job_data):
        """Validate that job data is meaningful"""
        if not job_data:
            return False
        
        # Title must be meaningful
        title = job_data.get('title', '')
        if title == "N/A" or len(title) < 10:
            return False
        
        # Check if title has job-related keywords
        job_keywords = [
            'recruitment', 'notification', 'vacancy', 'exam', 'job', 
            'post', 'position', 'officer', 'clerk', 'teacher', 'engineer',
            'scientist', 'professor', 'admit card', 'result', 'application'
        ]
        
        if not any(keyword in title.lower() for keyword in job_keywords):
            return False
        
        # Should have at least some extracted information beyond defaults
        extracted_fields = sum([
            job_data['organization'] != "Government of India",
            job_data['vacancies'] != "Multiple",
            job_data['salary'] != "As per norms",
            job_data['age'] != "As per rules",
            job_data['experience'] != "Not specified",
        ])
        
        return extracted_fields >= 1  # At least 1 field should be extracted
    
    def scrape_all_categories(self):
        """Scrape all four categories"""
        categories = ['Engineering', 'Science', 'Commerce', 'Education']
        results = {}
        
        print("\n" + "="*70)
        print("STARTING JOBYAARI.COM SCRAPING")
        print("="*70)
        
        for category in categories:
            try:
                jobs = self.scrape_category(category)
                results[category] = jobs
                time.sleep(3)  # Be respectful to the server
            except Exception as e:
                print(f"❌ Error scraping {category}: {e}")
                results[category] = []
        
        return results
    
    def save_to_json(self, data, filename='knowledge_base.json'):
        """Save scraped data to JSON file"""
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            print(f"\n✅ Data saved to {filename}")
            return True
        except Exception as e:
            print(f"❌ Error saving to file: {e}")
            return False


# Main execution
if __name__ == "__main__":
    print("\n" + "="*70)
    print("JobYaari.com Real-time Job Scraper")
    print("Assignment 2 - AI Chatbot Data Extraction")
    print("="*70)
    
    scraper = JobYaariScraper()
    
    print("\n🚀 Starting scraping process...")
    print("Extracting: Organization, Vacancies, Salary, Age, Experience, Qualification\n")
    
    start_time = time.time()
    data = scraper.scrape_all_categories()
    elapsed_time = time.time() - start_time
    
    # Print detailed summary
    print("\n" + "="*70)
    print("SCRAPING SUMMARY")
    print("="*70)
    
    total = 0
    for category, jobs in data.items():
        count = len(jobs)
        total += count
        print(f"\n📁 {category}: {count} jobs")
        
        if jobs:
            for i, job in enumerate(jobs, 1):
                print(f"  {i}. {job['title'][:60]}")
                print(f"     └─ Org: {job['organization'][:40]}")
                print(f"     └─ Vacancies: {job['vacancies']} | Exp: {job['experience']}")
    
    print(f"\n{'='*70}")
    print(f"✅ Total jobs scraped: {total}")
    print(f"⏱️  Time taken: {elapsed_time:.2f} seconds")
    print(f"{'='*70}")
    
    # Save to file
    if scraper.save_to_json(data):
        print("\n✅ Scraping completed successfully!")
        print("📁 Data saved to knowledge_base.json")
        print("🚀 You can now run the Flask app: python app.py")
    else:
        print("\n⚠️  Scraping completed but failed to save data")
