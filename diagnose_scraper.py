#!/usr/bin/env python3
"""
Diagnostic script to identify scraping issues
"""

import requests
from bs4 import BeautifulSoup
import json

def test_website_access():
    """Test if JobYaari.com is accessible"""
    print("=" * 60)
    print("TEST 1: Website Accessibility")
    print("=" * 60)
    
    url = "https://www.jobyaari.com"
    
    try:
        response = requests.get(url, timeout=10, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        })
        
        print(f"✓ Status Code: {response.status_code}")
        print(f"✓ Content Length: {len(response.content)} bytes")
        print(f"✓ Content Type: {response.headers.get('content-type', 'Unknown')}")
        
        if response.status_code == 200:
            return True, response
        else:
            print(f"✗ Unexpected status code: {response.status_code}")
            return False, None
            
    except requests.exceptions.ConnectionError as e:
        print(f"✗ Connection Error: {e}")
        print("  → Check your internet connection")
        return False, None
    except requests.exceptions.Timeout:
        print("✗ Timeout Error")
        print("  → Website is too slow or unresponsive")
        return False, None
    except Exception as e:
        print(f"✗ Error: {e}")
        return False, None


def analyze_html_structure(response):
    """Analyze the HTML structure"""
    print("\n" + "=" * 60)
    print("TEST 2: HTML Structure Analysis")
    print("=" * 60)
    
    soup = BeautifulSoup(response.content, 'html.parser')
    
    # Check title
    title = soup.title.string if soup.title else "No title found"
    print(f"Page Title: {title}")
    
    # Check for common job listing patterns
    print("\nSearching for job listings...")
    
    patterns = [
        ("article.post", "article.post"),
        ("div.job-listing", "div.job-listing"),
        ("div.job-item", "div.job-item"),
        ("li.job", "li.job"),
        ("article", "article (generic)"),
        ("div[class*='job']", "div with 'job' in class"),
    ]
    
    found_any = False
    for selector, name in patterns:
        try:
            elements = soup.select(selector)
            if elements:
                print(f"  ✓ Found {len(elements)} elements: {name}")
                found_any = True
                
                # Show sample
                if len(elements) > 0:
                    sample = elements[0]
                    text = sample.get_text(strip=True)[:100]
                    print(f"    Sample text: {text}...")
        except Exception as e:
            print(f"  ✗ Error with {name}: {e}")
    
    if not found_any:
        print("  ⚠ No job listings found with common selectors")
        print("\n  Showing page structure:")
        
        # Show first few divs/articles
        divs = soup.find_all(['div', 'article', 'section'], limit=10)
        for i, elem in enumerate(divs, 1):
            classes = elem.get('class', [])
            id_attr = elem.get('id', '')
            print(f"    {i}. <{elem.name}> class={classes} id={id_attr}")
    
    return found_any


def test_category_urls():
    """Test if category URLs work"""
    print("\n" + "=" * 60)
    print("TEST 3: Category URL Testing")
    print("=" * 60)
    
    categories = {
        "Engineering": "https://www.jobyaari.com/category/engineering",
        "Science": "https://www.jobyaari.com/category/science",
        "Commerce": "https://www.jobyaari.com/category/commerce",
        "Education": "https://www.jobyaari.com/category/education",
    }
    
    working_urls = []
    
    for cat_name, url in categories.items():
        try:
            response = requests.get(url, timeout=10, headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            })
            
            if response.status_code == 200:
                print(f"  ✓ {cat_name}: {response.status_code}")
                working_urls.append((cat_name, url))
            elif response.status_code == 404:
                print(f"  ✗ {cat_name}: 404 Not Found")
                print(f"    → URL structure might be different")
            else:
                print(f"  ⚠ {cat_name}: {response.status_code}")
                
        except Exception as e:
            print(f"  ✗ {cat_name}: {e}")
    
    return working_urls


def test_actual_scraping():
    """Test the actual scraper"""
    print("\n" + "=" * 60)
    print("TEST 4: Actual Scraper Test")
    print("=" * 60)
    
    try:
        from scraper import JobYaariScraperEnhanced
        
        print("✓ Scraper module imported successfully")
        
        scraper = JobYaariScraperEnhanced(timeout=15, max_retries=2)
        print("✓ Scraper initialized")
        
        # Test one category
        print("\nTesting Engineering category...")
        jobs = scraper.scrape_category("Engineering", max_jobs=3)
        
        if jobs:
            print(f"✓ Found {len(jobs)} jobs!")
            for i, job in enumerate(jobs, 1):
                print(f"\n  Job {i}:")
                print(f"    Title: {job.get('title', 'N/A')}")
                print(f"    Organization: {job.get('organization', 'N/A')}")
                print(f"    URL: {job.get('url', 'N/A')}")
            return True
        else:
            print("✗ No jobs found")
            print("  → Check if website structure has changed")
            return False
            
    except ImportError as e:
        print(f"✗ Cannot import scraper: {e}")
        return False
    except Exception as e:
        print(f"✗ Scraping error: {e}")
        import traceback
        traceback.print_exc()
        return False


def provide_recommendations():
    """Provide recommendations based on tests"""
    print("\n" + "=" * 60)
    print("RECOMMENDATIONS")
    print("=" * 60)
    
    print("""
Based on the tests above, here are possible solutions:

1. **If website is inaccessible:**
   - Check your internet connection
   - Check if JobYaari.com is down
   - Try accessing the website in a browser
   - Check firewall/proxy settings

2. **If HTML structure changed:**
   - The website might have redesigned
   - Update the CSS selectors in scraper.py
   - Contact me to update the scraper

3. **If category URLs don't work:**
   - The URL structure might be different
   - Try browsing the website manually
   - Update base_url in scraper.py

4. **Temporary solution:**
   - Use the sample knowledge_base.json I provided
   - This will let the chatbot work while we fix scraping

5. **Alternative approach:**
   - Manually create knowledge_base.json with current jobs
   - Copy job details from JobYaari.com into the JSON format
    """)


def main():
    print("\n" + "=" * 70)
    print("JobYaari Scraper Diagnostic Tool")
    print("=" * 70 + "\n")
    
    # Test 1: Website access
    accessible, response = test_website_access()
    
    if not accessible:
        print("\n⚠ Cannot access JobYaari.com")
        print("   The website might be down or blocked")
        provide_recommendations()
        return
    
    # Test 2: HTML structure
    has_listings = analyze_html_structure(response)
    
    # Test 3: Category URLs
    working_urls = test_category_urls()
    
    # Test 4: Actual scraping
    scraper_works = test_actual_scraping()
    
    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Website Accessible: {'✓ Yes' if accessible else '✗ No'}")
    print(f"Job Listings Found: {'✓ Yes' if has_listings else '✗ No'}")
    print(f"Category URLs Work: {len(working_urls)}/4")
    print(f"Scraper Works: {'✓ Yes' if scraper_works else '✗ No'}")
    
    if scraper_works:
        print("\n✓ Everything looks good! Run: python scraper.py")
    else:
        provide_recommendations()


if __name__ == "__main__":
    main()
