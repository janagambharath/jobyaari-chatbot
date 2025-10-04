#!/usr/bin/env python3
"""
Inspect actual JobYaari.com HTML structure to understand the real format
"""

import requests
from bs4 import BeautifulSoup
import json

def inspect_page(url):
    """Inspect a page and show its structure"""
    
    print(f"\n{'='*70}")
    print(f"Inspecting: {url}")
    print('='*70)
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        print(f"✓ Status: {response.status_code}")
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Save raw HTML for inspection
        with open('page_source.html', 'w', encoding='utf-8') as f:
            f.write(soup.prettify())
        print("✓ Saved raw HTML to: page_source.html")
        
        # Find all links that might be jobs
        print("\n" + "="*70)
        print("ANALYZING LINKS (potential job postings)")
        print("="*70)
        
        all_links = soup.find_all('a', href=True)
        job_links = []
        
        for link in all_links[:100]:  # Check first 100 links
            href = link.get('href', '')
            text = link.get_text(strip=True)
            
            # Filter out navigation/footer links
            if text and len(text) > 10 and len(text) < 200:
                # Look for job-related keywords
                job_keywords = ['recruitment', 'notification', 'vacancy', 'job', 'apply', 
                               'exam', 'admit', 'result', 'registration', '2024', '2025']
                
                if any(keyword in text.lower() or keyword in href.lower() for keyword in job_keywords):
                    job_links.append({
                        'text': text,
                        'href': href,
                        'parent_tag': link.parent.name if link.parent else None,
                        'parent_class': link.parent.get('class', []) if link.parent else []
                    })
        
        print(f"\nFound {len(job_links)} potential job links:")
        
        for i, job in enumerate(job_links[:10], 1):  # Show first 10
            print(f"\n{i}. {job['text'][:80]}")
            print(f"   URL: {job['href'][:100]}")
            print(f"   Parent: <{job['parent_tag']}> class={job['parent_class']}")
        
        # Analyze structure patterns
        print("\n" + "="*70)
        print("COMMON PATTERNS")
        print("="*70)
        
        # Find common parent structures
        parent_tags = {}
        parent_classes = {}
        
        for job in job_links:
            tag = job['parent_tag']
            if tag:
                parent_tags[tag] = parent_tags.get(tag, 0) + 1
            
            for cls in job['parent_class']:
                parent_classes[cls] = parent_classes.get(cls, 0) + 1
        
        print("\nMost common parent tags:")
        for tag, count in sorted(parent_tags.items(), key=lambda x: x[1], reverse=True)[:5]:
            print(f"  {tag}: {count} times")
        
        print("\nMost common parent classes:")
        for cls, count in sorted(parent_classes.items(), key=lambda x: x[1], reverse=True)[:10]:
            print(f"  .{cls}: {count} times")
        
        # Try to extract a sample job
        print("\n" + "="*70)
        print("SAMPLE JOB EXTRACTION")
        print("="*70)
        
        if job_links:
            sample = job_links[0]
            print(f"\nTrying to extract details from: {sample['text'][:60]}")
            
            # Find the parent container
            link_elem = soup.find('a', string=lambda s: s and sample['text'][:30] in s)
            if link_elem:
                # Go up to find the container
                container = link_elem.parent
                for _ in range(3):  # Try up to 3 levels
                    if container:
                        print(f"\nContainer: <{container.name}> class={container.get('class', [])}")
                        
                        # Extract all text
                        full_text = container.get_text(separator=' | ', strip=True)
                        print(f"Full text: {full_text[:300]}")
                        
                        # Look for common patterns
                        if any(word in full_text.lower() for word in ['vacancy', 'post', 'salary', 'age', 'qualification']):
                            print("\n✓ This looks like a job posting!")
                            
                            # Save the HTML structure
                            print(f"\nHTML Structure:")
                            print(container.prettify()[:500])
                            break
                        
                        container = container.parent
        
        # Generate recommended selectors
        print("\n" + "="*70)
        print("RECOMMENDED CSS SELECTORS")
        print("="*70)
        
        if parent_classes:
            top_class = sorted(parent_classes.items(), key=lambda x: x[1], reverse=True)[0][0]
            print(f"\nTry these selectors in your scraper:")
            print(f"  1. .{top_class}")
            print(f"  2. .{top_class} a")
            
            if parent_tags:
                top_tag = sorted(parent_tags.items(), key=lambda x: x[1], reverse=True)[0][0]
                print(f"  3. {top_tag}.{top_class}")
                print(f"  4. {top_tag} > a")
        
        # Save analysis
        analysis = {
            'url': url,
            'total_links': len(all_links),
            'job_links_found': len(job_links),
            'parent_tags': parent_tags,
            'parent_classes': parent_classes,
            'sample_jobs': job_links[:5]
        }
        
        with open('website_analysis.json', 'w', encoding='utf-8') as f:
            json.dump(analysis, f, indent=2, ensure_ascii=False)
        
        print("\n✓ Saved analysis to: website_analysis.json")
        
        return job_links
        
    except Exception as e:
        print(f"✗ Error: {e}")
        import traceback
        traceback.print_exc()
        return []


def main():
    print("\n" + "="*70)
    print("JobYaari.com HTML Structure Inspector")
    print("="*70)
    
    # Test multiple category pages
    urls = [
        'https://www.jobyaari.com/category/engineering',
        'https://www.jobyaari.com/category/science',
    ]
    
    all_findings = []
    
    for url in urls:
        findings = inspect_page(url)
        all_findings.extend(findings)
        
        if findings:
            print(f"\n✓ Found {len(findings)} potential jobs on this page")
            break  # If we found jobs, no need to check more
    
    if all_findings:
        print("\n" + "="*70)
        print("✓ SUCCESS - Jobs found!")
        print("="*70)
        print("\nNext steps:")
        print("1. Check 'page_source.html' to see the raw HTML")
        print("2. Check 'website_analysis.json' for detailed analysis")
        print("3. Update scraper.py with the recommended selectors")
    else:
        print("\n" + "="*70)
        print("⚠ No jobs found - website might use JavaScript")
        print("="*70)
        print("\nPossible reasons:")
        print("1. Website uses JavaScript to load content (needs Selenium)")
        print("2. URL structure is different")
        print("3. Website blocks automated access")
        print("\nRecommendation: Use the mock data loader for now")


if __name__ == "__main__":
    main()
