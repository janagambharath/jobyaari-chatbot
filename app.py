from flask import Flask, render_template, request, jsonify
from flask_cors import CORS
import requests
from bs4 import BeautifulSoup
import json
import os
from datetime import datetime
import re

app = Flask(__name__)
CORS(app)

# OpenRouter API Configuration
OPENROUTER_API_KEY = os.environ.get('OPENROUTER_API_KEY', 'your-api-key-here')
OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"

# Knowledge Base Storage
KNOWLEDGE_BASE = {
    "Engineering": [],
    "Science": [],
    "Commerce": [],
    "Education": []
}

def scrape_jobyaari_category(category_url, category_name):
    """Scrape job data from JobYaari for a specific category"""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        response = requests.get(category_url, headers=headers, timeout=10)
        soup = BeautifulSoup(response.content, 'html.parser')
        
        jobs = []
        
        # Find job listings - adjust selectors based on actual website structure
        job_cards = soup.find_all(['article', 'div'], class_=re.compile(r'job|post|card|item', re.I))
        
        for card in job_cards[:10]:  # Limit to 10 jobs per category
            try:
                # Extract job details
                title_elem = card.find(['h1', 'h2', 'h3', 'h4', 'a'], class_=re.compile(r'title|heading|name', re.I))
                title = title_elem.get_text(strip=True) if title_elem else "N/A"
                
                # Extract organization
                org_elem = card.find(string=re.compile(r'organization|department|company', re.I))
                organization = org_elem.strip() if org_elem else "Various Organizations"
                
                # Extract other details
                text_content = card.get_text()
                
                # Extract vacancies
                vacancy_match = re.search(r'(\d+)\s*(?:vacancy|vacancies|posts?)', text_content, re.I)
                vacancies = vacancy_match.group(1) if vacancy_match else "Multiple"
                
                # Extract salary
                salary_match = re.search(r'(?:Rs\.?|₹)\s*([\d,]+(?:-[\d,]+)?)', text_content)
                salary = salary_match.group(0) if salary_match else "As per norms"
                
                # Extract age
                age_match = re.search(r'(\d+)\s*(?:-\s*(\d+))?\s*years?', text_content, re.I)
                age = f"{age_match.group(1)}-{age_match.group(2)} years" if age_match and age_match.group(2) else "As per rules"
                
                # Extract experience
                exp_match = re.search(r'(\d+)\s*(?:year|yr)s?\s*(?:of\s*)?(?:experience|exp)', text_content, re.I)
                experience = f"{exp_match.group(1)} years" if exp_match else "Fresher/Experienced"
                
                # Extract qualification
                qual_keywords = ['graduate', 'postgraduate', 'diploma', 'degree', 'B.Tech', 'M.Tech', 'B.Sc', 'M.Sc', 'MBA', 'B.Com', 'M.Com']
                qualification = "Graduate"
                for keyword in qual_keywords:
                    if keyword.lower() in text_content.lower():
                        qualification = keyword
                        break
                
                job = {
                    "title": title,
                    "organization": organization,
                    "vacancies": vacancies,
                    "salary": salary,
                    "age": age,
                    "experience": experience,
                    "qualification": qualification,
                    "category": category_name
                }
                
                if title != "N/A" and len(title) > 3:
                    jobs.append(job)
                    
            except Exception as e:
                continue
        
        return jobs
        
    except Exception as e:
        print(f"Error scraping {category_name}: {str(e)}")
        return []

def load_sample_data():
    """Load sample data if scraping fails"""
    return {
        "Engineering": [
            {
                "title": "GATE 2025 Notification",
                "organization": "IIT Bombay",
                "vacancies": "Multiple",
                "salary": "As per organization",
                "age": "18-32 years",
                "experience": "Fresher",
                "qualification": "B.Tech/B.E.",
                "category": "Engineering"
            },
            {
                "title": "UPSC Engineering Services Exam",
                "organization": "UPSC",
                "vacancies": "400",
                "salary": "₹56,100 - ₹1,77,500",
                "age": "21-30 years",
                "experience": "Fresher",
                "qualification": "B.E./B.Tech",
                "category": "Engineering"
            }
        ],
        "Science": [
            {
                "title": "CSIR NET 2025",
                "organization": "CSIR",
                "vacancies": "Multiple",
                "salary": "₹31,000 - ₹35,000",
                "age": "28 years",
                "experience": "Fresher",
                "qualification": "M.Sc",
                "category": "Science"
            }
        ],
        "Commerce": [
            {
                "title": "SBI PO Recruitment 2025",
                "organization": "State Bank of India",
                "vacancies": "2000",
                "salary": "₹57,000/month",
                "age": "21-30 years",
                "experience": "Fresher",
                "qualification": "Graduate",
                "category": "Commerce"
            }
        ],
        "Education": [
            {
                "title": "KVS Teacher Recruitment",
                "organization": "Kendriya Vidyalaya Sangathan",
                "vacancies": "1000",
                "salary": "₹44,900 - ₹1,42,400",
                "age": "Up to 40 years",
                "experience": "2 years",
                "qualification": "B.Ed",
                "category": "Education"
            }
        ]
    }

def refresh_knowledge_base():
    """Refresh the knowledge base by scraping JobYaari"""
    global KNOWLEDGE_BASE
    
    # JobYaari category URLs (adjust these based on actual website structure)
    categories = {
        "Engineering": "https://www.jobyaari.com/category/engineering",
        "Science": "https://www.jobyaari.com/category/science",
        "Commerce": "https://www.jobyaari.com/category/commerce",
        "Education": "https://www.jobyaari.com/category/education"
    }
    
    # Try to scrape live data
    scraped_data = {}
    for category, url in categories.items():
        jobs = scrape_jobyaari_category(url, category)
        scraped_data[category] = jobs
    
    # Check if we got any data
    total_jobs = sum(len(jobs) for jobs in scraped_data.values())
    
    if total_jobs > 0:
        KNOWLEDGE_BASE = scraped_data
    else:
        # Fall back to sample data
        KNOWLEDGE_BASE = load_sample_data()
    
    return KNOWLEDGE_BASE

def query_deepseek(user_message, context):
    """Query DeepSeek R1 via OpenRouter"""
    try:
        system_prompt = f"""You are a helpful JobYaari assistant. You help users find job notifications and information.

Available job data:
{json.dumps(context, indent=2)}

Guidelines:
- Answer questions about job notifications in Engineering, Science, Commerce, and Education
- Provide specific details like organization, vacancies, salary, age, experience, and qualification
- Be concise and helpful
- If asked about a specific category, filter and show only those jobs
- If asked about specific criteria (experience, qualification), filter accordingly
- Format responses clearly with job details"""

        payload = {
            "model": "deepseek/deepseek-r1:free",
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_message}
            ],
            "temperature": 0.7,
            "max_tokens": 1000
        }
        
        headers = {
            "Authorization": f"Bearer {OPENROUTER_API_KEY}",
            "Content-Type": "application/json"
        }
        
        response = requests.post(OPENROUTER_URL, json=payload, headers=headers, timeout=30)
        
        if response.status_code == 200:
            result = response.json()
            return result['choices'][0]['message']['content']
        else:
            # Fallback to simple matching
            return simple_query_response(user_message, context)
            
    except Exception as e:
        print(f"DeepSeek error: {str(e)}")
        return simple_query_response(user_message, context)

def simple_query_response(query, context):
    """Simple fallback response without AI API"""
    query_lower = query.lower()
    
    # Category detection
    category = None
    if 'engineering' in query_lower:
        category = 'Engineering'
    elif 'science' in query_lower:
        category = 'Science'
    elif 'commerce' in query_lower:
        category = 'Commerce'
    elif 'education' in query_lower:
        category = 'Education'
    
    # Experience filter
    exp_match = re.search(r'(\d+)\s*year', query_lower)
    experience_filter = exp_match.group(1) if exp_match else None
    
    # Build response
    if category:
        jobs = context.get(category, [])
        if experience_filter:
            jobs = [j for j in jobs if experience_filter in j.get('experience', '')]
        
        if jobs:
            response = f"Here are the latest {category} job notifications:\n\n"
            for i, job in enumerate(jobs[:5], 1):
                response += f"{i}. {job['title']}\n"
                response += f"   Organization: {job['organization']}\n"
                response += f"   Vacancies: {job['vacancies']}\n"
                response += f"   Salary: {job['salary']}\n"
                response += f"   Age: {job['age']}\n"
                response += f"   Experience: {job['experience']}\n"
                response += f"   Qualification: {job['qualification']}\n\n"
            return response
        else:
            return f"No {category} jobs found matching your criteria."
    else:
        # Show all categories
        response = "Here are the available job categories:\n\n"
        for cat, jobs in context.items():
            response += f"**{cat}**: {len(jobs)} jobs available\n"
        response += "\nAsk me about specific categories like 'Show me Engineering jobs' or 'What are the latest Science notifications?'"
        return response

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/api/chat', methods=['POST'])
def chat():
    try:
        data = request.json
        user_message = data.get('message', '')
        
        if not user_message:
            return jsonify({'error': 'No message provided'}), 400
        
        # Get response from AI
        response = query_deepseek(user_message, KNOWLEDGE_BASE)
        
        return jsonify({
            'response': response,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/refresh', methods=['POST'])
def refresh_data():
    try:
        refresh_knowledge_base()
        total_jobs = sum(len(jobs) for jobs in KNOWLEDGE_BASE.values())
        return jsonify({
            'message': 'Knowledge base refreshed successfully',
            'total_jobs': total_jobs,
            'categories': {k: len(v) for k, v in KNOWLEDGE_BASE.items()}
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/data', methods=['GET'])
def get_data():
    return jsonify(KNOWLEDGE_BASE)

@app.route('/health', methods=['GET'])
def health():
    return jsonify({'status': 'healthy', 'timestamp': datetime.now().isoformat()})

if __name__ == '__main__':
    # Initialize knowledge base on startup
    print("Initializing knowledge base...")
    refresh_knowledge_base()
    print(f"Loaded {sum(len(jobs) for jobs in KNOWLEDGE_BASE.values())} jobs")
    
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)