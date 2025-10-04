from flask import Flask, render_template, request, jsonify
from flask_cors import CORS
import json
import os
from datetime import datetime
import re

# Import the scraper
from scraper import JobYaariScraper

app = Flask(__name__)
CORS(app)

# Configuration
OPENROUTER_API_KEY = os.environ.get('OPENROUTER_API_KEY')
OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"
KNOWLEDGE_BASE_FILE = "knowledge_base.json"

# Global knowledge base
KNOWLEDGE_BASE = {
    "Engineering": [],
    "Science": [],
    "Commerce": [],
    "Education": []
}

def load_knowledge_base_from_file():
    """Load knowledge base from JSON file"""
    global KNOWLEDGE_BASE
    try:
        if os.path.exists(KNOWLEDGE_BASE_FILE):
            with open(KNOWLEDGE_BASE_FILE, 'r', encoding='utf-8') as f:
                KNOWLEDGE_BASE = json.load(f)
                total = sum(len(jobs) for jobs in KNOWLEDGE_BASE.values())
                print(f"✅ Loaded {total} jobs from {KNOWLEDGE_BASE_FILE}")
                return True
        else:
            print(f"⚠️ Knowledge base file not found")
            return False
    except Exception as e:
        print(f"❌ Error loading knowledge base: {e}")
        return False

def refresh_knowledge_base():
    """Refresh knowledge base by scraping live data"""
    global KNOWLEDGE_BASE
    print("\n🔄 Scraping live job data from JobYaari.com...")
    
    try:
        scraper = JobYaariScraper()
        scraped_data = scraper.scrape_all_categories()
        
        total_jobs = sum(len(jobs) for jobs in scraped_data.values())
        
        if total_jobs > 0:
            KNOWLEDGE_BASE = scraped_data
            
            # Save to file
            with open(KNOWLEDGE_BASE_FILE, 'w', encoding='utf-8') as f:
                json.dump(KNOWLEDGE_BASE, f, indent=2, ensure_ascii=False)
            
            print(f"✅ Successfully scraped {total_jobs} jobs")
            return True
        else:
            print("⚠️ No jobs scraped. Using local data.")
            load_knowledge_base_from_file()
            return False
            
    except Exception as e:
        print(f"❌ Scraping failed: {e}")
        load_knowledge_base_from_file()
        return False

def query_ai_model(user_message, context):
    """Query OpenRouter API with context"""
    if not OPENROUTER_API_KEY:
        print("⚠️ OPENROUTER_API_KEY not set. Using rule-based responses.")
        return rule_based_response(user_message, context)
    
    try:
        import requests
        
        system_prompt = f"""You are JobYaari AI Assistant, a helpful chatbot for Indian government job notifications.

**Your Knowledge Base:**
```json
{json.dumps(context, indent=2)}
```

**Instructions:**
1. Answer questions about job notifications in Engineering, Science, Commerce, and Education categories
2. Provide specific details: organization name, vacancies, salary, age limit, experience, and qualification
3. Format responses clearly with proper structure
4. If asked about experience/qualification/age, filter and show matching jobs
5. Always be helpful, accurate, and conversational
6. Use bullet points and emojis for better readability
7. If no exact match, suggest similar jobs

**Response Format:**
- Use clear sections with headers
- Show key details in bullet points
- Include job URLs when relevant
- Be concise but complete

Answer the user's question based ONLY on the knowledge base provided."""

        payload = {
            "model": "meta-llama/llama-3.1-8b-instruct:free",
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_message}
            ],
            "temperature": 0.7,
            "max_tokens": 800
        }
        
        headers = {
            "Authorization": f"Bearer {OPENROUTER_API_KEY}",
            "Content-Type": "application/json"
        }
        
        response = requests.post(
            OPENROUTER_URL,
            headers=headers,
            json=payload,
            timeout=30
        )
        
        if response.status_code == 200:
            result = response.json()
            return result['choices'][0]['message']['content']
        else:
            print(f"API Error: {response.status_code}")
            return rule_based_response(user_message, context)
            
    except Exception as e:
        print(f"AI Model Error: {e}")
        return rule_based_response(user_message, context)

def rule_based_response(user_message, context):
    """Rule-based fallback response system"""
    message_lower = user_message.lower()
    
    # Determine category
    category = None
    if any(word in message_lower for word in ['engineering', 'engineer', 'technical', 'gate']):
        category = 'Engineering'
    elif any(word in message_lower for word in ['science', 'scientist', 'research', 'csir']):
        category = 'Science'
    elif any(word in message_lower for word in ['commerce', 'bank', 'sbi', 'ibps', 'clerk', 'ssc']):
        category = 'Commerce'
    elif any(word in message_lower for word in ['education', 'teacher', 'professor', 'kvs', 'teaching']):
        category = 'Education'
    
    # Extract filters
    experience_match = re.search(r'(\d+)\s*years?\s*(?:of\s*)?(?:experience|exp)', message_lower)
    experience_years = int(experience_match.group(1)) if experience_match else None
    
    # Qualification keywords
    qualification_keywords = {
        'graduate': ['graduate', 'graduation', 'bachelor'],
        'postgraduate': ['postgraduate', 'pg', 'masters', 'msc', 'mtech', 'mba', 'mcom'],
        'phd': ['phd', 'doctorate'],
        'diploma': ['diploma']
    }
    
    qualification_filter = None
    for qual, keywords in qualification_keywords.items():
        if any(kw in message_lower for kw in keywords):
            qualification_filter = qual
            break
    
    # Build response
    if category:
        jobs = context.get(category, [])
        
        if not jobs:
            return f"I don't have any {category} job information available at the moment. Please try refreshing the data."
        
        # Filter by experience if specified
        if experience_years is not None:
            filtered_jobs = []
            for job in jobs:
                exp = job.get('experience', '')
                if 'fresher' in exp.lower() and experience_years == 0:
                    filtered_jobs.append(job)
                elif re.search(r'(\d+)', exp):
                    job_exp = int(re.search(r'(\d+)', exp).group(1))
                    if job_exp <= experience_years:
                        filtered_jobs.append(job)
            jobs = filtered_jobs if filtered_jobs else jobs
        
        # Filter by qualification if specified
        if qualification_filter:
            filtered_jobs = [job for job in jobs if qualification_filter.lower() in job.get('qualification', '').lower()]
            jobs = filtered_jobs if filtered_jobs else jobs
        
        # Build response
        response = f"**{category} Job Notifications** 🎯\n\n"
        
        if experience_years is not None:
            response += f"*Filtered for {experience_years} years experience*\n\n"
        if qualification_filter:
            response += f"*Filtered for {qualification_filter} qualification*\n\n"
        
        for i, job in enumerate(jobs[:5], 1):
            response += f"**{i}. {job['title']}**\n"
            response += f"   🏢 Organization: {job['organization']}\n"
            response += f"   📊 Vacancies: {job['vacancies']}\n"
            response += f"   💰 Salary: {job['salary']}\n"
            response += f"   👤 Age: {job['age']}\n"
            response += f"   💼 Experience: {job['experience']}\n"
            response += f"   🎓 Qualification: {job['qualification']}\n"
            response += f"   🔗 [Apply Here]({job['url']})\n\n"
        
        return response
    
    # General queries
    if any(word in message_lower for word in ['latest', 'recent', 'new', 'all']):
        response = "**Latest Job Notifications Across All Categories** 📢\n\n"
        
        for cat, jobs in context.items():
            if jobs:
                response += f"**{cat}** ({len(jobs)} jobs)\n"
                for job in jobs[:2]:
                    response += f"  • {job['title']} - {job['organization']}\n"
                response += "\n"
        
        response += "\n💡 Ask about specific categories like:\n"
        response += "- 'Engineering jobs'\n"
        response += "- 'Science jobs with 1 year experience'\n"
        response += "- 'Commerce graduate positions'\n"
        
        return response
    
    # Help/greeting
    if any(word in message_lower for word in ['hello', 'hi', 'help', 'what can you']):
        return """👋 **Welcome to JobYaari AI Assistant!**

I can help you find Indian government job notifications in:
🔧 **Engineering** - Technical, GATE, PSU jobs
🔬 **Science** - Research, CSIR, Laboratory positions  
💼 **Commerce** - Banking, SSC, IBPS, Finance
🎓 **Education** - Teaching, KVS, NVS, University jobs

**Try asking:**
- "Show me latest Engineering jobs"
- "Science jobs with 1 year experience"
- "Commerce jobs for graduates"
- "Education qualification for KVS"

**Available filters:**
- Experience (e.g., "2 years experience")
- Qualification (e.g., "postgraduate", "diploma")
- Organization (e.g., "ISRO", "SBI")

How can I assist you today?"""
    
    # Default response
    return """I can help you find job notifications! 

Please specify:
- **Category**: Engineering, Science, Commerce, or Education
- **Filters** (optional): experience level, qualification

Example: "Show me Commerce jobs for graduates" or "Engineering jobs with 2 years experience"

Type 'help' for more information."""

@app.route('/')
def index():
    """Serve the chatbot interface"""
    return render_template('index.html')

@app.route('/health')
def health():
    """Health check endpoint"""
    total_jobs = sum(len(jobs) for jobs in KNOWLEDGE_BASE.values())
    return jsonify({
        'status': 'healthy',
        'total_jobs': total_jobs,
        'categories': list(KNOWLEDGE_BASE.keys()),
        'timestamp': datetime.now().isoformat()
    })

@app.route('/api/chat', methods=['POST'])
def chat():
    """Handle chat messages"""
    try:
        data = request.get_json()
        user_message = data.get('message', '').strip()
        
        if not user_message:
            return jsonify({'error': 'Empty message'}), 400
        
        # Check if knowledge base is loaded
        total_jobs = sum(len(jobs) for jobs in KNOWLEDGE_BASE.values())
        if total_jobs == 0:
            load_knowledge_base_from_file()
        
        # Generate response
        response = query_ai_model(user_message, KNOWLEDGE_BASE)
        
        return jsonify({
            'response': response,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        print(f"Chat error: {e}")
        return jsonify({
            'error': 'Sorry, I encountered an error processing your request.',
            'details': str(e)
        }), 500

@app.route('/api/refresh', methods=['POST'])
def refresh():
    """Refresh job data by scraping"""
    try:
        success = refresh_knowledge_base()
        
        total_jobs = sum(len(jobs) for jobs in KNOWLEDGE_BASE.values())
        
        return jsonify({
            'success': success,
            'total_jobs': total_jobs,
            'categories': {cat: len(jobs) for cat, jobs in KNOWLEDGE_BASE.items()},
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        print(f"Refresh error: {e}")
        return jsonify({
            'error': 'Failed to refresh data',
            'details': str(e)
        }), 500

@app.route('/api/jobs', methods=['GET'])
def get_jobs():
    """Get all jobs or filter by category"""
    try:
        category = request.args.get('category')
        
        if category and category in KNOWLEDGE_BASE:
            return jsonify({
                'category': category,
                'jobs': KNOWLEDGE_BASE[category],
                'count': len(KNOWLEDGE_BASE[category])
            })
        
        return jsonify({
            'categories': KNOWLEDGE_BASE,
            'total_jobs': sum(len(jobs) for jobs in KNOWLEDGE_BASE.values())
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/stats', methods=['GET'])
def get_stats():
    """Get statistics about the knowledge base"""
    try:
        stats = {
            'total_jobs': sum(len(jobs) for jobs in KNOWLEDGE_BASE.values()),
            'by_category': {},
            'last_updated': None
        }
        
        for category, jobs in KNOWLEDGE_BASE.items():
            stats['by_category'][category] = {
                'count': len(jobs),
                'jobs': [job['title'] for job in jobs[:3]]
            }
            
            if jobs and jobs[0].get('scraped_at'):
                if not stats['last_updated'] or jobs[0]['scraped_at'] > stats['last_updated']:
                    stats['last_updated'] = jobs[0]['scraped_at']
        
        return jsonify(stats)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# Initialize knowledge base on startup
print("\n" + "="*70)
print("JobYaari AI Chatbot - Starting Up")
print("="*70)

if not load_knowledge_base_from_file():
    print("⚠️ No local data found. Starting with empty knowledge base.")
    print("💡 Click 'Refresh Job Data' to scrape live data from JobYaari.com")
else:
    total = sum(len(jobs) for jobs in KNOWLEDGE_BASE.values())
    print(f"✅ Loaded {total} jobs from local storage")

print("\n🚀 Server is ready!")
print("="*70 + "\n")

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
