# app.py
"""
Production-Grade Flask App for JobYaari AI Chatbot
Features:
- Real-time scraping only (no fake data)
- Website structure inspector endpoint
- Robust error handling and logging
- Network resilience with retries
- Rate limiting and caching
- Health check endpoint
"""

import os
import json
import logging
import re
from datetime import datetime, timedelta
from flask import Flask, request, jsonify, render_template
from flask_cors import CORS
import requests
from typing import Dict, Any, List, Optional
from functools import lru_cache
import time

# BeautifulSoup for inspector
from bs4 import BeautifulSoup

# Try to import the OpenAI-style SDK
try:
    from openai import OpenAI
    SDK_AVAILABLE = True
except Exception:
    SDK_AVAILABLE = False

# Import scraper
try:
    from scraper import JobYaariScraperEnhanced as JobYaariScraper
except Exception:
    JobYaariScraper = None

# ============================================================
# CONFIGURATION
# ============================================================
OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY")
OPENROUTER_BASE = "https://openrouter.ai/api/v1"
OPENROUTER_CHAT_ENDPOINT = f"{OPENROUTER_BASE}/chat/completions"
MODEL_NAME = "deepseek/deepseek-chat-v3.1:free"

KNOWLEDGE_BASE_FILE = "knowledge_base.json"
MAX_JOBS_PER_CATEGORY_IN_PROMPT = 6
TRIMMED_CONTEXT_CHARS = 15000
REQUEST_TIMEOUT = 30
MAX_RETRIES = 3
CACHE_DURATION = 300  # 5 minutes

# ============================================================
# FLASK SETUP
# ============================================================
app = Flask(__name__, static_folder="static", template_folder="templates")
CORS(app)

# Enhanced Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('jobyaari.log')
    ]
)
logger = logging.getLogger("jobyaari")

# ============================================================
# GLOBAL VARIABLES
# ============================================================
KNOWLEDGE_BASE: Dict[str, List[Dict[str, Any]]] = {}
LAST_REFRESH_TIME: Optional[datetime] = None
REQUEST_CACHE: Dict[str, tuple] = {}  # {query: (response, timestamp)}

# SDK client initialization
sdk_client = None
if SDK_AVAILABLE and OPENROUTER_API_KEY:
    try:
        sdk_client = OpenAI(base_url=OPENROUTER_BASE, api_key=OPENROUTER_API_KEY)
        logger.info("✓ OpenAI SDK client initialized")
    except Exception as e:
        logger.warning(f"SDK init failed: {e}")


# ============================================================
# KNOWLEDGE BASE MANAGEMENT - REAL DATA ONLY
# ============================================================
def load_knowledge_base():
    """Load knowledge base - ONLY from scraped data file"""
    global KNOWLEDGE_BASE, LAST_REFRESH_TIME
    
    if os.path.exists(KNOWLEDGE_BASE_FILE):
        try:
            with open(KNOWLEDGE_BASE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            
            # Verify it has real data
            total = sum(len(v) for v in data.values())
            
            if total > 0:
                KNOWLEDGE_BASE = data
                LAST_REFRESH_TIME = datetime.fromtimestamp(os.path.getmtime(KNOWLEDGE_BASE_FILE))
                logger.info(f"✓ Loaded {total} REAL jobs from {KNOWLEDGE_BASE_FILE}")
                return True
            else:
                logger.warning(f"{KNOWLEDGE_BASE_FILE} exists but has 0 jobs")
                KNOWLEDGE_BASE = {}
                return False
                
        except Exception as e:
            logger.exception(f"Failed to load knowledge base: {e}")
            KNOWLEDGE_BASE = {}
            return False
    else:
        logger.warning(f"{KNOWLEDGE_BASE_FILE} not found - NO DATA LOADED")
        logger.warning("Please run scraper or use /api/refresh endpoint")
        KNOWLEDGE_BASE = {}
        return False


def save_knowledge_base():
    """Save knowledge base with backup"""
    global LAST_REFRESH_TIME
    
    try:
        # Create backup if file exists
        if os.path.exists(KNOWLEDGE_BASE_FILE):
            backup_file = f"{KNOWLEDGE_BASE_FILE}.backup"
            try:
                os.replace(KNOWLEDGE_BASE_FILE, backup_file)
            except Exception as e:
                logger.warning(f"Backup creation failed: {e}")
        
        # Save new data
        with open(KNOWLEDGE_BASE_FILE, "w", encoding="utf-8") as f:
            json.dump(KNOWLEDGE_BASE, f, ensure_ascii=False, indent=2)
        
        LAST_REFRESH_TIME = datetime.now()
        logger.info(f"✓ Saved knowledge base to {KNOWLEDGE_BASE_FILE}")
        return True
    except Exception as e:
        logger.exception(f"Failed to save knowledge base: {e}")
        return False


def _get_empty_kb():
    """Return empty knowledge base structure"""
    return {
        "Engineering": [],
        "Science": [],
        "Commerce": [],
        "Education": []
    }


def trimmed_context_for_prompt(context: Dict, max_per_category=MAX_JOBS_PER_CATEGORY_IN_PROMPT) -> Dict:
    """Create trimmed context for AI prompt"""
    out = {}
    for cat, jobs in (context or {}).items():
        summary_jobs = []
        for j in jobs[:max_per_category]:
            summary_jobs.append({
                "title": j.get("title", ""),
                "organization": j.get("organization", ""),
                "url": j.get("url", ""),
                "vacancies": j.get("vacancies", ""),
                "salary": j.get("salary", ""),
                "qualification": j.get("qualification", ""),
                "experience": j.get("experience", ""),
                "posted": j.get("posted") or j.get("scraped_at", "")
            })
        out[cat] = summary_jobs
    return out


def build_system_prompt(context: Dict) -> str:
    """Build optimized system prompt"""
    
    # Check if we have any data
    total_jobs = sum(len(v) for v in context.values())
    
    if total_jobs == 0:
        return """You are JobYaari AI Assistant. Currently, there is NO job data available in the system.

Please inform the user:
- The job database is empty
- They need to refresh the data using the "Refresh Live Data" button
- Or the scraper needs to be run to populate data
- Apologize for the inconvenience

Be polite and helpful."""
    
    trimmed = trimmed_context_for_prompt(context)
    
    try:
        kb_json = json.dumps(trimmed, indent=2, ensure_ascii=False)
    except Exception:
        kb_json = str(trimmed)
    
    if len(kb_json) > TRIMMED_CONTEXT_CHARS:
        kb_json = kb_json[:TRIMMED_CONTEXT_CHARS] + "\n...[TRUNCATED]"
    
    system_prompt = f"""You are JobYaari AI Assistant, an expert in Indian government job notifications.

INSTRUCTIONS:
- Answer using ONLY the information in the Knowledge Base below
- Be concise, accurate, and helpful
- Include job titles, organizations, and URLs when relevant
- If information is not available, say so clearly
- Format responses with bullet points for multiple jobs
- Use professional but friendly tone

KNOWLEDGE BASE (REAL-TIME DATA):
{kb_json}

Remember: Only reference these jobs. Do not hallucinate information."""
    
    return system_prompt


# ============================================================
# AI QUERY FUNCTIONS
# ============================================================
@lru_cache(maxsize=100)
def get_cached_response(query: str) -> Optional[str]:
    """Check cache for recent responses"""
    if query in REQUEST_CACHE:
        response, timestamp = REQUEST_CACHE[query]
        if (datetime.now() - timestamp).seconds < CACHE_DURATION:
            logger.info(f"Cache hit for query: {query[:50]}")
            return response
    return None


def query_openrouter_with_retry(user_message: str, system_prompt: str, retries=MAX_RETRIES) -> str:
    """Query OpenRouter with exponential backoff retry"""
    
    for attempt in range(retries):
        try:
            if sdk_client:
                return _query_via_sdk(user_message, system_prompt)
            else:
                return _query_via_requests(user_message, system_prompt)
        
        except requests.exceptions.Timeout as e:
            logger.warning(f"Timeout on attempt {attempt + 1}/{retries}: {e}")
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
                continue
            return "⏱️ Request timed out. The AI service may be busy. Please try again."
        
        except requests.exceptions.ConnectionError as e:
            logger.warning(f"Connection error on attempt {attempt + 1}/{retries}: {e}")
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
                continue
            return "🔌 Network connection error. Please check your internet and try again."
        
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                logger.warning(f"Rate limit hit on attempt {attempt + 1}")
                if attempt < retries - 1:
                    time.sleep(5 * (attempt + 1))
                    continue
                return "⚠️ Rate limit reached. Please wait a moment and try again."
            elif e.response.status_code >= 500:
                logger.error(f"Server error {e.response.status_code}")
                if attempt < retries - 1:
                    time.sleep(3 ** attempt)
                    continue
                return "🔧 AI service is temporarily unavailable. Please try again later."
            else:
                logger.error(f"HTTP error {e.response.status_code}: {e}")
                return f"❌ Request failed with error {e.response.status_code}. Please try again."
        
        except Exception as e:
            logger.exception(f"Unexpected error on attempt {attempt + 1}: {e}")
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
                continue
            return f"❌ An unexpected error occurred: {str(e)}"
    
    return "❌ Failed after multiple attempts. Please try again later."


def _query_via_sdk(user_message: str, system_prompt: str) -> str:
    """Query using OpenAI SDK"""
    completion = sdk_client.chat.completions.create(
        model=MODEL_NAME,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_message}
        ],
        temperature=0.3,
        max_tokens=1000,
        timeout=REQUEST_TIMEOUT
    )
    
    if hasattr(completion, 'choices'):
        return completion.choices[0].message.content or "No response"
    return str(completion)


def _query_via_requests(user_message: str, system_prompt: str) -> str:
    """Query using requests library"""
    payload = {
        "model": MODEL_NAME,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_message}
        ],
        "temperature": 0.3,
        "max_tokens": 1000
    }
    
    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://jobyaari-chatbot.onrender.com",
        "X-Title": "JobYaari AI Chatbot"
    }
    
    resp = requests.post(
        OPENROUTER_CHAT_ENDPOINT,
        headers=headers,
        json=payload,
        timeout=REQUEST_TIMEOUT
    )
    resp.raise_for_status()
    
    data = resp.json()
    choices = data.get("choices", [])
    if choices:
        return choices[0].get("message", {}).get("content") or "No response"
    
    raise ValueError("Invalid response format")


def query_ai_model(user_message: str, context: Dict) -> str:
    """Main AI query function with caching"""
    
    if not OPENROUTER_API_KEY:
        return "⚙️ AI model not configured. Please set OPENROUTER_API_KEY environment variable."
    
    # Check if we have data
    total_jobs = sum(len(v) for v in context.values())
    if total_jobs == 0:
        return """📭 **No job data available!**

The job database is currently empty. Please:
1. Click the **"Refresh Live Data"** button to scrape fresh jobs
2. Wait for the scraping to complete
3. Then ask your questions

The scraper will fetch real-time government job notifications from JobYaari.com."""
    
    # Check cache
    cached = get_cached_response(user_message)
    if cached:
        return cached
    
    # Build prompt and query
    system_prompt = build_system_prompt(context)
    response = query_openrouter_with_retry(user_message, system_prompt)
    
    # Cache successful responses
    if not response.startswith(("❌", "⚠️", "🔌", "⏱️", "🔧")):
        REQUEST_CACHE[user_message] = (response, datetime.now())
    
    return response


# ============================================================
# SCRAPER INTEGRATION
# ============================================================
def refresh_and_scrape_data():
    """Refresh job data with enhanced error handling"""
    global KNOWLEDGE_BASE
    
    if JobYaariScraper is None:
        logger.warning("Scraper not available")
        return False, "Scraper module not found", {}
    
    try:
        logger.info("Starting REAL-TIME scraper...")
        scraper = JobYaariScraper(timeout=20, max_retries=3)
        results = scraper.scrape_all_categories(
            categories=["Engineering", "Science", "Commerce", "Education"],
            max_jobs=7,
            delay=3
        )
        
        total_jobs = sum(len(v) for v in results.values())
        
        if total_jobs > 0:
            KNOWLEDGE_BASE = results
            save_knowledge_base()
            logger.info(f"✓ Scraped {total_jobs} REAL jobs")
            return True, f"Successfully refreshed {total_jobs} real-time jobs", results
        else:
            logger.warning("Scraper returned 0 jobs - website structure may have changed")
            return False, "Scraper found 0 jobs. Website structure may have changed. Use /api/inspect to diagnose.", {}
    
    except requests.exceptions.ConnectionError as e:
        logger.error(f"Network error during scraping: {e}")
        return False, "Network error. Cannot reach JobYaari.com", {}
    
    except Exception as e:
        logger.exception(f"Scraper error: {e}")
        return False, f"Scraper error: {str(e)}", {}


# ============================================================
# WEBSITE STRUCTURE INSPECTOR
# ============================================================
@app.route("/api/inspect", methods=["GET"])
def route_inspect():
    """Inspect JobYaari.com structure to diagnose scraping issues"""
    try:
        logger.info("Running website inspector...")
        
        url = "https://www.jobyaari.com/category/engineering"
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        response = requests.get(url, headers=headers, timeout=15)
        
        if response.status_code != 200:
            return jsonify({
                "error": f"Failed to fetch {url}",
                "status_code": response.status_code
            }), 500
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Find potential job elements
        all_elements = soup.find_all(['article', 'div', 'li'], class_=True, limit=100)
        
        job_candidates = []
        element_analysis = []
        
        for elem in all_elements[:20]:  # Analyze first 20
            tag = elem.name
            classes = ' '.join(elem.get('class', []))
            text = elem.get_text(strip=True)[:100]
            
            links = elem.find_all('a', href=True)
            
            element_info = {
                'tag': tag,
                'classes': classes,
                'text_preview': text,
                'link_count': len(links)
            }
            
            if links:
                first_link = links[0]
                link_text = first_link.get_text(strip=True)
                link_href = first_link.get('href', '')
                
                element_info['first_link_text'] = link_text[:80]
                element_info['first_link_url'] = link_href[:80]
                
                # Check if this is a job posting
                is_job = (len(link_text) > 10 and 
                         any(keyword in link_text.lower() 
                             for keyword in ['recruitment', 'notification', 'vacancy', 
                                           'job', 'exam', 'admit', 'result', '2024', '2025']))
                
                element_info['is_potential_job'] = is_job
                
                if is_job:
                    job_candidates.append({
                        'title': link_text,
                        'url': link_href,
                        'tag': tag,
                        'classes': classes
                    })
            
            element_analysis.append(element_info)
        
        # Analyze patterns
        if job_candidates:
            tag_counts = {}
            class_counts = {}
            
            for job in job_candidates:
                tag_counts[job['tag']] = tag_counts.get(job['tag'], 0) + 1
                
                for cls in job['classes'].split():
                    if cls:
                        class_counts[cls] = class_counts.get(cls, 0) + 1
            
            most_common_tag = max(tag_counts.items(), key=lambda x: x[1])[0] if tag_counts else 'div'
            most_common_class = max(class_counts.items(), key=lambda x: x[1])[0] if class_counts else 'post'
            
            recommended_selector = f"{most_common_tag}.{most_common_class}"
            
            return jsonify({
                "success": True,
                "url": url,
                "total_elements_analyzed": len(all_elements),
                "jobs_found": len(job_candidates),
                "recommended_selector": recommended_selector,
                "most_common_tag": most_common_tag,
                "most_common_class": most_common_class,
                "tag_counts": tag_counts,
                "class_counts": dict(sorted(class_counts.items(), key=lambda x: x[1], reverse=True)[:10]),
                "sample_jobs": job_candidates[:5],
                "element_analysis": element_analysis[:10],
                "fix_instructions": f"""
To fix the scraper, update scraper.py:

1. Find the _find_job_nodes method
2. Add this selector at the TOP of the selectors list:
   "{recommended_selector}"

Example:
def _find_job_nodes(self, soup: BeautifulSoup) -> List:
    selectors = [
        "{recommended_selector}",  # ADD THIS FIRST
        "article.post",
        "div.job-listing",
        # ... rest
    ]
"""
            })
        else:
            return jsonify({
                "success": False,
                "message": "No job candidates found in first 20 elements",
                "url": url,
                "total_elements_analyzed": len(all_elements),
                "element_analysis": element_analysis[:10],
                "recommendation": "Website might use JavaScript to load content. Consider using Selenium or check if URL structure changed."
            })
    
    except Exception as e:
        logger.exception(f"Inspector error: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500


# ============================================================
# FLASK ROUTES
# ============================================================
@app.route("/", methods=["GET"])
def home():
    """Serve main page"""
    try:
        return render_template("index.html")
    except Exception:
        return jsonify({
            "status": "ok",
            "service": "JobYaari AI Chatbot",
            "time": datetime.utcnow().isoformat() + "Z"
        })


@app.route("/health", methods=["GET"])
def health_check():
    """Health check endpoint for Render"""
    total_jobs = sum(len(v) for v in KNOWLEDGE_BASE.values())
    return jsonify({
        "status": "healthy",
        "jobs_loaded": total_jobs,
        "has_real_data": total_jobs > 0,
        "last_refresh": LAST_REFRESH_TIME.isoformat() if LAST_REFRESH_TIME else None,
        "ai_configured": bool(OPENROUTER_API_KEY),
        "timestamp": datetime.utcnow().isoformat() + "Z"
    })


@app.route("/api/chat", methods=["POST"])
def route_chat():
    """Main chat endpoint"""
    try:
        payload = request.get_json(force=True, silent=True) or {}
        user_message = payload.get("message", "").strip()
        
        if not user_message:
            return jsonify({"error": "Please provide a message"}), 400
        
        if len(user_message) > 500:
            return jsonify({"error": "Message too long (max 500 characters)"}), 400
        
        logger.info(f"Chat request: {user_message[:50]}...")
        answer = query_ai_model(user_message, KNOWLEDGE_BASE)
        
        return jsonify({
            "response": answer,
            "timestamp": datetime.utcnow().isoformat() + "Z"
        })
    
    except Exception as e:
        logger.exception(f"Chat error: {e}")
        return jsonify({
            "error": "Internal error",
            "response": "Sorry, I encountered an error. Please try again."
        }), 500


@app.route("/api/refresh", methods=["POST"])
def route_refresh():
    """Refresh job data endpoint"""
    try:
        logger.info("Refresh requested - will scrape REAL-TIME data")
        success, message, data = refresh_and_scrape_data()
        
        total = sum(len(v) for v in KNOWLEDGE_BASE.values())
        categories = {k: len(v) for k, v in KNOWLEDGE_BASE.items()}
        
        return jsonify({
            "success": success,
            "message": message,
            "total_jobs": total,
            "categories": categories,
            "timestamp": datetime.utcnow().isoformat() + "Z"
        }), 200 if success else 500
    
    except Exception as e:
        logger.exception(f"Refresh error: {e}")
        return jsonify({
            "success": False,
            "message": f"Refresh failed: {str(e)}",
            "total_jobs": sum(len(v) for v in KNOWLEDGE_BASE.values())
        }), 500


@app.route("/api/stats", methods=["GET"])
def route_stats():
    """Statistics endpoint"""
    try:
        total = sum(len(v) for v in KNOWLEDGE_BASE.values())
        by_category = {}
        
        for cat, jobs in KNOWLEDGE_BASE.items():
            by_category[cat] = {
                "count": len(jobs),
                "jobs": [j.get("title", "") for j in jobs[:3]]
            }
        
        return jsonify({
            "total_jobs": total,
            "has_real_data": total > 0,
            "by_category": by_category,
            "last_updated": LAST_REFRESH_TIME.isoformat() if LAST_REFRESH_TIME else None,
            "timestamp": datetime.utcnow().isoformat() + "Z"
        })
    
    except Exception as e:
        logger.exception(f"Stats error: {e}")
        return jsonify({"error": "Failed to load stats"}), 500


@app.route("/api/kb", methods=["GET"])
def route_kb():
    """View knowledge base (trimmed)"""
    try:
        return jsonify(trimmed_context_for_prompt(KNOWLEDGE_BASE))
    except Exception as e:
        logger.exception(f"KB view error: {e}")
        return jsonify({"error": "Failed to load knowledge base"}), 500


# ============================================================
# ERROR HANDLERS
# ============================================================
@app.errorhandler(404)
def not_found(e):
    return jsonify({"error": "Endpoint not found"}), 404


@app.errorhandler(500)
def internal_error(e):
    logger.exception("Internal server error")
    return jsonify({"error": "Internal server error"}), 500


# ============================================================
# STARTUP
# ============================================================
if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("🚀 Starting JobYaari AI Chatbot (REAL-TIME DATA ONLY)")
    logger.info("=" * 60)
    
    # Load knowledge base (only real data)
    load_knowledge_base()
    
    total = sum(len(v) for v in KNOWLEDGE_BASE.values())
    
    if total == 0:
        logger.warning("=" * 60)
        logger.warning("⚠️  NO DATA LOADED!")
        logger.warning("Please run one of these:")
        logger.warning("1. python scraper.py (to scrape data)")
        logger.warning("2. Use /api/refresh endpoint")
        logger.warning("3. Use /api/inspect to diagnose scraper issues")
        logger.warning("=" * 60)
    
    # Get port
    port = int(os.environ.get("PORT", 5000))
    
    logger.info(f"✓ Server starting on port {port}")
    logger.info(f"✓ Knowledge base: {total} REAL jobs loaded")
    logger.info(f"✓ AI Model: {MODEL_NAME}")
    logger.info(f"✓ Inspector endpoint: /api/inspect")
    logger.info("=" * 60)
    
    app.run(
        host="0.0.0.0",
        port=port,
        debug=False,
        threaded=True
    )
