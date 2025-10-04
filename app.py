from flask import Flask, render_template, request, jsonify
from flask_cors import CORS
import json
import os
from datetime import datetime
import re
import requests

# Import the new, production-grade scraper
from scraper import JobYaariScraper

app = Flask(__name__)
CORS(app)

# --- Configuration ---
OPENROUTER_API_KEY = os.environ.get('OPENROUTER_API_KEY')
OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"
KNOWLEDGE_BASE_FILE = "knowledge_base.json"

# --- Global In-Memory Knowledge Base ---
KNOWLEDGE_BASE = {}

def load_knowledge_base():
    """Load knowledge base from file, or initialize as empty."""
    global KNOWLEDGE_BASE
    try:
        if os.path.exists(KNOWLEDGE_BASE_FILE):
            with open(KNOWLEDGE_BASE_FILE, 'r', encoding='utf-8') as f:
                KNOWLEDGE_BASE = json.load(f)
                total = sum(len(j) for j in KNOWLEDGE_BASE.values())
                print(f"✅ Loaded {total} jobs from '{KNOWLEDGE_BASE_FILE}' into memory.")
        else:
            KNOWLEDGE_BASE = {"Engineering": [], "Science": [], "Commerce": [], "Education": []}
            print(f"⚠️ '{KNOWLEDGE_BASE_FILE}' not found. Initialized empty knowledge base.")
    except Exception as e:
        print(f"❌ Error loading knowledge base: {e}")
        KNOWLEDGE_BASE = {}

def refresh_and_scrape_data():
    """Scrapes live data using the scraper and updates the knowledge base."""
    global KNOWLEDGE_BASE
    print("🔄 Scraping live job data from JobYaari.com...")
    try:
        scraper = JobYaariScraper()
        scraped_data = scraper.scrape_all_categories()
        total_jobs = sum(len(jobs) for jobs in scraped_data.values())

        if total_jobs > 0:
            KNOWLEDGE_BASE = scraped_data
            with open(KNOWLEDGE_BASE_FILE, 'w', encoding='utf-8') as f:
                json.dump(KNOWLEDGE_BASE, f, indent=2, ensure_ascii=False)
            print(f"✅ Scrape successful. Loaded {total_jobs} new jobs.")
            return True
        else:
            print("⚠️ Scraper ran but found 0 jobs. Knowledge base not updated.")
            return False
    except Exception as e:
        print(f"❌ Scraping process failed: {e}")
        return False

def query_ai_model(user_message, context):
    """Queries the AI model with the current knowledge base as context."""
    if not OPENROUTER_API_KEY:
        return "AI model is not configured. Please set the OPENROUTER_API_KEY."

    system_prompt = f"""You are JobYaari AI Assistant, an expert chatbot for Indian government job notifications.

**Your Task:**
Answer the user's question based *only* on the real-time job data provided below in the knowledge base. Do not invent information.

**Knowledge Base (Live Scraped Data):**
```json
{json.dumps(context, indent=2)}
