from flask import Flask, render_template, request, jsonify
from flask_cors import CORS
import requests
from bs4 import BeautifulSoup
import json
import os
from datetime import datetime
import re

# Import the new scraper
from scraper import JobYaariScraper

app = Flask(__name__)
CORS(app)

# --- Configuration ---
OPENROUTER_API_KEY = os.environ.get('OPENROUTER_API_KEY')
OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"
KNOWLEDGE_BASE_FILE = "knowledge_base.json"

# --- Knowledge Base ---
KNOWLEDGE_BASE = {
    "Engineering": [],
    "Science": [],
    "Commerce": [],
    "Education": []
}

def load_knowledge_base_from_file():
    """Loads the knowledge base from the JSON file if it exists."""
    global KNOWLEDGE_BASE
    try:
        if os.path.exists(KNOWLEDGE_BASE_FILE):
            with open(KNOWLEDGE_BASE_FILE, 'r', encoding='utf-8') as f:
                KNOWLEDGE_BASE = json.load(f)
                print(f"✅ Successfully loaded knowledge base from {KNOWLEDGE_BASE_FILE}")
        else:
            print(f"⚠️ Knowledge base file not found. Starting with an empty one.")
    except Exception as e:
        print(f"❌ Error loading knowledge base from file: {e}")

def refresh_knowledge_base():
    """
    Refreshes the knowledge base by scraping live data using JobYaariScraper.
    If scraping fails, it falls back to the local knowledge_base.json file.
    """
    global KNOWLEDGE_BASE
    print("🔄 Attempting to scrape live job data...")
    
    try:
        scraper = JobYaariScraper()
        scraped_data = scraper.scrape_all_categories()
        
        # Check if scraping returned any valid data
        total_jobs = sum(len(jobs) for jobs in scraped_data.values())
        
        if total_jobs > 0:
            KNOWLEDGE_BASE = scraped_data
            # Save the newly scraped data to the file for persistence
            with open(KNOWLEDGE_BASE_FILE, 'w', encoding='utf-8') as f:
                json.dump(KNOWLEDGE_BASE, f, indent=2, ensure_ascii=False)
            print(f"✅ Live data scraped successfully. Total jobs: {total_jobs}")
        else:
            print("⚠️ Scraping returned no jobs. Falling back to local data.")
            load_knowledge_base_from_file()
            
    except Exception as e:
        print(f"❌ Scraping failed: {e}. Falling back to local data.")
        load_knowledge_base_from_file()

def query_ai_model(user_message, context):
    """Queries a large language model via OpenRouter with context."""
    if not OPENROUTER_API_KEY:
        print("❌ OPENROUTER_API_KEY not set. Using simple fallback.")
        return simple_query_response(user_message, context)

    # FIX: Ensure the triple-quoted f-string is properly terminated
    system_prompt = f"""You are a helpful and friendly JobYaari assistant. Your goal is to help users find Indian job notifications.

**Current Job Data (Knowledge Base):**
```json
{json.dumps(context, indent=2)}
    
