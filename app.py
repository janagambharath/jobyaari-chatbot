import requests
from flask import Flask, request, jsonify
import os, json, logging
from datetime import datetime

app = Flask(__name__)

# Load knowledge base as before
KNOWLEDGE_BASE = {}
LAST_REFRESH_TIME = None
KNOWLEDGE_BASE_FILE = "knowledge_base.json"

def load_knowledge_base():
    global KNOWLEDGE_BASE, LAST_REFRESH_TIME
    if os.path.exists(KNOWLEDGE_BASE_FILE):
        with open(KNOWLEDGE_BASE_FILE, "r", encoding="utf-8") as f:
            KNOWLEDGE_BASE = json.load(f)
        LAST_REFRESH_TIME = datetime.fromtimestamp(os.path.getmtime(KNOWLEDGE_BASE_FILE))
    else:
        KNOWLEDGE_BASE = {
            "Engineering": [],
            "Science": [],
            "Commerce": [],
            "Education": []
        }

@app.before_first_request
def init():
    load_knowledge_base()

# ========== DeepSeek call helper =============

OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY")

def ask_deepseek(user_message: str):
    """Call DeepSeek-R1 free via OpenRouter"""
    if not OPENROUTER_API_KEY:
        return None, "OpenRouter API key not set"
    url = "https://openrouter.ai/api/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type": "application/json"
    }
    payload = {
        "model": "deepseek/deepseek-r1:free",
        "messages": [
            {"role": "system", "content": "You are JobYaari AI Assistant. Answer using job data."},
            {"role": "user", "content": user_message}
        ],
        "temperature": 0.3,
        "max_tokens": 500
    }
    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=20)
        resp.raise_for_status()
        data = resp.json()
        choices = data.get("choices", [])
        if choices:
            return choices[0].get("message", {}).get("content"), None
        else:
            return None, "No choices in DeepSeek response"
    except Exception as e:
        logging.exception("DeepSeek call failed")
        return None, str(e)

# ========== Modified route_chat =============

@app.route("/api/chat", methods=["POST"])
def route_chat():
    payload = request.get_json(force=True)
    user_message = payload.get("message", "").strip()
    if not user_message:
        return jsonify({"response": "Please send a message."})
    
    # Try DeepSeek first
    deepseek_ans, error = ask_deepseek(user_message)
    if deepseek_ans:
        return jsonify({"response": deepseek_ans})

    # If DeepSeek fails, fallback to your knowledge base logic
    # (Your existing fallback logic)
    lower = user_message.lower()
    for category, jobs in KNOWLEDGE_BASE.items():
        if category.lower() in lower:
            if not jobs:
                return jsonify({"response": f"No active jobs in {category} currently."})
            reply = f"Here are some {category} jobs:\n"
            for j in jobs[:5]:
                if isinstance(j, dict):
                    title = j.get("title", "")
                    org = j.get("organization", "")
                    reply += f"• {title} ({org})\n"
                else:
                    reply += f"• {j}\n"
            return jsonify({"response": reply})
    
    return jsonify({"response": "I could not find matching category. Ask about Engineering, Science, Commerce or Education."})

