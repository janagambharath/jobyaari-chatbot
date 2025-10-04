#!/usr/bin/env python3
"""
app.py
Flask app that:
- Loads knowledge_base.json
- /api/stats, /api/kb, /api/refresh (runs scrapper.scrape_latest_jobs)
- /api/chat uses DeepSeek-R1 via OpenRouter (OPENROUTER_API_KEY env var)
- Falls back to local KB if AI fails
"""

import os
import json
import logging
import time
from datetime import datetime
from functools import lru_cache
from typing import Dict, Any, List, Optional

import requests
from flask import Flask, request, jsonify, render_template
from flask_cors import CORS

# Try to import scrapper module (it should be in same folder)
try:
    import scrapper
except Exception:
    scrapper = None

# -------- config ----------
KNOWLEDGE_BASE_FILE = os.environ.get("KNOWLEDGE_BASE_FILE", "knowledge_base.json")
OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY")
OPENROUTER_CHAT_ENDPOINT = "https://openrouter.ai/api/v1/chat/completions"
DEEPSEEK_MODEL = os.environ.get("DEEPSEEK_MODEL", "deepseek/deepseek-r1:free")
TRIM_CHARS = 14000
CACHE_TTL = 300  # seconds

# -------- logging ----------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("jobyaari_app")

# -------- app ----------
app = Flask(__name__, static_folder="static", template_folder="templates")
CORS(app)

# -------- globals ----------
KNOWLEDGE_BASE: Dict[str, List[Dict[str, Any]]] = {}
LAST_REFRESH_TIME: Optional[str] = None
REQUEST_CACHE: Dict[str, tuple] = {}  # {query: (response, timestamp)}

# -------- helpers ----------
def load_kb():
    global KNOWLEDGE_BASE, LAST_REFRESH_TIME
    if os.path.exists(KNOWLEDGE_BASE_FILE):
        with open(KNOWLEDGE_BASE_FILE, "r", encoding="utf-8") as f:
            KNOWLEDGE_BASE = json.load(f)
        LAST_REFRESH_TIME = datetime.fromtimestamp(os.path.getmtime(KNOWLEDGE_BASE_FILE)).isoformat() + "Z"
        logger.info(f"Loaded KB: {sum(len(v) for v in KNOWLEDGE_BASE.values())} jobs")
    else:
        KNOWLEDGE_BASE = {k: [] for k in ["Engineering","Science","Commerce","Education","Uncategorized"]}
        LAST_REFRESH_TIME = None
        logger.warning("No knowledge_base.json found")

def trimmed_context(context: Dict) -> str:
    try:
        ctx_json = json.dumps(context, ensure_ascii=False, indent=2)
    except Exception:
        ctx_json = str(context)
    if len(ctx_json) > TRIM_CHARS:
        return ctx_json[:TRIM_CHARS] + "\n...[TRUNCATED]"
    return ctx_json

def build_system_prompt(context: Dict) -> str:
    total = sum(len(v) for v in context.values())
    if total == 0:
        return ("You are JobYaari AI Assistant. There is NO job data available. "
                "Tell the user to refresh the data and apologize briefly.")
    ctx = trimmed_context(context)
    prompt = f"""You are JobYaari AI Assistant. Answer ONLY using the job data in the knowledge base below. Do NOT hallucinate.
Knowledge base:
{ctx}

Respond concisely, include titles, orgs and urls when present. If no matching info, say so."""
    return prompt

def get_cached(query: str) -> Optional[str]:
    row = REQUEST_CACHE.get(query)
    if row:
        resp, ts = row
        if time.time() - ts < CACHE_TTL:
            logger.info("Cache hit")
            return resp
        else:
            REQUEST_CACHE.pop(query, None)
    return None

def cache_response(query: str, resp: str):
    REQUEST_CACHE[query] = (resp, time.time())

# -------- DeepSeek via OpenRouter -----------
def ask_deepseek(user_message: str, context: Dict) -> (Optional[str], Optional[str]):
    if not OPENROUTER_API_KEY:
        return None, "OPENROUTER_API_KEY not configured"
    # Check cache
    cached = get_cached(user_message)
    if cached:
        return cached, None

    system_prompt = build_system_prompt(context)
    payload = {
        "model": DEEPSEEK_MODEL,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_message}
        ],
        "temperature": 0.2,
        "max_tokens": 800
    }
    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type": "application/json"
    }
    try:
        resp = requests.post(OPENROUTER_CHAT_ENDPOINT, headers=headers, json=payload, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        choices = data.get("choices") or []
        if choices:
            content = choices[0].get("message", {}).get("content")
            if content:
                cache_response(user_message, content)
                return content, None
            return None, "Empty content"
        return None, "No choices"
    except requests.exceptions.HTTPError as e:
        logger.exception("HTTP error from OpenRouter/DeepSeek")
        return None, f"HTTP error: {e}"
    except Exception as e:
        logger.exception("DeepSeek request failed")
        return None, str(e)

# -------- routes ----------
@app.route("/", methods=["GET"])
def home():
    try:
        return render_template("index.html")
    except Exception:
        return jsonify({"status":"ok","service":"JobYaari Chatbot"})

@app.route("/api/stats", methods=["GET"])
def api_stats():
    total = sum(len(v) for v in KNOWLEDGE_BASE.values())
    by_cat = {k: {"count": len(v), "sample": [item.get("title","") for item in v[:3]]} for k,v in KNOWLEDGE_BASE.items()}
    return jsonify({
        "total_jobs": total,
        "by_category": by_cat,
        "last_updated": LAST_REFRESH_TIME
    })

@app.route("/api/kb", methods=["GET"])
def api_kb():
    return jsonify(KNOWLEDGE_BASE)

@app.route("/api/refresh", methods=["POST"])
def api_refresh():
    if scrapper is None:
        return jsonify({"success": False, "message": "scrapper module not available"}), 500
    try:
        ok = True
        data = scrapper.scrape_latest_jobs(max_per_category=int(os.environ.get("MAX_PER_CATEGORY", "7")))
        saved = scrapper.save_results(data, OUTFILE := os.environ.get("KNOWLEDGE_BASE_FILE", "knowledge_base.json"))
        # reload KB
        load_kb()
        total = sum(len(v) for v in KNOWLEDGE_BASE.values())
        return jsonify({"success": saved, "message": "Refreshed", "total_jobs": total, "categories": {k: len(v) for k,v in KNOWLEDGE_BASE.items()}})
    except Exception as e:
        logger.exception("Refresh failed")
        return jsonify({"success": False, "message": str(e)}), 500

@app.route("/api/chat", methods=["POST"])
def api_chat():
    payload = request.get_json(force=True, silent=True) or {}
    user_message = payload.get("message", "").strip()
    if not user_message:
        return jsonify({"error":"Please provide a message"}), 400

    # Try DeepSeek
    ai_resp, err = ask_deepseek(user_message, KNOWLEDGE_BASE)
    if ai_resp:
        return jsonify({"response": ai_resp})
    logger.warning(f"DeepSeek failed: {err} - falling back to KB")

    # KB fallback: simple category matching
    lower = user_message.lower()
    for cat, jobs in KNOWLEDGE_BASE.items():
        if cat.lower() in lower:
            if not jobs:
                return jsonify({"response": f"Sorry — no {cat} jobs currently."})
            lines = []
            for j in jobs[:7]:
                title = j.get("title") or ""
                org = j.get("organization") or ""
                url = j.get("url") or ""
                tup = f"• {title} — {org}" + (f" ({url})" if url else "")
                lines.append(tup)
            return jsonify({"response": "\n".join(lines)})

    # If no category keyword, search titles
    q = lower
    matches = []
    for cat, jobs in KNOWLEDGE_BASE.items():
        for j in jobs:
            if q in (j.get("title","").lower() + " " + j.get("snippet","").lower()):
                matches.append((cat, j))
                if len(matches) >= 6:
                    break
        if len(matches) >= 6:
            break
    if matches:
        lines = []
        for cat, j in matches[:6]:
            lines.append(f"• [{cat}] {j.get('title')} — {j.get('organization')} ({j.get('url')})")
        return jsonify({"response": "\n".join(lines)})

    return jsonify({"response": "I couldn't find relevant jobs. Try: 'Show latest Engineering jobs' or click Refresh Live Data."})

@app.route("/health", methods=["GET"])
def health():
    total = sum(len(v) for v in KNOWLEDGE_BASE.values())
    return jsonify({
        "status":"healthy",
        "jobs_loaded": total,
        "last_refresh": LAST_REFRESH_TIME,
        "ai_configured": bool(OPENROUTER_API_KEY)
    })

# -------- startup ----------
if __name__ == "__main__":
    logger.info("Starting JobYaari Flask App")
    load_kb()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)
