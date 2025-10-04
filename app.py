# app.py
"""
Flask app that serves:
- POST /ask    -> ask the AI (uses OpenRouter free model)
- POST /refresh -> scrapes JobYaari and refreshes knowledge_base.json

Requirements:
- Set environment variable OPENROUTER_API_KEY
- Optional: install openai package that exposes OpenAI (for SDK usage).
- The scraper should be available as `from scraper import JobYaariScraper`
"""

import os
import json
import logging
from datetime import datetime
from flask import Flask, request, jsonify, render_template
from flask_cors import CORS
import requests
from typing import Dict, Any, List

# Try to import the OpenAI-style SDK (OpenRouter supports this)
try:
    from openai import OpenAI  # type: ignore
    SDK_AVAILABLE = True
except Exception:
    SDK_AVAILABLE = False

# Attempt to import your scraper class
try:
    from scraper import JobYaariScraper
except Exception:
    JobYaariScraper = None

# Configuration
OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY")
OPENROUTER_BASE = "https://openrouter.ai/api/v1"
OPENROUTER_CHAT_ENDPOINT = f"{OPENROUTER_BASE}/chat/completions"
MODEL_NAME = "deepseek/deepseek-chat-v3.1:free"  # chosen free model

KNOWLEDGE_BASE_FILE = "knowledge_base.json"
MAX_JOBS_PER_CATEGORY_IN_PROMPT = 6   # keep small to avoid token limits
TRIMMED_CONTEXT_CHARS = 15000         # rough fallback trim

# Flask setup
app = Flask(__name__, static_folder="static", template_folder="templates")
CORS(app)

# Logging
logger = logging.getLogger("jobyaari")
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger.addHandler(ch)

# In-memory knowledge base
KNOWLEDGE_BASE: Dict[str, List[Dict[str, Any]]] = {}

# Optional SDK client (if available)
sdk_client = None
if SDK_AVAILABLE and OPENROUTER_API_KEY:
    try:
        sdk_client = OpenAI(base_url=OPENROUTER_BASE, api_key=OPENROUTER_API_KEY)
        logger.info("OpenAI SDK client initialized for OpenRouter.")
    except Exception as e:
        logger.warning("OpenAI SDK available but client init failed: %s", e)
        sdk_client = None


# -------------------------
# Knowledge base utilities
# -------------------------
def load_knowledge_base():
    global KNOWLEDGE_BASE
    if os.path.exists(KNOWLEDGE_BASE_FILE):
        try:
            with open(KNOWLEDGE_BASE_FILE, "r", encoding="utf-8") as f:
                KNOWLEDGE_BASE = json.load(f)
            total = sum(len(v) for v in KNOWLEDGE_BASE.values())
            logger.info("Loaded %d jobs from %s", total, KNOWLEDGE_BASE_FILE)
        except Exception as e:
            logger.exception("Failed to load knowledge base: %s", e)
            KNOWLEDGE_BASE = {}
    else:
        KNOWLEDGE_BASE = {"Engineering": [], "Science": [], "Commerce": [], "Education": []}
        logger.info("%s not found — initialized empty knowledge base.", KNOWLEDGE_BASE_FILE)


def save_knowledge_base():
    try:
        with open(KNOWLEDGE_BASE_FILE, "w", encoding="utf-8") as f:
            json.dump(KNOWLEDGE_BASE, f, ensure_ascii=False, indent=2)
        logger.info("Saved knowledge base to %s", KNOWLEDGE_BASE_FILE)
    except Exception as e:
        logger.exception("Failed to save knowledge base: %s", e)


def trimmed_context_for_prompt(context: Dict[str, List[Dict[str, Any]]], max_per_category=MAX_JOBS_PER_CATEGORY_IN_PROMPT) -> Dict[str, List[Dict[str, Any]]]:
    """Return a small, relevant summary of the knowledge base for prompt (avoid huge dumps)."""
    out = {}
    for cat, jobs in (context or {}).items():
        summary_jobs = []
        for j in jobs[:max_per_category]:
            # Keep only a few useful fields
            summary_jobs.append({
                "title": j.get("title"),
                "organization": j.get("organization"),
                "url": j.get("url"),
                "posted": j.get("posted") or j.get("scraped_at")
            })
        out[cat] = summary_jobs
    return out


def build_system_prompt(context: Dict[str, List[Dict[str, Any]]]) -> str:
    """Build a safe, trimmed system prompt including a compact JSON of the KB."""
    trimmed = trimmed_context_for_prompt(context)
    try:
        kb_json = json.dumps(trimmed, indent=2, ensure_ascii=False)
    except Exception:
        kb_json = str(trimmed)

    # If still too large, truncate
    if len(kb_json) > TRIMMED_CONTEXT_CHARS:
        kb_json = kb_json[:TRIMMED_CONTEXT_CHARS] + "\n...TRUNCATED..."

    system_prompt = (
        "You are JobYaari AI Assistant — an assistant specialized in Indian government job notifications.\n\n"
        "Your job: Answer the user's question using ONLY the information present in the Knowledge Base below. "
        "If the answer is not present, say you don't know and optionally suggest how to find it. "
        "Do NOT hallucinate details.\n\n"
        "Knowledge Base (trimmed):\n"
        f"{kb_json}\n\n"
        "Important: reference only these jobs; when relevant, include job title, organization, posted date and URL.\n"
    )
    return system_prompt


# -------------------------
# AI Querying
# -------------------------
def query_openrouter_via_sdk(user_message: str, system_prompt: str):
    """Use the OpenAI-style SDK (if available) to query OpenRouter."""
    if not sdk_client:
        raise RuntimeError("SDK client not available")

    completion = sdk_client.chat.completions.create(
        model=MODEL_NAME,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_message}
        ],
        # optional extra params can go here (e.g., temperature, max_tokens)
        # temperature=0.0,
    )
    # SDK may return objects or dicts depending on version; handle both
    # Try to access in a safe manner:
    choices = None
    if isinstance(completion, dict):
        choices = completion.get("choices")
    else:
        choices = getattr(completion, "choices", None)
    if not choices:
        raise RuntimeError(f"Unexpected SDK response: {completion}")
    first = choices[0]
    # message may be object-like or dict-like
    msg = None
    if hasattr(first, "message"):
        msg = getattr(first.message, "content", None)
    elif isinstance(first, dict):
        msg = first.get("message", {}).get("content")
    else:
        msg = str(first)
    return msg


def query_openrouter_via_requests(user_message: str, system_prompt: str):
    """Call OpenRouter chat completions via requests (fallback)."""
    if not OPENROUTER_API_KEY:
        raise RuntimeError("OPENROUTER_API_KEY not set")

    payload = {
        "model": MODEL_NAME,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_message}
        ]
    }
    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type": "application/json"
    }
    resp = requests.post(OPENROUTER_CHAT_ENDPOINT, headers=headers, json=payload, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    # typical structure: {"choices":[{"message":{"role":"assistant","content":"..."}}], ...}
    choices = data.get("choices")
    if not choices:
        raise RuntimeError(f"Unexpected response from OpenRouter: {data}")
    first = choices[0]
    msg = first.get("message", {}).get("content") if isinstance(first, dict) else None
    return msg


def query_ai_model(user_message: str, context: Dict[str, Any]) -> str:
    """Public wrapper to query AI. Trims context and calls SDK or HTTP fallback."""
    if not OPENROUTER_API_KEY:
        return "AI model not configured. Please set OPENROUTER_API_KEY."

    system_prompt = build_system_prompt(context)

    try:
        if sdk_client:
            return query_openrouter_via_sdk(user_message, system_prompt) or "No reply from model."
        else:
            return query_openrouter_via_requests(user_message, system_prompt) or "No reply from model."
    except requests.HTTPError as e:
        logger.exception("HTTP error when contacting OpenRouter: %s", e)
        return f"AI request failed (HTTP): {e}"
    except Exception as e:
        logger.exception("Error querying AI model: %s", e)
        return f"AI request failed: {e}"


# -------------------------
# Scraper integration
# -------------------------
def refresh_and_scrape_data():
    global KNOWLEDGE_BASE
    if JobYaariScraper is None:
        logger.warning("No scraper available (scraper module not found). Cannot refresh.")
        return False, "No scraper available."

    try:
        scraper = JobYaariScraper()
        results = scraper.scrape_all_categories()
        total_jobs = sum(len(v) for v in results.values())
        if total_jobs:
            KNOWLEDGE_BASE = results
            save_knowledge_base()
            logger.info("Refreshed knowledge base. Total jobs=%d", total_jobs)
            return True, f"Refreshed {total_jobs} jobs."
        else:
            logger.info("Scraper ran but returned 0 jobs.")
            return False, "Scraper ran but returned 0 jobs."
    except Exception as e:
        logger.exception("Scraper error: %s", e)
        return False, str(e)


# -------------------------
# Flask routes
# -------------------------
@app.route("/", methods=["GET"])
def home():
    # optional: if you have an index.html in templates, render it. Otherwise return simple JSON.
    index_path = os.path.join(app.template_folder or "", "index.html")
    if os.path.exists(index_path):
        return render_template("index.html")
    return jsonify({"status": "ok", "time": datetime.utcnow().isoformat() + "Z"})


@app.route("/ask", methods=["POST"])
def route_ask():
    payload = request.get_json(force=True, silent=True) or {}
    user_message = payload.get("message") or payload.get("query") or ""
    if not user_message:
        return jsonify({"error": "Provide a 'message' in JSON body."}), 400

    logger.info("Received /ask request. Trimming KB and calling model.")
    answer = query_ai_model(user_message, KNOWLEDGE_BASE)
    return jsonify({"answer": answer})


@app.route("/refresh", methods=["POST"])
def route_refresh():
    ok, msg = refresh_and_scrape_data()
    status_code = 200 if ok else 500
    return jsonify({"success": ok, "message": msg, "total_jobs": sum(len(v) for v in KNOWLEDGE_BASE.values())}), status_code


@app.route("/kb", methods=["GET"])
def route_kb():
    """Optional: quick view of what's in memory (trimmed)"""
    return jsonify(trimmed_context_for_prompt(KNOWLEDGE_BASE))


# -------------------------
# Start app
# -------------------------
if __name__ == "__main__":
    load_knowledge_base()
    # If KB empty, optionally attempt initial refresh (comment/uncomment as you prefer)
    # If you run this where internet is blocked, leave it commented.
    # ok, msg = refresh_and_scrape_data()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), debug=False)
