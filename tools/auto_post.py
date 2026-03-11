import os
import re
import json
import time
import math
import html
import random
import hashlib
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Tuple, Dict, Any, Optional
 
import requests
from slugify import slugify
from openai import OpenAI
 
 
# =========================================================
# Paths
# =========================================================
ROOT = Path(__file__).resolve().parents[1]
POSTS_DIR = ROOT / "posts"
ASSETS_POSTS_DIR = ROOT / "assets" / "posts"
POSTS_JSON = ROOT / "posts.json"
KEYWORDS_JSON = ROOT / "keywords.json"
USED_IMAGES_JSON = ROOT / "used_images.json"
USED_TEXTS_JSON = ROOT / "used_texts.json"
REDIRECTS_JSON = ROOT / "redirects.json"
 
POSTS_DIR.mkdir(parents=True, exist_ok=True)
ASSETS_POSTS_DIR.mkdir(parents=True, exist_ok=True)
 
 
# =========================================================
# Config
# =========================================================
SITE_NAME = os.environ.get("SITE_NAME", "MingMong").strip()
SITE_URL = os.environ.get("SITE_URL", "https://mingmonglife.com").strip().rstrip("/")
POSTS_PER_RUN = int(os.environ.get("POSTS_PER_RUN", "3"))
IMG_COUNT = int(os.environ.get("IMG_COUNT", "7"))
MIN_REQUIRED_IMAGES = int(os.environ.get("MIN_REQUIRED_IMAGES", "7"))
VISIBLE_MIN_IMAGES = int(os.environ.get("VISIBLE_MIN_IMAGES", "5"))
EXTRA_TABLE_BUFFER = int(os.environ.get("EXTRA_TABLE_BUFFER", "2"))
COLLECT_TARGET_IMAGES = int(
    os.environ.get("COLLECT_TARGET_IMAGES", str(VISIBLE_MIN_IMAGES + EXTRA_TABLE_BUFFER))
)
print(
    f"[CONFIG] POSTS_PER_RUN={POSTS_PER_RUN} IMG_COUNT={IMG_COUNT} "
    f"MIN_REQUIRED_IMAGES={MIN_REQUIRED_IMAGES} VISIBLE_MIN_IMAGES={VISIBLE_MIN_IMAGES} "
    f"EXTRA_TABLE_BUFFER={EXTRA_TABLE_BUFFER} COLLECT_TARGET_IMAGES={COLLECT_TARGET_IMAGES}"
)
 
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "").strip()
MODEL_PLANNER = os.environ.get("MODEL_PLANNER", os.environ.get("MODEL", "gpt-4o-mini")).strip()
MODEL_WRITER = os.environ.get("MODEL_WRITER", "gpt-4.1").strip()
 
MIN_CHARS = int(os.environ.get("MIN_CHARS", "2200"))
MIN_SECTION_CHARS = int(os.environ.get("MIN_SECTION_CHARS", "420"))
MAX_KEYWORD_TRIES = int(os.environ.get("MAX_KEYWORD_TRIES", "5"))
 
HTTP_TIMEOUT = int(os.environ.get("HTTP_TIMEOUT", "35"))
ADSENSE_CLIENT = os.environ.get("ADSENSE_CLIENT", "").strip()
 
AUTHOR_NAME = os.environ.get("AUTHOR_NAME", "MingMong Editorial").strip()
AUTHOR_URL = os.environ.get("AUTHOR_URL", f"{SITE_URL}/about.html").strip()
SITE_TAGLINE = os.environ.get(
    "SITE_TAGLINE",
    "Practical guides for AI tools, investing, productivity, software, and extra income."
).strip()
 
TITLE_SIM_THRESHOLD = float(os.environ.get("TITLE_SIM_THRESHOLD", "0.83"))
KEYWORD_SIM_THRESHOLD = float(os.environ.get("KEYWORD_SIM_THRESHOLD", "0.74"))
TOPIC_SIM_THRESHOLD = float(os.environ.get("TOPIC_SIM_THRESHOLD", "0.70"))
MIN_KEYWORD_POOL = int(os.environ.get("MIN_KEYWORD_POOL", "18"))
 
GOOGLE_SUGGEST_ENABLED = os.environ.get("GOOGLE_SUGGEST_ENABLED", "1").strip() == "1"
GOOGLE_SUGGEST_MAX_SEEDS = int(os.environ.get("GOOGLE_SUGGEST_MAX_SEEDS", "4"))
GOOGLE_SUGGEST_PER_QUERY = int(os.environ.get("GOOGLE_SUGGEST_PER_QUERY", "4"))
GOOGLE_SUGGEST_SCORE_THRESHOLD = float(os.environ.get("GOOGLE_SUGGEST_SCORE_THRESHOLD", "0.28"))
 
SERPAPI_KEY = os.environ.get("SERPAPI_KEY", "").strip()
SERPAPI_ENGINE = os.environ.get("SERPAPI_ENGINE", "google").strip()
SERP_CHECK_ENABLED = os.environ.get("SERP_CHECK_ENABLED", "1").strip() == "1"
SERP_CHECK_LIMIT = int(os.environ.get("SERP_CHECK_LIMIT", "10"))
 
UNSPLASH_ACCESS_KEY = os.environ.get("UNSPLASH_ACCESS_KEY", "").strip()
PEXELS_API_KEY = os.environ.get("PEXELS_API_KEY", "").strip()
PIXABAY_API_KEY = os.environ.get("PIXABAY_API_KEY", "").strip()
ENABLE_WIKIMEDIA = os.environ.get("ENABLE_WIKIMEDIA", "1").strip() == "1"
 
UNSPLASH_MIN_WIDTH = int(os.environ.get("UNSPLASH_MIN_WIDTH", "1400"))
UNSPLASH_MIN_HEIGHT = int(os.environ.get("UNSPLASH_MIN_HEIGHT", "900"))
UNSPLASH_MIN_LIKES = int(os.environ.get("UNSPLASH_MIN_LIKES", "10"))
UNSPLASH_PER_PAGE = int(os.environ.get("UNSPLASH_PER_PAGE", "30"))
 
PEXELS_MIN_WIDTH = int(os.environ.get("PEXELS_MIN_WIDTH", "1400"))
PEXELS_MIN_HEIGHT = int(os.environ.get("PEXELS_MIN_HEIGHT", "900"))
PEXELS_PER_PAGE = int(os.environ.get("PEXELS_PER_PAGE", "30"))
 
PIXABAY_MIN_WIDTH = int(os.environ.get("PIXABAY_MIN_WIDTH", "1400"))
PIXABAY_MIN_HEIGHT = int(os.environ.get("PIXABAY_MIN_HEIGHT", "900"))
PIXABAY_PER_PAGE = int(os.environ.get("PIXABAY_PER_PAGE", "50"))
 
IMAGE_SOURCE_PRIORITY = [
    "unsplash",
    "pexels",
    "pixabay",
    "wikimedia",
]
 
RELATED_POST_LIMIT = int(os.environ.get("RELATED_POST_LIMIT", "3"))
 
CLUSTER_MODE = os.environ.get("CLUSTER_MODE", "1").strip() == "1"
CLUSTER_BATCH = int(os.environ.get("CLUSTER_BATCH", "12"))
CLUSTER_ROTATION_WINDOW = int(os.environ.get("CLUSTER_ROTATION_WINDOW", "18"))
TOPIC_CLUSTERS_JSON = os.environ.get("TOPIC_CLUSTERS_JSON", "").strip()
PILLAR_INTERVAL = int(os.environ.get("PILLAR_INTERVAL", "6"))
 
SECTION_COUNT_MIN = int(os.environ.get("SECTION_COUNT_MIN", "4"))
SECTION_COUNT_MAX = int(os.environ.get("SECTION_COUNT_MAX", "7"))
 
SEARCH_JS_VERSION = hashlib.sha1(str(int(time.time() // 3600)).encode("utf-8")).hexdigest()[:8]
BUILD_ID = hashlib.sha1(f"{datetime.now(timezone.utc).isoformat()}-{random.random()}".encode("utf-8")).hexdigest()[:10]
 
 
# =========================================================
# Policy
# =========================================================
ALLOWED_CATEGORIES = {
    "AI Tools",
    "Investing",
    "Make Money",
    "Productivity",
    "Software Reviews",
    "Side Hustles",
}
 
BANNED_TITLE_PATTERNS = [
    "ultimate guide",
    "comprehensive guide",
    "essential guide",
    "must-have",
    "must have",
    "complete guide",
    "top productivity tools",
    "top ai tools for",
    "best tools for everyone",
    "best apps for everyone",
    "top tools for everyone",
]
 
BANNED_OPENING_PHRASES = [
    "ai is transforming",
    "in today's fast-paced world",
    "in today’s fast-paced world",
    "in today's digital world",
    "in today’s digital world",
    "productivity is important",
    "there are many tools available",
    "in the modern workplace",
    "investing can be intimidating",
    "making money online has become popular",
"freelancers often need",
    "choosing the right tool",
    "client retention is important",
    "many businesses struggle",
    "one of the most important things",
    "this article will explore",
]
 
REQUIRED_CONTENT_SIGNALS = [
    "mistake",
    "tradeoff",
    "decision",
    "step",
]
 
MODE_REQUIRED_SIGNALS = {
    "workflow": ["workflow", "checklist", "mistake", "tradeoff", "decision", "step"],
    "review": ["pricing", "pros", "cons", "best for", "not ideal", "decision"],
    "investing": ["risk", "volatility", "long term", "beginner", "watch", "decision"],
    "money": ["income", "effort", "time", "mistake", "decision", "step"],
}
DEPTH_REQUIRED_SIGNALS = [
    "hidden reason",
    "friction",
    "re-engagement",
    "repeat client",
    "retention metric",
    "workflow",
    "decision",
    "tradeoff",
    "mistake",
    "example",
]

ENGAGEMENT_REQUIRED_SIGNALS = [
    "why this matters",
    "here is the catch",
    "most people think",
    "what actually happens",
    "for example",
    "in practice",
    "the real reason",
    "the problem is not",
]

RETENTION_SYSTEM_SIGNALS = [
    "onboarding system",
    "communication cadence",
    "progress visibility",
    "offboarding",
    "reactivation",
    "follow-up",
    "check-in",
    "renewal",
    "repeat booking",
]

TIME_BASED_SIGNAL_PATTERNS = [
    "7 days",
    "14 days",
    "30 days",
    "weekly",
    "monthly",
]

WEAK_SECTION_HEADINGS = [
    "who this is for",
    "the practical approach",
    "decision framework",
    "final recommendation",
    "step by step setup",
    "template checklist",
    "tradeoffs and limitations",
]

BANNED_SHALLOW_ADVICE = [
    "choose the right tool",
    "compare features",
    "test free trials",
    "there are many tools available",
    "streamline your workflow",
    "improve efficiency",
    "boost productivity",
]

REVIEW_COMPARISON_REQUIRED = [
    "price",
    "free plan",
    "setup difficulty",
    "automation",
    "best for",
    "not ideal for",
]

FREELANCER_SEGMENTS = [
    "solo freelancer",
    "designer",
    "developer",
    "copywriter",
    "consultant",
    "video editor",
    "virtual assistant",
    "coach",
]

REALISM_SIGNALS = [
    "in practice",
    "what happened when",
    "where this breaks down",
    "setup friction",
    "hidden cost",
    "manual workaround",
    "handoff issue",
    "client delay",
    "approval bottleneck",
    "renewal risk",
]

COMMERCIAL_DEPTH_SIGNALS = [
    "pricing reality",
    "free plan",
    "paid plan",
    "upgrade point",
    "budget",
    "monthly cost",
    "annual cost",
    "worth paying for",
    "best value",
    "overkill",
]

CTA_SIGNALS = [
    "start with",
    "compare",
    "read the full",
    "use this checklist",
    "next step",
    "before you choose",
]

INTERNAL_LINK_INTENT_SIGNALS = [
    "vs",
    "alternatives",
    "best for",
    "pricing",
    "free plan",
    "template",
    "checklist",
    "workflow",
]

SECTION_BLUEPRINTS = [
    [
        "who this is for and the exact problem",
        "why common advice fails",
        "the practical framework",
        "the setup or decision process",
        "examples and edge cases",
        "mistakes and tradeoffs",
        "clear recommendation or checklist",
    ],
    [
        "who this is for and the trigger moment",
        "what makes the choice expensive",
        "the comparison or workflow logic",
        "implementation steps or evaluation factors",
        "decision rules",
        "mistakes and failure modes",
        "when not to use this",
        "copyable checklist or summary framework",
    ],
    [
        "who this is for",
        "the broken default approach",
        "the practical approach",
        "step by step setup or evaluation",
        "decision framework",
        "tradeoffs and limitations",
        "template checklist or final recommendation",
    ],
]
 
DEFAULT_TOPIC_CLUSTERS = {
    "AI Tools": [
        "ai tools to make money online",
        "best ai tools for remote work",
        "ai tools for small business automation",
        "ai tools for creators",
        "chatgpt workflow for productivity",
        "best ai writing tools for marketers",
        "ai tools for side hustle",
        "ai tools that save time at work",
    ],
    "Investing": [
        "how to start investing with 100 dollars a month",
        "simple 3 etf portfolio for beginners",
        "how to choose your first etf as a beginner",
        "monthly investing plan for beginners",
        "s and p 500 vs total market etf for beginners",
        "how to build a beginner portfolio with 500 dollars",
        "best simple portfolio allocation for beginners",
        "how to start a long term portfolio without picking stocks",
    ],
    "Make Money": [
        "how to make money with ai tools",
        "how to make money selling digital products",
        "best online side hustles for beginners",
        "passive income ideas for young professionals",
        "how to make money with a niche blog",
        "how to earn extra income after work",
        "how to sell templates online",
        "small online business ideas with low startup cost",
    ],
    "Productivity": [
        "best note taking system for busy professionals",
        "weekly planning system for knowledge workers",
        "how to organize tasks across multiple projects",
        "email management system for busy workers",
        "focus system for remote workers",
        "meeting notes to task workflow",
        "how to stop context switching at work",
        "daily work reset checklist",
    ],
    "Software Reviews": [
        "notion vs clickup for solo business",
        "best email marketing tools for beginners",
        "best invoicing software for freelancers",
        "best crm for solo consultants",
        "best project management software for small teams",
        "otter ai alternatives for meeting notes",
        "best budget apps for young professionals",
        "best ai note taking tools",
    ],
    "Side Hustles": [
        "side hustles with ai tools",
        "side hustles for engineers",
        "weekend side hustles for full time workers",
        "low cost side hustle ideas for beginners",
        "digital side hustles that can scale",
        "best side hustles for remote workers",
        "how to start a side hustle after work",
        "side hustles that do not require inventory",
    ],
}
 
DEFAULT_PILLAR_TOPICS = {
    "AI Tools": [
        "how to choose ai tools that actually save time",
        "practical ai tools for work and side income",
        "how to build a useful ai stack for one person businesses",
        "ai tool buying guide for beginners",
    ],
    "Investing": [
        "simple long term investing plan for beginners",
        "how beginners can build a portfolio with monthly contributions",
        "how to choose a simple etf portfolio as a beginner",
        "beginner guide to starting a long term portfolio with small amounts",
    ],
    "Make Money": [
        "how to build extra income with digital systems",
        "practical online income ideas for beginners",
        "how to create repeatable income streams online",
        "how to start earning extra money after work",
    ],
    "Productivity": [
        "how to build a realistic productivity system for work",
        "practical productivity systems for busy professionals",
        "how to organize work without burning out",
        "productivity framework for people with too many tasks",
    ],
    "Software Reviews": [
        "how to choose software for a small business",
        "practical software buying guide for solo operators",
        "how to compare business software without wasting money",
        "software review framework for beginners",
    ],
    "Side Hustles": [
        "how to choose a side hustle that fits your schedule",
        "practical side hustle ideas for beginners",
        "how to start a side hustle while working full time",
        "side hustle framework for young professionals",
    ],
}
 
# =========================================================
# Logging
# =========================================================
def log(stage: str, message: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] [{stage}] {message}")
 
 
# =========================================================
# OpenAI
# =========================================================
def _get_openai_client() -> OpenAI:
    if not OPENAI_API_KEY:
        raise RuntimeError("Missing OPENAI_API_KEY")
    return OpenAI(api_key=OPENAI_API_KEY)
 
 
def openai_generate_text(prompt: str, model: str, temperature: float = 0.5) -> str:
    client = _get_openai_client()
    prompt_len = len(prompt)

    started = time.time()
    try:
        res = client.responses.create(
            model=model,
            input=prompt,
        )
        text = (getattr(res, "output_text", None) or "").strip()
        elapsed = round(time.time() - started, 2)
        log("OPENAI", f"responses.create model={model} prompt_len={prompt_len} output_len={len(text)} elapsed={elapsed}s")
        if text:
            return text
    except Exception as e:
        elapsed = round(time.time() - started, 2)
        log("OPENAI", f"responses.create failed model={model} elapsed={elapsed}s error={e}")

    started = time.time()
    try:
        res = client.chat.completions.create(
            model=model,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You write editorial-quality practical articles for real readers, not generic SEO filler. "
                     "You avoid repetition, generic introductions, bland section headings, and vague advice. "
                     "You make concrete decisions, tradeoffs, examples, scenarios, and operational detail visible in the first draft. "
                     "When asked for JSON you return strict JSON only."
                    ),
                },
                {"role": "user", "content": prompt},
            ],
            temperature=temperature,
        )
        text = (res.choices[0].message.content or "").strip()
        elapsed = round(time.time() - started, 2)
        log("OPENAI", f"chat.completions.create model={model} prompt_len={prompt_len} output_len={len(text)} elapsed={elapsed}s")
        return text
    except Exception as e:
        elapsed = round(time.time() - started, 2)
        raise RuntimeError(f"OpenAI call failed on model={model} elapsed={elapsed}s: {e}")
 
 
# =========================================================
# Date helpers
# =========================================================
def now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
 
 
def now_utc_date() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")
 
 
def current_year_utc() -> int:
    return int(datetime.now(timezone.utc).strftime("%Y"))
 
 
# =========================================================
# JSON and filesystem helpers
# =========================================================
def load_json(path: Path, default):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default
 
 
def save_json(path: Path, obj) -> None:
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
 
 
def safe_write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
 
 
def short_desc(text: str) -> str:
    t = (text or "").strip()
    if len(t) > 160:
        t = t[:157].rstrip() + "..."
    return t
 
 
def ensure_used_schema(raw):
    if isinstance(raw, dict):
        if "asset_ids" not in raw or not isinstance(raw.get("asset_ids"), list):
            raw["asset_ids"] = []
        return raw
    if isinstance(raw, list):
        return {"asset_ids": [x for x in raw if isinstance(x, str)]}
    return {"asset_ids": []}
 
 
def ensure_used_texts_schema(raw):
    if isinstance(raw, dict):
        if "fingerprints" not in raw or not isinstance(raw.get("fingerprints"), list):
            raw["fingerprints"] = []
        return raw
    if isinstance(raw, list):
        return {"fingerprints": [x for x in raw if isinstance(x, str)]}
    return {"fingerprints": []}
 
 
def _clean_text(s: str) -> str:
    s = s.replace("*", "")
    s = (s or "").replace("\r\n", "\n").replace("\r", "\n").strip()
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()
 
 
def _find_balanced_json(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return s
 
    s = re.sub(r"^```(?:json)?\s*", "", s, flags=re.IGNORECASE)
    s = re.sub(r"\s*```$", "", s)
 
    start_positions = [i for i, ch in enumerate(s) if ch in "{["]
    for start in start_positions:
        opener = s[start]
        closer = "}" if opener == "{" else "]"
        depth = 0
        in_string = False
        escape = False
 
        for i in range(start, len(s)):
            ch = s[i]
 
            if in_string:
                if escape:
                    escape = False
                elif ch == "\\":
                    escape = True
                elif ch == '"':
                    in_string = False
                continue
 
            if ch == '"':
                in_string = True
                continue
 
            if ch == opener:
                depth += 1
            elif ch == closer:
                depth -= 1
                if depth == 0:
                    candidate = s[start:i + 1]
                    try:
                        json.loads(candidate)
                        return candidate
                    except Exception:
                        break
 
    return s
 
 
def html_escape(s: str) -> str:
    return html.escape(s or "", quote=True)
 
 
# =========================================================
# Text normalization and similarity
# =========================================================
def _norm_title(s: str) -> str:
    s = (s or "").lower().strip()
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s
 
 
def normalize_keyword(s: str) -> str:
    s = (s or "").lower().strip()
    s = s.replace("&", " and ")
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s
 
 
def title_too_similar(new_title: str, existing_titles: List[str], threshold: float) -> bool:
    nt = _norm_title(new_title)
    if not nt:
        return True
    for old in existing_titles[:600]:
        oo = _norm_title(old)
        if not oo:
            continue
        if oo == nt:
            return True
        if similarity_ratio(nt, oo) >= threshold:
            return True
    return False
 
 
def keyword_too_similar(a: str, b: str, threshold: float = KEYWORD_SIM_THRESHOLD) -> bool:
    na = normalize_keyword(a)
    nb = normalize_keyword(b)
    if not na or not nb:
        return False
    if na == nb:
        return True
    return similarity_ratio(na, nb) >= threshold
 
 
def similarity_ratio(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    if a == b:
        return 1.0
    set_a = set(a.split())
    set_b = set(b.split())
    inter = len(set_a & set_b)
    union = len(set_a | set_b)
    jaccard = inter / union if union else 0.0
    prefix_bonus = 0.12 if a[:24] == b[:24] else 0.0
    return min(1.0, jaccard + prefix_bonus)
 
 
def token_signature(text: str, top_n: int = 12) -> str:
    words = [
        w for w in normalize_keyword(text).split()
        if len(w) > 2 and w not in {
            "this", "that", "with", "from", "into", "using", "your", "their",
            "have", "will", "what", "when", "where", "which", "about", "guide",
            "workflow", "system", "checklist", "template", "playbook", "steps",
        }
    ]
    counts = Counter(words)
    ranked = sorted(counts.items(), key=lambda x: (-x[1], x[0]))[:top_n]
    return "|".join([w for w, _ in ranked])
 
 
def semantic_overlap_score(a: str, b: str) -> float:
    na = normalize_keyword(a)
    nb = normalize_keyword(b)
    if not na or not nb:
        return 0.0
 
    wa = set(na.split())
    wb = set(nb.split())
    inter = len(wa & wb)
    union = len(wa | wb)
    jaccard = inter / union if union else 0.0
 
    sig_a = token_signature(a)
    sig_b = token_signature(b)
    sig_score = similarity_ratio(sig_a, sig_b)
 
    return round((jaccard * 0.62) + (sig_score * 0.38), 4)


def has_table_like_text(text: str) -> bool:
    lines = [ln.strip() for ln in (text or "").splitlines() if ln.strip()]

    pipe_lines = [ln for ln in lines if "|" in ln]
    if len(pipe_lines) >= 2:
        return True

    for i in range(len(lines) - 1):
        line = lines[i]
        next_line = lines[i + 1]

        if "|" in line and "|" in next_line:
            cleaned = next_line.replace("|", "").replace(":", "").replace("-", "").strip()
            if cleaned == "":
                return True

    return False

def enforce_comparison_visuals(data: Dict[str, Any], keyword: str = "") -> Dict[str, Any]:
    intent_type = (data.get("intent_type") or "").strip().lower()
    sections = data.get("sections", [])

    if not isinstance(sections, list):
        return data

    for s in sections:
        body = s.get("body", "") or ""
        s["body"] = strip_markdown_tables(body)

    if intent_type != "comparison":
        return data

    target_idx = None

    for i, s in enumerate(sections):
        heading = (s.get("heading", "") or "").lower()
        body = (s.get("body", "") or "").lower()

        if any(x in heading for x in ["difference", "differences", "compare", "comparison", "vs", "which"]) \
           or any(x in body for x in [
               "best for",
               "not ideal for",
               "setup difficulty",
               "free plan",
               "pricing reality",
               "price",
               "automation"
           ]):
            target_idx = i
            break

    if target_idx is None and sections:
        target_idx = 2

    if target_idx is not None:
        sec = sections[target_idx]
        sec["visual_type"] = "diagram"
        sec["alt_text"] = sec.get("alt_text") or f"{keyword} comparison infographic"
        sec["image_query"] = f"{keyword} comparison infographic clean minimal chart"

    data["sections"] = sections
    return data


def strip_markdown_tables(text: str) -> str:
    lines = text.splitlines()
    out = []
    i = 0

    while i < len(lines):
        line = lines[i]
        next_line = lines[i + 1] if i + 1 < len(lines) else ""

        is_separator = False
        if "|" in next_line:
            cleaned = next_line.replace("|", "").replace(":", "").replace("-", "").strip()
            is_separator = cleaned == ""

        is_table_start = "|" in line and "|" in next_line and is_separator

        if is_table_start:
            i += 2
            while i < len(lines) and "|" in lines[i]:
                i += 1
            continue

        out.append(line)
        i += 1

    return "\n".join(out).strip()

 
# =========================================================
# Keyword quality
# =========================================================
def is_search_intent_keyword(keyword: str) -> bool:
    k = normalize_keyword(keyword)
    if not k:
        return False
 
    words = k.split()
    if len(words) < 4 or len(words) > 18:
        return False

    if len(words) < 5 and not any(tok in k for tok in ["vs", "best", "review"]):
        return False
 
    if re.search(r"\b(2019|2020|2021|2022|2023|2024)\b", k):
        return False
 
    broad_bad = {
        "ai",
        "productivity",
        "investing",
        "money",
        "software",
        "stocks",
        "etf",
        "chatgpt",
    }
    if k in broad_bad:
        return False
 
    intent_tokens = [
        "workflow",
        "system",
        "playbook",
        "template",
        "checklist",
        "process",
        "automation",
        "set up",
        "setup",
        "how to",
        "reduce",
        "save time",
        "best",
        "vs",
        "review",
        "worth it",
        "for beginners",
        "make money",
        "extra income",
        "side hustle",
        "stocks",
        "etf",
        "dividend",
        "software",
        "crm",
        "invoicing",
        "budget app",
        "alternatives",
        "template",
        "script",
        "email example",
        "alternatives",
        "pricing",
        "free plan",
        "client retention",
        "repeat clients",
    ]
    if not any(tok in k for tok in intent_tokens):
        return False
 
    return True
 
def dedupe_keywords(keywords: List[str], existing_titles: List[str], existing_keywords: List[str]) -> List[str]:
    out: List[str] = []
    seen_norm = set()
 
    baseline = []
    for x in existing_titles[:800]:
        if isinstance(x, str) and x.strip():
            baseline.append(x)
    for x in existing_keywords[:1500]:
        if isinstance(x, str) and x.strip():
            baseline.append(x)
 
    for kw in keywords:
        kw = _clean_text(kw)
        if not kw or not is_search_intent_keyword(kw):
            continue
 
        n = normalize_keyword(kw)
        if not n or n in seen_norm:
            continue
 
        skip = False
        for ex in baseline:
            if keyword_too_similar(kw, ex):
                skip = True
                break
            if semantic_overlap_score(kw, ex) >= TOPIC_SIM_THRESHOLD:
                skip = True
                break
        if skip:
            continue
 
        for kept in out:
            if keyword_too_similar(kw, kept):
                skip = True
                break
            if semantic_overlap_score(kw, kept) >= TOPIC_SIM_THRESHOLD:
                skip = True
                break
        if skip:
            continue
 
        seen_norm.add(n)
        out.append(kw)
 
    return out
 
 
# =========================================================
# Search opportunity validation
# =========================================================
def fetch_google_suggest(query: str) -> List[str]:
    if not query or not GOOGLE_SUGGEST_ENABLED:
        return []
 
    try:
        url = "https://suggestqueries.google.com/complete/search"
        params = {"client": "firefox", "q": query}
        r = requests.get(url, params=params, timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        if isinstance(data, list) and len(data) > 1 and isinstance(data[1], list):
            out = []
            for item in data[1][:GOOGLE_SUGGEST_PER_QUERY]:
                if isinstance(item, str) and item.strip():
                    out.append(item.strip())
            return out
    except Exception as e:
        log("SUGGEST", f"Suggest fetch failed for '{query}': {e}")
    return []
 
 
def serpapi_search(query: str) -> Dict[str, Any]:
    if not SERPAPI_KEY or not SERP_CHECK_ENABLED:
        return {}
 
    try:
        r = requests.get(
            "https://serpapi.com/search.json",
            params={
                "engine": SERPAPI_ENGINE,
                "q": query,
                "api_key": SERPAPI_KEY,
                "num": 10,
                "hl": "en",
                "gl": "us",
            },
            timeout=HTTP_TIMEOUT,
        )
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log("SERP", f"SerpAPI failed for '{query}': {e}")
        return {}
 
 
def compute_keyword_opportunity(keyword: str, existing_titles: List[str]) -> Tuple[float, Dict[str, Any]]:
    base = normalize_keyword(keyword)
    suggests = fetch_google_suggest(keyword)
    exact_hits = sum(1 for s in suggests if normalize_keyword(s) == base)
    broad_penalty = 0.18 if len(base.split()) < 5 else 0.0
    suggest_richness = len(suggests) / max(GOOGLE_SUGGEST_PER_QUERY, 1)
 
    diversity_terms = 0
    if suggests:
        token_pool = set()
        for s in suggests:
            token_pool.update(normalize_keyword(s).split())
        diversity_terms = len(token_pool)
 
    overlap_penalty = 0.0
    for t in existing_titles[:300]:
        overlap_penalty = max(overlap_penalty, semantic_overlap_score(keyword, t) * 0.9)
 
    serp_penalty = 0.0
    serp_good = 0.0
    serp_checked = False
 
    if SERPAPI_KEY and SERP_CHECK_ENABLED:
        serp_checked = True
        serp = serpapi_search(keyword)
        organic = serp.get("organic_results") or []
        titles = []
        for item in organic[:10]:
            title = (item.get("title") or "").strip()
            link = (item.get("link") or "").strip().lower()
            if title:
                titles.append(title)
            if any(d in link for d in ["forbes.com", "hubspot.com", "zapier.com", "shopify.com", "semrush.com"]):
                serp_penalty += 0.12
            if "reddit.com" in link or "medium.com" in link:
                serp_good += 0.06
 
        for t in titles[:8]:
            serp_penalty = max(serp_penalty, semantic_overlap_score(keyword, t) * 0.55)
 
    score = (
        (suggest_richness * 1.2) +
        (min(diversity_terms / 18.0, 1.0) * 0.8) +
        (exact_hits * 0.08) +
        serp_good -
        broad_penalty -
        overlap_penalty -
        serp_penalty
    )
 
    details = {
        "suggest_count": len(suggests),
        "suggests": suggests[:8],
        "diversity_terms": diversity_terms,
        "exact_hits": exact_hits,
        "broad_penalty": broad_penalty,
        "overlap_penalty": round(overlap_penalty, 3),
        "serp_checked": serp_checked,
        "serp_penalty": round(serp_penalty, 3),
        "score": round(score, 3),
    }
    return round(score, 3), details
 
 
def filter_keywords_by_opportunity(keywords: List[str], existing_titles: List[str]) -> List[str]:
    ranked = []
    checked = 0
 
    for kw in keywords:
        if checked >= SERP_CHECK_LIMIT and not SERPAPI_KEY:
            ranked.append((GOOGLE_SUGGEST_SCORE_THRESHOLD, kw))
            continue
 
        score, info = compute_keyword_opportunity(kw, existing_titles)
        checked += 1
        log("KW", f"'{kw}' score={score} details={json.dumps(info, ensure_ascii=False)}")
        if score >= GOOGLE_SUGGEST_SCORE_THRESHOLD:
            ranked.append((score, kw))
 
    ranked.sort(key=lambda x: x[0], reverse=True)

    result = [kw for _, kw in ranked]

    # fallback
    if not result:
        log("KW", "All keywords filtered out. Falling back to original list.")
        return keywords[:min(len(keywords), 20)]

    return result
 
 
# =========================================================
# Topic clusters
# =========================================================
def load_topic_clusters() -> Dict[str, List[str]]:
    if TOPIC_CLUSTERS_JSON:
        try:
            raw = json.loads(TOPIC_CLUSTERS_JSON)
            if isinstance(raw, dict):
                out = {}
                for k, v in raw.items():
                    if isinstance(k, str) and isinstance(v, list):
                        out[k] = [str(x).strip() for x in v if str(x).strip()]
                if out:
                    return out
        except Exception as e:
            log("CLUSTER", f"TOPIC_CLUSTERS_JSON parse failed: {e}")
    return DEFAULT_TOPIC_CLUSTERS
 
 
def get_existing_keywords_from_posts(posts: List[dict]) -> List[str]:
    out = []
    for p in posts:
        if not isinstance(p, dict):
            continue
        for key in ["keyword", "title", "slug", "description", "audience", "problem"]:
            val = p.get(key)
            if isinstance(val, str) and val.strip():
                out.append(val.strip())
    return out
 
 
def pick_next_cluster(posts: List[dict], topic_clusters: Dict[str, List[str]]) -> str:
    names = list(topic_clusters.keys())
    if not names:
        return "AI Tools"
 
    recent = posts[:CLUSTER_ROTATION_WINDOW] if posts else []
    counts = {name: 0 for name in names}
 
    for p in recent:
        if not isinstance(p, dict):
            continue
        cluster = p.get("cluster")
        if isinstance(cluster, str) and cluster in counts:
            counts[cluster] += 1
 
    min_count = min(counts.values()) if counts else 0
    candidates = [name for name, c in counts.items() if c == min_count]
    return random.choice(candidates) if candidates else names[0]
 
 
def cluster_recent_saturation(posts: List[dict], cluster_name: str, window: int = 10) -> int:
    recent = posts[:window]
    return sum(1 for p in recent if isinstance(p, dict) and p.get("cluster") == cluster_name)
 
 
def should_make_pillar(posts: List[dict], cluster_name: str) -> bool:
    cluster_posts = [p for p in posts if isinstance(p, dict) and p.get("cluster") == cluster_name]
    if not cluster_posts:
        return True
 
    if not any(p.get("post_type") == "pillar" for p in cluster_posts):
        return True
 
    regular_count = sum(1 for p in cluster_posts if p.get("post_type") != "pillar")
    if regular_count > 0 and regular_count % max(PILLAR_INTERVAL, 1) == 0:
        recent_cluster = cluster_posts[:6]
        if not any(p.get("post_type") == "pillar" for p in recent_cluster):
            return True
    return False
 
 
def get_cluster_pillar(posts: List[dict], cluster_name: str) -> dict:
    for p in posts:
        if not isinstance(p, dict):
            continue
        if p.get("cluster") == cluster_name and p.get("post_type") == "pillar":
            return p
    return {}
 
 
# =========================================================
# Category mapping
# =========================================================
def cluster_to_category(cluster_name: str, keyword: str = "", post_type: str = "") -> str:
    c = (cluster_name or "").strip().lower()
    k = (keyword or "").strip().lower()
 
    if c == "ai tools":
        return "AI Tools"
    if c == "investing":
        return "Investing"
    if c == "make money":
        return "Make Money"
    if c == "productivity":
        return "Productivity"
    if c == "software reviews":
        return "Software Reviews"
    if c == "side hustles":
        return "Side Hustles"
 
    comparison_tokens = [" vs ", "versus", "compare", "comparison", "alternative", "alternatives", "review", "worth it"]
    if any(x in k for x in comparison_tokens):
        return "Software Reviews"
 
    if any(x in k for x in [
        "stock", "stocks", "etf", "etfs", "dividend", "portfolio", "investing",
        "watchlist", "long term", "brokerage", "valuation"
    ]):
        return "Investing"
 
    if any(x in k for x in [
        "make money", "income", "monetization", "sell templates", "digital products",
        "extra income", "earn more", "blog income", "passive income"
    ]):
        return "Make Money"
 
    if any(x in k for x in [
        "side hustle", "side hustles", "weekend hustle", "after work",
        "part time income", "low cost hustle"
    ]):
        return "Side Hustles"
 
    if any(x in k for x in [
        "notion", "clickup", "crm", "invoicing software", "email marketing",
        "project management software", "alternatives", "review", "worth it"
    ]):
        return "Software Reviews"
 
    if any(x in k for x in [
        "ai", "chatgpt", "automation", "meeting notes", "summarization", "ai writing"
    ]):
        return "AI Tools"
 
    return "Productivity"
 
def pick_category(keyword: str, cluster_name: str = "", post_type: str = "") -> str:
    return cluster_to_category(cluster_name, keyword, post_type)
 
 
# =========================================================
# Keyword generation
# =========================================================
def load_keywords() -> List[str]:
    data = load_json(KEYWORDS_JSON, [])
    if isinstance(data, list):
        return [x for x in data if isinstance(x, str) and x.strip()]
    if isinstance(data, dict):
        ks = data.get("keywords") or []
        if isinstance(ks, list):
            return [x for x in ks if isinstance(x, str) and x.strip()]
    return []
 
 
def save_keywords(keywords: List[str]) -> None:
    keywords = [k for k in keywords if isinstance(k, str) and k.strip()]
    unique = []
    seen = set()
    for k in keywords:
        nk = normalize_keyword(k)
        if not nk or nk in seen:
            continue
        seen.add(nk)
        unique.append(k.strip())
    save_json(KEYWORDS_JSON, {"keywords": unique})
 
 
def expand_keywords_from_google(seeds: List[str], existing_titles: List[str], existing_keywords: List[str]) -> List[str]:
    if not GOOGLE_SUGGEST_ENABLED:
        return []

    pool: List[str] = []
    base_seeds = [s for s in seeds if isinstance(s, str) and s.strip()][:GOOGLE_SUGGEST_MAX_SEEDS]

    for seed in base_seeds:
        seed = _clean_text(seed)
        if not seed:
            continue

        seed_word_count = len(normalize_keyword(seed).split())
        variants = [seed]

        if seed_word_count <= 6:
            variants.extend([
                f"how to {seed}",
                f"{seed} review",
                f"{seed} for beginners",
                f"{seed} worth it",
                f"{seed} vs alternatives",
                f"{seed} mistakes",
            ])
        elif seed_word_count <= 10:
            variants.extend([
                f"{seed} review",
                f"{seed} for beginners",
                f"{seed} worth it",
                f"{seed} mistakes",
            ])
        else:
            variants.extend([
                f"{seed} review",
                f"{seed} mistakes",
            ])

        seen_queries = set()
        clean_variants = []
        for q in variants:
            q = _clean_text(q)
            if not q:
                continue
            nq = normalize_keyword(q)
            if nq in seen_queries:
                continue
            seen_queries.add(nq)
            clean_variants.append(q)

        for q in clean_variants:
            pool.extend(fetch_google_suggest(q))

    return dedupe_keywords(pool, existing_titles, existing_keywords)
 
 
def build_cluster_keyword_prompt(
    cluster_name: str,
    seed_keywords: List[str],
    existing_titles: List[str],
    existing_keywords: List[str],
) -> str:
    seed_block = "\n".join([f"- {x}" for x in seed_keywords[:30]]) or "- ai tools to make money online"
    title_block = "\n".join([f"- {x}" for x in existing_titles[:70]])
    existing_kw_block = "\n".join([f"- {x}" for x in existing_keywords[:100]])
 
    return f"""
You generate SEO blog topic keywords for a site targeting US and EU readers.
 
Current cluster:
{cluster_name}
 
Need:
- prefer keywords that a newer site could realistically target
- avoid broad money keywords and broad investing keywords
- prefer specific search situations over broad education topics
- every keyword should imply a concrete article angle not a category page
- at least 5 of the keywords must be clear cluster articles that support one broader pillar
- include a mix of:
  pillar-supporting beginner questions
  comparison keywords
  mistake keywords
  allocation or setup keywords
- the keywords should be able to internally link to each other naturally
- avoid isolated topics that do not support a content cluster
- for investing prefer cluster chains like:
  simple portfolio
  portfolio allocation
  first etf
  monthly investing
  beginner mistakes
- exactly {CLUSTER_BATCH} keyword ideas
- long-tail keywords only
- practical search intent only
- suitable for a niche site that wants durable traffic
- no outdated years
- no news
- no politics
- no medical or legal advice
- avoid generic head terms
- each keyword must describe a concrete search intent
- include buyer intent, comparison intent, beginner intent, or income intent when natural
- vary audience, situation, and decision point
- avoid rewording the same topic
 
Avoid topics too similar to these existing post titles:
{title_block if title_block else "- none"}
 
Avoid topics too similar to these existing keywords:
{existing_kw_block if existing_kw_block else "- none"}
 
Cluster seeds:
{seed_block}
 
Return valid JSON only:
{{
  "keywords": [
    "keyword 1",
    "keyword 2"
  ]
}}
""".strip()
 
def generate_cluster_keywords(
    cluster_name: str,
    seed_keywords: List[str],
    existing_titles: List[str],
    existing_keywords: List[str],
) -> List[str]:
    prompt = build_cluster_keyword_prompt(cluster_name, seed_keywords, existing_titles, existing_keywords)
    raw = openai_generate_text(prompt, model=MODEL_PLANNER, temperature=0.6)
    data = json.loads(_find_balanced_json(raw))
 
    kws = data.get("keywords") or []
    if not isinstance(kws, list):
        return []
 
    clean = []
    for kw in kws:
        if isinstance(kw, str) and kw.strip():
            clean.append(_clean_text(kw))
 
    clean = dedupe_keywords(clean, existing_titles, existing_keywords)
    clean = filter_keywords_by_opportunity(clean, existing_titles)
    return clean
 
 
def build_general_keyword_prompt(seed_keywords: List[str], existing_titles: List[str], existing_keywords: List[str]) -> str:
    seed_block = "\n".join([f"- {x}" for x in seed_keywords[:30]]) or "- ai tools to make money online"
    title_block = "\n".join([f"- {x}" for x in existing_titles[:60]])
    existing_kw_block = "\n".join([f"- {x}" for x in existing_keywords[:100]])
 
    return f"""
You generate SEO blog topic keywords for a site targeting US and EU readers.
 
Site focus:
1. AI tools for work and income
2. beginner investing
3. practical make money strategies
4. productivity systems
5. software reviews and comparisons
6. side hustles for ordinary workers
 
Need:
- prioritize low to medium competition long-tail queries
- prefer search queries with clear beginner constraints
- prefer keywords with one of these modifiers when natural:
  with 100 dollars
  monthly
  for beginners
  first time
  simple
  small budget
  under 30
  without picking stocks
- avoid broad educational head terms even if they sound useful
- avoid inspirational titles
- avoid vague guide-like phrases
- exactly 14 keyword ideas
- long-tail keywords only
- practical search intent only
- human sounding
- suitable for a newer niche blog
- easier to rank than broad head terms
- no outdated years
- no celebrity or news topics
- no medical, legal, political, or unsafe topics
- no vague definitions
- do not create multiple keywords that only reword the same underlying problem
 
Good patterns:
- best X for Y
- X vs Y
- is X worth it for Y
- how to start X
- X for beginners
- how to choose X
- how to make money with X
- comparison keywords with buyer intent
- beginner investing and software buying intent
 
Seed keywords:
{seed_block}
 
Avoid topics too similar to these existing post titles:
{title_block if title_block else "- none"}
 
Avoid topics too similar to these existing keywords:
{existing_kw_block if existing_kw_block else "- none"}
 
Return valid JSON only:
{{
  "keywords": [
    "keyword 1",
    "keyword 2"
  ]
}}
""".strip()
 
def generate_auto_keywords(seed_keywords: List[str], existing_titles: List[str], existing_keywords: List[str]) -> List[str]:
    prompt = build_general_keyword_prompt(seed_keywords, existing_titles, existing_keywords)
    raw = openai_generate_text(prompt, model=MODEL_PLANNER, temperature=0.65)
    data = json.loads(_find_balanced_json(raw))
 
    kws = data.get("keywords") or []
    if not isinstance(kws, list):
        return []
 
    clean = []
    for kw in kws:
        if isinstance(kw, str) and kw.strip():
            clean.append(_clean_text(kw))
 
    clean = dedupe_keywords(clean, existing_titles, existing_keywords)
    clean = filter_keywords_by_opportunity(clean, existing_titles)
    return clean
 
 
def build_pillar_keyword_pool(cluster_name: str, posts: List[dict], existing_titles: List[str]) -> List[str]:
    existing_keywords = get_existing_keywords_from_posts(posts)
    base = DEFAULT_PILLAR_TOPICS.get(cluster_name) or []
    google_kw = expand_keywords_from_google(base, existing_titles, existing_keywords)
    merged = dedupe_keywords(base + google_kw, existing_titles, existing_keywords)
    merged = filter_keywords_by_opportunity(merged, existing_titles)
    return merged
 
 
def build_keyword_pool(base_keywords: List[str], existing_titles: List[str], posts: List[dict]) -> Tuple[List[str], str, str, str]:
    existing_keywords = get_existing_keywords_from_posts(posts)
    clean_base = dedupe_keywords(base_keywords, existing_titles, existing_keywords)
 
    if CLUSTER_MODE:
        topic_clusters = load_topic_clusters()
        cluster_name = pick_next_cluster(posts, topic_clusters)
 
        if cluster_recent_saturation(posts, cluster_name, window=10) >= 3:
            alternatives = [c for c in topic_clusters.keys() if c != cluster_name]
            if alternatives:
                cluster_name = random.choice(alternatives)
 
        pillar_mode = should_make_pillar(posts, cluster_name)
        current_pillar = get_cluster_pillar(posts, cluster_name)
        current_pillar_slug = (current_pillar.get("slug") or "").strip()
 
        if pillar_mode:
            pillar_pool = build_pillar_keyword_pool(cluster_name, posts, existing_titles)
            if pillar_pool:
                return pillar_pool, cluster_name, "pillar", current_pillar_slug
 
        seeds = topic_clusters.get(cluster_name) or []
        merged_seed = clean_base + seeds
        merged_seed = [x for x in merged_seed if isinstance(x, str) and x.strip()]
 
        try:
            cluster_keywords = generate_cluster_keywords(
                cluster_name=cluster_name,
                seed_keywords=merged_seed,
                existing_titles=existing_titles,
                existing_keywords=existing_keywords,
            )
            google_keywords = expand_keywords_from_google(merged_seed, existing_titles, existing_keywords)
            merged_all = dedupe_keywords(clean_base + cluster_keywords + google_keywords, existing_titles, existing_keywords)
            merged_all = filter_keywords_by_opportunity(merged_all, existing_titles)

            target_category = cluster_to_category(cluster_name)
            merged_all = [
                kw for kw in merged_all
                if pick_category(keyword=kw, cluster_name=cluster_name, post_type="normal") == target_category
            ]

            if merged_all:
                save_keywords(merged_all)
                return merged_all, cluster_name, "normal", current_pillar_slug

        except Exception as e:
            log("KW", f"Cluster keyword generation failed: {e}")
 
        fallback = dedupe_keywords(seeds + clean_base, existing_titles, existing_keywords)
        fallback = filter_keywords_by_opportunity(fallback, existing_titles)

        target_category = cluster_to_category(cluster_name)
        fallback = [
            kw for kw in fallback
            if pick_category(keyword=kw, cluster_name=cluster_name, post_type="normal") == target_category
        ]

        return fallback, cluster_name, "normal", current_pillar_slug
 
    auto_keywords: List[str] = []
    if len(clean_base) < MIN_KEYWORD_POOL:
        try:
            auto_keywords = generate_auto_keywords(clean_base or base_keywords, existing_titles, existing_keywords)
            google_keywords = expand_keywords_from_google(clean_base or base_keywords, existing_titles, existing_keywords)
            merged = dedupe_keywords(clean_base + auto_keywords + google_keywords, existing_titles, existing_keywords)
            merged = filter_keywords_by_opportunity(merged, existing_titles)
            if merged:
                save_keywords(merged)
                return merged, "General", "normal", ""
        except Exception as e:
            log("KW", f"Auto keyword generation failed: {e}")
 
    clean_base = filter_keywords_by_opportunity(clean_base, existing_titles)
    return clean_base, "General", "normal", ""
 
 
# =========================================================
# Strategy and article generation
# =========================================================
def build_planning_prompt(keyword: str, avoid_titles: List[str], cluster_name: str, post_type: str) -> str:
    avoid_block = "\n".join([f"- {x}" for x in avoid_titles[:40]]) if avoid_titles else "- none"
    category_hint = pick_category(keyword=keyword, cluster_name=cluster_name, post_type=post_type)
    intent_type = infer_search_intent_type(keyword, category_hint)

    audience_segmentation_note = """
- For freelancer topics do not treat all freelancers as one group
- Split recommendations by at least 2 concrete freelancer types
- Examples: solo freelancer, designer, developer, consultant, writer, video editor
- The article must help a reader identify "this is for me" quickly
"""

    review_depth_note = """
- If the article mentions software tools it must not read like a vague roundup
- Do not just name tools as examples
- Force a real buying or selection decision
- Include at least:
  - price reality
  - free plan status
  - setup difficulty
  - automation depth
  - client communication fit
  - invoicing or portal fit when relevant
- The structure must answer:
  - who should pick what
  - when to avoid a tool
  - what becomes painful after setup
"""

    originality_note = """
- Include at least 2 realism anchors in the plan
- Realism anchors can include:
  setup friction
  client delay
  approval bottleneck
  renewal risk
  hidden cost
  manual workaround
- Avoid generic SaaS blog structure
- The article should feel like it has seen messy real client work
"""
 
    INTENT_BLUEPRINTS = {
        "comparison": [
            "quick verdict and who each option is for",
            "comparison table with real selection criteria",
            "where each tool wins and breaks down",
            "best fit by user type or budget",
            "final decision and what to do next",
        ],
        "template": [
            "the situation this template solves",
            "copyable template or checklist",
            "how to customize it without breaking it",
            "example scenario and common mistake",
            "when not to use this template",
        ],
        "review": [
            "quick verdict and target user",
            "pricing reality and setup difficulty",
            "main strengths and where it breaks down",
            "best for and not ideal for",
            "final recommendation",
        ],
        "howto": [
            "why the problem keeps happening",
            "the hidden reason common advice fails",
            "the exact system or workflow",
            "example scenario with timing",
            "mistakes tradeoffs and final decision",
        ],
    }

    blueprint = INTENT_BLUEPRINTS.get(intent_type, INTENT_BLUEPRINTS["howto"])
    section_count = len(blueprint)

    post_guidance = """
This is a pillar guide.
It should explain a family of related choices clearly.
It should stay practical and grounded.
It must not sound like an encyclopedia entry.
""" if post_type == "pillar" else """
This is a focused article.
It should solve one specific search intent in detail.
It can be a workflow article, a review article, a comparison article, a make money article, or a beginner investing article.
"""

    return f"""
You are planning a practical article for US and EU readers.

Cluster:
{cluster_name}

Seed keyword:
{keyword}

Preferred category:
{category_hint}

Avoid titles too similar to:
{avoid_block}

Return valid JSON only.

Schema:
{{
  "audience": "specific reader type",
  "problem": "specific painful situation",
  "outcome": "clear promised result",
  "angle": "specific article angle",
  "title": "specific practical title",
  "description": "155-170 chars meta description not equal to title",
  "category": "AI Tools|Investing|Make Money|Productivity|Software Reviews|Side Hustles",
  "intent": "pillar|cluster",
  "intent_type": "comparison|template|review|howto",
  "search_intent_summary": "one sentence",
  "section_plan": [
    {{
      "heading": "section heading",
      "section_role": "problem|insight|solution|example|decision|checklist|ending",
      "goal": "what this section must achieve",
      "image_query": "2-6 words concrete visual idea",
      "visual_type": "photo|diagram|workspace",
      "must_include": ["point 1", "point 2"],
      "alt_hint": "specific image alt text idea"
    }}
  ],
  "faq_questions": [
    "question 1",
    "question 2"
  ],
  "tldr_focus": [
    "point 1",
    "point 2"
  ]
}}

Hard rules:
- The article must fit into a broader topic cluster
- The plan should identify at least 2 adjacent follow-up topics a reader would logically need next
- These adjacent topics should be reflected in section goals or must_include points
- At least 2 sections must include a concrete number, cost, percentage, amount, or time example
- At least 1 section must include a realistic beginner scenario with money, timing, and consequence
- For investing topics include practical realism such as:
  monthly amount
  allocation percentage
  fee difference
  review cadence
- Avoid generic illustrations as the only visual idea
- image_query must be specific enough to look unique and editorial
- Titles must match a real beginner search query closely
- Prefer concrete patterns like:
  how to start X with Y
  simple X for beginners
  X vs Y for beginners
  how much to invest in X
  best X for beginners with a constraint
- Avoid abstract title words such as:
  roadmap
  blueprint
  framework
  journey
  path
  strategy
- The title should feel like something a real person would type into Google
- The article must naturally create at least 2 follow-up reading moments
- These should reference related decision points such as:
  alternatives
  vs comparisons
  pricing
  beginner mistakes
  setup checklist
  portfolio allocation
- At least 2 sections must include a line that naturally leads to a related article topic
- These follow-up hooks must sound useful not promotional
- intent_type must match the keyword
- comparison articles must prioritize selection criteria and tradeoffs
- template articles must include a reusable asset
- review articles must feel like real reviews not generic summaries
- howto articles must include timing and a repeatable workflow
- Avoid fake sophistication
- Avoid shallow SEO filler
- Avoid generic software roundup structure
- Do not write a generic roundup that only mentions tool names
- If software is mentioned the article must create a real selection decision
- For freelancer software topics segment the audience into at least 2 concrete freelancer types
- The article must include at least 2 realism anchors such as setup friction, hidden cost, approval bottleneck, or renewal risk
- The article must create commercial depth, not just awareness
- The reader should be able to answer:
  what should I choose
  what should I avoid
  what will break later
- The article must go one level deeper than a standard blog post
- The article structure must follow this logic:
  problem -> insight -> solution
- At least the first 3 sections must clearly map to:
  1. the visible problem
  2. the hidden insight or misunderstanding
  3. the practical solution or system
- The section headings must reflect this progression
- Avoid flat list-style structure where each section feels interchangeable
- Each section heading must create curiosity, tension, contrast, or a decision point
- Avoid bland headings like:
  who this is for
  practical approach
  decision framework
  final recommendation
  step by step setup
- Prefer headings that imply a hidden truth, invisible moment, overlooked tradeoff, or real-world consequence
- The last section must not feel like a generic summary
- The final section should leave the reader with a decision, a reflection, or a pressure point
- The title may use high CTR formats when natural:
  - best X for Y
  - X vs Y
  - is X worth it for Y
  - X for beginners
  - how to choose X
- Avoid vague clickbait
- Avoid generic titles like "Top Tools" or "Best Apps"
- Do not restate the seed keyword as the title
- The title must include a real audience or real decision point
- Keep title under 72 characters when possible
- Section count must be exactly {section_count}
- The section flow should roughly cover this structure:
{json.dumps(blueprint, ensure_ascii=False, indent=2)}
- Each section must be materially distinct
- image_query must be visual and believable
- visual_type should prefer "diagram" for abstract comparison topics and "photo" or "workspace" for concrete environments
- The first section must be built around a strong opening hook
- The first section goal must explicitly include:
  - what people usually think
  - what actually happens
  - why that gap matters
- At least 2 section headings must imply tension, contrast, consequence, or a hidden truth
- At least 2 sections must include timing such as 7 days, 14 days, 30 days, weekly, or monthly
- At least 2 sections must include one named scenario or example
- The section plan must not sound like a generic SaaS blog outline
- Each section must include at least one concrete operational detail
- At least 2 sections must include a specific time marker such as 7 days, 14 days, 30 days, weekly, or monthly
- At least 2 sections must include an example scenario with sequence and consequence
- The audience must be named directly in section 1
- Section 1 must clearly state who this article is for
- Avoid generic titles such as:
  The best way to...
  A complete guide to...
  Effective strategies for...
- Titles must include a real audience, real constraint, or real decision
{audience_segmentation_note}
{review_depth_note}
{originality_note}
{post_guidance}
""".strip()


def parse_planning_json(text: str, keyword: str, cluster_name: str, post_type: str) -> Dict[str, Any]:
    raw = _find_balanced_json(text)
    data = json.loads(raw)

    if not isinstance(data, dict):
        raise ValueError("planning JSON root is not object")

    audience = _clean_text(data.get("audience", ""))
    problem = _clean_text(data.get("problem", ""))
    outcome = _clean_text(data.get("outcome", ""))
    angle = _clean_text(data.get("angle", ""))
    title = _clean_text(data.get("title", ""))
    description = _clean_text(data.get("description", ""))
    category = _clean_text(data.get("category", ""))
    intent = _clean_text(data.get("intent", post_type or "cluster"))
    search_intent_summary = _clean_text(data.get("search_intent_summary", ""))
    intent_type = _clean_text(data.get("intent_type", "")).lower()

    if category not in ALLOWED_CATEGORIES:
        category = pick_category(keyword=keyword, cluster_name=cluster_name, post_type=post_type)

    if intent_type not in {"comparison", "template", "review", "howto"}:
        intent_type = infer_search_intent_type(keyword, category)

    if not audience or not problem or not outcome or not angle or not title:
        raise ValueError("planning fields missing")

    section_plan = data.get("section_plan") or []
    if not isinstance(section_plan, list):
        raise ValueError("section_plan must be a list")
    if len(section_plan) < SECTION_COUNT_MIN or len(section_plan) > SECTION_COUNT_MAX:
        raise ValueError(f"section_plan must be between {SECTION_COUNT_MIN} and {SECTION_COUNT_MAX}")

    clean_sections = []
    for s in section_plan:
        if not isinstance(s, dict):
            raise ValueError("section item must be object")

        heading = _clean_text(s.get("heading", ""))
        goal = _clean_text(s.get("goal", ""))
        section_role = _clean_text(s.get("section_role", "")).lower()
        image_query = _clean_text(s.get("image_query", ""))
        visual_type = _clean_text(s.get("visual_type", "diagram")).lower()
        alt_hint = _clean_text(s.get("alt_hint", ""))
        must_include = s.get("must_include") or []

        if not isinstance(must_include, list):
            must_include = []
        must_include = [_clean_text(x) for x in must_include if isinstance(x, str) and _clean_text(x)]

        if visual_type not in {"photo", "diagram", "workspace"}:
            visual_type = "diagram"
        if section_role not in {"problem", "insight", "solution", "example", "decision", "checklist", "ending"}:
            section_role = "solution"

        if not heading or not goal or not image_query or len(must_include) < 2:
            raise ValueError("section_plan item missing required fields")

        clean_sections.append({
            "heading": heading,
            "goal": goal,
            "section_role": section_role,
            "image_query": image_query,
            "visual_type": visual_type,
            "must_include": must_include[:6],
            "alt_hint": alt_hint or heading,
        })

    faq_questions = data.get("faq_questions") or []
    if not isinstance(faq_questions, list):
        faq_questions = []
    faq_questions = [_clean_text(x) for x in faq_questions if isinstance(x, str) and _clean_text(x)][:5]

    tldr_focus = data.get("tldr_focus") or []
    if not isinstance(tldr_focus, list):
        tldr_focus = []
    tldr_focus = [_clean_text(x) for x in tldr_focus if isinstance(x, str) and _clean_text(x)][:5]

    log(
        "PLAN",
        f"keyword='{keyword}' title='{title}' category='{category}' intent_type='{intent_type}' sections={len(clean_sections)}"
    )
 
    return {
        "audience": audience,
        "problem": problem,
        "outcome": outcome,
        "angle": angle,
        "title": title,
        "description": description or short_desc(f"{angle}. {outcome}"),
        "category": category,
        "intent": intent or post_type,
        "intent_type": intent_type,
        "search_intent_summary": search_intent_summary or angle,
        "section_plan": clean_sections,
        "faq_questions": faq_questions,
        "tldr_focus": tldr_focus,
    }
 

def infer_search_intent_type(keyword: str, category: str = "") -> str:
    k = (keyword or "").lower().strip()
    c = (category or "").lower().strip()

    if " vs " in k or "versus" in k or "alternative" in k or "alternatives" in k or "compare" in k:
        return "comparison"

    if "template" in k or "checklist" in k or "script" in k or "email example" in k:
        return "template"

    if "best " in k or "review" in k or "worth it" in k or c == "software reviews":
        return "review"

    if "how to" in k or "system" in k or "workflow" in k or "process" in k:
        return "howto"

    return "howto"

def infer_content_mode(category: str, text: str, intent: str = "cluster") -> str:
    joined = f"{category} {text} {intent}".lower()

    if any(x in joined for x in ["template", "checklist", "workflow", "system", "process"]):
        return "workflow"
    if any(x in joined for x in ["tool", "software", "app", "platform", "compare", "comparison"]):
        return "review"
    if any(x in joined for x in ["invest", "stock", "stocks", "etf", "etfs", "portfolio", "dividend"]):
        return "investing"
    if any(x in joined for x in ["make money", "income", "side hustle", "monetization"]):
        return "money"
    return "workflow"

def build_mode_rules(mode: str) -> str:
    if mode == "review":
        return """
- Explicitly include who the product is for
- Explain pricing clearly
- Include pros and cons
- Include best for and not ideal for
- Explain the decision logic between options
- If comparing tools, explain why one wins for one audience and loses for another
"""
 
    if mode == "investing":
        return """
- Explain who this is for
- Explain the beginner angle clearly
- Use one of these exact phrases:
  this is for
  if you are
  for beginners
- Include risk and volatility
- Use the exact phrase long term in a natural sentence
- Explain what to watch before buying
- Include at least one limitation such as:
  not ideal for
  avoid this if
  time horizon
  risk tolerance
  educational content
- Do not give reckless certainty
- Do not sound like day trading hype
- Keep it educational and practical
"""
 
    if mode == "money":
        return """
- Explain the effort level
- Explain the time requirement
- Explain how income is actually generated
- Include one mistake beginners make
- Include one tradeoff that makes the method less attractive for some people
- Explain who should avoid this path
"""
 
    return """
- Explicitly include:
  workflow, checklist, mistake, tradeoff, decision, step
- Explain who this workflow is for
- Explain why common advice fails
- Include setup steps
- Include mistakes
- Include tradeoffs
- Include a reusable checklist or template
- Include when not to use this setup
- Include at least 2 time-based actions
- Include at least 1 measurable threshold or review cadence
- Convert abstract advice into specific operational timing
- Do not use markdown bold like **text**
- When writing numbered steps, put each step on its own block
"""

INVESTING_STRUCTURE_RULES = """
Structure rules:
- The article must use exactly 6 sections
- FAQ must not be inside sections
- FAQ must remain only in the faq field
- The sections must appear in this exact order:

  1. Quick Answer
  2. Why Most Beginners Pick the Wrong AI Stocks
  3. A 30-Day Screening Process
  4. A Simple Beginner Example With Numbers
  5. Mistakes That Cost Real Money
  6. Which Tools or ETFs Make This Simpler

Section heading rules:
- Section 1 must directly answer what a beginner should start with
- Section 2 must explain the main misunderstanding or trap
- Section 3 must include a repeatable monthly workflow
- Section 4 must include a realistic amount and allocation example
- Section 5 must explain specific loss-causing mistakes
- Section 6 must compare tools, ETFs, or research sources by fit and tradeoff

Section body rules:
- Section 1 must be short, direct, and concrete
- Section 1 must mention at least one named stock, ETF, amount, or beginner constraint
- Section 3 must contain at least one numbered process
- Section 4 must show sequence, numbers, and consequence
- Section 5 must include at least 2 common beginner mistakes
- Section 6 must explain what to use first and what to ignore for now
"""

REVIEW_STRUCTURE_RULES = """
Structure rules:
- The article must use exactly 6 sections
- FAQ must not be inside sections
- FAQ must remain only in the faq field
- The sections must appear in this exact order:

  1. Quick Verdict
  2. Who Each Option Is Actually For
  3. Where the Differences Start to Matter
  4. A Real Setup or Buying Scenario
  5. Mistakes and Overkill Choices
  6. Final Recommendation by User Type

Section heading rules:
- Section 1 must directly answer the comparison or buying question
- Section 2 must split users into concrete user types
- Section 3 must explain practical tradeoffs
- Section 4 must show a realistic use case or setup path
- Section 5 must explain what people choose badly and why
- Section 6 must tell the reader what to pick based on need and budget
"""

WORKFLOW_STRUCTURE_RULES = """
Structure rules:
- The article must use exactly 6 sections
- FAQ must not be inside sections
- FAQ must remain only in the faq field
- The sections must appear in this exact order:

  1. Quick Answer
  2. Why the Default Approach Fails
  3. The Core Workflow
  4. A Real Example or Scenario
  5. Mistakes and Tradeoffs
  6. What to Use Next

Section heading rules:
- Section 1 must clearly say who this is for
- Section 2 must explain why common advice fails
- Section 3 must contain the main workflow or system
- Section 4 must show sequence and consequence
- Section 5 must explain repeat mistakes and tradeoffs
- Section 6 must leave the reader with a concrete next decision
"""


def build_table_rules(post_type: str, mode: str, intent_type: str = "") -> str:
    it = (intent_type or "").strip().lower()
    pt = (post_type or "").strip().lower()
    md = (mode or "").strip().lower()

    if it == "comparison":
        return """
HTML table rules:
- For comparison-style sections, include exactly 1 HTML table in the most relevant section only.
- Do NOT use Markdown tables.
- Do NOT use SVG tables.
- Use real product or software names.
- Never use placeholders like Option A, Option B, Option C.
- Place the table between normal paragraphs.
- The table must be wrapped like this:
  <div class="table-wrap"><table class="cmp-table">...</table></div>
- Use this structure only:
  <table class="cmp-table">
    <thead>
      <tr><th>Feature</th><th>Real Name 1</th><th>Real Name 2</th><th>Real Name 3</th></tr>
    </thead>
    <tbody>
      <tr><td>Feature row</td><td>Value</td><td>Value</td><td>Value</td></tr>
    </tbody>
  </table>
- Keep the table to 4 columns total.
- Keep the table to 4 to 6 body rows.
- The first column must be Feature.
- The other columns must be real products, tools, apps, platforms, or methods that match the article topic.
- Add one short paragraph before the table and one short paragraph after the table.
- Only include a table when it genuinely improves comparison clarity.
"""
    return """
Table rules:
- Do not include any HTML table unless the article clearly needs one.
- Do not include Markdown tables.
- Do not include SVG table instructions.
"""


def build_article_prompt(
    keyword: str,
    cluster_name: str,
    post_type: str,
    planning: Dict[str, Any],
) -> str:
    category = planning.get("category") or pick_category(
        keyword=keyword,
        cluster_name=cluster_name,
        post_type=post_type,
    )
    intent_type = planning.get("intent_type") or infer_search_intent_type(keyword, category)
    mode = infer_content_mode(
        category,
        planning.get("search_intent_summary", "") or planning.get("title", ""),
        planning.get("intent", "cluster"),
    )
    mode_rules = build_mode_rules(mode)
    table_rules = build_table_rules(post_type, mode, intent_type)
 
    if mode == "investing":
        structure_rules = INVESTING_STRUCTURE_RULES
    elif mode == "review":
        structure_rules = REVIEW_STRUCTURE_RULES
    else:
        structure_rules = WORKFLOW_STRUCTURE_RULES

    visual_rules = """
    Visual rules:
    - Use photo or workspace for normal sections when an image helps.
    - Do NOT use SVG infographic tables.
    - Do NOT force diagram visuals for comparison sections.
    - If a section includes an HTML comparison table, do not add an image to that section.
    - Comparison data should appear as an HTML table inside the body when a table is genuinely useful.
    - Use real product names, not placeholders like Option A or Option B.
    """

    return f"""
You are writing a practical editorial-quality blog article for US and EU readers.

Seed keyword:
{keyword}

Cluster:
{cluster_name}

Post type:
{post_type}

Content mode:
{mode}

Visual rules:
{visual_rules}

Visual type rules:

photo = real-world photography
workspace = desk or workflow setup
diagram = infographic, comparison chart, or conceptual visual


Planning JSON:
{json.dumps(planning, ensure_ascii=False, indent=2)}

Output must be valid JSON only.

Schema:
{{
  "title": "string",
  "description": "string",
  "category": "AI Tools|Investing|Make Money|Productivity|Software Reviews|Side Hustles",
  "intent_type": "comparison|template|review|howto",
  "sections": [
    {{
      "heading": "string",
      "image_query": "string",
      "visual_type": "photo|diagram|workspace",
      "alt_text": "string",
      "body": "string"
    }}
  ],
  "faq": [
    {{"q":"string","a":"string"}}
  ],
  "tldr": "string",
  "editorial_note": "string"
}}

The sections array must contain exactly 6 section objects only.
FAQ must not appear inside sections.
FAQ must be written only in the faq field.
The 6 section objects must follow the exact required order defined in Structure rules.

Core writing standard:
- This article must feel publishable on the first draft
- Do not write a draft that needs a quality checker to fix obvious weaknesses
- Do not write generic SEO filler
- Do not write like a content farm
- Do not write like a neutral encyclopedia
- Write like a sharp niche editor who understands real reader decisions
- Every section must add a new angle, new decision, new tradeoff, or new consequence
- Do not restate the same advice in different wording
- Avoid repetitive sentence openings
- Vary paragraph rhythm and sentence length
- Avoid obvious AI phrasing and predictable blog language
- Avoid bland openers and bland transitions
- Do not pad the article just to make it longer
- Depth matters more than fluff

Title and heading quality rules:
- The title must start with a search-friendly phrase when natural
- Prefer concrete title patterns such as:
  Best X for beginners
  X to buy now for beginners
  3 X beginners can start with
  X vs Y for beginners
  How to start X with Y dollars
- Avoid editorial title endings such as:
  practical guide
  real tradeoffs
  practical first picks
  complete framework
  hidden truths
- Use plain search language over clever wording
- The first 5 words of the title should make the topic obvious
- The title must be specific and non-generic
- Do not use titles that sound like generic SEO blog posts
- Avoid title patterns such as:
  ultimate guide
  complete guide
  comprehensive guide
  essential guide
  top tools for everyone
  best apps for everyone
- The title must include a real audience, real constraint, or real decision point
- The title must match the search intent closely
- Do not simply restate the seed keyword
- Section headings must not be generic
- Avoid weak headings such as:
  who this is for
  practical approach
  decision framework
  final recommendation
  step by step setup
  template checklist
  tradeoffs and limitations
- Each heading should imply tension, contrast, hidden truth, consequence, or a real decision

{structure_rules}

Opening rules:
- Intro must stay under 80 words
- The first 2 sentences must answer the reader's question directly
- The opening must mention at least one concrete example, stock, tool, amount, or decision
- Do not begin with broad context or abstract statements
- The opening should feel like a direct answer, not a warm-up

Length rules:
- Aim for 4500 to 7000 characters for most articles
- Keep sections focused and avoid filler
- Do not add generic explanations just to increase length

TLDR rules:
- TLDR must be 3 to 5 short lines
- TLDR must answer the core query immediately
- TLDR must include at least one concrete recommendation, number, or named example
- TLDR must not sound abstract or editorial
- TLDR should read like a fast answer box, not a summary paragraph

Reader takeaway rule:
- At the end of the article include one short paragraph that clearly states the key takeaway for the reader.
- This paragraph should summarize the main decision in plain language.

Depth rules:
- The body must satisfy the exact promise implied by the title
- If the title includes beginner, simple, first, monthly, or small amount, explain that constraint directly
- Do not drift into a broader article than the title promises
- Every section must contain concrete operational detail
- Every section must contain at least one of these in a natural way:
  example
  scenario
  tradeoff
  mistake
  decision
  consequence
- Include concrete numbers whenever relevant
- At least 2 sections must include timing such as:
  7 days
  14 days
  30 days
  weekly
  monthly
- At least 2 sections must include a mini-scenario with sequence and consequence
- At least 1 section must include a numbered step block
- At least 2 sections must include examples or edge cases
- Translate abstract advice into observable actions, thresholds, timing, or criteria
- No empty motivational filler
- No vague lines like improve efficiency, streamline workflow, or choose the right tool unless followed by exact operational detail

Concrete example rule:
- At least 2 sections must include a concrete real-world example with numbers.
- Examples should include specific products, companies, tools, or dollar amounts.
- Avoid vague examples such as "a typical user" or "many people".

Anti-repetition rules:
- Do not repeat the same point across multiple sections
- Do not repeat the same keyword unnaturally
- Do not recycle the same sentence pattern across the article
- Do not let multiple sections say the same thing with slightly different words
- If a point has already been made, move forward instead of rephrasing it
- Each section must introduce at least one fresh decision point, friction point, or practical angle

Sentence variation rule:
- Avoid repeating the same sentence structure more than twice in a row.
- Mix short and medium sentences.
- Occasionally use a one-sentence paragraph for emphasis.

Readability and engagement rules:
- Intro must stay under 120 words
- The TLDR and opening paragraph must create curiosity immediately
- The first 3 lines must explain why the reader should keep reading
- The opening must not begin with broad context like:
  many people
  in today's world
  there are many tools available
  productivity is important
  investing can be intimidating
- Use short and medium paragraphs
- Most paragraphs should be 1 to 3 sentences
- No paragraph should exceed 90 words unless it is a numbered step block
- Include at least one one-sentence paragraph in every section for emphasis
- Break reading rhythm at least 2 times with:
  Example
  Scenario
  In practice
  Edge case
  What this looks like
  Where this breaks down

Step formatting rules:
- If a section includes numbered steps, each step must begin on its own new line.
- Each step must start with a number and a period.
- Use a clean continuous sequence such as 1. 2. 3. 4.
- Do not restart numbering in the same sequence.
- Do not place step 2 or step 3 inside the same paragraph as step 1.
- If a section includes numbered steps, format them as a clean 1–4 step sequence.
- Do not mix numbered steps inside normal paragraphs.
- Each step must start on a new line.
- Do not restart numbering in the same sequence.

Limit numbered sequences to a maximum of 4 steps unless the workflow truly requires more.

Internal-link and cluster rules:
- Include at least 2 natural internal-link hook moments inside the article body
- These hooks should naturally point to related article angles such as:
  comparison
  alternatives
  pricing
  checklist
  beginner mistakes
  portfolio allocation
- Do not write raw URLs
- Write hooks as natural next-step lines
- The article should create follow-up reading demand without sounding promotional

Tool and software rules:
- If the article mentions tools or software, do not stop at naming them
- Every mentioned tool must have a role in a decision, comparison, tradeoff, or fit judgment
- Do not write "tools like X, Y, Z" unless you explain who each one is for
- Force product differentiation
- Force user-type differentiation
- Force operational detail
- The reader must not finish the article asking which one they should use

Length and completeness rules:
- Total text must be at least {MIN_CHARS} characters
- Aim for 9000 to 11000 characters when the topic supports it
- Each section body must be at least {MIN_SECTION_CHARS} characters
- Most sections should be meaningfully longer than the minimum
- FAQ must have 3 to 5 realistic follow-up questions
- TLDR must be 3 to 5 short lines
- TLDR must answer the core query immediately
- TLDR must include at least one concrete recommendation, example, number, or named option
- TLDR must not sound abstract or editorial
- TLDR should read like a fast answer box, not a summary paragraph
- editorial_note should briefly explain that the article is reviewed for practical usefulness and updated when information changes

Required natural language signals:
- The article must explicitly include these exact words in natural sentences:
  mistake
  tradeoff
  decision
  step
- Include at least 2 uses of tradeoff
- Include at least 2 uses of decision
- Include at least 2 uses of mistake

Intent specific requirements:
- intent_type is {intent_type}
- If intent_type is comparison:
  - include exactly 1 HTML table only when it genuinely improves clarity
  - do not generate a Markdown table
  - do not use placeholder labels like Option A, Option B, or Option C
  - use real product, software, tool, app, platform, or method names
  - write the actual HTML table inside the section body
  - wrap the table like:
    <div class="table-wrap"><table class="cmp-table">...</table></div>
  - keep the table to 4 columns total
  - keep the table to 4 to 6 rows in the tbody
  - place one short paragraph before the table
  - place one short paragraph after the table
  - do not describe the table abstractly
  - the section with the HTML table should not rely on an image
  - compare using practical factors such as price, free plan, setup difficulty, automation, communication fit, best for, and not ideal for when relevant
  - include a clear best fit for at least 2 user types
  - include one overkill option and explain why
  - include one best free starting point when relevant
  - include one best paid upgrade case when relevant
  - include one section that explains what changes after 30 days of real use
- If intent_type is template:
  - include one copyable template, checklist, script, or sequence
  - include one example of customization
  - include one misuse case
- If intent_type is review:
  - include these exact labels in natural text:
    Best for
    Pricing reality
    Setup difficulty
    Main strength
    Main weakness
    My verdict
  - the article must feel like an actual review or decision guide not a summary
  - include at least 3 concrete tools if the topic is a roundup
  - for each tool explain:
    what it does well
    where it starts to feel heavy or weak
    which user type should use it
  - include at least one sentence about hidden cost, setup friction, or long-term workflow pain
- If intent_type is howto:
  - include one weekly workflow
  - include one 30-day cadence or review cycle
  - include one specific scenario with consequence

Realism requirements:
- Include at least 2 grounded realism blocks that feel like observed real usage
- Use labels such as:
  In practice
  Scenario
  Where this breaks down
  Best fit if you are
  Not worth it if
- The article must feel aware of real friction
- Include at least 2 lines that sound like real-world behavior
- Good patterns include:
  clients stop replying after the proposal stage
  the tool feels fine until approvals start
  solo operators often overbuild too early
  the free plan works until follow-up automation matters
- Avoid fake personal claims
- Use grounded observational language instead

Investing requirements:
- For investing topics include at least 2 practical realism examples with numbers
- Good examples:
  100 dollars per month
  500 dollar starting portfolio
  80 20 allocation
  0.07 percent fee vs 0.60 percent fee
- Include one sentence that frames the article as educational content not personal financial advice
- include at least one sample allocation with percentages
- include at least one monthly contribution example
- include at least one beginner mistake tied to a number or timing
- include at least one platform or ETF selection decision point when relevant


Formatting rules:
- Do not use markdown symbols such as *, **, or _
- Do not use bullet symbols like * or -
- Use plain text only
- Emphasis should be written using normal words, not markdown formatting
- Do not wrap emphasis words in markdown
- Use plain text labels only
- Use simple labels such as:
  Best for:
  Tradeoff:
  Decision:
- Do not wrap them in markdown symbols
- Write them as normal text lines

Step formatting rules:
- If a section includes numbered steps, each step must begin on its own new line.
- Each step must start with a number and a period.
- Use a clean continuous sequence such as 1. 2. 3. 4.
- Do not mix numbered steps into normal paragraphs.

Mode specific requirements:
{mode_rules}

Table requirements:
{table_rules}

""".strip()
 
def is_generic_title(title: str) -> bool:
    t = _norm_title(title)
    if not t:
        return True
 
    if any(t.startswith(x) for x in BANNED_TITLE_PATTERNS):
        return True
 
    broad_bad = [
        "ai tools",
        "productivity tools",
        "investing guide",
        "make money guide",
        "software reviews",
        "remote work tools",
        "side hustles",
    ]
    if t in broad_bad:
        return True
 
    words = t.split()
    if len(words) < 4:
        return True
 
    useful_patterns = [
        "best ",
        " vs ",
        "review",
        "worth it",
        "for beginners",
        "how to ",
        "checklist",
        "system",
        "workflow",
    ]
    if any(p in t for p in useful_patterns):
        return False
 
    audience_terms = [
        "beginner", "beginners", "young professionals", "freelancer", "freelancers",
        "creator", "creators", "consultant", "consultants", "small team", "small teams",
        "remote worker", "remote workers", "solo", "one person", "investor", "investors",
        "full time worker", "full time workers",
    ]
    problem_terms = [
        "workflow", "checklist", "system", "template", "playbook", "review",
        "comparison", "pricing", "portfolio", "watchlist", "income", "side hustle",
        "software", "stocks", "etf", "alternatives", "worth it", "budget app",
    ]
    has_audience = any(x in t for x in audience_terms)
    has_problem = any(x in t for x in problem_terms)
 
    return not (has_audience or has_problem)
 
 
def opening_too_generic(text: str) -> bool:
    t = (text or "").lower().strip()[:550]
    return any(p in t for p in BANNED_OPENING_PHRASES)
 
 
def make_fingerprint(title: str, sections: List[Dict[str, str]], tldr: str, faq: List[Dict[str, str]]) -> str:
    parts = [title.strip(), (tldr or "").strip()[:400]]
    for s in sections[:8]:
        parts.append((s.get("heading") or "").strip())
        parts.append((s.get("body") or "").strip()[:500])
    for item in (faq or [])[:5]:
        parts.append((item.get("q") or "").strip()[:200])
        parts.append((item.get("a") or "").strip()[:200])
 
    joined = "\n".join([p for p in parts if p])
    return hashlib.sha1(joined.encode("utf-8")).hexdigest()
 

def quality_check_post(
    data: Dict[str, Any],
    keyword: str = "",
    post_type: str = "normal",
) -> Tuple[bool, str]:
    title = _clean_text(data.get("title", ""))
    tldr = _clean_text(data.get("tldr", ""))
    sections = data.get("sections", [])
    faq = data.get("faq", [])
    category = _clean_text(data.get("category", ""))
    editorial_note = _clean_text(data.get("editorial_note", ""))

    if not title:
        return False, "missing-title"

    #if is_generic_title(title):
    #   return False, "generic-title"

    if category not in ALLOWED_CATEGORIES:
        return False, "bad-category"

    if not isinstance(sections, list):
        return False, "bad-sections"

    if len(sections) < SECTION_COUNT_MIN or len(sections) > SECTION_COUNT_MAX:
        return False, "bad-sections"

    clean_headings = []
    for s in sections:
        if not isinstance(s, dict):
            return False, "bad-section-item"

        heading = _clean_text(s.get("heading", ""))
        body = _clean_text(s.get("body", ""))
        image_query = _clean_text(s.get("image_query", ""))
        visual_type = _clean_text(s.get("visual_type", "")).lower()
        alt_text = _clean_text(s.get("alt_text", ""))

        if not heading or not body:
            return False, "missing-section-content"

        if has_table_like_text(body):
            return False, "table-like-text-detected"
     
        if len(body) < max(260, MIN_SECTION_CHARS // 2):
            return False, "thin-section"

        if visual_type and visual_type not in {"photo", "diagram", "workspace"}:
            return False, "bad-visual-type"

        if not image_query:
            return False, "missing-image-query"

        if not alt_text:
            return False, "missing-alt-text"

        clean_headings.append(_norm_title(heading))

    if len(set(clean_headings)) < len(clean_headings):
        return False, "duplicate-headings"

    if not tldr or len(tldr) < 60:
        return False, "weak-tldr"

    if not isinstance(faq, list):
        return False, "bad-faq"

    valid_faq = 0
    for item in faq[:5]:
        if isinstance(item, dict):
            q = _clean_text(item.get("q", ""))
            a = _clean_text(item.get("a", ""))
            if q and a:
                valid_faq += 1

    if valid_faq < 2:
        return False, "bad-faq"

    if not editorial_note:
        return False, "missing-editorial-note"

    joined = "\n".join(
        [title, tldr] +
        [(_clean_text(s.get("heading", "")) + "\n" + _clean_text(s.get("body", ""))) for s in sections] +
        [(_clean_text(item.get("q", "")) + "\n" + _clean_text(item.get("a", ""))) for item in faq if isinstance(item, dict)]
    )

    if len(joined) < max(2200, MIN_CHARS // 2):
        return False, "too-short"

    nk = normalize_keyword(keyword)
    nt = normalize_keyword(title)
    if nk and nt and nk == nt:
        return False, "title-too-close-to-keyword"

    return True, "ok"
 
 
def post_semantically_too_close(
    keyword: str,
    planning: Dict[str, Any],
    posts: List[dict],
    threshold: float = TOPIC_SIM_THRESHOLD,
) -> bool:
    new_parts = [
        normalize_keyword(keyword),
        normalize_keyword(planning.get("audience", "")),
        normalize_keyword(planning.get("problem", "")),
        normalize_keyword(planning.get("outcome", "")),
        normalize_keyword(planning.get("angle", "")),
        normalize_keyword(planning.get("title", "")),
    ]
    new_text = " ".join([x for x in new_parts if x]).strip()
    if not new_text:
        return False
 
    recent_posts = posts[:160]
    for p in recent_posts:
        if not isinstance(p, dict):
            continue
 
        old_parts = [
            normalize_keyword(p.get("keyword", "")),
            normalize_keyword(p.get("title", "")),
            normalize_keyword(p.get("description", "")),
            normalize_keyword(p.get("audience", "")),
            normalize_keyword(p.get("problem", "")),
            normalize_keyword(p.get("cluster", "")),
            normalize_keyword(p.get("category", "")),
        ]
        old_text = " ".join([x for x in old_parts if x]).strip()
        if not old_text:
            continue
 
        if semantic_overlap_score(new_text, old_text) >= threshold:
            return True
 
    return False

def parse_article_json(article_raw: str, keyword: str, cluster_name: str, post_type: str) -> Dict[str, Any]:
    raw = _find_balanced_json(article_raw)
    data = json.loads(raw)

    if not isinstance(data, dict):
        raise ValueError("article JSON root is not object")

    title = _clean_text(data.get("title", "")) or keyword.title()
    description = _clean_text(data.get("description", ""))
    category = _clean_text(data.get("category", "")) or pick_category(
        keyword=keyword,
        cluster_name=cluster_name,
        post_type=post_type,
    )

    intent_type = _clean_text(data.get("intent_type", "")).lower()
    if intent_type not in {"comparison", "template", "review", "howto"}:
        intent_type = infer_search_intent_type(keyword, category)

    sections = data.get("sections") or []
    if not isinstance(sections, list):
        sections = []

    clean_sections = []
    for s in sections:
        if not isinstance(s, dict):
            continue

        heading = _clean_text(s.get("heading", ""))
        body = _clean_text(s.get("body", ""))
        image_query = _clean_text(s.get("image_query", ""))
        visual_type = _clean_text(s.get("visual_type", "photo")).lower()
        alt_text = _clean_text(s.get("alt_text", "")) or heading

        if visual_type not in {"photo", "diagram", "workspace"}:
            visual_type = "photo"

        if heading and body:
            clean_sections.append({
                "heading": heading,
                "body": body,
                "image_query": image_query or heading,
                "visual_type": visual_type,
                "alt_text": alt_text,
            })

    faq = data.get("faq") or []
    if not isinstance(faq, list):
        faq = []

    clean_faq = []
    for item in faq:
        if not isinstance(item, dict):
            continue
        q = _clean_text(item.get("q", ""))
        a = _clean_text(item.get("a", ""))
        if q and a:
            clean_faq.append({"q": q, "a": a})

    tldr = _clean_text(data.get("tldr", ""))
    editorial_note = _clean_text(data.get("editorial_note", ""))
    if not editorial_note:
        editorial_note = "This article is reviewed for practical usefulness and updated when information changes."

    return {
        "title": title,
        "description": description,
        "category": category,
        "intent_type": intent_type,
        "sections": clean_sections,
        "faq": clean_faq[:5],
        "tldr": tldr,
        "editorial_note": editorial_note,
    }
 
def generate_deep_post(
    *,
    keyword: str,
    cluster_name: str,
    post_type: str,
    avoid_titles: List[str],
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    t0 = time.time()

    planning_raw = openai_generate_text(
        build_planning_prompt(keyword, avoid_titles, cluster_name, post_type),
        model=MODEL_PLANNER,
        temperature=0.55,
    )
    planning = parse_planning_json(planning_raw, keyword=keyword, cluster_name=cluster_name, post_type=post_type)

    article_raw = openai_generate_text(
        build_article_prompt(keyword, cluster_name, post_type, planning),
        model=MODEL_WRITER,
        temperature=0.6,
    )
    data = parse_article_json(article_raw, keyword=keyword, cluster_name=cluster_name, post_type=post_type)

    elapsed = time.time() - t0
    log("GEN", f"Full generation keyword='{keyword}' took {elapsed:.2f}s")

    if not data.get("description"):
        data["description"] = planning.get("description") or short_desc(data.get("title", ""))

    if not data.get("category"):
        data["category"] = planning.get("category") or pick_category(
            keyword=keyword,
            cluster_name=cluster_name,
            post_type=post_type,
        )

    return data, planning
 
 
# =========================================================
# Images and visuals
# =========================================================
def sanitize_query_for_image(q: str) -> str:
    q = (q or "").strip().lower()

    replacements = {
        "client retention system": "crm dashboard laptop",
        "client retention": "freelancer crm dashboard",
        "decision framework": "comparison chart laptop",
        "practical approach": "workspace planning desk",
        "template checklist": "checklist notebook desk",
        "follow-up automation": "crm automation dashboard",
        "offboarding": "client handoff desk",
        "reactivation": "email follow up workspace",
        "simple long term portfolio": "investment portfolio laptop",
        "beginner portfolio allocation": "portfolio allocation chart",
        "monthly investing plan": "budget spreadsheet laptop",
        "etf comparison": "etf comparison chart",
        "risk tolerance": "investment risk chart",
        "ai stocks": "stock market dashboard",
        "investment performance": "investment dashboard laptop",
        "screening process": "stock chart laptop",
        "beginner investing": "finance workspace desk",
    }

    for src, dst in replacements.items():
        if src in q:
            q = q.replace(src, dst)

    q = re.sub(
        r"\b(workflow|system|checklist|template|playbook|automation|process|guide|how to|why|what|when|best|mistake|tradeoff|decision|quick answer|final recommendation|for beginners|worth it)\b",
        "",
        q,
        flags=re.IGNORECASE,
    )
    q = re.sub(r"[^a-z0-9\s]", " ", q)
    q = re.sub(r"\s+", " ", q).strip()

    words = q.split()

    stop_words = {
        "the", "a", "an", "this", "that", "these", "those",
        "most", "more", "less", "real", "simple", "wrong",
        "pick", "using", "start", "with", "your", "their",
        "into", "from", "begin", "easy", "easily", "guide",
    }
    words = [w for w in words if w not in stop_words]

    q = " ".join(words[:4])

    return q or "modern office workspace laptop"
 

def build_image_query_candidates(query: str, heading: str = "", visual_type: str = "photo") -> List[str]:
    base = sanitize_query_for_image(query)
    heading_clean = sanitize_query_for_image(heading)

    candidates = []

    def add(q: str) -> None:
        q = re.sub(r"\s+", " ", (q or "").strip())
        if q and q not in candidates:
            candidates.append(q)

    add(base)
    add(heading_clean)

    if (visual_type or "").lower() == "diagram":
        add("business dashboard laptop")
    elif (visual_type or "").lower() == "workspace":
        add("workspace desk laptop")
    else:
        add("modern office desk")

    return candidates[:3]
 

def normalize_asset_id(source: str, raw_id: str) -> str:
    return f"{source}:{raw_id}"
 
 
def score_query_match(query: str, haystack: str) -> float:
    q = normalize_keyword(query)
    h = normalize_keyword(haystack)
    if not q or not h:
        return 0.0
    return similarity_ratio(q, h)
 
 
def build_image_alt(title: str, heading: str, image_query: str) -> str:
    base = (heading or image_query or title or "article visual").strip()
    base = re.sub(r"\s+", " ", base).strip()
    if len(base) > 140:
        base = base[:137].rstrip() + "..."
    return base
 
 
# -----------------------------
# Unsplash
# -----------------------------
def unsplash_search(query: str, page: int = 1) -> List[dict]:
    if not UNSPLASH_ACCESS_KEY:
        return []

    try:
        url = "https://api.unsplash.com/search/photos"
        headers = {
            "Accept-Version": "v1",
            "Authorization": f"Client-ID {UNSPLASH_ACCESS_KEY}",
            "User-Agent": "MingMongBot/1.0 (https://mingmonglife.com)",
        }
        params = {
            "query": query,
            "page": page,
            "per_page": UNSPLASH_PER_PAGE,
            "orientation": "landscape",
            "content_filter": "high",
        }
        r = requests.get(url, headers=headers, params=params, timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        results = data.get("results") or []

        out = []
        for item in results:
            try:
                pid = str(item.get("id") or "").strip()
                if not pid:
                    continue

                w = int(item.get("width") or 0)
                h = int(item.get("height") or 0)
                likes = int(item.get("likes") or 0)
                if w < UNSPLASH_MIN_WIDTH or h < UNSPLASH_MIN_HEIGHT:
                    continue
                if likes < UNSPLASH_MIN_LIKES:
                    continue

                ratio = w / max(h, 1)
                if ratio < 1.2 or ratio > 2.2:
                    continue

                urls = item.get("urls") or {}
                hotlink_url = urls.get("regular") or urls.get("full") or urls.get("raw")
                if not hotlink_url:
                    continue

                links = item.get("links") or {}
                download_location = (links.get("download_location") or "").strip()

                user = item.get("user") or {}
                user_name = (user.get("name") or "").strip()
                user_link = ((user.get("links") or {}).get("html") or "").strip()
                page_link = (links.get("html") or "").strip()
                if not user_name or not user_link or not page_link:
                    continue

                desc = " ".join([
                    str(item.get("description") or ""),
                    str(item.get("alt_description") or ""),
                    user_name,
                ]).strip()

                out.append({
                    "download_location": (links.get("download_location") or "").strip(),
                    "source": "unsplash",
                    "id": normalize_asset_id("unsplash", pid),
                    "raw_id": pid,
                    "width": w,
                    "height": h,
                    "score": score_query_match(query, desc) + min(likes / 500.0, 0.4),
                    "hotlink_url": hotlink_url,
                    "page_url": page_link,
                    "creator_name": user_name,
                    "creator_url": user_link,
                })
            except Exception:
                continue

        out.sort(key=lambda x: x["score"], reverse=True)
        return out
    except Exception as e:
        log("IMG", f"Unsplash search failed for '{query}': {e}")
        return []
 
 
# -----------------------------
# Pexels
# -----------------------------
def pexels_search(query: str, page: int = 1) -> List[dict]:
    if not PEXELS_API_KEY:
        return []
 
    try:
        url = "https://api.pexels.com/v1/search"
        headers = {"Authorization": PEXELS_API_KEY}
        params = {
            "query": query,
            "page": page,
            "per_page": PEXELS_PER_PAGE,
            "orientation": "landscape",
        }
        r = requests.get(url, headers=headers, params=params, timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        results = data.get("photos") or []
 
        out = []
        for item in results:
            try:
                pid = str(item.get("id") or "").strip()
                if not pid:
                    continue
 
                w = int(item.get("width") or 0)
                h = int(item.get("height") or 0)
                if w < PEXELS_MIN_WIDTH or h < PEXELS_MIN_HEIGHT:
                    continue
 
                ratio = w / max(h, 1)
                if ratio < 1.2 or ratio > 2.2:
                    continue
 
                src = item.get("src") or {}
                download_url = src.get("large2x") or src.get("large") or src.get("original")
                if not download_url:
                    continue
 
                creator_name = (item.get("photographer") or "").strip()
                creator_url = (item.get("photographer_url") or "").strip()
                page_url = (item.get("url") or "").strip()
 
                desc = " ".join([
                    creator_name,
                    str(item.get("alt") or ""),
                    str(item.get("avg_color") or ""),
                ]).strip()
 
                out.append({
                    "source": "pexels",
                    "id": normalize_asset_id("pexels", pid),
                    "raw_id": pid,
                    "width": w,
                    "height": h,
                    "score": score_query_match(query, desc),
                    "download_url": download_url,
                    "page_url": page_url or creator_url,
                    "creator_name": creator_name or "Pexels contributor",
                    "creator_url": creator_url or "https://www.pexels.com",
                })
            except Exception:
                continue
 
        out.sort(key=lambda x: x["score"], reverse=True)
        return out
    except Exception as e:
        log("IMG", f"Pexels search failed for '{query}': {e}")
        return []
 
 
# -----------------------------
# Pixabay
# -----------------------------
def pixabay_search(query: str, page: int = 1) -> List[dict]:
    if not PIXABAY_API_KEY:
        return []
 
    try:
        url = "https://pixabay.com/api/"
        params = {
            "key": PIXABAY_API_KEY,
            "q": query,
            "image_type": "photo",
            "orientation": "horizontal",
            "safesearch": "true",
            "page": page,
            "per_page": PIXABAY_PER_PAGE,
        }
        r = requests.get(url, params=params, timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        results = data.get("hits") or []
 
        out = []
        for item in results:
            try:
                pid = str(item.get("id") or "").strip()
                if not pid:
                    continue
 
                w = int(item.get("imageWidth") or 0)
                h = int(item.get("imageHeight") or 0)
                if w < PIXABAY_MIN_WIDTH or h < PIXABAY_MIN_HEIGHT:
                    continue
 
                ratio = w / max(h, 1)
                if ratio < 1.2 or ratio > 2.2:
                    continue
 
                download_url = (item.get("largeImageURL") or item.get("webformatURL") or "").strip()
                if not download_url:
                    continue
 
                creator_name = (item.get("user") or "").strip()
                page_url = (item.get("pageURL") or "").strip()
                tags = (item.get("tags") or "").strip()
 
                out.append({
                    "source": "pixabay",
                    "id": normalize_asset_id("pixabay", pid),
                    "raw_id": pid,
                    "width": w,
                    "height": h,
                    "score": score_query_match(query, tags),
                    "download_url": download_url,
                    "page_url": page_url or "https://pixabay.com",
                    "creator_name": creator_name or "Pixabay contributor",
                    "creator_url": "https://pixabay.com",
                })
            except Exception:
                continue
 
        out.sort(key=lambda x: x["score"], reverse=True)
        return out
    except Exception as e:
        log("IMG", f"Pixabay search failed for '{query}': {e}")
        return []
 
 
# -----------------------------
# Wikimedia Commons
# -----------------------------
def wikimedia_search(query: str, page: int = 1) -> List[dict]:
    if not ENABLE_WIKIMEDIA:
        return []
 
    try:
        offset = (page - 1) * 20
        params = {
            "action": "query",
            "generator": "search",
            "gsrsearch": f"filetype:bitmap {query}",
            "gsrnamespace": 6,
            "gsrlimit": 20,
            "gsroffset": offset,
            "prop": "imageinfo",
            "iiprop": "url",
            "iiurlwidth": 1800,
            "format": "json",
            "origin": "*",
        }
        r = requests.get("https://commons.wikimedia.org/w/api.php", params=params, timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        pages = (data.get("query") or {}).get("pages") or {}
 
        out = []
        for _, item in pages.items():
            try:
                title = str(item.get("title") or "").strip()
                if not title:
                    continue
 
                pageid = str(item.get("pageid") or title).strip()
                imageinfo = item.get("imageinfo") or []
                if not imageinfo:
                    continue
 
                info = imageinfo[0]
                thumb = (info.get("thumburl") or info.get("url") or "").strip()
                if not thumb:
                    continue
 
                w = int(info.get("thumbwidth") or info.get("width") or 0)
                h = int(info.get("thumbheight") or info.get("height") or 0)
                if w < 1000 or h < 600:
                    continue
 
                ratio = w / max(h, 1)
                if ratio < 1.1 or ratio > 2.4:
                    continue
 
                page_url = f"https://commons.wikimedia.org/wiki/{title.replace(' ', '_')}"
                desc = " ".join([
                    title,
                    str(item.get("snippet") or ""),
                ]).strip()
 
                out.append({
                    "source": "wikimedia",
                    "id": normalize_asset_id("wikimedia", pageid),
                    "raw_id": pageid,
                    "width": w,
                    "height": h,
                    "score": score_query_match(query, desc),
                    "download_url": thumb,
                    "page_url": page_url,
                    "creator_name": "Wikimedia Commons",
                    "creator_url": "https://commons.wikimedia.org",
                })
            except Exception:
                continue
 
        out.sort(key=lambda x: x["score"], reverse=True)
        return out
    except Exception as e:
        log("IMG", f"Wikimedia search failed for '{query}': {e}")
        return []
 
 
def search_source(source: str, query: str, page: int = 1) -> List[dict]:
    if source == "unsplash":
        return unsplash_search(query, page=page)
    if source == "pexels":
        return pexels_search(query, page=page)
    if source == "pixabay":
        return pixabay_search(query, page=page)
    if source == "wikimedia":
        return wikimedia_search(query, page=page)
    return []
 

def trigger_unsplash_download(asset: dict) -> None:
    if (asset.get("source") or "").strip().lower() != "unsplash":
        return

    download_location = (asset.get("download_location") or "").strip()
    if not download_location:
        log("IMG", f"Unsplash download trigger skipped: missing download_location for id='{asset.get('id', '')}'")
        return

    try:
        headers = {
            "Accept-Version": "v1",
            "Authorization": f"Client-ID {UNSPLASH_ACCESS_KEY}",
            "User-Agent": "MingMongBot/1.0 (https://mingmonglife.com)",
        }
        r = requests.get(download_location, headers=headers, timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        log("IMG", f"Unsplash download triggered for id='{asset.get('id', '')}'")
    except Exception as e:
        log("IMG", f"Unsplash download trigger failed for id='{asset.get('id', '')}': {e}")


def download_asset(asset: dict, out_path: Path) -> None:
    url = asset.get("download_url") or ""
    if not url:
        raise RuntimeError("download_url missing")
 
    if asset.get("source") == "unsplash":
        if "?" in url:
            url = url + "&fm=jpg&q=80&w=1800&fit=max"
        else:
            url = url + "?fm=jpg&q=80&w=1800&fit=max"
 
    r = requests.get(url, timeout=HTTP_TIMEOUT, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_bytes(r.content)
 

import math
from html import escape as escape_xml


def estimate_text_width(text: str, font_size: int = 12) -> float:
    text = str(text or "")
    wide = sum(1 for ch in text if ord(ch) > 127)
    narrow = len(text) - wide
    return narrow * font_size * 0.56 + wide * font_size * 0.9



def svg_text_block(x: float, y: float, lines: list[str], font_size: int = 12,
                   fill: str = "#1F2937", weight: str = "500",
                   line_gap: float = 1.35, anchor: str = "start") -> str:
    parts = [
        f'<text x="{x}" y="{y}" font-size="{font_size}" '
        f'font-weight="{weight}" fill="{fill}" text-anchor="{anchor}" '
        f'font-family="Inter, Arial, sans-serif">'
    ]

    for i, line in enumerate(lines):
        dy = "0" if i == 0 else str(font_size * line_gap)
        parts.append(f'<tspan x="{x}" dy="{dy}">{escape_xml(line)}</tspan>')

    parts.append("</text>")
    return "".join(parts)

def wrap_text_to_width(text: str, max_width: int, font_size: int = 20):
    words = (text or "").split()
    if not words:
        return [""]

    max_chars = max(10, int(max_width / (font_size * 0.55)))

    lines = []
    current = ""

    for word in words:
        test = f"{current} {word}".strip()
        if len(test) <= max_chars:
            current = test
        else:
            if current:
                lines.append(current)
            current = word

    if current:
        lines.append(current)

    return lines


def build_svg_placeholder(
    out_path: Path,
    title: str,
    heading: str,
    image_query: str,
    visual_type: str = "photo",
) -> None:
    w = 1600
    h = 900

    bg = "#F8FAFC"
    panel = "#FFFFFF"
    stroke = "#E2E8F0"
    title_color = "#0F172A"
    body_color = "#475569"
    accent = "#CBD5E1"

    safe_heading = (heading or "Article visual").strip()
    safe_query = (image_query or "").strip()
    safe_type = (visual_type or "photo").strip().title()

    heading_lines = wrap_text_to_width(safe_heading, 980, font_size=44)[:3]
    query_lines = wrap_text_to_width(safe_query, 900, font_size=24)[:3]

    svg = f'''<svg xmlns="http://www.w3.org/2000/svg" width="{w}" height="{h}" viewBox="0 0 {w} {h}">
  <rect width="100%" height="100%" fill="{bg}"/>
  <rect x="70" y="70" width="1460" height="760" rx="32" fill="{panel}" stroke="{stroke}" stroke-width="2"/>
  <rect x="120" y="120" width="220" height="40" rx="20" fill="{accent}"/>
  <text x="230" y="147" font-size="18" font-weight="700" fill="{body_color}" text-anchor="middle" font-family="Inter, Arial, sans-serif">{escape_xml(safe_type)} visual</text>
  <line x1="120" y1="220" x2="1480" y2="220" stroke="{stroke}" stroke-width="2"/>

  {svg_text_block(120, 310, heading_lines, font_size=44, fill=title_color, weight="700")}
  {svg_text_block(120, 470, query_lines, font_size=24, fill=body_color, weight="500")}

  <rect x="120" y="620" width="340" height="110" rx="24" fill="#EFF6FF" stroke="#BFDBFE"/>
  <text x="150" y="665" font-size="22" font-weight="700" fill="{title_color}" font-family="Inter, Arial, sans-serif">Fallback image</text>
  <text x="150" y="703" font-size="18" fill="{body_color}" font-family="Inter, Arial, sans-serif">Generated because no external asset matched.</text>
</svg>'''

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(svg, encoding="utf-8")


def ensure_minimum_image_paths(
    slug: str,
    image_paths: List[str],
    alt_texts: List[str],
    sections: List[Dict[str, str]],
    min_count: int,
) -> Tuple[List[str], List[str]]:
    folder = ASSETS_POSTS_DIR / slug

    if len(image_paths) < len(sections):
        image_paths.extend([""] * (len(sections) - len(image_paths)))

    if len(alt_texts) < len(sections):
        for i in range(len(alt_texts), len(sections)):
            sec = sections[i] if i < len(sections) else {}
            alt_texts.append(sec.get("alt_text") or sec.get("heading") or f"Section {i+1}")

    non_empty_count = sum(1 for p in image_paths if isinstance(p, str) and p.strip())

    for i, sec in enumerate(sections):
        if non_empty_count >= min_count:
            break

        current_path = image_paths[i] if i < len(image_paths) else ""
        if isinstance(current_path, str) and current_path.strip():
            continue

        idx = i + 1
        heading = sec.get("heading", f"Section {idx}")
        image_query = sec.get("image_query", heading)
        alt_text = sec.get("alt_text", heading) or heading

        svg_path = folder / f"{idx}.svg"
        build_svg_placeholder(
            out_path=svg_path,
            title=slug,
            heading=heading,
            image_query=image_query,
            visual_type=sec.get("visual_type", "photo"),
        )

        rel_path = f"assets/posts/{slug}/{idx}.svg"

        if i < len(image_paths):
            image_paths[i] = rel_path
        else:
            image_paths.append(rel_path)

        if i < len(alt_texts):
            alt_texts[i] = alt_text
        else:
            alt_texts.append(alt_text)

        non_empty_count += 1
        log("IMG", f"Fallback filled empty slot slug='{slug}' idx={idx}")

    return image_paths, alt_texts
 
 
def find_best_asset_for_query(query: str, heading: str, visual_type: str, used_ids: set) -> Optional[dict]:
    query_candidates = build_image_query_candidates(query, heading, visual_type)

    for candidate_query in query_candidates[:3]:
        for source in IMAGE_SOURCE_PRIORITY:
            for page in [1]:
                results = search_source(source, candidate_query, page=page)
                if not results:
                    continue

                filtered = [asset for asset in results if asset["id"] not in used_ids]
                if filtered:
                    filtered.sort(key=lambda x: x.get("score", 0.0), reverse=True)
                    best = filtered[0]
                    log("IMG", f"Best asset source={best.get('source')} id={best.get('id')} query='{candidate_query}'")
                    return best

    log("IMG", f"No asset found for query='{query}' heading='{heading}'")
    return None
    
 
def build_image_asset_for_section(
    slug: str,
    idx: int,
    heading: str,
    image_query: str,
    visual_type: str,
    alt_hint: str,
    used_ids: set,
) -> Tuple[str, str, Optional[str], set]:
    alt_text = alt_hint or build_image_alt(heading, heading, image_query)

    clean_query = sanitize_query_for_image(
        (image_query or "").strip() or (heading or "").strip()
    ) or "modern office workspace laptop"

    alt_text = alt_hint or build_image_alt(heading, heading, clean_query)

    should_try_external = len(clean_query.split()) >= 1
    if should_try_external:
        asset = find_best_asset_for_query(
            query=clean_query,
            heading=heading,
            visual_type=visual_type,
            used_ids=used_ids,
        )

        if asset:
            hotlink_url = (asset.get("hotlink_url") or "").strip()
            if hotlink_url:
                used_ids.add(asset["id"])
                trigger_unsplash_download(asset)
             
                creator_name = html_escape(asset.get("creator_name") or asset.get("source", "Image source"))
                creator_url = html_escape(asset.get("creator_url") or asset.get("page_url") or "#")
                page_url = html_escape(asset.get("page_url") or creator_url)
                source_label = html_escape(asset.get("source", "source").title())

                photo_credit_html = (
                    f'<li>Photo {idx}: '
                    f'<a href="{creator_url}" target="_blank" rel="noopener noreferrer">{creator_name}</a> '
                    f'via <a href="{page_url}" target="_blank" rel="noopener noreferrer">{source_label}</a></li>'
                )

                log("IMG", f"Using hotlink image for slug='{slug}' idx={idx} source='{asset.get('source')}'")
                return hotlink_url, alt_text, photo_credit_html, used_ids

            log("IMG", f"Asset found but hotlink_url missing for slug='{slug}' idx={idx} source='{asset.get('source')}'")

    log("IMG", f"No external image found for slug='{slug}' idx={idx} query='{clean_query}'")
    return "", alt_text, None, used_ids
 
 
def build_visual_assets(slug: str, sections: List[Dict[str, str]]) -> Tuple[List[str], List[str], List[str]]:
    used_raw = load_json(USED_IMAGES_JSON, {})
    used = ensure_used_schema(used_raw)
    used_ids = set(used.get("asset_ids") or [])

    image_paths: List[str] = []
    alt_texts: List[str] = []
    credits_li: List[str] = []

    table_count = sum(1 for sec in sections if section_has_html_table(sec))
    target_count = min(
        len(sections),
        max(COLLECT_TARGET_IMAGES, VISIBLE_MIN_IMAGES + table_count)
    )
    table_sections = [sec for sec in sections if section_has_html_table(sec)]
    non_table_sections = [sec for sec in sections if not section_has_html_table(sec)]

    preferred_sections = non_table_sections + table_sections

    target_count = min(
        len(preferred_sections),
        max(COLLECT_TARGET_IMAGES, VISIBLE_MIN_IMAGES + len(table_sections))
    )
    target_sections = preferred_sections[:target_count]
 
    for i, sec in enumerate(target_sections, start=1):
        path, alt, credit, used_ids = build_image_asset_for_section(
            slug=slug,
            idx=i,
            heading=sec.get("heading", f"Section {i}"),
            image_query=sec.get("image_query", sec.get("heading", "")),
            visual_type=sec.get("visual_type", "photo"),
            alt_hint=sec.get("alt_text", sec.get("alt_hint", sec.get("heading", ""))),
            used_ids=used_ids,
        )
        image_paths.append(path)
        alt_texts.append(alt or sec.get("heading", f"Section {i}"))
        if credit:
            credits_li.append(credit)

    used["asset_ids"] = sorted(list(used_ids))
    save_json(USED_IMAGES_JSON, used)

    non_empty_count = sum(1 for p in image_paths if isinstance(p, str) and p.strip())
    log(
        "IMG",
        f"slug='{slug}' requested={len(target_sections)} found={non_empty_count} table_sections={table_count}"
    )
 
    return image_paths, alt_texts, credits_li
 
 
# =========================================================
# Internal links
# =========================================================
def resolve_post_url_path(p: dict) -> str:
    if not isinstance(p, dict):
        return ""
    url = (p.get("url") or "").strip()
    slug = (p.get("slug") or "").strip()
 
    if url:
        url = url.lstrip("/")
        if url.endswith(".md"):
            url = url[:-3] + ".html"
        if url.startswith("posts/") and "." not in Path(url).name:
            url = url + ".html"
        return url
 
    if slug:
        return f"posts/{slug}.html"
    return ""
 
 
def post_href_from_post_page(p: dict) -> str:
    url = resolve_post_url_path(p)
    if not url:
        return "#"
    if url.startswith("posts/"):
        return url.split("/", 1)[1]
    return "../" + url
 
 
def select_related_posts(
    posts: List[dict],
    *,
    current_slug: str,
    category: str,
    cluster: str,
    pillar_slug: str = "",
    limit: int = 4,
) -> List[dict]:
    scored = []
 
    for p in posts:
        if not isinstance(p, dict):
            continue
        slug = (p.get("slug") or "").strip()
        if not slug or slug == current_slug:
            continue
 
        score = 0
        if p.get("cluster") == cluster:
            score += 50
        if p.get("category") == category:
            score += 20
        if p.get("post_type") == "pillar":
            score += 15
        if pillar_slug and slug == pillar_slug:
            score += 100
 
        date_text = str(p.get("updated") or p.get("date") or "")
        if date_text:
            score += 2
 
        if p.get("keyword") and p.get("title"):
            score += int(semantic_overlap_score(category + " " + cluster, (p.get("category") or "") + " " + (p.get("cluster") or "")) * 10)
 
        scored.append((score, p))
 
    scored.sort(key=lambda x: x[0], reverse=True)
 
    out = []
    seen = set()
    for _, p in scored:
        slug = p.get("slug")
        if slug in seen:
            continue
        seen.add(slug)
        out.append(p)
        if len(out) >= limit:
            break
    return out
 
 
def render_related_guides_html(related_posts: List[dict]) -> str:
    if not related_posts:
        return ""
 
    items = []
    for p in related_posts:
        href = post_href_from_post_page(p)
        title = html_escape(p.get("title") or "Untitled")
        kicker = html_escape(p.get("category") or "Article")
        badge = ""
        if p.get("post_type") == "pillar":
            badge = '<span class="rg-badge">Guide</span>'
        items.append(
            f'<a class="related-guide" href="{href}">'
            f'<span class="rg-kicker">{kicker}</span>'
            f'<span class="rg-title">{title}</span>'
            f'{badge}'
            f'</a>'
        )
 
    return (
        '<section class="related-guides">'
        '<h2>Related Guides</h2>'
        '<div class="related-guides-list">'
        + "".join(items)
        + "</div></section>"
    )
 
def render_conversion_cta_html(category: str, keyword: str = "") -> str:
    cat = (category or "").strip()

    if cat == "Software Reviews":
        title = "Choose faster"
        desc = "Use this article to narrow the field then compare your top options by price, setup friction, and upgrade risk."
        links = [
            ("Best free tools for solo freelancers", "../category.html?cat=Software%20Reviews"),
            ("Compare similar software guides", "../category.html?cat=Software%20Reviews"),
        ]
    elif cat == "Productivity":
        title = "Turn this into a workflow"
        desc = "Do not stop at ideas. Pick one workflow, test it for 14 days, then keep only what reduces friction."
        links = [
            ("See more workflow guides", "../category.html?cat=Productivity"),
            ("Browse practical checklists", "../category.html?cat=Productivity"),
        ]
    else:
        title = "Next step"
        desc = "Use the article as a decision point, not just reading material. Your next system matters more than more browsing."
        links = [
            ("Read related guides", "../index.html"),
            ("Browse more categories", "../category.html"),
        ]

    items = "".join(
        f'<a class="cta-link" href="{href}">{html_escape(label)}</a>'
        for label, href in links
    )

    return (
        '<section class="conversion-cta">'
        f'<h2>{html_escape(title)}</h2>'
        f'<p>{html_escape(desc)}</p>'
        f'<div class="cta-links">{items}</div>'
        '</section>'
    )
    
# =========================================================
# Slug and redirects
# =========================================================
def build_clean_slug(title: str, keyword: str = "") -> str:
    raw = slugify(title) or slugify(keyword) or f"post-{int(time.time())}"
    raw = raw[:72].strip("-")
    raw = re.sub(r"-{2,}", "-", raw).strip("-")
    if len(raw) < 12:
        raw = f"{raw}-{int(time.time())}"
    return raw
 
 
def load_redirects() -> Dict[str, str]:
    raw = load_json(REDIRECTS_JSON, {})
    if isinstance(raw, dict):
        return {str(k): str(v) for k, v in raw.items()}
    return {}
 
 
def save_redirects(data: Dict[str, str]) -> None:
    save_json(REDIRECTS_JSON, data)
 
 
def normalize_existing_post(p: dict) -> dict:
    if not isinstance(p, dict):
        return p
 
    slug = (p.get("slug") or "").strip()
    title = (p.get("title") or "").strip()
    keyword = (p.get("keyword") or "").strip()
    cluster = (p.get("cluster") or "").strip()
    post_type = (p.get("post_type") or "normal").strip()
 
    if title:
        title = re.sub(r"\bin (2019|2020|2021|2022|2023|2024)\b", f"in {current_year_utc()}", title, flags=re.IGNORECASE)
 
    if slug:
        slug = slug.strip("-")[:72]
    elif title or keyword:
        slug = build_clean_slug(title or keyword, keyword)
 
    url = resolve_post_url_path(p)
    category = p.get("category") or pick_category(keyword=keyword or title, cluster_name=cluster, post_type=post_type)

    legacy_map = {
        "Freelance Systems": "Productivity",
        "Creator Income": "Make Money",
    }
    category = legacy_map.get(category, category)

    if category not in ALLOWED_CATEGORIES:
        category = pick_category(keyword=keyword or title, cluster_name=cluster, post_type=post_type)
     
    p["title"] = title
    p["slug"] = slug
    p["url"] = url if url else f"posts/{slug}.html"
    p["category"] = category
 
    if not p.get("description"):
        p["description"] = short_desc(title)
 
    if p["url"].endswith(".md"):
        p["url"] = p["url"][:-3] + ".html"
 
    if "updated" not in p and p.get("date"):
        p["updated"] = p["date"]
 
    return p
 
def first_non_empty_image(image_paths: List[str]) -> str:
    for p in image_paths:
        if isinstance(p, str) and p.strip():
            return p.strip()
    return ""
    
# =========================================================
# HTML rendering helpers
# =========================================================
def soften_dense_paragraphs(text: str) -> str:
    text = (text or "").strip()
    if not text:
        return ""

    blocks = re.split(r"\n\s*\n+", text)
    out = []

    for block in blocks:
        block = block.strip()
        if not block:
            continue

        if len(block) > 420 and ". " in block:
            parts = re.split(r"(?<=[.!?])\s+", block)
            chunk = []
            chunk_len = 0
            for part in parts:
                chunk.append(part)
                chunk_len += len(part)
                if chunk_len >= 180:
                    out.append(" ".join(chunk).strip())
                    chunk = []
                    chunk_len = 0
            if chunk:
                out.append(" ".join(chunk).strip())
        else:
            out.append(block)

    return "\n\n".join(out)

def paragraphs_to_html(text: str) -> str:
    text = soften_dense_paragraphs((text or "").strip())
    if not text:
        return ""

    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r'(?<!\n)(\s+)(\d+\.\s+)', r'\n\2', text)

    blocks = re.split(r"\n\s*\n+", text)

    out = []
    for block in blocks:
        block = block.strip()
        if not block:
            continue

        lines = [ln.strip() for ln in block.split("\n") if ln.strip()]
        if not lines:
            continue

        numbered_items = []
        bullet_items = []
        is_numbered_block = True
        is_bullet_block = True

        for ln in lines:
            m_num = re.match(r"^\s*(\d+)\.\s+(.*)$", ln)
            m_bullet = re.match(r"^\s*[-*]\s+(.*)$", ln)

            if m_num:
                numbered_items.append(m_num.group(2).strip())
            else:
                is_numbered_block = False

            if m_bullet:
                bullet_items.append(m_bullet.group(1).strip())
            else:
                is_bullet_block = False

        if is_numbered_block and numbered_items:
            items = "".join(f"<li>{html_escape(item)}</li>" for item in numbered_items)
            out.append(f"<ol>{items}</ol>")
            continue

        if is_bullet_block and bullet_items:
            items = "".join(f"<li>{html_escape(item)}</li>" for item in bullet_items)
            out.append(f"<ul>{items}</ul>")
            continue

        para = " ".join(lines)
        out.append(f"<p>{html_escape(para)}</p>")

    return "\n".join(out)


def section_has_html_table(section: Dict[str, str]) -> bool:
    body = (section.get("body") or "").lower()
    return "<table" in body and "</table>" in body


def body_to_html(text: str) -> str:
    text = (text or "").strip()
    if not text:
        return ""

    # HTML table이 포함된 경우
    if "<table" in text.lower() and "</table>" in text.lower():
        blocks = re.split(r"\n\s*\n+", text)
        out = []

        for block in blocks:
            block = block.strip()
            if not block:
                continue

            if "<table" in block.lower() and "</table>" in block.lower():
                out.append(block)
            else:
                lines = [ln.strip() for ln in block.split("\n") if ln.strip()]
                if lines:
                    para = " ".join(lines)
                    out.append(f"<p>{html_escape(para)}</p>")

        return "\n".join(out)

    return paragraphs_to_html(text)


def build_json_ld(
    *,
    title: str,
    description: str,
    canonical: str,
    og_image: str,
    updated_iso: str,
    faq: List[Dict[str, str]],
    category: str,
) -> str:
    article = {
        "@context": "https://schema.org",
        "@type": "Article",
        "headline": title,
        "description": description,
        "mainEntityOfPage": canonical,
        "image": [og_image] if og_image else [],
        "datePublished": updated_iso,
        "dateModified": updated_iso,
        "author": {
            "@type": "Organization",
            "name": AUTHOR_NAME,
            "url": AUTHOR_URL,
        },
        "publisher": {
            "@type": "Organization",
            "name": SITE_NAME,
            "url": SITE_URL,
        },
        "articleSection": category,
    }
 
    breadcrumb = {
        "@context": "https://schema.org",
        "@type": "BreadcrumbList",
        "itemListElement": [
            {"@type": "ListItem", "position": 1, "name": "Home", "item": f"{SITE_URL}/index.html"},
            {"@type": "ListItem", "position": 2, "name": category, "item": f"{SITE_URL}/category.html?cat={category.replace(' ', '%20')}"},
            {"@type": "ListItem", "position": 3, "name": title, "item": canonical},
        ],
    }
 
    blocks = [
        f'<script type="application/ld+json">{json.dumps(article, ensure_ascii=False)}</script>',
        f'<script type="application/ld+json">{json.dumps(breadcrumb, ensure_ascii=False)}</script>',
    ]
 
    if faq:
        faq_schema = {
            "@context": "https://schema.org",
            "@type": "FAQPage",
            "mainEntity": [
                {
                    "@type": "Question",
                    "name": item["q"],
                    "acceptedAnswer": {
                        "@type": "Answer",
                        "text": item["a"],
                    },
                }
                for item in faq
            ],
        }
        blocks.append(f'<script type="application/ld+json">{json.dumps(faq_schema, ensure_ascii=False)}</script>')
 
    return "\n".join(blocks)
 
 
def render_post_html(
    *,
    title: str,
    description: str,
    category: str,
    updated_iso: str,
    slug: str,
    image_paths: List[str],
    alt_texts: List[str],
    sections: List[Dict[str, str]],
    tldr: str,
    faq: List[Dict[str, str]],
    photo_credits_li: List[str],
    related_posts: List[dict],
    post_type: str,
    editorial_note: str,
    keyword: str,
) -> str:
    canonical = f"{SITE_URL}/posts/{slug}.html"
    primary_image = first_non_empty_image(image_paths)
    if primary_image.startswith("http://") or primary_image.startswith("https://"):
        og_image = primary_image
    else:
        og_image = f"{SITE_URL}/{primary_image}" if primary_image else ""
 
    blocks = []
    blocks.append("<h2>TL;DR</h2>")
    blocks.append(paragraphs_to_html(tldr))
 
    for i, sec in enumerate(sections):
        img_path = image_paths[i] if i < len(image_paths) else ""
        alt = html_escape(alt_texts[i] if i < len(alt_texts) else sec.get("heading", title))
        visual_type = (sec.get("visual_type") or "").strip().lower()
        is_diagram = visual_type == "diagram"

        raw_body = sec.get("body", "") or ""
        has_html_table = "<table" in raw_body.lower() and "</table>" in raw_body.lower()

        if has_html_table:
            img_path = ""
         

        blocks.append(f"<h2>{html_escape(sec['heading'])}</h2>")

        section_body_html = body_to_html(sec["body"])
 
        if img_path:
            img_src = img_path if img_path.startswith("http://") or img_path.startswith("https://") else f"../{img_path}"

            if is_diagram:
                blocks.append(
                    f'''
    <div class="section-media-block section-media-block-diagram">
      <figure class="section-hero-visual">
        <img src="{img_src}" alt="{alt}" loading="lazy">
        <figcaption>{alt}</figcaption>
      </figure>
      {section_body_html}
    </div>
    '''.strip()
                )
            else:
                blocks.append(
                    f'''
    <div class="section-media-block">
      <figure class="section-float">
        <img src="{img_src}" alt="{alt}" loading="lazy">
        <figcaption>{alt}</figcaption>
      </figure>
      {section_body_html}
    </div>
    '''.strip()
                )
        else:
            blocks.append(section_body_html)
 
    if faq:
        blocks.append("<h2>FAQ</h2>")
        for item in faq:
            blocks.append(f"<p><strong>{html_escape(item['q'])}</strong><br>{html_escape(item['a'])}</p>")

    if editorial_note:
        blocks.append(f'<p class="editorial-note">{html_escape(editorial_note)}</p>')

    related_html = render_related_guides_html(related_posts)
    if related_html:
        blocks.append(related_html)
        blocks.append(render_conversion_cta_html(category, keyword))
 
    blocks.append("""
<div class="post-search-block">
  <h2 class="post-search-title">Search more articles</h2>
  <p class="post-search-sub">Find related tools, workflows, and guides.</p>
  <form class="site-search-form js-inline-search-form" autocomplete="off">
    <div class="site-search-bar">
      <span class="site-search-icon">🔍</span>
      <input
        type="search"
        class="site-search-input js-inline-search-input"
        placeholder="Search guides, workflows, templates"
        aria-label="Search guides, workflows, templates"
      />
      <button type="submit" class="site-search-submit">Search</button>
    </div>
  </form>
  <div class="site-search-inline-results js-inline-search-results"></div>
</div>
""".strip())
 
    if photo_credits_li:
        blocks.append("<h2>Photo credits</h2>")
        blocks.append("<ul>" + "\n".join(photo_credits_li) + "</ul>")
 
    article_html = "\n".join([b for b in blocks if b])
 
    adsense_tag = ""
    if ADSENSE_CLIENT:
        adsense_tag = f"""
  <script async
  src="https://pagead2.googlesyndication.com/pagead/js/adsbygoogle.js?client={html_escape(ADSENSE_CLIENT)}"
  crossorigin="anonymous"></script>
""".rstrip()
 
    guide_badge = ""
    if post_type == "pillar":
        guide_badge = '<span class="post-type-badge">Featured Guide</span>'
 
    json_ld = build_json_ld(
        title=title,
        description=description,
        canonical=canonical,
        og_image=og_image,
        updated_iso=updated_iso,
        faq=faq,
        category=category,
    )
 
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>{html_escape(title)} | {html_escape(SITE_NAME)}</title>
  <meta name="description" content="{html_escape(description)}">
  <link rel="canonical" href="{html_escape(canonical)}">
 
  <meta property="og:type" content="article">
  <meta property="og:site_name" content="{html_escape(SITE_NAME)}">
  <meta property="og:title" content="{html_escape(title)}">
  <meta property="og:description" content="{html_escape(description)}">
  <meta property="og:url" content="{html_escape(canonical)}">
  <meta property="og:image" content="{html_escape(og_image)}">
 
  <meta name="twitter:card" content="summary_large_image">
  <meta name="twitter:title" content="{html_escape(title)}">
  <meta name="twitter:description" content="{html_escape(description)}">
  <meta name="twitter:image" content="{html_escape(og_image)}">
 
  <meta name="author" content="{html_escape(AUTHOR_NAME)}">
  <meta name="article:section" content="{html_escape(category)}">
  <meta name="robots" content="index,follow,max-image-preview:large">
 
  <link rel="stylesheet" href="../style.css?v=10">
{adsense_tag}
{json_ld}
</head>
<body>
 
<header class="topbar">
  <div class="container topbar-inner">
    <a class="brand" href="../index.html" aria-label="{html_escape(SITE_NAME)} Home">
      <span class="mark" aria-hidden="true"></span>
      <span>{html_escape(SITE_NAME)}</span>
    </a>
    <nav class="nav" aria-label="Primary">
      <a href="../index.html">Home</a>
      <a href="../about.html">About</a>
      <a href="../contact.html">Contact</a>
    </nav>
  </div>
</header>
 
<main class="container post-page">
  <div class="post-shell has-aside">
 
    <div class="post-main">
      <header class="post-header">
        {guide_badge}
        <div class="kicker">{html_escape(category)}</div>
        <h1 class="post-h1">{html_escape(title)}</h1>
        <p class="post-dek">{html_escape(description)}</p>
        <div class="post-meta">
          <span>{html_escape(category)}</span>
          <span>•</span>
          <span>Updated: {html_escape(updated_iso)}</span>
        </div>
      </header>
 
      <article class="post-content">
        {article_html}
      </article>
    </div>
 
    <aside class="post-aside">
      <div class="sidecard">
        <h3>About this site</h3>
        <p>{html_escape(SITE_TAGLINE)}</p>
        <p>Written and reviewed by {html_escape(AUTHOR_NAME)}.</p>
      </div>
 
<div class="sidecard">
        <h3>Categories</h3>
        <div class="catlist">
          <a class="catitem" href="../category.html?cat=AI%20Tools"><span class="caticon">🤖</span><span class="cattext"><span class="catname">AI Tools</span><span class="catsub">Automation and useful AI</span></span></a>
          <a class="catitem" href="../category.html?cat=Investing"><span class="caticon">📈</span><span class="cattext"><span class="catname">Investing</span><span class="catsub">Beginner stocks and ETFs</span></span></a>
          <a class="catitem" href="../category.html?cat=Make%20Money"><span class="caticon">💰</span><span class="cattext"><span class="catname">Make Money</span><span class="catsub">Income systems and ideas</span></span></a>
          <a class="catitem" href="../category.html?cat=Productivity"><span class="caticon">⚡</span><span class="cattext"><span class="catname">Productivity</span><span class="catsub">Focus and work systems</span></span></a>
          <a class="catitem" href="../category.html?cat=Software%20Reviews"><span class="caticon">🧰</span><span class="cattext"><span class="catname">Software Reviews</span><span class="catsub">Comparisons and buying decisions</span></span></a>
          <a class="catitem" href="../category.html?cat=Side%20Hustles"><span class="caticon">🚀</span><span class="cattext"><span class="catname">Side Hustles</span><span class="catsub">Extra income after work</span></span></a>
        </div>
      </div>
    </aside>
 
  </div>
</main>
 
<footer class="footer">
  <div class="container">
    <div>© 2026 {html_escape(SITE_NAME)}</div>
    <div class="footer-links">
      <a href="../privacy.html">Privacy</a>
      <a href="../about.html">About</a>
      <a href="../contact.html">Contact</a>
    </div>
  </div>
</footer>
 
<script src="../search.js?v={SEARCH_JS_VERSION}"></script>
 
</body>
</html>
""".strip()
 
 
# =========================================================
# Post index
# =========================================================
def load_posts_index() -> List[dict]:
    data = load_json(POSTS_JSON, [])
    posts = data if isinstance(data, list) else []
    normalized = [normalize_existing_post(p) for p in posts if isinstance(p, dict)]
    return normalized
 
 
def save_posts_index(posts: List[dict]) -> None:
    clean = [normalize_existing_post(p) for p in posts if isinstance(p, dict)]
    save_json(POSTS_JSON, clean)
 
 
def add_post_to_index(
    posts: List[dict],
    *,
    title: str,
    slug: str,
    category: str,
    description: str,
    image_paths: List[str],
    created_iso: str,
    keyword: str,
    cluster: str,
    post_type: str,
    pillar_slug: str,
    planning: Dict[str, Any],
) -> None:
    thumb = first_non_empty_image(image_paths)
    posts.insert(0, {
        "title": title,
        "slug": slug,
        "category": category,
        "description": description,
        "date": created_iso,
        "updated": created_iso,
        "thumbnail": thumb,
        "image": thumb,
        "url": f"posts/{slug}.html",
        "keyword": keyword,
        "cluster": cluster,
        "post_type": post_type,
        "pillar_slug": (pillar_slug or slug) if post_type == "pillar" else pillar_slug,
        "audience": planning.get("audience", ""),
        "problem": planning.get("problem", ""),
        "outcome": planning.get("outcome", ""),
        "angle": planning.get("angle", ""),
        "intent_type": planning.get("intent_type", ""),
        "search_intent_summary": planning.get("search_intent_summary", ""),
        "faq_questions": planning.get("faq_questions", []),
        "tldr_focus": planning.get("tldr_focus", []),
    })
 
 
# =========================================================
# Main
# =========================================================
def main() -> int:
    base_keywords = load_keywords()
    posts = load_posts_index()
    redirects = load_redirects()

    existing_slugs = set(p.get("slug") for p in posts if isinstance(p, dict))
    existing_titles = [p.get("title", "") for p in posts if isinstance(p, dict) and p.get("title")]

    keyword_pool, cluster_name, post_type, current_pillar_slug = build_keyword_pool(
        base_keywords,
        existing_titles,
        posts,
    )
    if not keyword_pool:
        log("MAIN", "No keyword pool available")
        return 0

    used_texts_raw = load_json(USED_TEXTS_JSON, {})
    used_texts = ensure_used_texts_schema(used_texts_raw)
    used_fps = set(used_texts.get("fingerprints") or [])

    made = 0
    tries = 0
    tried_keywords = set()

    while made < POSTS_PER_RUN and tries < MAX_KEYWORD_TRIES:
        tries += 1

        remaining_keywords = [k for k in keyword_pool if normalize_keyword(k) not in tried_keywords]
        if not remaining_keywords:
            log("MAIN", "Keyword pool exhausted for this run")
            break

        keyword = random.choice(remaining_keywords).strip()
        effective_cluster_name = cluster_name
        effective_category = pick_category(
            keyword=keyword,
            cluster_name=cluster_name,
            post_type=post_type,
        )

        tried_keywords.add(normalize_keyword(keyword))
        if not keyword:
            continue

        created_iso = now_utc_iso()
        log("MAIN", f"Selected keyword='{keyword}' cluster='{effective_cluster_name}' post_type='{post_type}'")

        data = None
        planning = {}

        try:
            log("PLAN", "Generating planning and article")

            cand, cand_planning = generate_deep_post(
                keyword=keyword,
                cluster_name=effective_cluster_name,
                post_type=post_type,
                avoid_titles=existing_titles,
            )

            cand = enforce_comparison_visuals(cand, keyword=keyword)

            if post_semantically_too_close(keyword, cand_planning, posts):
                log("DUP", f"Semantic overlap detected for keyword='{keyword}'")
                continue

            cand_title = cand["title"]

            if title_too_similar(cand_title, existing_titles, TITLE_SIM_THRESHOLD):
                log("DUP", f"Title too similar: '{cand_title}'")
                continue

            ok, reason = quality_check_post(
                cand,
                keyword=keyword,
                post_type=post_type,
            )
            if not ok:
                log("QUALITY", f"Post rejected: reason='{reason}'")
                continue

            fp = make_fingerprint(cand_title, cand["sections"], cand["tldr"], cand["faq"])
            if fp in used_fps:
                log("DUP", f"Fingerprint duplicate for keyword='{keyword}'")
                continue

            data = cand
            planning = cand_planning
            used_fps.add(fp)

        except Exception as e:
            import traceback
            log("GEN", f"Generation crashed for keyword='{keyword}': {e}")
            traceback.print_exc()
            continue

        if not data:
            log("MAIN", f"Rejected keyword='{keyword}' after single-pass generation")
            continue

        title = data["title"]
        description = data["description"] or planning.get("description") or short_desc(title)
        category = data["category"] or planning.get("category") or effective_category
        sections = data["sections"]
        tldr = data["tldr"]
        faq = data["faq"]
        editorial_note = data.get("editorial_note", "")

        if category not in ALLOWED_CATEGORIES:
            category = effective_category

        slug = build_clean_slug(title, keyword)
        if slug in existing_slugs:
            slug = f"{slug}-{int(time.time())}"

        old_slug = ""
        for p in posts[:100]:
            if isinstance(p, dict) and normalize_keyword(p.get("title", "")) == normalize_keyword(title):
                old_slug = (p.get("slug") or "").strip()
                break

        if old_slug and old_slug != slug:
            redirects[f"/posts/{old_slug}.html"] = f"/posts/{slug}.html"

        pillar_slug = current_pillar_slug
        if post_type == "pillar":
            pillar_slug = slug

        image_paths, alt_texts, credits_li = build_visual_assets(slug, sections)

        real_image_count = sum(1 for p in image_paths if isinstance(p, str) and p.strip())

        visible_image_count = 0
        for i, sec in enumerate(sections[:len(image_paths)]):
            path = image_paths[i] if i < len(image_paths) else ""
            if not path or not path.strip():
                continue
            if section_has_html_table(sec):
                continue
            visible_image_count += 1

        if real_image_count < MIN_REQUIRED_IMAGES:
            log(
                "IMG",
                f"Rejecting post slug='{slug}' because only {real_image_count} images were found, need at least {MIN_REQUIRED_IMAGES}"
            )
            continue

        if visible_image_count < VISIBLE_MIN_IMAGES:
            log(
                "IMG",
                f"Rejecting post slug='{slug}' because only {visible_image_count} images would be visible, need at least {VISIBLE_MIN_IMAGES}"
            )
            continue

        log(
            "IMG",
            f"slug='{slug}' total_sections={len(sections)} image_paths={len(image_paths)} non_empty={sum(1 for p in image_paths if p.strip())}"
        )

        related_posts = select_related_posts(
            posts,
            current_slug=slug,
            category=category,
            cluster=effective_cluster_name,
            pillar_slug=current_pillar_slug if post_type != "pillar" else "",
            limit=RELATED_POST_LIMIT,
        )

        html_out = render_post_html(
            title=title,
            description=description,
            category=category,
            updated_iso=created_iso[:19].replace("T", " "),
            slug=slug,
            image_paths=image_paths,
            alt_texts=alt_texts,
            sections=sections,
            tldr=tldr,
            faq=faq,
            photo_credits_li=credits_li,
            related_posts=related_posts,
            post_type=post_type,
            editorial_note=editorial_note,
            keyword=keyword,
        )

        html_path = POSTS_DIR / f"{slug}.html"
        safe_write(html_path, html_out)

        add_post_to_index(
            posts,
            title=title,
            slug=slug,
            category=category,
            description=description,
            image_paths=image_paths,
            created_iso=created_iso,
            keyword=keyword,
            cluster=effective_cluster_name,
            post_type=post_type,
            pillar_slug=pillar_slug,
            planning=planning,
        )
        existing_slugs.add(slug)
        existing_titles.insert(0, title)

        used_texts["fingerprints"] = sorted(list(used_fps))
        save_json(USED_TEXTS_JSON, used_texts)
        save_redirects(redirects)

        log("DONE", f"Generated HTML: posts/{slug}.html")
        log("DONE", f"Source keyword: {keyword}")
        log("DONE", f"Topic cluster: {effective_cluster_name}")
        log("DONE", f"Category: {category}")
        log("DONE", f"Post type: {post_type}")
        log("DONE", f"Audience: {planning.get('audience', '')}")
        log("DONE", f"Problem: {planning.get('problem', '')}")
        log("DONE", f"Angle: {planning.get('angle', '')}")
        made += 1

    if made == 0:
        log("MAIN", "No posts generated this run.")
        raise RuntimeError("No posts generated this run")

    save_posts_index(posts)
    log("MAIN", f"Finished build_id={BUILD_ID} made={made}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
