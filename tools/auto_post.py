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


def log(stage: str, message: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] [{stage}] {message}")
                              

def safe_json_loads(text: str, default=None):
    if default is None:
        default = {}

    if not text:
        return default

    text = text.strip()

    # fenced json 제거
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text, flags=re.IGNORECASE)
        text = re.sub(r"\s*```$", "", text)

    try:
        return json.loads(text)
    except Exception:
        pass

    try:
        start = text.find("{")
        end = text.rfind("}") + 1
        if start != -1 and end != -1 and end > start:
            return json.loads(text[start:end])
    except Exception:
        pass

    return default


UNSPLASH_SEARCH_CACHE: Dict[str, List[dict]] = {}
IMAGE_RESULT_CACHE: Dict[str, Optional[dict]] = {}
UNSPLASH_CALL_COUNT = 0
UNSPLASH_CALL_LIMIT = int(os.environ.get("UNSPLASH_CALL_LIMIT", "50"))
PEXELS_SEARCH_CACHE: Dict[str, List[dict]] = {}
PIXABAY_SEARCH_CACHE: Dict[str, List[dict]] = {}

IMAGE_QUERY_STOPWORDS = {
    "actually", "really", "very", "best", "guide", "tips", "tip",
    "how", "what", "why", "when", "where", "which",
    "beginner", "beginners", "starter", "starting",
    "strong", "finish", "finishing",
    "resources", "resource", "tools", "tool",
    "for", "to", "and", "or", "the", "a", "an", "of", "in", "on", "with",
    "your", "you", "from", "by", "at", "is", "are", "be", "this", "that",
    "can", "could", "should", "would", "will", "into", "about"
}

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
POSTS_PER_RUN = int(os.environ.get("POSTS_PER_RUN", "1"))
IMG_COUNT = int(os.environ.get("IMG_COUNT", "6"))
MIN_REQUIRED_IMAGES = int(os.environ.get("MIN_REQUIRED_IMAGES", "5"))
VISIBLE_MIN_IMAGES = int(os.environ.get("VISIBLE_MIN_IMAGES", "4"))
EXTRA_TABLE_BUFFER = int(os.environ.get("EXTRA_TABLE_BUFFER", "2"))
COLLECT_TARGET_IMAGES = int(
    os.environ.get("COLLECT_TARGET_IMAGES", str(max(MIN_REQUIRED_IMAGES + EXTRA_TABLE_BUFFER, VISIBLE_MIN_IMAGES + EXTRA_TABLE_BUFFER)))
)

print(
    f"[CONFIG] POSTS_PER_RUN={POSTS_PER_RUN} IMG_COUNT={IMG_COUNT} "
    f"MIN_REQUIRED_IMAGES={MIN_REQUIRED_IMAGES} VISIBLE_MIN_IMAGES={VISIBLE_MIN_IMAGES} "
    f"EXTRA_TABLE_BUFFER={EXTRA_TABLE_BUFFER} COLLECT_TARGET_IMAGES={COLLECT_TARGET_IMAGES}"
)
 
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "").strip()
MODEL_PLANNER = os.environ.get("MODEL_PLANNER", os.environ.get("MODEL", "gpt-4o-mini")).strip()
MODEL_WRITER = os.environ.get("MODEL_WRITER", os.environ.get("MODEL", "gpt-4o-mini")).strip() 
MIN_CHARS = int(os.environ.get("MIN_CHARS", "4200"))
MIN_SECTION_CHARS = int(os.environ.get("MIN_SECTION_CHARS", "220"))
MAX_SECTION_CHARS = int(os.environ.get("MAX_SECTION_CHARS", "980"))
MAX_KEYWORD_TRIES = int(os.environ.get("MAX_KEYWORD_TRIES", "18"))
MAX_CHARS = int(os.environ.get("MAX_CHARS", str(int(MIN_CHARS * 1.3))))

print(
    f"[CONFIG] MIN_CHARS={MIN_CHARS} MAX_CHARS={MAX_CHARS} "
    f"MIN_SECTION_CHARS={MIN_SECTION_CHARS} MAX_SECTION_CHARS={MAX_SECTION_CHARS}"
)
HTTP_TIMEOUT = int(os.environ.get("HTTP_TIMEOUT", "35"))
ADSENSE_CLIENT = os.environ.get("ADSENSE_CLIENT", "").strip()
GA_MEASUREMENT_ID = os.environ.get("GA_MEASUREMENT_ID", "").strip()

AUTHOR_NAME = os.environ.get("AUTHOR_NAME", "MingMong Editorial").strip()
AUTHOR_URL = os.environ.get("AUTHOR_URL", f"{SITE_URL}/about.html").strip()
SITE_TAGLINE = os.environ.get(
    "SITE_TAGLINE",
    "Sharp guides on AI tools, side hustles, software, productivity, and making extra money without fluff."
).strip()
 
TITLE_SIM_THRESHOLD = float(os.environ.get("TITLE_SIM_THRESHOLD", "0.83"))
KEYWORD_SIM_THRESHOLD = float(os.environ.get("KEYWORD_SIM_THRESHOLD", "0.74"))
TOPIC_SIM_THRESHOLD = float(os.environ.get("TOPIC_SIM_THRESHOLD", "0.70"))
MIN_KEYWORD_POOL = int(os.environ.get("MIN_KEYWORD_POOL", "18"))
 
GOOGLE_SUGGEST_ENABLED = os.environ.get("GOOGLE_SUGGEST_ENABLED", "1").strip() == "1"
GOOGLE_SUGGEST_MAX_SEEDS = int(os.environ.get("GOOGLE_SUGGEST_MAX_SEEDS", "4"))
GOOGLE_SUGGEST_PER_QUERY = int(os.environ.get("GOOGLE_SUGGEST_PER_QUERY", "4"))
GOOGLE_SUGGEST_SCORE_THRESHOLD = float(os.environ.get("GOOGLE_SUGGEST_SCORE_THRESHOLD", "-0.2"))
 
SERPAPI_KEY = os.environ.get("SERPAPI_KEY", "").strip()
SERPAPI_ENGINE = os.environ.get("SERPAPI_ENGINE", "google").strip()
SERP_CHECK_ENABLED = os.environ.get("SERP_CHECK_ENABLED", "1").strip() == "1"
SERP_CHECK_LIMIT = int(os.environ.get("SERP_CHECK_LIMIT", "10"))
 
UNSPLASH_ACCESS_KEY = os.environ.get("UNSPLASH_ACCESS_KEY", "").strip()
PEXELS_API_KEY = os.environ.get("PEXELS_API_KEY", "").strip()
PIXABAY_API_KEY = os.environ.get("PIXABAY_API_KEY", "").strip()
ENABLE_WIKIMEDIA = True

UNSPLASH_PER_PAGE = max(1, min(30, int(os.environ.get("UNSPLASH_PER_PAGE", "10"))))
PEXELS_PER_PAGE = max(1, min(80, int(os.environ.get("PEXELS_PER_PAGE", "10"))))
PIXABAY_PER_PAGE = max(3, min(200, int(os.environ.get("PIXABAY_PER_PAGE", "10"))))

log(
    "IMG",
    f"API keys loaded unsplash={bool(UNSPLASH_ACCESS_KEY)} "
    f"pexels={bool(PEXELS_API_KEY)} "
    f"pixabay={bool(PIXABAY_API_KEY)} "
    f"wikimedia={ENABLE_WIKIMEDIA}"
)

UNSPLASH_MIN_WIDTH = int(os.environ.get("UNSPLASH_MIN_WIDTH", "800"))
UNSPLASH_MIN_HEIGHT = int(os.environ.get("UNSPLASH_MIN_HEIGHT", "500"))
UNSPLASH_MIN_LIKES = int(os.environ.get("UNSPLASH_MIN_LIKES", "0"))

PEXELS_MIN_WIDTH = int(os.environ.get("PEXELS_MIN_WIDTH", "800"))
PEXELS_MIN_HEIGHT = int(os.environ.get("PEXELS_MIN_HEIGHT", "500"))

PIXABAY_MIN_WIDTH = int(os.environ.get("PIXABAY_MIN_WIDTH", "800"))
PIXABAY_MIN_HEIGHT = int(os.environ.get("PIXABAY_MIN_HEIGHT", "500"))
 
IMAGE_SOURCE_PRIORITY = [
    "unsplash",
    "pexels",
    "pixabay",
]

RELATED_POST_LIMIT = int(os.environ.get("RELATED_POST_LIMIT", "3"))
 
CLUSTER_MODE = os.environ.get("CLUSTER_MODE", "1").strip() == "1"
CLUSTER_BATCH = int(os.environ.get("CLUSTER_BATCH", "12"))
CLUSTER_ROTATION_WINDOW = int(os.environ.get("CLUSTER_ROTATION_WINDOW", "18"))
TOPIC_CLUSTERS_JSON = os.environ.get("TOPIC_CLUSTERS_JSON", "").strip()
PILLAR_INTERVAL = int(os.environ.get("PILLAR_INTERVAL", "6"))
 
SECTION_COUNT_MIN = int(os.environ.get("SECTION_COUNT_MIN", "6"))
SECTION_COUNT_MAX = int(os.environ.get("SECTION_COUNT_MAX", "8"))
 
SEARCH_JS_VERSION = hashlib.sha1(str(int(time.time() // 3600)).encode("utf-8")).hexdigest()[:8]
BUILD_ID = hashlib.sha1(f"{datetime.now(timezone.utc).isoformat()}-{random.random()}".encode("utf-8")).hexdigest()[:10]
 
 
# =========================================================
# Policy
# =========================================================
ALLOWED_CATEGORIES = {
    "AI Tools",
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
        "best ai writing tools for marketers who still need human sounding drafts",
        "chatgpt workflows that save real time for solo operators",
        "ai tools that help remote workers without adding another dashboard",
        "best ai tools for repurposing one piece of content into three formats",
        "cheap ai tools that quietly fail in real marketing workflows",
        "best ai note taking tools when you hate fixing bad summaries",
        "which ai writing tool is best for landing pages emails and briefs",
        "ai tools that save time without creating generic copy",
    ],
    "Make Money": [
        "what actually makes money online when you start with less than 100 dollars",
        "best digital products to sell when you have no audience",
        "how to make your first 100 dollars online without pretending it is passive",
        "low cost online business ideas that do not depend on posting every day",
        "what to sell if you are broke but have 5 hours a week",
        "how to earn extra income after work without becoming a freelancer",
        "best beginner income systems that survive after week two",
        "why most ai side hustles fail before the first sale",
    ],
    "Productivity": [
        "how to stop context switching when your day is broken into meetings",
        "weekly planning system for people who keep carrying work into the weekend",
        "best note taking system for busy professionals who never review their notes",
        "email management system for people who answer fast but still fall behind",
        "how to organize tasks across multiple projects without rebuilding your whole life",
        "focus system for remote workers who lose the afternoon every day",
        "meeting notes to task workflow that does not die after one week",
        "daily work reset checklist for overloaded knowledge workers",
    ],
    "Software Reviews": [
        "notion vs clickup for a one person business with too many moving parts",
        "best email marketing tools for beginners who hate complex automation",
        "best invoicing software for freelancers who send less than 20 invoices a month",
        "best crm for solo consultants who do not want sales team bloat",
        "best project management software for small teams that need less admin",
        "otter ai alternatives for meeting notes that still need cleanup",
        "best budget apps for young professionals who keep overspending after payday",
        "best ai note taking tools for messy real world meetings",
    ],
    "Side Hustles": [
        "7 side hustles remote workers can start with 100 dollars or less",
        "which side hustle actually fits a full time worker after 8 pm",
        "best side hustles for remote workers who hate client work",
        "low cost side hustle ideas that do not need an audience",
        "what fails first when beginners start a side hustle after work",
        "digital side hustles that can reach the first 500 dollars without inventory",
        "weekend side hustles that do not turn into a second full time job",
        "how to choose one side hustle instead of trying seven at once",
    ],
}
 
DEFAULT_PILLAR_TOPICS = {
    "AI Tools": [
        "how to choose ai tools that actually save time",
        "practical ai tools for work and side income",
        "how to build a useful ai stack for one person businesses",
        "ai tool buying guide for beginners who hate bloated software",
    ],
    "Make Money": [
        "how to build extra income without fake passive income promises",
        "practical online income ideas that survive the first 30 days",
        "how to create repeatable income streams when you are starting broke",
        "how to start earning extra money after work without burning out",
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
        "how to choose a side hustle that fits your schedule and actually pays",
        "practical side hustle ideas that are better than the usual beginner list",
        "how to start a side hustle while working full time without wasting 30 days",
        "side hustle framework for people who need money not motivation",
    ],
}
 
 
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

    def extract_balanced(text: str, opener: str, closer: str) -> Optional[str]:
        start_positions = [i for i, ch in enumerate(text) if ch == opener]

        for start in start_positions:
            depth = 0
            in_string = False
            escape = False

            for i in range(start, len(text)):
                ch = text[i]

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
                        candidate = text[start:i + 1]
                        try:
                            json.loads(candidate)
                            return candidate
                        except Exception:
                            break
        return None

    obj = extract_balanced(s, "{", "}")
    if obj:
        return obj

    arr = extract_balanced(s, "[", "]")
    if arr:
        return arr

    return s
 
 
def html_escape(s: str) -> str:
    return html.escape(s or "", quote=True)


def build_svg_placeholder(*args, **kwargs):
    return None

def score_query_match(query: str, text: str) -> float:
    q_words = set(normalize_keyword(query).split())
    t_words = set(normalize_keyword(text).split())

    if not q_words or not t_words:
        return 0.0

    inter = len(q_words & t_words)
    union = len(q_words | t_words)

    return inter / union if union else 0.0


def build_image_alt(slug: str, heading: str, query: str) -> str:
    text = (heading or query or slug or "article image").strip()
    text = re.sub(r"\s+", " ", text)
    return text[:140]


def filter_reusable_assets(results: List[dict], used_ids: set) -> List[dict]:
    out = []

    for item in results:
        asset_id = str(item.get("id") or "").strip()
        source = str(item.get("source") or "").strip().lower()
        width = int(item.get("width") or 0)
        height = int(item.get("height") or 0)

        image_url = (
            str(item.get("hotlink_url") or "").strip()
            or str(item.get("download_url") or "").strip()
        )

        if not asset_id:
            continue

        if asset_id in used_ids:
            continue

        if not image_url:
            continue

        if source == "unsplash":
            if width < UNSPLASH_MIN_WIDTH or height < UNSPLASH_MIN_HEIGHT:
                continue

        elif source == "pexels":
            if width < PEXELS_MIN_WIDTH or height < PEXELS_MIN_HEIGHT:
                continue

        elif source == "pixabay":
            if width < PIXABAY_MIN_WIDTH or height < PIXABAY_MIN_HEIGHT:
                continue

        out.append(item)

    return out


def pick_best_asset(filtered: List[dict], heading: str = "", visual_type: str = "") -> Optional[dict]:
    if not filtered:
        return None

    ranked = sorted(
        filtered,
        key=lambda x: (
            float(x.get("score") or 0.0),
            int(x.get("width") or 0) * int(x.get("height") or 0),
        ),
        reverse=True,
    )

    return ranked[0]


def render_image_block(img_src: Optional[str], alt_text: str = "", caption: str = "", class_name: str = "post-image") -> str:
    if not img_src:
        return ""

    alt_escaped = html_escape(alt_text)
    caption_html = f"<figcaption>{html_escape(caption)}</figcaption>" if caption else ""

    return f'''
<figure class="{class_name}">
  <img src="{img_src}" alt="{alt_escaped}" loading="lazy">
  {caption_html}
</figure>
'''.strip()
 
 
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


def title_has_click_tension(title: str) -> bool:
    t = normalize_keyword(title)
    patterns = [
        "most ", "what actually", "why ", "vs ", "versus ", "before you ",
        "under ", "without ", "after ", "mistake", "fails", "worth it",
        "bad fit", "waste", "only", "unless", "instead of"
    ]
    return any(p in t for p in patterns) or bool(re.search(r"\b\d+\b", t))


def title_is_generic(title: str) -> bool:
    t = normalize_keyword(title)
    generic_patterns = [
        "best tools for", "guide to", "how to succeed", "complete guide",
        "comprehensive guide", "top ways to", "essential tips", "strategies for",
        "tips for", "ways to improve"
    ]
    if any(p in t for p in generic_patterns):
        return True
    if len(t.split()) < 6:
        return True
    return False


def sanitize_title_for_ctr(title: str, keyword: str) -> str:
    t = _clean_text(title).strip("-: ")
    if not t:
        t = keyword.title()
    t = re.sub(r"\s+", " ", t).strip()
    if title_is_generic(t):
        base = keyword.strip().rstrip("?")
        options = [
            f"What Actually Works for {base.title()}",
            f"{base.title()}: The Good Fit, Bad Fit, and Hidden Cost",
            f"Before You Choose {base.title()}, Read This",
        ]
        t = random.choice(options)
    if len(t) > 72:
        t = t[:72].rsplit(" ",1)[0].rstrip(" -:,")
    return t

def make_click_title(keyword: str, ai_title: str = "") -> str:
    keyword = _clean_text(keyword)
    ai_title = _clean_text(ai_title)

    base = ai_title or keyword.title()

    patterns = [
        f"{base}: What Actually Works and What Fails First",
        f"Before You Choose {base}, Read This",
        f"{base}: The Good Fit, Bad Fit, and Hidden Cost",
        f"{base}: What Most People Get Wrong",
        f"{base}: Which Option Actually Makes Sense?",
    ]

    title = random.choice(patterns)

    if len(title) > 72:
        title = title[:72].rsplit(" ", 1)[0].rstrip(" -:,")

    return title


def title_score(title: str) -> int:
    score = 0
    t = normalize_keyword(title)
    if title_has_click_tension(title):
        score += 2
    if re.search(r"\b\d+\b", t):
        score += 1
    if any(x in t for x in ["under", "without", "after", "before", "instead of", "vs", "worth it"]):
        score += 1
    if len(t.split()) <= 12:
        score += 1
    if title_is_generic(title):
        score -= 3
    return score
 
 
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




def looks_like_model_refusal(text: str) -> bool:
    t = (text or "").strip().lower()
    if not t:
        return False
    refusal_markers = [
        "i'm sorry",
        "i am sorry",
        "i can’t assist",
        "i can't assist",
        "i cannot assist",
        "i can’t provide",
        "i can't provide",
        "unable to provide",
        "cannot provide the content",
        "can't provide the content",
        "not provide the content in the format requested",
    ]
    return any(m in t for m in refusal_markers)


def make_fingerprint(title: str, sections: List[Dict[str, Any]], tldr: str = "", faq: List[Dict[str, str]] = None) -> str:
    faq = faq or []
    section_chunks = []
    for sec in sections[:6]:
        if not isinstance(sec, dict):
            continue
        heading = normalize_keyword(sec.get("heading", ""))
        body = normalize_keyword((sec.get("body", "") or "")[:320])
        section_chunks.append(f"{heading}::{body}")
    faq_chunk = "|".join(
        normalize_keyword((item.get("q", "") + " " + item.get("a", ""))[:160])
        for item in faq[:4] if isinstance(item, dict)
    )
    base = " || ".join([
        normalize_keyword(title),
        normalize_keyword(tldr),
        " || ".join(section_chunks),
        faq_chunk,
    ])
    return hashlib.sha1(base.encode("utf-8")).hexdigest()


def post_semantically_too_close(keyword: str, cand_planning: Dict[str, Any], posts: List[Dict[str, Any]]) -> bool:
    new_title = cand_planning.get("title", "") or ""
    new_combo = " ".join([
        keyword or "",
        new_title,
        cand_planning.get("angle", "") or "",
        cand_planning.get("problem", "") or "",
        cand_planning.get("outcome", "") or "",
    ]).strip()
    if not new_combo:
        return False

    for post in posts[:180]:
        if not isinstance(post, dict):
            continue
        old_combo = " ".join([
            post.get("keyword", "") or "",
            post.get("title", "") or "",
            post.get("angle", "") or "",
            post.get("problem", "") or "",
            post.get("outcome", "") or "",
        ]).strip()
        if not old_combo:
            continue
        if semantic_overlap_score(new_combo, old_combo) >= TOPIC_SIM_THRESHOLD:
            return True
        if keyword_too_similar(keyword, post.get("keyword", ""), KEYWORD_SIM_THRESHOLD):
            return True
    return False


def has_real_scenario_section(sections: List[Dict[str, Any]]) -> bool:
    signals = [
        "for example", "imagine", "scenario", "let's say", "month 1", "week 1",
        "after 30 days", "after 90 days", "$", "hours", "budget", "portfolio",
        "if you invest", "if you buy", "if you only have", "case:",
    ]
    for sec in sections:
        if not isinstance(sec, dict):
            continue
        body = (sec.get("body") or "").lower()
        if sum(1 for s in signals if s in body) >= 2:
            return True
    return False


def build_real_scenario_section(keyword: str, category: str) -> Dict[str, str]:
    if category == "Investing":
        heading = "A realistic first 12 months with only $100 a month"
        body = (
            "Assume you start with $100 a month and keep the plan painfully simple. In month 1 you open the account, "
            "pick one broad low-cost fund, and set an automatic contribution date right after payday. The first risk is not "
            "market volatility. It is breaking the habit after one red week or one unexpected expense. By month 3 the real "
            "test is whether you still invest when the balance looks too small to feel exciting. By month 6 the portfolio "
            "may still look underwhelming, but that is normal. What matters is that your process now survives boring months, "
            "small pullbacks, and the temptation to chase whatever just went up on social media. By month 12 the win is not "
            "that you became rich. The win is that you built a repeatable system that can scale from $100 to $300 without "
            "changing the core rules."
        )
        image_query = "monthly investing spreadsheet budget"
    else:
        heading = "A realistic scenario before you commit"
        body = (
            "Run the idea through one real month instead of a fantasy version. Count the hours you can actually protect, the "
            "money you can risk, the tools you already have, and the first thing that breaks when life gets noisy. A useful "
            "plan survives interruptions, not just motivation."
        )
        image_query = "workflow planning spreadsheet"
    return {
        "heading": heading,
        "body": format_generated_body(body),
        "image_query": image_query,
        "visual_type": "workspace",
        "alt_text": heading,
    }


def quality_check_post(data: Dict[str, Any], keyword: str, post_type: str = "normal") -> Tuple[bool, str]:
    sections = data.get("sections") or []
    if not isinstance(sections, list) or len(sections) < 6:
        return False, "not enough sections"

    total_body_len = sum(len((s.get("body") or "").strip()) for s in sections if isinstance(s, dict))
    min_total = max(4800, int(MIN_CHARS * 0.78))
    if total_body_len < min_total:
        return False, "body length below target"

    headings = [normalize_keyword(s.get("heading", "")) for s in sections if isinstance(s, dict)]
    if len(set([h for h in headings if h])) < 5:
        return False, "section headings too repetitive"

    short_sections = 0
    for idx, sec in enumerate(sections[:6]):
        body_len = len((sec.get("body") or "").strip())
        min_len = 320 if idx in {0, 5} else 450
        if body_len < min_len:
            short_sections += 1
    if short_sections >= 2:
        return False, "too many thin sections"

    body_joined = "\n\n".join((s.get("body") or "") for s in sections if isinstance(s, dict)).lower()
    banned = [
        "in today's fast-paced world",
        "this article will explore",
        "there are many options available",
        "boost productivity",
        "streamline your workflow",
    ]
    if sum(1 for b in banned if b in body_joined) >= 2:
        return False, "generic filler detected"

    faq = data.get("faq") or []
    if len([x for x in faq if isinstance(x, dict) and x.get("q") and x.get("a")]) < 3:
        return False, "faq too thin"

    tldr = (data.get("tldr") or "").strip()
    if len(tldr) < 120:
        return False, "tldr too thin"

    if data.get("category") == "Investing":
        investing_signals = ["expense ratio", "broad", "diversified", "automatic", "time horizon", "allocation"]
        if sum(1 for s in investing_signals if s in body_joined) < 2:
            return False, "investing article lacks practical guardrails"

    return True, "ok"

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
    if len(words) < 3 or len(words) > 18:
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
        "script",
        "email example",
        "pricing",
        "free plan",
        "client retention",
        "repeat clients",
        "note taking",
        "focus system",
        "remote work",
        "project management",
        "digital side hustle",
        "ai tools",
        "productivity tools",
    ]

    if len(words) >= 5:
        return True

    if any(tok in k for tok in intent_tokens):
        return True

    return False


def dedupe_keywords(keywords: List[str], existing_titles: List[str], existing_keywords: List[str]) -> List[str]:
    out: List[str] = []
    seen_norm = set()

    total_in = len(keywords)
    filtered_by_intent = 0
    filtered_by_seen = 0
    filtered_by_baseline = 0
    filtered_by_out = 0

    baseline = []
    for x in existing_titles[:800]:
        if isinstance(x, str) and x.strip():
            baseline.append(x)
    for x in existing_keywords[:1500]:
        if isinstance(x, str) and x.strip():
            baseline.append(x)

    for kw in keywords:
        kw = _clean_text(kw)
        if not kw:
            continue

        if not is_search_intent_keyword(kw):
            filtered_by_intent += 1
            continue

        n = normalize_keyword(kw)
        if not n or n in seen_norm:
            filtered_by_seen += 1
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
            filtered_by_baseline += 1
            continue

        for kept in out:
            if keyword_too_similar(kw, kept):
                skip = True
                break
            if semantic_overlap_score(kw, kept) >= TOPIC_SIM_THRESHOLD:
                skip = True
                break
        if skip:
            filtered_by_out += 1
            continue

        seen_norm.add(n)
        out.append(kw)

    log(
        "KW",
        f"dedupe_keywords in={total_in} out={len(out)} "
        f"intent_drop={filtered_by_intent} seen_drop={filtered_by_seen} "
        f"baseline_drop={filtered_by_baseline} out_drop={filtered_by_out}"
    )
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
    last_seen_index = {name: 10**9 for name in names}

    for idx, p in enumerate(recent):
        if not isinstance(p, dict):
            continue

        cluster = str(p.get("cluster") or "").strip()
        if cluster in counts:
            counts[cluster] += 1
            if last_seen_index[cluster] == 10**9:
                last_seen_index[cluster] = idx

    ranked = sorted(
        names,
        key=lambda name: (
            counts[name],
            last_seen_index[name],
            name,
        )
    )

    return ranked[0]
 
 
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


def detect_category_from_keyword(keyword: str) -> str:
    return cluster_to_category("", keyword, "")
  
 
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
    title_block = "\n".join([f"- {x}" for x in existing_titles[:80]])
    existing_kw_block = "\n".join([f"- {x}" for x in existing_keywords[:140]])

    return f"""
You generate elite editorial keyword ideas for a practical blog targeting US and EU readers.

Current cluster:
{cluster_name}

Your mission:
Generate topics that can create articles people actually finish reading.
The blog does not want polite filler, vague explainers, recycled listicles, or soft corporate content.
It wants sharp topics built around hard decisions, real constraints, realistic tradeoffs, and honest outcomes.

Output target:
- exactly {CLUSTER_BATCH} keyword ideas
- long-tail only
- each keyword must sound publishable, clickable, and narrow enough to avoid generic writing

Non-negotiable rules:
- no broad beginner head terms
- no motivational topics
- no vague "complete guide" style ideas
- no celebrity, politics, medical, legal, or news topics
- no year-stamped trends
- no duplicate problem phrased with small wording changes
- no keyword that would naturally produce a safe or generic article

Every keyword must include at least 2 of these:
- a specific audience
- a hard constraint such as budget, time, workload, skill level, client count, traffic level, or stage
- a trigger moment
- a measurable outcome
- a common mistake, loss, or failure risk

Strong patterns:
- which X actually works for Y under Z constraint
- X vs Y when a specific limit is true
- what fails first when beginners try X
- when free X stops being enough
- how much X realistically costs, earns, or saves in a narrow scenario
- 30 day or weekly system for a specific user and a specific outcome
- what not to choose if your situation is X

Weak patterns to reject:
- how professionals can improve productivity
- best tools for everyone
- effective strategies for growth
- complete guide to X
- tips for success
- how to succeed with X
- any topic that sounds like generic SaaS content
- any topic that sounds too polite or too safe

Editorial standards:
- prefer topics that force a decision
- prefer topics that include risk, friction, cost, or hidden downside
- prefer topics that naturally support numbers, scenarios, and tradeoffs
- prefer topics that make a reader think this is exactly my problem
- prefer topics that let the article say some options are bad fits
- avoid topics that merely summarize options without judgment

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
    data = safe_json_loads(_find_balanced_json(raw), {})
 
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
    title_block = "\n".join([f"- {x}" for x in existing_titles[:70]])
    existing_kw_block = "\n".join([f"- {x}" for x in existing_keywords[:140]])

    return f"""
You generate elite editorial keyword ideas for a practical niche blog.

Site focus:
1. AI tools for work and income
2. practical make money strategies
3. productivity systems
4. software reviews and comparisons
5. side hustles for ordinary workers

Goal:
Generate topics that feel more specific, more honest, and more useful than typical SEO blog topics.
The final article should feel like a smart operator telling the reader what actually works, what fails, and what to choose next.

Hard rules:
- exactly 14 keyword ideas
- long-tail only
- no broad definitions
- no motivational or inspirational topics
- no vague guide language
- no celebrity, news, political, medical, legal, or unsafe topics
- no duplicate underlying topics with slight wording changes
- no topics that would naturally produce filler

Each keyword must contain at least 2 of the following:
- audience
- constraint
- trigger moment
- measurable outcome
- failure risk
- comparison or buying decision

Prefer keyword shapes like:
- which X actually works for Y when Z is true
- X vs Y for a very specific user
- when free X becomes a bad fit
- what beginners get wrong about X
- how much X realistically earns, costs, or saves
- weekly system for X when the reader has a real limit
- best X under a specific budget, workload, or business stage
- what to avoid when starting X

Avoid keyword shapes like:
- how professionals can improve productivity
- effective strategies for X
- complete guide to X
- how to succeed with X
- best tools for everyone
- broad educational explainers with no decision angle

Editorial standards:
- each topic should support a strong opinion or clear recommendation
- each topic should support concrete numbers, thresholds, or scenarios
- each topic should give the writer room to say some choices are bad
- each topic should be narrow enough that the title does not sound like a copy of 50 other blogs

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
 
 
def build_keyword_pool(
    base_keywords: List[str],
    existing_titles: List[str],
    posts: List[dict],
) -> Tuple[List[str], str, str, str]:
    existing_keywords = get_existing_keywords_from_posts(posts)
    clean_base = dedupe_keywords(base_keywords, existing_titles, existing_keywords)

    if CLUSTER_MODE:
        cluster_name = "General"
        current_pillar_slug = ""

        try:
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
                    log("KW", f"Using pillar pool cluster='{cluster_name}' size={len(pillar_pool)}")
                    return pillar_pool, cluster_name, "pillar", current_pillar_slug

            seeds = topic_clusters.get(cluster_name) or []
            target_category = cluster_to_category(cluster_name)

            cluster_base = [
                kw for kw in clean_base
                if detect_category_from_keyword(kw) == target_category
            ]

            # category detect가 빡세서 아무 것도 안 남는 경우 대비
            if not cluster_base:
                cluster_base = clean_base[:]

            merged_seed = cluster_base + seeds
            merged_seed = [x for x in merged_seed if isinstance(x, str) and x.strip()]

            strict_cluster_terms = set(normalize_keyword(" ".join(seeds)).split())
            soft_cluster_terms = {
                tok for tok in strict_cluster_terms
                if len(tok) >= 4 and tok not in {
                    "best", "guide", "tools", "tool", "apps", "app",
                    "software", "system", "review", "reviews",
                    "beginners", "beginner"
                }
            }

            def is_cluster_relevant(kw: str) -> bool:
                nkw = normalize_keyword(kw)
                kw_tokens = set(nkw.split())
                detected = detect_category_from_keyword(kw)

                # 1) 카테고리 판별이 다르더라도 완전 즉시 탈락하지 않음
                category_match = (detected == target_category)

                # 2) 카테고리별 금지어
                if target_category == "AI Tools":
                    banned = {"stock", "stocks", "etf", "etfs", "portfolio", "dividend", "investing"}
                    if kw_tokens & banned:
                        return False

                elif target_category == "Investing":
                    banned_terms = {"chatgpt", "prompt", "prompts", "crm", "invoicing"}
                    banned_phrases = ["ai writing", "meeting notes", "note taking"]
                    if kw_tokens & banned_terms:
                        return False
                    if any(x in nkw for x in banned_phrases):
                        return False

                elif target_category == "Software Reviews":
                    soft_must_have = {
                        "software", "tool", "tools", "app", "apps", "platform",
                        "crm", "invoicing", "notion", "clickup", "review",
                        "reviews", "vs", "compare", "comparison",
                        "alternative", "alternatives", "pricing", "free plan"
                    }
                    if not any(tok in nkw for tok in soft_must_have):
                        return False

                elif target_category == "Make Money":
                    banned = {"etf", "etfs", "stocks", "dividend", "portfolio"}
                    if kw_tokens & banned:
                        return False

                elif target_category == "Productivity":
                    banned = {"etf", "stocks", "dividend", "portfolio", "investing"}
                    if kw_tokens & banned:
                        return False

                elif target_category == "Side Hustles":
                    banned = {"etf", "stocks", "dividend", "portfolio"}
                    if kw_tokens & banned:
                        return False

                # 3) seed overlap은 hard가 아니라 soft 체크
                overlap = len(kw_tokens & soft_cluster_terms) if soft_cluster_terms else 0

                # category 일치면 웬만하면 통과
                if category_match:
                    return True

                # category 불일치여도 seed 단어 겹치면 통과
                if overlap >= 1:
                    return True

                # 일부 카테고리는 표현 다양성이 커서 널널하게 허용
                if target_category in {"Make Money", "Productivity", "Side Hustles"}:
                    broad_signals = {
                        "make money", "extra income", "side hustle", "workflow",
                        "focus", "planning", "remote work", "task management",
                        "project management", "productivity", "automation"
                    }
                    if any(sig in nkw for sig in broad_signals):
                        return True

                return False

            merged_all = dedupe_keywords(merged_seed, existing_titles, existing_keywords)
            merged_all = filter_keywords_by_opportunity(merged_all, existing_titles)
            merged_all = [kw for kw in merged_all if is_cluster_relevant(kw)]

            log(
                "KW",
                f"cluster='{cluster_name}' category='{target_category}' "
                f"seeds={len(seeds)} clean_base={len(clean_base)} "
                f"cluster_base={len(cluster_base)} merged_seed={len(merged_seed)} "
                f"merged_all={len(merged_all)}"
            )

            if merged_all:
                save_keywords(merged_all)
                return merged_all, cluster_name, "normal", current_pillar_slug

            # fallback 1: category 필터 완화
            fallback = dedupe_keywords(seeds + clean_base, existing_titles, existing_keywords)
            fallback = filter_keywords_by_opportunity(fallback, existing_titles)
            fallback = [kw for kw in fallback if is_cluster_relevant(kw) or detect_category_from_keyword(kw) == target_category]

            log("KW", f"fallback1 cluster='{cluster_name}' size={len(fallback)}")

            if fallback:
                save_keywords(fallback)
                return fallback, cluster_name, "normal", current_pillar_slug

            # fallback 2: seed 우선 사용
            seed_only = dedupe_keywords(seeds, existing_titles, existing_keywords)
            seed_only = filter_keywords_by_opportunity(seed_only, existing_titles)

            log("KW", f"fallback2 seed_only cluster='{cluster_name}' size={len(seed_only)}")

            if seed_only:
                save_keywords(seed_only)
                return seed_only, cluster_name, "normal", current_pillar_slug

        except Exception as e:
            log("KW", f"Cluster keyword generation failed: {e}")

    auto_keywords: List[str] = []
    if len(clean_base) < MIN_KEYWORD_POOL:
        try:
            auto_keywords = generate_auto_keywords(
                clean_base or base_keywords,
                existing_titles,
                existing_keywords,
            )
            google_keywords = expand_keywords_from_google(
                clean_base or base_keywords,
                existing_titles,
                existing_keywords,
            )

            merged = dedupe_keywords(
                clean_base + auto_keywords + google_keywords,
                existing_titles,
                existing_keywords,
            )
            merged = filter_keywords_by_opportunity(merged, existing_titles)

            log(
                "KW",
                f"general auto clean_base={len(clean_base)} "
                f"auto={len(auto_keywords)} google={len(google_keywords)} merged={len(merged)}"
            )

            if merged:
                save_keywords(merged)
                return merged, "General", "normal", ""

        except Exception as e:
            log("KW", f"Auto keyword generation failed: {e}")

    clean_base = filter_keywords_by_opportunity(clean_base, existing_titles)
    log("KW", f"final fallback clean_base={len(clean_base)}")
    return clean_base, "General", "normal", ""


def build_run_plan(base_keywords: List[str], existing_titles: List[str], posts: List[dict]) -> List[Dict[str, str]]:
    topic_clusters = load_topic_clusters()
    existing_keywords = get_existing_keywords_from_posts(posts)
    clean_base = dedupe_keywords(base_keywords, existing_titles, existing_keywords)

    category_order = [
        "AI Tools",
        "Investing",
        "Make Money",
        "Productivity",
        "Software Reviews",
        "Side Hustles",
    ]

    run_plan: List[Dict[str, str]] = []

    for category_name in category_order:
        cluster_name = category_name
        current_pillar = get_cluster_pillar(posts, cluster_name)
        current_pillar_slug = (current_pillar.get("slug") or "").strip()

        pillar_mode = should_make_pillar(posts, cluster_name)
        post_type = "pillar" if pillar_mode else "normal"

        if pillar_mode:
            keyword_pool = build_pillar_keyword_pool(cluster_name, posts, existing_titles)
        else:
            seeds = topic_clusters.get(cluster_name) or []

            cluster_base = [
                kw for kw in clean_base
                if detect_category_from_keyword(kw) == category_name
            ]

            merged_seed = cluster_base + seeds
            merged_seed = [x for x in merged_seed if isinstance(x, str) and x.strip()]

            keyword_pool: List[str] = []

            try:
                cluster_keywords = generate_cluster_keywords(
                    cluster_name=cluster_name,
                    seed_keywords=merged_seed,
                    existing_titles=existing_titles,
                    existing_keywords=existing_keywords,
                )
                google_keywords = expand_keywords_from_google(
                    merged_seed,
                    existing_titles,
                    existing_keywords,
                )
                merged_all = dedupe_keywords(
                    cluster_base + seeds + cluster_keywords + google_keywords,
                    existing_titles,
                    existing_keywords,
                )
                merged_all = filter_keywords_by_opportunity(merged_all, existing_titles)

                keyword_pool = [
                    kw for kw in merged_all
                    if detect_category_from_keyword(kw) == category_name
                ]

            except Exception as e:
                log("KW", f"Run-plan keyword generation failed for cluster='{cluster_name}': {e}")

            if not keyword_pool:
                fallback = dedupe_keywords(seeds + cluster_base, existing_titles, existing_keywords)
                fallback = filter_keywords_by_opportunity(fallback, existing_titles)
                keyword_pool = [
                    kw for kw in fallback
                    if detect_category_from_keyword(kw) == category_name
                ]

        if not keyword_pool:
            log("PLAN", f"No keyword pool for category='{category_name}'")
            continue

        chosen_keyword = random.choice(keyword_pool[:min(5, len(keyword_pool))]).strip()
        if not chosen_keyword:
            continue

        run_plan.append({
            "keyword": chosen_keyword,
            "cluster_name": cluster_name,
            "post_type": post_type,
            "pillar_slug": current_pillar_slug,
            "category": category_name,
        })

        existing_titles.insert(0, chosen_keyword)

    return run_plan[:POSTS_PER_RUN]
 
# =========================================================
# Strategy and article generation
# =========================================================
def build_cluster_guardrails(cluster_name: str) -> str:
    c = (cluster_name or "").strip()

    if c == "AI Tools":
        return """
- The article must stay focused on practical AI tool usage, prompt workflows, automation, integrations, or tool selection
- Do not turn the topic into stock investing, ETF selection, portfolio allocation, trading, or valuation
- Do not frame AI mainly as an investing theme
- Focus on how people use AI tools in real work
""".strip()

    if c == "Investing":
        return """
- The article must stay focused on investing decisions, ETFs, stocks, allocation, risk, and portfolio construction
- Do not drift into general productivity advice or generic AI workflow content
""".strip()

    if c == "Productivity":
        return """
- Focus on planning, focus, task execution, meeting workflow, organization, or time management
- Do not turn the article into investing or stock selection content
""".strip()

    if c == "Software Reviews":
        return """
- Focus on comparing real software tools or platforms
- Do not drift into investing advice unless the software itself is specifically an investing product
""".strip()

    if c == "Make Money":
        return """
- Focus on income generation, monetization, services, digital products, or repeatable income systems
- Do not turn the article into ETF or stock investing content
""".strip()

    if c == "Side Hustles":
        return """
- Focus on practical side hustle models and execution after work
- Do not turn the article into long term portfolio investing content
""".strip()

    return ""


def build_planning_prompt(keyword: str, avoid_titles: List[str], cluster_name: str, post_type: str) -> str:
    avoid_block = "\n".join([f"- {x}" for x in avoid_titles[:60]]) if avoid_titles else "- none"
    category_hint = pick_category(keyword=keyword, cluster_name=cluster_name, post_type=post_type)
    intent_type = infer_search_intent_type(keyword, category_hint)
    cluster_guardrails = build_cluster_guardrails(cluster_name)

    return f"""
You are an elite editorial strategist for a practical niche blog.
Your job is to design an article plan that feels sharp, opinionated, concrete, and impossible to confuse with generic SEO filler.

Keyword:
{keyword}

Cluster:
{cluster_name}

Post type:
{post_type}

Detected category:
{category_hint}

Detected intent type:
{intent_type}

Avoid titles too similar to:
{avoid_block}

Cluster-specific guardrails:
{cluster_guardrails}

Core objective:
Create a plan for an article that makes a reader stop scrolling because it sounds more honest and more specific than normal blog content.
The article must be built around a real user in a real moment making a real decision under a real constraint.

This site needs:
- stronger titles
- higher dwell time
- clearer decision logic
- realistic scenarios
- sharper opinions
- permission to say some choices are bad fits

Do not produce:
- polite generic intros
- textbook section headings
- soft motivational framing
- broad "how to succeed" content
- obvious listicles with no point of view
- articles where every option sounds equally good
- sections that simply define or summarize
- listicles that pad weak ideas just to hit a number

Title rules:
- the title must sound like a real search query or a strong editorial headline tied to search intent
- the title should usually be 48 to 68 characters
- the title must include at least one of:
  specific audience
  hard constraint
  measurable result
  trigger moment
  failure risk
  strong comparison
- the title should make the reader feel there is something at stake
- the title may be slightly provocative or contrarian if it still feels useful and credible
- do not use fake first person claims such as I tried, I tested, or I made unless the article clearly frames them as hypothetical or aggregated observations
- avoid abstract words like strategy, blueprint, roadmap, framework, journey, success formula
- avoid titles that could fit dozens of sites
- avoid safe magazine language like complete guide, top tips, ultimate list, or essential ways

Angle rules:
- the plan must include a clear point of view
- the article should not just list options, it should rank, eliminate, or redirect choices
- the article must include at least one section explaining why common advice fails
- the article must include at least one section saying when not to do this
- the article must include at least one section showing what breaks first or what people underestimate
- the article must give the reader a decision, not just information

Depth rules:
- every section must require concrete detail, not generic explanation
- at least 4 sections must include one or more of:
  money amount
  pricing band
  time estimate
  count threshold
  percentage
  workload condition
  budget condition
  timeline
- at least 2 sections must contain a realistic scenario with sequence and consequence
- if the topic is software, include upgrade point, switching friction, hidden cost, best-fit user, and bad-fit user
- if the topic is investing, include monthly amount, allocation range, review cadence, fee drag, and realistic beginner behavior
- if the topic is make money or side hustles, include setup effort, income timing, what fails first, why most beginners stall, and which options should be rejected early
- if the topic is productivity, include actual workload conditions, review cycle, tradeoffs, and what the system does not fix
- if the keyword implies a count such as 5, 7, or 10, the plan must include exactly that many items in one ranked list section and the items must be numbered continuously

Section rules:
- plan exactly 6 sections
- no filler intro and no filler conclusion
- section 1 must identify the reader, the trigger moment, and what is at stake
- section 2 must explain why default advice fails
- middle sections must carry the real comparison, system, or decision logic
- one section must contain a realistic scenario or mini case
- one section must contain mistakes, edge cases, or failure modes
- final section must end with a hard choice, decision checklist, or recommended next move
- section headings must be concrete and curiosity-producing, not generic or academic

Image rules:
- image_query must be specific and editorial
- avoid generic concepts like success, productivity concept, business growth, or office teamwork
- visual ideas should feel grounded in the section

Return valid JSON only with this schema:
{{
  "title": "specific title",
  "description": "155-170 chars meta description not equal to title",
  "category": "AI Tools|Make Money|Productivity|Software Reviews|Side Hustles",
  "intent": "pillar|cluster",
  "intent_type": "comparison|template|review|howto",
  "search_intent_summary": "one sentence about what the reader actually wants",
  "audience": "narrow audience description",
  "problem": "specific pain point",
  "outcome": "specific transformation or result",
  "angle": "why this article is sharper than generic advice",
  "section_plan": [
    {{
      "heading": "specific heading",
      "section_role": "problem|insight|solution|example|decision|checklist|ending",
      "goal": "what this section must achieve",
      "image_query": "2-8 words concrete visual idea",
      "visual_type": "photo|diagram|workspace",
      "must_include": ["point 1", "point 2", "point 3"],
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

Extra hard rules:
- every section goal must be rich enough to support at least 450 to 900 characters of body text
- at least 2 section headings should create curiosity or tension
- at least 2 sections must naturally support internal links to related comparisons, alternatives, or next-step guides
- at least 2 sections must include a cost, time, count, percentage, or threshold
- the plan must be opinionated enough that a generic content writer would struggle to produce it without thinking
- the final section must force the reader to choose one next move, not admire a list
""".strip()


def parse_planning_json(text: str, keyword: str, cluster_name: str, post_type: str) -> Dict[str, Any]:
    clean_raw = _find_balanced_json(text)
    data = safe_json_loads(clean_raw, {})

    if not isinstance(data, dict) or not data:
        raise ValueError("planning JSON parse failed")

    audience = _clean_text(data.get("audience", ""))
    problem = _clean_text(data.get("problem", ""))
    outcome = _clean_text(data.get("outcome", ""))
    angle = _clean_text(data.get("angle", ""))
    raw_ai_title = _clean_text(data.get("title", ""))
    title = make_click_title(keyword, sanitize_title_for_ctr(raw_ai_title, keyword))
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
    if len(section_plan) != 6:
        raise ValueError("section_plan must be exactly 6")

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

    comparison_markers = [
        " vs ", "versus", "compare", "comparison",
        "alternative", "alternatives"
    ]
    template_markers = [
        "template", "checklist", "script", "email example"
    ]
    review_markers = [
        "best ", "review", "reviews", "worth it",
        "pricing", "free plan", "upgrade", "should i switch",
        "when should", "which tool", "which software"
    ]
    howto_markers = [
        "how to", "system", "workflow", "process", "setup"
    ]

    if any(x in k for x in comparison_markers):
        return "comparison"

    if any(x in k for x in template_markers):
        return "template"

    if c == "software reviews":
        if any(x in k for x in comparison_markers):
            return "comparison"
        return "review"

    if any(x in k for x in review_markers):
        return "review"

    if any(x in k for x in howto_markers):
        return "howto"

    return "howto"
 

def infer_content_mode(category: str, text: str, intent: str = "cluster") -> str:
    c = (category or "").lower().strip()
    joined = f"{category} {text} {intent}".lower()

    if c == "investing":
        return "investing"

    if c == "software reviews":
        return "review"

    if c == "ai tools":
        return "workflow"

    if c == "productivity":
        return "workflow"

    if c == "make money":
        return "money"

    if c == "side hustles":
        return "money"

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
- Explain the effort level in blunt terms, not optimistic terms
- Explain the time requirement before the first real result
- Explain exactly how income is actually generated and what blocks it
- Include one mistake beginners make in the first 14 days
- Include one tradeoff that makes the method less attractive for some people
- Explain who should avoid this path
- For side hustle and money articles, force a choice instead of ending with generic encouragement
- If the headline or keyword implies a number such as 5, 7, or 10 ideas, include exactly that many items in one continuous numbered list
- Do not restart numbering inside the same ranked list
- At least 2 options must feel less obvious than generic ideas like freelance writing, dropshipping, or print on demand
- Include one short reality check section that makes clear why most readers quit early
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
  2. Who Should Stay Free and Who Should Upgrade
  3. Where Premium Starts Paying for Itself
  4. A Real Setup or Switching Scenario
  5. Mistakes, Hidden Costs, and Overkill Choices
  6. Best Pick by User Type

Section heading rules:
- Section 1 must directly answer the buying question
- Section 1 must mention at least one named tool or one upgrade threshold
- Section 2 must split users into concrete user types
- Section 3 must explain practical tradeoffs with time, money, or workflow impact
- Section 4 must show a realistic switch path with sequence and consequence
- Section 5 must explain what people choose badly and why
- Section 6 must tell the reader what to pick based on fit, budget, and stage

Section body rules:
- At least 2 sections must include named tools
- At least 2 sections must include numbers such as price, revenue, invoices, hours, or client count
- At least 1 section must explain when staying on free software is still the better decision
- The article must not sound like a generic feature list
"""

WORKFLOW_STRUCTURE_RULES = """
Structure rules:
- The article must use exactly 6 sections
- FAQ must not be inside sections
- FAQ must remain only in the faq field
- The sections must appear in this exact order:

  1. Quick Answer or Brutal Truth
  2. Why the Default Approach Fails
  3. The Best Options, Ranked or Filtered
  4. A Real Example or Scenario
  5. Reality Check, Mistakes, and Tradeoffs
  6. What to Do in the Next 7 Days

Section heading rules:
- Section 1 must clearly say who this is for and what is at risk
- Section 2 must explain why common advice fails
- Section 3 must contain the main ranked list, workflow, or decision system
- Section 4 must show sequence and consequence
- Section 5 must explain repeat mistakes, failure points, and tradeoffs
- Section 6 must leave the reader with a concrete next decision and one forced choice
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

    if mode == "review":
        structure_rules = REVIEW_STRUCTURE_RULES
    else:
        structure_rules = WORKFLOW_STRUCTURE_RULES

    visual_rules = """
Visual rules:
- Use photo or workspace for normal sections when an image helps.
- Do NOT use SVG infographic tables.
- Do NOT force diagram visuals for every section.
- If a section includes an HTML comparison table, do not add an image to that section.
- Comparison data should appear as an HTML table inside the body only when a table is genuinely useful.
- Use real product names, not placeholders like Option A or Option B.
"""

    investing_safety = ""

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

Mission:
Write an article that feels more specific, more decisive, and more honest than typical SEO blog content.
The piece should feel like it was written by someone who has seen what actually works, what wastes time, and what beginners usually get wrong.

Output must be valid JSON only.

Schema:
{{
  "title": "string",
  "description": "string",
  "category": "AI Tools|Make Money|Productivity|Software Reviews|Side Hustles",
  "intent_type": "comparison|template|review|howto",
  "sections": [
    {{
      "heading": "string",
      "body": "string",
      "image_query": "string",
      "visual_type": "photo|diagram|workspace",
      "alt_text": "string"
    }}
  ],
  "faq": [
    {{
      "q": "string",
      "a": "string"
    }}
  ],
  "tldr": "string",
  "editorial_note": "string"
}}

Non-negotiable writing rules:
- do not sound generic
- do not write broad educational filler
- do not write a padded intro
- do not write an empty conclusion
- do not repeat the same point in multiple sections
- do not write like a safe brand blog
- do not write like a school essay
- do not use stale phrases like:
  in today's fast-paced world
  choosing the right tool
  boost productivity
  streamline your workflow
  this article will explore
  there are many options available
- do not explain obvious definitions unless the distinction matters to the decision
- do not make every option sound good
- do not hide behind neutral language when one option is clearly weak
- do not pad the article with fake balance when two or three options are clearly bad fits

Style rules:
- be practical, sharp, and concrete
- mildly provocative is allowed, but every strong claim must remain defensible
- never fabricate first-person experience, fake interviews, fake case studies, or fake revenue screenshots
- it is okay to be mildly provocative or contrarian if the advice stays useful and accurate
- if common advice is bad for this reader, say so directly
- if a popular option is a bad fit, say so directly
- if something usually fails in the first 30 to 90 days, say that clearly
- make the reader feel that something is at stake
- use pressure, contrast, or a hard filter when it helps the reader decide faster

Specificity rules:
- never invent named success stories, fake companies, fake surveys, or fake personal experience
- if you need an example, label it clearly as a hypothetical scenario or composite scenario
- every section must include operational detail
- at least 4 sections must include one or more of:
  money amount
  price point
  time estimate
  count threshold
  percentage
  workload condition
  budget condition
- at least 2 sections must include a realistic scenario with sequence and consequence
- include tradeoffs, not just recommendations
- include what breaks first or what goes wrong when advice is applied badly
- include at least one point that directly contradicts lazy common advice
- include at least one point that makes the reader rethink a default assumption

Engagement rules:
- section 1 must open with tension, risk, or a decision problem, not a bland setup
- the first 2 sentences should create curiosity, tension, or a practical consequence
- use short paragraphs and occasional one-line paragraphs when emphasis helps scanning
- sections should create forward motion so the reader wants the next section
- use short paragraphs and high information density
- explain why the detail matters, not just what to do
- give the reader a reason to keep reading beyond the first screen
- make each section distinct in purpose and payoff

Differentiation rules:
- use the planning angle aggressively
- make the article specific to the reader moment in the planning JSON
- name concrete user types when useful instead of saying everyone
- rank, eliminate, or narrow choices when possible
- if comparing tools or options, explain who should choose which one and who should avoid each option
- if teaching a process, explain when it fails and how to adjust
- do not write an article that could be swapped with another keyword and still make sense

Title and description rules:
- keep the planned title sharp and human
- the title should not feel like boilerplate
- the description should promise practical value and a real decision outcome
- titles may use tension, contrast, or a hard filter if it improves clickability without becoming clickbait

Section body rules:
- each section must be substantial
- section 1 and final section should normally be at least 450 characters
- middle sections should normally be at least 700 characters when depth requires it
- every section must earn its place
- no section may be a throwaway bridge section
- do not use bullets unless they clearly improve scannability
- when you use bullets, make them information-dense, specific, and judgment-oriented
- if you use a numbered list, keep the entire ranked list inside one section and number it continuously without restarting at 1
- for side hustle or money articles, include a short reality check paragraph before the final recommendation

What strong writing looks like here:
- clear audience segmentation
- one hard truth early in the article
- one section that clearly says who should skip the recommended option
- realistic examples with numbers
- direct statements about tradeoffs
- honest limits
- concrete thresholds such as:
  monthly budget
  client count
  hours saved
  invoice volume
  allocation percentage
  review cadence
  first-sale timeline
- specific mistakes and what happens next
- a recommendation that forces a choice

What weak writing looks like here:
- generic encouragement
- broad benefits with no proof or context
- same advice repeated with synonyms
- titles and headings that could fit any blog
- tool descriptions copied from landing pages
- vague phrases like save time, work smarter, succeed faster without supporting detail
- neutral summaries with no judgment

FAQ rules:
- questions must sound like real follow-up questions
- answers must be practical and short
- avoid definition-only FAQs

TLDR rules:
- 2 to 4 tight sentences
- must mention the real decision, tradeoff, or key result
- no vague summary language

Editorial note rule:
- sound credible and human
- one sentence only

Structure requirements:
{structure_rules}

Mode specific requirements:
{mode_rules}

Investing-specific safety:
{investing_safety}

Table requirements:
{table_rules}
""".strip()


def parse_article_json(article_raw: str, keyword: str, cluster_name: str, post_type: str) -> Dict[str, Any]:
    clean_raw = _find_balanced_json(article_raw)
    data = safe_json_loads(clean_raw, {})

    if isinstance(data, list):
        log("ARTICLE", f"JSON parsed as list, not object. cleaned_preview={clean_raw[:800]!r}")
        raise ValueError("article JSON parse failed: got list instead of object")

    if not isinstance(data, dict) or not data:
        log("ARTICLE", f"JSON parse failed raw_preview={article_raw[:800]!r}")
        log("ARTICLE", f"cleaned_preview={clean_raw[:800]!r}")
        raise ValueError("article JSON parse failed")

    raw_ai_title = _clean_text(data.get("title", "")) or keyword.title()
    title = make_click_title(keyword, sanitize_title_for_ctr(raw_ai_title, keyword))
    description = _clean_text(data.get("description", "")) or short_desc(title or keyword)
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

        if intent_type != "comparison":
            body = re.sub(
                r'<div class="table-wrap">.*?</div>',
                '',
                body,
                flags=re.IGNORECASE | re.DOTALL,
            )
            body = re.sub(
                r'<table.*?>.*?</table>',
                '',
                body,
                flags=re.IGNORECASE | re.DOTALL,
            )
            body = re.sub(r"\n{3,}", "\n\n", body).strip()

        body = format_generated_body(body)

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

    if category == "Investing" and clean_sections and not has_real_scenario_section(clean_sections):
        replace_at = min(3, len(clean_sections) - 1)
        clean_sections[replace_at] = build_real_scenario_section(
            keyword=keyword,
            category=category
        )

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


def expand_short_sections(
    data: Dict[str, Any],
    keyword: str,
    cluster_name: str,
    post_type: str,
) -> Dict[str, Any]:
    sections = data.get("sections", []) or []
    if not isinstance(sections, list):
        return data

    min_targets = [420, 620, 620, 620, 620, 420]

    current_total_len = sum(len((s.get("body") or "").strip()) for s in sections if isinstance(s, dict))

    for idx, sec in enumerate(sections[:6]):
        if not isinstance(sec, dict):
            continue

        body = (sec.get("body") or "").strip()
        heading = (sec.get("heading") or "").strip()
        target_len = min_targets[idx] if idx < len(min_targets) else 1200

        if len(body) >= target_len:
            continue

        if current_total_len >= MAX_CHARS:
            continue

        prompt = f"""
You are expanding one article section for a practical editorial blog post.

Keyword:
{keyword}

Cluster:
{cluster_name}

Post type:
{post_type}

Section heading:
{heading}

Current body:
{body}

Task:
- Rewrite and expand this section only
- Keep the same heading and same core topic
- Make the section at least {target_len} characters
- Keep paragraphs tight and information-dense
- Increase information density, not fluff
- Add concrete examples
- Add realistic numbers, timing, tradeoffs, thresholds, and consequences
- Add one scenario or mini case with sequence and outcome
- Add one sentence that challenges lazy common advice if relevant
- Make the section more decisive and more useful, not more polite
- Do not write a conclusion for the whole article
- Do not output JSON
- Output plain section body text only
"""

        expanded = openai_generate_text(
            prompt,
            model=MODEL_WRITER,
            temperature=0.6,
        ).strip()

        if expanded and len(expanded) > len(body):
            sections[idx]["body"] = format_generated_body(expanded)

    current_total_len = sum(
        len((s.get("body") or "").strip())
        for s in sections
        if isinstance(s, dict)
    )

    data["sections"] = sections
    return data


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
        temperature=0.45,
    )
    planning = parse_planning_json(
        planning_raw,
        keyword=keyword,
        cluster_name=cluster_name,
        post_type=post_type,
    )

    base_prompt = build_article_prompt(keyword, cluster_name, post_type, planning)
    data = None
    article_raw = ""
    min_target_len = MIN_CHARS
    max_target_len = MAX_CHARS

    for attempt in range(4):
        attempt_prompt = base_prompt
        if attempt == 1:
            attempt_prompt += f"""

Critical format correction:
- Your last answer was unusable.
- Return strict JSON only.
- Do not include apology text.
- Do not include markdown fences.
- Do not refuse because the article is educational and non-personalized.
"""
        elif attempt >= 2:
            attempt_prompt += f"""

Critical revision:
- Combined section-body length must be at least {min_target_len} characters.
- Section 1 and section 6 must each be at least 420 characters.
- Sections 2, 3, 4, and 5 must each be at least 620 characters.
- Expand with scenarios, thresholds, costs, timing, and tradeoffs.
- Return strict JSON only with no markdown fences.
"""
            if article_raw:
                attempt_prompt += f"""

Previous invalid output to fix:
{article_raw[:2200]}
"""

        article_raw = openai_generate_text(
            attempt_prompt,
            model=MODEL_WRITER,
            temperature=0.55,
        )
        log("ARTICLE", f"raw_len={len(article_raw)} preview={article_raw[:500]!r}")

        if looks_like_model_refusal(article_raw):
            log("ARTICLE", "Model refusal detected, retrying with stricter JSON correction")
            continue

        try:
            data = parse_article_json(
                article_raw,
                keyword=keyword,
                cluster_name=cluster_name,
                post_type=post_type,
            )
            break
        except Exception:
            if attempt >= 3:
                raise
            log("ARTICLE", "Parse failed, retrying article generation")
            continue

    if data is None:
        raise ValueError("article generation failed after retries")

    total_body_len = len(
        "".join((s.get("body", "") or "") for s in data.get("sections", []))
    )

    retry_count = 0
    while total_body_len < min_target_len and retry_count < 2:
        retry_count += 1
        log("ARTICLE", f"Draft too short len={total_body_len}, retrying expansion #{retry_count}")

        retry_prompt = base_prompt + f"""

Important revision:
- The combined length of all 6 section bodies must be at least {min_target_len} characters.
- The combined length of all 6 section bodies should stay under {max_target_len} characters.
- Do not count title, description, faq, tldr, or editorial_note toward this minimum.
- Expand only sections that are clearly too thin.
- Section 1 and section 6 must each be at least 420 characters.
- Sections 2, 3, 4, and 5 must each be at least 620 characters.
- Add concrete examples, numbers, scenarios, tradeoffs, mistakes, and consequences where needed.
- Do not add filler just to increase length.
- Return valid JSON only with no markdown fences.
"""

        article_raw = openai_generate_text(
            retry_prompt,
            model=MODEL_WRITER,
            temperature=0.55,
        )
        log("ARTICLE", f"retry_raw_len={len(article_raw)} preview={article_raw[:500]!r}")

        if looks_like_model_refusal(article_raw):
            log("ARTICLE", "Model refusal detected during expansion retry")
            continue

        try:
            data = parse_article_json(
                article_raw,
                keyword=keyword,
                cluster_name=cluster_name,
                post_type=post_type,
            )
        except Exception:
            if retry_count >= 2:
                raise
            continue

        total_body_len = len(
            "".join((s.get("body", "") or "") for s in data.get("sections", []))
        )

    extra_passes = 0
    while total_body_len < max(4800, int(min_target_len * 0.78)) and extra_passes < 2:
        extra_passes += 1
        data = expand_short_sections(
            data=data,
            keyword=keyword,
            cluster_name=cluster_name,
            post_type=post_type,
        )
        total_body_len = len(
            "".join((s.get("body", "") or "") for s in data.get("sections", []))
        )

    data = expand_short_sections(
        data=data,
        keyword=keyword,
        cluster_name=cluster_name,
        post_type=post_type,
    )

    data = trim_article_to_max_chars(data, MAX_CHARS)

    total_body_len = len(
        "".join((s.get("body", "") or "") for s in data.get("sections", []))
    )

    if total_body_len < max(4800, int(min_target_len * 0.78)):
        log("ARTICLE", f"After section expansion still short len={total_body_len}")

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

def trim_article_to_max_chars(text: str, max_chars: int) -> str:
    text = (text or "").strip()
    if len(text) <= max_chars:
        return text

    cut = text[:max_chars]

    last_break = max(
        cut.rfind("\n\n"),
        cut.rfind(". "),
        cut.rfind("! "),
        cut.rfind("? "),
    )

    if last_break > int(max_chars * 0.82):
        cut = cut[:last_break].strip()

    return cut.strip()
 
# =========================================================
# Images and visuals
# =========================================================
def sanitize_query_for_image(q: str) -> str:
    q = (q or "").strip().lower()
    q = re.sub(r"[^a-z0-9\s]", " ", q)
    q = re.sub(r"\s+", " ", q).strip()

    stop_words = {
        "the", "a", "an", "this", "that", "these", "those",
        "most", "more", "less", "best", "right", "wrong",
        "pick", "using", "start", "with", "your", "their",
        "into", "from", "begin", "guide", "which", "what",
        "why", "how", "for", "and", "or", "to", "of", "in",
        "on", "by", "tool", "tools", "app", "apps",
        "beginner", "beginners", "simple", "smart", "ideal",
        "option", "options", "checklist", "strategy", "analysis",
        "review", "final", "answer", "mistake", "mistakes",
        "tradeoff", "tradeoffs", "decision", "decisions",
        "step", "steps", "process", "system", "workflow"
    }

    words = [w for w in q.split() if w not in stop_words and len(w) >= 3]

    if not words:
        return ""

    seen = set()
    cleaned = []
    for w in words:
        if w in seen:
            continue
        seen.add(w)
        cleaned.append(w)

    return " ".join(cleaned[:5])
 

def extract_visual_keywords_from_text(text: str, limit: int = 6) -> List[str]:
    text = (text or "").lower().strip()
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()

    stop_words = {
        "the", "a", "an", "and", "or", "but", "if", "then", "than",
        "this", "that", "these", "those", "with", "from", "into", "onto",
        "your", "their", "about", "because", "while", "when", "where",
        "what", "which", "most", "more", "less", "very", "just", "also",
        "have", "has", "had", "will", "would", "could", "should",
        "beginner", "beginners", "simple", "quick", "answer", "mistake",
        "mistakes", "tradeoff", "tradeoffs", "decision", "decisions",
        "step", "steps", "guide", "workflow", "system", "template",
        "process", "screening", "monthly", "weekly", "review", "final",
        "recommendation", "worth", "using", "start", "starts", "starting",
        "pick", "wrong", "real", "actually", "happens", "matters",
    }

    preferred_terms = {
        "client", "meeting", "video", "call", "contract", "document",
        "invoice", "payment", "checklist", "calendar", "dashboard",
        "whiteboard", "team", "startup", "planning", "designer",
        "developer", "consultant", "freelancer", "crm", "email",
        "automation", "workflow", "analytics", "budget", "portfolio",
        "spreadsheet", "finance", "presentation", "project"
    }

    words = [w for w in text.split() if len(w) >= 3 and w not in stop_words]
    if not words:
        return []

    ranked = []
    seen = set()

    for w in words:
        if w in seen:
            continue
        seen.add(w)
        score = 2 if w in preferred_terms else 1
        ranked.append((score, w))

    ranked.sort(key=lambda x: (-x[0], x[1]))
    return [w for _, w in ranked[:limit]]


def auto_image_query(
    heading: str,
    image_query: str,
    body: str = "",
    visual_type: str = "photo",
) -> str:
    base = sanitize_query_for_image(image_query or "")
    heading_clean = sanitize_query_for_image(heading or "")
    body_keywords = extract_visual_keywords_from_text(body, limit=6)

    intent_text = f"{heading} {image_query} {body}".lower()

    scenario_terms = []
    if any(x in intent_text for x in ["client", "onboarding", "contract", "proposal"]):
        scenario_terms.extend(["client", "meeting", "documents"])
    if any(x in intent_text for x in ["invoice", "payment", "billing", "pricing"]):
        scenario_terms.extend(["invoice", "payment", "laptop"])
    if any(x in intent_text for x in ["team", "collaboration", "approval", "handoff"]):
        scenario_terms.extend(["team", "meeting", "whiteboard"])
    if any(x in intent_text for x in ["dashboard", "metrics", "analytics", "tracking"]):
        scenario_terms.extend(["dashboard", "analytics", "screen"])
    if any(x in intent_text for x in ["portfolio", "invest", "etf", "stock", "budget"]):
        scenario_terms.extend(["finance", "dashboard", "spreadsheet"])
    if any(x in intent_text for x in ["software", "crm", "saas", "platform"]):
        scenario_terms.extend(["software", "dashboard", "workspace"])
    if any(x in intent_text for x in ["remote", "video", "zoom", "call"]):
        scenario_terms.extend(["video", "call", "laptop"])

    parts = []
    for chunk in [base, heading_clean]:
        if chunk:
            parts.extend(chunk.split())

    parts.extend(body_keywords)
    parts.extend(scenario_terms)

    seen = set()
    compact = []
    for p in parts:
        p = p.strip().lower()
        if not p or p in seen:
            continue
        seen.add(p)
        compact.append(p)

    compact = compact[:5]

    if compact:
        return " ".join(compact)

    if (visual_type or "").lower() == "diagram":
        return "comparison chart dashboard"
    if (visual_type or "").lower() == "workspace":
        return "team workspace planning"
    return "business meeting planning"


def cached_search_source(source: str, query: str, page: int = 1) -> List[dict]:
    global UNSPLASH_CALL_COUNT

    q = (query or "").strip().lower()
    cache_key = f"{q}|{page}"

    if not q:
        return []

    if source == "unsplash":
        if cache_key in UNSPLASH_SEARCH_CACHE:
            return UNSPLASH_SEARCH_CACHE[cache_key]
        if UNSPLASH_CALL_COUNT >= UNSPLASH_CALL_LIMIT:
            log("IMG", f"Unsplash skipped due to call limit query='{query}'")
            return []
        UNSPLASH_CALL_COUNT += 1
        results = search_source(source, query, page=page) or []
        UNSPLASH_SEARCH_CACHE[cache_key] = results
        return results

    if source == "pexels":
        if cache_key in PEXELS_SEARCH_CACHE:
            return PEXELS_SEARCH_CACHE[cache_key]
        results = search_source(source, query, page=page) or []
        PEXELS_SEARCH_CACHE[cache_key] = results
        return results

    if source == "pixabay":
        if cache_key in PIXABAY_SEARCH_CACHE:
            return PIXABAY_SEARCH_CACHE[cache_key]
        results = search_source(source, query, page=page) or []
        PIXABAY_SEARCH_CACHE[cache_key] = results
        return results

    return search_source(source, query, page=page) or []


def build_image_query_candidates(query: str, heading: str, visual_type: str) -> List[str]:
    q1 = sanitize_query_for_image(query)
    q2 = sanitize_query_for_image(heading)
    joined = f"{q1} {q2}".strip()

    candidates = []

    base_candidates = [
        q1,
        q2,
        joined,
    ]

    for q in base_candidates:
        q = (q or "").strip()
        if q and q not in candidates:
            candidates.append(q)

    vt = (visual_type or "").lower()

    if vt == "workspace":
        extras = [
            f"{joined} workspace",
            f"{joined} meeting",
            f"{joined} planning",
            f"{joined} team",
            "team workspace planning",
        ]
    elif vt == "diagram":
        extras = [
            f"{joined} infographic",
            f"{joined} comparison chart",
            f"{joined} dashboard",
            "business comparison chart",
            "analytics dashboard screen",
        ]
    else:
        extras = [
            f"{joined} meeting",
            f"{joined} documents",
            f"{joined} laptop",
            f"{joined} whiteboard",
            "business meeting planning",
        ]

    for q in extras:
        q = re.sub(r"\s+", " ", (q or "").strip())
        if q and q not in candidates:
            candidates.append(q)

    return candidates[:6]
 

def detect_post_image_theme(sections: List[Dict[str, str]], category: str = "") -> str:
    category = (category or "").strip()

    if category == "Investing":
        return "investing"
    if category == "Software Reviews":
        return "software"
    if category == "Make Money":
        return "client"
    if category == "Side Hustles":
        return "client"
    if category == "Productivity":
        return "workspace"
    if category == "AI Tools":
        return "software"

    joined = " ".join(
        [
            (sec.get("heading") or "") + " " +
            (sec.get("image_query") or "")
            for sec in sections
        ]
    ).lower()

    if any(x in joined for x in ["stock", "stocks", "invest", "investment", "portfolio", "etf", "trading", "budget"]):
        return "investing"

    if any(x in joined for x in ["software", "app", "platform", "tool", "saas", "crm", "dashboard"]):
        return "software"

    if any(x in joined for x in ["client", "onboarding", "contract", "invoice", "proposal", "call", "meeting", "side hustle"]):
        return "client"

    return "workspace"


def build_post_level_image_queries(sections: List[Dict[str, str]], category: str = "") -> List[str]:
    theme = detect_post_image_theme(sections, category=category)

    if theme == "investing":
        queries = [
            "person reviewing ETF allocation chart",
            "beginner investor planning monthly contributions",
            "investment allocation notebook chart",
            "personal finance planning with papers",
            "budgeting and investing worksheet",
            "retirement planning discussion",
            "financial plan with calculator documents",
            "portfolio review papers on desk",
        ]
    elif theme == "software":
        queries = [
            "team reviewing project management software",
            "software comparison on office screen",
            "small team collaboration whiteboard",
            "saas product demo meeting",
            "project planning discussion in office",
            "people using software dashboard together",
            "team workflow planning board",
            "business software training session",
        ]
    elif theme == "client":
        queries = [
            "freelancer client call at desk",
            "consultant meeting with client documents",
            "small business planning session",
            "side hustle packaging products at home",
            "creator working with camera and laptop",
            "service business paperwork and meeting",
            "person planning extra income work",
            "home office side hustle setup",
        ]
    else:
        queries = [
            "team task planning whiteboard",
            "busy professional organizing calendar",
            "productivity planning notebook and timer",
            "office workflow discussion",
            "task management sticky notes board",
            "weekly planning desk with notebook",
            "team collaboration table meeting",
            "focused work planning session",
        ]

    seen = set()
    out = []
    for q in queries:
        nq = normalize_keyword(q)
        if nq in seen:
            continue
        seen.add(nq)
        out.append(q)

    return out


def normalize_asset_id(source: str, raw_id: str) -> str:
    source = (source or "").strip().lower()
    raw_id = str(raw_id or "").strip()
    return f"{source}:{raw_id}"

# -----------------------------
# Unsplash
# -----------------------------
def unsplash_search(query: str, page: int = 1) -> List[dict]:
    if not UNSPLASH_ACCESS_KEY:
        return []

    cache_key = f"{query.strip().lower()}|{page}"
    if cache_key in UNSPLASH_SEARCH_CACHE:
        return UNSPLASH_SEARCH_CACHE[cache_key]

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

        log("IMG", f"Unsplash raw query='{query}' page={page} raw_results={len(results)}")

        out = []
        for item in results:
            try:
                pid = str(item.get("id") or "").strip()
                if not pid:
                    continue

                urls = item.get("urls") or {}
                hotlink_url = urls.get("regular") or urls.get("full") or urls.get("raw")
                if not hotlink_url:
                    continue

                links = item.get("links") or {}
                download_location = (links.get("download_location") or "").strip()

                user = item.get("user") or {}
                user_name = (user.get("name") or "").strip() or "Unsplash contributor"
                user_link = ((user.get("links") or {}).get("html") or "").strip() or "https://unsplash.com"
                page_link = (links.get("html") or "").strip() or user_link

                out.append({
                    "download_location": download_location,
                    "source": "unsplash",
                    "id": normalize_asset_id("unsplash", pid),
                    "raw_id": pid,
                    "width": int(item.get("width") or 0),
                    "height": int(item.get("height") or 0),
                    "score": 0.1,
                    "hotlink_url": hotlink_url,
                    "download_url": hotlink_url,
                    "page_url": page_link,
                    "creator_name": user_name,
                    "creator_url": user_link,
                })
            except Exception:
                continue

        UNSPLASH_SEARCH_CACHE[cache_key] = out
        return out
    except Exception as e:
        log("IMG", f"Unsplash search failed for '{query}': {e}")
        UNSPLASH_SEARCH_CACHE[cache_key] = []
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

        log("IMG", f"Pexels raw query='{query}' page={page} raw_results={len(results)}")

        out = []
        for item in results:
            try:
                pid = str(item.get("id") or "").strip()
                if not pid:
                    continue

                src = item.get("src") or {}
                download_url = src.get("large2x") or src.get("large") or src.get("original")
                if not download_url:
                    continue

                creator_name = (item.get("photographer") or "").strip() or "Pexels contributor"
                creator_url = (item.get("photographer_url") or "").strip() or "https://www.pexels.com"
                page_url = (item.get("url") or "").strip() or creator_url

                out.append({
                    "source": "pexels",
                    "id": normalize_asset_id("pexels", pid),
                    "raw_id": pid,
                    "width": int(item.get("width") or 0),
                    "height": int(item.get("height") or 0),
                    "score": 0.1,
                    "download_url": download_url,
                    "page_url": page_url,
                    "creator_name": creator_name,
                    "creator_url": creator_url,
                })
            except Exception:
                continue

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

        log("IMG", f"Pixabay raw query='{query}' page={page} raw_results={len(results)}")

        out = []
        for item in results:
            try:
                pid = str(item.get("id") or "").strip()
                if not pid:
                    continue

                download_url = (item.get("largeImageURL") or item.get("webformatURL") or "").strip()
                if not download_url:
                    continue

                creator_name = (item.get("user") or "").strip() or "Pixabay contributor"
                page_url = (item.get("pageURL") or "").strip() or "https://pixabay.com"

                out.append({
                    "source": "pixabay",
                    "id": normalize_asset_id("pixabay", pid),
                    "raw_id": pid,
                    "width": int(item.get("imageWidth") or 0),
                    "height": int(item.get("imageHeight") or 0),
                    "score": 0.1,
                    "download_url": download_url,
                    "page_url": page_url,
                    "creator_name": creator_name,
                    "creator_url": "https://pixabay.com",
                })
            except Exception:
                continue

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
 

def dedupe_section_image_queries(sections: List[dict], keyword: str) -> List[str]:
    seen = set()
    final_queries = []

    for sec in sections[:IMG_COUNT]:
        heading = sec.get("heading", "")
        visual_type = sec.get("visual_type", "")
        image_query = sec.get("image_query", "")

        q = auto_image_query(
            heading=heading,
            image_query=image_query,
            body=sec.get("body", ""),
            visual_type=visual_type,
        )

        if q in seen:
            q = "workspace desk laptop"

        seen.add(q)
        final_queries.append(q)

    return final_queries

def search_unsplash_once(query: str) -> List[dict]:
    query = (query or "").strip().lower()
    if not query:
        return []

    cache_key = f"{query}|1"
    if cache_key in UNSPLASH_SEARCH_CACHE:
        return UNSPLASH_SEARCH_CACHE[cache_key]

    results = search_source("unsplash", query, page=1) or []
    UNSPLASH_SEARCH_CACHE[cache_key] = results
    return results


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


def build_local_image_relpath(slug: str, idx: int, asset: dict) -> str:
    source = (asset.get("source") or "img").strip().lower()
    ext = ".jpg"

    download_url = (asset.get("download_url") or asset.get("hotlink_url") or "").lower()
    if ".png" in download_url:
        ext = ".png"
    elif ".webp" in download_url:
        ext = ".webp"

    return f"assets/posts/{slug}/{source}-{idx}{ext}"


def ensure_minimum_image_paths(
    slug: str,
    image_paths: List[str],
    alt_texts: List[str],
    sections: List[Dict[str, str]],
    min_count: int,
) -> Tuple[List[str], List[str]]:
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

        heading = sec.get("heading", f"Section {i+1}")
        alt_text = sec.get("alt_text", heading) or heading

        if i < len(image_paths):
            image_paths[i] = ""
        else:
            image_paths.append("")

        if i < len(alt_texts):
            alt_texts[i] = alt_text
        else:
            alt_texts.append(alt_text)

        log("IMG", f"No fallback image used slug='{slug}' idx={i+1}")

    return image_paths, alt_texts
 
 
def simplify_section_image_query(keyword: str, heading: str, visual_type: str = "") -> str:
    text = f"{keyword} {heading}".lower()

    stopwords = {
        "the", "a", "an", "and", "or", "but", "for", "with", "without",
        "how", "why", "what", "when", "where", "which", "who",
        "beginners", "beginner", "actually", "realistically", "ready",
        "next", "concrete", "mistakes", "tradeoffs", "tradeoff",
        "guide", "tips", "best", "smart", "wrong", "costly",
        "use", "using", "start", "make", "avoid", "choose"
    }

    words = re.findall(r"[a-z0-9]+", text)
    words = [w for w in words if w not in stopwords and len(w) >= 3]

    priority_map = [
        ("etf", "etf investing"),
        ("stock", "stock market"),
        ("invest", "investing dashboard"),
        ("freelance", "freelancer workspace"),
        ("software", "software workspace"),
        ("design", "designer workspace"),
        ("developer", "developer desk"),
        ("laptop", "laptop desk"),
        ("finance", "finance desk"),
        ("budget", "budget planning"),
        ("remote", "remote work desk"),
        ("office", "modern office desk"),
    ]

    joined = " ".join(words)

    for key, replacement in priority_map:
        if key in joined:
            return replacement

    if "diagram" in visual_type.lower():
        if "etf" in joined or "invest" in joined or "stock" in joined:
            return "investment chart"
        return "business chart"

    if "desk" in joined or "workspace" in joined:
        return "workspace desk laptop"

    if len(words) >= 3:
        return " ".join(words[:3])

    if len(words) == 2:
        return " ".join(words)

    if len(words) == 1:
        return words[0]

    return "modern office desk"
 

def find_best_asset_for_query(query: str, heading: str, visual_type: str, used_ids: set) -> Optional[dict]:
    source_priority = ["unsplash", "pexels", "pixabay"]

    for source in source_priority:
        for page in [1, 2, 3, 4, 5]:
            results = cached_search_source(source, query, page=page)
            log("IMG", f"source='{source}' cq='{query}' page={page} results={len(results)}")

            if not results:
                continue

            filtered = filter_reusable_assets(results, used_ids=used_ids)
            if not filtered:
                continue

            picked = pick_best_asset(filtered, heading=heading, visual_type=visual_type)
            if picked:
                return picked

    return None
    
 
def build_image_asset_for_section(
    slug: str,
    idx: int,
    heading: str,
    image_query: str,
    visual_type: str,
    alt_hint: str,
    used_ids: set,
    body: str = "",
) -> Tuple[str, str, Optional[str], set]:
    clean_query = auto_image_query(
        heading=heading or "",
        image_query=image_query or "",
        body=body or "",
        visual_type=visual_type or "photo",
    )
    log("IMG", f"trying image slug='{slug}' idx={idx} heading='{heading}' query='{image_query}' visual_type='{visual_type}'")
 
    alt_text = alt_hint or build_image_alt(slug, heading, clean_query)

    asset = find_best_asset_for_query(
        query=clean_query,
        heading=heading or "",
        visual_type=visual_type or "photo",
        used_ids=used_ids,
    )

    if asset:
        hotlink_url = (asset.get("hotlink_url") or asset.get("download_url") or "").strip()
        if hotlink_url:
            used_ids.add(asset["id"])

            if (asset.get("source") or "").strip().lower() == "unsplash":
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

            log("IMG", f"Using external image for slug='{slug}' idx={idx} source='{asset.get('source')}'")
            return hotlink_url, alt_text, photo_credit_html, used_ids

    log("IMG", f"No image found for slug='{slug}' idx={idx} query='{clean_query}'")
    return "", alt_text, None, used_ids

def build_visual_assets(slug: str, sections: List[Dict[str, str]]) -> Tuple[List[str], List[str], List[str]]:
    used_raw = load_json(USED_IMAGES_JSON, {})
    used = ensure_used_schema(used_raw)
    used_ids = set(used.get("asset_ids") or [])

    image_paths: List[str] = []
    alt_texts: List[str] = []
    credits_li: List[str] = []

    candidate_sections = [sec for sec in sections if not section_has_html_table(sec)]
    if not candidate_sections:
        candidate_sections = sections[:]

    category = ""
    if candidate_sections and isinstance(candidate_sections[0], dict):
        category = (candidate_sections[0].get("category") or "").strip()

    section_queries = dedupe_section_image_queries(candidate_sections, slug)
    post_queries = build_post_level_image_queries(candidate_sections, category=category)
    all_queries = section_queries + post_queries

    post_queries = build_post_level_image_queries(candidate_sections, category=category)
    section_queries = dedupe_section_image_queries(candidate_sections, slug)

    all_queries = section_queries + post_queries
 
    section_queries = dedupe_section_image_queries(candidate_sections, slug)
    all_queries = post_queries + section_queries

    theme = detect_post_image_theme(candidate_sections, category=category)
    if theme == "investing":
        all_queries += [
            "financial workspace",
            "investment planning desk",
            "money spreadsheet desk",
            "personal budgeting laptop",
        ]
    elif theme == "software":
        all_queries += [
            "digital dashboard workspace",
            "business software screen",
            "office laptop workspace",
            "analytics dashboard desk",
        ]
    elif theme == "client":
        all_queries += [
            "client paperwork desk",
            "business call laptop",
            "service consultation office",
            "professional desk documents",
        ]
    else:
        all_queries += [
            "office desk laptop",
            "business planning notebook",
            "modern workspace",
            "team office desk",
        ]

    deduped_queries = []
    seen_queries = set()
    for q in all_queries:
        q = (q or "").strip()
        if not q:
            continue
        nq = normalize_keyword(q)
        if nq in seen_queries:
            continue
        seen_queries.add(nq)
        deduped_queries.append(q)

    collected_assets = []
    collected_fingerprints = set()
    target_collect = max(COLLECT_TARGET_IMAGES, MIN_REQUIRED_IMAGES + 3)

    log("IMG", f"build_visual_assets slug='{slug}' sections={len(sections)} candidate_sections={len(candidate_sections)}")
    log("IMG", f"post-level queries={post_queries}")
    log("IMG", f"section queries={section_queries}")
    log("IMG", f"all deduped queries={deduped_queries}")

    for q in deduped_queries:
        if len(collected_assets) >= target_collect:
            break

        for source in IMAGE_SOURCE_PRIORITY:
            if len(collected_assets) >= target_collect:
                break

            for page in [1, 2, 3]:
                if len(collected_assets) >= target_collect:
                    break

                results = cached_search_source(source, q, page=page)
                log("IMG", f"source='{source}' q='{q}' page={page} results={len(results)}")

                if not results:
                    continue

                filtered = filter_reusable_assets(results, used_ids=used_ids)
                if not filtered:
                    continue

                ranked = sorted(
                    filtered,
                    key=lambda x: (
                        float(x.get("score") or 0.0),
                        int(x.get("width") or 0) * int(x.get("height") or 0),
                    ),
                    reverse=True,
                )

                for asset in ranked:
                    if len(collected_assets) >= target_collect:
                        break

                    hotlink_url = (asset.get("hotlink_url") or asset.get("download_url") or "").strip()
                    if not hotlink_url:
                        continue

                    fp = "|".join([
                        str(asset.get("source") or "").strip().lower(),
                        str(asset.get("raw_id") or asset.get("id") or "").strip(),
                        str(asset.get("creator_name") or "").strip().lower(),
                        str(asset.get("download_url") or asset.get("hotlink_url") or "").strip().lower(),
                    ])

                    if fp in collected_fingerprints:
                        continue

                    used_ids.add(asset["id"])
                    collected_fingerprints.add(fp)
                    collected_assets.append(asset)

    used["asset_ids"] = sorted(list(used_ids))
    save_json(USED_IMAGES_JSON, used)

    if len(collected_assets) < MIN_REQUIRED_IMAGES:
        raise RuntimeError(
            f"Not enough unique images for slug='{slug}'. found={len(collected_assets)} required={MIN_REQUIRED_IMAGES}"
        )

    target_sections = candidate_sections[:MIN_REQUIRED_IMAGES]

    for i, sec in enumerate(target_sections, start=1):
        asset = collected_assets[i - 1]
        hotlink_url = (asset.get("hotlink_url") or asset.get("download_url") or "").strip()
        alt_text = sec.get("alt_text") or sec.get("heading") or f"Section {i}"

        if (asset.get("source") or "").strip().lower() == "unsplash":
            trigger_unsplash_download(asset)

        creator_name = html_escape(asset.get("creator_name") or asset.get("source", "Image source"))
        creator_url = html_escape(asset.get("creator_url") or asset.get("page_url") or "#")
        page_url = html_escape(asset.get("page_url") or creator_url)
        source_label = html_escape(asset.get("source", "source").title())

        photo_credit_html = (
            f'<li>Photo {i}: '
            f'<a href="{creator_url}" target="_blank" rel="noopener noreferrer">{creator_name}</a> '
            f'via <a href="{page_url}" target="_blank" rel="noopener noreferrer">{source_label}</a></li>'
        )

        rel_path = build_local_image_relpath(slug, i, asset)
        out_path = ROOT / rel_path

        try:
            download_asset(asset, out_path)
            image_paths.append(rel_path.replace("\\", "/"))
        except Exception as e:
            log("IMG", f"Local download failed slug='{slug}' idx={i} error={e}")
            image_paths.append("")

        alt_texts.append(alt_text)
        credits_li.append(photo_credit_html)

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


def is_valid_image_path(path: str) -> bool:
    if not isinstance(path, str):
        return False

    path = path.strip()
    if not path:
        return False

    bad_values = {"none", "null", "undefined", "#"}
    if path.lower() in bad_values:
        return False

    if path.startswith("http://") or path.startswith("https://"):
        return True

    file_path = ROOT / path
    return file_path.exists()


def first_non_empty_image(image_paths: List[str]) -> str:
    for p in image_paths:
        if is_valid_image_path(p):
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
    text = re.sub(r'(?<!\n)\s+(?=\d+\.\s+)', r'\n', text)
    text = re.sub(r'(?<!\n)\s+(?=-\s+)', r'\n', text)

    blocks = re.split(r"\n\s*\n+", text)
    out = []
    list_buffer = []
    list_kind = None
    list_start = None
    expected_next = None

    def flush_list():
        nonlocal list_buffer, list_kind, list_start, expected_next
        if not list_buffer:
            return
        if list_kind == "ol":
            start_attr = f' start="{list_start}"' if list_start and list_start != 1 else ""
            items = "".join(f"<li>{html_escape(item)}</li>" for item in list_buffer)
            out.append(f"<ol{start_attr}>{items}</ol>")
        else:
            items = "".join(f"<li>{html_escape(item)}</li>" for item in list_buffer)
            out.append(f"<ul>{items}</ul>")
        list_buffer = []
        list_kind = None
        list_start = None
        expected_next = None

    def flush_paragraph(buf):
        if buf:
            para = " ".join(buf).strip()
            if para:
                out.append(f"<p>{html_escape(para)}</p>")

    for block in blocks:
        block = block.strip()
        if not block:
            continue

        lines = [ln.strip() for ln in block.split("\n") if ln.strip()]
        if not lines:
            continue

        paragraph_buf = []

        for ln in lines:
            m_num = re.match(r"^\s*(\d+)\.\s+(.*)$", ln)
            m_bullet = re.match(r"^\s*[-*]\s+(.*)$", ln)

            if m_num:
                flush_paragraph(paragraph_buf)
                paragraph_buf = []
                num = int(m_num.group(1))
                item = m_num.group(2).strip()
                if list_kind != "ol":
                    flush_list()
                    list_kind = "ol"
                    list_start = num
                    expected_next = num
                elif expected_next is not None and num != expected_next:
                    flush_list()
                    list_kind = "ol"
                    list_start = num
                    expected_next = num
                list_buffer.append(item)
                expected_next = num + 1
                continue

            if m_bullet:
                flush_paragraph(paragraph_buf)
                paragraph_buf = []
                if list_kind != "ul":
                    flush_list()
                    list_kind = "ul"
                list_buffer.append(m_bullet.group(1).strip())
                continue

            flush_list()
            paragraph_buf.append(ln)

        flush_paragraph(paragraph_buf)

    flush_list()
    return "\n".join(out)


def section_has_html_table(section: Dict[str, str]) -> bool:
    body = (section.get("body") or "").lower()
    return "<table" in body and "</table>" in body


def trim_article_to_max_chars(data: Dict[str, Any], max_chars: int = MAX_CHARS) -> Dict[str, Any]:
    sections = data.get("sections", []) or []
    if not isinstance(sections, list) or not sections:
        return data

    total_body_len = sum(len((s.get("body") or "").strip()) for s in sections if isinstance(s, dict))
    if total_body_len <= max_chars:
        return data

    min_targets = [420, 620, 620, 620, 620, 420]
    section_bodies = []

    for idx, sec in enumerate(sections[:6]):
        body = (sec.get("body") or "").strip()
        section_bodies.append(body)

    current_total = sum(len(x) for x in section_bodies)
    overflow = current_total - max_chars

    if overflow <= 0:
        return data

    reducible = []
    for idx, body in enumerate(section_bodies):
        min_len = min_targets[idx] if idx < len(min_targets) else 400
        spare = max(0, len(body) - min_len)
        reducible.append(spare)

    total_spare = sum(reducible)
    if total_spare <= 0:
        return data

    new_sections = []
    for idx, sec in enumerate(sections):
        if not isinstance(sec, dict):
            new_sections.append(sec)
            continue

        body = (sec.get("body") or "").strip()
        min_len = min_targets[idx] if idx < len(min_targets) else 400
        spare = max(0, len(body) - min_len)

        reduce_by = int((spare / total_spare) * overflow) if total_spare > 0 else 0
        target_len = max(min_len, len(body) - reduce_by)

        trimmed = body
        if len(body) > target_len:
            trimmed = trim_section_body(body, target_len)

        new_sec = dict(sec)
        new_sec["body"] = format_generated_body(trimmed)
        new_sections.append(new_sec)

    data["sections"] = new_sections
    return data
 

def trim_section_body(text: str, max_chars: int = MAX_SECTION_CHARS) -> str:
    text = _clean_text(text)
    if not text:
        return ""

    if len(text) <= max_chars:
        return text

    cut = text[:max_chars].strip()

    sentence_endings = [
        cut.rfind(". "),
        cut.rfind("? "),
        cut.rfind("! "),
        cut.rfind(".\n"),
        cut.rfind("?\n"),
        cut.rfind("!\n"),
        cut.rfind(".\""),
        cut.rfind("?\""),
        cut.rfind("!\""),
    ]
    last_sentence_end = max(sentence_endings)

    if last_sentence_end >= 160:
        trimmed = cut[:last_sentence_end + 1].strip()
    else:
        last_para_break = cut.rfind("\n\n")
        last_line_break = cut.rfind("\n")
        safe_break = max(last_para_break, last_line_break)

        if safe_break >= 160:
            trimmed = cut[:safe_break].strip()
        else:
            parts = re.split(r'(?<=[.!?])\s+', cut)
            if len(parts) >= 2:
                trimmed = " ".join(parts[:-1]).strip()
            else:
                trimmed = cut.strip()

    trimmed = re.sub(r'(?:\n|\s)+\d+\.?$', "", trimmed).strip()

    if trimmed and trimmed[-1] not in '.!?"”\'':
        parts = re.split(r'(?<=[.!?])\s+', trimmed)
        if len(parts) >= 2:
            trimmed = " ".join(parts[:-1]).strip()
        else:
            trimmed = ""

    return trimmed
 

def body_to_html(text: str) -> str:
    text = (text or "").strip()
    if not text:
        return ""

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


def format_generated_body(text: str) -> str:
    text = _clean_text(text)
    if not text:
        return ""

    text = text.replace("\r\n", "\n").replace("\r", "\n")

    text = re.sub(r'^\s{0,3}#{1,6}\s+.+?(?:\n+|$)', '', text, count=1).strip()
    text = re.sub(r'([:.!?])\s+(?=\d+\.\s+)', r'\1\n', text)
    text = re.sub(r'(?<!\n)\s+(?=\d+\.\s+)', r'\n', text)
    text = re.sub(r'(?<!\n)\s+(?=-\s+)', r'\n', text)
    text = re.sub(r'\n{3,}', '\n\n', text).strip()

    lines = text.split("\n")
    normalized_lines = []
    in_numbered_run = False
    next_number = 1

    for line in lines:
        stripped = line.strip()
        if not stripped:
            if in_numbered_run:
                continue
            normalized_lines.append("")
            continue

        m_num = re.match(r'^\s*(\d+)\.\s+(.*)$', stripped)
        if m_num:
            item_body = m_num.group(2).strip()
            if not in_numbered_run:
                next_number = 1
                in_numbered_run = True
            normalized_lines.append(f"{next_number}. {item_body}")
            next_number += 1
            continue

        in_numbered_run = False
        normalized_lines.append(stripped)

    text = "\n".join(normalized_lines)
    text = re.sub(r'\n{3,}', '\n\n', text).strip()
    return text


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
 

def build_ga_tag() -> str:
    if not GA_MEASUREMENT_ID:
        return ""

    return f"""
<script async src="https://www.googletagmanager.com/gtag/js?id={GA_MEASUREMENT_ID}"></script>
<script>
window.dataLayer = window.dataLayer || [];
function gtag(){{dataLayer.push(arguments);}}
gtag('js', new Date());
gtag('config', '{GA_MEASUREMENT_ID}');
</script>
""".strip()


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
 
        if is_valid_image_path(img_path):
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

    ga_tag = build_ga_tag()
 
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
{ga_tag}
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

    run_plan = build_run_plan(
        base_keywords,
        existing_titles[:],
        posts,
    )
    if not run_plan:
        log("MAIN", "No run plan available")
        return 0

    used_texts_raw = load_json(USED_TEXTS_JSON, {})
    used_texts = ensure_used_texts_schema(used_texts_raw)
    used_fps = set(used_texts.get("fingerprints") or [])

    made = 0

    for plan_item in run_plan:
        keyword = (plan_item.get("keyword") or "").strip()
        effective_cluster_name = plan_item.get("cluster_name") or ""
        post_type = plan_item.get("post_type") or "normal"
        current_pillar_slug = plan_item.get("pillar_slug") or ""
        effective_category = plan_item.get("category") or pick_category(
            keyword=keyword,
            cluster_name=effective_cluster_name,
            post_type=post_type,
        )

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
            cand_title = cand["title"]

            if post_semantically_too_close(keyword, cand_planning, posts):
                log("DUP", f"Semantic overlap detected for keyword='{keyword}'")
                continue

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
        for sec in sections:
            sec["body"] = format_generated_body(sec.get("body", ""))

        data["sections"] = sections
        data = trim_article_to_max_chars(data, MAX_CHARS)
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
                f"Low image count for slug='{slug}' found={real_image_count} required={MIN_REQUIRED_IMAGES}, keeping post anyway"
            )

        if visible_image_count < VISIBLE_MIN_IMAGES:
            log(
                "IMG",
                f"Low visible image count for slug='{slug}' visible={visible_image_count} required={VISIBLE_MIN_IMAGES}, keeping post anyway"
            )

        log(
            "IMG",
            f"slug='{slug}' real_image_count={real_image_count} visible_image_count={visible_image_count}"
        )

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
        return 0

    save_posts_index(posts)
    log("MAIN", f"Finished build_id={BUILD_ID} made={made}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
