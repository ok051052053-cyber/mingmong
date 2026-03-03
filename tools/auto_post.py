import os
import re
import json
import time
import html
import random
import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set

import requests
from slugify import slugify
from openai import OpenAI

# -----------------------------
# Paths
# -----------------------------
ROOT = Path(__file__).resolve().parents[1]
POSTS_DIR = ROOT / "posts"
ASSETS_POSTS_DIR = ROOT / "assets" / "posts"
POSTS_JSON = ROOT / "posts.json"
KEYWORDS_JSON = ROOT / "keywords.json"
USED_IMAGES_JSON = ROOT / "used_images.json"

# -----------------------------
# Config
# -----------------------------
SITE_NAME = os.environ.get("SITE_NAME", "MingMong").strip()
POSTS_PER_RUN = int(os.environ.get("POSTS_PER_RUN", "3"))
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "").strip()
MODEL = os.environ.get("MODEL", "gpt-4o-mini").strip()

# 5 images -> 6 text sections
IMG_COUNT = int(os.environ.get("IMG_COUNT", "5"))
IMAGE_MODEL = os.environ.get("IMAGE_MODEL", "gpt-image-1").strip()

# SEO
SITE_URL = os.environ.get("SITE_URL", "").strip().rstrip("/")  # 예: https://yourdomain.com
MIN_BODY_CHARS = int(os.environ.get("MIN_BODY_CHARS", "2500"))
MAX_REGEN_TRIES = int(os.environ.get("MAX_REGEN_TRIES", "4"))

client = OpenAI(api_key=OPENAI_API_KEY)

# -----------------------------
def now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")

def safe_read_json(path: Path, default):
    try:
        if not path.exists():
            return default
        txt = path.read_text(encoding="utf-8").strip()
        if not txt:
            return default
        return json.loads(txt)
    except Exception:
        return default

def safe_write_json(path: Path, data):
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

def esc(s: str) -> str:
    return html.escape(s or "", quote=True)

def strip_tags(s: str) -> str:
    s = re.sub(r"<[^>]+>", " ", s or "")
    s = re.sub(r"\s+", " ", s).strip()
    return s

def clamp_desc(s: str, n: int = 155) -> str:
    s = strip_tags(s)
    if len(s) <= n:
        return s
    return s[:n].rsplit(" ", 1)[0].strip() or s[:n].strip()

def normalize_ws(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

# =========================================================
# IMAGE SYSTEM (GLOBAL DUPLICATE PREVENTION)
# =========================================================
def load_used_hashes() -> Set[str]:
    arr = safe_read_json(USED_IMAGES_JSON, [])
    if isinstance(arr, list):
        return set(str(x) for x in arr if x)
    return set()

def save_used_hashes(s: Set[str]):
    safe_write_json(USED_IMAGES_JSON, sorted(list(s)))

def generate_unique_image_bytes(keyword: str, slug: str) -> bytes:
    used = load_used_hashes()

    while True:
        uniq = hashlib.sha1(
            f"{slug}-{time.time()}-{random.random()}".encode()
        ).hexdigest()[:10]

        prompt = (
            f"High quality realistic professional photo for a blog post about {keyword}. "
            f"Clean composition. Natural lighting. No text. No logos. No watermark. "
            f"Unique variation token {uniq}."
        )

        result = client.images.generate(
            model=IMAGE_MODEL,
            prompt=prompt,
            size="1024x1024",
        )

        import base64
        img = base64.b64decode(result.data[0].b64_json)
        h = sha256_bytes(img)

        if h not in used:
            used.add(h)
            save_used_hashes(used)
            return img

# =========================================================
# ARTICLE GENERATION (EVEN DISTRIBUTION + MIN CHARS)
# =========================================================
ALLOWED_CATS = ["AI Tools", "Make Money", "Productivity", "Reviews"]

def generate_article(keyword: str, sections: int, min_chars: int) -> Dict[str, str]:
    system_prompt = (
        "You are a premium SEO blog writer for US and European audiences. "
        "Write in English only. "
        "Use short sentences. Avoid filler. "
        "Make every section similar length. "
        "Do not mention that you are an AI."
    )

    user_prompt = (
        f"Topic: {keyword}\n"
        f"Write exactly {sections} sections.\n"
        "Rules:\n"
        "- Each section must start with one <h2>\n"
        "- Each section must include 3 to 5 <p> paragraphs\n"
        "- Keep sections similar length\n"
        f"- Total plain text length (without HTML tags) must be at least {min_chars} characters\n"
        "- Add one simple comparison table using <table> <thead> <tbody> <tr> <th> <td>\n"
        "- Add one short checklist as <ul><li>\n"
        f"- Category must be one of: {', '.join(ALLOWED_CATS)}\n"
        "Return JSON only with keys:\n"
        "- title\n"
        "- description\n"
        "- category\n"
        "- body_html\n"
        "Do not include outer <html>.\n"
    )

    res = client.responses.create(
        model=MODEL,
        input=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
    )

    raw = (res.output_text or "").strip()
    data = json.loads(raw)

    title = normalize_ws(str(data.get("title", "")).strip())
    description = normalize_ws(str(data.get("description", "")).strip())
    category = normalize_ws(str(data.get("category", "")).strip())
    body_html = str(data.get("body_html", "")).strip()

    if category not in ALLOWED_CATS:
        category = "Productivity"

    if not title:
        title = keyword.strip().title()

    if not description:
        description = f"A practical guide to {keyword}."

    description = clamp_desc(description, 155)

    return {
        "title": title,
        "description": description,
        "category": category,
        "body_html": body_html,
    }

def generate_article_with_min(keyword: str, sections: int) -> Dict[str, str]:
    last = None
    for _ in range(MAX_REGEN_TRIES):
        art = generate_article(keyword, sections, MIN_BODY_CHARS)
        plain = strip_tags(art["body_html"])
        last = art
        if len(plain) >= MIN_BODY_CHARS:
            return art
        time.sleep(0.2)
    return last

# =========================================================
# SEO META + HTML BUILD
# =========================================================
def canonical_url(slug: str) -> str:
    if not SITE_URL:
        return ""
    return f"{SITE_URL}/posts/{slug}.html"

def build_json_ld(site_url: str, slug: str, title: str, description: str, date_iso: str, og_image_abs: str) -> str:
    # Article schema
    obj = {
        "@context": "https://schema.org",
        "@type": "Article",
        "headline": title,
        "description": description,
        "datePublished": date_iso,
        "dateModified": date_iso,
        "mainEntityOfPage": (f"{site_url}/posts/{slug}.html" if site_url else ""),
        "image": [og_image_abs] if og_image_abs else [],
        "author": {"@type": "Organization", "name": SITE_NAME},
        "publisher": {"@type": "Organization", "name": SITE_NAME},
    }
    return json.dumps(obj, ensure_ascii=False)

def build_post_html(
    title: str,
    description: str,
    category: str,
    date_iso: str,
    slug: str,
    body_html: str,
    sections: int,
) -> str:
    # Split into exact sections by <h2>
    parts = re.split(r"(?=<h2>)", body_html.strip())
    parts = [p for p in parts if p.strip()]

    # If model returned fewer, pad with empty but keep structure stable
    if len(parts) < sections:
        missing = sections - len(parts)
        parts += [f"<h2>Notes</h2><p></p>" for _ in range(missing)]

    parts = parts[:sections]

    # Interleave images after each section except the last
    final = []
    for i in range(sections):
        final.append(parts[i])
        if i < IMG_COUNT:
            final.append(
                f'<div class="post-img">'
                f'<img src="../assets/posts/{slug}/{i+1}.jpg" alt="{esc(title)} image {i+1}" loading="lazy">'
                f'</div>'
            )
    body = "".join(final)

    # SEO URLs
    can = canonical_url(slug)
    og_img_rel = f"assets/posts/{slug}/1.jpg"
    og_img_abs = f"{SITE_URL}/{og_img_rel}" if SITE_URL else ""

    ld = build_json_ld(SITE_URL, slug, title, description, date_iso, og_img_abs)

    canonical_tag = f'<link rel="canonical" href="{esc(can)}" />' if can else ""
    og_url_tag = f'<meta property="og:url" content="{esc(can)}" />' if can else ""

    og_img_tag = ""
    tw_img_tag = ""
    if og_img_abs:
        og_img_tag = f'<meta property="og:image" content="{esc(og_img_abs)}" />'
        tw_img_tag = f'<meta name="twitter:image" content="{esc(og_img_abs)}" />'

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8" />
<meta name="viewport" content="width=device-width, initial-scale=1.0" />
<title>{esc(title)} | {esc(SITE_NAME)}</title>
<meta name="description" content="{esc(description)}" />
<meta name="robots" content="index, follow" />
{canonical_tag}

<meta property="og:type" content="article" />
<meta property="og:site_name" content="{esc(SITE_NAME)}" />
<meta property="og:title" content="{esc(title)}" />
<meta property="og:description" content="{esc(description)}" />
{og_url_tag}
{og_img_tag}

<meta name="twitter:card" content="summary_large_image" />
<meta name="twitter:title" content="{esc(title)}" />
<meta name="twitter:description" content="{esc(description)}" />
{tw_img_tag}

<script type="application/ld+json">{ld}</script>

<link rel="stylesheet" href="../style.css?v=999" />
</head>

<body>

<header class="topbar">
  <div class="container topbar-inner">
    <a class="brand" href="../index.html" aria-label="{esc(SITE_NAME)} Home">
      <span class="mark" aria-hidden="true"></span>
      <span>{esc(SITE_NAME)}</span>
    </a>
    <nav class="nav" aria-label="Primary">
      <a href="../index.html">Home</a>
      <a href="../about.html">About</a>
      <a href="../contact.html">Contact</a>
    </nav>
  </div>
</header>

<main class="container post-page">
  <div class="post-shell">

    <header class="post-header">
      <div class="kicker">{esc(category)}</div>
      <h1 class="post-h1">{esc(title)}</h1>
      <div class="post-meta">Updated: {esc(date_iso)}</div>
    </header>

    <article class="post-content">
      {body}
    </article>

  </div>
</main>

<footer class="footer">
  <div class="container">
    <div>© 2026 {esc(SITE_NAME)}</div>
  </div>
</footer>

</body>
</html>
"""

# =========================================================
# POSTS JSON
# =========================================================
def add_post(posts: List[dict], obj: dict) -> List[dict]:
    posts.append(obj)
    posts = [p for p in posts if isinstance(p, dict) and p.get("slug")]
    posts.sort(key=lambda x: str(x.get("date", "")), reverse=True)
    return posts

def load_keywords() -> List[str]:
    data = safe_read_json(KEYWORDS_JSON, [])
    out = []
    if isinstance(data, list):
        for item in data:
            if isinstance(item, str):
                k = item.strip()
                if k:
                    out.append(k)
            elif isinstance(item, dict) and item.get("keyword"):
                k = str(item.get("keyword")).strip()
                if k:
                    out.append(k)
    return out

# =========================================================
# MAIN
# =========================================================
def main():
    POSTS_DIR.mkdir(parents=True, exist_ok=True)
    ASSETS_POSTS_DIR.mkdir(parents=True, exist_ok=True)

    posts = safe_read_json(POSTS_JSON, [])
    if not isinstance(posts, list):
        posts = []

    keywords = load_keywords()
    if not keywords:
        raise SystemExit("keywords.json has no keywords")

    existing_slugs = {p.get("slug") for p in posts if isinstance(p, dict) and p.get("slug")}

    created = 0
    tries = 0
    while created < POSTS_PER_RUN and tries < POSTS_PER_RUN * 10:
        tries += 1

        keyword = random.choice(keywords).strip()
        if not keyword:
            continue

        # slug from keyword is ok but can collide
        slug = slugify(keyword)[:80].strip("-")
        if not slug or slug in existing_slugs:
            continue

        date_iso = now_iso()
        sections = IMG_COUNT + 1

        # 1) article with min 2500 chars
        art = generate_article_with_min(keyword, sections)

        # safety clamp description
        art["description"] = clamp_desc(art["description"], 155)

        # 2) images (unique global)
        post_dir = ASSETS_POSTS_DIR / slug
        post_dir.mkdir(parents=True, exist_ok=True)

        for i in range(IMG_COUNT):
            img = generate_unique_image_bytes(keyword, slug)
            (post_dir / f"{i+1}.jpg").write_bytes(img)

        # 3) html
        html_doc = build_post_html(
            title=art["title"],
            description=art["description"],
            category=art["category"],
            date_iso=date_iso,
            slug=slug,
            body_html=art["body_html"],
            sections=sections,
        )
        (POSTS_DIR / f"{slug}.html").write_text(html_doc, encoding="utf-8")

        # 4) posts.json update
        post_obj = {
            "title": art["title"],
            "description": art["description"],
            "category": art["category"],
            "date": date_iso,
            "slug": slug,
            "thumbnail": f"assets/posts/{slug}/1.jpg",
            "url": f"posts/{slug}.html",
            "keyword": keyword,
        }
        posts = add_post(posts, post_obj)
        safe_write_json(POSTS_JSON, posts)

        existing_slugs.add(slug)
        created += 1
        print("Generated:", slug)

    if created == 0:
        raise SystemExit("No posts generated")

if __name__ == "__main__":
    main()
