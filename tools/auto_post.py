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

# 🔥 5 images → text will be 6 equal sections
IMG_COUNT = int(os.environ.get("IMG_COUNT", "5"))
IMAGE_MODEL = os.environ.get("IMAGE_MODEL", "gpt-image-1").strip()

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

# =========================================================
# IMAGE SYSTEM (GLOBAL DUPLICATE PREVENTION)
# =========================================================
def load_used_hashes() -> Set[str]:
    return set(safe_read_json(USED_IMAGES_JSON, []))

def save_used_hashes(s: Set[str]):
    safe_write_json(USED_IMAGES_JSON, list(s))

def generate_unique_image(keyword: str, slug: str) -> bytes:
    used = load_used_hashes()

    while True:
        uniq = hashlib.sha1(
            f"{slug}-{time.time()}-{random.random()}".encode()
        ).hexdigest()[:10]

        prompt = (
            f"High quality realistic professional photo about {keyword}. "
            f"Clean composition. No text. No logos. No watermark. "
            f"Unique variation token {uniq}."
        )

        result = client.images.generate(
            model=IMAGE_MODEL,
            prompt=prompt,
            size="1024x1024"
        )

        import base64
        img = base64.b64decode(result.data[0].b64_json)
        h = sha256_bytes(img)

        if h not in used:
            used.add(h)
            save_used_hashes(used)
            return img

# =========================================================
# ARTICLE GENERATION (FORCED EVEN DISTRIBUTION)
# =========================================================
def generate_article(keyword: str, sections: int) -> Dict[str, str]:

    system_prompt = (
        "You are a professional SEO blog writer for US and European audiences. "
        "The article must be evenly distributed across all sections. "
        "Every section should feel similar length. "
        "No fluff. Clear structure."
    )

    user_prompt = (
        f"Topic: {keyword}\n"
        f"Write exactly {sections} sections.\n"
        "Each section must start with one <h2>.\n"
        "Each section must contain 2-4 short <p> paragraphs.\n"
        "All sections must be similar length.\n"
        "Return JSON with keys: title, description, category, body_html.\n"
        "Category must be one of: AI Tools, Make Money, Productivity, Reviews.\n"
        "Do not include outer <html>.\n"
    )

    response = client.responses.create(
        model=MODEL,
        input=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
    )

    data = json.loads(response.output_text.strip())

    return {
        "title": data["title"],
        "description": data["description"],
        "category": data["category"],
        "body_html": data["body_html"],
    }

# =========================================================
# HTML BUILD (TEXT + IMAGES INTERLEAVED EVENLY)
# =========================================================
def build_post_html(
    title: str,
    description: str,
    category: str,
    date_iso: str,
    slug: str,
    body_html: str,
    sections: int,
):

    parts = re.split(r"(?=<h2>)", body_html.strip())
    parts = [p for p in parts if p.strip()]
    parts = parts[:sections]

    final = []

    for i in range(sections):
        final.append(parts[i])

        if i < IMG_COUNT:
            final.append(
                f'<div class="post-img">'
                f'<img src="../assets/posts/{slug}/{i+1}.jpg" loading="lazy">'
                f'</div>'
            )

    body = "".join(final)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{esc(title)} | {SITE_NAME}</title>
<meta name="description" content="{esc(description)}">
<link rel="stylesheet" href="../style.css?v=999">
</head>
<body>

<header class="topbar">
<div class="container topbar-inner">
<a class="brand" href="../index.html">
<span class="mark"></span>
<span>{SITE_NAME}</span>
</a>
<nav class="nav">
<a href="../index.html">Home</a>
<a href="../about.html">About</a>
<a href="../contact.html">Contact</a>
</nav>
</div>
</header>

<main class="container post-page">
<div class="post-shell">

<header class="post-header">
<div class="kicker">{category}</div>
<h1 class="post-h1">{title}</h1>
<div class="post-meta">Updated: {date_iso}</div>
</header>

<article class="post-content">
{body}
</article>

</div>
</main>

<footer class="footer">
<div class="container">© 2026 {SITE_NAME}</div>
</footer>

</body>
</html>
"""

# =========================================================
# MAIN
# =========================================================
def main():
    POSTS_DIR.mkdir(exist_ok=True)
    ASSETS_POSTS_DIR.mkdir(exist_ok=True)

    posts = safe_read_json(POSTS_JSON, [])
    keywords = safe_read_json(KEYWORDS_JSON, [])

    existing_slugs = {p.get("slug") for p in posts}

    created = 0

    for keyword in keywords:
        if created >= POSTS_PER_RUN:
            break

        slug = slugify(keyword)
        if slug in existing_slugs:
            continue

        date_iso = now_iso()
        sections = IMG_COUNT + 1

        article = generate_article(keyword, sections)

        post_dir = ASSETS_POSTS_DIR / slug
        post_dir.mkdir(parents=True, exist_ok=True)

        for i in range(IMG_COUNT):
            img = generate_unique_image(keyword, slug)
            (post_dir / f"{i+1}.jpg").write_bytes(img)

        html_doc = build_post_html(
            article["title"],
            article["description"],
            article["category"],
            date_iso,
            slug,
            article["body_html"],
            sections,
        )

        (POSTS_DIR / f"{slug}.html").write_text(html_doc, encoding="utf-8")

        posts.append({
            "title": article["title"],
            "description": article["description"],
            "category": article["category"],
            "date": date_iso,
            "slug": slug,
            "thumbnail": f"assets/posts/{slug}/1.jpg",
            "url": f"posts/{slug}.html"
        })

        created += 1

    posts.sort(key=lambda x: x["date"], reverse=True)
    safe_write_json(POSTS_JSON, posts)

if __name__ == "__main__":
    main()
