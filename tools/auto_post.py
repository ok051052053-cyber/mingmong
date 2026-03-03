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

IMG_COUNT = 5  # 🔥 5장이면 텍스트는 6등분됨
IMAGE_MODEL = "gpt-image-1"

client = OpenAI(api_key=OPENAI_API_KEY)

# -----------------------------
def now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")

def safe_read_json(path: Path, default):
    try:
        if not path.exists():
            return default
        return json.loads(path.read_text(encoding="utf-8"))
    except:
        return default

def safe_write_json(path: Path, data):
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

def esc(s: str) -> str:
    return html.escape(s or "", quote=True)

# -----------------------------
# IMAGE UNIQUE SYSTEM
# -----------------------------
def load_used_images() -> Set[str]:
    return set(safe_read_json(USED_IMAGES_JSON, []))

def save_used_images(s: Set[str]):
    safe_write_json(USED_IMAGES_JSON, list(s))

def generate_unique_image(keyword: str, slug: str) -> bytes:
    used = load_used_images()

    while True:
        uniq = hashlib.sha1(f"{slug}-{time.time()}-{random.random()}".encode()).hexdigest()[:10]

        prompt = (
            f"High quality realistic professional photo about {keyword}. "
            f"No text. No logo. Clean composition. Unique variation token {uniq}."
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
            save_used_images(used)
            return img

# -----------------------------
# TEXT GENERATION
# -----------------------------
def generate_article(keyword: str, blocks: int):
    sys = (
        "You are a professional SEO writer. "
        "The article must be evenly distributed across sections. "
        "Each section must be similar length."
    )

    user = (
        f"Topic: {keyword}\n"
        f"Write exactly {blocks} sections.\n"
        "Each section must start with <h2>.\n"
        "Each section must contain 2-4 short <p> paragraphs.\n"
        "Return JSON with keys: title, description, category, body_html.\n"
        "Allowed categories: AI Tools, Make Money, Productivity, Reviews.\n"
    )

    res = client.responses.create(
        model=MODEL,
        input=[
            {"role": "system", "content": sys},
            {"role": "user", "content": user},
        ],
    )

    data = json.loads(res.output_text.strip())

    return data

# -----------------------------
# BUILD HTML
# -----------------------------
def build_post_html(site_name, title, description, category, date_iso, slug, images, body_html, blocks):

    sections = re.split(r"(?=<h2>)", body_html.strip())
    sections = [s for s in sections if s.strip()]
    sections = sections[:blocks]

    composed = []

    for i in range(blocks):
        composed.append(sections[i])
        if i < IMG_COUNT:
            composed.append(
                f'<div class="post-img"><img src="../assets/posts/{slug}/{i+1}.jpg" loading="lazy"></div>'
            )

    final_body = "".join(composed)

    return f"""<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{esc(title)} | {site_name}</title>
<link rel="stylesheet" href="../style.css?v=999">
</head>
<body>

<header class="topbar">
<div class="container topbar-inner">
<a class="brand" href="../index.html"><span class="mark"></span><span>{site_name}</span></a>
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
{final_body}
</article>

</div>
</main>

<footer class="footer">
<div class="container">© 2026 {site_name}</div>
</footer>

</body>
</html>
"""

# -----------------------------
def main():
    POSTS_DIR.mkdir(exist_ok=True)
    ASSETS_POSTS_DIR.mkdir(exist_ok=True)

    posts = safe_read_json(POSTS_JSON, [])
    keywords = safe_read_json(KEYWORDS_JSON, [])

    for keyword in keywords[:POSTS_PER_RUN]:

        slug = slugify(keyword)
        date_iso = now_iso()

        blocks = IMG_COUNT + 1
        article = generate_article(keyword, blocks)

        post_dir = ASSETS_POSTS_DIR / slug
        post_dir.mkdir(parents=True, exist_ok=True)

        images = []
        for i in range(IMG_COUNT):
            img = generate_unique_image(keyword, slug)
            path = post_dir / f"{i+1}.jpg"
            path.write_bytes(img)
            images.append(path)

        html_doc = build_post_html(
            SITE_NAME,
            article["title"],
            article["description"],
            article["category"],
            date_iso,
            slug,
            images,
            article["body_html"],
            blocks
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

    posts.sort(key=lambda x: x["date"], reverse=True)
    safe_write_json(POSTS_JSON, posts)

if __name__ == "__main__":
    main()
