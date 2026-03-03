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

IMG_COUNT = int(os.environ.get("IMG_COUNT", "3"))
HTTP_TIMEOUT = 25

IMAGE_PROVIDER = os.environ.get("IMAGE_PROVIDER", "wikimedia").strip().lower()
IMAGE_MODEL = os.environ.get("IMAGE_MODEL", "gpt-image-1").strip()

client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

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

def ensure_dirs(slug: str):
  POSTS_DIR.mkdir(parents=True, exist_ok=True)
  (ASSETS_POSTS_DIR / slug).mkdir(parents=True, exist_ok=True)

def esc(s: str) -> str:
  return html.escape(s or "", quote=True)

# -----------------------------------
# Wikimedia image fetch
# -----------------------------------
WIKI_API = "https://commons.wikimedia.org/w/api.php"

def wikimedia_search_image_urls(query: str, limit: int = 12) -> List[str]:
  params = {
    "action": "query",
    "format": "json",
    "origin": "*",
    "generator": "search",
    "gsrsearch": f"filetype:bitmap {query}",
    "gsrlimit": str(limit),
    "gsrnamespace": "6",
    "prop": "imageinfo",
    "iiprop": "url",
  }
  try:
    r = requests.get(WIKI_API, params=params, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    data = r.json()
    pages = (data.get("query") or {}).get("pages") or {}
    out = []
    for _, page in pages.items():
      infos = page.get("imageinfo") or []
      if infos:
        url = infos[0].get("url") or ""
        if url:
          out.append(url)
    random.shuffle(out)
    return out
  except Exception:
    return []

def download_image(url: str) -> Optional[bytes]:
  try:
    r = requests.get(url, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    ct = (r.headers.get("content-type") or "").lower()
    if "image" not in ct:
      return None
    return r.content
  except Exception:
    return None

# -----------------------------------
# OpenAI image
# -----------------------------------
def openai_generate_image_bytes(prompt: str) -> Optional[bytes]:
  if not client:
    return None
  try:
    res = client.images.generate(
      model=IMAGE_MODEL,
      prompt=prompt,
      size="1024x1024",
    )
    b64 = res.data[0].b64_json
    import base64
    return base64.b64decode(b64)
  except Exception:
    return None

# -----------------------------------
# Global image de-dup
# -----------------------------------
def load_used_images() -> Set[str]:
  arr = safe_read_json(USED_IMAGES_JSON, [])
  if isinstance(arr, list):
    return set([str(x) for x in arr if x])
  return set()

def save_used_images(s: Set[str]):
  safe_write_json(USED_IMAGES_JSON, sorted(list(s)))

def pick_unique_images_for_post(keyword: str, slug: str, count: int) -> List[str]:
  ensure_dirs(slug)

  used_global = load_used_images()
  used_in_post: Set[str] = set()

  saved_paths: List[str] = []
  attempts = 0

  candidates: List[str] = []
  if IMAGE_PROVIDER in ("wikimedia", "auto", "hybrid"):
    q = f"{keyword} abstract modern photo"
    candidates = wikimedia_search_image_urls(q, limit=40)

  while len(saved_paths) < count and attempts < 160:
    attempts += 1

    img_bytes = None
    h = None

    url = None
    while candidates:
      u = candidates.pop()
      if u in used_global:
        continue
      url = u
      break

    if url:
      b = download_image(url)
      if b:
        hh = sha256_bytes(b)
        if hh not in used_global and hh not in used_in_post:
          img_bytes = b
          h = hh

    if img_bytes is None:
      uniq = hashlib.sha1(f"{slug}-{len(saved_paths)}-{time.time()}-{random.random()}".encode()).hexdigest()[:10]
      prompt = (
        f"High quality realistic photo for a blog post about: {keyword}. "
        f"Clean composition, professional lighting, no text, no logos, no watermarks. "
        f"Unique variation token: {uniq}."
      )
      b = openai_generate_image_bytes(prompt)
      if not b:
        continue
      hh = sha256_bytes(b)
      if hh in used_global or hh in used_in_post:
        continue
      img_bytes = b
      h = hh

    idx = len(saved_paths) + 1
    out_file = ASSETS_POSTS_DIR / slug / f"{idx}.jpg"
    out_file.write_bytes(img_bytes)

    used_in_post.add(h)
    used_global.add(h)
    saved_paths.append(f"assets/posts/{slug}/{idx}.jpg")

  save_used_images(used_global)
  return saved_paths

# -----------------------------------
# Content generation
# -----------------------------------
def llm_generate_article(keyword: str) -> Dict[str, str]:
  if not client:
    title = keyword.title()
    desc = f"A practical guide about {keyword}."
    cat = "AI Tools"
    body = f"<h2>Overview</h2><p>{esc(desc)}</p>"
    return {"title": title, "description": desc, "category": cat, "body": body}

  sys = (
    "You write SEO-friendly helpful blog content for young professionals in the US and Europe. "
    "No fluff. Clear structure. Short paragraphs. Add useful steps and comparisons. "
    "Do not mention that you are an AI."
  )
  user = (
    f"Write one blog post about: {keyword}\n"
    "Output JSON with keys: title, description, category(one of: AI Tools, Make Money, Productivity, Reviews), "
    "body_html (HTML only, use <h2>, <p>, <ul><li>). "
    "Do not include outer <html>."
  )
  res = client.responses.create(
    model=MODEL,
    input=[
      {"role":"system","content":sys},
      {"role":"user","content":user},
    ],
  )
  txt = res.output_text.strip()
  try:
    data = json.loads(txt)
    return {
      "title": str(data.get("title","")).strip() or keyword.title(),
      "description": str(data.get("description","")).strip() or f"A practical guide about {keyword}.",
      "category": str(data.get("category","AI Tools")).strip(),
      "body": str(data.get("body_html","")).strip() or "<p></p>",
    }
  except Exception:
    title = keyword.title()
    desc = f"A practical guide about {keyword}."
    cat = "AI Tools"
    body = "<p></p>"
    return {"title": title, "description": desc, "category": cat, "body": body}

def build_post_html(site_name: str, title: str, description: str, category: str, date_iso: str, slug: str, images: List[str], body_html: str) -> str:
  hero_img = images[0] if images else ""
  extra_imgs = images[1:]

  if extra_imgs:
    parts = re.split(r"(<h2>.*?</h2>)", body_html, flags=re.S)
    out = []
    img_i = 0
    for chunk in parts:
      out.append(chunk)
      if img_i < len(extra_imgs) and chunk.startswith("<h2>"):
        out.append(f'<img src="../{esc(extra_imgs[img_i])}" alt="{esc(title)}" loading="lazy">')
        img_i += 1
    body_html = "".join(out)

  return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>{esc(title)} | {esc(site_name)}</title>
  <meta name="description" content="{esc(description)}" />
  <link rel="stylesheet" href="../style.css?v=999" />
</head>
<body>

<header class="topbar">
  <div class="container topbar-inner">
    <a class="brand" href="../index.html" aria-label="{esc(site_name)} Home">
      <span class="mark" aria-hidden="true"></span>
      <span>{esc(site_name)}</span>
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
      <div class="post-meta">
        <span>{esc(category)}</span>
        <span>•</span>
        <span>Updated: {esc(date_iso)}</span>
      </div>
    </header>

    {"<div class='post-hero'><img src='../"+esc(hero_img)+"' alt='"+esc(title)+"' loading='eager'></div>" if hero_img else ""}

    <article class="post-content">
      {body_html}
    </article>

  </div>
</main>

<footer class="footer">
  <div class="container">
    <div>© 2026 {esc(site_name)}</div>
    <div class="footer-links">
      <a href="../privacy.html">Privacy</a>
      <a href="../about.html">About</a>
      <a href="../contact.html">Contact</a>
    </div>
  </div>
</footer>

</body>
</html>
"""

def add_post_to_index(posts: List[dict], post_obj: dict) -> List[dict]:
  posts.append(post_obj)
  posts = [p for p in posts if isinstance(p, dict) and p.get("slug")]
  posts.sort(key=lambda x: str(x.get("date","")), reverse=True)
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

def main():
  POSTS_DIR.mkdir(parents=True, exist_ok=True)
  ASSETS_POSTS_DIR.mkdir(parents=True, exist_ok=True)

  posts = safe_read_json(POSTS_JSON, [])
  if not isinstance(posts, list):
    posts = []

  keywords = load_keywords()
  if not keywords:
    raise SystemExit("keywords.json has no keywords")

  existing_slugs = set([p.get("slug") for p in posts if isinstance(p, dict)])

  made = 0
  tries = 0
  while made < POSTS_PER_RUN and tries < POSTS_PER_RUN * 10:
    tries += 1
    keyword = random.choice(keywords).strip()
    if not keyword:
      continue

    slug = slugify(keyword)[:80]
    if not slug or slug in existing_slugs:
      continue

    art = llm_generate_article(keyword)
    title = art["title"]
    description = art["description"]
    category = art["category"]
    body = art["body"]
    date_iso = now_iso()

    images = pick_unique_images_for_post(keyword, slug, IMG_COUNT)

    html_doc = build_post_html(SITE_NAME, title, description, category, date_iso, slug, images, body)
    out_path = POSTS_DIR / f"{slug}.html"
    out_path.write_text(html_doc, encoding="utf-8")

    post_obj = {
      "title": title,
      "description": description,
      "category": category,
      "date": date_iso,
      "slug": slug,
      "thumbnail": images[0] if images else f"assets/posts/{slug}/1.jpg",
      "image": images[0] if images else f"assets/posts/{slug}/1.jpg",
      "url": f"posts/{slug}.html",
    }

    posts = add_post_to_index(posts, post_obj)
    safe_write_json(POSTS_JSON, posts)

    existing_slugs.add(slug)
    made += 1
    print(f"Generated: {slug}")

if __name__ == "__main__":
  main()
