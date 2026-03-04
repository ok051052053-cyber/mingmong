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
SITE_URL = os.environ.get("SITE_URL", "https://mingmonglife.com").strip().rstrip("/")
POSTS_PER_RUN = int(os.environ.get("POSTS_PER_RUN", "2"))

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "").strip()
MODEL = os.environ.get("MODEL", "gpt-4o-mini").strip()

IMG_COUNT = int(os.environ.get("IMG_COUNT", "4"))  # 최소 4장
MIN_CHARS = int(os.environ.get("MIN_CHARS", "2500"))

HTTP_TIMEOUT = 25

# 이미지: Wikimedia 고정. 실패하면 글 생성 중단(저품질/빈사진 방지)
IMAGE_PROVIDER = os.environ.get("IMAGE_PROVIDER", "wikimedia").strip().lower()

# -----------------------------
# Utils
# -----------------------------
def now_iso_datetime() -> str:
  return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

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

def esc(s: str) -> str:
  return html.escape(s or "", quote=True)

def strip_tags(s: str) -> str:
  s = re.sub(r"<script.*?>.*?</script>", "", s, flags=re.S | re.I)
  s = re.sub(r"<style.*?>.*?</style>", "", s, flags=re.S | re.I)
  s = re.sub(r"<[^>]+>", "", s)
  return html.unescape(s).strip()

def sha256_bytes(b: bytes) -> str:
  return hashlib.sha256(b).hexdigest()

def ensure_dirs(slug: str):
  POSTS_DIR.mkdir(parents=True, exist_ok=True)
  (ASSETS_POSTS_DIR / slug).mkdir(parents=True, exist_ok=True)

def normalize_img_path(pth: str) -> str:
  s = (pth or "").strip()
  if not s:
    return s
  if s.lower().endswith(".svg"):
    return s[:-4] + ".jpg"
  return s

# -----------------------------
# Wikimedia image fetch
# -----------------------------
WIKI_API = "https://commons.wikimedia.org/w/api.php"

def wikimedia_search_image_urls(query: str, limit: int = 40) -> List[str]:
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

# -----------------------------
# Global image de-dup
# -----------------------------
def load_used_images() -> Set[str]:
  arr = safe_read_json(USED_IMAGES_JSON, [])
  if isinstance(arr, list):
    return set([str(x) for x in arr if x])
  return set()

def save_used_images(s: Set[str]):
  safe_write_json(USED_IMAGES_JSON, sorted(list(s)))

def pick_unique_wikimedia_images(keyword: str, slug: str, count: int) -> List[str]:
  """
  Wikimedia만 사용
  실패하면 빈 리스트 반환 -> 글 생성 중단
  """
  if IMAGE_PROVIDER not in ("wikimedia", "auto", "hybrid"):
    return []

  ensure_dirs(slug)

  used_global = load_used_images()
  used_in_post: Set[str] = set()
  saved_paths: List[str] = []

  # 검색 쿼리 폭 넓힘
  q = f'{keyword} photo high quality'
  candidates = wikimedia_search_image_urls(q, limit=80)

  attempts = 0
  while len(saved_paths) < count and attempts < 250:
    attempts += 1
    if not candidates:
      break

    url = candidates.pop()
    b = download_image(url)
    if not b:
      continue

    h = sha256_bytes(b)
    if h in used_global or h in used_in_post:
      continue

    idx = len(saved_paths) + 1
    out_file = ASSETS_POSTS_DIR / slug / f"{idx}.jpg"
    out_file.write_bytes(b)

    used_in_post.add(h)
    used_global.add(h)
    saved_paths.append(f"assets/posts/{slug}/{idx}.jpg")

  save_used_images(used_global)

  # count 미달이면 실패 처리 (저품질/빈사진 방지)
  if len(saved_paths) < count:
    return []

  return saved_paths

# -----------------------------
# OpenAI text generation
# -----------------------------
def openai_client():
  if not OPENAI_API_KEY:
    return None
  try:
    # v1 SDK
    from openai import OpenAI
    return OpenAI(api_key=OPENAI_API_KEY)
  except Exception:
    return None

def llm_generate_article(keyword: str) -> Dict[str, str]:
  """
  title, description, category, body_html
  최소 글자수 강제
  """
  client = openai_client()
  if not client:
    title = keyword.title()
    desc = f"A practical guide about {keyword}."
    cat = "AI Tools"
    body = (
      f"<h2>TL;DR</h2><ul><li>Clear steps</li><li>Real examples</li><li>Common mistakes</li></ul>"
      f"<h2>Overview</h2><p>{esc(desc)}</p>"
      f"<h2>Step by step</h2><p>{esc(desc)} {esc(desc)} {esc(desc)}</p>"
      f"<h2>Checklist</h2><ul><li>Do this</li><li>Then this</li><li>Measure results</li></ul>"
    )
    while len(strip_tags(body)) < MIN_CHARS:
      body += f"<p>{esc(desc)} {esc(desc)} {esc(desc)}</p>"
    return {"title": title, "description": desc, "category": cat, "body": body}

  sys = (
    "Write deep helpful SEO blog content for US and EU readers. "
    "No fluff. Concrete steps. Comparisons. Pitfalls. FAQs. "
    "Use short paragraphs. Natural English. "
    "Do not mention AI."
  )
  user = (
    f"Topic: {keyword}\n"
    "Return JSON with keys: title, description, category(one of: AI Tools, Make Money, Productivity, Reviews), body_html.\n"
    "body_html must use only <h2>, <p>, <ul><li>.\n"
    f"Visible text length must be at least {MIN_CHARS} characters.\n"
    "Include sections in this order:\n"
    "1) TL;DR (bullets)\n"
    "2) Who this is for\n"
    "3) Key ideas\n"
    "4) Step by step\n"
    "5) Mistakes to avoid\n"
    "6) FAQ\n"
  )

  # v1 SDK: chat.completions 사용 (responses 안 쓰기)
  res = client.chat.completions.create(
    model=MODEL,
    messages=[
      {"role": "system", "content": sys},
      {"role": "user", "content": user},
    ],
    temperature=0.7,
  )
  txt = (res.choices[0].message.content or "").strip()

  try:
    data = json.loads(txt)
  except Exception:
    data = {}

  title = str(data.get("title", "")).strip() or keyword.title()
  description = str(data.get("description", "")).strip() or f"A practical guide about {keyword}."
  category = str(data.get("category", "AI Tools")).strip() or "AI Tools"
  body = str(data.get("body_html", "")).strip() or "<p></p>"

  # 최종 패딩
  while len(strip_tags(body)) < MIN_CHARS:
    body += f"<p>{esc(description)} {esc(description)} {esc(description)}</p>"

  return {"title": title, "description": description, "category": category, "body": body}

# -----------------------------
# Inject images evenly
# -----------------------------
def inject_images_evenly(body_html: str, images: List[str], title: str) -> str:
  extras = images[1:] if len(images) > 1 else []
  if not extras:
    return body_html

  units = re.split(r"(?i)(</p>\s*|</ul>\s*|</h2>\s*)", body_html)
  chunks: List[str] = []
  buf = ""
  for part in units:
    buf += part
    if re.search(r"(?i)</p>\s*$|</ul>\s*$|</h2>\s*$", buf.strip()):
      chunks.append(buf)
      buf = ""
  if buf.strip():
    chunks.append(buf)

  n = max(len(chunks), 1)
  m = len(extras)

  positions = []
  for i in range(1, m + 1):
    pos = round(i * n / (m + 1))
    pos = min(max(pos, 1), n - 1)
    positions.append(pos)

  out: List[str] = []
  img_i = 0
  for idx, c in enumerate(chunks):
    out.append(c)
    if img_i < m and idx in positions:
      out.append(f'<img src="../{esc(extras[img_i])}" alt="{esc(title)}" loading="lazy">')
      img_i += 1

  while img_i < m:
    out.append(f'<img src="../{esc(extras[img_i])}" alt="{esc(title)}" loading="lazy">')
    img_i += 1

  return "".join(out)

# -----------------------------
# Build post html (템플릿 1번 고정)
# -----------------------------
def build_post_html(site_name: str, title: str, description: str, category: str, date_iso: str, slug: str, images: List[str], body_html: str) -> str:
  hero_img = images[0] if images else ""
  canonical = f"{SITE_URL}/posts/{slug}.html"

  body_html = inject_images_evenly(body_html, images, title)

  json_ld = {
    "@context": "https://schema.org",
    "@type": "BlogPosting",
    "headline": title,
    "description": description,
    "datePublished": date_iso,
    "dateModified": date_iso,
    "author": {"@type": "Organization", "name": site_name},
    "publisher": {"@type": "Organization", "name": site_name},
    "mainEntityOfPage": canonical,
  }
  if hero_img:
    json_ld["image"] = f"{SITE_URL}/{hero_img}"

  og_image = f"{SITE_URL}/{hero_img}" if hero_img else f"{SITE_URL}/assets/og-default.jpg"

  # 카테고리 카드(사이드바)
  cats = [
    ("AI Tools", "🤖", "ChatGPT, Claude, Notion AI, automation", "category.html?cat=AI%20Tools"),
    ("Make Money", "💸", "Side hustles, freelancing, remote income", "category.html?cat=Make%20Money"),
    ("Productivity", "⚡", "Workflows, systems, checklists", "category.html?cat=Productivity"),
    ("Reviews", "🧾", "Pricing, comparisons, alternatives", "category.html?cat=Reviews"),
  ]

  cat_html = []
  for name, ico, sub, href in cats:
    cat_html.append(
      f'<a class="catitem" href="../{esc(href)}">'
      f'  <span class="caticon">{esc(ico)}</span>'
      f'  <span class="cattext"><span class="catname">{esc(name)}</span><span class="catsub">{esc(sub)}</span></span>'
      f'</a>'
    )

  return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>{esc(title)} | {esc(site_name)}</title>
  <meta name="description" content="{esc(description)}" />
  <link rel="canonical" href="{esc(canonical)}" />

  <meta property="og:type" content="article" />
  <meta property="og:site_name" content="{esc(site_name)}" />
  <meta property="og:title" content="{esc(title)}" />
  <meta property="og:description" content="{esc(description)}" />
  <meta property="og:url" content="{esc(canonical)}" />
  <meta property="og:image" content="{esc(og_image)}" />

  <meta name="twitter:card" content="summary_large_image" />
  <meta name="twitter:title" content="{esc(title)}" />
  <meta name="twitter:description" content="{esc(description)}" />
  <meta name="twitter:image" content="{esc(og_image)}" />

  <link rel="stylesheet" href="../style.css?v=2000" />
  <script type="application/ld+json">{json.dumps(json_ld, ensure_ascii=False)}</script>
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

    <div class="post-main">
      <header class="post-header">
        <div class="kicker">{esc(category)}</div>
        <h1 class="post-h1">{esc(title)}</h1>
        <div class="post-meta">
          <span>{esc(category)}</span>
          <span>•</span>
          <span>Updated: {esc(date_iso[:10])}</span>
        </div>
      </header>

      {"<div class='post-hero'><img src='../"+esc(hero_img)+"' alt='"+esc(title)+"' loading='eager'></div>" if hero_img else ""}

      <article class="post-content">
        {body_html}
      </article>
    </div>

    <aside class="post-aside">
      <div class="sidecard">
        <h3>Browse by focus</h3>
        <div class="catlist">
          {''.join(cat_html)}
        </div>
      </div>
    </aside>

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

  def parse_dt(x: dict) -> float:
    d = str(x.get("date", ""))
    try:
      return datetime.fromisoformat(d.replace("Z", "+00:00")).timestamp()
    except Exception:
      return 0.0

  posts.sort(key=parse_dt, reverse=True)
  return posts

def load_keywords() -> List[str]:
  data = safe_read_json(KEYWORDS_JSON, [])
  out: List[str] = []
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
  while made < POSTS_PER_RUN and tries < POSTS_PER_RUN * 15:
    tries += 1
    keyword = random.choice(keywords).strip()
    if not keyword:
      continue

    slug = slugify(keyword)[:80]
    if not slug or slug in existing_slugs:
      continue

    # 1) 이미지 먼저 확보 (실패하면 글 생성 안 함)
    images = pick_unique_wikimedia_images(keyword, slug, max(4, IMG_COUNT))
    if not images:
      print(f"Skip (no enough high quality images): {slug}")
      continue

    # 2) 글 생성
    art = llm_generate_article(keyword)
    title = art["title"]
    description = art["description"]
    category = art["category"]
    body = art["body"]
    date_iso = now_iso_datetime()

    # 3) HTML 생성 (1번 템플릿 고정)
    html_doc = build_post_html(SITE_NAME, title, description, category, date_iso, slug, images, body)
    out_path = POSTS_DIR / f"{slug}.html"
    out_path.write_text(html_doc, encoding="utf-8")

    post_obj = {
      "title": title,
      "description": description,
      "category": category,
      "date": date_iso,
      "slug": slug,
      "thumbnail": normalize_img_path(images[0]),
      "image": normalize_img_path(images[0]),
      "url": f"posts/{slug}.html",
    }

    posts = add_post_to_index(posts, post_obj)
    safe_write_json(POSTS_JSON, posts)

    existing_slugs.add(slug)
    made += 1
    print(f"Generated: {slug}")

if __name__ == "__main__":
  main()
