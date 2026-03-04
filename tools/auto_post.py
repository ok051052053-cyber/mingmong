import os
import re
import json
import time
import html
import random
import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import requests
from slugify import slugify
from openai import OpenAI

# =============================
# Paths
# =============================
ROOT = Path(__file__).resolve().parents[1]
POSTS_DIR = ROOT / "posts"
ASSETS_POSTS_DIR = ROOT / "assets" / "posts"
POSTS_JSON = ROOT / "posts.json"
KEYWORDS_JSON = ROOT / "keywords.json"
USED_IMAGES_JSON = ROOT / "used_images.json"

# =============================
# Config
# =============================
SITE_NAME = os.environ.get("SITE_NAME", "MingMong").strip()
SITE_URL = os.environ.get("SITE_URL", "https://mingmonglife.com").strip().rstrip("/")
POSTS_PER_RUN = int(os.environ.get("POSTS_PER_RUN", "3"))

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "").strip()
MODEL = os.environ.get("MODEL", "gpt-4o-mini").strip()

# 글 안 이미지 개수 (hero 포함)
IMG_COUNT = int(os.environ.get("IMG_COUNT", "6"))

# 글 최소 글자수
MIN_CHARS = int(os.environ.get("MIN_CHARS", "2500"))

# 고품질 이미지만
MIN_IMG_BYTES = int(os.environ.get("MIN_IMG_BYTES", "350000"))  # 350KB+
MIN_IMG_W = int(os.environ.get("MIN_IMG_W", "1800"))            # 1800px+
MIN_IMG_H = int(os.environ.get("MIN_IMG_H", "1100"))            # 1100px+

HTTP_TIMEOUT = 25

# ✅ AI 이미지 생성 금지
ALLOW_AI_IMAGES = os.environ.get("ALLOW_AI_IMAGES", "0").strip() == "1"

# Wikimedia only
IMAGE_PROVIDER = "wikimedia"

client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

# =============================
# Utils
# =============================
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

# =============================
# Global image de-dup
# =============================
def load_used_images() -> Set[str]:
  arr = safe_read_json(USED_IMAGES_JSON, [])
  if isinstance(arr, list):
    return set([str(x) for x in arr if x])
  return set()

def save_used_images(s: Set[str]):
  safe_write_json(USED_IMAGES_JSON, sorted(list(s)))

# =============================
# Wikimedia image fetch
# =============================
WIKI_API = "https://commons.wikimedia.org/w/api.php"

def wikimedia_search_images(query: str, limit: int = 60) -> List[Dict[str, str]]:
  """
  Returns list of dicts: {"url": ..., "width": int, "height": int}
  Uses imageinfo size metadata
  """
  params = {
    "action": "query",
    "format": "json",
    "origin": "*",
    "generator": "search",
    "gsrsearch": f"filetype:bitmap {query}",
    "gsrlimit": str(limit),
    "gsrnamespace": "6",
    "prop": "imageinfo",
    "iiprop": "url|size",
  }
  try:
    r = requests.get(WIKI_API, params=params, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    data = r.json()
    pages = (data.get("query") or {}).get("pages") or {}
    out: List[Dict[str, str]] = []
    for _, page in pages.items():
      infos = page.get("imageinfo") or []
      if not infos:
        continue
      info = infos[0] or {}
      url = info.get("url") or ""
      w = info.get("width")
      h = info.get("height")
      if url and isinstance(w, int) and isinstance(h, int):
        out.append({"url": url, "width": w, "height": h})
    random.shuffle(out)
    return out
  except Exception:
    return []

def head_content_length(url: str) -> Optional[int]:
  try:
    r = requests.head(url, timeout=HTTP_TIMEOUT, allow_redirects=True)
    if r.status_code >= 400:
      return None
    cl = r.headers.get("content-length")
    if not cl:
      return None
    return int(cl)
  except Exception:
    return None

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

def pick_unique_images_for_post(keyword: str, slug: str, count: int) -> List[str]:
  """
  ✅ AI 이미지 생성 금지
  ✅ 고품질 기준 통과만
  ✅ 포스트 내부 중복 금지
  ✅ 전체 사이트 중복 금지 (hash)
  조건 만족 못하면 빈 리스트 반환
  """
  ensure_dirs(slug)

  used_global = load_used_images()
  used_in_post: Set[str] = set()
  saved_paths: List[str] = []

  # query 더 프리미엄하게
  q = f'{keyword} "photograph" high resolution premium'
  candidates = wikimedia_search_images(q, limit=90)

  attempts = 0
  while len(saved_paths) < count and attempts < 600:
    attempts += 1
    if not candidates:
      break

    c = candidates.pop()

    url = c["url"]
    w = int(c["width"])
    h = int(c["height"])

    # 크기 컷
    if w < MIN_IMG_W or h < MIN_IMG_H:
      continue

    # 용량 컷 (가능하면 HEAD로 먼저)
    cl = head_content_length(url)
    if cl is not None and cl < MIN_IMG_BYTES:
      continue

    b = download_image(url)
    if not b:
      continue

    if len(b) < MIN_IMG_BYTES:
      continue

    hsh = sha256_bytes(b)
    if hsh in used_global or hsh in used_in_post:
      continue

    idx = len(saved_paths) + 1
    out_file = ASSETS_POSTS_DIR / slug / f"{idx}.jpg"
    out_file.write_bytes(b)

    used_in_post.add(hsh)
    used_global.add(hsh)
    saved_paths.append(f"assets/posts/{slug}/{idx}.jpg")

  # ✅ 고품질만 고집
  if len(saved_paths) < count:
    # 포스트 폴더에 만든 파일 정리
    for p in (ASSETS_POSTS_DIR / slug).glob("*.jpg"):
      try:
        p.unlink()
      except Exception:
        pass
    return []

  save_used_images(used_global)
  return saved_paths

# =============================
# LLM content generation
# =============================
def llm_generate_article(keyword: str) -> Dict[str, str]:
  """
  Return title, description, category, body_html
  MIN_CHARS 강제
  """
  # fallback
  if not client:
    title = keyword.title()
    desc = f"A practical guide about {keyword}."
    cat = "AI Tools"
    body = (
      f"<h2>Overview</h2><p>{esc(desc)}</p>"
      f"<h2>Steps</h2><p>{esc(desc)} {esc(desc)} {esc(desc)}</p>"
      f"<h2>Mistakes</h2><p>{esc(desc)} {esc(desc)} {esc(desc)}</p>"
      f"<h2>Checklist</h2><ul><li>Pick a tool</li><li>Try a workflow</li><li>Measure</li></ul>"
    )
    while len(strip_tags(body)) < MIN_CHARS:
      body += f"<p>{esc(desc)} {esc(desc)} {esc(desc)}</p>"
    return {"title": title, "description": desc, "category": cat, "body": body}

  sys = (
    "You write SEO-friendly helpful blog content for young professionals in the US and Europe. "
    "No fluff. Clear structure. Short paragraphs. Useful steps and comparisons. "
    "Do not mention that you are an AI. "
    "Write in natural English."
  )

  user = (
    f"Write one blog post about: {keyword}\n"
    "Output JSON with keys: title, description, category(one of: AI Tools, Make Money, Productivity, Reviews), "
    "body_html (HTML only, use <h2>, <p>, <ul><li>). "
    f"The visible text length must be at least {MIN_CHARS} characters. "
    "Do not include outer <html>."
  )

  res = client.responses.create(
    model=MODEL,
    input=[
      {"role": "system", "content": sys},
      {"role": "user", "content": user},
    ],
  )

  txt = (res.output_text or "").strip()
  try:
    data = json.loads(txt)
  except Exception:
    data = {}

  title = str(data.get("title", "")).strip() or keyword.title()
  description = str(data.get("description", "")).strip() or f"A practical guide about {keyword}."
  category = str(data.get("category", "AI Tools")).strip() or "AI Tools"
  body = str(data.get("body_html", "")).strip() or "<p></p>"

  # 부족하면 1번 확장
  if len(strip_tags(body)) < MIN_CHARS:
    user2 = (
      f"Expand the article below to at least {MIN_CHARS} visible characters. "
      "Keep structure. Add useful specifics. "
      "Return JSON with only one key: body_html.\n\n"
      f"ARTICLE_HTML:\n{body}"
    )
    res2 = client.responses.create(
      model=MODEL,
      input=[
        {"role": "system", "content": sys},
        {"role": "user", "content": user2},
      ],
    )
    t2 = (res2.output_text or "").strip()
    try:
      d2 = json.loads(t2)
      body2 = str(d2.get("body_html", "")).strip()
      if body2 and len(strip_tags(body2)) >= MIN_CHARS:
        body = body2
    except Exception:
      pass

  while len(strip_tags(body)) < MIN_CHARS:
    body += f"<p>{esc(description)} {esc(description)} {esc(description)}</p>"

  return {"title": title, "description": description, "category": category, "body": body}

# =============================
# Text distribution between images
# =============================
def split_units(body_html: str) -> List[str]:
  """
  Split into block units
  Keep <h2> with following content by grouping later
  """
  parts = re.split(r"(?i)(</p>\s*|</ul>\s*|</ol>\s*|</h2>\s*)", body_html)
  units: List[str] = []
  buf = ""
  for p in parts:
    buf += p
    if re.search(r"(?i)</p>\s*$|</ul>\s*$|</ol>\s*$|</h2>\s*$", buf.strip()):
      units.append(buf)
      buf = ""
  if buf.strip():
    units.append(buf)
  return [u for u in units if u.strip()]

def rebalance_units_into_k(units: List[str], k: int) -> List[str]:
  if k <= 1:
    return ["".join(units)]
  if not units:
    return [""] * k

  # target by visible char
  sizes = [len(strip_tags(u)) for u in units]
  total = sum(sizes)
  target = max(1, total // k)

  buckets: List[List[str]] = [[] for _ in range(k)]
  bucket_sizes = [0] * k

  bi = 0
  for u, sz in zip(units, sizes):
    if bi < k - 1 and bucket_sizes[bi] >= target:
      bi += 1
    buckets[bi].append(u)
    bucket_sizes[bi] += sz

  out = ["".join(b) for b in buckets]

  # if trailing empties, shift from previous
  for i in range(k - 1, -1, -1):
    if out[i].strip():
      continue
    j = i - 1
    while j >= 0 and not out[j].strip():
      j -= 1
    if j >= 0:
      # move last unit
      uj = split_units(out[j])
      if uj:
        last = uj.pop()
        out[j] = "".join(uj)
        out[i] = last
  return out

def inject_images_evenly(body_html: str, image_paths: List[str], title: str) -> str:
  """
  hero는 별도
  나머지 (IMG_COUNT-1) 이미지를 본문에 균등 삽입
  본문도 이미지 수에 맞춰 균등 분배
  """
  if not image_paths:
    return body_html

  extras = image_paths[1:]
  if not extras:
    return body_html

  units = split_units(body_html)
  # 텍스트를 (extras 개수 + 1) 덩어리로 분할
  chunks = rebalance_units_into_k(units, len(extras) + 1)

  out: List[str] = []
  out.append(chunks[0])

  for i, img in enumerate(extras):
    out.append(
      f'<img src="../{esc(img)}" alt="{esc(title)}" loading="lazy">'
    )
    out.append(chunks[i + 1])

  return "".join(out)

# =============================
# Build post html
# =============================
def build_post_html(
  site_name: str,
  title: str,
  description: str,
  category: str,
  date_iso: str,
  slug: str,
  images: List[str],
  body_html: str,
) -> str:
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

  <link rel="stylesheet" href="../style.css?v=2001" />
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
          <a class="catitem" href="../category.html?cat=AI%20Tools">
            <div class="caticon">🤖</div>
            <div class="cattext">
              <div class="catname">AI Tools</div>
              <div class="catsub">ChatGPT, Claude, Notion AI</div>
            </div>
          </a>
          <a class="catitem" href="../category.html?cat=Make%20Money">
            <div class="caticon">💸</div>
            <div class="cattext">
              <div class="catname">Make Money</div>
              <div class="catsub">Side hustles, remote income</div>
            </div>
          </a>
          <a class="catitem" href="../category.html?cat=Productivity">
            <div class="caticon">⚡</div>
            <div class="cattext">
              <div class="catname">Productivity</div>
              <div class="catsub">Workflows, systems</div>
            </div>
          </a>
          <a class="catitem" href="../category.html?cat=Reviews">
            <div class="caticon">🧾</div>
            <div class="cattext">
              <div class="catname">Reviews</div>
              <div class="catsub">Pricing, alternatives</div>
            </div>
          </a>
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

# =============================
# Posts index sorting
# =============================
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

# =============================
# Main
# =============================
def main():
  POSTS_DIR.mkdir(parents=True, exist_ok=True)
  ASSETS_POSTS_DIR.mkdir(parents=True, exist_ok=True)

  posts = safe_read_json(POSTS_JSON, [])
  if not isinstance(posts, list):
    posts = []

  keywords = load_keywords()
  if not keywords:
    raise SystemExit("keywords.json has no keywords")

  existing_slugs = set([p.get("slug") for p in posts if isinstance(p, dict) and p.get("slug")])

  made = 0
  tries = 0

  # 고품질만 고집하니까 더 많이 시도
  while made < POSTS_PER_RUN and tries < POSTS_PER_RUN * 80:
    tries += 1

    keyword = random.choice(keywords).strip()
    if not keyword:
      continue

    slug = slugify(keyword)[:80]
    if not slug or slug in existing_slugs:
      continue

    # 1) 글 생성
    art = llm_generate_article(keyword)
    title = art["title"]
    description = art["description"]
    category = art["category"]
    body = art["body"]
    date_iso = now_iso_datetime()

    # 2) 이미지 고품질만
    images = pick_unique_images_for_post(keyword, slug, max(1, IMG_COUNT))
    if not images:
      # 이 키워드는 스킵
      continue

    # 3) html 생성
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

  print(f"Done. made={made} tries={tries}")

if __name__ == "__main__":
  if not ALLOW_AI_IMAGES and IMAGE_PROVIDER != "wikimedia":
    raise SystemExit("AI images are disabled. IMAGE_PROVIDER must be wikimedia.")
  main()
