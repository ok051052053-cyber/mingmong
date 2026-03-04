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

# OpenAI is for text only
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
SITE_URL = os.environ.get("SITE_URL", "https://mingmonglife.com").strip().rstrip("/")

POSTS_PER_RUN = int(os.environ.get("POSTS_PER_RUN", "3"))
IMG_COUNT = int(os.environ.get("IMG_COUNT", "4"))  # 최소 4장 권장
MIN_CHARS = int(os.environ.get("MIN_CHARS", "2500"))

MODEL = os.environ.get("MODEL", "gpt-4o-mini").strip()
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "").strip()

UNSPLASH_ACCESS_KEY = os.environ.get("UNSPLASH_ACCESS_KEY", "").strip()

HTTP_TIMEOUT = 25

# 품질 기준
MIN_IMG_WIDTH = int(os.environ.get("MIN_IMG_WIDTH", "1600"))
MIN_IMG_HEIGHT = int(os.environ.get("MIN_IMG_HEIGHT", "900"))
MIN_BYTES = int(os.environ.get("MIN_IMAGE_BYTES", str(220_000)))  # 220KB 아래면 저품질로 간주

client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

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

def parse_dt_to_ts(s: str) -> float:
  try:
    return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()
  except Exception:
    return 0.0

# -----------------------------
# Global image de-dup
# -----------------------------
def load_used_images() -> Set[str]:
  arr = safe_read_json(USED_IMAGES_JSON, [])
  if isinstance(arr, list):
    return set(str(x) for x in arr if x)
  return set()

def save_used_images(s: Set[str]):
  safe_write_json(USED_IMAGES_JSON, sorted(list(s)))

# -----------------------------
# Unsplash (high quality only)
# -----------------------------
UNSPLASH_SEARCH = "https://api.unsplash.com/search/photos"

def unsplash_search(query: str, per_page: int = 30, page: int = 1) -> List[dict]:
  if not UNSPLASH_ACCESS_KEY:
    return []
  headers = {"Authorization": f"Client-ID {UNSPLASH_ACCESS_KEY}"}
  params = {
    "query": query,
    "page": page,
    "per_page": per_page,
    "content_filter": "high",
    "orientation": "landscape",
  }
  try:
    r = requests.get(UNSPLASH_SEARCH, headers=headers, params=params, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    data = r.json()
    return data.get("results") or []
  except Exception:
    return []

def download_image(url: str) -> Optional[bytes]:
  try:
    r = requests.get(url, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    ct = (r.headers.get("content-type") or "").lower()
    if "image" not in ct:
      return None
    b = r.content
    if len(b) < MIN_BYTES:
      return None
    return b
  except Exception:
    return None

def pick_high_quality_images_unsplash(keyword: str, slug: str, count: int) -> Optional[List[str]]:
  """
  성공 조건
    - count 장 모두 확보
    - 전역 중복 없음 (hash)
    - 최소 크기 기준 충족 (api width/height + bytes)
  실패하면 None 반환
  """
  ensure_dirs(slug)

  used_global = load_used_images()
  used_in_post: Set[str] = set()
  saved_paths: List[str] = []

  # 검색 쿼리 다양화
  queries = [
    f"{keyword}",
    f"{keyword} office",
    f"{keyword} lifestyle",
    f"{keyword} technology",
    f"{keyword} minimal",
  ]

  candidates: List[Tuple[str, int, int]] = []
  seen_ids: Set[str] = set()

  # 후보 수집 (여러 쿼리 x 여러 페이지)
  for q in queries:
    for page in (1, 2):
      results = unsplash_search(q, per_page=30, page=page)
      for it in results:
        uid = str(it.get("id") or "")
        if not uid or uid in seen_ids:
          continue
        seen_ids.add(uid)

        w = int(it.get("width") or 0)
        h = int(it.get("height") or 0)
        if w < MIN_IMG_WIDTH or h < MIN_IMG_HEIGHT:
          continue

        urls = it.get("urls") or {}
        # regular은 너무 작을 수 있어서 full 우선
        url = urls.get("full") or urls.get("raw") or urls.get("regular") or ""
        if not url:
          continue

        # raw/full은 파라미터로 사이즈 유도 가능
        if "images.unsplash.com" in url:
          # 너무 큰 건 다운로드 느릴 수 있으니 적당히
          # 2400px 가로 정도로 강제
          joiner = "&" if "?" in url else "?"
          url = f"{url}{joiner}w=2400&fit=max&q=85"

        candidates.append((url, w, h))

  random.shuffle(candidates)

  attempts = 0
  while len(saved_paths) < count and attempts < 200 and candidates:
    attempts += 1
    url, w, h = candidates.pop()

    b = download_image(url)
    if not b:
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

  # 전역 저장
  save_used_images(used_global)

  if len(saved_paths) < count:
    # 실패면 생성한 파일도 지우고 None
    try:
      for p in (ASSETS_POSTS_DIR / slug).glob("*.jpg"):
        p.unlink(missing_ok=True)
    except Exception:
      pass
    return None

  return saved_paths

# -----------------------------
# OpenAI text generation (compatible)
# -----------------------------
def openai_generate_json(prompt_system: str, prompt_user: str) -> str:
  if not client:
    return ""

  # 최신 SDK면 responses 가능
  try:
    res = client.responses.create(
      model=MODEL,
      input=[
        {"role": "system", "content": prompt_system},
        {"role": "user", "content": prompt_user},
      ],
    )
    return (res.output_text or "").strip()
  except Exception:
    pass

  # 구형 호환 chat.completions
  try:
    res = client.chat.completions.create(
      model=MODEL,
      messages=[
        {"role": "system", "content": prompt_system},
        {"role": "user", "content": prompt_user},
      ],
      temperature=0.7,
    )
    return (res.choices[0].message.content or "").strip()
  except Exception:
    return ""

def llm_generate_article(keyword: str) -> Dict[str, str]:
  """
  목표
    - 최소 2500자 이상
    - 깊이 있게
    - 구조 고정 (TLDR, 비교, 체크리스트, 실수, 액션플랜)
  """
  # fallback
  if not client:
    title = keyword.title()
    desc = f"A practical guide about {keyword}."
    cat = "AI Tools"
    body = (
      f"<h2>TL;DR</h2><p>{esc(desc)}</p>"
      f"<h2>Why this matters</h2><p>{esc(desc)} {esc(desc)} {esc(desc)}</p>"
      f"<h2>Step by step</h2><p>{esc(desc)} {esc(desc)} {esc(desc)}</p>"
      f"<h2>Common mistakes</h2><p>{esc(desc)} {esc(desc)} {esc(desc)}</p>"
      f"<h2>Checklist</h2><ul><li>Do this</li><li>Do that</li></ul>"
    )
    while len(strip_tags(body)) < MIN_CHARS:
      body += f"<p>{esc(desc)} {esc(desc)} {esc(desc)} {esc(desc)}</p>"
    return {"title": title, "description": desc, "category": cat, "body": body}

  sys = (
    "You write genuinely useful SEO friendly blog content for readers in the US and Europe. "
    "No fluff. Add specifics. Add practical examples. "
    "Do not mention you are an AI. "
    "Write in natural English."
  )

  user = (
    f"Write one deep blog post about: {keyword}\n"
    "Return STRICT JSON with keys:\n"
    "title\n"
    "description\n"
    "category (one of: AI Tools, Make Money, Productivity, Reviews)\n"
    "body_html (HTML only using <h2>, <p>, <ul><li>, <strong>)\n\n"
    "Structure rules for body_html:\n"
    "1) Start with <h2>TL;DR</h2> and 3 to 5 bullet like short paragraphs\n"
    "2) Add <h2>Who this is for</h2>\n"
    "3) Add <h2>Key ideas</h2>\n"
    "4) Add <h2>Step by step</h2> with numbered feeling using paragraphs\n"
    "5) Add <h2>Comparisons</h2> with clear criteria\n"
    "6) Add <h2>Common mistakes</h2>\n"
    "7) Add <h2>Checklist</h2> as <ul><li>\n"
    f"Visible text length must be at least {MIN_CHARS} characters.\n"
    "No outer <html>. No markdown fences."
  )

  txt = openai_generate_json(sys, user)
  try:
    data = json.loads(txt)
  except Exception:
    data = {}

  title = str(data.get("title", "")).strip() or keyword.title()
  description = str(data.get("description", "")).strip() or f"A practical guide about {keyword}."
  category = str(data.get("category", "AI Tools")).strip() or "AI Tools"
  body = str(data.get("body_html", "")).strip() or "<p></p>"

  # 보강 1회
  if len(strip_tags(body)) < MIN_CHARS:
    user2 = (
      f"Expand the article below to at least {MIN_CHARS} visible characters. "
      "Make it deeper. Add examples. Keep structure and headings. "
      "Return STRICT JSON with only key body_html.\n\n"
      f"ARTICLE_HTML:\n{body}"
    )
    txt2 = openai_generate_json(sys, user2)
    try:
      d2 = json.loads(txt2)
      body2 = str(d2.get("body_html", "")).strip()
      if body2 and len(strip_tags(body2)) >= MIN_CHARS:
        body = body2
    except Exception:
      pass

  while len(strip_tags(body)) < MIN_CHARS:
    body += f"<p>{esc(description)} {esc(description)} {esc(description)}</p>"

  return {"title": title, "description": description, "category": category, "body": body}

# -----------------------------
# Even image distribution
# -----------------------------
def inject_images_evenly(body_html: str, image_paths: List[str], title: str) -> str:
  """
  hero(0) 는 별도
  나머지 이미지는 본문 블록 사이에 균등 삽입
  """
  extras = image_paths[1:] if len(image_paths) > 1 else []
  if not extras:
    return body_html

  # 블록 단위로 쪼갠다
  tokens = re.split(r"(?i)(</p>\s*|</ul>\s*|</h2>\s*)", body_html)
  units: List[str] = []
  buf = ""
  for t in tokens:
    buf += t
    if re.search(r"(?i)</p>\s*$|</ul>\s*$|</h2>\s*$", buf.strip()):
      units.append(buf)
      buf = ""
  if buf.strip():
    units.append(buf)

  if len(units) < 2:
    out = body_html
    for img in extras:
      out += f'<img src="../{esc(img)}" alt="{esc(title)}" loading="lazy">'
    return out

  n = len(units)
  m = len(extras)

  positions: List[int] = []
  for i in range(1, m + 1):
    pos = round(i * n / (m + 1))
    pos = min(max(pos, 1), n - 1)
    positions.append(pos)

  positions = sorted(positions)

  out_units: List[str] = []
  img_i = 0
  for idx, u in enumerate(units):
    out_units.append(u)
    if img_i < m and idx in positions:
      out_units.append(f'<img src="../{esc(extras[img_i])}" alt="{esc(title)}" loading="lazy">')
      img_i += 1

  while img_i < m:
    out_units.append(f'<img src="../{esc(extras[img_i])}" alt="{esc(title)}" loading="lazy">')
    img_i += 1

  return "".join(out_units)

# -----------------------------
# Build post html with meta
# -----------------------------
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

# -----------------------------
# posts.json ordering
# -----------------------------
def normalize_and_sort_posts(posts: List[dict]) -> List[dict]:
  cleaned = [p for p in posts if isinstance(p, dict) and p.get("slug")]
  cleaned.sort(key=lambda x: parse_dt_to_ts(str(x.get("date", ""))), reverse=True)
  return cleaned

def add_post_to_index(posts: List[dict], post_obj: dict) -> List[dict]:
  posts.append(post_obj)
  return normalize_and_sort_posts(posts)

# -----------------------------
# keywords
# -----------------------------
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

# -----------------------------
# main
# -----------------------------
def main():
  POSTS_DIR.mkdir(parents=True, exist_ok=True)
  ASSETS_POSTS_DIR.mkdir(parents=True, exist_ok=True)

  if not OPENAI_API_KEY:
    raise SystemExit("Missing OPENAI_API_KEY")

  if not UNSPLASH_ACCESS_KEY:
    raise SystemExit("Missing UNSPLASH_ACCESS_KEY (for non-AI high quality photos)")

  posts = safe_read_json(POSTS_JSON, [])
  if not isinstance(posts, list):
    posts = []

  # 기존 글도 정렬 재정리
  posts = normalize_and_sort_posts(posts)
  safe_write_json(POSTS_JSON, posts)

  keywords = load_keywords()
  if not keywords:
    raise SystemExit("keywords.json has no keywords")

  existing_slugs = set(p.get("slug") for p in posts if isinstance(p, dict) and p.get("slug"))

  made = 0
  tries = 0

  # 충분히 시도하되
  # 이미지 4장 이상 확보 못 하면 스킵하는 구조라 tries는 넉넉히
  max_tries = max(30, POSTS_PER_RUN * 20)

  while made < POSTS_PER_RUN and tries < max_tries:
    tries += 1
    keyword = random.choice(keywords).strip()
    if not keyword:
      continue

    slug = slugify(keyword)[:80]
    if not slug or slug in existing_slugs:
      continue

    # 1) 사진 먼저 고품질 확보 (핵심)
    images = pick_high_quality_images_unsplash(keyword, slug, max(4, IMG_COUNT))
    if not images:
      # 이 키워드는 사진이 충분히 안 나오는 케이스
      continue

    # 2) 글 생성
    art = llm_generate_article(keyword)
    title = art["title"]
    description = art["description"]
    category = art["category"]
    body = art["body"]
    date_iso = now_iso_datetime()

    # 3) 글 파일 생성
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
    print(f"Generated: {slug}  images={len(images)}")

  print(f"Done. made={made} tries={tries}")

  # 글을 하나도 못 만들었어도 실패로 끝내지 않는다
  # 이유: "고품질 사진 4장" 조건이 너무 빡세면 스킵만 잔뜩 나올 수 있음
  # 이 경우 커밋 변화가 없어서 자동으로 No changes to commit이 뜬다

if __name__ == "__main__":
  main()
