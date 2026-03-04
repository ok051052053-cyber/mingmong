import os
import re
import json
import time
import html
import random
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

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
USED_UNSPLASH_JSON = ROOT / "used_unsplash.json"

# -----------------------------
# Config
# -----------------------------
SITE_NAME = os.environ.get("SITE_NAME", "MingMong").strip()
SITE_URL = os.environ.get("SITE_URL", "https://mingmonglife.com").strip().rstrip("/")

POSTS_PER_RUN = int(os.environ.get("POSTS_PER_RUN", "1"))
IMG_COUNT = int(os.environ.get("IMG_COUNT", "4"))          # 최소 4장 권장
MIN_CHARS = int(os.environ.get("MIN_CHARS", "2500"))       # 본문 가시 텍스트 최소 길이

MODEL = os.environ.get("MODEL", "gpt-4o-mini").strip()
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "").strip()

UNSPLASH_ACCESS_KEY = os.environ.get("UNSPLASH_ACCESS_KEY", "").strip()

HTTP_TIMEOUT = int(os.environ.get("HTTP_TIMEOUT", "30"))
MAX_IMAGE_ATTEMPTS = int(os.environ.get("MAX_IMAGE_ATTEMPTS", "80"))

# 고품질 사진만
MIN_IMAGE_BYTES = int(os.environ.get("MIN_IMAGE_BYTES", "120000"))  # 120KB 미만은 탈락
# 검색 키워드 보정
UNSPLASH_QUERY_SUFFIX = os.environ.get(
  "UNSPLASH_QUERY_SUFFIX",
  "high quality photo, realistic, professional, minimal, editorial"
).strip()

# 글 품질 강화
WRITING_STYLE = os.environ.get(
  "WRITING_STYLE",
  "deep, specific, step-by-step, practical, with comparisons, with pitfalls, with checklists"
).strip()

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

def ensure_dirs(slug: str):
  POSTS_DIR.mkdir(parents=True, exist_ok=True)
  (ASSETS_POSTS_DIR / slug).mkdir(parents=True, exist_ok=True)

def load_used_unsplash_ids() -> Set[str]:
  arr = safe_read_json(USED_UNSPLASH_JSON, [])
  if isinstance(arr, list):
    return set([str(x) for x in arr if x])
  return set()

def save_used_unsplash_ids(s: Set[str]):
  safe_write_json(USED_UNSPLASH_JSON, sorted(list(s)))

# -----------------------------
# Unsplash (NO AI images)
# -----------------------------
UNSPLASH_SEARCH = "https://api.unsplash.com/search/photos"
UNSPLASH_DOWNLOAD = "https://api.unsplash.com/photos/{id}/download"

def _unsplash_headers():
  if not UNSPLASH_ACCESS_KEY:
    raise SystemExit("Missing UNSPLASH_ACCESS_KEY secret")
  return {
    "Authorization": f"Client-ID {UNSPLASH_ACCESS_KEY}",
    "Accept-Version": "v1",
    "User-Agent": f"{SITE_NAME}-bot/1.0",
  }

def unsplash_search(query: str, per_page: int = 30, page: int = 1) -> List[dict]:
  params = {
    "query": query,
    "per_page": per_page,
    "page": page,
    "orientation": "landscape",
    "content_filter": "high",
  }
  try:
    r = requests.get(
      UNSPLASH_SEARCH,
      params=params,
      headers=_unsplash_headers(),
      timeout=HTTP_TIMEOUT,
    )
    r.raise_for_status()
    data = r.json()
    return (data.get("results") or [])
  except Exception:
    return []

def unsplash_get_download_url(photo_id: str) -> Optional[str]:
  try:
    r = requests.get(
      UNSPLASH_DOWNLOAD.format(id=photo_id),
      headers=_unsplash_headers(),
      timeout=HTTP_TIMEOUT,
    )
    r.raise_for_status()
    j = r.json()
    url = (j.get("url") or "").strip()
    return url or None
  except Exception:
    return None

def unsplash_download_bytes(download_url: str) -> Optional[bytes]:
  # Unsplash가 준 download endpoint url은 redirect를 타고 실제 이미지로 감
  # 여기서는 인증 없이 받아도 됨
  for _ in range(3):
    try:
      r = requests.get(download_url, timeout=HTTP_TIMEOUT, allow_redirects=True)
      if r.status_code in (403, 429, 503):
        time.sleep(2)
        continue
      r.raise_for_status()

      ct = (r.headers.get("content-type") or "").lower()
      if "image" not in ct:
        return None

      b = r.content
      if not b or len(b) < MIN_IMAGE_BYTES:
        return None
      return b
    except Exception:
      time.sleep(1)
  return None

def pick_high_quality_unsplash_photos(keyword: str, slug: str, count: int) -> Tuple[List[str], List[dict]]:
  """
  Returns:
    - local relative image paths: assets/posts/<slug>/1.jpg ...
    - credits list: [{id, user_name, user_url, photo_url}]
  Hard rules:
    - NO AI images
    - Must collect exactly `count` or fail
  """
  ensure_dirs(slug)

  used_global = load_used_unsplash_ids()
  used_in_post: Set[str] = set()

  saved_paths: List[str] = []
  credits: List[dict] = []

  q = f"{keyword} {UNSPLASH_QUERY_SUFFIX}".strip()

  attempts = 0
  page = 1
  pool: List[dict] = []

  while len(saved_paths) < count and attempts < MAX_IMAGE_ATTEMPTS:
    attempts += 1

    if not pool:
      pool = unsplash_search(q, per_page=30, page=page)
      page += 1
      random.shuffle(pool)

    if not pool:
      break

    item = pool.pop()
    pid = str(item.get("id") or "").strip()
    if not pid:
      continue
    if pid in used_global or pid in used_in_post:
      continue

    dl = unsplash_get_download_url(pid)
    if not dl:
      continue

    b = unsplash_download_bytes(dl)
    if not b:
      continue

    idx = len(saved_paths) + 1
    out_file = ASSETS_POSTS_DIR / slug / f"{idx}.jpg"
    out_file.write_bytes(b)

    saved_paths.append(f"assets/posts/{slug}/{idx}.jpg")
    used_in_post.add(pid)
    used_global.add(pid)

    user = item.get("user") or {}
    links = item.get("links") or {}
    user_links = user.get("links") or {}

    credits.append({
      "id": pid,
      "user_name": (user.get("name") or "").strip(),
      "user_url": (user_links.get("html") or "").strip(),
      "photo_url": (links.get("html") or "").strip(),
    })

  save_used_unsplash_ids(used_global)

  if len(saved_paths) < count:
    raise SystemExit(f"Could not source enough high quality non-AI photos. Got {len(saved_paths)}/{count}")

  return saved_paths, credits

# -----------------------------
# LLM content generation (TEXT ONLY)
# -----------------------------
def llm_generate_article(keyword: str) -> Dict[str, str]:
  """
  Returns dict with:
    title, description, category, body_html
  body_html: HTML only, no outer html
  """
  if not client:
    raise SystemExit("Missing OPENAI_API_KEY")

  sys = (
    "You write SEO-friendly blog posts for young professionals in the US and Europe. "
    "Write natural English. No fluff. No generic filler. "
    "Use concrete examples, practical steps, tradeoffs, and common mistakes. "
    "Do not mention that you are an AI."
  )

  user = (
    f"Topic: {keyword}\n\n"
    f"Write ONE deep, useful article. Style: {WRITING_STYLE}.\n"
    "Output JSON only with keys:\n"
    "title\n"
    "description\n"
    "category (one of: AI Tools, Make Money, Productivity, Reviews)\n"
    "body_html (HTML only. Use <h2>, <p>, <ul><li>. Include a TL;DR section near the top.)\n\n"
    f"Constraints:\n"
    f"- Visible text length must be at least {MIN_CHARS} characters\n"
    "- No outer <html>\n"
    "- Avoid repeating the same sentence\n"
  )

  # 가장 호환 좋은 chat.completions 사용
  res = client.chat.completions.create(
    model=MODEL,
    messages=[
      {"role": "system", "content": sys},
      {"role": "user", "content": user},
    ],
    temperature=0.6,
  )

  txt = (res.choices[0].message.content or "").strip()
  try:
    data = json.loads(txt)
  except Exception:
    data = {}

  title = str(data.get("title", "")).strip() or keyword.title()
  description = str(data.get("description", "")).strip() or f"A practical guide about {keyword}."
  category = str(data.get("category", "AI Tools")).strip() or "AI Tools"
  body = str(data.get("body_html", "")).strip()

  if not body:
    body = "<h2>TL;DR</h2><ul><li>Key takeaways</li></ul><h2>Guide</h2><p>...</p>"

  # 최소 글자 보장: 부족하면 한 번 확장
  if len(strip_tags(body)) < MIN_CHARS:
    user2 = (
      f"Expand the article HTML below to at least {MIN_CHARS} visible characters. "
      "Keep it specific and actionable. "
      "Return JSON only with key body_html.\n\n"
      f"ARTICLE_HTML:\n{body}"
    )
    res2 = client.chat.completions.create(
      model=MODEL,
      messages=[
        {"role": "system", "content": sys},
        {"role": "user", "content": user2},
      ],
      temperature=0.6,
    )
    t2 = (res2.choices[0].message.content or "").strip()
    try:
      d2 = json.loads(t2)
      body2 = str(d2.get("body_html", "")).strip()
      if body2 and len(strip_tags(body2)) >= MIN_CHARS:
        body = body2
    except Exception:
      pass

  # 그래도 부족하면 최소한의 패딩(남발 방지)
  while len(strip_tags(body)) < MIN_CHARS:
    body += f"<p>{esc(description)} This section adds more practical detail and context.</p>"

  return {"title": title, "description": description, "category": category, "body": body}

# -----------------------------
# Image injection + credits
# -----------------------------
def inject_images_evenly(body_html: str, image_paths: List[str], title: str) -> str:
  if not image_paths:
    return body_html

  # hero는 별도, 나머지 3장 이상을 본문에 분산
  extras = image_paths[1:] if len(image_paths) > 1 else []
  if not extras:
    return body_html

  blocks = re.split(r"(?i)(</p>\s*|</ul>\s*|</ol>\s*|</h2>\s*)", body_html)
  units: List[str] = []
  buf = ""
  for part in blocks:
    buf += part
    if re.search(r"(?i)</p>\s*$|</ul>\s*$|</ol>\s*$|</h2>\s*$", buf.strip()):
      units.append(buf)
      buf = ""
  if buf.strip():
    units.append(buf)

  if len(units) <= 1:
    out = body_html
    for img in extras:
      out += f'<img src="../{esc(img)}" alt="{esc(title)}" loading="lazy">'
    return out

  n = len(units)
  m = len(extras)

  positions = []
  for i in range(1, m + 1):
    pos = round(i * n / (m + 1))
    pos = min(max(pos, 1), n - 1)
    positions.append(pos)

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

def build_credits_html(credits: List[dict]) -> str:
  if not credits:
    return ""
  # 너무 길지 않게
  items = credits[:6]
  lis = []
  for c in items:
    name = esc(c.get("user_name") or "Photographer")
    uurl = esc(c.get("user_url") or "")
    purl = esc(c.get("photo_url") or "")
    if uurl and purl:
      lis.append(f'<li>Photo by <a href="{uurl}" rel="nofollow noopener" target="_blank">{name}</a> on <a href="{purl}" rel="nofollow noopener" target="_blank">Unsplash</a></li>')
    else:
      lis.append(f"<li>Photo credit: {name} (Unsplash)</li>")
  return "<h2>Photo credits</h2><ul>" + "".join(lis) + "</ul>"

# -----------------------------
# Build post HTML
# -----------------------------
def build_post_html(
  site_name: str,
  title: str,
  description: str,
  category: str,
  date_iso: str,
  slug: str,
  images: List[str],
  credits: List[dict],
  body_html: str
) -> str:
  hero_img = images[0] if images else ""
  canonical = f"{SITE_URL}/posts/{slug}.html"

  body_html = inject_images_evenly(body_html, images, title)
  body_html += build_credits_html(credits)

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

  <link rel="stylesheet" href="../style.css?v=1001" />
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
# posts.json index sorting
# -----------------------------
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

# -----------------------------
# Main
# -----------------------------
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

  while made < POSTS_PER_RUN and tries < POSTS_PER_RUN * 20:
    tries += 1
    keyword = random.choice(keywords).strip()
    if not keyword:
      continue

    slug = slugify(keyword)[:80]
    if not slug or slug in existing_slugs:
      continue

    date_iso = now_iso_datetime()

    # 1) 글 생성 (고품질 텍스트)
    art = llm_generate_article(keyword)
    title = art["title"]
    description = art["description"]
    category = art["category"]
    body = art["body"]

    # 2) 사진 수급 (Unsplash only, 실패하면 즉시 종료)
    images, credits = pick_high_quality_unsplash_photos(keyword, slug, max(4, IMG_COUNT))

    # 3) HTML 생성
    html_doc = build_post_html(SITE_NAME, title, description, category, date_iso, slug, images, credits, body)

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
