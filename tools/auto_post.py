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

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "").strip()
MODEL = os.environ.get("MODEL", "gpt-4o-mini").strip()

IMG_COUNT = int(os.environ.get("IMG_COUNT", "3"))  # total images per post (including hero)
MIN_CHARS = int(os.environ.get("MIN_CHARS", "2500"))  # body text min chars

HTTP_TIMEOUT = 25

IMAGE_PROVIDER = os.environ.get("IMAGE_PROVIDER", "wikimedia").strip().lower()
IMAGE_MODEL = os.environ.get("IMAGE_MODEL", "gpt-image-1").strip()

client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

ALLOWED_CATEGORIES = {"AI Tools", "Make Money", "Productivity", "Reviews"}

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
  import hashlib as _h
  return _h.sha256(b).hexdigest()

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

def clamp_category(cat: str) -> str:
  c = (cat or "").strip()
  if c in ALLOWED_CATEGORIES:
    return c
  # fallback mapping
  low = c.lower()
  if "money" in low or "income" in low or "side" in low:
    return "Make Money"
  if "review" in low or "price" in low or "alternat" in low:
    return "Reviews"
  if "productiv" in low or "workflow" in low:
    return "Productivity"
  return "AI Tools"

def extract_json_object(s: str) -> Optional[dict]:
  """
  Try strict parse.
  If fails, extract first {...} block and parse.
  """
  s = (s or "").strip()
  if not s:
    return None
  try:
    return json.loads(s)
  except Exception:
    pass

  # Extract first JSON object
  m = re.search(r"\{.*\}", s, flags=re.S)
  if not m:
    return None
  blob = m.group(0).strip()
  try:
    return json.loads(blob)
  except Exception:
    return None

def has_section(body_html: str, needle: str) -> bool:
  return needle.lower() in strip_tags(body_html).lower()

def ensure_core_sections(body_html: str) -> str:
  """
  Ensure the post is not "generic".
  Force these minimum parts by appending if missing:
    TL;DR
    Workflow
    Mistakes
    Templates
    Comparison table
  """
  add = []

  if not has_section(body_html, "TL;DR"):
    add.append(
      "<h2>TL;DR</h2>"
      "<ul>"
      "<li>Pick one tool stack for the job</li>"
      "<li>Run a 30 minute setup then reuse weekly</li>"
      "<li>Track time saved and cost per outcome</li>"
      "</ul>"
    )

  if not has_section(body_html, "Workflow"):
    add.append(
      "<h2>Workflow you can copy</h2>"
      "<p>Define the input. Define the output. Run a repeatable checklist. Save the template.</p>"
      "<ul>"
      "<li>Step 1: Collect examples and constraints</li>"
      "<li>Step 2: Use a structured prompt with role, goal, format</li>"
      "<li>Step 3: Add a quality gate then revise once</li>"
      "<li>Step 4: Store the template and reuse</li>"
      "</ul>"
    )

  if not has_section(body_html, "Mistakes"):
    add.append(
      "<h2>Common mistakes</h2>"
      "<ul>"
      "<li>Picking tools before defining the outcome</li>"
      "<li>Asking for vague output with no format</li>"
      "<li>Skipping examples and constraints</li>"
      "<li>Not verifying facts or links</li>"
      "<li>Not measuring time saved</li>"
      "</ul>"
    )

  if not has_section(body_html, "Templates"):
    add.append(
      "<h2>Templates</h2>"
      "<p><strong>Prompt template</strong></p>"
      "<p>Role: [expert role]</p>"
      "<p>Goal: [one sentence outcome]</p>"
      "<p>Context: [inputs and constraints]</p>"
      "<p>Output format: [bullets table steps]</p>"
      "<p>Quality rules: [no fluff include examples]</p>"
      "<p><strong>Checklist</strong></p>"
      "<ul>"
      "<li>Input quality checked</li>"
      "<li>Constraints listed</li>"
      "<li>Output format fixed</li>"
      "<li>One revision pass</li>"
      "</ul>"
    )

  # table check
  if "<table" not in body_html.lower():
    add.append(
      "<h2>Quick comparison</h2>"
      "<table>"
      "<thead><tr><th>Option</th><th>Best for</th><th>Cost</th><th>Gotcha</th></tr></thead>"
      "<tbody>"
      "<tr><td>Free plan</td><td>Testing the workflow</td><td>$0</td><td>Limits and caps</td></tr>"
      "<tr><td>Paid plan</td><td>Daily use at work</td><td>$10 to $30</td><td>Vendor lock in</td></tr>"
      "<tr><td>Stack</td><td>Automation and scale</td><td>$20 to $80</td><td>Setup time</td></tr>"
      "</tbody>"
      "</table>"
    )

  if add:
    body_html = body_html.strip() + "".join(add)

  return body_html

# -----------------------------
# Wikimedia image fetch (bitmap)
# -----------------------------
WIKI_API = "https://commons.wikimedia.org/w/api.php"

def wikimedia_search_image_urls(query: str, limit: int = 24) -> List[str]:
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
# OpenAI image
# -----------------------------
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

def pick_unique_images_for_post(keyword: str, slug: str, count: int) -> List[str]:
  """
  Returns relative paths:
    assets/posts/<slug>/1.jpg ...
  Guarantees:
    - no duplicates inside the post
    - no duplicates across the whole site (hash based)
  """
  ensure_dirs(slug)

  used_global = load_used_images()
  used_in_post: Set[str] = set()
  saved_paths: List[str] = []

  attempts = 0
  candidates: List[str] = []

  if IMAGE_PROVIDER in ("wikimedia", "auto", "hybrid"):
    q = f"{keyword} modern minimal realistic photo"
    candidates = wikimedia_search_image_urls(q, limit=40)

  while len(saved_paths) < count and attempts < 160:
    attempts += 1

    img_bytes = None
    img_hash = None

    # 1) try wikimedia
    url = None
    while candidates:
      u = candidates.pop()
      url = u
      break

    if url:
      b = download_image(url)
      if b:
        h = sha256_bytes(b)
        if h not in used_global and h not in used_in_post:
          img_bytes = b
          img_hash = h

    # 2) fallback to openai generation
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
      h = sha256_bytes(b)
      if h in used_global or h in used_in_post:
        continue
      img_bytes = b
      img_hash = h

    idx = len(saved_paths) + 1
    out_file = ASSETS_POSTS_DIR / slug / f"{idx}.jpg"
    out_file.write_bytes(img_bytes)

    used_in_post.add(img_hash)
    used_global.add(img_hash)
    saved_paths.append(f"assets/posts/{slug}/{idx}.jpg")

  save_used_images(used_global)

  # hard guarantee: fill remaining by openai until complete
  while len(saved_paths) < count:
    uniq = hashlib.sha1(f"{slug}-{len(saved_paths)}-{time.time()}-{random.random()}".encode()).hexdigest()[:12]
    b = openai_generate_image_bytes(
      f"High quality realistic photo for a blog post about: {keyword}. "
      f"Clean composition, professional lighting, no text, no logos, no watermarks. "
      f"Unique variation token: {uniq}."
    )
    if not b:
      break
    h = sha256_bytes(b)
    used_global = load_used_images()
    if h in used_global:
      continue
    idx = len(saved_paths) + 1
    out_file = ASSETS_POSTS_DIR / slug / f"{idx}.jpg"
    out_file.write_bytes(b)
    used_global.add(h)
    save_used_images(used_global)
    saved_paths.append(f"assets/posts/{slug}/{idx}.jpg")

  return saved_paths

# -----------------------------
# LLM content generation
# -----------------------------
def llm_generate_article(keyword: str) -> Dict[str, str]:
  """
  Return title, description, category, body_html (NO outer html).
  Enforce MIN_CHARS on text content.
  Make the content "deep" by forcing:
    - TL;DR
    - scenario
    - step by step workflow
    - comparison table
    - mistakes
    - templates
    - FAQ
    - numbers and tradeoffs
  """
  if not client:
    title = keyword.title()
    desc = f"A practical guide about {keyword}."
    cat = "AI Tools"
    body = (
      "<h2>TL;DR</h2>"
      "<ul><li>Pick one workflow and reuse it weekly</li><li>Measure time saved</li><li>Upgrade only if ROI is clear</li></ul>"
      "<h2>Scenario</h2>"
      "<p>You work 9 to 6 and need results fast with low budget.</p>"
      "<h2>Workflow</h2>"
      "<ul><li>Define outcome</li><li>Pick tools</li><li>Run checklist</li><li>Store template</li></ul>"
      "<h2>Quick comparison</h2>"
      "<table><thead><tr><th>Option</th><th>Best for</th><th>Cost</th><th>Gotcha</th></tr></thead>"
      "<tbody><tr><td>Free</td><td>Testing</td><td>$0</td><td>Limits</td></tr>"
      "<tr><td>Paid</td><td>Daily</td><td>$10 to $30</td><td>Lock in</td></tr></tbody></table>"
      "<h2>Mistakes</h2>"
      "<ul><li>Vague prompts</li><li>No examples</li><li>No measurement</li></ul>"
      "<h2>Templates</h2>"
      "<p>Role Goal Context Output Quality rules</p>"
      "<h2>FAQ</h2>"
      "<p>Start small then scale.</p>"
    )
    while len(strip_tags(body)) < MIN_CHARS:
      body += f"<p>{esc(desc)} {esc(desc)} {esc(desc)}</p>"
    body = ensure_core_sections(body)
    return {"title": title, "description": desc, "category": cat, "body": body}

  sys = (
    "You are a senior editor for a practical blog for young professionals in the US and Europe. "
    "Write content that is specific and actionable. "
    "No fluff. No generic definitions. "
    "Use concrete numbers, tradeoffs, and realistic constraints. "
    "Do not mention being an AI. "
    "Write in natural American English. "
    "Short paragraphs. "
    "Use only these HTML tags inside body_html: <h2>, <p>, <ul>, <li>, <table>, <thead>, <tbody>, <tr>, <th>, <td>, <strong>. "
    "Do not include outer <html>."
  )

  user = (
    f"Topic keyword: {keyword}\n\n"
    "Return ONLY valid JSON with keys:\n"
    "title\n"
    "description\n"
    "category (one of: AI Tools, Make Money, Productivity, Reviews)\n"
    "body_html\n\n"
    f"Rules for body_html:\n"
    f"- Visible text length at least {MIN_CHARS} characters\n"
    "- Start with <h2>TL;DR</h2> then a <ul> with 3 to 5 bullets\n"
    "- Include <h2>Scenario</h2> with a specific persona and constraints\n"
    "- Include <h2>Workflow</h2> with step by step bullets and time estimates\n"
    "- Include <h2>Tool options</h2> with 3 options and when to choose each\n"
    "- Include <h2>Quick comparison</h2> and a HTML table with at least 3 rows and columns: Option, Best for, Cost, Gotcha\n"
    "- Include <h2>Common mistakes</h2> with 5 to 8 bullets\n"
    "- Include <h2>Templates</h2> with at least 2 copy ready templates or checklists\n"
    "- Include <h2>FAQ</h2> with 4 questions and answers\n"
    "- Use numbers where reasonable. Example: time, cost, limits, ROI\n"
    "- Avoid vague claims. Avoid filler.\n\n"
    "Important:\n"
    "- Do not include markdown fences\n"
    "- Do not include any keys besides the four keys\n"
    "- body_html must be HTML only\n"
  )

  res = client.responses.create(
    model=MODEL,
    input=[
      {"role": "system", "content": sys},
      {"role": "user", "content": user},
    ],
  )

  txt = (res.output_text or "").strip()
  data = extract_json_object(txt) or {}

  title = str(data.get("title", "")).strip() or keyword.title()
  description = str(data.get("description", "")).strip() or f"A practical guide about {keyword}."
  category = clamp_category(str(data.get("category", "AI Tools")))
  body = str(data.get("body_html", "")).strip() or "<p></p>"

  # If short, do an "upgrade pass" that adds depth, not padding
  if len(strip_tags(body)) < MIN_CHARS:
    user2 = (
      f"Improve and expand the article HTML below to reach at least {MIN_CHARS} visible characters. "
      "Make it more specific and more actionable. "
      "Add numbers, examples, and checklists. "
      "Keep all required sections. "
      "Return ONLY valid JSON with one key: body_html.\n\n"
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
    d2 = extract_json_object(t2) or {}
    body2 = str(d2.get("body_html", "")).strip()
    if body2:
      body = body2

  # Ensure sections exist even if the model missed them
  body = ensure_core_sections(body)

  # Final guarantee for length
  while len(strip_tags(body)) < MIN_CHARS:
    body += f"<p>{esc(description)} {esc(description)} {esc(description)}</p>"

  return {"title": title, "description": description, "category": category, "body": body}

# -----------------------------
# Evenly distribute images in body
# -----------------------------
def inject_images_evenly(body_html: str, image_paths: List[str], title: str) -> str:
  """
  Keep hero image separate.
  Distribute remaining images evenly between paragraph blocks.
  """
  extras = image_paths[1:] if len(image_paths) > 1 else []
  if not extras:
    return body_html

  blocks = re.split(r"(?i)(</p>\s*|</ul>\s*|</ol>\s*|</h2>\s*|</table>\s*)", body_html)
  units: List[str] = []
  buf = ""
  for part in blocks:
    buf += part
    if re.search(r"(?i)</p>\s*$|</ul>\s*$|</ol>\s*$|</h2>\s*$|</table>\s*$", buf.strip()):
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

  positions = sorted(set(positions))
  p = 1
  while len(positions) < m and p < n:
    if p not in positions and p != 0 and p != n:
      positions.append(p)
    p += 1
  positions = sorted(positions)[:m]

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
# Build post html with SEO meta
# -----------------------------
def build_post_html(
  site_name: str,
  title: str,
  description: str,
  category: str,
  date_iso: str,
  slug: str,
  images: List[str],
  body_html: str
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
  <meta property="og:site_name" content="{esc(site_name)}</meta>
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

    <aside class="post-aside" aria-label="Sidebar">
      <div class="sidecard">
        <h3>Browse by focus</h3>

        <div class="catlist">
          <a class="catitem" href="../category.html?cat=AI%20Tools">
            <span class="caticon">🤖</span>
            <span class="cattext">
              <span class="catname">AI Tools</span>
              <span class="catsub">ChatGPT, Claude, Notion AI, automation</span>
            </span>
          </a>

          <a class="catitem" href="../category.html?cat=Make%20Money">
            <span class="caticon">💸</span>
            <span class="cattext">
              <span class="catname">Make Money</span>
              <span class="catsub">Side hustles, freelancing, remote income</span>
            </span>
          </a>

          <a class="catitem" href="../category.html?cat=Productivity">
            <span class="caticon">⚡</span>
            <span class="cattext">
              <span class="catname">Productivity</span>
              <span class="catsub">Workflows, systems, checklists</span>
            </span>
          </a>

          <a class="catitem" href="../category.html?cat=Reviews">
            <span class="caticon">🧾</span>
            <span class="cattext">
              <span class="catname">Reviews</span>
              <span class="catsub">Pricing, comparisons, alternatives</span>
            </span>
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
    category = clamp_category(art["category"])
    body = art["body"]
    date_iso = now_iso_datetime()

    images = pick_unique_images_for_post(keyword, slug, max(1, IMG_COUNT))

    html_doc = build_post_html(SITE_NAME, title, description, category, date_iso, slug, images, body)
    out_path = POSTS_DIR / f"{slug}.html"
    out_path.write_text(html_doc, encoding="utf-8")

    thumb = normalize_img_path(images[0]) if images else f"assets/posts/{slug}/1.jpg"

    post_obj = {
      "title": title,
      "description": description,
      "category": category,
      "date": date_iso,
      "slug": slug,
      "thumbnail": thumb,
      "image": thumb,
      "url": f"posts/{slug}.html",
    }

    posts = add_post_to_index(posts, post_obj)
    safe_write_json(POSTS_JSON, posts)

    existing_slugs.add(slug)
    made += 1
    print(f"Generated: {slug}")

if __name__ == "__main__":
  main()
