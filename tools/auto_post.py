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
from openai import OpenAI  # text only

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

BASE_MIN_CHARS = int(os.environ.get("MIN_CHARS", "2500"))
TARGET_MIN_CHARS = BASE_MIN_CHARS * 2

HTTP_TIMEOUT = int(os.environ.get("HTTP_TIMEOUT", "25"))

# user ask: "1시간 걸려도 됨"
IMAGE_SEARCH_BUDGET_SECONDS = int(os.environ.get("IMAGE_SEARCH_BUDGET_SECONDS", "3600"))

# slow but safer for rate limits
IMAGE_SEARCH_SLEEP_SECONDS = float(os.environ.get("IMAGE_SEARCH_SLEEP_SECONDS", "0.35"))

# stronger quality bar
MIN_IMG_BYTES = int(os.environ.get("MIN_IMG_BYTES", "60000"))  # ~60KB
MIN_W = int(os.environ.get("MIN_W", "1400"))
MIN_H = int(os.environ.get("MIN_H", "900"))

# IMPORTANT: no AI images
IMAGE_PROVIDER = "free_only"

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
  low = c.lower()
  if "money" in low or "income" in low or "side" in low:
    return "Make Money"
  if "review" in low or "price" in low or "alternat" in low:
    return "Reviews"
  if "productiv" in low or "workflow" in low:
    return "Productivity"
  return "AI Tools"

def extract_json_object(s: str) -> Optional[dict]:
  s = (s or "").strip()
  if not s:
    return None
  try:
    return json.loads(s)
  except Exception:
    pass
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
  add = []

  if not has_section(body_html, "TL;DR"):
    add.append(
      "<h2>TL;DR</h2>"
      "<ul>"
      "<li>Pick one outcome then build a repeatable workflow</li>"
      "<li>Use checklists and templates so results compound weekly</li>"
      "<li>Track ROI using time saved and error rate</li>"
      "</ul>"
    )

  if not has_section(body_html, "Scenario"):
    add.append(
      "<h2>Scenario</h2>"
      "<p>You have a full time job and you want results you can verify. "
      "Your constraint is time and focus not ideas.</p>"
    )

  if not has_section(body_html, "Workflow"):
    add.append(
      "<h2>Workflow</h2>"
      "<ul>"
      "<li>Step 1 (10 min): Collect 3 examples and define output format</li>"
      "<li>Step 2 (15 min): Draft a template and run one test</li>"
      "<li>Step 3 (10 min): Add a quality gate and revise once</li>"
      "<li>Step 4 (5 min weekly): Reuse and improve one thing</li>"
      "</ul>"
    )

  if not has_section(body_html, "Tool options"):
    add.append(
      "<h2>Tool options</h2>"
      "<ul>"
      "<li><strong>Free stack</strong>: best for testing and habits</li>"
      "<li><strong>Paid single tool</strong>: best for daily speed</li>"
      "<li><strong>Automation stack</strong>: best for scale after proof</li>"
      "</ul>"
    )

  if "<table" not in body_html.lower():
    add.append(
      "<h2>Quick comparison</h2>"
      "<table>"
      "<thead><tr><th>Option</th><th>Best for</th><th>Cost</th><th>Gotcha</th></tr></thead>"
      "<tbody>"
      "<tr><td>Free plan</td><td>Proving the workflow</td><td>$0</td><td>Limits</td></tr>"
      "<tr><td>Paid plan</td><td>Daily use</td><td>$10 to $30</td><td>Lock in</td></tr>"
      "<tr><td>Automation</td><td>Scaling output</td><td>$20 to $80</td><td>Maintenance</td></tr>"
      "</tbody>"
      "</table>"
    )

  if not has_section(body_html, "Decision rules"):
    add.append(
      "<h2>Decision rules</h2>"
      "<ul>"
      "<li>If success is not 1 sentence then stop and define it</li>"
      "<li>If repeatability matters then make a checklist first</li>"
      "<li>If accuracy matters then add verification and evidence</li>"
      "<li>If output is slow then simplify format before automation</li>"
      "</ul>"
    )

  if not has_section(body_html, "Example walkthrough"):
    add.append(
      "<h2>Example walkthrough</h2>"
      "<p>Pick one micro goal. Example: produce one publish ready outline in 20 minutes. "
      "Run the template. Check against the checklist. Fix the biggest issue. Save it. Reuse it.</p>"
    )

  if not has_section(body_html, "Metrics"):
    add.append(
      "<h2>Metrics to track</h2>"
      "<ul>"
      "<li>Time to first usable draft (minutes)</li>"
      "<li>Revision count (aim for 1)</li>"
      "<li>Error rate (facts links formatting)</li>"
      "<li>Cost per published piece (tools plus time)</li>"
      "</ul>"
    )

  if not has_section(body_html, "FAQ"):
    add.append(
      "<h2>FAQ</h2>"
      "<p><strong>How do I start fast?</strong> Start with one template and one checklist.</p>"
      "<p><strong>When should I pay?</strong> When you use the workflow weekly.</p>"
      "<p><strong>How do I reduce wrong info?</strong> Add verification and require evidence.</p>"
      "<p><strong>How do I scale?</strong> Automate only after quality is stable.</p>"
      "<p><strong>What if I get stuck?</strong> Shrink the scope and ship one small output.</p>"
    )

  if add:
    body_html = body_html.strip() + "".join(add)

  return body_html

# -----------------------------
# FREE image search sources
#   1) Wikimedia Commons
#   2) Openverse (CC)
# -----------------------------
WIKI_API = "https://commons.wikimedia.org/w/api.php"
OPENVERSE_API = "https://api.openverse.engineering/v1/images"

def tokenize(s: str) -> List[str]:
  s = (s or "").lower()
  s = re.sub(r"[^a-z0-9\s\-]", " ", s)
  parts = re.split(r"\s+", s)
  out = []
  for p in parts:
    p = p.strip("- ").strip()
    if not p:
      continue
    if len(p) <= 2:
      continue
    out.append(p)
  return out

def candidate_relevance_score(keyword: str, title: str, meta_text: str) -> int:
  kt = tokenize(keyword)
  if not kt:
    return 0

  hay = f"{title} {meta_text}".lower()
  score = 0

  for t in kt:
    if t in hay:
      score += 3

  if keyword.lower().strip() and keyword.lower().strip() in hay:
    score += 6

  bad = [
    "logo", "icon", "diagram", "clipart", "svg", "vector", "flag", "coat of arms",
    "seal", "watermark", "meme", "screenshot", "poster"
  ]
  for b in bad:
    if b in hay:
      score -= 3

  return score

def wikimedia_search_candidates(keyword: str, query: str, limit: int = 50) -> List[Dict[str, str]]:
  params = {
    "action": "query",
    "format": "json",
    "origin": "*",
    "generator": "search",
    "gsrsearch": query,
    "gsrlimit": str(limit),
    "gsrnamespace": "6",
    "prop": "imageinfo",
    "iiprop": "url|size|extmetadata",
    "iiextmetadatafilter": "ImageDescription|ObjectName|Categories|LicenseShortName|UsageTerms|Artist|Credit",
  }
  try:
    r = requests.get(WIKI_API, params=params, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    data = r.json()
    pages = (data.get("query") or {}).get("pages") or {}

    out: List[Dict[str, str]] = []
    for _, page in pages.items():
      title = str(page.get("title") or "")
      infos = page.get("imageinfo") or []
      if not infos:
        continue
      ii = infos[0]
      url = str(ii.get("url") or "")
      if not url:
        continue

      width = int(ii.get("width") or 0)
      height = int(ii.get("height") or 0)
      if width < MIN_W or height < MIN_H:
        continue

      ext = ii.get("extmetadata") or {}
      def get_meta(k: str) -> str:
        v = ext.get(k) or {}
        if isinstance(v, dict):
          return str(v.get("value") or "")
        return str(v or "")

      meta_text = " ".join([
        strip_tags(get_meta("ImageDescription")),
        strip_tags(get_meta("ObjectName")),
        strip_tags(get_meta("Categories")),
        strip_tags(get_meta("LicenseShortName")),
        strip_tags(get_meta("UsageTerms")),
        strip_tags(get_meta("Artist")),
        strip_tags(get_meta("Credit")),
      ]).strip()

      out.append({"url": url, "title": title, "meta": meta_text, "src": "wikimedia"})
    return out
  except Exception:
    return []

def openverse_search_candidates(keyword: str, query: str, limit: int = 50) -> List[Dict[str, str]]:
  """
  Openverse returns CC licensed content.
  We filter to image with large size when possible.
  """
  params = {
    "q": query,
    "page_size": str(limit),
    "mature": "false",
    # allow common CC families including CC0
    "license_type": "commercial,modification",  # conservative for ads use
  }
  try:
    r = requests.get(OPENVERSE_API, params=params, timeout=HTTP_TIMEOUT, headers={"User-Agent": "mingmonglife-bot/1.0"})
    r.raise_for_status()
    data = r.json() or {}
    results = data.get("results") or []
    out: List[Dict[str, str]] = []

    for it in results:
      url = str(it.get("url") or "")
      if not url:
        continue
      w = int(it.get("width") or 0)
      h = int(it.get("height") or 0)
      if w and h:
        if w < MIN_W or h < MIN_H:
          continue

      title = str(it.get("title") or "")
      meta_text = " ".join([
        str(it.get("description") or ""),
        str(it.get("tags") or ""),
        str(it.get("creator") or ""),
        str(it.get("license") or ""),
        str(it.get("provider") or ""),
        str(it.get("source") or ""),
      ]).strip()

      out.append({"url": url, "title": title, "meta": meta_text, "src": "openverse"})
    return out
  except Exception:
    return []

def download_image(url: str) -> Optional[bytes]:
  try:
    r = requests.get(url, timeout=HTTP_TIMEOUT, headers={"User-Agent": "mingmonglife-bot/1.0"})
    r.raise_for_status()
    ct = (r.headers.get("content-type") or "").lower()
    if "image" not in ct:
      return None
    b = r.content
    if not b or len(b) < MIN_IMG_BYTES:
      return None
    return b
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

def bootstrap_used_images_from_assets():
  used = load_used_images()
  ASSETS_POSTS_DIR.mkdir(parents=True, exist_ok=True)

  exts = {".jpg", ".jpeg", ".png", ".webp"}
  changed = 0

  for p in ASSETS_POSTS_DIR.rglob("*"):
    if not p.is_file():
      continue
    if p.suffix.lower() not in exts:
      continue
    try:
      b = p.read_bytes()
      h = sha256_bytes(b)
      if h not in used:
        used.add(h)
        changed += 1
    except Exception:
      continue

  if changed:
    save_used_images(used)

# -----------------------------
# Query expansion for "deeply related" images
# -----------------------------
def expand_image_queries(keyword: str) -> List[str]:
  """
  No AI needed.
  Heuristic expansions.
  Also include a few structure patterns for tools topics.
  """
  k = keyword.strip()
  low = k.lower()

  base = [k]

  # add common photo intent words
  base += [
    f"{k} photo",
    f"{k} realistic photo",
    f"{k} in office",
    f"{k} at work",
    f"{k} laptop",
    f"{k} desk",
  ]

  # reduce too abstract queries
  if any(x in low for x in ["ai", "chatgpt", "automation", "workflow", "productivity"]):
    base += [
      "person using laptop in office",
      "team meeting in office laptop",
      "remote work laptop coffee",
      "notebook planning desk",
      "business analytics dashboard laptop",
    ]

  if any(x in low for x in ["make money", "side hustle", "freelance", "income"]):
    base += [
      "freelancer working laptop",
      "online business laptop desk",
      "invoice paperwork desk",
      "home office workspace",
    ]

  if any(x in low for x in ["review", "pricing", "comparison", "alternatives"]):
    base += [
      "product comparison table",
      "shopping decision laptop",
      "price tag retail shelf",
    ]

  # de-dup while preserving order
  seen = set()
  out = []
  for q in base:
    qq = " ".join(q.split()).strip()
    if not qq:
      continue
    if qq.lower() in seen:
      continue
    seen.add(qq.lower())
    out.append(qq)
  return out

def pick_unique_images_for_post(keyword: str, slug: str, count: int) -> List[str]:
  """
  1 hour budget
  Must be FREE
  Must be strongly relevant
  Must never repeat across site
  Sources:
    Wikimedia + Openverse
  """
  ensure_dirs(slug)

  used_global = load_used_images()
  used_in_post: Set[str] = set()
  saved_paths: List[str] = []

  start = time.time()
  budget = max(60, IMAGE_SEARCH_BUDGET_SECONDS)

  queries = expand_image_queries(keyword)

  # strict then loosen a bit but still related
  strict_min_score = 7
  loose_min_score = 4

  # search loop
  round_i = 0
  while len(saved_paths) < count and (time.time() - start) < budget:
    round_i += 1

    # rotate queries
    q = queries[(round_i - 1) % len(queries)]

    # build source queries
    # wikimedia supports filetype:bitmap
    wiki_q = f'filetype:bitmap "{q}"'
    wiki_q2 = f'filetype:bitmap {q} photo'
    wiki_q3 = f'filetype:bitmap {q}'

    # collect candidates
    cands: List[Dict[str, str]] = []
    cands += wikimedia_search_candidates(keyword, wiki_q, limit=50)
    time.sleep(IMAGE_SEARCH_SLEEP_SECONDS)
    cands += wikimedia_search_candidates(keyword, wiki_q2, limit=50)
    time.sleep(IMAGE_SEARCH_SLEEP_SECONDS)
    cands += wikimedia_search_candidates(keyword, wiki_q3, limit=50)
    time.sleep(IMAGE_SEARCH_SLEEP_SECONDS)

    cands += openverse_search_candidates(keyword, q, limit=50)
    time.sleep(IMAGE_SEARCH_SLEEP_SECONDS)

    # score and sort
    scored: List[Tuple[int, Dict[str, str]]] = []
    for c in cands:
      s = candidate_relevance_score(keyword, c.get("title", ""), c.get("meta", ""))
      scored.append((s, c))
    scored.sort(key=lambda x: x[0], reverse=True)

    # try strict first then loose
    for min_score in (strict_min_score, loose_min_score):
      for s, c in scored:
        if len(saved_paths) >= count:
          break
        if s < min_score:
          break

        url = c.get("url") or ""
        if not url:
          continue

        b = download_image(url)
        time.sleep(IMAGE_SEARCH_SLEEP_SECONDS)
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

      if len(saved_paths) >= count:
        break

    # if nothing found for a while, slightly relax size constraints by 10% near end of budget
    if (time.time() - start) > (budget * 0.75) and len(saved_paths) == 0:
      # last resort, still no AI, still relevance gated
      global MIN_W, MIN_H, MIN_IMG_BYTES
      MIN_W = max(900, int(MIN_W * 0.9))
      MIN_H = max(600, int(MIN_H * 0.9))
      MIN_IMG_BYTES = max(35000, int(MIN_IMG_BYTES * 0.8))

  save_used_images(used_global)
  return saved_paths

# -----------------------------
# LLM content generation (longer + deeper)
# -----------------------------
def llm_generate_article(keyword: str) -> Dict[str, str]:
  if not client:
    title = keyword.title()
    desc = f"A practical guide about {keyword}."
    cat = "AI Tools"
    body = (
      "<h2>TL;DR</h2>"
      "<ul><li>Pick one workflow and reuse it weekly</li><li>Measure time saved</li><li>Upgrade only if ROI is clear</li></ul>"
      "<h2>Scenario</h2><p>You work 9 to 6 and need results fast with low budget.</p>"
      "<h2>Workflow</h2><ul><li>Define outcome</li><li>Pick tools</li><li>Run checklist</li><li>Store template</li></ul>"
      "<h2>Quick comparison</h2>"
      "<table><thead><tr><th>Option</th><th>Best for</th><th>Cost</th><th>Gotcha</th></tr></thead>"
      "<tbody><tr><td>Free</td><td>Testing</td><td>$0</td><td>Limits</td></tr>"
      "<tr><td>Paid</td><td>Daily</td><td>$10 to $30</td><td>Lock in</td></tr>"
      "<tr><td>Automation</td><td>Scale</td><td>$20 to $80</td><td>Maintenance</td></tr></tbody></table>"
      "<h2>Templates</h2><p>Role Goal Context Output Quality rules</p>"
      "<h2>FAQ</h2><p>Start small then scale.</p>"
    )
    body = ensure_core_sections(body)
    while len(strip_tags(body)) < TARGET_MIN_CHARS:
      body += f"<p>{esc(desc)} {esc(desc)} {esc(desc)}</p>"
    return {"title": title, "description": desc, "category": cat, "body": body}

  sys = (
    "You are a senior editor for a practical blog for young professionals in the US and Europe. "
    "Write content that is specific and actionable. "
    "No fluff. No generic definitions. "
    "Use concrete numbers, tradeoffs, and realistic constraints. "
    "Write in natural American English. "
    "Short paragraphs. "
    "Do not mention being an AI. "
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
    "Rules for body_html:\n"
    f"- Visible text length at least {TARGET_MIN_CHARS} characters\n"
    "- Start with <h2>TL;DR</h2> then a <ul> with 3 to 5 bullets\n"
    "- Include <h2>Scenario</h2> with a specific persona and constraints\n"
    "- Include <h2>Workflow</h2> with step by step bullets and time estimates\n"
    "- Include <h2>Tool options</h2> with 3 options and when to choose each\n"
    "- Include <h2>Quick comparison</h2> and a HTML table with at least 3 rows and columns: Option, Best for, Cost, Gotcha\n"
    "- Include <h2>Decision rules</h2>\n"
    "- Include <h2>Example walkthrough</h2>\n"
    "- Include <h2>Common mistakes</h2> with 7 to 10 bullets\n"
    "- Include <h2>Templates</h2> with at least 3 copy ready templates or checklists\n"
    "- Include <h2>Metrics to track</h2>\n"
    "- Include <h2>FAQ</h2> with 5 questions and answers\n"
    "- Use numbers where reasonable\n"
    "- Avoid vague claims\n\n"
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

  if len(strip_tags(body)) < TARGET_MIN_CHARS:
    user2 = (
      f"Improve and expand the article HTML below to reach at least {TARGET_MIN_CHARS} visible characters. "
      "Add numbers, examples, constraints, and checklists. "
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

  body = ensure_core_sections(body)

  while len(strip_tags(body)) < TARGET_MIN_CHARS:
    body += f"<p>{esc(description)} {esc(description)} {esc(description)}</p>"

  return {"title": title, "description": description, "category": category, "body": body}

# -----------------------------
# Evenly distribute images in body
# -----------------------------
def inject_images_evenly(body_html: str, image_paths: List[str], title: str) -> str:
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

  # hard guarantee: never reuse any existing site image
  bootstrap_used_images_from_assets()

  posts = safe_read_json(POSTS_JSON, [])
  if not isinstance(posts, list):
    posts = []

  keywords = load_keywords()
  if not keywords:
    raise SystemExit("keywords.json has no keywords")

  existing_slugs = set([p.get("slug") for p in posts if isinstance(p, dict)])

  made = 0
  tries = 0
  while made < POSTS_PER_RUN and tries < POSTS_PER_RUN * 12:
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

    # strict: if no good free images, do not publish a post
    if not images:
      print(f"Skip (no high quality related free images within budget): {slug}")
      continue

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
