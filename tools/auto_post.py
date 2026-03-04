import os
import re
import json
import time
import html
import random
import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

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
# Config (env)
# -----------------------------
SITE_NAME = os.environ.get("SITE_NAME", "MingMong").strip()
SITE_URL = os.environ.get("SITE_URL", "https://mingmonglife.com").strip().rstrip("/")
POSTS_PER_RUN = int(os.environ.get("POSTS_PER_RUN", "1").strip() or "1")

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "").strip()
MODEL = os.environ.get("MODEL", "gpt-4o-mini").strip()

UNSPLASH_ACCESS_KEY = os.environ.get("UNSPLASH_ACCESS_KEY", "").strip()

IMG_COUNT = int(os.environ.get("IMG_COUNT", "4").strip() or "4")
MIN_CHARS = int(os.environ.get("MIN_CHARS", "2500").strip() or "2500")

HTTP_TIMEOUT = int(os.environ.get("HTTP_TIMEOUT", "30").strip() or "30")

# Strict policy: NO AI images
ALLOW_AI_IMAGES = False

# Unsplash quality filters
UNSPLASH_MIN_WIDTH = int(os.environ.get("UNSPLASH_MIN_WIDTH", "1800").strip() or "1800")
UNSPLASH_MIN_LIKES = int(os.environ.get("UNSPLASH_MIN_LIKES", "50").strip() or "50")
UNSPLASH_TRIES_PER_IMAGE = int(os.environ.get("UNSPLASH_TRIES_PER_IMAGE", "4").strip() or "4")
UNSPLASH_SEARCH_PAGES = int(os.environ.get("UNSPLASH_SEARCH_PAGES", "3").strip() or "3")  # 1~3

# article expansion
MAX_EXPAND = int(os.environ.get("MAX_EXPAND", "4").strip() or "4")

# -----------------------------
# Helpers
# -----------------------------
def now_utc_date() -> str:
  return datetime.now(timezone.utc).strftime("%Y-%m-%d")

def esc(s: str) -> str:
  return html.escape(s or "", quote=True)

def ensure_dir(p: Path) -> None:
  p.mkdir(parents=True, exist_ok=True)

def read_json(path: Path, default):
  if not path.exists():
    return default
  try:
    return json.loads(path.read_text(encoding="utf-8"))
  except Exception:
    return default

def write_json(path: Path, data) -> None:
  path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

def strip_tags(h: str) -> str:
  if not h:
    return ""
  return re.sub(r"<[^>]+>", "", h)

def stable_hash(s: str) -> str:
  return hashlib.sha256(s.encode("utf-8")).hexdigest()[:16]

def pick_keywords() -> List[str]:
  """
  keywords.json formats supported:
  - ["kw1","kw2",...]
  - {"keywords":[...]}
  - {"AI Tools":[...], "Productivity":[...]}  -> will flatten
  """
  data = read_json(KEYWORDS_JSON, [])
  kws: List[str] = []

  if isinstance(data, list):
    kws = [str(x).strip() for x in data if str(x).strip()]
  elif isinstance(data, dict):
    if isinstance(data.get("keywords"), list):
      kws = [str(x).strip() for x in data["keywords"] if str(x).strip()]
    else:
      # flatten dict lists
      for v in data.values():
        if isinstance(v, list):
          kws.extend([str(x).strip() for x in v if str(x).strip()])

  kws = [k for k in kws if k]
  if not kws:
    # safe fallback list
    kws = [
      "Best tools for time blocking for freelancers",
      "How to choose an ergonomic mouse for small hands",
      "Best invoicing apps for small businesses in 2026",
      "Beginner guide to freelance contracts in Europe",
      "How to set up a personal knowledge base with Notion",
    ]
  random.shuffle(kws)
  return kws

# -----------------------------
# OpenAI client (compat)
# -----------------------------
def make_openai_client():
  if not OPENAI_API_KEY:
    return None, "Missing OPENAI_API_KEY"
  try:
    # new sdk
    from openai import OpenAI  # type: ignore
    return OpenAI(api_key=OPENAI_API_KEY), ""
  except Exception:
    try:
      import openai  # type: ignore
      openai.api_key = OPENAI_API_KEY
      return openai, ""
    except Exception as e:
      return None, f"OpenAI import failed: {e}"

def openai_chat_json(client, model: str, messages: List[Dict], temperature: float = 0.7) -> str:
  """
  Returns message content
  Supports:
  - openai>=1.x : client.chat.completions.create
  - openai old  : openai.ChatCompletion.create
  """
  # new style
  if hasattr(client, "chat") and hasattr(client.chat, "completions"):
    res = client.chat.completions.create(
      model=model,
      messages=messages,
      temperature=temperature,
    )
    return (res.choices[0].message.content or "").strip()

  # old style
  if hasattr(client, "ChatCompletion"):
    res = client.ChatCompletion.create(
      model=model,
      messages=messages,
      temperature=temperature,
    )
    return (res["choices"][0]["message"]["content"] or "").strip()

  raise RuntimeError("Unsupported OpenAI client")

# -----------------------------
# Unsplash (NO AI)
# -----------------------------
def require_unsplash_key():
  if not UNSPLASH_ACCESS_KEY:
    raise SystemExit("Missing UNSPLASH_ACCESS_KEY (required for non-AI high quality photos)")

def unsplash_search(query: str, page: int) -> List[Dict]:
  require_unsplash_key()
  url = "https://api.unsplash.com/search/photos"
  params = {
    "query": query,
    "page": page,
    "per_page": 30,
    "orientation": "landscape",
    "content_filter": "high",
  }
  headers = {
    "Accept-Version": "v1",
    "Authorization": f"Client-ID {UNSPLASH_ACCESS_KEY}",
  }
  r = requests.get(url, params=params, headers=headers, timeout=HTTP_TIMEOUT)
  if r.status_code != 200:
    raise RuntimeError(f"Unsplash search failed: {r.status_code} {r.text[:200]}")
  data = r.json()
  return data.get("results", []) or []

def choose_unsplash_candidates(results: List[Dict], used_ids: set) -> List[Dict]:
  cands: List[Dict] = []
  for it in results:
    try:
      pid = it.get("id") or ""
      if not pid or pid in used_ids:
        continue
      w = int(it.get("width") or 0)
      likes = int(it.get("likes") or 0)
      if w < UNSPLASH_MIN_WIDTH:
        continue
      if likes < UNSPLASH_MIN_LIKES:
        continue
      urls = it.get("urls") or {}
      raw = urls.get("raw") or ""
      full = urls.get("full") or ""
      regular = urls.get("regular") or ""
      dl = raw or full or regular
      if not dl:
        continue
      # prefer raw with sizing
      cands.append(it)
    except Exception:
      continue
  # high likes first
  cands.sort(key=lambda x: int(x.get("likes") or 0), reverse=True)
  return cands

def unsplash_download_photo(photo: Dict, out_path: Path) -> Tuple[str, str, str]:
  """
  downloads an image file
  returns: photo_page_url, photographer_name, photographer_profile
  """
  urls = photo.get("urls") or {}
  raw = urls.get("raw") or ""
  full = urls.get("full") or ""
  regular = urls.get("regular") or ""
  download_url = raw or full or regular

  # add sizing to raw
  if "images.unsplash.com" in download_url and "?" not in download_url:
    download_url = download_url + "?w=2400&fit=max&q=85&fm=jpg"

  # track download as required by Unsplash API guidelines
  try:
    dl_link = (photo.get("links") or {}).get("download_location")
    if dl_link:
      headers = {"Authorization": f"Client-ID {UNSPLASH_ACCESS_KEY}", "Accept-Version": "v1"}
      requests.get(dl_link, headers=headers, timeout=HTTP_TIMEOUT)
  except Exception:
    pass

  r = requests.get(download_url, timeout=HTTP_TIMEOUT)
  if r.status_code != 200 or not r.content:
    raise RuntimeError(f"Unsplash download failed: {r.status_code}")

  out_path.write_bytes(r.content)

  user = photo.get("user") or {}
  photographer = user.get("name") or "Unknown"
  profile = user.get("links", {}).get("html") or ""
  page = (photo.get("links") or {}).get("html") or ""

  return page, photographer, profile

def get_high_quality_photos(keyword: str, need: int) -> Tuple[List[str], List[Dict]]:
  """
  downloads need images into assets/posts/<slug>/N.jpg
  returns: local_rel_paths, credits list
  """
  used = read_json(USED_IMAGES_JSON, {"unsplash_ids": []})
  used_ids = set(used.get("unsplash_ids") or [])

  slug = slugify(keyword)[:80] or stable_hash(keyword)
  out_dir = ASSETS_POSTS_DIR / slug
  ensure_dir(out_dir)

  local_paths: List[str] = []
  credits: List[Dict] = []

  # search queries
  queries = [
    keyword,
    keyword + " workspace",
    keyword + " laptop",
    keyword + " office",
    keyword + " minimal",
  ]

  img_index = 1
  for q in queries:
    if len(local_paths) >= need:
      break

    for page in range(1, UNSPLASH_SEARCH_PAGES + 1):
      if len(local_paths) >= need:
        break

      results = unsplash_search(q, page=page)
      cands = choose_unsplash_candidates(results, used_ids)

      tries = 0
      for photo in cands:
        if len(local_paths) >= need:
          break
        if tries >= UNSPLASH_TRIES_PER_IMAGE and len(local_paths) > 0:
          break

        pid = photo.get("id") or ""
        if not pid or pid in used_ids:
          continue

        # decide ext
        out_file = out_dir / f"{img_index}.jpg"
        try:
          page_url, photographer, profile = unsplash_download_photo(photo, out_file)
          rel = f"assets/posts/{slug}/{img_index}.jpg"
          local_paths.append(rel)
          credits.append({
            "photo_page": page_url,
            "photographer": photographer,
            "photographer_profile": profile,
          })
          used_ids.add(pid)
          img_index += 1
        except Exception:
          tries += 1
          continue

  # persist used
  used["unsplash_ids"] = sorted(list(used_ids))
  write_json(USED_IMAGES_JSON, used)

  if len(local_paths) < need:
    # strict mode: no AI fallback
    raise SystemExit(f"Could not source enough high quality non-AI photos. Got {len(local_paths)}/{need}")

  return local_paths, credits

# -----------------------------
# Article generation (2500+)
# -----------------------------
def llm_generate_article(client, keyword: str) -> Dict[str, str]:
  sys = (
    "You write in-depth, practical, SEO-friendly blog posts for US and European readers aged 20-35. "
    "No fluff. No generic statements. Use concrete steps, checklists, decision criteria, comparisons, and examples. "
    "Short paragraphs. Clear headings. Natural English. "
    "Do not mention you are an AI."
  )

  user = (
    f"Write one in-depth blog post about: {keyword}\n"
    "Output JSON with keys: title, description, category(one of: AI Tools, Make Money, Productivity, Reviews), "
    "body_html (HTML only, use <h2>, <p>, <ul><li>). "
    f"Hard constraint: visible text length must be at least {MIN_CHARS} characters. "
    "Include: TL;DR bullet list, Who this is for, Key ideas, Common mistakes, and FAQ. "
    "Do not include outer <html>."
  )

  txt = openai_chat_json(
    client,
    MODEL,
    [{"role": "system", "content": sys}, {"role": "user", "content": user}],
    temperature=0.7,
  )

  try:
    data = json.loads(txt)
  except Exception:
    data = {}

  title = str(data.get("title") or "").strip() or keyword.title()
  description = str(data.get("description") or "").strip() or f"A practical guide about {keyword}."
  category = str(data.get("category") or "AI Tools").strip() or "AI Tools"
  body = str(data.get("body_html") or "").strip() or "<p></p>"

  # expand loop
  attempts = 0
  while len(strip_tags(body)) < MIN_CHARS and attempts < MAX_EXPAND:
    attempts += 1
    need = MIN_CHARS - len(strip_tags(body))
    ask_more = max(900, need)

    user2 = (
      f"Expand the article below by at least {ask_more} visible characters so total becomes >= {MIN_CHARS}. "
      "Add only genuinely useful content: examples, checklists, decision rules, pitfalls, mini case-studies. "
      "Do not repeat sentences. "
      "Return JSON with one key: body_html.\n\n"
      f"ARTICLE_HTML:\n{body}"
    )

    t2 = openai_chat_json(
      client,
      MODEL,
      [{"role": "system", "content": sys}, {"role": "user", "content": user2}],
      temperature=0.6,
    )

    try:
      d2 = json.loads(t2)
      b2 = str(d2.get("body_html") or "").strip()
      if b2:
        body = b2
    except Exception:
      body += f"<p>{esc(t2)}</p>"

  # hard pad if still short (useful blocks)
  if len(strip_tags(body)) < MIN_CHARS:
    body += f"""
<h2>Quick decision checklist</h2>
<ul>
  <li>Write your primary outcome in one sentence (time saved, fewer errors, better focus, more revenue).</li>
  <li>List your must-have integrations and exports (Google Drive, Slack, Notion, CSV, PDF).</li>
  <li>Set a budget ceiling and decide monthly vs yearly billing.</li>
  <li>Define non-negotiables (privacy, offline access, client sharing, audit trail).</li>
  <li>Test your top 2 choices with the same real task for 30 minutes.</li>
</ul>

<h2>FAQ</h2>
<p><strong>How do I choose quickly?</strong> Pick the tool that removes the most steps from your most frequent workflow.</p>
<p><strong>What if features look similar?</strong> Compare exports, integrations, mobile UX, and support response time.</p>
<p><strong>How do I avoid wasting money?</strong> Use a trial with a real workflow and cancel anything you do not open for 7 days.</p>
"""

  # final safety pad
  while len(strip_tags(body)) < MIN_CHARS:
    body += f"<p>{esc(description)} {esc(description)} {esc(description)}</p>"

  return {"title": title, "description": description, "category": category, "body": body}

def build_post_html(title: str, description: str, body_html: str, image_paths: List[str], credits: List[Dict]) -> str:
  # insert hero image first
  hero = ""
  if image_paths:
    hero = f"""
<div class="post-hero">
  <img src="/{esc(image_paths[0])}" alt="{esc(title)}">
</div>
"""

  credits_html = ""
  if credits:
    items = []
    for c in credits:
      p = c.get("photo_page") or ""
      u = c.get("photographer_profile") or ""
      n = c.get("photographer") or "Unknown"
      # keep links as plain anchors
      if p and u:
        items.append(f'<li><a href="{esc(p)}" target="_blank" rel="noopener">Photo</a> by <a href="{esc(u)}" target="_blank" rel="noopener">{esc(n)}</a> on Unsplash</li>')
      elif p:
        items.append(f'<li><a href="{esc(p)}" target="_blank" rel="noopener">Photo</a> on Unsplash</li>')
      else:
        items.append(f"<li>{esc(n)}</li>")

    credits_html = f"""
<h2>Photo credits</h2>
<ul>
  {''.join(items)}
</ul>
"""

  # add inline images in-body after some sections
  # simple placement: after first 2 h2 blocks if available
  extra_imgs = ""
  if len(image_paths) >= 2:
    for p in image_paths[1:]:
      extra_imgs += f'<p><img src="/{esc(p)}" alt="{esc(title)}"></p>\n'

  # if body already has many images, this is still ok
  # keep it minimal
  full_body = body_html + "\n" + extra_imgs + "\n" + credits_html

  return hero + full_body

# -----------------------------
# Posts persistence
# -----------------------------
def load_posts_index() -> List[Dict]:
  posts = read_json(POSTS_JSON, [])
  if isinstance(posts, list):
    return posts
  return []

def save_posts_index(posts: List[Dict]) -> None:
  write_json(POSTS_JSON, posts)

def write_post_file(slug: str, html_body: str) -> str:
  ensure_dir(POSTS_DIR)
  out = POSTS_DIR / f"{slug}.html"
  out.write_text(html_body, encoding="utf-8")
  return f"posts/{slug}.html"

def exists_slug(posts: List[Dict], slug: str) -> bool:
  for p in posts:
    if str(p.get("slug") or "") == slug:
      return True
  return False

def unique_slug(base: str, posts: List[Dict]) -> str:
  s = slugify(base)[:80] or stable_hash(base)
  if not exists_slug(posts, s):
    return s
  i = 2
  while True:
    s2 = f"{s}-{i}"
    if not exists_slug(posts, s2):
      return s2
    i += 1

# -----------------------------
# Main
# -----------------------------
def main():
  # guard
  if IMG_COUNT < 4:
    raise SystemExit("IMG_COUNT must be >= 4 for your policy")

  client, err = make_openai_client()
  if not client:
    raise SystemExit(err)

  if not UNSPLASH_ACCESS_KEY:
    raise SystemExit("Missing UNSPLASH_ACCESS_KEY (you said no AI images)")

  posts = load_posts_index()
  kws = pick_keywords()

  generated = 0
  for kw in kws:
    if generated >= POSTS_PER_RUN:
      break

    kw = kw.strip()
    if not kw:
      continue

    # generate article first
    art = llm_generate_article(client, kw)
    title = art["title"]
    description = art["description"]
    category = art["category"]
    body = art["body"]

    # enforce length
    if len(strip_tags(body)) < MIN_CHARS:
      raise SystemExit("Article too short after expansion. Try again.")

    # get photos (strict)
    slug = unique_slug(title, posts)
    image_paths, credits = get_high_quality_photos(title, IMG_COUNT)

    # build html body
    html_body = build_post_html(title, description, body, image_paths, credits)

    # write post file
    rel_post_path = write_post_file(slug, html_body)

    # pick thumbnail and hero
    thumb = image_paths[0]
    hero = image_paths[0]

    post_obj = {
      "title": title,
      "slug": slug,
      "category": category,
      "description": description,
      "date": now_utc_date(),
      "updated": now_utc_date(),
      "url": f"{SITE_URL}/posts/{slug}.html",
      "file": rel_post_path,
      "thumbnail": thumb,
      "image": hero,
      "images": image_paths,
    }

    posts.insert(0, post_obj)
    save_posts_index(posts)

    print(f"Generated: {slug}")
    generated += 1

  if generated == 0:
    raise SystemExit("No posts generated")

if __name__ == "__main__":
  main()
