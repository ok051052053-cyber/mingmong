# tools/auto_post.py
import os
import re
import json
import time
import html
import random
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path

import requests
import feedparser
from slugify import slugify
from openai import OpenAI

# -----------------------------
# Config
# -----------------------------
ROOT = Path(__file__).resolve().parents[1]
POSTS_DIR = ROOT / "posts"
ASSETS_POSTS_DIR = ROOT / "assets" / "posts"
POSTS_JSON = ROOT / "posts.json"

SITE_NAME = os.environ.get("SITE_NAME", "MingMong").strip()
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "").strip()
MODEL = os.environ.get("MODEL", "gpt-4o-mini").strip()

# 기본 3개 권장
POSTS_PER_RUN = int(os.environ.get("POSTS_PER_RUN", "3"))

# 이미지 개수 고정 6
IMG_COUNT = 6

# 요청 타임아웃
HTTP_TIMEOUT = 25

# GitHub Actions 환경에서 간헐적 429, 503 대비
HTTP_RETRY = 3
HTTP_SLEEP = 1.2

client = OpenAI(api_key=OPENAI_API_KEY)

# -----------------------------
# Helpers
# -----------------------------
def now_utc_date():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def ensure_dirs():
    POSTS_DIR.mkdir(parents=True, exist_ok=True)
    ASSETS_POSTS_DIR.mkdir(parents=True, exist_ok=True)


def load_posts_json():
    if POSTS_JSON.exists():
        try:
            return json.loads(POSTS_JSON.read_text(encoding="utf-8"))
        except Exception:
            return []
    return []


def save_posts_json(posts):
    POSTS_JSON.write_text(json.dumps(posts, indent=2, ensure_ascii=False), encoding="utf-8")


def safe_text(s: str):
    return html.escape(s or "", quote=True)


def clean_title(t: str):
    t = re.sub(r"\s+", " ", (t or "").strip())
    return t[:160].strip()


def has_digit(s: str):
    return bool(re.search(r"\d", s or ""))


def title_with_number_or_year(base_title: str, category: str):
    t = clean_title(base_title)
    if has_digit(t):
        return t
    cat = (category or "").lower()
    if "cool" in cat:
        return f"5 Things About {t} (2026)"
    if "guide" in cat:
        return f"2026: How to {t}"
    return f"2026: {t}"


def make_meta_description(keyword: str, source_name: str):
    # 애드센스에 불리한 과도한 선정 문구는 피함
    base = f"{keyword}. What it is. Why it matters. Practical tips you can use in 2026."
    if source_name:
        base += f" Source: {source_name}."
    return base[:155]


def pick_category_for_item(title: str):
    t = (title or "").lower()
    cool_keys = [
        "iphone", "android", "chip", "ai", "gadget", "phone", "laptop", "app", "tool",
        "review", "camera", "tesla", "meta", "openai", "google", "samsung", "qualcomm",
        "nvidia", "amd", "intel", "wearable", "headset",
    ]
    guide_keys = [
        "how to", "guide", "tips", "checklist", "best way", "beginner", "explained",
        "what is", "steps", "plan", "avoid", "setup",
    ]
    if any(k in t for k in guide_keys):
        return "Guides"
    if any(k in t for k in cool_keys):
        return "Cool Finds"
    return "Trends & News"


def rss_url_for_query(q: str):
    q = (q or "").strip()[:140]
    qp = urllib.parse.quote(q)
    return f"https://news.google.com/rss/search?q={qp}&hl=en-US&gl=US&ceid=US:en"


def fetch_rss_items(queries, limit_each=12):
    items = []
    for q in queries:
        url = rss_url_for_query(q)
        feed = feedparser.parse(url)
        for e in getattr(feed, "entries", [])[:limit_each]:
            title = clean_title(getattr(e, "title", "") or "")
            link = getattr(e, "link", "") or ""
            source = ""
            try:
                source = (e.source.title or "").strip()
            except Exception:
                source = ""
            if title and link:
                items.append({"title": title, "link": link, "source": source})
        time.sleep(0.4)

    # 중복 제거
    seen = set()
    uniq = []
    for it in items:
        k = it["title"].lower()
        if k in seen:
            continue
        seen.add(k)
        uniq.append(it)

    random.shuffle(uniq)
    return uniq


def pick_items_for_daily_mix():
    # 애드센스 관점에서 "사건사고 자극"은 리스크가 큼
    # 대신 논쟁, 정책, 제품 이슈, 트렌드 변화 같은 안전한 고관심 키워드로 구성
    trend_queries = [
        "viral trend 2026 explained",
        "social media backlash trend 2026",
        "policy change debate 2026",
        "creator economy trend 2026",
        "privacy controversy tech 2026",
    ]
    cool_queries = [
        "new chip launch 2026 wearable",
        "smartphone camera leak 2026",
        "best productivity app 2026",
        "new AI tool release 2026",
        "laptop ultralight 2026",
    ]
    guide_queries = [
        "how to travel lighter weekend trip",
        "how to save money in 2026 practical guide",
        "how to build focus habits 2026",
        "how to choose a carry-on 2026 guide",
        "how to set up remote work tools 2026",
    ]

    trends = fetch_rss_items(trend_queries, limit_each=10)
    cools = fetch_rss_items(cool_queries, limit_each=10)
    guides = fetch_rss_items(guide_queries, limit_each=10)

    # 섞어서 1개씩 뽑되 부족하면 풀에서 보충
    picked = []
    if trends:
        picked.append(trends[0])
    if cools:
        picked.append(cools[0])
    if guides:
        picked.append(guides[0])

    pool = trends[1:] + cools[1:] + guides[1:]
    random.shuffle(pool)

    while len(picked) < POSTS_PER_RUN and pool:
        picked.append(pool.pop(0))

    return picked


def choose_internal_links(existing_posts, current_slug, k=2):
    candidates = [p for p in existing_posts if p.get("slug") and p.get("slug") != current_slug]
    random.shuffle(candidates)
    picks = candidates[:k]
    out = []
    for p in picks:
        out.append({"slug": p["slug"], "title": p.get("title", p["slug"])})
    return out


# -----------------------------
# Images
# -----------------------------
def guess_ext_from_content_type(ct: str):
    ct = (ct or "").lower()
    if "jpeg" in ct or "jpg" in ct:
        return ".jpg"
    if "png" in ct:
        return ".png"
    if "webp" in ct:
        return ".webp"
    if "gif" in ct:
        return ".gif"
    return ""


def http_get(url, params=None, stream=False):
    last_err = None
    for _ in range(HTTP_RETRY):
        try:
            r = requests.get(
                url,
                params=params,
                timeout=HTTP_TIMEOUT,
                stream=stream,
                headers={"User-Agent": "mingmong-bot/1.0"},
            )
            r.raise_for_status()
            return r
        except Exception as e:
            last_err = e
            time.sleep(HTTP_SLEEP)
    raise last_err


def wikimedia_image_url(query: str):
    api = "https://commons.wikimedia.org/w/api.php"
    params = {
        "action": "query",
        "format": "json",
        "generator": "search",
        "gsrsearch": query,
        "gsrlimit": 3,
        "prop": "imageinfo",
        "iiprop": "url",
        "iiurlwidth": 1600,
    }
    r = http_get(api, params=params, stream=False)
    j = r.json()
    pages = (j.get("query") or {}).get("pages") or {}
    for _, p in pages.items():
        info = (p.get("imageinfo") or [])
        if info:
            return info[0].get("thumburl") or info[0].get("url")
    return None


def download_file_detect_ext(url, out_base_path: Path):
    """
    out_base_path: 확장자 없는 경로 예) /assets/posts/slug/1
    return: 실제 저장된 Path
    """
    r = http_get(url, params=None, stream=True)

    ct = r.headers.get("Content-Type", "")
    ext = guess_ext_from_content_type(ct)

    if not ext:
        parsed = urllib.parse.urlparse(url)
        path = (parsed.path or "").lower()
        for cand in [".jpg", ".jpeg", ".png", ".webp", ".gif"]:
            if path.endswith(cand):
                ext = ".jpg" if cand == ".jpeg" else cand
                break

    if not ext:
        ext = ".jpg"

    out_path = out_base_path.with_suffix(ext)

    with out_path.open("wb") as f:
        for chunk in r.iter_content(chunk_size=1024 * 128):
            if chunk:
                f.write(chunk)

    return out_path


def write_svg_placeholder(path: Path, title: str):
    t = (title or "MingMong").strip()[:48]
    svg = f"""<svg xmlns="http://www.w3.org/2000/svg" width="1600" height="900" viewBox="0 0 1600 900">
  <defs>
    <linearGradient id="g" x1="0" y1="0" x2="1" y2="1">
      <stop offset="0" stop-color="#e0f2fe"/>
      <stop offset="1" stop-color="#f0f9ff"/>
    </linearGradient>
  </defs>
  <rect width="1600" height="900" rx="48" fill="url(#g)"/>
  <rect x="120" y="120" width="1360" height="660" rx="36" fill="white" opacity="0.70"/>
  <text x="160" y="260" font-family="ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto,Helvetica,Arial" font-size="54" font-weight="800" fill="#0f172a">{html.escape(t)}</text>
</svg>
"""
    path.write_text(svg, encoding="utf-8")


def ensure_images(slug: str, img_queries):
    folder = ASSETS_POSTS_DIR / slug
    folder.mkdir(parents=True, exist_ok=True)

    paths = []
    for i in range(IMG_COUNT):
        q = (img_queries[i] if i < len(img_queries) else img_queries[-1]).strip()

        # 이미 존재하는 파일 재사용
        existing = None
        for ext in [".jpg", ".png", ".webp", ".gif", ".svg"]:
            p = folder / f"{i+1}{ext}"
            if p.exists():
                existing = p
                break

        if existing:
            paths.append(f"../assets/posts/{slug}/{existing.name}")
            continue

        ok = False
        try:
            url = wikimedia_image_url(q)
            if url:
                saved = download_file_detect_ext(url, folder / f"{i+1}")
                paths.append(f"../assets/posts/{slug}/{saved.name}")
                ok = True
        except Exception:
            ok = False

        if not ok:
            svg_path = folder / f"{i+1}.svg"
            write_svg_placeholder(svg_path, q)
            paths.append(f"../assets/posts/{slug}/{i+1}.svg")

        time.sleep(0.35)

    return paths


def build_image_block(src, alt):
    # figcaption 없음
    return f"""
<figure class="photo" style="margin:18px 0;">
  <img src="{src}" alt="{safe_text(alt)}" loading="lazy" />
</figure>
""".strip()


# -----------------------------
# Writing prompts
# -----------------------------
def make_body_prompt(keyword, title, category, internal_links):
    link_hints = ""
    if len(internal_links) >= 2:
        a = internal_links[0]
        b = internal_links[1]
        link_hints = f"""
Internal links you MUST reference naturally in the body
- <a href="{a['slug']}.html">{a['title']}</a>
- <a href="{b['slug']}.html">{b['title']}</a>
"""

    prompt = f"""
You are writing for a premium blog called {SITE_NAME}.

Topic keyword
{keyword}

Category
{category}

Final page title
{title}

Hard requirements
- Output ONLY valid HTML that goes inside <div class="prose">. No <html>. No <head>. No <body>.
- Use only these tags
<h2> <h3> <p> <ul> <li> <hr> <strong> <a>
- The FIRST paragraph must include the exact keyword once
"{keyword}"
- Include a Quick checklist section
- Include an FAQ section with 3 to 5 questions
- Include practical tips
- Tone is clean modern helpful not salesy
- Avoid unsafe or graphic content
- Do not include captions like "Context image" or "Image:" anywhere
- No medical or legal advice tone

Image placement
- Insert these markers exactly once each
<!--IMG1-->
<!--IMG2-->
<!--IMG3-->
<!--IMG4-->
<!--IMG5-->
<!--IMG6-->
- Spread them evenly through the article
- Each marker goes right after a paragraph that fits an image
- Do not place two markers back to back

SEO
- Make the intro highly clear
- Use descriptive subheads
- Add a short "What happened" and "Why it matters" section
- Keep it original and specific
- No fluff

{link_hints}

Length
- 1200 to 1600 words
"""
    return prompt.strip()


def pick_image_queries(keyword, title, category):
    base = keyword
    cat = (category or "").lower()

    if "cool" in cat:
        return [
            f"{base} product photo",
            f"{base} close up device",
            f"{base} hands using gadget",
            f"{base} app interface concept",
            f"{base} desk workspace modern",
            f"{base} technology abstract",
        ]

    if "guide" in cat:
        return [
            f"{base} checklist concept",
            f"{base} packing flat lay",
            f"{base} travel carry on bag",
            f"{base} map planning notebook",
            f"{base} minimal lifestyle items",
            f"{base} calendar planning",
        ]

    # Trends
    return [
        f"{base} news concept",
        f"{base} social media reaction",
        f"{base} city headline",
        f"{base} discussion crowd",
        f"{base} modern lifestyle trend",
        f"{base} analysis chart newsroom",
    ]


# -----------------------------
# HTML template
# -----------------------------
def build_post_html(
    slug,
    keyword,
    title,
    category,
    description,
    source_link,
    internal_links,
    body_html,
    image_srcs,
):
    today = now_utc_date()

    # 마커 치환
    for idx in range(IMG_COUNT):
        marker = f"<!--IMG{idx+1}-->"
        body_html = body_html.replace(marker, build_image_block(image_srcs[idx], f"{keyword} image {idx+1}"))

    # 마커가 남아있으면 제거
    body_html = re.sub(r"<!--IMG[1-6]-->", "", body_html)

    inline_links_html = ""
    if len(internal_links) >= 2:
        a = internal_links[0]
        b = internal_links[1]
        inline_links_html = f"""
<hr class="hr" />
<p>
  <strong>Related on {safe_text(SITE_NAME)}:</strong>
  <a href="{safe_text(a['slug'])}.html">{safe_text(a['title'])}</a>
  and
  <a href="{safe_text(b['slug'])}.html">{safe_text(b['title'])}</a>
</p>
""".strip()

    more_links = ""
    if len(internal_links) >= 2:
        a = internal_links[0]
        b = internal_links[1]
        more_links = f"""
<a href="{safe_text(a['slug'])}.html"><span>{safe_text(a['title'])}</span><small>Guide</small></a>
<a href="{safe_text(b['slug'])}.html"><span>{safe_text(b['title'])}</span><small>Guide</small></a>
""".strip()

    html_doc = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>{safe_text(title)} | {safe_text(SITE_NAME)}</title>
  <meta name="description" content="{safe_text(description)}" />
  <link rel="stylesheet" href="../style.css" />
</head>

<body class="page-bg">

<header class="topbar">
  <div class="container topbar-inner">
    <a class="brand" href="../index.html">
      <span class="mark" aria-hidden="true"></span>
      <span>{safe_text(SITE_NAME)}</span>
    </a>

    <nav class="nav" aria-label="Primary">
      <a href="../index.html">Home</a>
      <a href="../about.html">About</a>
      <a href="../contact.html">Contact</a>
      <a class="btn primary" href="../index.html">Home</a>
    </nav>
  </div>
</header>

<main class="container">

  <section class="post-hero">
    <p class="breadcrumb"><a href="../index.html">Home</a> | <span>{safe_text(category)}</span></p>

    <h1 class="post-title-xl">{safe_text(title)}</h1>

    <p class="post-lead">
      {safe_text(keyword)}. What it is. Why it matters. Practical tips you can use in 2026.
    </p>

    <div class="post-meta">
      <span class="badge">📰 {safe_text(category)}</span>
      <span>•</span>
      <span>Updated: {today}</span>
      <span>•</span>
      <span>Read time: 8–12 min</span>
    </div>
  </section>

  <section class="layout">

    <article class="card article">
      <div class="prose">
        {body_html}
        {inline_links_html}
        <p style="margin-top:14px;">
          Source:
          <a href="{safe_text(source_link)}" rel="nofollow noopener" target="_blank">Link</a>
        </p>
      </div>
    </article>

    <aside class="sidebar">

      <div class="card related hotnews">
        <h4>Hot News!</h4>
        <div class="side-links" id="hotNewsList">
          <a href="{safe_text(slug)}.html"><span>{safe_text(title)}</span><small>Hot</small></a>
        </div>
      </div>

      <div class="card related">
        <h4>More to read</h4>
        <div class="side-links">
          {more_links or '<a href="../index.html"><span>Browse latest posts</span><small>Home</small></a>'}
        </div>
      </div>

    </aside>

  </section>
</main>

<footer class="footer">
  <div class="container">
    <div>© 2026 {safe_text(SITE_NAME)}</div>
    <div class="footer-links">
      <a href="../privacy.html">Privacy</a>
      <a href="../about.html">About</a>
      <a href="../contact.html">Contact</a>
    </div>
  </div>
</footer>

<script>
(async function () {{
  try {{
    const res = await fetch("../posts.json", {{ cache: "no-store" }});
    const posts = await res.json();
    posts.sort((a, b) => (b.date || "").localeCompare(a.date || ""));

    const hot = [];
    for (const p of posts) {{
      if ((p.category || "").toLowerCase().includes("trends")) hot.push(p);
      if (hot.length >= 5) break;
    }}
    if (hot.length < 5) {{
      for (const p of posts) {{
        if (!hot.find(x => x.slug === p.slug)) hot.push(p);
        if (hot.length >= 5) break;
      }}
    }}

    const el = document.getElementById("hotNewsList");
    if (!el) return;

    el.innerHTML = hot.map((p, idx) => {{
      const title = p.title || "Untitled";
      const tag = (p.category || "News").split("&")[0].trim();
      const url = `${{p.slug}}.html`;
      return `
        <a href="${{url}}">
          <span>${{title}}</span>
          <small>${{idx === 0 ? "Hot" : tag}}</small>
        </a>
      `;
    }}).join("");
  }} catch (e) {{}}
}})();
</script>

</body>
</html>
"""
    return html_doc


# -----------------------------
# Create post
# -----------------------------
def create_post_from_item(item, existing_posts):
    raw_title = clean_title(item.get("title", ""))
    category_guess = pick_category_for_item(raw_title)

    # 강제로 3개 믹스일 때는, 아이템 제목으로 분류된 카테고리 사용
    category = category_guess

    title = title_with_number_or_year(raw_title, category)

    # keyword는 너무 길면 짧게
    keyword = raw_title
    if len(keyword) > 90:
        keyword = keyword[:90].rsplit(" ", 1)[0].strip()

    slug = slugify(title, lowercase=True)
    slug = re.sub(r"-{2,}", "-", slug).strip("-")
    slug = slug[:120].strip("-")

    used = {p.get("slug") for p in existing_posts if p.get("slug")}
    if slug in used:
        slug = f"{slug}-{random.randint(100, 999)}"

    internal_links = choose_internal_links(existing_posts, slug, k=2)

    prompt = make_body_prompt(keyword, title, category, internal_links)

    res = client.chat.completions.create(
        model=MODEL,
        messages=[{"role": "user", "content": prompt}],
    )
    body_html = (res.choices[0].message.content or "").strip()

    # 마커 누락 대비
    for i in range(1, IMG_COUNT + 1):
        m = f"<!--IMG{i}-->"
        if m not in body_html:
            body_html += f"\n<p></p>\n{m}\n"

    img_queries = pick_image_queries(keyword, title, category)
    image_srcs = ensure_images(slug, img_queries)

    description = make_meta_description(keyword, item.get("source", ""))

    html_doc = build_post_html(
        slug=slug,
        keyword=keyword,
        title=title,
        category=category,
        description=description,
        source_link=item.get("link", "#"),
        internal_links=internal_links,
        body_html=body_html,
        image_srcs=image_srcs,
    )

    (POSTS_DIR / f"{slug}.html").write_text(html_doc, encoding="utf-8")

    # 썸네일은 1번 이미지로 저장
    thumb = image_srcs[0] if image_srcs else ""

    new_item = {
        "slug": slug,
        "title": title,
        "description": description,
        "category": category,
        "date": now_utc_date(),
        "views": 0,
        "thumbnail": thumb.replace("../", "") if thumb else "",
    }
    return new_item


def main():
    if not OPENAI_API_KEY:
        raise SystemExit("OPENAI_API_KEY is missing")

    ensure_dirs()

    existing = load_posts_json()

    items = pick_items_for_daily_mix()
    if not items:
        raise SystemExit("No RSS items fetched")

    created = 0
    new_posts = []

    for it in items:
        if created >= POSTS_PER_RUN:
            break
        try:
            new_item = create_post_from_item(it, existing + new_posts)
            new_posts.append(new_item)
            created += 1
            print("CREATED:", new_item["slug"])
        except Exception as e:
            print("SKIP (error):", str(e))
            continue

    if not new_posts:
        raise SystemExit("No posts created")

    # posts.json 앞에 추가
    merged = new_posts + [p for p in existing if p.get("slug") not in {n["slug"] for n in new_posts}]
    save_posts_json(merged)

    print("POSTS CREATED:", created)


if __name__ == "__main__":
    main()
