import os
import json
from datetime import datetime
from openai import OpenAI

client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])

KEYWORD = os.environ.get("KEYWORD", "korea travel tips").strip()
CATEGORY = os.environ.get("CATEGORY", "Trends & News").strip()

slug = "-".join(KEYWORD.lower().split())

prompt = f"""
You are writing for a premium blog called MingMong.
Write a 1200+ word SEO article about: {KEYWORD}

Requirements
- Output ONLY valid HTML for inside <div class="prose"> (no <html>, no <head>, no <body>)
- Use <h2>, <h3>, <p>, <ul>, <li>, <hr>, <strong>
- Include: quick checklist section, FAQ section (3-5 questions), practical tips
- Tone: clean, modern, helpful, not salesy
"""

res = client.chat.completions.create(
    model="gpt-4o-mini",
    messages=[{"role": "user", "content": prompt}]
)

content = res.choices[0].message.content or ""

today = datetime.today().strftime("%Y-%m-%d")

html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>{KEYWORD.title()} | MingMong</title>
  <meta name="description" content="{KEYWORD}" />
  <link rel="stylesheet" href="../style.css" />
</head>

<body class="page-bg">

<header class="topbar">
  <div class="container topbar-inner">
    <a class="brand" href="../index.html">
      <span class="mark" aria-hidden="true"></span>
      <span>MingMong</span>
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
    <p class="breadcrumb"><a href="../index.html">Home</a> | <span>{CATEGORY}</span></p>

    <h1 class="post-title-xl">{KEYWORD.title()}</h1>

    <p class="post-lead">
      A clean, practical guide about {KEYWORD}.
    </p>

    <div class="post-meta">
      <span class="badge">📰 {CATEGORY}</span>
      <span>•</span>
      <span>Updated: {today}</span>
      <span>•</span>
      <span>Read time: 6–9 min</span>
    </div>
  </section>

  <section class="layout">

    <article class="card article">
      <div class="prose">

        <div class="grid-2">
          <figure class="photo">
            <img src="../assets/posts/{slug}/1.jpg" alt="{KEYWORD} photo 1" loading="lazy" />
            <figcaption class="caption">A visual cue related to {KEYWORD}.</figcaption>
          </figure>

          <figure class="photo">
            <img src="../assets/posts/{slug}/2.jpg" alt="{KEYWORD} photo 2" loading="lazy" />
            <figcaption class="caption">Another angle on the topic.</figcaption>
          </figure>
        </div>

        <figure class="photo" style="margin-top:14px;">
          <img src="../assets/posts/{slug}/3.jpg" alt="{KEYWORD} photo 3" loading="lazy" />
          <figcaption class="caption">A practical detail you can use.</figcaption>
        </figure>

        {content}

      </div>
    </article>

    <aside class="sidebar">

      <div class="card related hotnews">
        <h4>Hot News!</h4>
        <div class="side-links" id="hotNewsList">
          <a href="{slug}.html">
            <span>{KEYWORD.title()}</span>
            <small>Hot</small>
          </a>
        </div>
      </div>

      <div class="card related">
        <h4>More to read</h4>
        <div class="side-links">
          <a href="carry-less-travel-kit-20s.html">
            <span>Carry-Less Travel Kit</span>
            <small>Gear</small>
          </a>
          <a href="focus-stack-digital-nomads.html">
            <span>Focus Stack for Remote Work</span>
            <small>Tools</small>
          </a>
        </div>
      </div>

    </aside>

  </section>
</main>

<footer class="footer">
  <div class="container">
    <div>© 2026 MingMong</div>
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

os.makedirs("posts", exist_ok=True)
with open(f"posts/{slug}.html", "w", encoding="utf-8") as f:
    f.write(html)

data = []
if os.path.exists("posts.json"):
    with open("posts.json", "r", encoding="utf-8") as f:
        data = json.load(f)

existing = [p for p in data if p.get("slug") != slug]

new_item = {
    "slug": slug,
    "title": KEYWORD.title(),
    "description": f"{KEYWORD}",
    "category": CATEGORY,
    "date": today,
    "views": 0
}

data = [new_item] + existing

with open("posts.json", "w", encoding="utf-8") as f:
    json.dump(data, f, indent=2, ensure_ascii=False)

print("POST CREATED:", slug)
