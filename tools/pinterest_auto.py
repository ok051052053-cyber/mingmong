import os
import json
import base64
import time
from pathlib import Path

import requests


SITE_URL = "https://mingmonglife.com"

ROOT = Path(__file__).resolve().parents[1]
POSTS_JSON = ROOT / "posts.json"
UPLOADED_JSON = ROOT / "data" / "pinterest_uploaded.json"
IMAGES_DIR = ROOT / "pinterest_images"

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
PINTEREST_ACCESS_TOKEN = os.getenv("PINTEREST_ACCESS_TOKEN", "").strip()

PINTEREST_MODE = os.getenv("PINTEREST_MODE", "full").strip().lower()
PINTEREST_SEO_MODE = os.getenv("PINTEREST_SEO_MODE", "1") == "1"

BOARD_MAP = {
    "AI Tools": os.getenv("PINTEREST_BOARD_AI_TOOLS", "").strip(),
    "Side Hustles": os.getenv("PINTEREST_BOARD_SIDE_HUSTLES", "").strip(),
    "Make Money": os.getenv("PINTEREST_BOARD_MAKE_MONEY", "").strip(),
    "Productivity": os.getenv("PINTEREST_BOARD_PRODUCTIVITY", "").strip(),
    "Software Reviews": os.getenv("PINTEREST_BOARD_SOFTWARE_REVIEWS", "").strip(),
    "Investing": os.getenv("PINTEREST_BOARD_INVESTING", "").strip(),
}

DAILY_LIMIT = int(os.getenv("PINTEREST_DAILY_LIMIT", "5"))
DEBUG_ONLY = os.getenv("PINTEREST_DEBUG_ONLY", "0") == "1"


def log(msg: str):
    print(msg, flush=True)


def load_json(path: Path, default):
    if not path.exists():
        return default
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: Path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def full_url(url: str):
    url = (url or "").strip()
    if url.startswith("http"):
        return url
    return f"{SITE_URL}/{url.lstrip('/')}"


def pinterest_headers(json_mode=False):
    headers = {
        "Authorization": f"Bearer {PINTEREST_ACCESS_TOKEN}",
    }
    if json_mode:
        headers["Content-Type"] = "application/json"
    return headers


def require_env():
    if not OPENAI_API_KEY:
        raise RuntimeError("Missing OPENAI_API_KEY")
    if not PINTEREST_ACCESS_TOKEN:
        raise RuntimeError("Missing PINTEREST_ACCESS_TOKEN")


def test_pinterest_token():
    url = "https://api.pinterest.com/v5/boards"
    r = requests.get(url, headers=pinterest_headers(), timeout=30)

    log(f"[TOKEN TEST] STATUS {r.status_code}")

    if r.status_code != 200:
        raise RuntimeError(f"Pinterest token invalid {r.text}")


def generate_image(title, slug):

    prompt = f"""
Create a Pinterest pin.
Vertical 1024x1536.
Large headline text.

Headline:
{title}
"""

    url = "https://api.openai.com/v1/images/generations"

    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json",
    }

    payload = {
        "model": "gpt-image-1-mini",
        "size": "1024x1536",
        "quality": "low",
        "prompt": prompt,
    }

    log(f"[OPENAI] Generate image {slug}")

    r = requests.post(url, headers=headers, json=payload, timeout=120)

    if r.status_code != 200:
        raise RuntimeError(r.text)

    img = base64.b64decode(r.json()["data"][0]["b64_json"])

    IMAGES_DIR.mkdir(parents=True, exist_ok=True)

    path = IMAGES_DIR / f"{slug}.png"

    with open(path, "wb") as f:
        f.write(img)

    return path


def verify_public_image(url, retries=12, delay=15):

    for i in range(retries):

        r = requests.get(url)

        log(f"[VERIFY IMAGE] {url} -> {r.status_code}")

        if r.status_code == 200:
            return

        time.sleep(delay)

    raise RuntimeError("Image not public yet")


def create_pin(title, description, link, board_id, image_url):

    url = "https://api.pinterest.com/v5/pins"

    payload = {
        "board_id": board_id,
        "title": title[:100],
        "description": description[:500],
        "link": link,
        "media_source": {
            "source_type": "image_url",
            "url": image_url,
        },
    }
    
    log(f"[PIN DESC] {description}")
    log(f"[PIN CREATE PAYLOAD] {json.dumps(payload, ensure_ascii=False)[:1500]}")
    
    r = requests.post(
        url,
        headers=pinterest_headers(True),
        json=payload,
        timeout=60,
    )

    log(f"[PIN CREATE] {r.status_code}")

    if r.status_code >= 300:
        raise RuntimeError(r.text)

    return r.json()


def build_seo_keywords(title: str, category: str) -> str:
    raw = f"{title} {category}".lower()

    replacements = {
        "ai tools": ["ai tools", "automation", "productivity tools"],
        "side hustles": ["side hustle", "online business", "passive income"],
        "make money": ["make money online", "income ideas", "digital income"],
        "productivity": ["productivity tips", "workflow", "time management"],
        "software reviews": ["software review", "best tools", "app comparison"],
        "investing": ["investing tips", "beginner investing", "wealth building"],
    }

    bucket = replacements.get((category or "").strip().lower(), [])
    parts = [title.strip()] + bucket
    parts = [x for x in parts if x]

    seen = []
    for p in parts:
        if p not in seen:
            seen.append(p)

    return ", ".join(seen[:6])


def generate_pinterest_description(title: str, category: str, final_url: str, fallback_desc: str = "") -> str:
    fallback_desc = (fallback_desc or "").strip()
    keywords = build_seo_keywords(title, category)

    if not PINTEREST_SEO_MODE:
        text = f"{title}\n\n{fallback_desc}\n\nRead more: {final_url}"
        return text[:500]

    prompt = f"""
Write a Pinterest pin description in English.

Goal:
- High click-through rate
- SEO-friendly
- Natural and human
- Clear and specific
- No hashtags
- No emojis
- 2 to 4 short sentences
- Under 400 characters
- Include strong search intent keywords naturally
- End with a soft CTA

Title: {title}
Category: {category}
Related keywords: {keywords}
Article URL: {final_url}
Fallback article description: {fallback_desc}

Return plain text only.
""".strip()

    try:
        url = "https://api.openai.com/v1/chat/completions"
        headers = {
            "Authorization": f"Bearer {OPENAI_API_KEY}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": "gpt-4.1-mini",
            "temperature": 0.7,
            "messages": [
                {
                    "role": "system",
                    "content": "You write short Pinterest descriptions that feel natural and are optimized for search and clicks."
                },
                {
                    "role": "user",
                    "content": prompt
                },
            ],
        }

        r = requests.post(url, headers=headers, json=payload, timeout=60)

        if r.status_code != 200:
            log(f"[PINTEREST SEO] STATUS {r.status_code}")
            log(f"[PINTEREST SEO] BODY {r.text[:1200]}")
            text = f"{title}. {fallback_desc} Learn more here: {final_url}"
            return text[:500]

        data = r.json()
        text = data["choices"][0]["message"]["content"].strip()
        text = " ".join(text.split())

        if final_url not in text:
            text = f"{text} Read more: {final_url}"

        return text[:500]

    except Exception as e:
        log(f"[PINTEREST SEO] FALLBACK {repr(e)}")
        text = f"{title}. {fallback_desc} Read more: {final_url}"
        return text[:500]


def describe_post(post, final_url):
    title = (post.get("title") or "").strip()
    desc = (post.get("description") or "").strip()
    category = (post.get("category") or "").strip()

    return generate_pinterest_description(
        title=title,
        category=category,
        final_url=final_url,
        fallback_desc=desc,
    )


def main():

    require_env()

    posts = load_json(POSTS_JSON, [])
    uploaded = load_json(UPLOADED_JSON, [])

    uploaded_slugs = set(uploaded)

    test_pinterest_token()

    count = 0

    for post in posts:

        if count >= DAILY_LIMIT:
            break

        slug = post.get("slug")

        if not slug:
            continue

        if slug in uploaded_slugs:
            continue

        title = post.get("title")
        final_url = full_url(post.get("url"))
        category = post.get("category")

        board_id = BOARD_MAP.get(category)

        if not board_id:
            continue

        public_image_url = f"{SITE_URL}/pinterest_images/{slug}.png"

        log("=" * 60)
        log(f"[POST] {slug}")

        try:

            # MODE 1 이미지 생성
            if PINTEREST_MODE == "generate-only":

                generate_image(title, slug)
            
                log("[MODE] generate only")

                count += 1
                continue

            # MODE 2 업로드만
            if PINTEREST_MODE == "upload-only":

                verify_public_image(public_image_url)

                description = describe_post(post, final_url)

                create_pin(
                    title,
                    description,
                    final_url,
                    board_id,
                    public_image_url,
                )

                uploaded.append(slug)
                save_json(UPLOADED_JSON, uploaded)

                log("[SUCCESS] Pinterest uploaded")

                count += 1
                continue

            # FULL MODE (테스트용)

            generate_image(title, slug)

            verify_public_image(public_image_url)

            description = describe_post(post, final_url)

            create_pin(
                title,
                description,
                final_url,
                board_id,
                public_image_url,
            )

            uploaded.append(slug)
            save_json(UPLOADED_JSON, uploaded)

            count += 1

        except Exception as e:

            log(f"[ERROR] {e}")
            raise

    log(f"[DONE] Uploaded {count}")


if __name__ == "__main__":
    main()
