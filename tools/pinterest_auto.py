import os
import json
import requests
import base64
from pathlib import Path

SITE_URL = "https://mingmonglife.com"

ROOT = Path(__file__).resolve().parents[1]

POSTS_JSON = ROOT / "posts.json"
UPLOADED_JSON = ROOT / "data" / "pinterest_uploaded.json"
IMAGES_DIR = ROOT / "pinterest_images"

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
PINTEREST_ACCESS_TOKEN = os.getenv("PINTEREST_ACCESS_TOKEN")

BOARD_MAP = {
    "AI Tools": os.getenv("PINTEREST_BOARD_AI_TOOLS"),
    "Side Hustles": os.getenv("PINTEREST_BOARD_SIDE_HUSTLES"),
    "Make Money": os.getenv("PINTEREST_BOARD_MAKE_MONEY"),
    "Productivity": os.getenv("PINTEREST_BOARD_PRODUCTIVITY"),
    "Software Reviews": os.getenv("PINTEREST_BOARD_SOFTWARE_REVIEWS"),
    "Investing": os.getenv("PINTEREST_BOARD_INVESTING"),
}

DAILY_LIMIT = int(os.getenv("PINTEREST_DAILY_LIMIT", "10"))

def load_json(path, default):
    if not path.exists():
        return default
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def save_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def full_url(url):
    if url.startswith("http"):
        return url
    return f"{SITE_URL}/{url}"

def generate_image(title, slug):

    prompt = f"""
Create a vertical Pinterest pin image.
Size: 1024x1536.
Clean modern style.
Large readable headline.

Headline text:
{title}

Bright background
High click-through design
No watermark
"""

    url = "https://api.openai.com/v1/images/generations"

    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json"
    }

    data = {
        "model": "gpt-image-1-mini",
        "size": "1024x1536",
        "quality": "low",
        "prompt": prompt
    }

    res = requests.post(url, headers=headers, json=data)
    res.raise_for_status()

    img_b64 = res.json()["data"][0]["b64_json"]
    img = base64.b64decode(img_b64)

    path = IMAGES_DIR / f"{slug}.png"

    with open(path, "wb") as f:
        f.write(img)

    return path


def upload_pin(title, description, link, image_path, board_id):

    upload_url = "https://api.pinterest.com/v5/media"

    headers = {
        "Authorization": f"Bearer {PINTEREST_ACCESS_TOKEN}"
    }

    files = {
        "media": open(image_path, "rb")
    }

    r = requests.post(upload_url, headers=headers, files=files)
    r.raise_for_status()

    media_id = r.json()["id"]

    pin_url = "https://api.pinterest.com/v5/pins"

    payload = {
        "board_id": board_id,
        "title": title[:100],
        "description": description[:500],
        "link": link,
        "media_source": {
            "source_type": "image_id",
            "image_id": media_id
        }
    }

    r = requests.post(pin_url, headers=headers, json=payload)
    r.raise_for_status()

    return r.json()


def main():

    posts = load_json(POSTS_JSON, [])
    uploaded = load_json(UPLOADED_JSON, [])

    uploaded_slugs = set(uploaded)

    IMAGES_DIR.mkdir(exist_ok=True)

    count = 0

    for post in posts:

        if count >= DAILY_LIMIT:
            break

        slug = post["slug"]

        if slug in uploaded_slugs:
            continue

        title = post["title"]
        url = full_url(post["url"])
        category = post.get("category")

        board = BOARD_MAP.get(category)

        if not board:
            print("No board for", category)
            continue

        print("Generating image for:", title)

        image = generate_image(title, slug)

        description = f"{title}\n\nRead more: {url}"

        print("Uploading pin:", title)

        upload_pin(title, description, url, image, board)

        uploaded.append(slug)
        save_json(UPLOADED_JSON, uploaded)

        count += 1

        print("Uploaded:", slug)


if __name__ == "__main__":
    main()
