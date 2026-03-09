import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
POSTS_JSON = ROOT / "posts.json"
ASSETS_POSTS_DIR = ROOT / "assets" / "posts"

DEFAULT_IMAGE = "assets/default-thumb.jpg"


def find_existing_image(slug: str) -> str:
    candidates = [
        ASSETS_POSTS_DIR / slug / "1.jpg",
        ASSETS_POSTS_DIR / slug / "1.jpeg",
        ASSETS_POSTS_DIR / slug / "1.png",
        ASSETS_POSTS_DIR / slug / "1.webp",
        ASSETS_POSTS_DIR / f"{slug}.jpg",
        ASSETS_POSTS_DIR / f"{slug}.jpeg",
        ASSETS_POSTS_DIR / f"{slug}.png",
        ASSETS_POSTS_DIR / f"{slug}.webp",
    ]

    for path in candidates:
        if path.exists():
            return path.relative_to(ROOT).as_posix()

    return ""


def main():
    with open(POSTS_JSON, "r", encoding="utf-8") as f:
        posts = json.load(f)

    changed = 0

    for post in posts:
        slug = (post.get("slug") or "").strip()
        image = (post.get("image") or "").strip()
        thumbnail = (post.get("thumbnail") or "").strip()

        if not slug:
            continue

        found = find_existing_image(slug)

        if found:
            if not image:
                post["image"] = found
                changed += 1
            if not thumbnail:
                post["thumbnail"] = found
                changed += 1
        else:
            if not image:
                post["image"] = DEFAULT_IMAGE
                changed += 1
            if not thumbnail:
                post["thumbnail"] = DEFAULT_IMAGE
                changed += 1

    with open(POSTS_JSON, "w", encoding="utf-8") as f:
        json.dump(posts, f, ensure_ascii=False, indent=2)

    print(f"Updated fields: {changed}")


if __name__ == "__main__":
    main()
