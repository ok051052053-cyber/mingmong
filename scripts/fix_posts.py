import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
POSTS_JSON = ROOT / "posts.json"

IMG_RE = re.compile(r'<img[^>]+src=["\']([^"\']+)["\']', re.IGNORECASE)


def extract_first_image_from_html(html_path: Path) -> str:
    if not html_path.exists():
        return ""

    try:
        html = html_path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return ""

    matches = IMG_RE.findall(html)
    if not matches:
        return ""

    for src in matches:
        src = (src or "").strip()

        if not src:
            continue

        if src.startswith("data:"):
            continue

        if "logo" in src.lower():
            continue

        if "icon" in src.lower():
            continue

        return src

    return ""


def normalize_image_path(src: str, post_url: str) -> str:
    src = (src or "").strip()
    post_url = (post_url or "").strip()

    if not src:
        return ""

    if src.startswith("http://") or src.startswith("https://"):
        return src

    if src.startswith("/"):
        return src.lstrip("/")

    if src.startswith("assets/"):
        return src

    if src.startswith("../"):
        while src.startswith("../"):
            src = src[3:]
        return src

    if src.startswith("./"):
        src = src[2:]

    # post_url 예: posts/abc.html
    # 글 내부에서 src="assets/posts/..." 이면 그대로 유지
    if src.startswith("assets/"):
        return src

    # 글 내부에서 src="images/1.jpg" 처럼 상대경로면
    # posts/ 폴더 기준으로 맞춰줌
    post_dir = Path(post_url).parent
    normalized = (post_dir / src).as_posix()

    return normalized


def main():
    if not POSTS_JSON.exists():
        print("posts.json not found")
        return

    posts = json.loads(POSTS_JSON.read_text(encoding="utf-8"))

    changed = 0
    missing = 0

    for post in posts:
        post_url = (post.get("url") or "").strip()
        html_path = ROOT / post_url if post_url else None

        if not html_path or not html_path.exists():
            missing += 1
            continue

        first_img = extract_first_image_from_html(html_path)

        if not first_img:
            missing += 1
            continue

        normalized = normalize_image_path(first_img, post_url)

        current_image = (post.get("image") or "").strip()
        current_thumbnail = (post.get("thumbnail") or "").strip()

        if current_image != normalized:
            post["image"] = normalized
            changed += 1

        if current_thumbnail != normalized:
            post["thumbnail"] = normalized
            changed += 1

    POSTS_JSON.write_text(
        json.dumps(posts, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )

    print(f"Updated fields: {changed}")
    print(f"Posts without usable image: {missing}")


if __name__ == "__main__":
    main()
