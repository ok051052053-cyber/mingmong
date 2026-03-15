import os
import json
import base64
from pathlib import Path

import requests


SITE_URL = "https://mingmonglife.com"

ROOT = Path(__file__).resolve().parents[1]
POSTS_JSON = ROOT / "posts.json"
UPLOADED_JSON = ROOT / "data" / "pinterest_uploaded.json"
IMAGES_DIR = ROOT / "pinterest_images"

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
PINTEREST_ACCESS_TOKEN = os.getenv("PINTEREST_ACCESS_TOKEN", "").strip()

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


def log(msg: str) -> None:
    print(msg, flush=True)


def load_json(path: Path, default):
    if not path.exists():
        return default
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def full_url(url: str) -> str:
    url = (url or "").strip()
    if url.startswith("http://") or url.startswith("https://"):
        return url
    return f"{SITE_URL}/{url.lstrip('/')}"


def pinterest_headers(json_mode: bool = False) -> dict:
    headers = {
        "Authorization": f"Bearer {PINTEREST_ACCESS_TOKEN}",
    }
    if json_mode:
        headers["Content-Type"] = "application/json"
    return headers


def safe_preview(text: str, limit: int = 1000) -> str:
    if not text:
        return ""
    return text[:limit]


def require_env() -> None:
    if not OPENAI_API_KEY:
        raise RuntimeError("Missing OPENAI_API_KEY")
    if not PINTEREST_ACCESS_TOKEN:
        raise RuntimeError("Missing PINTEREST_ACCESS_TOKEN")


def test_pinterest_token() -> dict:
    """
    인증 확인 + 보드 목록 확인
    """
    url = "https://api.pinterest.com/v5/boards"
    r = requests.get(url, headers=pinterest_headers(), timeout=30)

    log(f"[TOKEN TEST] STATUS: {r.status_code}")
    log(f"[TOKEN TEST] BODY: {safe_preview(r.text, 1500)}")

    if r.status_code != 200:
        raise RuntimeError(
            f"Pinterest token test failed with {r.status_code}. "
            f"Response: {safe_preview(r.text, 1000)}"
        )

    data = r.json()

    items = data.get("items", [])
    if items:
        log("[TOKEN TEST] Boards found:")
        for item in items[:20]:
            board_id = item.get("id")
            name = item.get("name")
            owner = ((item.get("owner") or {}).get("username")) if isinstance(item.get("owner"), dict) else None
            log(f"  - name={name!r} id={board_id!r} owner={owner!r}")
    else:
        log("[TOKEN TEST] No boards returned.")

    return data


def generate_image(title: str, slug: str) -> Path:
    prompt = f"""
Create a vertical Pinterest pin image.
Size: 1024x1536.
Clean modern editorial style.
Large readable headline.
Bright premium background.
High click-through design.
No watermark.
No logo.

Headline text:
{title}
""".strip()

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

    log(f"[OPENAI] Generating image for slug={slug!r}")
    r = requests.post(url, headers=headers, json=payload, timeout=120)
    log(f"[OPENAI] STATUS: {r.status_code}")

    if r.status_code != 200:
        log(f"[OPENAI] BODY: {safe_preview(r.text, 1500)}")
        r.raise_for_status()

    data = r.json()
    img_b64 = data["data"][0]["b64_json"]
    img = base64.b64decode(img_b64)

    IMAGES_DIR.mkdir(parents=True, exist_ok=True)
    path = IMAGES_DIR / f"{slug}.png"
    with open(path, "wb") as f:
        f.write(img)

    log(f"[OPENAI] Saved image -> {path}")
    return path


def register_media_upload() -> dict:
    """
    Pinterest media 등록
    공식 docs 상 media 등록 후 업로드 정보를 받는 흐름이다. :contentReference[oaicite:1]{index=1}
    """
    url = "https://api.pinterest.com/v5/media"
    payload = {
        "media_type": "image"
    }

    r = requests.post(
        url,
        headers=pinterest_headers(json_mode=True),
        json=payload,
        timeout=60,
    )

    log(f"[MEDIA REGISTER] STATUS: {r.status_code}")
    log(f"[MEDIA REGISTER] BODY: {safe_preview(r.text, 1500)}")

    r.raise_for_status()
    return r.json()


def upload_binary_to_pinterest(upload_url: str, image_path: Path) -> None:
    with open(image_path, "rb") as f:
        binary = f.read()

    headers = {
        "Content-Type": "image/png"
    }

    r = requests.put(upload_url, headers=headers, data=binary, timeout=120)

    log(f"[MEDIA BINARY UPLOAD] STATUS: {r.status_code}")
    log(f"[MEDIA BINARY UPLOAD] BODY: {safe_preview(r.text, 800)}")

    r.raise_for_status()


def create_pin(title: str, description: str, link: str, board_id: str, media_source: dict) -> dict:
    url = "https://api.pinterest.com/v5/pins"

    payload = {
        "board_id": board_id,
        "title": title[:100],
        "description": description[:500],
        "link": link,
        "media_source": media_source,
    }

    log(f"[PIN CREATE] PAYLOAD: {json.dumps(payload, ensure_ascii=False)[:1500]}")

    r = requests.post(
        url,
        headers=pinterest_headers(json_mode=True),
        json=payload,
        timeout=60,
    )

    log(f"[PIN CREATE] STATUS: {r.status_code}")
    log(f"[PIN CREATE] BODY: {safe_preview(r.text, 2000)}")

    r.raise_for_status()
    return r.json()


def upload_pin(title: str, description: str, link: str, image_path: Path, board_id: str) -> dict:
    """
    1) media 등록
    2) 업로드 URL 받기
    3) 이미지 바이너리 업로드
    4) pin 생성
    """
    media_info = register_media_upload()

    media_id = media_info.get("id") or media_info.get("media_id")
    upload_url = media_info.get("upload_url")

    if not upload_url:
        raise RuntimeError(
            f"Pinterest media register succeeded but no upload_url found. "
            f"Response: {json.dumps(media_info, ensure_ascii=False)}"
        )

    upload_binary_to_pinterest(upload_url, image_path)

    # Pinterest 응답 구조가 앱/계정 상태에 따라 조금 다를 수 있어서
    # media_source를 두 방식으로 순차 시도
    attempts = []

    if media_id:
        attempts.append({
            "source_type": "image_id",
            "image_id": media_id,
        })

    attempts.append({
        "source_type": "image_url",
        "url": upload_url,
    })

    last_error = None

    for media_source in attempts:
        try:
            log(f"[PIN CREATE] Trying media_source={media_source}")
            return create_pin(title, description, link, board_id, media_source)
        except requests.HTTPError as e:
            last_error = e
            log(f"[PIN CREATE] Attempt failed: {e}")

    if last_error:
        raise last_error

    raise RuntimeError("Pin creation failed for unknown reason.")


def describe_post(post: dict, final_url: str) -> str:
    title = (post.get("title") or "").strip()
    desc = (post.get("description") or "").strip()
    text = f"{title}\n\n{desc}\n\nRead more: {final_url}"
    return text[:500]


def main() -> None:
    require_env()

    posts = load_json(POSTS_JSON, [])
    uploaded = load_json(UPLOADED_JSON, [])

    if not isinstance(posts, list):
        raise RuntimeError("posts.json must be a JSON array")
    if not isinstance(uploaded, list):
        uploaded = []

    uploaded_slugs = set(str(x).strip() for x in uploaded if str(x).strip())

    log(f"[CONFIG] DAILY_LIMIT={DAILY_LIMIT}")
    log(f"[CONFIG] TOKEN_PREFIX={(PINTEREST_ACCESS_TOKEN or '')[:8]}")
    log(f"[CONFIG] POSTS={len(posts)} ALREADY_UPLOADED={len(uploaded_slugs)}")

    boards_data = test_pinterest_token()

    # 디버그만 하고 끝내고 싶으면 secret 또는 workflow env로 1 설정
    if DEBUG_ONLY:
        log("[DEBUG] PINTEREST_DEBUG_ONLY=1, stopping after board/token test.")
        return

    IMAGES_DIR.mkdir(parents=True, exist_ok=True)
    UPLOADED_JSON.parent.mkdir(parents=True, exist_ok=True)

    count = 0

    for post in posts:
        if count >= DAILY_LIMIT:
            break

        slug = (post.get("slug") or "").strip()
        if not slug:
            continue
        if slug in uploaded_slugs:
            continue

        title = (post.get("title") or "").strip()
        final_url = full_url(post.get("url", ""))
        category = (post.get("category") or "").strip()
        board_id = BOARD_MAP.get(category, "").strip()

        if not board_id:
            log(f"[SKIP] No board mapped for category={category!r}")
            continue

        log("=" * 80)
        log(f"[POST] slug={slug}")
        log(f"[POST] category={category}")
        log(f"[POST] board_id={board_id}")
        log(f"[POST] url={final_url}")

        try:
            image_path = generate_image(title, slug)
            description = describe_post(post, final_url)
            result = upload_pin(title, description, final_url, image_path, board_id)

            uploaded.append(slug)
            save_json(UPLOADED_JSON, uploaded)
            uploaded_slugs.add(slug)
            count += 1

            log(f"[SUCCESS] Uploaded slug={slug}")
            log(f"[SUCCESS] Pinterest response: {json.dumps(result, ensure_ascii=False)[:1500]}")

        except Exception as e:
            log(f"[ERROR] Failed for slug={slug}: {repr(e)}")
            raise

    log(f"[DONE] Uploaded count this run: {count}")


if __name__ == "__main__":
    main()
