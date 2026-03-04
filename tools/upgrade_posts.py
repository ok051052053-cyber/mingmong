from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
POSTS = ROOT / "posts"

SIDEBAR_HTML = """
    <aside class="post-aside">
      <div class="sidecard">
        <h3>Categories</h3>
        <div class="catlist">
          <a class="catitem" href="../category.html?cat=AI%20Tools"><span class="caticon">🤖</span><span class="cattext"><span class="catname">AI Tools</span><span class="catsub">Tools and workflows</span></span></a>
          <a class="catitem" href="../category.html?cat=Productivity"><span class="caticon">⚡</span><span class="cattext"><span class="catname">Productivity</span><span class="catsub">Time and focus</span></span></a>
          <a class="catitem" href="../category.html?cat=Make%20Money"><span class="caticon">💰</span><span class="cattext"><span class="catname">Make Money</span><span class="catsub">Freelance and digital</span></span></a>
          <a class="catitem" href="../category.html?cat=Reviews"><span class="caticon">🧾</span><span class="cattext"><span class="catname">Reviews</span><span class="catsub">Comparisons and pricing</span></span></a>
        </div>
      </div>
    </aside>
""".strip("\n")


def ensure_post_shell_wrapped(text: str) -> str:
    # 이미 wrapper 있으면 그대로
    if 'class="container post-page"' in text and 'class="post-shell"' in text:
        return text

    # <main class="container"> 형태를 post-page로 바꿈
    text = text.replace(
        '<main class="container">',
        '<main class="container post-page">\n  <div class="post-shell">'
    )

    # </main> 앞에 </div> 닫기
    text = text.replace(
        '</main>',
        '  </div>\n</main>'
    )

    return text


def ensure_has_aside_class(text: str) -> str:
    # post-shell 없으면 패스
    if 'class="post-shell"' not in text:
        return text

    # 이미 has-aside 있으면 그대로
    if 'class="post-shell has-aside"' in text:
        return text

    return text.replace('class="post-shell"', 'class="post-shell has-aside"', 1)


def ensure_sidebar_exists(text: str) -> str:
    # 이미 aside 있으면 그대로
    if 'class="post-aside"' in text:
        return text

    # post-shell 닫히기 직전에 aside 넣기
    marker = "  </div>\n</main>"
    if marker in text:
        return text.replace(marker, f"{SIDEBAR_HTML}\n{marker}", 1)

    # 혹시 닫는 형태가 다르면 post-shell 끝을 찾아서 삽입
    alt_marker = "</div>\n</main>"
    if alt_marker in text and 'class="post-shell"' in text:
        return text.replace(alt_marker, f"\n{SIDEBAR_HTML}\n{alt_marker}", 1)

    return text


def main() -> None:
    html_files = list(POSTS.glob("*.html"))

    updated = 0
    skipped = 0

    for file in html_files:
        text = file.read_text(encoding="utf-8")

        before = text
        text = ensure_post_shell_wrapped(text)
        text = ensure_has_aside_class(text)
        text = ensure_sidebar_exists(text)

        if text != before:
            file.write_text(text, encoding="utf-8")
            updated += 1
        else:
            skipped += 1

    print(f"updated {updated} posts")
    print(f"skipped {skipped} posts")


if __name__ == "__main__":
    main()
