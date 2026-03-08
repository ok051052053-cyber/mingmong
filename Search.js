(function () {
  let cachedPosts = null;

  function esc(s) {
    return String(s || "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function getPostsPath() {
    const path = window.location.pathname || "";
    if (path.includes("/posts/")) return "../posts.json";
    return "posts.json";
  }

  function getHomePath() {
    const path = window.location.pathname || "";
    if (path.includes("/posts/")) return "../index.html";
    return "index.html";
  }

  function normalize(text) {
    return String(text || "")
      .toLowerCase()
      .replace(/\s+/g, " ")
      .trim();
  }

  function postHref(post) {
    if (post && post.url) return post.url;
    if (post && post.slug) return `posts/${post.slug}.html`;
    return "#";
  }

  function resolveHref(post) {
    const href = postHref(post);
    const path = window.location.pathname || "";
    if (path.includes("/posts/") && !href.startsWith("../")) {
      return `../${href}`;
    }
    return href;
  }

  function dateKey(post) {
    const d = String((post && (post.updated || post.date)) || "");
    const t = Date.parse(d);
    if (!Number.isNaN(t)) return t;
    return 0;
  }

  async function loadPosts() {
    if (cachedPosts) return cachedPosts;

    try {
      const res = await fetch(getPostsPath(), { cache: "no-store" });
      const data = await res.json();
      cachedPosts = Array.isArray(data) ? data.filter(Boolean) : [];
      return cachedPosts;
    } catch (e) {
      cachedPosts = [];
      return cachedPosts;
    }
  }

  function searchPosts(posts, query) {
    const q = normalize(query);
    if (!q) return [];

    const terms = q.split(" ").filter(Boolean);

    const scored = posts
      .filter((post) => post && post.slug)
      .map((post) => {
        const title = normalize(post.title);
        const description = normalize(post.description);
        const category = normalize(post.category);
        const keyword = normalize(post.keyword);
        const cluster = normalize(post.cluster);

        const haystack = [title, description, category, keyword, cluster].join(" ");

        const matchesAll = terms.every((term) => haystack.includes(term));
        if (!matchesAll) return null;

        let score = 0;
        if (title.includes(q)) score += 100;
        if (keyword.includes(q)) score += 70;
        if (description.includes(q)) score += 30;
        if (post.post_type === "pillar") score += 20;
        score += Math.min(Number(post.views || 0), 50);
        score += Math.floor(dateKey(post) / 1000000000);

        return { post, score };
      })
      .filter(Boolean)
      .sort((a, b) => b.score - a.score)
      .map((item) => item.post);

    return scored.slice(0, 10);
  }

  function renderResultCards(results, emptyText) {
    if (!results.length) {
      return `<div class="search-empty">${esc(emptyText)}</div>`;
    }

    return results
      .map((post) => {
        const href = resolveHref(post);
        const category = esc(post.category || "Article");
        const title = esc(post.title || "Untitled");
        const description = esc(post.description || "");
        return `
          <a class="search-result-item" href="${href}">
            <div class="search-result-kicker">${category}</div>
            <div class="search-result-title">${title}</div>
            <div class="search-result-desc">${description}</div>
          </a>
        `;
      })
      .join("");
  }

  function ensureOverlay() {
    if (document.querySelector(".site-search-overlay")) return;

    const html = `
      <div class="site-search-overlay" hidden>
        <div class="site-search-backdrop js-close-search"></div>
        <div class="site-search-modal" role="dialog" aria-modal="true" aria-label="Site search">
          <div class="site-search-modal-head">
            <div class="site-search-modal-title">Search MingMong</div>
            <button type="button" class="site-search-close js-close-search" aria-label="Close search">✕</button>
          </div>

          <form class="site-search-form js-modal-search-form" autocomplete="off">
            <div class="site-search-bar">
              <span class="site-search-icon">🔍</span>
              <input
                type="search"
                class="site-search-input js-modal-search-input"
                placeholder="Search guides, workflows, templates"
                aria-label="Search guides, workflows, templates"
              />
              <button type="submit" class="site-search-submit">Search</button>
            </div>
          </form>

          <div class="site-search-modal-links">
            <a href="${getHomePath()}">Go to home</a>
          </div>

          <div class="site-search-results js-modal-search-results">
            <div class="search-empty">Start typing to search the site.</div>
          </div>
        </div>
      </div>
    `;
    document.body.insertAdjacentHTML("beforeend", html);
  }

  function openOverlay() {
    const overlay = document.querySelector(".site-search-overlay");
    if (!overlay) return;
    overlay.hidden = false;
    document.body.classList.add("search-open");
    const input = overlay.querySelector(".js-modal-search-input");
    if (input) {
      setTimeout(() => input.focus(), 30);
    }
  }

  function closeOverlay() {
    const overlay = document.querySelector(".site-search-overlay");
    if (!overlay) return;
    overlay.hidden = true;
    document.body.classList.remove("search-open");
  }

  async function bindInlineSearch() {
    const form = document.querySelector(".js-inline-search-form");
    const input = document.querySelector(".js-inline-search-input");
    const resultsWrap = document.querySelector(".js-inline-search-results");

    if (!form || !input || !resultsWrap) return;

    const posts = await loadPosts();

    async function runSearch() {
      const query = input.value.trim();
      if (!query) {
        resultsWrap.innerHTML = "";
        resultsWrap.classList.remove("is-active");
        return;
      }

      const results = searchPosts(posts, query);
      resultsWrap.innerHTML = renderResultCards(results, "No matching articles found.");
      resultsWrap.classList.add("is-active");
    }

    form.addEventListener("submit", function (e) {
      e.preventDefault();
      runSearch();
    });

    input.addEventListener("input", function () {
      runSearch();
    });

    document.addEventListener("click", function (e) {
      if (!resultsWrap.contains(e.target) && !form.contains(e.target)) {
        resultsWrap.classList.remove("is-active");
      }
    });
  }

  async function bindModalSearch() {
    ensureOverlay();

    const overlay = document.querySelector(".site-search-overlay");
    const form = document.querySelector(".js-modal-search-form");
    const input = document.querySelector(".js-modal-search-input");
    const resultsWrap = document.querySelector(".js-modal-search-results");

    if (!overlay || !form || !input || !resultsWrap) return;

    const posts = await loadPosts();

    async function runSearch() {
      const query = input.value.trim();
      if (!query) {
        resultsWrap.innerHTML = `<div class="search-empty">Start typing to search the site.</div>`;
        return;
      }
      const results = searchPosts(posts, query);
      resultsWrap.innerHTML = renderResultCards(results, "No matching articles found.");
    }

    form.addEventListener("submit", function (e) {
      e.preventDefault();
      runSearch();
    });

    input.addEventListener("input", function () {
      runSearch();
    });

    document.querySelectorAll(".js-open-search").forEach((btn) => {
      btn.addEventListener("click", function () {
        openOverlay();
      });
    });

    document.querySelectorAll(".js-close-search").forEach((btn) => {
      btn.addEventListener("click", function () {
        closeOverlay();
      });
    });

    document.addEventListener("keydown", function (e) {
      if (e.key === "Escape") closeOverlay();
    });
  }

  async function init() {
    ensureOverlay();
    await Promise.all([bindInlineSearch(), bindModalSearch()]);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
