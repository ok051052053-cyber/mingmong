(function () {
  function esc(s) {
    return String(s || "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function addHeaderSearchButton() {
    const nav = document.querySelector(".nav");
    if (!nav) return;
    if (nav.querySelector(".search-trigger")) return;

    const a = document.createElement("a");
    a.href = "search.html";
    a.className = "search-trigger";
    a.setAttribute("aria-label", "Search");
    a.textContent = "Search";
    nav.appendChild(a);
  }

  function bindSearchForms() {
    document.querySelectorAll(".site-search-form").forEach((form) => {
      if (form.dataset.bound === "1") return;
      form.dataset.bound = "1";

      form.addEventListener("submit", function (e) {
        e.preventDefault();
        const input = form.querySelector('input[name="q"]');
        const q = (input ? input.value : "").trim();
        const isPostPage = location.pathname.includes("/posts/");
        const target = isPostPage ? "../search.html" : "search.html";
        location.href = `${target}?q=${encodeURIComponent(q)}`;
      });
    });
  }

  async function renderSearchPage() {
    const root = document.getElementById("search-results");
    if (!root) return;

    const params = new URLSearchParams(location.search);
    const q = (params.get("q") || "").trim().toLowerCase();

    const input = document.getElementById("search-input");
    if (input) input.value = params.get("q") || "";

    if (!q) {
      root.innerHTML = `<div class="empty-soft">Type a keyword to search articles.</div>`;
      return;
    }

    let posts = [];
    try {
      const res = await fetch("posts.json", { cache: "no-store" });
      posts = await res.json();
      if (!Array.isArray(posts)) posts = [];
    } catch (e) {
      posts = [];
    }

    const results = posts.filter((p) => {
      const hay = [
        p.title || "",
        p.description || "",
        p.category || "",
        p.keyword || "",
        p.cluster || "",
      ].join(" ").toLowerCase();
      return hay.includes(q);
    });

    if (!results.length) {
      root.innerHTML = `<div class="empty-soft">No results found for <strong>${esc(params.get("q"))}</strong>.</div>`;
      return;
    }

    root.innerHTML = results.map((p) => {
      const href = p.url || `posts/${p.slug}.html`;
      const thumb = p.thumbnail || p.image || (p.slug ? `assets/posts/${p.slug}/1.jpg` : "");
      const bg = thumb ? `style="background-image:url('${esc(thumb)}')"` : "";
      return `
        <a class="post-card post-card--v" href="${href}">
          <div class="thumb has-img" ${bg}></div>
          <div class="post-body">
            <div class="kicker">${esc(p.category || "Article")}</div>
            <div class="post-title">${esc(p.title || "Untitled")}</div>
            <p class="post-desc">${esc(p.description || "")}</p>
          </div>
        </a>
      `;
    }).join("");
  }

  document.addEventListener("DOMContentLoaded", function () {
    addHeaderSearchButton();
    bindSearchForms();
    renderSearchPage();
  });
})();
