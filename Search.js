(function(){
let __mm_posts_cache = null;

function norm(s){
  return String(s || "").toLowerCase().trim();
}

function esc(s){
  return String(s||"")
    .replace(/&/g,"&amp;")
    .replace(/</g,"&lt;")
    .replace(/>/g,"&gt;")
    .replace(/"/g,"&quot;");
}

function resolveSearchBase(){
  const path = location.pathname || "";
  if(path.includes("/posts/")) return "../";
  return "";
}

function goSearch(q){
  const base = resolveSearchBase();
  const clean = String(q || "").trim();
  if(!clean) return;
  location.href = `${base}search.html?q=${encodeURIComponent(clean)}`;
}

async function loadPosts(){
  if(__mm_posts_cache) return __mm_posts_cache;

  const base = resolveSearchBase();
  try{
    const res = await fetch(`${base}posts.json`, { cache:"no-store" });
    const data = await res.json();
    __mm_posts_cache = Array.isArray(data) ? data : [];
  }catch(e){
    __mm_posts_cache = [];
  }
  return __mm_posts_cache;
}

function scorePost(p, q){
  const title = norm(p.title);
  const desc = norm(p.description);
  const cat = norm(p.category);
  const keyword = norm(p.keyword);
  const cluster = norm(p.cluster);
  const query = norm(q);

  let score = 0;
  if(title.includes(query)) score += 10;
  if(keyword.includes(query)) score += 8;
  if(desc.includes(query)) score += 4;
  if(cat.includes(query)) score += 2;
  if(cluster.includes(query)) score += 2;
  return score;
}

function postHref(p){
  if(p && p.url) return p.url;
  if(p && p.slug) return `posts/${p.slug}.html`;
  return "#";
}

function postHrefWithBase(p){
  const href = postHref(p);
  const base = resolveSearchBase();
  if(href.startsWith("posts/") && base === "../") return "../" + href;
  return href;
}

function renderMiniResults(items){
  if(!items.length){
    return `<div class="site-search-empty">No matching posts found.</div>`;
  }

  return items.map((p) => {
    return `
      <a class="site-search-mini-card" href="${postHrefWithBase(p)}">
        <div class="site-search-mini-kicker">${esc(p.category || "Article")}</div>
        <div class="site-search-mini-title">${esc(p.title || "Untitled")}</div>
      </a>
    `;
  }).join("");
}

async function bindInlineSearch(){
  const forms = document.querySelectorAll(".js-inline-search-form");
  if(!forms.length) return;

  const posts = await loadPosts();

  forms.forEach((form) => {
    const input = form.querySelector(".js-inline-search-input");
    const resultBox = form.parentElement.querySelector(".js-inline-search-results");
    if(!input) return;

    form.addEventListener("submit", function(e){
      e.preventDefault();
      goSearch(input.value);
    });

    input.addEventListener("input", function(){
      if(!resultBox) return;

      const q = String(input.value || "").trim();
      if(q.length < 2){
        resultBox.innerHTML = "";
        return;
      }

      const ranked = posts
        .map((p) => ({ p, s: scorePost(p, q) }))
        .filter((x) => x.s > 0 && x.p && x.p.slug)
        .sort((a,b) => b.s - a.s)
        .slice(0,5)
        .map((x) => x.p);

      resultBox.innerHTML = renderMiniResults(ranked);
    });
  });
}

function bindHeaderSearchButton(){
  const buttons = document.querySelectorAll(".js-open-search");
  if(!buttons.length) return;

  buttons.forEach((btn) => {
    btn.addEventListener("click", function(){
      const localInput = document.querySelector(".js-inline-search-input");
      const q = localInput ? String(localInput.value || "").trim() : "";
      if(q){
        goSearch(q);
      }else{
        const base = resolveSearchBase();
        location.href = `${base}search.html`;
      }
    });
  });
}

async function renderSearchPage(){
  const root = document.getElementById("search-results");
  if(!root) return;

  const params = new URLSearchParams(location.search);
  const q = String(params.get("q") || "").trim();
  const input = document.getElementById("search-page-input");
  if(input) input.value = q;

  if(!q){
    root.innerHTML = `<div class="empty-soft">Type at least one keyword to search.</div>`;
    return;
  }

  const posts = await loadPosts();
  const ranked = posts
    .map((p) => ({ p, s: scorePost(p, q) }))
    .filter((x) => x.s > 0 && x.p && x.p.slug)
    .sort((a,b) => b.s - a.s)
    .map((x) => x.p);

  if(!ranked.length){
    root.innerHTML = `<div class="empty-soft">No results found for <strong>${esc(q)}</strong>.</div>`;
    return;
  }

  root.innerHTML = ranked.map((p) => {
    const href = postHrefWithBase(p);
    const thumb = p.thumbnail || p.image || (p.slug ? `${resolveSearchBase()}assets/posts/${p.slug}/1.jpg` : "");
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

document.addEventListener("DOMContentLoaded", function(){
  bindHeaderSearchButton();
  bindInlineSearch();
  renderSearchPage();
});
})();
