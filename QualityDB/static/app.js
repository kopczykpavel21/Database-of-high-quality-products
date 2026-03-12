/* ============================
   QualityDB – Frontend Logic
   ============================ */

let currentPage = 1;
let debounceTimer = null;
let isListView = false;
let activeKeyword = "";

const SOURCE_LABELS = { alza: "Alza.cz", heureka: "Heureka.cz", zbozi: "Zbozi.cz", amazon: "Amazon.de" };

// ---- State ----
function getFilters() {
  const sortVal = document.getElementById("sort-by").value;
  const [sortField, sortDir] = sortVal.endsWith("_desc")
    ? [sortVal.replace("_desc", ""), "desc"]
    : [sortVal, "asc"];

  const starsVal = parseFloat(document.getElementById("filter-stars").value);
  const returnVal = parseFloat(document.getElementById("filter-return").value);
  const reviewsVal = parseInt(document.getElementById("filter-reviews").value);
  const recommendVal = parseInt(document.getElementById("filter-recommend").value);

  return {
    q: document.getElementById("search-input").value.trim(),
    category: document.getElementById("filter-category").value,
    min_stars: starsVal > 0 ? starsVal : "",
    max_return: returnVal < 1.4 ? returnVal : "",
    min_reviews: reviewsVal > 0 ? reviewsVal : "",
    min_recommend: recommendVal > 0 ? recommendVal : "",
    sort: sortField,
    order: sortDir,
    source: document.getElementById("filter-source").value,
    keyword: activeKeyword,
    page: currentPage
  };
}

// ---- API calls ----
async function fetchProducts() {
  const grid = document.getElementById("product-grid");
  grid.innerHTML = '<div class="loading-state"><div class="spinner"></div><p>Loading products…</p></div>';

  const params = new URLSearchParams(getFilters());
  const res = await fetch(`/api/products?${params}`);
  const data = await res.json();
  renderProducts(data);
}

async function fetchKeywords() {
  const res = await fetch("/api/keywords");
  const data = await res.json();
  const container = document.getElementById("kw-filter-pills");
  if (!container) return;
  // Show top 20 keywords as clickable pills
  container.innerHTML = data.slice(0, 20).map(({ tag, count }) =>
    `<button class="kw-pill" data-kw="${escHtml(tag)}" title="${count} products">
       ${escHtml(tag)} <span class="kw-pill-count">${count}</span>
     </button>`
  ).join("");
  container.querySelectorAll(".kw-pill").forEach(btn => {
    btn.addEventListener("click", () => {
      const kw = btn.dataset.kw;
      if (activeKeyword === kw) {
        // deselect
        activeKeyword = "";
        btn.classList.remove("active");
      } else {
        activeKeyword = kw;
        container.querySelectorAll(".kw-pill").forEach(b => b.classList.remove("active"));
        btn.classList.add("active");
      }
      currentPage = 1;
      fetchProducts();
      if (window.innerWidth <= 900) closeSidebar();
    });
  });
}

async function fetchStats() {
  const res = await fetch("/api/stats");
  const d = await res.json();
  document.getElementById("stat-total").textContent = d.total.toLocaleString();
  document.getElementById("stat-stars").textContent = d.avg_stars ?? "—";
}

// ---- Render ----
function starsVisual(rating) {
  if (!rating) return "—";
  const full = Math.floor(rating);
  const half = rating - full >= 0.3 ? 1 : 0;
  const empty = 5 - full - half;
  return "★".repeat(full) + (half ? "½" : "") + "☆".repeat(empty);
}

function returnClass(val) {
  if (val === null || val === undefined) return "";
  if (val === 0) return "good";
  if (val <= 0.5) return "good";
  if (val <= 1.0) return "warn";
  return "bad";
}

function recommendClass(val) {
  if (!val) return "";
  if (val >= 95) return "good";
  if (val >= 85) return "warn";
  return "bad";
}

function renderCard(p) {
  const returnRateDisplay = p.ReturnRate_pct !== null && p.ReturnRate_pct !== undefined
    ? p.ReturnRate_pct.toFixed(2) + "%" : "—";
  const starsDisplay = p.AvgStarRating ? p.AvgStarRating.toFixed(1) : "—";
  const recommendDisplay = p.RecommendRate_pct ? p.RecommendRate_pct + "%" : "—";
  const reviewsDisplay = p.ReviewsCount ? p.ReviewsCount.toLocaleString() : "—";
  const priceDisplay = p.Price_CZK ? Math.round(p.Price_CZK).toLocaleString() + " Kč" : "";
  const sourceLabel = SOURCE_LABELS[p.source] || p.source || "Unknown";
  const sourceCls = p.source === "alza" ? "source-badge" : `source-badge scraper src-${p.source}`;
  const sourceBadge = `<span class="${sourceCls}">${sourceLabel}</span>`;

  const rankDisplay = (p.source_rank && p.source_total)
    ? `${p.source_rank} / ${p.source_total}`
    : "—";
  const rankClass = (p.source_rank && p.source_total)
    ? (p.source_rank <= Math.ceil(p.source_total * 0.1) ? "good"
      : p.source_rank >= Math.floor(p.source_total * 0.9) ? "bad" : "")
    : "";

  const firstMetric = p.source === "alza"
    ? `<div class="metric">
        <div class="metric-label">Return rate</div>
        <div class="metric-value ${returnClass(p.ReturnRate_pct)}">${returnRateDisplay}</div>
       </div>`
    : `<div class="metric">
        <div class="metric-label">Rank</div>
        <div class="metric-value ${rankClass}">${rankDisplay}</div>
       </div>`;

  const keywords = p.keywords ? JSON.parse(p.keywords) : [];
  const cardTags = keywords.slice(0, 2).map(k =>
    `<span class="kw-tag">${escHtml(k)}</span>`
  ).join("");

  return `
  <div class="product-card" onclick="openModal(${JSON.stringify(JSON.stringify(p))})">
    ${sourceBadge}
    <div class="card-category">${escHtml(p.Category || "")}</div>
    <div class="card-name">${escHtml(p.Name || "Unnamed")}</div>
    <div class="card-metrics">
      ${firstMetric}
      <div class="metric">
        <div class="metric-label">Recommend</div>
        <div class="metric-value ${recommendClass(p.RecommendRate_pct)}">${recommendDisplay}</div>
      </div>
    </div>
    ${cardTags ? `<div class="card-tags">${cardTags}</div>` : ""}
    <div class="card-stars">
      <span class="stars-visual">${starsVisual(p.AvgStarRating)}</span>
      <span>${starsDisplay}</span>
      <span style="color:var(--text3)">(${reviewsDisplay} reviews)</span>
    </div>
    <div class="card-footer">
      <span class="card-price">${priceDisplay}</span>
      ${p.ProductURL ? `<a class="card-link" href="${escHtml(p.ProductURL)}" target="_blank" onclick="event.stopPropagation()">View →</a>` : ""}
    </div>
  </div>`;
}

function renderProducts(data) {
  const grid = document.getElementById("product-grid");
  const info = document.getElementById("results-info");

  if (isListView) grid.classList.add("list-view");
  else grid.classList.remove("list-view");

  if (!data.products.length) {
    grid.innerHTML = `
      <div class="empty-state">
        <div class="empty-state-icon">🔍</div>
        <h3>No products found</h3>
        <p>Try adjusting your filters or search term</p>
      </div>`;
    info.innerHTML = "No results";
    document.getElementById("pagination").innerHTML = "";
    return;
  }

  const start = (data.page - 1) * data.page_size + 1;
  const end = Math.min(data.page * data.page_size, data.total);
  info.innerHTML = `Showing <strong>${start}–${end}</strong> of <strong>${data.total.toLocaleString()}</strong> products`;

  grid.innerHTML = data.products.map(renderCard).join("");
  renderPagination(data.page, data.pages);
}

// ---- Pagination ----
function renderPagination(current, total) {
  const pag = document.getElementById("pagination");
  if (total <= 1) { pag.innerHTML = ""; return; }

  const pages = [];
  pages.push({ type: "btn", label: "‹", page: current - 1, disabled: current === 1 });

  const range = paginationRange(current, total);
  let prev = null;
  for (const p of range) {
    if (prev !== null && p - prev > 1) pages.push({ type: "ellipsis" });
    pages.push({ type: "btn", label: p, page: p, active: p === current });
    prev = p;
  }
  pages.push({ type: "btn", label: "›", page: current + 1, disabled: current === total });

  pag.innerHTML = pages.map(p => {
    if (p.type === "ellipsis") return `<span class="page-ellipsis">…</span>`;
    const cls = ["page-btn", p.active ? "active" : "", p.disabled ? "" : ""].filter(Boolean).join(" ");
    const disabled = p.disabled ? "disabled" : "";
    return `<button class="${cls}" ${disabled} onclick="goPage(${p.page})">${p.label}</button>`;
  }).join("");
}

function paginationRange(current, total) {
  const delta = 2;
  const left = current - delta, right = current + delta;
  const pages = new Set([1, total]);
  for (let i = left; i <= right; i++) if (i > 1 && i < total) pages.add(i);
  return Array.from(pages).sort((a, b) => a - b);
}

function goPage(page) {
  currentPage = page;
  fetchProducts();
  window.scrollTo({ top: 200, behavior: "smooth" });
}

// ---- Modal ----
function openModal(jsonStr) {
  const p = JSON.parse(jsonStr);
  const overlay = document.getElementById("modal-overlay");
  const content = document.getElementById("modal-content");

  const totalRatings = (p.Stars5_Count || 0) + (p.Stars4_Count || 0) + (p.Stars3_Count || 0)
    + (p.Stars2_Count || 0) + (p.Stars1_Count || 0);

  function barPct(cnt) {
    return totalRatings ? Math.round((cnt || 0) / totalRatings * 100) : 0;
  }

  const starBars = [5,4,3,2,1].map(n => {
    const cnt = p[`Stars${n}_Count`] || 0;
    const pct = barPct(cnt);
    return `
    <div class="star-bar-row">
      <span class="star-bar-label">★${n}</span>
      <div class="star-bar-track"><div class="star-bar-fill" style="width:${pct}%"></div></div>
      <span class="star-bar-count">${cnt}</span>
    </div>`;
  }).join("");

  content.innerHTML = `
    <div class="modal-category">${escHtml(p.Category || "")}</div>
    <div class="modal-name">${escHtml(p.Name || "Unnamed")}</div>

    <div class="modal-stars-row">
      <span class="modal-stars-big">${p.AvgStarRating ? p.AvgStarRating.toFixed(1) : "—"}</span>
      <span class="modal-stars-visual">${starsVisual(p.AvgStarRating)}</span>
      <span class="modal-reviews-count">${p.ReviewsCount ? p.ReviewsCount.toLocaleString() + " reviews" : ""}</span>
    </div>

    <div class="modal-metrics">
      <div class="modal-metric">
        <div class="modal-metric-label">Return Rate</div>
        <div class="modal-metric-value ${returnClass(p.ReturnRate_pct)}">
          ${p.ReturnRate_pct !== null && p.ReturnRate_pct !== undefined ? p.ReturnRate_pct.toFixed(2) + "%" : "—"}
        </div>
      </div>
      <div class="modal-metric">
        <div class="modal-metric-label">Recommend</div>
        <div class="modal-metric-value ${recommendClass(p.RecommendRate_pct)}">
          ${p.RecommendRate_pct ? p.RecommendRate_pct + "%" : "—"}
        </div>
      </div>
      <div class="modal-metric">
        <div class="modal-metric-label">Price</div>
        <div class="modal-metric-value">
          ${p.Price_CZK ? Math.round(p.Price_CZK).toLocaleString() + " Kč" : "—"}
        </div>
      </div>
    </div>

    ${totalRatings > 0 ? `<div class="star-bar-wrap">${starBars}</div>` : ""}

    ${p.Description ? `<div class="modal-desc">${escHtml(p.Description).substring(0, 500)}${p.Description.length > 500 ? "…" : ""}</div>` : ""}

    ${keywords.length > 0 ? `
    <div class="modal-keywords">
      <div class="modal-keywords-label">Quality signals</div>
      <div class="modal-keywords-tags">
        ${keywords.map(k => `<span class="kw-tag kw-tag-modal">${escHtml(k)}</span>`).join("")}
      </div>
    </div>` : ""}

    <div class="modal-actions">
      ${p.ProductURL ? `<a class="btn-primary" href="${escHtml(p.ProductURL)}" target="_blank">View on ${SOURCE_LABELS[p.source] || "Shop"} →</a>` : ""}
      <button class="btn-secondary" onclick="closeModal()">Close</button>
    </div>`;

  overlay.classList.add("open");
}

function closeModal() {
  document.getElementById("modal-overlay").classList.remove("open");
}

// ---- Helpers ----
function escHtml(str) {
  return String(str).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
}

function triggerSearch() {
  currentPage = 1;
  fetchProducts();
}

function debouncedSearch() {
  clearTimeout(debounceTimer);
  debounceTimer = setTimeout(triggerSearch, 350);
}

// ---- Event listeners ----
document.addEventListener("DOMContentLoaded", () => {
  fetchStats();
  fetchKeywords();
  fetchProducts();

  // Search
  document.getElementById("search-input").addEventListener("input", function() {
    document.getElementById("search-clear").style.display = this.value ? "block" : "none";
    debouncedSearch();
  });
  document.getElementById("search-clear").addEventListener("click", () => {
    document.getElementById("search-input").value = "";
    document.getElementById("search-clear").style.display = "none";
    triggerSearch();
  });

  // Sort & dropdowns (mobile versions registered later with closeSidebar)

  // Ranges
  const starSlider = document.getElementById("filter-stars");
  starSlider.addEventListener("input", () => {
    const v = parseFloat(starSlider.value);
    document.getElementById("stars-val").textContent = v > 0 ? v.toFixed(1) + "★" : "Any";
    debouncedSearch();
  });

  const returnSlider = document.getElementById("filter-return");
  returnSlider.addEventListener("input", () => {
    const v = parseFloat(returnSlider.value);
    document.getElementById("return-val").textContent = v.toFixed(1) + "%";
    debouncedSearch();
  });

  const reviewsSlider = document.getElementById("filter-reviews");
  reviewsSlider.addEventListener("input", () => {
    const v = parseInt(reviewsSlider.value);
    document.getElementById("reviews-val").textContent = v > 0 ? v + "+" : "Any";
    debouncedSearch();
  });

  const recommendSlider = document.getElementById("filter-recommend");
  recommendSlider.addEventListener("input", () => {
    const v = parseInt(recommendSlider.value);
    document.getElementById("recommend-val").textContent = v > 0 ? v + "%+" : "Any";
    debouncedSearch();
  });

  // Quick star buttons
  document.querySelectorAll(".star-btn[data-val]").forEach(btn => {
    btn.addEventListener("click", () => {
      const v = parseFloat(btn.dataset.val);
      starSlider.value = v;
      document.getElementById("stars-val").textContent = v.toFixed(1) + "★";
      document.querySelectorAll(".star-btn[data-val]").forEach(b => b.classList.remove("active"));
      btn.classList.add("active");
      triggerSearch();
    });
  });

  // Quick return buttons
  document.querySelectorAll(".star-btn[data-return]").forEach(btn => {
    btn.addEventListener("click", () => {
      const v = parseFloat(btn.dataset.return);
      returnSlider.value = v;
      document.getElementById("return-val").textContent = v.toFixed(1) + "%";
      document.querySelectorAll(".star-btn[data-return]").forEach(b => b.classList.remove("active"));
      btn.classList.add("active");
      triggerSearch();
    });
  });

  // Reset
  document.getElementById("reset-filters").addEventListener("click", () => {
    document.getElementById("search-input").value = "";
    document.getElementById("search-clear").style.display = "none";
    document.getElementById("filter-category").value = "";
    document.getElementById("filter-source").value = "";
    activeKeyword = "";
    document.querySelectorAll(".kw-pill").forEach(b => b.classList.remove("active"));
    document.getElementById("sort-by").value = "ReturnRate_pct";
    starSlider.value = 0; document.getElementById("stars-val").textContent = "Any";
    returnSlider.value = 1.4; document.getElementById("return-val").textContent = "1.4%";
    reviewsSlider.value = 0; document.getElementById("reviews-val").textContent = "Any";
    recommendSlider.value = 0; document.getElementById("recommend-val").textContent = "Any";
    document.querySelectorAll(".star-btn").forEach(b => b.classList.remove("active"));
    currentPage = 1;
    fetchProducts();
  });

  // View toggle
  document.getElementById("view-grid").addEventListener("click", () => {
    isListView = false;
    document.getElementById("view-grid").classList.add("active");
    document.getElementById("view-list").classList.remove("active");
    document.getElementById("product-grid").classList.remove("list-view");
  });
  document.getElementById("view-list").addEventListener("click", () => {
    isListView = true;
    document.getElementById("view-list").classList.add("active");
    document.getElementById("view-grid").classList.remove("active");
    document.getElementById("product-grid").classList.add("list-view");
    triggerSearch();
  });

  // Modal close
  document.getElementById("modal-close").addEventListener("click", closeModal);
  document.getElementById("modal-overlay").addEventListener("click", e => {
    if (e.target === e.currentTarget) closeModal();
  });
  document.addEventListener("keydown", e => {
    if (e.key === "Escape") { closeModal(); closeSidebar(); }
  });

  // Mobile sidebar toggle
  function openSidebar() {
    document.getElementById("sidebar").classList.add("open");
    document.getElementById("sidebar-backdrop").classList.add("open");
    document.body.style.overflow = "hidden";
  }
  function closeSidebar() {
    document.getElementById("sidebar").classList.remove("open");
    document.getElementById("sidebar-backdrop").classList.remove("open");
    document.body.style.overflow = "";
  }
  document.getElementById("mobile-filter-btn").addEventListener("click", openSidebar);
  document.getElementById("sidebar-backdrop").addEventListener("click", closeSidebar);
  document.getElementById("sidebar-close").addEventListener("click", closeSidebar);

  // Auto-close sidebar after applying a filter on mobile
  function triggerSearchAndClose() {
    triggerSearch();
    if (window.innerWidth <= 900) closeSidebar();
  }
  ["filter-category", "filter-source", "sort-by"].forEach(id => {
    document.getElementById(id).addEventListener("change", triggerSearchAndClose);
  });
  [starSlider, returnSlider, reviewsSlider, recommendSlider].forEach(sl => {
    sl.addEventListener("change", () => { if (window.innerWidth <= 900) closeSidebar(); });
  });
});
