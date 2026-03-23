/* ============================
   QualityDB – Frontend Logic
   ============================ */

let currentPage = 1;
let debounceTimer = null;
let isListView = false;
let activeKeyword = "";
let avoidMode = false;
let categoriesTree = [];   // [{main, subs:[{sub,count}]}]

const SOURCE_LABELS = {
  alza: "Alza.cz", heureka: "Heureka.cz", zbozi: "Zbozi.cz",
  amazon: "Amazon.de", amazon_us: "Amazon.com", otto: "Otto.de", warentest: "Stiftung Warentest",
  dtest: "D-test.cz", datart: "Datart.cz", ceneo: "Ceneo.pl",
  czc: "CZC.cz", heureka_sk: "Heureka.sk", conrad: "Conrad.de"
};

function priceStr(p) {
  if (p.currency === "USD" && p.Price_EUR) return "$" + p.Price_EUR.toLocaleString("en-US", {minimumFractionDigits: 0, maximumFractionDigits: 2});
  if (p.currency === "PLN" && p.Price_CZK) return Math.round(p.Price_CZK).toLocaleString("pl-PL") + " zł";
  if (p.Price_EUR) return Math.round(p.Price_EUR).toLocaleString("de-DE") + " €";
  if (p.Price_CZK) return Math.round(p.Price_CZK).toLocaleString("cs-CZ") + " Kč";
  return "";
}

// ---- Sort dropdown ----
function buildSortOptions() {
  const options = [
    { value: "RecommendRate_pct_desc", label: "Recommend rate (high → low)" },
    { value: "RecommendRate_pct",      label: "Recommend rate (low → high)" },
    { value: "AvgStarRating_desc",     label: "Star rating (high → low)" },
    { value: "AvgStarRating",          label: "Star rating (low → high)" },
    { value: "ReviewsCount_desc",      label: "Most reviewed" },
    { value: "Price_CZK",              label: "Price (low → high)" },
    { value: "Price_CZK_desc",         label: "Price (high → low)" },
    { value: "Price_EUR",              label: "Price USD (low → high)" },
    { value: "Price_EUR_desc",         label: "Price USD (high → low)" },
    { value: "ReturnRate_pct",         label: "Return rate (low → high)" },
    { value: "Name",                   label: "Name (A → Z)" },
  ];
  const sel = document.getElementById("sort-by");
  sel.innerHTML = options.map(o =>
    `<option value="${o.value}">${o.label}</option>`
  ).join("");
  sel.value = "RecommendRate_pct_desc";
}

// ---- Quality badge ----
function qualityBadge(p) {
  // For D-test: use overall_score from details_json
  let score = null;
  if (p.source === "dtest" && p.details_json) {
    try {
      const dj = typeof p.details_json === "string" ? JSON.parse(p.details_json) : p.details_json;
      score = dj.overall_score;
    } catch(e) {}
  }
  // Fallback to RecommendRate_pct for CZ/DE/PL sources
  const rec = p.RecommendRate_pct;

  if (score !== null && score !== undefined) {
    if (score >= 80) return `<span class="quality-badge badge-excellent">🏆 D-test Top</span>`;
    if (score >= 70) return `<span class="quality-badge badge-good">✅ D-test Dobrý</span>`;
    return "";
  }
  if (rec !== null && rec !== undefined) {
    if (rec >= 97) return `<span class="quality-badge badge-excellent">🏆 Top Pick</span>`;
    if (rec >= 93) return `<span class="quality-badge badge-excellent">⭐ Excellent</span>`;
    if (rec >= 88) return `<span class="quality-badge badge-good">✅ Good</span>`;
  }
  // For Amazon US: use star rating + review count as quality signal
  if (p.source === "amazon_us") {
    const stars = p.AvgStarRating;
    const reviews = p.ReviewsCount || 0;
    if (stars >= 4.7 && reviews >= 1000) return `<span class="quality-badge badge-excellent">🏆 Top Rated</span>`;
    if (stars >= 4.5 && reviews >= 500)  return `<span class="quality-badge badge-good">⭐ Highly Rated</span>`;
  }
  return "";
}

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
    main_category: document.getElementById("filter-main-category").value,
    category: document.getElementById("filter-category").value,
    min_stars: starsVal > 0 ? starsVal : "",
    max_return: returnVal < 1.4 ? returnVal : "",
    min_reviews: reviewsVal > 0 ? reviewsVal : "",
    min_recommend: recommendVal > 0 ? recommendVal : "",
    sort: sortField,
    order: sortDir,
    source: document.getElementById("filter-source").value,
    keyword: activeKeyword,
    avoid: avoidMode ? "1" : "",
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

async function fetchCategories() {
  const src = document.getElementById("filter-source").value;
  const SOURCE_COUNTRY_MAP = { otto:"DE", warentest:"DE", amazon:"DE", ceneo:"PL",
                                dtest:"CZ", alza:"CZ", heureka:"CZ", zbozi:"CZ", datart:"CZ",
                                amazon_us:"US", czc:"CZ", heureka_sk:"SK", conrad:"DE" };
  // No source selected → no country filter → all-market categories
  const country = src ? (SOURCE_COUNTRY_MAP[src] || "") : "";
  const res = await fetch(`/api/categories${country ? "?country=" + country : ""}`);
  categoriesTree = await res.json();

  const mainSel = document.getElementById("filter-main-category");
  mainSel.innerHTML = '<option value="">All categories</option>' +
    categoriesTree.map(({ main, subs }) => {
      const total = subs.reduce((s, x) => s + x.count, 0);
      return `<option value="${escHtml(main)}">${escHtml(main)} (${total.toLocaleString()})</option>`;
    }).join("");
}

function populateSubcategories(mainValue) {
  const subGroup = document.getElementById("sub-category-group");
  const subSel   = document.getElementById("filter-category");

  if (!mainValue) {
    subGroup.style.display = "none";
    subSel.innerHTML = '<option value="">All subcategories</option>';
    return;
  }

  const entry = categoriesTree.find(e => e.main === mainValue);
  if (!entry) { subGroup.style.display = "none"; return; }

  subSel.innerHTML = '<option value="">All subcategories</option>' +
    entry.subs.map(({ sub, count }) =>
      `<option value="${escHtml(sub)}">${escHtml(sub)} (${count.toLocaleString()})</option>`
    ).join("");
  subGroup.style.display = "";
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
      const clearBtn = document.getElementById("kw-clear-btn");
      if (activeKeyword === kw) {
        // deselect
        activeKeyword = "";
        btn.classList.remove("active");
        if (clearBtn) clearBtn.style.display = "none";
      } else {
        activeKeyword = kw;
        container.querySelectorAll(".kw-pill").forEach(b => b.classList.remove("active"));
        btn.classList.add("active");
        if (clearBtn) clearBtn.style.display = "";
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

function recommendClass(val, isStars) {
  if (!val) return "";
  if (isStars) {
    if (val >= 4.5) return "good";
    if (val >= 4.0) return "warn";
    return "bad";
  }
  if (val >= 95) return "good";
  if (val >= 85) return "warn";
  return "bad";
}

function renderCard(p) {
  const returnRateDisplay = p.ReturnRate_pct !== null && p.ReturnRate_pct !== undefined
    ? p.ReturnRate_pct.toFixed(2) + "%" : "—";
  const starsDisplay = p.AvgStarRating ? p.AvgStarRating.toFixed(1) : "—";
  const useStarsForRec = !p.RecommendRate_pct && p.AvgStarRating;
  const recommendDisplay = p.RecommendRate_pct
    ? p.RecommendRate_pct + "%"
    : (p.AvgStarRating ? p.AvgStarRating.toFixed(1) + " ★" : "—");
  const recommendLabel = useStarsForRec ? "Rating" : "Recommend";
  const reviewsDisplay = p.ReviewsCount ? p.ReviewsCount.toLocaleString() : "—";
  const priceDisplay = priceStr(p);
  const sourceLabel = SOURCE_LABELS[p.source] || p.source || "Unknown";
  const sourceCls = p.source === "alza" ? "source-badge" : `source-badge scraper src-${p.source}`;
  const sourceBadge = `<span class="${sourceCls}">${sourceLabel}</span>`;

  const rankDisplay = (p.source_rank && p.source_total)
    ? `${p.source_rank.toLocaleString()} / ${p.source_total.toLocaleString()}`
    : "—";
  const rankClass = (p.source_rank && p.source_total)
    ? (p.source_rank <= Math.ceil(p.source_total * 0.1) ? "good"
      : p.source_rank >= Math.floor(p.source_total * 0.9) ? "bad" : "")
    : "";
  // Truncate long category names so they fit in the metric label
  const rankLabel = p.Category
    ? "in " + (p.Category.length > 18 ? p.Category.slice(0, 16) + "…" : p.Category)
    : "Rank";

  const firstMetric = p.source === "alza"
    ? `<div class="metric">
        <div class="metric-label">Return rate</div>
        <div class="metric-value ${returnClass(p.ReturnRate_pct)}">${returnRateDisplay}</div>
       </div>`
    : `<div class="metric">
        <div class="metric-label">${rankLabel}</div>
        <div class="metric-value ${rankClass}">${rankDisplay}</div>
       </div>`;

  const keywords = p.keywords ? JSON.parse(p.keywords) : [];
  const cardTags = keywords.slice(0, 2).map(k =>
    `<span class="kw-tag">${escHtml(k)}</span>`
  ).join("");

  const badge = qualityBadge(p);

  return `
  <div class="product-card" onclick="openModal(${JSON.stringify(JSON.stringify(p))})">
    <div class="card-top-row">${sourceBadge}${badge}</div>
    <div class="card-category">${escHtml(p.Category || "")}</div>
    <div class="card-name">${escHtml(p.Name || "Unnamed")}</div>
    <div class="card-metrics">
      ${firstMetric}
      <div class="metric">
        <div class="metric-label">${recommendLabel}</div>
        <div class="metric-value ${recommendClass(useStarsForRec ? p.AvgStarRating : p.RecommendRate_pct, useStarsForRec)}">${recommendDisplay}</div>
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

  // Parse details_json for source-specific extras
  let dj = {};
  try { dj = p.details_json ? JSON.parse(p.details_json) : {}; } catch(e) {}

  // Star bars — ceneo stores star_distribution as percentages; others use raw counts
  let starBars = "";
  if (p.source === "ceneo") {
    const sd = dj.star_distribution || {};
    starBars = [5,4,3,2,1].map(n => {
      const pct = sd[String(n)] || 0;
      return `
      <div class="star-bar-row">
        <span class="star-bar-label">★${n}</span>
        <div class="star-bar-track"><div class="star-bar-fill" style="width:${pct}%"></div></div>
        <span class="star-bar-count">${pct}%</span>
      </div>`;
    }).join("");
  } else {
    const totalRatings = (p.Stars5_Count || 0) + (p.Stars4_Count || 0) + (p.Stars3_Count || 0)
      + (p.Stars2_Count || 0) + (p.Stars1_Count || 0);
    if (totalRatings > 0) {
      starBars = [5,4,3,2,1].map(n => {
        const cnt = p[`Stars${n}_Count`] || 0;
        const pct = Math.round(cnt / totalRatings * 100);
        return `
        <div class="star-bar-row">
          <span class="star-bar-label">★${n}</span>
          <div class="star-bar-track"><div class="star-bar-fill" style="width:${pct}%"></div></div>
          <span class="star-bar-count">${cnt}</span>
        </div>`;
      }).join("");
    }
  }

  // Ceneo feature scores block
  const featScores = dj.feature_scores || {};
  const featKeys = Object.keys(featScores);
  const featBlock = featKeys.length > 0 ? `
    <div class="modal-keywords" style="margin-top:12px">
      <div class="modal-keywords-label">User ratings by feature</div>
      <div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(160px,1fr));gap:6px;margin-top:6px">
        ${featKeys.map(k => `
          <div style="background:var(--surface2);border-radius:6px;padding:6px 8px;font-size:12px">
            <div style="color:var(--text2);margin-bottom:2px">${escHtml(k)}</div>
            <div style="font-weight:600;color:${featScores[k]>=90?'var(--green)':featScores[k]>=75?'var(--amber)':'var(--red)'}">${featScores[k]}%</div>
          </div>`).join("")}
      </div>
    </div>` : "";

  const totalRatings = 0; // used below only for the old path (now handled above)

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
        <div class="modal-metric-value">${priceStr(p) || "—"}</div>
      </div>
    </div>

    ${starBars ? `<div class="star-bar-wrap">${starBars}</div>` : ""}

    ${featBlock}

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
  buildSortOptions();   // populate sort dropdown before first fetch
  fetchStats();
  fetchCategories();
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

  // Products to avoid toggle
  const avoidToggle = document.getElementById("avoid-toggle");
  const avoidInfo   = document.getElementById("avoid-info");
  if (avoidToggle) {
    avoidToggle.addEventListener("click", () => {
      avoidMode = !avoidMode;
      avoidToggle.dataset.active = avoidMode ? "1" : "0";
      avoidToggle.textContent = avoidMode ? "⚠️ Showing products to avoid" : "Show products to avoid";
      avoidToggle.classList.toggle("avoid-btn-active", avoidMode);
      if (avoidInfo) avoidInfo.style.display = avoidMode ? "" : "none";
      currentPage = 1;
      triggerSearch();
    });
  }

  // Clear keyword filter button
  const kwClearBtn = document.getElementById("kw-clear-btn");
  if (kwClearBtn) {
    kwClearBtn.addEventListener("click", () => {
      activeKeyword = "";
      document.querySelectorAll(".kw-pill").forEach(b => b.classList.remove("active"));
      kwClearBtn.style.display = "none";
      currentPage = 1;
      triggerSearch();
    });
  }

  // Reset
  document.getElementById("reset-filters").addEventListener("click", () => {
    document.getElementById("search-input").value = "";
    document.getElementById("search-clear").style.display = "none";
    document.getElementById("filter-main-category").value = "";
    populateSubcategories("");   // hides sub-dropdown and clears it
    document.getElementById("filter-source").value = "";
    activeKeyword = "";
    document.querySelectorAll(".kw-pill").forEach(b => b.classList.remove("active"));
    if (kwClearBtn) kwClearBtn.style.display = "none";
    // Reset avoid mode
    avoidMode = false;
    if (avoidToggle) { avoidToggle.dataset.active = "0"; avoidToggle.textContent = "Show products to avoid"; avoidToggle.classList.remove("avoid-btn-active"); }
    if (avoidInfo) avoidInfo.style.display = "none";
    document.getElementById("sort-by").value = "RecommendRate_pct_desc";
    // Restore recommend filter (hidden when amazon_us was selected)
    const recGroup = document.getElementById("recommend-filter-group");
    if (recGroup) recGroup.style.display = "";
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

  // Main category → populate subcategory dropdown, then search
  document.getElementById("filter-main-category").addEventListener("change", function() {
    populateSubcategories(this.value);
    currentPage = 1;
    triggerSearchAndClose();
  });

  document.getElementById("filter-source").addEventListener("change", () => {
    const src = document.getElementById("filter-source").value;
    const isAmazonUS = src === "amazon_us";

    // Auto-switch sort to "Most reviewed" for amazon_us (RecommendRate is always null there)
    const sortSel = document.getElementById("sort-by");
    if (isAmazonUS && sortSel.value === "RecommendRate_pct_desc") {
      sortSel.value = "ReviewsCount_desc";
    } else if (!isAmazonUS && sortSel.value === "ReviewsCount_desc") {
      sortSel.value = "RecommendRate_pct_desc";
    }

    // Hide recommend rate filter for amazon_us (all products have NULL there)
    const recGroup = document.getElementById("recommend-filter-group");
    if (recGroup) recGroup.style.display = isAmazonUS ? "none" : "";

    // Re-fetch categories in the correct country when source changes
    document.getElementById("filter-main-category").value = "";
    populateSubcategories("");
    fetchCategories();
    triggerSearchAndClose();
  });
  ["filter-category", "sort-by"].forEach(id => {
    document.getElementById(id).addEventListener("change", triggerSearchAndClose);
  });
  [starSlider, returnSlider, reviewsSlider, recommendSlider].forEach(sl => {
    sl.addEventListener("change", () => { if (window.innerWidth <= 900) closeSidebar(); });
  });
});
