/* ============================
   QualityDB – Frontend Logic
   ============================ */

// API base URL — empty string when served from the same origin (default / local / Fly.io).
// Set window.__API_BASE = "https://your-app.fly.dev" in index.html when deploying
// the static frontend to Cloudflare Pages or GitHub Pages.
const API_BASE = (window.__API_BASE || "").replace(/\/$/, "");

let currentPage = 1;
let debounceTimer = null;
let isListView = false;
let activeKeyword = "";
let activeBrand   = "";  // exact brand filter set from brands leaderboard
let avoidMode = false;
let photosMode = false;
let categoriesTree = [];   // [{main, subs:[{sub,count}]}]
// ProductURL → [rec_delta, stars_delta, price_delta, days, first_price, last_price]
let snapshotDeltaMap = new Map();
// Set of ProductURLs that have ≥2 price/rating snapshots (loaded once on page init)
let snapshotUrlSet = new Set();
// Count of products with price history (from /api/stats — used for toggle button label)
let _withHistoryCount = 0;
// Count of products with images (from /api/stats)
let _withImagesCount = 0;
// Normalise a URL for snapshot-set lookup: lowercase + strip trailing slash
const normUrl = u => (u || "").toLowerCase().replace(/\/$/, "");
// card-id → product object  (avoids inline JSON in onclick attributes)
let cardDataMap = new Map();
let cardIdSeq = 0;
// Track the currently open modal's card ID for ←→ navigation
let currentModalCardId = null;

// ── Sparkline lazy-loader ────────────────────────────────────────────────────
// Per-card SVG sparklines are loaded when the card scrolls into view.
const _sparklineCache = new Map(); // url → [{snapshot_date, price_czk}, ...]
let   _sparklineObserver = null;

function _renderSparklineSvg(rows, svgEl) {
  // Use price_czk for CZK products, fall back to price_eur for EUR products
  const pts = rows.filter(r => r.price_czk != null || r.price_eur != null);
  if (pts.length < 2) { svgEl.innerHTML = ""; return; }
  const prices = pts.map(r => r.price_czk ?? r.price_eur);
  const minP = Math.min(...prices), maxP = Math.max(...prices);
  const W = 80, H = 32, px = 4, py = 4;
  const last = prices[prices.length - 1], first = prices[0];
  const color = last < first ? "#22c55e" : last > first ? "#ef4444" : "#94a3b8";
  const n = pts.length;
  const xStep = (W - px * 2) / Math.max(n - 1, 1);
  // Flat line: draw centered dashed line to indicate stable price (not hugging the bottom)
  if (maxP === minP) {
    const cy = (H / 2).toFixed(1);
    svgEl.innerHTML = `<line x1="${px}" y1="${cy}" x2="${W - px}" y2="${cy}"
      stroke="${color}" stroke-width="1.5" stroke-dasharray="3 3" opacity="0.7"/>
      <circle cx="${(W - px).toFixed(1)}" cy="${cy}" r="2.5" fill="${color}" opacity="0.7"/>`;
    return;
  }
  const range = maxP - minP;
  const polyPts = prices.map((p, i) => {
    const x = px + i * xStep;
    const y = py + (1 - (p - minP) / range) * (H - py * 2);
    return `${x.toFixed(1)},${y.toFixed(1)}`;
  }).join(" ");
  const lx = (px + (n - 1) * xStep).toFixed(1);
  const ly = (py + (1 - (last - minP) / range) * (H - py * 2)).toFixed(1);
  svgEl.innerHTML = `<polyline points="${polyPts}" fill="none" stroke="${color}" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"/>
    <circle cx="${lx}" cy="${ly}" r="3" fill="${color}"/>`;
}

function _loadSparkline(url, svgEl) {
  if (_sparklineCache.has(url)) {
    _renderSparklineSvg(_sparklineCache.get(url), svgEl);
    return;
  }
  fetch(`${API_BASE}/api/product-history?url=${encodeURIComponent(url)}`)
    .then(r => r.ok ? r.json() : [])
    .then(rows => { _sparklineCache.set(url, rows); _renderSparklineSvg(rows, svgEl); })
    .catch(() => {});
}

function _initSparklineObserver() {
  if (!window.IntersectionObserver) return;
  _sparklineObserver = new IntersectionObserver(entries => {
    entries.forEach(e => {
      if (!e.isIntersecting) return;
      const svg = e.target;
      if (svg.dataset.loaded === "1") return;
      svg.dataset.loaded = "1";
      _sparklineObserver.unobserve(svg);
      _loadSparkline(svg.dataset.url, svg);
    });
  }, { rootMargin: "120px" });
}

function observeSparklines() {
  if (!_sparklineObserver) return;
  document.querySelectorAll('.card-sparkline[data-loaded="0"]').forEach(svg => {
    _sparklineObserver.observe(svg);
  });
}

// ── IR data lazy-load ─────────────────────────────────────────────────────────
// When served from the Python server, __IR_SCORES and __FR_GOV are injected inline
// into the HTML (zero extra request).  When served from a static host (Cloudflare
// Pages), they start empty and are fetched once from /api/ir-data on first need.
window.__IR_SCORES = window.__IR_SCORES || {};
window.__FR_GOV    = window.__FR_GOV    || [];
let _irLoaded = Object.keys(window.__IR_SCORES).length > 0 || window.__FR_GOV.length > 0;
let _irLoading = null;  // Promise while in-flight, null otherwise

async function ensureIrData() {
  if (_irLoaded) return;
  if (_irLoading) return _irLoading;
  _irLoading = fetch(`${API_BASE}/api/ir-data`)
    .then(r => r.json())
    .then(d => {
      window.__IR_SCORES = d.ir_scores || {};
      window.__FR_GOV    = d.fr_gov    || [];
      _irLoaded  = true;
      _irLoading = null;
    })
    .catch(e => {
      console.warn("Could not load IR data:", e);
      _irLoaded  = true;  // stop retrying on error
      _irLoading = null;
    });
  return _irLoading;
}

// Repairability score lookup via window.__IR_SCORES injected in the HTML page
// Keyed by product Name (survives API caching layers that strip extra fields)
function getIR(p) {
  if (!p || !p.Name) return null;
  const scores = window.__IR_SCORES;
  if (!scores) return null;
  const entry = scores[p.Name];
  if (!entry || entry.s == null) return null;
  return { s: entry.s, d: entry.d, sub: entry.sub };
}

// Returns true for Fnac products and any product that has French index scores
function isFrenchProduct(p) {
  return p.source === "fnac" || p.source === "fr_ir";
}

// Parse French score data from a product's details_json (merged by server) or IR_SCORES fallback
function getFrenchScores(p) {
  let dj = null;
  if (p.details_json) {
    try { dj = typeof p.details_json === "string" ? JSON.parse(p.details_json) : p.details_json; }
    catch(e) {}
  }
  const irScore  = dj && dj._ir_score  != null ? dj._ir_score  : null;
  const durScore = dj && dj._dur_score != null ? dj._dur_score : null;
  if (irScore == null && durScore == null) return null;

  let sub = null;
  if (dj && dj._ir_sub) {
    try { sub = typeof dj._ir_sub === "string" ? JSON.parse(dj._ir_sub) : dj._ir_sub; }
    catch(e) {}
  }
  let durSub = null;
  if (dj && dj._dur_sub) {
    try { durSub = typeof dj._dur_sub === "string" ? JSON.parse(dj._dur_sub) : dj._dur_sub; }
    catch(e) {}
  }
  return {
    repair:   irScore,
    repairDate: dj ? dj._ir_date : null,
    sub,
    durability: durScore,
    durSub,
    warranty: dj ? dj._warranty : null,
    energy:   dj ? dj._energy   : null,
    brand:    dj ? dj._brand    : null,
  };
}

// C1–C5 sub-criteria labels for the Indice de Réparabilité
const IR_CRITERIA = {
  C1: "Documentation",
  C2: "Démontage",
  C3: "Pièces",
  C4: "Délai pièces",
  C5: "Spécifique",
};

// ── Category translation (Czech → English) ────────────────────────────────────
// Maps raw DB category strings to clean English display labels.
// Falls back to the original string for anything not listed.
const CATEGORY_EN = {
  // ── Main categories ─────────────────────────────────────────────────────────
  "Telefony a tablety":          "Phones & Tablets",
  "Děti a hračky":               "Kids & Toys",
  "Hry a hračky":                "Kids & Toys",
  "Dětské zboží":                "Kids & Toys",
  "Elektro":                     "Electronics",
  "Elektronika":                 "Electronics",
  "Dům a zahrada":               "Home & Garden",
  "Zahrada":                     "Home & Garden",
  "Zahrada a dílna":             "Home & Garden",
  "Zahrada a outdoor":           "Home & Garden",
  "Bytové vybavení":             "Home & Garden",
  "Hobby":                       "Hobbies",
  "Počítače a notebooky":        "Computers & Laptops",
  "Počítače a hry":              "Computers & Laptops",
  "PC komponenty":               "Computer Components",
  "Průmysl":                     "Industrial",
  "Průmyslové zboží":            "Industrial Goods",
  "Ostatní":                     "Other",
  "Foto a video":                "Photography & Video",
  "Foto a kamery":               "Photography & Video",
  "Hudba":                       "Audio & Music",
  "Zvuk":                        "Audio & Music",
  "Zvuk a hudba":                "Audio & Music",
  "Zvuk a sluchátka":            "Audio & Music",
  "Móda a oblečení":             "Fashion & Clothing",
  "Móda":                        "Fashion & Clothing",
  "Kancelář":                    "Office & Stationery",
  "Kancelářské potřeby":         "Office Supplies",
  "Kancelářské vybavení":        "Office Equipment",
  "Kosmetika":                   "Beauty & Cosmetics",
  "Zdraví a hygiena":            "Health & Beauty",
  "Zdraví":                      "Health",
  "Sport a outdoor":             "Sports & Outdoor",
  "Sport":                       "Sports & Outdoor",
  "Zdraví a sport":              "Sports & Outdoor",
  "Sport a kola":                "Sports & Outdoor",
  "Auto a moto":                 "Automotive",
  "Automotive":                  "Automotive",
  "Velké domácí spotřebiče":     "Home Appliances",
  "Malé domácí spotřebiče":      "Home Appliances",
  "Domácí spotřebiče":           "Home Appliances",
  "Spotřebiče":                  "Appliances",
  "Malé spotřebiče":             "Small Appliances",
  "Ostatní spotřebiče":          "Other Appliances",
  "Vysavače a úklid":            "Vacuums & Cleaning",
  "Sítě a konektivita":          "Networks & Connectivity",
  "Periferie a příslušenství":   "Peripherals & Accessories",
  "Příslušenství":               "Accessories",
  "Televize a video":            "TVs & Video",
  "Datová úložiště":             "Storage Devices",
  "Chytré zařízení":             "Smart Devices",
  "Potraviny":                   "Food & Groceries",
  "Herní technika":              "Gaming",
  "Knihy a média":               "Books & Media",
  "Cestování":                   "Travel",
  "Zvířata":                     "Pets",
  // ── Toys & Kids ─────────────────────────────────────────────────────────────
  "Hračky":                      "Toys",
  "Plyšové hračky":              "Plush Toys",
  "Vzdělávací hračky":           "Educational Toys",
  "Venkovní hračky":             "Outdoor Toys",
  "Deskové a karetní hry":       "Board & Card Games",
  "Figurky a sběratelství":      "Figures & Collectibles",
  "Panenky":                     "Dolls",
  "LEGO a stavebnice":           "LEGO & Building Sets",
  "Puzzle":                      "Puzzles",
  "RC modely":                   "RC Models",
  "Kostýmy a party":             "Costumes & Party",
  "Tvoření s dětmi":             "Kids Crafts",
  "Dětské autosedačky":          "Child Car Seats",
  "Dětské kočárky":              "Strollers",
  "Míčové hry":                  "Ball Games",
  // ── Phone & Tablet accessories ───────────────────────────────────────────────
  "Pouzdra a kryty":             "Cases & Covers",
  "Ochranné fólie":              "Screen Protectors",
  "Chytré telefony":             "Smartphones",
  "Mobilní telefony":            "Mobile Phones",
  "Telefony":                    "Phones",
  "Tablety":                     "Tablets",
  "Tablety a čtečky":            "Tablets & E-Readers",
  "Příslušenství Apple Watch":   "Apple Watch Accessories",
  // ── Computers ───────────────────────────────────────────────────────────────
  "Notebooky":                   "Laptops",
  "Počítače":                    "Desktops",
  "Mini počítače":               "Mini PCs",
  "Grafické karty":              "Graphics Cards",
  "Pevné disky a SSD":           "Hard Drives & SSDs",
  "Pevné disky":                 "Hard Drives",
  "SSD":                         "SSDs",
  "SSD disky":                   "SSDs",
  "NAS úložiště":                "NAS Storage",
  "Operační paměti":             "RAM",
  "Procesory":                   "CPUs",
  "Základní desky":              "Motherboards",
  "PC skříně":                   "PC Cases",
  "Chlazení":                    "Cooling",
  "Klávesnice":                  "Keyboards",
  "Myši":                        "Mice",
  "Monitory":                    "Monitors",
  "Tiskárny":                    "Printers",
  "Tisk a kopírování":           "Print & Copy",
  "Dokovací stanice":            "Docking Stations",
  "Webkamery":                   "Webcams",
  "Příslušenství k notebookům":  "Laptop Accessories",
  "Herní sedačky":               "Gaming Chairs",
  "Herní ovladače":              "Game Controllers",
  "Herní konzole":               "Game Consoles",
  "Herní příslušenství":         "Gaming Accessories",
  "Počítačové hry":              "PC Games",
  "Software":                    "Software",
  "3D tisk a modelování":        "3D Printing",
  // ── TV & Audio ───────────────────────────────────────────────────────────────
  "Televizory":                  "TVs",
  "Televize":                    "TVs",
  "Domácí kino":                 "Home Cinema",
  "Soundbary a reproduktory":    "Soundbars & Speakers",
  "Soundbary":                   "Soundbars",
  "Sluchátka":                   "Headphones",
  "Reproduktory":                "Speakers",
  "Přenosný zvuk":               "Portable Audio",
  "Přehrávače":                  "Media Players",
  "Multimediální přehrávače":    "Media Players",
  "Streaming zařízení":          "Streaming Devices",
  "Streamovací zařízení":        "Streaming Devices",
  "Rádia a Hi-Fi":               "Radios & Hi-Fi",
  "Gramofony":                   "Turntables",
  "Projektory":                  "Projectors",
  "Dálkové ovladače":            "Remote Controls",
  // ── Photo & Video ────────────────────────────────────────────────────────────
  "Fotoaparáty":                 "Cameras",
  "Videokamery":                 "Camcorders",
  "Akční kamery":                "Action Cameras",
  "Drony":                       "Drones",
  "Objektivy":                   "Lenses",
  "Blesky":                      "Camera Flashes",
  "Dalekohledce":                "Binoculars",
  "Stativy a stab.":             "Tripods & Stabilizers",
  "Držáky a rigy":               "Rigs & Mounts",
  "Filtry":                      "Filters",
  "Tašky a pouzdra":             "Bags & Cases",
  "Tašky a batohy":              "Bags & Backpacks",
  "IP kamery":                   "IP Cameras",
  // ── Networking & Connectivity ────────────────────────────────────────────────
  "Síťové prvky":                "Networking",
  "Síťové přepínače":            "Network Switches",
  "Síťové komponenty":           "Network Components",
  "Routery":                     "Routers",
  "Extendery":                   "Range Extenders",
  "Kabely a adaptéry":           "Cables & Adapters",
  "Kabely a rozbočovače":        "Cables & Hubs",
  "Kabely":                      "Cables",
  // ── Storage ─────────────────────────────────────────────────────────────────
  "Úložiště":                    "Storage",
  "Úložiště a USB":              "Storage & USB",
  "Flash disky":                 "USB Drives",
  "Paměťové karty":              "Memory Cards",
  "Paměťová média":              "Memory Media",
  "Externí disky":               "External Drives",
  "Ostatní úložiště":            "Other Storage",
  // ── Smart Home & Security ────────────────────────────────────────────────────
  "Chytrá domácnost":            "Smart Home",
  "Bezpečnost a ochrana":        "Security & Safety",
  // ── Power & Batteries ────────────────────────────────────────────────────────
  "Baterie":                     "Batteries",
  "Baterie a nabíječky":         "Batteries & Chargers",
  "Nabíječky":                   "Chargers",
  "Powerbanky":                  "Power Banks",
  "Napájení":                    "Power",
  // ── Musical Instruments ──────────────────────────────────────────────────────
  "Hudební nástroje":            "Musical Instruments",
  "Kytary":                      "Guitars",
  "Bicí":                        "Drums",
  "Klávesy":                     "Keyboards (Music)",
  "Mikrofony":                   "Microphones",
  "Dechové nástroje":            "Wind Instruments",
  "Audio rozhraní":              "Audio Interfaces",
  "Zesilovače":                  "Amplifiers",
  "Struny a příslušenství":      "Strings & Accessories",
  "Elektronické hud. nástroje":  "Electronic Instruments",
  "Hudební příslušenství":       "Music Accessories",
  // ── Wearables ────────────────────────────────────────────────────────────────
  "Chytré hodinky":              "Smartwatches",
  "Smartwatch":                  "Smartwatches",
  "Fitness náramky":             "Fitness Trackers",
  // ── Home Appliances ──────────────────────────────────────────────────────────
  "Chladničky a mrazničky":      "Fridges & Freezers",
  "Ledničky":                    "Fridges",
  "Pračky":                      "Washing Machines",
  "Pračky a péče o prádlo":      "Washing & Laundry",
  "Myčky nádobí":                "Dishwashers",
  "Sušičky":                     "Dryers",
  "Sušičky prádla":              "Tumble Dryers",
  "Trouby":                      "Ovens",
  "Sporáky":                     "Cookers",
  "Klimatizace":                 "Air Conditioners",
  "Vytápění a klimatizace":      "Heating & Air Conditioning",
  "Čističky vzduchu":            "Air Purifiers",
  "Vysavače":                    "Vacuums",
  "Tyčové vysavače":             "Stick Vacuums",
  "Robotické vysavače":          "Robot Vacuums",
  "Vysavač":                     "Vacuums",
  "Úklid":                       "Cleaning",
  "Úklid (vysavače)":            "Cleaning (Vacuums)",
  "Kuchyňské spotřebiče":        "Kitchen Appliances",
  "Kuchyňské nádobí":            "Cookware",
  "Vaření a pečení":             "Cooking & Baking",
  "Mixéry a roboty":             "Blenders & Food Processors",
  "Varné konvice":               "Kettles",
  "Toustovače":                  "Toasters",
  "Kávovary":                    "Coffee Machines",
  "Kávovar":                     "Coffee Machines",
  "Grilování":                   "Grills & BBQ",
  "Žehličky":                    "Irons",
  "Sekačky a péče o trávník":    "Lawn Mowers",
  "Svítidla":                    "Lighting",
  "Koupelna":                    "Bathroom",
  "Organizace":                  "Organization",
  "Dekorace":                    "Decor",
  "Domácí potřeby":              "Household Goods",
  "Skladování":                  "Storage Solutions",
  // ── Tools ────────────────────────────────────────────────────────────────────
  "Elektrické nářadí":           "Power Tools",
  "Ruční nářadí":                "Hand Tools",
  "Nástroje":                    "Tools",
  "Měřicí přístroje":            "Measuring Instruments",
  "Pájení a elektronika":        "Soldering & Electronics",
  "Elektroinstalace":            "Electrical Installation",
  "Šrouby a spojovací mat.":     "Screws & Fasteners",
  "Lepidla a těsnicí látky":     "Adhesives & Sealants",
  "Čerpadla a motory":           "Pumps & Motors",
  "Žebříky a lešení":            "Ladders & Scaffolding",
  "Zahradní nářadí":             "Garden Tools",
  "Komponenty":                  "Components",
  "Pájení":                      "Soldering",
  // ── Crafts & Arts ────────────────────────────────────────────────────────────
  "Kreativní práce":             "Crafts & DIY",
  "Tvoření a výtvarno":          "Arts & Crafts",
  "Šití a pletení":              "Sewing & Knitting",
  "Háčkování a haptika":         "Crochet & Knitting",
  "Malování a kreslení":         "Drawing & Painting",
  "Scrapbooking":                "Scrapbooking",
  "Dřevo a řemesla":             "Woodwork & Crafts",
  "3D tisk a modelování":        "3D Printing",
  "Handmade":                    "Handmade",
  "Fine Art":                    "Fine Art",
  // ── Fashion ──────────────────────────────────────────────────────────────────
  "Boty":                        "Shoes",
  "Dámské oblečení":             "Women's Clothing",
  "Pánské oblečení":             "Men's Clothing",
  "Sportovní oblečení":          "Sports Clothing",
  "Spodní prádlo a ponožky":     "Underwear & Socks",
  "Doplňky a šperky":            "Accessories & Jewellery",
  "Zavazadla a kufry":           "Luggage & Suitcases",
  "Brýle na počítač":            "Computer Glasses",
  "Školní potřeby":              "School Supplies",
  // ── Beauty & Health ──────────────────────────────────────────────────────────
  "Líčení a make-up":            "Makeup",
  "Vlasová kosmetika":           "Hair Care",
  "Péče o pleť":                 "Skincare",
  "Parfumy":                     "Perfumes",
  "Manikúra a pedikúra":         "Nail Care",
  "Dentální hygiena":            "Dental Hygiene",
  "Ústní hygiena":               "Oral Hygiene",
  "Holení a depilace":           "Shaving & Epilation",
  "Holicí strojky":              "Shavers",
  "Fény a stylingové přístroje": "Hair Dryers & Stylers",
  "Deodoranty a antiperspiranty":"Deodorants",
  "Opalovací krémy":             "Sunscreen",
  "Prémiová kosmetika":          "Premium Cosmetics",
  "Nástrojová kosmetika":        "Cosmetic Tools",
  "Zdravotnické pomůcky":        "Medical Devices",
  // ── Sports & Outdoor ────────────────────────────────────────────────────────
  "Cyklistika":                  "Cycling",
  "Fitness a posilování":        "Fitness & Gym",
  "Camping a turistika":         "Camping & Hiking",
  "Outdoor a turistika":         "Outdoor & Hiking",
  "Vodní sporty":                "Water Sports",
  "Zimní sporty":                "Winter Sports",
  "Bojové sporty":               "Combat Sports",
  "Běhání a atletika":           "Running & Athletics",
  "GPS a navigace":              "GPS & Navigation",
  // ── Automotive ──────────────────────────────────────────────────────────────
  "Auto elektronika":            "Car Electronics",
  "Pneumatika":                  "Tyres",
  // ── Office & Stationery ──────────────────────────────────────────────────────
  "Papír a notesy":              "Paper & Notebooks",
  "Psací potřeby":               "Writing Supplies",
  "Organizace kanceláře":        "Office Organization",
  // ── Books & Media ────────────────────────────────────────────────────────────
  "Filmy":                       "Movies",
  "Audioknihy":                  "Audiobooks",
  "E-čtečky":                    "E-Readers",
  "Digitální hudba":             "Digital Music",
  "Zábava":                      "Entertainment",
  // ── Miscellaneous ────────────────────────────────────────────────────────────
  "Držáky a stojany":            "Holders & Stands",
  "Sběratelství":                "Collectibles",
  "Umění a sběratelství":        "Art & Collectibles",
  "Ostatní příslušenství":       "Other Accessories",
  "Nezařazeno":                  "Uncategorized",
  // ── Polish (Ceneo / Polish sources) ─────────────────────────────────────────
  "Smartfony":                   "Smartphones",
  "Słuchawki":                   "Headphones",
  "Telewizory":                  "TVs",
  "Głośniki przenośne":          "Portable Speakers",
  "Laptopy":                     "Laptops",
  "Ekspresy do kawy":            "Coffee Machines",
  "Odkurzacze":                  "Vacuum Cleaners",
  "Telefony komórkowe":          "Mobile Phones",
  "Tablety i czytniki":          "Tablets & E-Readers",
  "Smartwatche i opaski":        "Smartwatches & Bands",
  "Drukarki":                    "Printers",
  "Monitory":                    "Monitors",
  "Klawiatury":                  "Keyboards",
  "Mysz":                        "Mice",
  "Dyski SSD":                   "SSDs",
  "Lodówki":                     "Fridges",
  "Pralki":                      "Washing Machines",
  "Zmywarki":                    "Dishwashers",
  "Telewizory LED":              "LED TVs",
  "Aparaty fotograficzne":       "Cameras",
  "Głośniki":                    "Speakers",
  "Roboty sprzątające":          "Robot Vacuums",
  "Suszarki bębnowe":            "Tumble Dryers",
  "Konsole do gier":             "Game Consoles",
  "Gry na PC":                   "PC Games",
  // ── German (Testberichte / German sources) ────────────────────────────────────
  "Küche & Haushalt":            "Kitchen & Household",
  "Computer & Zubehör":          "Computers & Accessories",
  "Kühlschränke":                "Refrigerators",
  "Gefrierschränke":             "Freezers",
  "Wäschetrockner":              "Tumble Dryers",
  "Geschirrspüler":              "Dishwashers",
  "Kopfhörer":                   "Headphones",
  "Videospiele":                 "Video Games",
  "Kamera & Foto":               "Cameras & Photo",
  "Fernseher":                   "TVs",
  "Waschmaschinen":              "Washing Machines",
  "Smartphones und Handys":      "Smartphones",
  "Handys und Smartphones im Test": "Smartphones",
  "Onlineshops fuer refurbished Smartphones im Test": "Refurbished Phones",
  "Shops fuer refurbished Laptops im Test": "Refurbished Laptops",
  "Kameras im Vergleich Smartphone Kameras gegen richtige Kameras": "Camera Comparison",
  "Staubsauger":                 "Vacuum Cleaners",
  "Notebooks":                   "Laptops",
  "Klimageräte":                 "Air Conditioners",
  "Luftreiniger":                "Air Purifiers",
  "Tablets":                     "Tablets",
  "Kaffeevollautomaten":         "Coffee Machines (Auto)",
  "Kaffeemaschinen":             "Coffee Machines",
  "Mikrowellen":                 "Microwaves",
  "Lautsprecher":                "Speakers",
  "E-Reader":                    "E-Readers",
  "Drucker":                     "Printers",
  // ── Dutch (Coolblue / Dutch sources) ─────────────────────────────────────────
  "Laptops & Computers":         "Laptops & Computers",
  "Laptops & Notebooks":         "Laptops",
  "Laptop Accessories":          "Laptop Accessories",
  "TV & Video":                  "TV & Video",
  "Koelkasten":                  "Fridges",
  "Wasmachines":                 "Washing Machines",
  "Vaatwassers":                 "Dishwashers",
  "Drogers":                     "Dryers",
  "Stofzuigers":                 "Vacuum Cleaners",
  "Smartphones & mobiele telefoons": "Smartphones",
  "Bluetooth speakers":          "Bluetooth Speakers",
};

/** Translate a raw DB category string to English. Returns original if not mapped. */
function translateCat(s) {
  if (!s) return s;
  return CATEGORY_EN[s] || s;
}

// Render a compact C1–C5 bar row for French products.
// Accepts either { C1: val, C2: val, ... } (Fnac format)
// or { "note_c2.1": val, "note_c3.1": val, ... } (fr_gov sub-criteria format).
function irSubCriteriaRow(sub) {
  if (!sub) return "";

  // FR-gov format: keys like "note_c2.1", "note_c3.2" — group by C-number
  const frGovKeys = Object.keys(sub).filter(k => /^note_c\d+\.\d+$/.test(k));
  if (frGovKeys.length) {
    // Aggregate per-criterion averages
    const totals = {}, counts = {};
    frGovKeys.forEach(k => {
      const c = "C" + k.match(/note_c(\d+)/)[1];
      totals[c] = (totals[c] || 0) + sub[k];
      counts[c] = (counts[c] || 0) + 1;
    });
    const bars = Object.keys(totals).sort().map(c => {
      const val = totals[c] / counts[c];   // average of sub-sub-criteria → 0-10
      const pct = Math.round((val / 10) * 100);
      const cls = val >= 7 ? "ir-good" : val >= 4 ? "ir-mid" : "ir-bad";
      const label = IR_CRITERIA[c] || c;
      return `<div class="ir-sub-item" title="${label}: ${val.toFixed(1)}/10">
        <div class="ir-sub-label">${c}</div>
        <div class="ir-sub-bar"><div class="ir-sub-fill ${cls}" style="width:${pct}%"></div></div>
        <div class="ir-sub-val">${val.toFixed(1)}</div>
      </div>`;
    }).join("");
    return bars ? `<div class="ir-sub-row">${bars}</div>` : "";
  }

  // Fnac format: { C1: val, C2: val, ... }
  const keys = ["C1","C2","C3","C4","C5"].filter(k => sub[k] != null);
  if (!keys.length) return "";
  const bars = keys.map(k => {
    const val = sub[k];
    const pct = Math.round((val / 10) * 100);
    const cls = val >= 7 ? "ir-good" : val >= 4 ? "ir-mid" : "ir-bad";
    return `<div class="ir-sub-item" title="${IR_CRITERIA[k]}: ${val.toFixed(1)}/10">
      <div class="ir-sub-label">${k}</div>
      <div class="ir-sub-bar"><div class="ir-sub-fill ${cls}" style="width:${pct}%"></div></div>
      <div class="ir-sub-val">${val.toFixed(1)}</div>
    </div>`;
  }).join("");
  return `<div class="ir-sub-row">${bars}</div>`;
}

const SOURCE_FLAGS = {
  "alza.cz": "🇨🇿", alza: "🇨🇿",
  "heureka.cz": "🇨🇿", heureka: "🇨🇿",
  "zbozi.cz": "🇨🇿", zbozi: "🇨🇿",
  "datart.cz": "🇨🇿", datart: "🇨🇿",
  "planeo.cz": "🇨🇿", planeo: "🇨🇿",
  "czc.cz": "🇨🇿", czc: "🇨🇿",
  "heureka.sk": "🇸🇰", heureka_sk: "🇸🇰",
  "amazon_de": "🇩🇪", "amazon.de": "🇩🇪",
  "otto_de": "🇩🇪", otto: "🇩🇪",
  "saturn_de": "🇩🇪",
  "mediamarkt": "🇩🇪",
  "testberichte": "🇩🇪",
  "conrad": "🇩🇪",
  "geizhals": "🇦🇹",
  "digitec": "🇨🇭",
  "fnac": "🇫🇷", "darty": "🇫🇷", "darty.fr": "🇫🇷", fr_ir: "🇫🇷",
  "ceneo": "🇵🇱",
  "coolblue": "🇳🇱",
  "prisjakt": "🇸🇪", "prisjakt.nu": "🇸🇪",
  "pricerunner_se": "🇸🇪",
  "pricerunner": "🇩🇰",
  "amazon_us": "🇺🇸",
};

const SOURCE_LABELS = {
  alza: "Alza.cz",         "alza.cz": "Alza.cz",
  heureka: "Heureka.cz",   "heureka.cz": "Heureka.cz",
  zbozi: "Zbozi.cz",       "zbozi.cz": "Zbozi.cz",
  datart: "Datart.cz",     "datart.cz": "Datart.cz",
  planeo: "Planeo.cz",     "planeo.cz": "Planeo.cz",
  czc: "CZC.cz",           "czc.cz": "CZC.cz",
  heureka_sk: "Heureka.sk","heureka.sk": "Heureka.sk",
  ceneo: "Ceneo.pl",       "ceneo.pl": "Ceneo.pl",
  amazon: "Amazon.de",     "amazon.de": "Amazon.de",
  amazon_us: "Amazon.com", "amazon.com": "Amazon.com",
  otto: "Otto.de",         "otto.de": "Otto.de",
  conrad: "Conrad.de",     "conrad.de": "Conrad.de",
  fnac: "Fnac.fr",         "fnac.fr": "Fnac.fr",
  darty: "Darty.fr",       "darty.fr": "Darty.fr",
  coolblue: "Coolblue.nl", "coolblue.nl": "Coolblue.nl",
  digitec: "Digitec.ch",   "digitec.ch": "Digitec.ch",
  prisjakt: "Prisjakt.se", "prisjakt.nu": "Prisjakt.se",
  pricerunner: "PriceRunner","pricerunner.dk": "PriceRunner",
  pricerunner_se: "PriceRunner.se", "pricerunner.se": "PriceRunner.se",
  amazon_de: "Amazon.de",    "amazon.de": "Amazon.de",
  geizhals: "Geizhals.de",   "geizhals.de": "Geizhals.de",
  mediamarkt: "MediaMarkt",  "mediamarkt.de": "MediaMarkt",
  saturn_de: "Saturn.de",    "saturn.de": "Saturn.de",
  otto_de: "Otto.de",
  testberichte: "Testberichte.de", "testberichte.de": "Testberichte.de",
  fr_ir: "🏛️ Indice de Réparabilité",
};

function repairabilityBadge(score, date) {
  if (score === null || score === undefined) return "";
  const num = parseFloat(score);
  const cls = num >= 7 ? "ir-good" : num >= 4 ? "ir-mid" : "ir-bad";
  const tip = date ? ` title="Indice de Réparabilité · ${date}"` : ' title="Indice de Réparabilité (French repairability score)"';
  return `<span class="ir-badge ${cls}"${tip}>🔧 ${num.toFixed(1)}/10</span>`;
}

function priceStr(p) {
  const cur = p.currency || "";
  // Pick the value: EUR column for EUR/GBP/CHF/USD, CZK column for everything else
  const val = (p.Price_EUR != null && p.Price_EUR > 0) ? p.Price_EUR
            : (p.Price_CZK != null && p.Price_CZK > 0) ? p.Price_CZK
            : null;
  if (val == null) return "";
  switch (cur) {
    case "USD": return "$" + val.toLocaleString("en-US", {minimumFractionDigits: 0, maximumFractionDigits: 2});
    case "GBP": return "£" + Math.round(val).toLocaleString("en-GB");
    case "CHF": return "CHF " + Math.round(val).toLocaleString("de-CH");
    case "EUR": return Math.round(val).toLocaleString("de-DE") + " €";
    case "SEK": case "NOK": case "DKK": return Math.round(val).toLocaleString("sv-SE") + " kr";
    case "PLN": return Math.round(val).toLocaleString("pl-PL") + " zł";
    case "HUF": return Math.round(val).toLocaleString("hu-HU") + " Ft";
    default:    return Math.round(val).toLocaleString("cs-CZ") + " Kč";
  }
}

// ---- Sort dropdown ----
function buildSortOptions() {
  const options = [
    { value: "cat_rank",                     label: "🏆 Category rank (best first)" },
    { value: "cat_rank_desc",                label: "🏆 Category rank (worst first)" },
    { value: "value_score_desc",             label: "💎 Best value (quality/price)" },
    { value: "scraped_at_desc",              label: "🆕 Newest additions first" },
    { value: "RecommendRate_pct_desc",       label: "Recommend rate (high → low)" },
    { value: "RecommendRate_pct",            label: "Recommend rate (low → high)" },
    { value: "ReviewsCount_desc",            label: "Most reviewed" },
    { value: "AvgStarRating_desc",           label: "⭐ Star rating (high → low)" },
    { value: "AvgStarRating",                label: "⭐ Star rating (low → high)" },
    { value: "repairability_score_fr_desc",  label: "🔧 Réparabilité (high → low)" },
    { value: "repairability_score_fr",       label: "🔧 Réparabilité (low → high)" },
    { value: "durability_score_fr_desc",     label: "🛡️ Durabilité (high → low)" },
    { value: "Price_CZK",                    label: "Price (low → high)" },
    { value: "Price_CZK_desc",               label: "Price (high → low)" },
    { value: "Price_EUR",                    label: "Price EUR (low → high)" },
    { value: "Price_EUR_desc",               label: "Price EUR (high → low)" },
    { value: "ReturnRate_pct",               label: "Return rate (low → high)" },
    { value: "Name",                         label: "Name (A → Z)" },
  ];
  const sel = document.getElementById("sort-by");
  sel.innerHTML = options.map(o =>
    `<option value="${o.value}">${o.label}</option>`
  ).join("");
  // Restore saved sort preference; fall back to default
  const savedSort = localStorage.getItem("qdb_sort");
  sel.value = (savedSort && options.some(o => o.value === savedSort)) ? savedSort : "cat_rank";
  // Persist sort when it changes
  sel.addEventListener("change", () => localStorage.setItem("qdb_sort", sel.value));
}

// When source filter changes trigger a fresh search (sort stays as user chose it)
function onSourceFilterChange() {
  triggerSearch();
}

// ---- Warentest helpers ----

// Returns { label, labelFull, cls, fillCls, textCls, pct } for a warentest grade (1.0–5.5 scale)
// Lower grade = better (German school grades)
function wtGradeInfo(grade) {
  if (grade === undefined || grade === null) return null;
  // Convert grade (1.0=best, 5.5=worst) to a 0–100% quality bar
  // 1.0 → 100%, 5.5 → 0%
  const pct = Math.round(Math.max(0, Math.min(100, (5.5 - grade) / 4.5 * 100)));
  if (grade <= 1.5) return { label: "Sehr gut", labelFull: "Sehr gut",  cls: "wt-sehr-gut",     fillCls: "fill-great", textCls: "text-great", pct };
  if (grade <= 2.5) return { label: "Gut",      labelFull: "Gut",       cls: "wt-gut",           fillCls: "fill-good",  textCls: "text-good",  pct };
  if (grade <= 3.5) return { label: "Befr.",    labelFull: "Befriedigend", cls: "wt-befriedigend", fillCls: "fill-ok",  textCls: "text-ok",    pct };
  if (grade <= 4.5) return { label: "Ausr.",    labelFull: "Ausreichend",  cls: "wt-ausreichend",  fillCls: "fill-warn", textCls: "text-warn", pct };
  return               { label: "Mang.",    labelFull: "Mangelhaft",   cls: "wt-mangelhaft",    fillCls: "fill-bad",  textCls: "text-bad",   pct };
}

// Priority sub-rating keys in order of importance, with display labels
const WT_SUB_PRIORITY = [
  { keys: ["functions","funktionen","funktion","communication","fitness","testergebnis","messung"], label: "Functions" },
  { keys: ["camera","kamera"],                                                                       label: "Camera"    },
  { keys: ["display","bild"],                                                                        label: "Display"   },
  { keys: ["battery","akku","laufzeit"],                                                             label: "Battery"   },
  { keys: ["handling","anwendung","bedienung"],                                                      label: "Handling"  },
  { keys: ["stability","durability","build_quality","verarbeitung","schutzes"],                      label: "Build"     },
  { keys: ["safety","sicherheit"],                                                                   label: "Safety"    },
  { keys: ["pollutants","schadstoffe"],                                                              label: "Pollutants"},
  { keys: ["noise","ton","klang","sound"],                                                           label: "Sound"     },
  { keys: ["environmental","environmental_impact","umwelt","verpackung"],                            label: "Eco"       },
];

// Returns array of { label, grade } for all matching sub-ratings (up to maxItems)
function wtGetSubRatings(subs, maxItems = 99) {
  const found = [];
  for (const { keys, label } of WT_SUB_PRIORITY) {
    for (const key of keys) {
      if (subs[key] && subs[key].grade !== undefined) {
        found.push({ label, grade: subs[key].grade });
        break;
      }
    }
    if (found.length >= maxItems) break;
  }
  return found;
}

// Renders compact sub-rating pills for warentest cards (max 5)
function warentestSubRatings(dj) {
  const subs = dj.sub_ratings || {};
  const found = wtGetSubRatings(subs, 5);
  if (found.length === 0) return "";

  const items = found.map(f => {
    const info = wtGradeInfo(f.grade);
    return `<span class="wt-sub ${info.cls}" title="${f.label}: ${info.labelFull} (${f.grade})">${f.label.slice(0,5)} <b>${f.grade}</b></span>`;
  }).join("");
  return `<div class="wt-sub-row">${items}</div>`;
}

// Formats a YYYY-MM or YYYY test_date string → "June 2025" or "2025"
function wtFormatDate(d) {
  if (!d) return null;
  const m = d.match(/^(\d{4})-(\d{2})$/);
  if (m) {
    const months = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"];
    const mon = months[parseInt(m[2], 10) - 1];
    return mon ? `${mon} ${m[1]}` : m[1];
  }
  return d.match(/^\d{4}$/) ? d : null;
}

// Renders full sub-rating bars for warentest modal
function wtModalSubRatingBars(subs) {
  const found = wtGetSubRatings(subs);
  if (found.length === 0) return "";

  const rows = found.map(f => {
    const info = wtGradeInfo(f.grade);
    return `
      <div class="wt-bar-row">
        <span class="wt-bar-label">${escHtml(f.label)}</span>
        <div class="score-bar-track wt-bar-track">
          <div class="score-bar-fill ${info.fillCls}" style="width:${info.pct}%"></div>
        </div>
        <span class="score-bar-val ${info.textCls}">${f.grade}</span>
        <span class="wt-bar-word ${info.cls}">${info.label}</span>
      </div>`;
  }).join("");
  return `<div class="wt-modal-bars">${rows}</div>`;
}

// ── Image error fallback: replace broken <img> with the category placeholder ──
window._imgErr = function(img) {
  const wrap = img.closest('.card-img-wrap');
  if (!wrap) return;
  const ph = document.createElement('div');
  ph.className = 'card-img-placeholder';
  ph.setAttribute('aria-hidden', 'true');
  ph.innerHTML = '<span>' + (img.dataset.fallback || '📦') + '</span>';
  wrap.replaceWith(ph);
};

function qualityBadge(p) {
  // For Stiftung Warentest: use grade label, falling back to AvgStarRating approximation
  if (false) { // warentest hidden
    let grade = undefined;
    try {
      if (p.details_json) {
        const dj = typeof p.details_json === "string" ? JSON.parse(p.details_json) : p.details_json;
        const overall = (dj.sub_ratings || {}).overall || {};
        const label = overall.label;
        grade = overall.grade;
        const gradeStr = grade !== undefined ? ` (${grade})` : "";
        if (label === "sehr gut")     return `<span class="quality-badge badge-excellent">🏆 Sehr gut${gradeStr}</span>`;
        if (label === "gut")          return `<span class="quality-badge badge-good">✅ Gut${gradeStr}</span>`;
        if (label === "befriedigend") return `<span class="quality-badge badge-warn">⚠️ Befr.${gradeStr}</span>`;
        if (label === "ausreichend")  return `<span class="quality-badge badge-bad">❌ Ausr.${gradeStr}</span>`;
        if (label === "mangelhaft")   return `<span class="quality-badge badge-bad">❌ Mang.${gradeStr}</span>`;
      }
    } catch(e) {}
    // Fallback: approximate from AvgStarRating (5★→1.0, 1★→5.5)
    if (grade === undefined && p.AvgStarRating) {
      grade = parseFloat((6.0 - p.AvgStarRating).toFixed(1));
    }
    if (grade !== undefined) {
      if (grade <= 1.5) return `<span class="quality-badge badge-excellent">🏆 Sehr gut (${grade})</span>`;
      if (grade <= 2.5) return `<span class="quality-badge badge-good">✅ Gut (${grade})</span>`;
      if (grade <= 3.5) return `<span class="quality-badge badge-warn">⚠️ Befr. (${grade})</span>`;
      if (grade <= 4.5) return `<span class="quality-badge badge-bad">❌ Ausr. (${grade})</span>`;
      return `<span class="quality-badge badge-bad">❌ Mang. (${grade})</span>`;
    }
    return "";
  }
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
    if (score >= 80) return `<span class="quality-badge badge-excellent" title="D-test score: ${score}/100 — Top rated by Czech consumer magazine">🏆 D-test Top</span>`;
    if (score >= 70) return `<span class="quality-badge badge-good" title="D-test score: ${score}/100 — Good rating by Czech consumer magazine">✅ D-test Dobrý</span>`;
    return "";
  }
  if (rec !== null && rec !== undefined) {
    const n = p.ReviewsCount || p.StarRatingsCount || 0;
    const nStr = n > 0 ? ` of ${n.toLocaleString()} customers` : "";
    const nReq97 = n >= 20 || n === 0;   // require ≥20 reviews for top badge (or unknown count)
    const nReq93 = n >= 10 || n === 0;
    if (rec >= 97 && nReq97) return `<span class="quality-badge badge-excellent" title="${rec}%${nStr} recommend — exceptional quality signal">🏆 Top Pick</span>`;
    if (rec >= 93 && nReq93) return `<span class="quality-badge badge-excellent" title="${rec}%${nStr} recommend">⭐ Excellent</span>`;
    if (rec >= 88)           return `<span class="quality-badge badge-good"      title="${rec}%${nStr} recommend">✅ Good</span>`;
  }
  // For Amazon US: use star rating + review count as quality signal
  if (p.source === "amazon_us") {
    const stars = p.AvgStarRating;
    const reviews = p.ReviewsCount || 0;
    if (stars >= 4.7 && reviews >= 1000) return `<span class="quality-badge badge-excellent" title="${stars}★ from ${reviews.toLocaleString()} reviews — top-rated on Amazon">🏆 Top Rated</span>`;
    if (stars >= 4.5 && reviews >= 500)  return `<span class="quality-badge badge-good" title="${stars}★ from ${reviews.toLocaleString()} reviews">⭐ Highly Rated</span>`;
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

  // Price range: new number inputs replace old slider
  const minPriceEl = document.getElementById("filter-min-price");
  const maxPriceEl = document.getElementById("filter-max-price");
  const minPriceRaw = minPriceEl ? parseInt(minPriceEl.value) : 0;
  const maxPriceRaw = maxPriceEl ? parseInt(maxPriceEl.value) : 0;
  const minPrice = !isNaN(minPriceRaw) && minPriceRaw > 0 ? minPriceRaw : "";
  const maxPrice = !isNaN(maxPriceRaw) && maxPriceRaw > 0 ? maxPriceRaw : "";

  const historyToggle  = document.getElementById("history-toggle");
  const priceDropToggle = document.getElementById("price-drop-toggle");
  const hasHistory  = historyToggle  && historyToggle.dataset.active  === "1" ? "1" : "";
  const priceDrop   = priceDropToggle && priceDropToggle.dataset.active === "1" ? "1" : "";

  return {
    q: document.getElementById("search-input").value.trim(),
    main_category: document.getElementById("filter-main-category").value,
    category: document.getElementById("filter-category").value,
    min_stars: starsVal > 0 ? starsVal : "",
    max_return: returnVal < 1.4 ? returnVal : "",
    min_reviews: reviewsVal > 0 ? reviewsVal : "",
    min_recommend: recommendVal > 0 ? recommendVal : "",
    min_price: minPrice,
    max_price: maxPrice,
    sort: sortField,
    order: sortDir,
    source: document.getElementById("filter-source").value,
    keyword: activeKeyword,
    brand: activeBrand,
    avoid: avoidMode ? "1" : "",
    has_image: (typeof photosMode !== "undefined" && photosMode) ? "1" : "",
    has_history: hasHistory,
    price_drop: priceDrop,
    page: currentPage
  };
}

// ---- FR Gov client-side rendering (bypasses API layer) ----
function frGovToProduct(g) {
  return {
    id: null, Name: g.n, Category: g.c, MainCategory: g.m,
    ProductURL: g.u || "", Price_CZK: null, Price_EUR: null,
    country: "FR", currency: "EUR",
    AvgStarRating: null, StarRatingsCount: null, ReviewsCount: null,
    RecommendRate_pct: null, ReturnRate_pct: null,
    Stars5_Count: null, Stars4_Count: null, Stars3_Count: null,
    Stars2_Count: null, Stars1_Count: null,
    source: "fr_ir", source_rank: 0, source_total: 0,
    keywords: null,
    details_json: { _ir_score: g.s, _ir_date: g.d, _ir_sub: g.sub },
  };
}

function renderFrGov(filters) {
  const PAGE = 24;
  let data = window.__FR_GOV || [];
  const q = (filters.q || "").toLowerCase();
  if (q) data = data.filter(g => g.n.toLowerCase().includes(q) || g.c.toLowerCase().includes(q));
  if (filters.main_category) data = data.filter(g => g.m === filters.main_category);
  if (filters.category)      data = data.filter(g => g.c === filters.category);

  // Sort: default = note_ir desc; Name = alphabetical; repairability/durability = by score
  const sortField = filters.sort || "repairability_score_fr";
  const sortAsc   = filters.order === "asc";
  if (sortField === "Name") {
    data = [...data].sort((a,b) => sortAsc ? a.n.localeCompare(b.n) : b.n.localeCompare(a.n));
  } else {
    // All numeric sorts fall back to note_ir (g.s) since fr_gov has no other numeric fields
    data = [...data].sort((a,b) => sortAsc ? (a.s||0) - (b.s||0) : (b.s||0) - (a.s||0));
  }

  const total = data.length;
  const page  = parseInt(filters.page) || 1;
  const slice = data.slice((page-1)*PAGE, page*PAGE);
  renderProducts({
    products: slice.map(frGovToProduct),
    total, page,
    pages: Math.ceil(Math.max(total,1)/PAGE),
    page_size: PAGE,
  });
}

function buildFrGovCategories() {
  const data = window.__FR_GOV || [];
  const tree = {};
  data.forEach(g => {
    const main = g.m || "Autres";
    if (!tree[main]) tree[main] = {};
    tree[main][g.c] = (tree[main][g.c] || 0) + 1;
  });
  return Object.keys(tree).sort().map(main => ({
    main,
    subs: Object.entries(tree[main])
      .sort((a,b) => b[1]-a[1])
      .map(([sub, count]) => ({ sub, count }))
  }));
}

// ---- API calls ----
/** Build 24 skeleton cards that match real card layout height during load. */
function buildSkeletonGrid() {
  const card = `<div class="product-card skel-card">
    <div class="skel skel-img"></div>
    <div class="skel skel-badge"></div>
    <div class="skel skel-name skel-name-1"></div>
    <div class="skel skel-name skel-name-2"></div>
    <div class="skel-metrics">
      <div class="skel skel-metric"></div>
      <div class="skel skel-metric"></div>
    </div>
    <div class="skel skel-stars"></div>
    <div class="skel skel-footer"></div>
  </div>`;
  return Array(24).fill(card).join("");
}

/** Push current filter state to the browser URL bar (allows sharing/bookmarking). */
function pushFilterState() {
  const f = getFilters();
  const sp = new URLSearchParams();
  // Only include non-default values to keep the URL clean
  if (f.q)            sp.set("q", f.q);
  if (f.main_category) sp.set("mc", f.main_category);
  if (f.category)     sp.set("cat", f.category);
  if (f.source)       sp.set("src", f.source);
  if (f.min_stars)    sp.set("stars", f.min_stars);
  if (f.max_return && parseFloat(f.max_return) < 1.4) sp.set("ret", f.max_return);
  if (f.min_reviews)  sp.set("rev", f.min_reviews);
  if (f.min_recommend) sp.set("rec", f.min_recommend);
  if (f.min_price)    sp.set("minprice", f.min_price);
  if (f.max_price)    sp.set("price", f.max_price);
  if (f.keyword)      sp.set("kw", f.keyword);
  if (f.brand)        sp.set("brand", f.brand);
  if (f.avoid === "1")     sp.set("avoid", "1");
  if (f.has_image === "1")   sp.set("img", "1");
  if (f.has_history === "1") sp.set("hist", "1");
  if (f.price_drop === "1")  sp.set("drop", "1");
  if (f.sort && f.sort !== "RecommendRate_pct") sp.set("sort", f.sort);
  if (f.order && f.order !== "desc") sp.set("order", f.order);
  const qs = sp.toString();
  const newUrl = qs ? "?" + qs : window.location.pathname;
  history.replaceState(null, "", newUrl);
}

/** Apply filter values from current URL search params on page load. */
function applyUrlFilters() {
  const sp = new URLSearchParams(window.location.search);
  if (!sp.toString()) return; // nothing in URL
  const set = (id, val) => { const el = document.getElementById(id); if (el && val !== null) el.value = val; };
  if (sp.get("q"))    set("search-input", sp.get("q"));
  if (sp.get("src"))  set("filter-source", sp.get("src"));
  if (sp.get("mc"))   set("filter-main-category", sp.get("mc"));
  // category needs to wait for subcategory dropdown to populate — handled after fetchCategories()
  if (sp.get("stars")) { set("filter-stars", sp.get("stars")); const el=document.getElementById("stars-val"); if(el) el.textContent=sp.get("stars")+"★"; }
  if (sp.get("ret"))   { set("filter-return", sp.get("ret")); const el=document.getElementById("return-val"); if(el) el.textContent=sp.get("ret")+"%"; }
  if (sp.get("rev"))   { set("filter-reviews", sp.get("rev")); const el=document.getElementById("reviews-val"); if(el) el.textContent=sp.get("rev"); }
  if (sp.get("rec"))   { set("filter-recommend", sp.get("rec")); const el=document.getElementById("recommend-val"); if(el) el.textContent=sp.get("rec")+"%"; }
  if (sp.get("minprice")) {
    set("filter-min-price", sp.get("minprice"));
  }
  if (sp.get("price")) {
    set("filter-max-price", sp.get("price"));
  }
  if (sp.get("kw")) {
    activeKeyword = sp.get("kw");
    document.querySelectorAll(".kw-pill").forEach(b => {
      if (b.dataset.keyword === activeKeyword) b.classList.add("active");
    });
  }
  if (sp.get("brand")) {
    activeBrand = sp.get("brand");
  }
  if (sp.get("avoid") === "1") avoidMode = true;
  if (sp.get("img")   === "1") {
    photosMode = true;
    const pt = document.getElementById("photos-toggle");
    if (pt) { pt.dataset.active = "1"; pt.textContent = "📷 Showing only products with photos"; pt.classList.add("avoid-btn-active"); }
  }
  if (sp.get("hist") === "1") {
    const ht = document.getElementById("history-toggle");
    if (ht) { ht.dataset.active = "1"; ht.textContent = "📈 Showing only products with history"; ht.classList.add("avoid-btn-active"); }
  }
  if (sp.get("drop") === "1") {
    const dt = document.getElementById("price-drop-toggle");
    if (dt) { dt.dataset.active = "1"; dt.textContent = "💸 Showing only price drops"; dt.classList.add("avoid-btn-active"); }
  }
  if (sp.get("sort")) {
    const sortEl = document.getElementById("sort-by");
    const dir = sp.get("order") || "desc";
    const val = sp.get("sort") + (dir === "desc" ? "_desc" : "");
    if (sortEl) sortEl.value = val;
  }
  // Store pending category for after subcats load
  if (sp.get("cat")) window._pendingCategory = sp.get("cat");
}

async function fetchProducts(appendMode) {
  const grid = document.getElementById("product-grid");
  if (!appendMode) grid.innerHTML = buildSkeletonGrid();

  const filters = getFilters();
  if (!appendMode) pushFilterState();    // sync URL bar with current filters

  if (filters.source === "fr_ir") {
    await ensureIrData();
    renderFrGov(filters);
    return;
  }

  const params = new URLSearchParams(filters);
  const res = await fetch(`${API_BASE}/api/products?${params}`);
  const data = await res.json();
  renderProducts(data, appendMode);
}

async function loadMoreProducts() {
  const btn = document.getElementById("load-more-btn");
  if (btn) { btn.disabled = true; btn.textContent = "Loading…"; }
  currentPage++;
  await fetchProducts(true /* appendMode */);
}

async function fetchCategories() {
  const src = document.getElementById("filter-source").value;

  // FR Gov uses embedded data — no API call needed
  if (src === "fr_ir") {
    categoriesTree = buildFrGovCategories();
    const mainSel = document.getElementById("filter-main-category");
    const seen = new Set();
    mainSel.innerHTML = '<option value="">All categories</option>' +
      categoriesTree.map(({ main, subs }) => {
        const label = translateCat(main);
        if (seen.has(label)) return "";   // collapse same-English duplicates
        seen.add(label);
        return `<option value="${escHtml(main)}">${escHtml(label)}</option>`;
      }).join("");
    renderCatPills();
    return;
  }

  const SOURCE_COUNTRY_MAP = {
    "alza.cz":"CZ", "heureka.cz":"CZ", "zbozi.cz":"CZ", "datart.cz":"CZ", "planeo.cz":"CZ",
    "heureka.sk":"SK",
    "amazon_de":"DE", "otto_de":"DE", "otto":"DE", "saturn_de":"DE",
    "mediamarkt":"DE", "testberichte":"DE", "conrad":"DE",
    "geizhals":"AT",
    "digitec":"CH",
    "fnac":"FR", "fr_ir":"FR_IR",
    "ceneo":"PL",
    "coolblue":"NL",
    "prisjakt":"SE", "pricerunner_se":"SE",
    "pricerunner":"DK",
    "amazon_us":"US",
    // legacy keys (kept for backwards compat)
    alza:"CZ", heureka:"CZ", zbozi:"CZ", datart:"CZ",
    amazon:"DE", otto:"DE", heureka_sk:"SK",
  };
  // No source selected → no country filter → all-market categories
  const country = src ? (SOURCE_COUNTRY_MAP[src] || "") : "";
  // When a specific source is chosen, pass it to filter categories to only those
  // that actually have products from that source — avoids showing phantom categories.
  const qp = new URLSearchParams();
  if (country) qp.set("country", country);
  if (src)     qp.set("source",  src);
  const qs = qp.toString();
  const res = await fetch(`${API_BASE}/api/categories${qs ? "?" + qs : ""}`);
  categoriesTree = await res.json();

  const mainSel = document.getElementById("filter-main-category");
  const seenMain = new Set();
  mainSel.innerHTML = '<option value="">All categories</option>' +
    categoriesTree.map(({ main, subs }) => {
      const label = translateCat(main);
      if (seenMain.has(label)) return "";   // collapse same-English duplicates
      seenMain.add(label);
      return `<option value="${escHtml(main)}">${escHtml(label)}</option>`;
    }).join("");

  // Restore main category value if it was in URL (set by applyUrlFilters)
  const sp = new URLSearchParams(window.location.search);
  if (sp.get("mc")) {
    mainSel.value = sp.get("mc");
    if (mainSel.value) {
      populateSubcategories(mainSel.value);
      // Restore subcategory value after dropdown is populated
      if (window._pendingCategory) {
        const subSel = document.getElementById("filter-category");
        if (subSel) subSel.value = window._pendingCategory;
        window._pendingCategory = null;
      }
    }
  }

  // Render category quick-pick pills now that categoriesTree is loaded
  renderCatPills();
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

  // Group subs by their group label for <optgroup> rendering
  const groups = {};   // group_label -> [{sub, count}]
  const groupOrder = [];
  entry.subs.forEach(({ sub, count, group }) => {
    const g = group || "";
    if (!groups[g]) { groups[g] = []; groupOrder.push(g); }
    groups[g].push({ sub, count });
  });

  let html = '<option value="">All subcategories</option>';
  groupOrder.forEach(g => {
    const items = groups[g];
    const opts = items.map(({ sub }) =>
      `<option value="${escHtml(sub)}">${escHtml(sub)}</option>`
    ).join("");
    if (g) {
      html += `<optgroup label="${escHtml(g)}">${opts}</optgroup>`;
    } else {
      html += opts;
    }
  });
  subSel.innerHTML = html;
  subGroup.style.display = "";
}

async function fetchKeywords() {
  const res = await fetch(`${API_BASE}/api/keywords`);
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
  const res = await fetch(`${API_BASE}/api/stats`);
  const d = await res.json();
  document.getElementById("stat-total").textContent = d.total.toLocaleString();
  document.getElementById("stat-stars").textContent = d.avg_stars ?? "—";
  // Show image coverage percentage if available
  if (d.with_images != null && d.total > 0) {
    const pct = Math.round(d.with_images / d.total * 100);
    const imgPill = document.getElementById("stat-images");
    if (imgPill) {
      imgPill.textContent = `📷 ${pct}%`;
      imgPill.style.display = "";
      imgPill.title = `${d.with_images.toLocaleString()} of ${d.total.toLocaleString()} products have photos`;
    }
  }
  // Update history toggle button with count
  if (d.with_history != null && d.with_history > 0) {
    _withHistoryCount = d.with_history;
    const ht = document.getElementById("history-toggle");
    if (ht && ht.dataset.active !== "1") {
      ht.textContent = `📈 Price history (${d.with_history.toLocaleString()})`;
    }
  }
  // Update photos toggle with image count
  if (d.with_images != null && d.with_images > 0) {
    _withImagesCount = d.with_images;
    const pt = document.getElementById("photos-toggle");
    if (pt && pt.dataset.active !== "1") {
      pt.textContent = `📷 Products with photos (${d.with_images.toLocaleString()})`;
    }
  }
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

async function fetchSnapshotDeltas() {
  try {
    const CACHE_KEY = "qdb_snap_deltas";
    const TTL = 2 * 3600 * 1000; // 2 hours
    let j = null;
    // Try localStorage cache first
    try {
      const raw = localStorage.getItem(CACHE_KEY);
      if (raw) {
        const { ts, data } = JSON.parse(raw);
        if (Date.now() - ts < TTL) j = data;
      }
    } catch (_) {}
    if (!j) {
      const res = await fetch(`${API_BASE}/api/snapshot-deltas`);
      j = await res.json();
      try { localStorage.setItem(CACHE_KEY, JSON.stringify({ ts: Date.now(), data: j })); } catch (_) {}
    }
    if (j && typeof j === "object" && !j.error) {
      snapshotDeltaMap = new Map(Object.entries(j));
      // Update price-drop button label with count
      const dropCount = [...snapshotDeltaMap.values()].filter(v => v[2] != null && v[2] < 0).length;
      const dropBtn = document.getElementById("price-drop-toggle");
      if (dropBtn && dropBtn.dataset.active !== "1" && dropCount > 0) {
        dropBtn.textContent = `💸 Price drops (${dropCount.toLocaleString()})`;
      }
    }
    // Re-render grid so delta chips appear.
    if (document.getElementById("product-grid")?.children.length) {
      fetchProducts();
    }
  } catch (_) { /* delta is optional, fail silent */ }
}


const _CURRENCY_SYMBOLS = {
  CZK: "Kč", EUR: "€", USD: "$", GBP: "£", CHF: "CHF",
  SEK: "kr", DKK: "kr", NOK: "kr", PLN: "zł", HUF: "Ft",
};
function _currSym(currency) {
  return _CURRENCY_SYMBOLS[currency] || (currency || "Kč");
}

/** Build the inline history strip HTML for a product card.
 *  Shows price/rating trend + lazy sparkline. Replaces the old 📈 badge.
 */
function buildHistoryStrip(url, hasHistory, currency, p) {
  const hasHist = hasHistory || snapshotUrlSet.has(normUrl(url));
  if (!hasHist && !snapshotDeltaMap.has(url)) return "";

  const d = snapshotDeltaMap.get(url);

  if (!d) {
    // Has history but no notable change — show stable indicator with current quality metrics
    const stableDays = typeof hasHistory === "number" && hasHistory > 0
      ? (hasHistory >= 60 ? `${Math.round(hasHistory / 30)}mo` : `${hasHistory}d`)
      : "";
    const stableLabel = stableDays ? `Stable ${stableDays}` : "Price tracked";
    // Show current quality metrics inline for quick reference
    let metricBits = "";
    if (p) {
      if (p.RecommendRate_pct != null) {
        const cls = p.RecommendRate_pct >= 90 ? "hist-good" : p.RecommendRate_pct >= 70 ? "" : "hist-bad";
        metricBits += `<span class="hist-stable-metric ${cls}">${Math.round(p.RecommendRate_pct)}%</span>`;
      } else if (p.AvgStarRating != null) {
        const cls = p.AvgStarRating >= 4.0 ? "hist-good" : p.AvgStarRating >= 3.0 ? "" : "hist-bad";
        metricBits += `<span class="hist-stable-metric ${cls}">★${p.AvgStarRating.toFixed(1)}</span>`;
      }
    }
    return `<div class="card-history-strip stable" data-hist-url="${escHtml(url)}" style="cursor:pointer" title="Click to view full history chart">`+
      `<span class="hist-icon">📊</span>`+
      `<span class="hist-stable-lbl">${stableLabel}</span>`+
      `${metricBits}`+
      `<span class="hist-chart-hint">chart →</span>`+
      `</div>`;
  }

  const [recDelta, starsDelta, priceDelta, days, firstPrice, lastPrice] = d;
  const daysLabel = days > 0 ? (days >= 60 ? `${Math.round(days / 30)}mo` : `${days}d`) : "";

  // Price row: show "now X (was Y)" when we have both first and last price
  let priceHtml = "";
  if (firstPrice != null && lastPrice != null) {
    const arrow = lastPrice < firstPrice ? "↓" : lastPrice > firstPrice ? "↑" : "→";
    const priceCls = lastPrice < firstPrice ? "hist-good" : lastPrice > firstPrice ? "hist-bad" : "hist-neutral";
    const diff = lastPrice - firstPrice;
    const sym = _currSym(currency);
    const subStr = diff === 0 ? "" : `was ${Math.round(firstPrice).toLocaleString()} ${sym}`;
    priceHtml = `<div class="hist-row"><span class="hist-lbl">Price</span>`+
      `<span class="hist-val ${priceCls}">${arrow} ${Math.round(lastPrice).toLocaleString()} ${sym}</span>`+
      `${subStr ? `<span class="hist-sub">${subStr}</span>` : ""}</div>`;
  } else if (priceDelta != null) {
    const arrow = priceDelta < 0 ? "↓" : priceDelta > 0 ? "↑" : "→";
    const priceCls = priceDelta < 0 ? "hist-good" : priceDelta > 0 ? "hist-bad" : "hist-neutral";
    const sym = _currSym(currency);
    priceHtml = `<div class="hist-row"><span class="hist-lbl">Price</span>`+
      `<span class="hist-val ${priceCls}">${arrow} ${(priceDelta > 0 ? "+" : "")}${Math.round(priceDelta).toLocaleString()} ${sym}</span></div>`;
  }

  // Rating row
  let ratingHtml = "";
  if (recDelta !== null && Math.abs(recDelta) >= 0.5) {
    const arrow = recDelta > 0 ? "↑" : "↓";
    const cls = recDelta > 0 ? "hist-good" : "hist-bad";
    ratingHtml = `<div class="hist-row"><span class="hist-lbl">Rating</span>`+
      `<span class="hist-val ${cls}">${arrow} ${(recDelta > 0 ? "+" : "")}${recDelta.toFixed(1)}%</span></div>`;
  } else if (starsDelta !== null && Math.abs(starsDelta) >= 0.05) {
    const arrow = starsDelta > 0 ? "↑" : "↓";
    const cls = starsDelta > 0 ? "hist-good" : "hist-bad";
    ratingHtml = `<div class="hist-row"><span class="hist-lbl">Stars</span>`+
      `<span class="hist-val ${cls}">${arrow} ${(starsDelta > 0 ? "+" : "")}${starsDelta.toFixed(2)}★</span></div>`;
  }

  // Title: "Price & rating" when both changed, "Price" or "Rating" for single change
  const hasPriceChange  = priceHtml !== "";
  const hasRatingChange = ratingHtml !== "";
  const histTitle = hasPriceChange && hasRatingChange ? "Price & rating"
                  : hasPriceChange ? "Price change"
                  : "Rating change";

  // Color the strip border based on primary direction.
  // Use relative thresholds (≥3% change) so EUR and CZK products are treated equally.
  let stripCls = "card-history-strip";
  const _pctDrop = (priceDelta !== null && lastPrice && lastPrice > 0)
    ? priceDelta / (lastPrice - priceDelta)   // delta / firstPrice
    : null;
  if (_pctDrop !== null && _pctDrop <= -0.03)       stripCls += " hist-strip-drop";
  else if (_pctDrop !== null && _pctDrop >= 0.03)   stripCls += " hist-strip-rise";
  else if (priceHtml === "" && ratingHtml !== "")    stripCls += " hist-strip-rating";

  const stripTitle = buildHistoryTitle(url, currency);
  return `<div class="${stripCls}" data-hist-url="${escHtml(url)}" title="${escHtml(stripTitle)}">`+
    `<div class="hist-header">`+
      `<span class="hist-title">${histTitle}</span>`+
      `${daysLabel ? `<span class="hist-days">${daysLabel}</span>` : ""}`+
    `</div>`+
    `<div class="hist-body">`+
      `<svg class="card-sparkline" data-url="${escHtml(url)}" data-loaded="0" viewBox="0 0 80 32" width="80" height="32" xmlns="http://www.w3.org/2000/svg">`+
        `<line x1="4" y1="16" x2="76" y2="16" stroke="var(--border)" stroke-width="1" stroke-dasharray="3,2"/>`+
      `</svg>`+
      `<div class="hist-changes">${priceHtml}${ratingHtml}</div>`+
    `</div>`+
  `</div>`;
}

// ── Brand logo helpers ────────────────────────────────────────────────────────
const _BRAND_DOMAINS = {
  Samsung:"samsung.com", LG:"lg.com", Bosch:"bosch-home.com", Miele:"miele.com",
  Siemens:"siemens-home.com", AEG:"aeg.com", Beko:"beko.com", Hisense:"hisense.com",
  Haier:"haier.com", Candy:"candy.it", Hoover:"hoover.com", Hotpoint:"hotpoint.eu",
  Whirlpool:"whirlpool.com", Electrolux:"electrolux.com", Indesit:"indesit.com",
  Gorenje:"gorenje.com", Apple:"apple.com", Microsoft:"microsoft.com", Dell:"dell.com",
  Lenovo:"lenovo.com", HP:"hp.com", Asus:"asus.com", Acer:"acer.com",
  Sony:"sony.com", Panasonic:"panasonic.com", Philips:"philips.com", TCL:"tcl.com",
  Sharp:"sharp.com", Toshiba:"toshiba.com", Nokia:"nokia.com", Dyson:"dyson.com",
  Rowenta:"rowenta.com", Neff:"neff.com", Bauknecht:"bauknecht.eu", Zanussi:"zanussi.com",
  Liebherr:"liebherr.com", DeLonghi:"delonghi.com", Jura:"jura.com", Melitta:"melitta.com",
  Krups:"krups.com", Saeco:"saeco.com", Nespresso:"nespresso.com", Xiaomi:"xiaomi.com",
  OnePlus:"oneplus.com", Google:"google.com", Motorola:"motorola.com", Huawei:"huawei.com",
  Honor:"honor.com", Bose:"bose.com", JBL:"jbl.com", Sennheiser:"sennheiser.com",
  Jabra:"jabra.com", Beats:"beatsbydre.com", Canon:"canon.com", Nikon:"nikon.com",
  Fujifilm:"fujifilm.com", Olympus:"olympus.com", Garmin:"garmin.com",
  Realme:"realme.com", Oppo:"oppo.com", Vivo:"vivo.com", "Bowers & Wilkins":"bowerswilkins.com",
  Marshall:"marshallheadphones.com", Denon:"denon.com", Pioneer:"pioneer-audiovisual.eu",
  "Audio-Technica":"audio-technica.com", Shure:"shure.com", AKG:"akg.com",
};

function brandLogoUrl(brand) {
  if (!brand) return null;
  const domain = _BRAND_DOMAINS[brand] || _BRAND_DOMAINS[brand.split(" ")[0]];
  if (domain) return `https://logo.clearbit.com/${domain}`;
  const slug = brand.toLowerCase().replace(/[^a-z0-9]/g, "");
  if (slug.length < 2) return null;
  return `https://logo.clearbit.com/${slug}.com`;
}

/** Export the current visible products as a CSV file download. */
function exportVisibleCSV() {
  // Collect all products currently in cardDataMap (rendered in the grid)
  const products = [...cardDataMap.values()];
  if (!products.length) return;

  const headers = ["Name","Category","Source","Price_CZK","Price_EUR","Stars","Reviews",
                   "RecommendRate%","ReturnRate%","URL","Scraped"];
  const esc = v => v == null ? "" : String(v).replace(/"/g, '""');

  const rows = products.map(p => [
    `"${esc(p.Name)}"`,
    `"${esc(p.NormalizedCategory || p.Category)}"`,
    `"${esc(p.source)}"`,
    p.Price_CZK ?? "",
    p.Price_EUR ?? "",
    p.AvgStarRating != null ? p.AvgStarRating.toFixed(2) : "",
    p.ReviewsCount ?? "",
    p.RecommendRate_pct != null ? p.RecommendRate_pct.toFixed(1) : "",
    p.ReturnRate_pct != null ? p.ReturnRate_pct.toFixed(2) : "",
    `"${esc(p.ProductURL)}"`,
    `"${esc(p.scraped_at)}"`,
  ].join(","));

  const csv = [headers.join(","), ...rows].join("\n");
  const blob = new Blob(["﻿" + csv], { type: "text/csv;charset=utf-8;" });
  const url  = URL.createObjectURL(blob);
  const a    = document.createElement("a");
  a.href = url;
  a.download = `qualitydb-export-${new Date().toISOString().slice(0,10)}.csv`;
  a.click();
  URL.revokeObjectURL(url);
}

/** Highlight search-query tokens in a card name. Returns safe HTML string. */
function highlightQuery(text) {
  const q = (document.getElementById("search-input")?.value || "").trim();
  if (!q || q.length < 2) return escHtml(text);
  // Split query into tokens, escape each for regex and for HTML
  const tokens = q.split(/\s+/).filter(t => t.length >= 2)
    .map(t => t.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"));
  if (!tokens.length) return escHtml(text);
  const re = new RegExp(`(${tokens.join("|")})`, "gi");
  return escHtml(text).replace(re, '<mark class="hl">$1</mark>');
}

function renderCard(p) {
  const cardId = ++cardIdSeq;
  cardDataMap.set(cardId, p);
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
  const _srcLbl = SOURCE_LABELS[p.source] || p.source || "Unknown";
  const _srcFlag = SOURCE_FLAGS[p.source] || "";
  const sourceLabel = _srcFlag ? `${_srcFlag} ${_srcLbl}` : _srcLbl;
  // Sanitize source name for CSS: replace dots with dashes (e.g. "heureka.cz" → "src-heureka-cz")
  const srcSlug = (p.source || "").replace(/\./g, "-").replace(/[^a-z0-9_-]/gi, "-").toLowerCase();
  const sourceCls = `source-badge scraper src-${srcSlug}`;
  const sourceBadge = `<span class="${sourceCls}" data-filter-source="${escHtml(p.source || "")}" title="Click to filter by ${escHtml(_srcLbl)}">${sourceLabel}</span>`;

  // Brand logo — use image_url from DB when available, otherwise Clearbit brand logo.
  // Fall back to extracting brand from first word(s) of product name when brand col is null.
  const _knownBrands = Object.keys(_BRAND_DOMAINS);
  const _brandRe = new RegExp(`\\b(${_knownBrands.map(b => b.replace(/[.*+?^${}()|[\]\\]/g,`\\$&`)).join("|")})\\b`, "i");
  function _inferBrand(name) {
    const m = name && name.match(_brandRe);
    if (m) return _knownBrands.find(b => b.toLowerCase() === m[1].toLowerCase()) || m[1];
    return (name || "").split(" ")[0];
  }
  const brandName = p.brand || _inferBrand(p.Name || "");
  const logoUrl = brandLogoUrl(brandName);
  const brandLogo = logoUrl
    ? `<img class="card-brand-logo" src="${escHtml(logoUrl)}" alt="${escHtml(brandName)}"
         onerror="this.style.display='none'" loading="lazy">`
    : "";

  // Product thumbnail: full-width image from og:image (fetched by server, cached in image_url)
  // Show placeholder when no image yet — keeps card height consistent across the grid.
  const catIcon = (() => {
    const c = (p.Category || p.NormalizedCategory || "").toLowerCase();
    // Phones & Tablets
    if (/phone|smartphone|mobil|telefon|handy/.test(c)) return "📱";
    if (/tablet/.test(c)) return "📱";
    // Computers
    if (/laptop|notebook/.test(c)) return "💻";
    if (/monitor/.test(c)) return "🖥️";
    if (/desktop|pc case|mini pc/.test(c)) return "🖥️";
    if (/keyboard|klávesnic/.test(c)) return "⌨️";
    if (/mouse|myš/.test(c)) return "🖱️";
    if (/ssd|hard drive|flash drive|external drive|storage/.test(c)) return "💾";
    if (/ram|memory/.test(c)) return "🧠";
    if (/graphics|gpu/.test(c)) return "🎴";
    if (/cpu|processor/.test(c)) return "⚙️";
    if (/power supply/.test(c)) return "🔋";
    if (/pc cool/.test(c)) return "❄️";
    if (/printer/.test(c)) return "🖨️";
    if (/webcam/.test(c)) return "📹";
    if (/motherboard/.test(c)) return "🔌";
    if (/docking/.test(c)) return "🔌";
    if (/laptop acc|computer acc/.test(c)) return "🎒";
    if (/software/.test(c)) return "💿";
    // Audio & Entertainment
    if (/tv|televiz|fernseher|televisor/.test(c)) return "📺";
    if (/headphone|sluch|kopfhör|earphone|earbud/.test(c)) return "🎧";
    if (/soundbar/.test(c)) return "📻";
    if (/speaker|reproduk|głośn|lautsprecher/.test(c)) return "🔊";
    if (/microphone/.test(c)) return "🎤";
    if (/music acc|hi.fi|turntable|radio/.test(c)) return "🎵";
    if (/musical instrument/.test(c)) return "🎸";
    // Cameras & Wearables
    if (/camera|fotoaparát|kamera/.test(c)) return "📷";
    if (/drone|dron/.test(c)) return "🚁";
    if (/watch|hodinky/.test(c)) return "⌚";
    if (/fitness|tracker/.test(c)) return "🏃";
    // Gaming
    if (/game controller|gamepad/.test(c)) return "🕹️";
    if (/gaming chair/.test(c)) return "🪑";
    if (/gaming acc/.test(c)) return "🎮";
    if (/lego/.test(c)) return "🧱";
    if (/toys|gaming|herní|video game/.test(c)) return "🎮";
    // Home Appliances
    if (/robot vacuum/.test(c)) return "🤖";
    if (/vacuum|stick vac|vysavač|staubsauger/.test(c)) return "🧹";
    if (/washing|pračka|waschmaschine/.test(c)) return "🫧";
    if (/dishwasher|myčka/.test(c)) return "🍽️";
    if (/dryer|sušička/.test(c)) return "🌀";
    if (/fridge|refrigerator|ledničk|kühlschrank/.test(c)) return "🧊";
    if (/microwave/.test(c)) return "♨️";
    if (/oven|stove|sporák|trouba/.test(c)) return "🍳";
    if (/air purif|luftreinig/.test(c)) return "💨";
    if (/air condition/.test(c)) return "❄️";
    if (/coffee|kávovar|kaffee|espresso/.test(c)) return "☕";
    if (/kettle|varná konvice|wasserkocher/.test(c)) return "🫖";
    if (/kitchen|kuchyň|küche/.test(c)) return "🥘";
    if (/blender|mixer/.test(c)) return "🥤";
    if (/iron|žehlič/.test(c)) return "👔";
    if (/hair dry|fén/.test(c)) return "💨";
    if (/fan|ventilátor/.test(c)) return "🌬️";
    // Cables, accessories, other
    if (/cable|kabel|accessori/.test(c)) return "🔌";
    return "📦";
  })();
  const productImg = (p.image_url && p.image_url !== "__none__")
    ? `<div class="card-img-wrap">
         <img class="card-product-img" src="${escHtml(p.image_url)}"
              alt="${escHtml(p.Name || '')}" loading="lazy"
              data-fallback="${escHtml(catIcon)}"
              onload="this.classList.add('img-loaded');this.closest('.card-img-wrap').style.animation='none';this.closest('.card-img-wrap').style.background='#f5f5f5'"
              onerror="window._imgErr(this)">
       </div>`
    : `<div class="card-img-placeholder" aria-hidden="true"><span>${catIcon}</span></div>`;

  // Category rank badge: show "#N of T" with colour coding for top/bottom 10%
  // Require ≥10 products in category for a meaningful rank
  const hasRank = p.source_rank && p.source_total && p.source_total >= 10;
  const catEn   = p.NormalizedCategory || translateCat(p.Category || "");
  const rankDisplay = hasRank
    ? (p.source_rank === 1
        ? `<span class="rank-top1">🏆 Best</span><span class="rank-total"> of ${p.source_total.toLocaleString()}</span>`
        : `<span class="rank-num">#${p.source_rank.toLocaleString()}</span><span class="rank-total"> of ${p.source_total.toLocaleString()}</span>`)
    : "—";
  const rankClass = hasRank
    ? (p.source_rank === 1 ? "good rank-is-1"
      : p.source_rank <= Math.ceil(p.source_total * 0.1) ? "good"
      : p.source_rank >= Math.floor(p.source_total * 0.9) ? "bad" : "")
    : "";
  const shortCat = catEn && catEn.length > 16 ? catEn.slice(0, 14) + "…" : catEn;
  const rankLabel = shortCat ? "in " + shortCat : "Rank";

  const keywords = p.keywords ? JSON.parse(p.keywords) : [];
  const cardTags = keywords.slice(0, 2).map(k =>
    `<span class="kw-tag">${escHtml(k)}</span>`
  ).join("");

  const badge = qualityBadge(p);

  // 🔬 Expert-test brand badge — shows brand's average score from independent consumer-org tests
  const expertBadge = (() => {
    const raw = p.qt_brand_score;
    if (raw == null || raw === '') return '';
    const score = parseFloat(raw);
    if (isNaN(score)) return '';
    const cls   = score >= 75 ? 'expert-good' : score >= 65 ? 'expert-warn' : 'expert-bad';
    const bName = p.brand ? escHtml(p.brand) : '';
    const cat   = escHtml(p.NormalizedCategory || '');
    const tip   = bName
      ? `${bName} averages ${score.toFixed(1)}/100 in independent expert tests (${cat})`
      : `Brand avg expert score: ${score.toFixed(1)}/100 (${cat})`;
    return `<span class="expert-badge ${cls}" title="${tip}">🔬 ${score.toFixed(1)}</span>`;
  })();

  // 🆕 "new" badge — shown on products first seen within the last 14 days.
  // Uses first_seen_at (set once on first INSERT, never updated on re-scrape).
  // Falls back to scraped_at ONLY if first_seen_at is null AND the date is unique
  // enough that it can't be a bulk re-scrape (i.e., recent and not shared by >5k products).
  const newBadge = (() => {
    const ts = p.first_seen_at || null;   // null for existing products before this feature
    if (!ts) return "";
    const added = new Date(ts.replace(" ", "T"));
    if (isNaN(added.getTime())) return "";
    const daysAgo = (Date.now() - added.getTime()) / 86400000;
    if (daysAgo > 14) return "";
    const label = daysAgo < 1 ? "today"
      : daysAgo < 2 ? "yesterday"
      : `${Math.floor(daysAgo)}d ago`;
    return `<span class="new-badge" title="First seen ${label}">NEW</span>`;
  })();

  // ── Metric boxes: decide what to show in the two card metric slots ──────────
  // For French government products (source=fr_ir, no reviews): swap both metrics
  // for repairability/durability scores.
  // For Fnac products (source=fnac): they have real reviews/stars — keep normal
  // metrics but append the C1–C5 sub-criteria bar underneath if scores are present.
  const frScores = getFrenchScores(p);   // non-null when _ir_score or _dur_score exists
  const isFrGov  = p.source === "fr_ir"; // government-only rows: no stars/reviews at all
  let firstMetric, secondMetric, wtSubRow = "", irSubRow = "";

  if (isFrGov) {
    // Government data: replace BOTH metrics with repair + durability scores.
    // Read directly from details_json so this works even if getFrenchScores() returns null.
    let djFr = null;
    if (p.details_json) {
      try { djFr = typeof p.details_json === "string" ? JSON.parse(p.details_json) : p.details_json; }
      catch(e) {}
    }
    const repScore = djFr && djFr._ir_score != null ? parseFloat(djFr._ir_score) : (frScores ? frScores.repair : null);
    const durScore = djFr && djFr._dur_score != null ? parseFloat(djFr._dur_score) : (frScores ? frScores.durability : null);

    const repVal = repScore != null ? repScore.toFixed(1) + "/10" : "—";
    const repCls = repScore != null ? (repScore >= 7 ? "good" : repScore >= 4 ? "warn" : "bad") : "";
    firstMetric = `<div class="metric">
      <div class="metric-label">🔧 Réparabilité</div>
      <div class="metric-value ${repCls}">${repVal}</div>
    </div>`;

    if (durScore != null) {
      const durVal = durScore.toFixed(1) + "/10";
      const durCls = durScore >= 7 ? "good" : durScore >= 4 ? "warn" : "bad";
      secondMetric = `<div class="metric">
        <div class="metric-label">🛡️ Durabilité</div>
        <div class="metric-value ${durCls}">${durVal}</div>
      </div>`;
    } else {
      secondMetric = `<div class="metric">
        <div class="metric-label">Source</div>
        <div class="metric-value" style="font-size:0.75em">🏛️ Loi AGEC</div>
      </div>`;
    }
    // Sub-criteria bar — works with either C1/C2 (Fnac) or note_c2.1 (fr_gov) key format
    const subData = (djFr && djFr._ir_sub)
      ? (typeof djFr._ir_sub === "string" ? (() => { try { return JSON.parse(djFr._ir_sub); } catch(e) { return null; } })() : djFr._ir_sub)
      : (frScores ? frScores.sub : null);
    irSubRow = irSubCriteriaRow(subData);

  } else {
    // All other sources (including Fnac): normal first metric (rank / return rate)
    firstMetric = p.source === "alza"
      ? `<div class="metric">
          <div class="metric-label">Return rate</div>
          <div class="metric-value ${returnClass(p.ReturnRate_pct)}">${returnRateDisplay}</div>
         </div>`
      : `<div class="metric" ${hasRank ? `title="Ranked #${p.source_rank} of ${p.source_total} products in ${catEn || 'this category'} across all sources (Wilson score — weighs recommendation rate and star ratings by review count)"` : ""}>
          <div class="metric-label">${rankLabel}</div>
          <div class="metric-value rank-value ${rankClass}">${rankDisplay}</div>
         </div>`;

    if (false) { // warentest hidden
      // Warentest: always show WT-specific metrics, never rank/recommend
      // Grade: prefer details_json.sub_ratings.overall, fall back to AvgStarRating→grade approx
      let grade = undefined, info = null, djWt = {};
      try {
        if (p.details_json) {
          djWt = typeof p.details_json === "string" ? JSON.parse(p.details_json) : p.details_json;
          grade = ((djWt.sub_ratings || {}).overall || {}).grade;
        }
      } catch(e) {}
      // Fallback: approximate grade from star rating (stars → grade: 5★=1.0, 1★=5.5)
      if (grade === undefined && p.AvgStarRating) {
        grade = parseFloat((6.0 - p.AvgStarRating * (5.0 / 5.0)).toFixed(1));
        grade = Math.max(1.0, Math.min(5.5, grade));
      }
      info = wtGradeInfo(grade);
      const gradeDisplay = grade !== undefined ? grade.toFixed(1) : "—";
      firstMetric = `<div class="metric wt-grade-metric">
        <div class="metric-label">WT Grade</div>
        <div class="metric-value ${info ? info.textCls : ''}">
          ${gradeDisplay}
          ${info ? `<span class="wt-grade-label ${info.cls}">${info.label}</span>` : ""}
        </div>
      </div>`;
      const testDateFmt = wtFormatDate(p.test_date);
      const priceVal = priceStr(p);
      secondMetric = `<div class="metric">
        <div class="metric-label">${testDateFmt ? "Tested" : (priceVal ? "Price" : "Source")}</div>
        <div class="metric-value" style="font-size:0.85em;font-weight:600;color:var(--text2)">
          ${testDateFmt || priceVal || "Stiftung Warentest"}
        </div>
      </div>`;
      if (Object.keys(djWt).length > 0) wtSubRow = warentestSubRatings(djWt);
    } else {
      secondMetric = `<div class="metric">
        <div class="metric-label">${recommendLabel}</div>
        <div class="metric-value ${recommendClass(useStarsForRec ? p.AvgStarRating : p.RecommendRate_pct, useStarsForRec)}">${recommendDisplay}</div>
      </div>`;
    }

    // Fnac products with scores: add C1–C5 bars below normal metrics
    if (p.source === "fnac" && frScores) {
      irSubRow = irSubCriteriaRow(frScores.sub);
    }
  }

  // Stars row: hide for warentest (stars = grade proxy, not user reviews) and fr_ir gov
  const starsRow = isFrGov
    ? `<div class="card-stars ir-legal-note" title="Score légalement obligatoire — Loi AGEC / Décret 2020-1757">
         🏛️ <span>Score officiel obligatoire (Loi AGEC)</span>
       </div>`
    : p.source === "warentest" ? ""
    : `<div class="card-stars">
         <span class="stars-visual">${starsVisual(p.AvgStarRating)}</span>
         <span>${starsDisplay}</span>
         <span style="color:var(--text3)">(${reviewsDisplay} reviews)</span>
       </div>`;

  const historyStrip = p.ProductURL ? buildHistoryStrip(p.ProductURL, p.has_history, p.currency, p) : "";

  // Top-row history indicator: compact 📊 badge on the right when product has history.
  // Re-checks snapshotUrlSet (loaded async) AND the per-product has_history from DB.
  const hasHistData = (p.has_history > 0) || snapshotUrlSet.has(normUrl(p.ProductURL || ""));
  const histBadgeHtml = hasHistData && p.ProductURL
    ? `<span class="history-badge" data-hist-card="${cardId}" title="${escHtml(buildHistoryTitle(p.ProductURL, p.currency))}">📊</span>`
    : "";

  return `
  <div class="product-card" data-card-id="${cardId}">
    ${productImg}
    <div class="card-top-row">${sourceBadge}${badge}${expertBadge}${newBadge}${histBadgeHtml}</div>
    ${brandLogo}
    <div class="card-category" data-filter-cat="${escHtml(p.NormalizedCategory || p.Category || "")}" title="Click to filter by this category">${escHtml(catEn || translateCat(p.Category || ""))}</div>
    <div class="card-name">${highlightQuery(p.Name || "Unnamed")}</div>
    <div class="card-metrics">
      ${firstMetric}
      ${secondMetric}
    </div>
    ${wtSubRow}
    ${irSubRow}
    ${cardTags ? `<div class="card-tags">${cardTags}</div>` : ""}
    ${!isFrGov ? (() => { const ir = getIR(p); return ir ? repairabilityBadge(ir.s, ir.d) : ""; })() : ""}
    ${starsRow}
    ${historyStrip}
    <div class="card-footer">
      <span class="card-price">${priceDisplay}</span>
      ${p.ProductURL ? `<a class="card-link" href="${escHtml(p.ProductURL)}" target="_blank" onclick="event.stopPropagation()">View →</a>` : ""}
    </div>
  </div>`;
}

function renderProducts(data, appendMode) {
  const grid = document.getElementById("product-grid");
  const info = document.getElementById("results-info");

  if (isListView) grid.classList.add("list-view");
  else grid.classList.remove("list-view");

  if (!data.products.length) {
    const filters = getFilters();
    const tips = [];
    if (filters.q)            tips.push(`Search term <strong>"${escHtml(filters.q)}"</strong>`);
    if (filters.min_stars)    tips.push(`Min stars ≥ ${filters.min_stars}`);
    if (filters.min_reviews)  tips.push(`Min reviews ≥ ${filters.min_reviews}`);
    if (filters.min_recommend) tips.push(`Min recommend ≥ ${filters.min_recommend}%`);
    if (filters.min_price)    tips.push(`Min price ≥ ${parseInt(filters.min_price).toLocaleString()} Kč`);
    if (filters.max_price)    tips.push(`Max price ≤ ${parseInt(filters.max_price).toLocaleString()} Kč`);
    if (filters.source)       tips.push(`Source filter: ${escHtml(filters.source)}`);
    if (filters.brand)        tips.push(`Brand: ${escHtml(filters.brand)}`);
    if (filters.has_image === "1")  tips.push("Photos only");
    if (filters.has_history === "1") tips.push("Has price history");
    if (filters.price_drop === "1") tips.push("Price drops only");
    const tipsHtml = tips.length
      ? `<p style="font-size:0.85em;color:var(--text2)">Active filters: ${tips.join(" · ")}</p>`
      : "";
    grid.innerHTML = `
      <div class="empty-state">
        <div class="empty-state-icon">🔍</div>
        <h3>No products found</h3>
        ${tipsHtml}
        <p>Try relaxing one or more filters</p>
        <button class="reset-btn" onclick="document.getElementById('reset-filters').click()" style="margin-top:8px">Clear all filters</button>
      </div>`;
    info.innerHTML = "No results";
    document.getElementById("pagination").innerHTML = "";
    const lmb = document.getElementById("load-more-btn");
    if (lmb) lmb.style.display = "none";
    return;
  }

  const start = (data.page - 1) * data.page_size + 1;
  const end = Math.min(data.page * data.page_size, data.total);
  if (appendMode) {
    info.innerHTML = `Showing <strong>${end}</strong> of <strong>${data.total.toLocaleString()}</strong> products`;
    // Append new cards without clearing existing ones
    const fragment = document.createDocumentFragment();
    const temp = document.createElement("div");
    temp.innerHTML = data.products.map(renderCard).join("");
    while (temp.firstChild) fragment.appendChild(temp.firstChild);
    grid.appendChild(fragment);
  } else {
    info.innerHTML = `Showing <strong>${start}–${end}</strong> of <strong>${data.total.toLocaleString()}</strong> products`;
    cardDataMap.clear();
    grid.innerHTML = data.products.map(renderCard).join("");
  }

  // Show/hide load-more button
  const loadMoreBtn = document.getElementById("load-more-btn");
  if (loadMoreBtn) {
    if (data.page < data.pages) {
      loadMoreBtn.style.display = "";
      loadMoreBtn.disabled = false;
      loadMoreBtn.textContent = `Load more (${(data.total - end).toLocaleString()} remaining)`;
    } else {
      loadMoreBtn.style.display = "none";
    }
  }

  renderPagination(data.page, data.pages);
  if (!appendMode) renderActiveFilters();
  // Start lazy-loading sparklines for newly visible cards
  observeSparklines();
}

/** Render "active filter" chips above the grid so users can see what's filtered. */
function renderActiveFilters() {
  const bar = document.getElementById("active-filters-bar");
  if (!bar) return;
  const f = getFilters();
  const chips = [];

  if (f.q)                chips.push({ label: `"${f.q}"`,            key: "q" });
  if (f.main_category)    chips.push({ label: f.main_category.replace(/^[^\w]/,"").trim(), key: "mc" });
  if (f.category)         chips.push({ label: f.category,            key: "cat" });
  if (f.source)           chips.push({ label: f.source,              key: "src" });
  if (f.min_stars)        chips.push({ label: `≥${f.min_stars}★`,    key: "stars" });
  if (f.min_reviews)      chips.push({ label: `≥${f.min_reviews} reviews`, key: "rev" });
  if (f.min_recommend)    chips.push({ label: `≥${f.min_recommend}% rec`, key: "rec" });
  if (f.min_price)        chips.push({ label: `≥${parseInt(f.min_price).toLocaleString()} Kč`, key: "minprice" });
  if (f.max_price)        chips.push({ label: `≤${parseInt(f.max_price).toLocaleString()} Kč`, key: "price" });
  if (f.keyword)          chips.push({ label: `#${f.keyword}`,       key: "kw" });
  if (f.brand)            chips.push({ label: `🏷️ ${f.brand}`,        key: "brand" });
  if (f.max_return)       chips.push({ label: `≤${parseFloat(f.max_return).toFixed(1)}% return`, key: "ret" });
  if (f.has_image==="1")  chips.push({ label: "📷 photos only",      key: "img" });
  if (f.has_history==="1") chips.push({ label: "📈 has history",    key: "hist" });
  if (f.price_drop==="1")  chips.push({ label: "💸 price drops",    key: "drop" });
  if (f.avoid==="1")      chips.push({ label: "⚠️ avoid list",       key: "avoid" });
  if (f.sort && f.sort !== "cat_rank") {
    const sortLabel = document.querySelector(`#sort-by option[value="${escHtml(f.sort + (f.order === "desc" ? "_desc" : ""))}"]`);
    if (sortLabel) chips.push({ label: `Sort: ${sortLabel.textContent.trim()}`, key: "sort" });
  }

  if (!chips.length) { bar.style.display = "none"; return; }

  bar.style.display = "flex";
  bar.innerHTML = `<span class="af-label">Filters:</span>` +
    chips.map(c =>
      `<span class="af-chip" data-key="${escHtml(c.key)}">${escHtml(c.label)} <span class="af-x">✕</span></span>`
    ).join("") +
    `<button id="active-filters-clear" title="Clear all filters">Clear all</button>`;

  bar.querySelectorAll(".af-chip").forEach(chip => {
    chip.addEventListener("click", () => {
      const key = chip.dataset.key;
      if (key === "q")      { document.getElementById("search-input").value = ""; document.getElementById("search-clear").style.display = "none"; }
      if (key === "mc")     { document.getElementById("filter-main-category").value = ""; populateSubcategories(""); const cr=document.getElementById("cat-pills-row"); if(cr) cr.querySelectorAll(".cat-pill").forEach(b=>b.classList.remove("cat-pill-active")); renderSubCatPills(""); }
      if (key === "cat")    { document.getElementById("filter-category").value = ""; }
      if (key === "src")    { document.getElementById("filter-source").value = ""; }
      if (key === "stars")  { document.getElementById("filter-stars").value = 0; document.getElementById("stars-val").textContent = "Any"; document.querySelectorAll(".star-btn[data-val]").forEach(b=>b.classList.remove("active")); }
      if (key === "rev")    { document.getElementById("filter-reviews").value = 0; document.getElementById("reviews-val").textContent = "Any"; }
      if (key === "rec")    { document.getElementById("filter-recommend").value = 0; document.getElementById("recommend-val").textContent = "Any"; }
      if (key === "minprice"){ const p=document.getElementById("filter-min-price"); if(p) p.value=""; document.querySelectorAll(".star-btn[data-price-preset]").forEach(b=>b.classList.remove("active")); }
      if (key === "price")  { const p=document.getElementById("filter-max-price"); if(p) p.value=""; document.querySelectorAll(".star-btn[data-price-preset]").forEach(b=>b.classList.remove("active")); }
      if (key === "kw")     { activeKeyword = ""; document.querySelectorAll(".kw-pill").forEach(b=>b.classList.remove("active")); }
      if (key === "brand")  { activeBrand = ""; const bi=document.getElementById("filter-brand"); if(bi) bi.value=""; }
      if (key === "ret")    { const r=document.getElementById("filter-return"); if(r){r.value=1.4; document.getElementById("return-val").textContent="1.4%"; document.querySelectorAll(".star-btn[data-return]").forEach(b=>b.classList.remove("active"));} }
      if (key === "img")    { photosMode = false; const pt=document.getElementById("photos-toggle"); if(pt){pt.dataset.active="0";pt.textContent=_withImagesCount>0?`📷 Products with photos (${_withImagesCount.toLocaleString()})`:"Show only products with photos";pt.classList.remove("avoid-btn-active");} }
      if (key === "hist")   { const ht=document.getElementById("history-toggle"); if(ht){ht.dataset.active="0";ht.textContent=_withHistoryCount>0?`📈 Price history (${_withHistoryCount.toLocaleString()})`:"Show only products with history";ht.classList.remove("avoid-btn-active");} }
      if (key === "drop")   { const dt=document.getElementById("price-drop-toggle"); if(dt){dt.dataset.active="0"; const dc=[...snapshotDeltaMap.values()].filter(v=>v[2]!=null&&v[2]<0).length; dt.textContent=dc>0?`💸 Price drops (${dc.toLocaleString()})`:"💸 Show only price drops"; dt.classList.remove("avoid-btn-active");} }
      if (key === "avoid")  { avoidMode = false; const at=document.getElementById("avoid-toggle"); if(at){at.dataset.active="0";at.textContent="Show products to avoid";at.classList.remove("avoid-btn-active");} }
      if (key === "sort")   { document.getElementById("sort-by").value = "cat_rank"; }
      currentPage = 1;
      triggerSearch();
    });
  });

  const clearBtn = document.getElementById("active-filters-clear");
  if (clearBtn) clearBtn.addEventListener("click", () => document.getElementById("reset-filters").click());
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
function openModal(productOrJson, cardId) {
  // Accept either a plain object (from event delegation) or a JSON string (legacy)
  const p = (typeof productOrJson === "string") ? JSON.parse(productOrJson) : productOrJson;
  const overlay = document.getElementById("modal-overlay");
  const content = document.getElementById("modal-content");
  currentModalCardId = cardId ?? null;

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

  // ── Warentest-specific modal block ────────────────────────────────────────
  let wtModalBlock = "";
  if (false) { // warentest hidden
    const subs = dj.sub_ratings || {};
    const overall = subs.overall || {};
    const grade = overall.grade;
    const info = wtGradeInfo(grade);
    const testDateFmt = wtFormatDate(p.test_date);
    const brandName = p.brand || (dj._brand) || null;

    // Overall grade hero
    const gradeHero = grade !== undefined ? `
      <div class="wt-modal-grade-hero">
        <div class="wt-modal-grade-circle ${info ? info.cls : ''}">
          <span class="wt-modal-grade-num">${grade.toFixed(1)}</span>
          <span class="wt-modal-grade-label">${info ? info.labelFull : ''}</span>
        </div>
        <div class="wt-modal-grade-meta">
          ${brandName ? `<div class="wt-meta-row">🏷️ <strong>Brand:</strong> ${escHtml(brandName)}</div>` : ""}
          ${testDateFmt ? `<div class="wt-meta-row">📅 <strong>Tested:</strong> ${testDateFmt}</div>` : ""}
          ${priceStr(p) ? `<div class="wt-meta-row">💶 <strong>Price at test:</strong> ${priceStr(p)}</div>` : ""}
          <div class="wt-meta-row">🏛️ <strong>Source:</strong> Stiftung Warentest</div>
        </div>
      </div>` : `
      <div class="wt-modal-grade-meta" style="padding:10px 0 4px">
        ${brandName ? `<div class="wt-meta-row">🏷️ <strong>Brand:</strong> ${escHtml(brandName)}</div>` : ""}
        ${testDateFmt ? `<div class="wt-meta-row">📅 <strong>Tested:</strong> ${testDateFmt}</div>` : ""}
        ${priceStr(p) ? `<div class="wt-meta-row">💶 <strong>Price at test:</strong> ${priceStr(p)}</div>` : ""}
        <div class="wt-meta-row">🏛️ <strong>Source:</strong> Stiftung Warentest</div>
      </div>`;

    // Sub-rating bars (all available, not capped)
    const subBars = wtModalSubRatingBars(subs);

    // Verdict / test summary from details_json
    const verdict = dj.test_program || dj.verdict || dj.summary || null;
    const verdictHtml = verdict ? `
      <div class="wt-modal-verdict">
        <div class="wt-modal-section-title">📋 Test Verdict</div>
        <div class="wt-modal-verdict-text">${escHtml(verdict)}</div>
      </div>` : "";

    wtModalBlock = `
      <div class="wt-modal-block">
        <div class="wt-modal-section-title">Stiftung Warentest Result</div>
        ${gradeHero}
        ${subBars ? `<div class="wt-modal-section-title" style="margin-top:14px">Sub-ratings</div>${subBars}` : ""}
        ${verdictHtml}
      </div>`;
  }

  // Product image banner at top of modal (same CDN as card thumbnail)
  const modalImgHtml = (p.image_url && p.image_url !== "__none__")
    ? `<div class="modal-img-wrap">
         <img class="modal-product-img" src="${escHtml(p.image_url)}"
              alt="${escHtml(p.Name || '')}"
              onload="this.classList.add('img-loaded');this.closest('.modal-img-wrap').style.animation='none';this.closest('.modal-img-wrap').style.background='#f5f5f5'"
              onerror="this.closest('.modal-img-wrap').style.display='none'">
       </div>`
    : "";

  // Build freshness indicator
  const freshnessHtml = (() => {
    if (!p.scraped_at) return "";
    const d = new Date(p.scraped_at.replace(" ", "T"));
    if (isNaN(d.getTime())) return "";
    const days = Math.floor((Date.now() - d.getTime()) / 86400000);
    const label = days === 0 ? "updated today"
      : days === 1 ? "updated yesterday"
      : days < 30 ? `updated ${days}d ago`
      : days < 365 ? `updated ${Math.round(days/30)}mo ago`
      : `updated ${Math.round(days/365)}y ago`;
    const cls = days <= 3 ? "fresh-new" : days <= 14 ? "fresh-ok" : "fresh-old";
    return `<span class="modal-freshness ${cls}" title="Data last scraped on ${p.scraped_at}">🕒 ${label}</span>`;
  })();

  content.innerHTML = `
    ${modalImgHtml}
    <div class="modal-category">${escHtml(translateCat(p.Category || ""))}</div>
    <div class="modal-name">${escHtml(p.Name || "Unnamed")}${freshnessHtml ? ` ${freshnessHtml}` : ""}</div>

    ${wtModalBlock}

    ${p.source !== "warentest" ? `
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
    </div>` : ""}

    ${p.qt_brand_score != null ? (() => {
      const score = parseFloat(p.qt_brand_score);
      if (isNaN(score)) return '';
      const cls   = score >= 75 ? 'expert-good' : score >= 65 ? 'expert-warn' : 'expert-bad';
      const brand = p.brand ? escHtml(p.brand) : '';
      const cat   = escHtml(p.NormalizedCategory || '');
      return `
      <div class="modal-expert-block">
        <div class="modal-expert-label">🔬 Independent Expert Test Score</div>
        <div class="modal-expert-row">
          <div class="modal-expert-score ${cls}">${score.toFixed(1)}<span class="modal-expert-max"> / 100</span></div>
          <div class="modal-expert-meta">
            ${brand ? `<div class="modal-expert-brand">${brand} brand average · ${cat}</div>` : ''}
            <div class="modal-expert-note">Average score across independent consumer-organisation tests</div>
            <a class="modal-expert-link" href="https://institutkvality.cz/hodnoceni" target="_blank" rel="noopener noreferrer">Full brand rankings at institutkvality.cz →</a>
          </div>
        </div>
      </div>`;
    })() : ""}

    ${getIR(p) ? (() => {
      const ir = getIR(p);
      const score = parseFloat(ir.s);
      const cls = score >= 7 ? 'ir-good' : score >= 4 ? 'ir-mid' : 'ir-bad';
      const labels = { C1: "Documentation", C2: "Disassembly", C3: "Spare parts", C4: "Parts price ratio", C5: "Manufacturer support" };
      let subHtml = "";
      if (ir.sub) { try { const sub = JSON.parse(ir.sub); subHtml = '<div class="ir-sub-scores">' + Object.entries(sub).map(([k,v]) => '<div class="ir-sub"><span class="ir-sub-label">' + (labels[k]||k) + '</span><span class="ir-sub-val">' + parseFloat(v).toFixed(2) + '</span></div>').join("") + '</div>'; } catch(e) {} }
      return `<div class="ir-modal-block"><div class="ir-modal-label">🔧 Indice de Réparabilité <span class="ir-modal-sub">(French Repairability Index)</span></div><div class="ir-modal-score-row"><span class="ir-modal-score ${cls}">${score.toFixed(1)} / 10</span>${ir.d ? '<span class="ir-modal-date">Updated ' + ir.d + '</span>' : ''}</div>${subHtml}</div>`;
    })() : ""}

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

    <div id="modal-also-at" class="modal-also-at" style="display:none"></div>

    <div class="modal-actions">
      ${p.ProductURL ? `<a class="btn-primary" href="${escHtml(p.ProductURL)}" target="_blank">View on ${SOURCE_LABELS[p.source] || "Shop"} →</a>` : ""}
      ${p.id != null ? `<button class="btn-share" id="modal-share-btn" data-pid="${escHtml(String(p.id))}" title="Copy link to this product">🔗 Share</button>` : ""}
      <button class="btn-secondary" onclick="closeModal()">Close</button>
    </div>
    ${cardDataMap.size > 1 ? `<div class="modal-nav-hint">← → navigate · <kbd>Esc</kbd> close</div>` : ""}`;

  overlay.classList.add("open");

  // Push URL state so the product is deep-linkable and browser back closes the modal
  if (p.id != null) {
    const u = new URL(location.href);
    u.hash = "";
    u.searchParams.set("p", p.id);
    history.pushState({ modalPid: p.id }, "", u.toString());
  }

  // Async: load sparkline history after modal renders
  if (p.ProductURL) {
    const histSection = document.getElementById("modal-history");
    if (histSection) histSection.style.display = "none"; // reset
    loadModalHistory(p.ProductURL, p.currency);
  }

  // Async: load "also available at" cross-market matches
  if (p.Name && p.source) {
    const alsoAt = document.getElementById("modal-also-at");
    if (alsoAt) {
      alsoAt.style.display = "none";
      loadAlsoAt(p.Name, p.source, alsoAt);
    }
  }

  // Wire share button — now URL already contains ?p=ID, just copy it
  const shareBtn = document.getElementById("modal-share-btn");
  if (shareBtn) {
    shareBtn.onclick = () => {
      navigator.clipboard.writeText(location.href)
        .then(() => { shareBtn.textContent = "✓ Copied!"; setTimeout(() => { shareBtn.textContent = "🔗 Share"; }, 2000); })
        .catch(() => { shareBtn.textContent = "🔗 Share"; });
    };
  }
}

function closeModal() {
  document.getElementById("modal-overlay").classList.remove("open");
  currentModalCardId = null;
  // Hide history section for next open
  const histSection = document.getElementById("modal-history");
  if (histSection) histSection.style.display = "none";
  // Remove ?p= from URL when modal closes (go back without adding to history)
  const u = new URL(location.href);
  if (u.searchParams.has("p")) {
    u.searchParams.delete("p");
    history.replaceState(null, "", u.toString() || u.pathname);
  }
}

/** Open modal and scroll to the history/sparkline section. */
function openModalHistory(productOrJson) {
  openModal(productOrJson);
  // Scroll the modal container so the history chart section is at the top.
  // scrollIntoView doesn't reliably target overflow:auto containers, so we set
  // scrollTop directly on #modal once #modal-history is visible.
  function tryScroll(attemptsLeft) {
    const h     = document.getElementById("modal-history");
    const modal = document.getElementById("modal");
    if (!h || !modal) return;
    if (h.style.display !== "none") {
      // offsetTop is relative to #modal (its offsetParent via position:relative)
      modal.scrollTop = Math.max(0, h.offsetTop - 12);
    } else if (attemptsLeft > 0) {
      setTimeout(() => tryScroll(attemptsLeft - 1), 100);
    }
  }
  // Start quickly — loadModalHistory sets display="" synchronously before the fetch
  setTimeout(() => tryScroll(8), 60);
}

/** Build tooltip text for 📈 history badge (called lazily on first hover). */
function buildHistoryTitle(url, currency) {
  const d = snapshotDeltaMap.get(url);
  if (!d) return "Has price/rating history — click to view chart";
  const [recD, starD, priceD, days] = d;
  const parts = [];
  if (recD  !== null && Math.abs(recD)  >= 0.5)  parts.push(`Rating ${recD  > 0 ? "▲ +" : "▼ "}${recD.toFixed(1)}%`);
  if (starD !== null && Math.abs(starD) >= 0.05) parts.push(`Stars ${starD  > 0 ? "▲ +" : "▼ "}${starD.toFixed(2)}`);
  if (priceD !== null) {
    const sym = _currSym(currency || "CZK");
    parts.push(`Price ${priceD > 0 ? "▲ +" : "▼ "}${Math.round(Math.abs(priceD)).toLocaleString()} ${sym}`);
  }
  const dayStr = days > 0 ? ` (${days}d)` : "";
  if (parts.length) return `${parts.join(" · ")}${dayStr} — click for full chart`;
  return `Tracked ${days > 0 ? days + " days" : "recently"} — all metrics stable — click to view chart`;
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
// Fetch the set of product URLs that have ≥2 snapshots for the 📈 badge.
// Cached in localStorage for 3 hours to avoid re-downloading 340KB on every visit.
async function fetchSnapshotCoverage() {
  try {
    const CACHE_KEY = "qdb_snap_coverage";
    const TTL = 3 * 3600 * 1000; // 3 hours
    const cached = (() => {
      try {
        const raw = localStorage.getItem(CACHE_KEY);
        if (!raw) return null;
        const { ts, urls } = JSON.parse(raw);
        if (Date.now() - ts < TTL) return urls;
      } catch (_) {}
      return null;
    })();
    if (cached) {
      snapshotUrlSet = new Set(cached.map(normUrl));
      return;
    }
    const res  = await fetch(`${API_BASE}/api/snapshot-coverage`);
    const data = await res.json();
    const urls = data.urls || [];
    snapshotUrlSet = new Set(urls.map(normUrl));
    try { localStorage.setItem(CACHE_KEY, JSON.stringify({ ts: Date.now(), urls })); } catch (_) {}
  } catch (e) { /* non-fatal */ }
}

// Browser back/forward: close modal when navigating away from ?p=ID state
window.addEventListener("popstate", e => {
  const overlay = document.getElementById("modal-overlay");
  if (!overlay) return;
  const pid = new URLSearchParams(location.search).get("p");
  if (!pid && overlay.classList.contains("open")) {
    // Back pressed while modal was open — close without changing URL again
    overlay.classList.remove("open");
    currentModalCardId = null;
    const histSection = document.getElementById("modal-history");
    if (histSection) histSection.style.display = "none";
  } else if (pid && !overlay.classList.contains("open")) {
    // Forward pressed to a modal URL — open it
    checkProductDeepLink();
  }
});

document.addEventListener("DOMContentLoaded", () => {
  buildSortOptions();   // populate sort dropdown before first fetch
  applyUrlFilters();    // restore any filter state from URL params (for shared links)
  fetchStats();
  document.getElementById("export-csv-btn")?.addEventListener("click", exportVisibleCSV);
  fetchCategories();    // populates subcategory dropdown (also applies _pendingCategory from URL)
  fetchKeywords();
  fetchSnapshotDeltas();
  // has_history is now served inline per product — no separate coverage fetch needed
  fetchProducts();
  // Deep-link: if ?p=ID is in URL, open that product's modal after products load
  checkProductDeepLink();

  // Initialise sparkline IntersectionObserver
  _initSparklineObserver();

  // ── Grid event delegation: card click → open modal, history strip → jump to chart ──
  document.getElementById("product-grid").addEventListener("click", e => {
    // Top-row history badge click: open history modal
    const histBadge = e.target.closest(".history-badge[data-hist-card]");
    if (histBadge) {
      e.stopPropagation();
      const cid = Number(histBadge.dataset.histCard);
      const p = cardDataMap.get(cid);
      if (p) openModalHistory(p);
      return;
    }
    // History strip click: open modal and scroll straight to the chart
    const histStrip = e.target.closest(".card-history-strip[data-hist-url]");
    if (histStrip) {
      e.stopPropagation();
      const card = histStrip.closest(".product-card[data-card-id]");
      const p = card ? cardDataMap.get(Number(card.dataset.cardId)) : null;
      if (p) openModalHistory(p);
      return;
    }
    // Source badge click: filter by source
    const srcBadge = e.target.closest(".source-badge[data-filter-source]");
    if (srcBadge) {
      e.stopPropagation();
      const src = srcBadge.dataset.filterSource;
      if (src) {
        const el = document.getElementById("filter-source");
        if (el) { el.value = src; currentPage = 1; triggerSearch(); }
      }
      return;
    }
    // Category label click: filter by category
    const catEl = e.target.closest(".card-category[data-filter-cat]");
    if (catEl) {
      e.stopPropagation();
      const cat = catEl.dataset.filterCat;
      if (cat) filterByCategory(cat);
      return;
    }
    // "View →" link: let it navigate normally
    if (e.target.closest(".card-link")) return;
    // Any other click on the card → open modal
    const card = e.target.closest(".product-card[data-card-id]");
    if (card) {
      const cid = Number(card.dataset.cardId);
      const p = cardDataMap.get(cid);
      if (p) openModal(p, cid);
    }
  });

  // ── Search autocomplete ────────────────────────────────────────────────────
  const searchInput = document.getElementById("search-input");
  const suggestBox  = document.getElementById("search-suggestions");
  let suggestTimer  = null;
  let suggestActive = -1; // keyboard-selected index
  let lastSuggestQ  = "";

  function hideSuggestions() {
    if (suggestBox) suggestBox.style.display = "none";
    suggestActive = -1;
  }

  function showSuggestions(suggestions, q) {
    if (!suggestBox || !suggestions.length || !q) { hideSuggestions(); return; }
    const re = new RegExp(`(${q.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")})`, "gi");
    suggestBox.innerHTML = suggestions.map((item, i) => {
      // Handle both old string format and new object format
      const name = typeof item === "string" ? item : (item.name || "");
      const img  = typeof item === "object" ? (item.img || "") : "";
      const cat  = typeof item === "object" ? (item.cat || "") : "";
      const display = name.length > 68 ? name.slice(0, 65) + "…" : name;
      const highlighted = escHtml(display).replace(re, '<strong>$1</strong>');
      const thumb = img
        ? `<img class="sug-thumb" src="${escHtml(img)}" alt="" loading="lazy" onerror="this.style.display='none'">`
        : `<span class="sug-icon">🔍</span>`;
      const catLabel = cat ? `<span class="sug-hint">${escHtml(cat.length > 20 ? cat.slice(0,18)+"…" : cat)}</span>` : "";
      return `<div class="search-suggestion-item" data-idx="${i}" data-val="${escHtml(name)}">
        ${thumb}
        <span class="sug-text">${highlighted}</span>
        ${catLabel}
      </div>`;
    }).join("");
    suggestBox.style.display = "";
    suggestActive = -1;

    suggestBox.querySelectorAll(".search-suggestion-item").forEach(el => {
      el.addEventListener("mousedown", e => {
        e.preventDefault(); // prevent blur from hiding before click
        searchInput.value = el.dataset.val;
        document.getElementById("search-clear").style.display = "block";
        hideSuggestions();
        triggerSearch();
      });
    });
  }

  async function fetchSuggestions(q) {
    if (!q || q.length < 2) { hideSuggestions(); return; }
    if (q === lastSuggestQ) return;
    lastSuggestQ = q;
    try {
      const res = await fetch(`${API_BASE}/api/search-suggest?q=${encodeURIComponent(q)}`);
      const d = await res.json();
      if (searchInput.value === q) showSuggestions(d.suggestions || [], q);
    } catch(e) { /* non-fatal */ }
  }

  // Search input events
  searchInput.addEventListener("input", function() {
    document.getElementById("search-clear").style.display = this.value ? "block" : "none";
    debouncedSearch();
    clearTimeout(suggestTimer);
    if (this.value.trim().length >= 2) {
      suggestTimer = setTimeout(() => fetchSuggestions(this.value.trim()), 200);
    } else {
      hideSuggestions();
    }
  });

  searchInput.addEventListener("keydown", e => {
    const items = suggestBox ? suggestBox.querySelectorAll(".search-suggestion-item") : [];
    if (!items.length) return;
    if (e.key === "ArrowDown") {
      e.preventDefault();
      suggestActive = Math.min(suggestActive + 1, items.length - 1);
      items.forEach((el, i) => el.classList.toggle("active", i === suggestActive));
    } else if (e.key === "ArrowUp") {
      e.preventDefault();
      suggestActive = Math.max(suggestActive - 1, -1);
      items.forEach((el, i) => el.classList.toggle("active", i === suggestActive));
    } else if (e.key === "Enter" && suggestActive >= 0) {
      e.stopPropagation();
      const sel = items[suggestActive];
      if (sel) { searchInput.value = sel.dataset.val; hideSuggestions(); triggerSearch(); }
    } else if (e.key === "Escape") {
      hideSuggestions();
    }
  });

  searchInput.addEventListener("blur", () => setTimeout(hideSuggestions, 150));
  searchInput.addEventListener("focus", () => {
    if (searchInput.value.trim().length >= 2) fetchSuggestions(searchInput.value.trim());
  });

  document.getElementById("search-clear").addEventListener("click", () => {
    searchInput.value = "";
    document.getElementById("search-clear").style.display = "none";
    hideSuggestions();
    triggerSearch();
  });

  // Keyboard shortcut: press "/" to focus search (unless already in an input)
  document.addEventListener("keydown", e => {
    if (e.key === "/" && !["INPUT", "TEXTAREA", "SELECT"].includes(document.activeElement.tagName)) {
      e.preventDefault();
      searchInput.focus();
      searchInput.select();
    }
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
    document.querySelectorAll(".star-btn[data-reviews]").forEach(b => b.classList.remove("active"));
    debouncedSearch();
  });

  const recommendSlider = document.getElementById("filter-recommend");
  recommendSlider.addEventListener("input", () => {
    const v = parseInt(recommendSlider.value);
    document.getElementById("recommend-val").textContent = v > 0 ? v + "%+" : "Any";
    document.querySelectorAll(".star-btn[data-rec]").forEach(b => b.classList.remove("active"));
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

  // Quick reviews buttons
  document.querySelectorAll(".star-btn[data-reviews]").forEach(btn => {
    btn.addEventListener("click", () => {
      const v = parseInt(btn.dataset.reviews);
      reviewsSlider.value = Math.min(v, parseInt(reviewsSlider.max));
      document.getElementById("reviews-val").textContent = v + "+";
      document.querySelectorAll(".star-btn[data-reviews]").forEach(b => b.classList.remove("active"));
      btn.classList.add("active");
      currentPage = 1;
      triggerSearch();
    });
  });

  // Quick recommend rate buttons
  document.querySelectorAll(".star-btn[data-rec]").forEach(btn => {
    btn.addEventListener("click", () => {
      const v = parseInt(btn.dataset.rec);
      recommendSlider.value = v;
      document.getElementById("recommend-val").textContent = v + "%+";
      document.querySelectorAll(".star-btn[data-rec]").forEach(b => b.classList.remove("active"));
      btn.classList.add("active");
      currentPage = 1;
      triggerSearch();
    });
  });

  // Price range — number inputs
  const minPriceInput = document.getElementById("filter-min-price");
  const maxPriceInput = document.getElementById("filter-max-price");
  if (minPriceInput) minPriceInput.addEventListener("input", () => { document.querySelectorAll(".star-btn[data-price-preset]").forEach(b=>b.classList.remove("active")); debouncedSearch(); });
  if (maxPriceInput) maxPriceInput.addEventListener("input", () => { document.querySelectorAll(".star-btn[data-price-preset]").forEach(b=>b.classList.remove("active")); debouncedSearch(); });

  // Price preset buttons (data-price-preset="minVal,maxVal")
  document.querySelectorAll(".star-btn[data-price-preset]").forEach(btn => {
    btn.addEventListener("click", () => {
      const [mn, mx] = btn.dataset.pricePreset.split(",").map(Number);
      if (minPriceInput) minPriceInput.value = mn > 0 ? mn : "";
      if (maxPriceInput) maxPriceInput.value = mx > 0 ? mx : "";
      document.querySelectorAll(".star-btn[data-price-preset]").forEach(b => b.classList.remove("active"));
      btn.classList.add("active");
      currentPage = 1;
      triggerSearch();
    });
  });

  // Brand text input — syncs with activeBrand; triggers search on Enter
  const brandInput = document.getElementById("filter-brand");
  if (brandInput) {
    // Keep input in sync when brand is set via leaderboard click or URL param
    const _syncBrandInput = () => { brandInput.value = activeBrand; };
    // Poll-free sync: we call _syncBrandInput whenever we set activeBrand elsewhere
    // (leaderboard click patches activeBrand then calls fetchProducts; restore input here)
    const _origFetch = fetchProducts;
    fetchProducts = function() {
      if (brandInput) brandInput.value = activeBrand;
      return _origFetch.apply(this, arguments);
    };

    brandInput.addEventListener("keydown", e => {
      if (e.key === "Enter") {
        activeBrand = brandInput.value.trim();
        currentPage = 1;
        triggerSearch();
      }
      if (e.key === "Escape") {
        activeBrand = "";
        brandInput.value = "";
        currentPage = 1;
        triggerSearch();
      }
    });
    // Trigger immediately when datalist item is selected (click or keyboard pick)
    brandInput.addEventListener("change", () => {
      const v = brandInput.value.trim();
      if (v !== activeBrand) {
        activeBrand = v;
        currentPage = 1;
        triggerSearch();
      }
    });
    // Also update on blur (so tabbing away applies the filter)
    brandInput.addEventListener("blur", () => {
      const v = brandInput.value.trim();
      if (v !== activeBrand) {
        activeBrand = v;
        currentPage = 1;
        triggerSearch();
      }
    });

    // Populate datalist with known brands on first focus (lazy)
    let _brandListLoaded = false;
    brandInput.addEventListener("focus", () => {
      if (_brandListLoaded) return;
      _brandListLoaded = true;
      // Immediately seed from _BRAND_DOMAINS (available at load time)
      const dl = document.getElementById("brand-autocomplete");
      if (!dl) return;
      Object.keys(_BRAND_DOMAINS).sort().forEach(b => {
        const opt = document.createElement("option"); opt.value = b; dl.appendChild(opt);
      });
      // Then supplement with API brands (top brands by product count)
      fetch(`${API_BASE}/api/brands?min_products=3`)
        .then(r => r.ok ? r.json() : {brands:[]})
        .then(d => {
          const existing = new Set(Array.from(dl.options).map(o => o.value.toLowerCase()));
          (d.brands || []).forEach(b => {
            if (!existing.has(b.brand.toLowerCase())) {
              const opt = document.createElement("option"); opt.value = b.brand; dl.appendChild(opt);
            }
          });
        })
        .catch(() => {});
    });
  }

  // Photos-only toggle
  const photosToggle = document.getElementById("photos-toggle");
  if (photosToggle) {
    photosToggle.addEventListener("click", () => {
      photosMode = !photosMode;
      photosToggle.dataset.active = photosMode ? "1" : "0";
      photosToggle.textContent = photosMode ? "📷 Showing only products with photos" : "Show only products with photos";
      photosToggle.classList.toggle("avoid-btn-active", photosMode);
      currentPage = 1;
      triggerSearch();
    });
  }

  // Has price history toggle
  const historyToggle = document.getElementById("history-toggle");
  if (historyToggle) {
    historyToggle.addEventListener("click", () => {
      const active = historyToggle.dataset.active !== "1";
      historyToggle.dataset.active = active ? "1" : "0";
      historyToggle.textContent = active ? "📈 Showing only products with history" : "Show only products with history";
      historyToggle.classList.toggle("avoid-btn-active", active);
      currentPage = 1;
      triggerSearch();
    });
  }

  // Price drop filter toggle
  const priceDropToggle = document.getElementById("price-drop-toggle");
  if (priceDropToggle) {
    priceDropToggle.addEventListener("click", () => {
      const active = priceDropToggle.dataset.active !== "1";
      priceDropToggle.dataset.active = active ? "1" : "0";
      priceDropToggle.textContent = active ? "💸 Showing only price drops" : "💸 Show only price drops";
      priceDropToggle.classList.toggle("avoid-btn-active", active);
      currentPage = 1;
      triggerSearch();
    });
  }

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
  // Share/copy link button
  const shareBtn = document.getElementById("share-filters");
  if (shareBtn) {
    shareBtn.addEventListener("click", () => {
      pushFilterState(); // ensure URL is current
      navigator.clipboard.writeText(window.location.href).then(() => {
        const orig = shareBtn.textContent;
        shareBtn.textContent = "✓";
        shareBtn.title = "Copied!";
        setTimeout(() => { shareBtn.textContent = orig; shareBtn.title = "Copy link to current filters"; }, 1500);
      }).catch(() => {
        // Fallback: show the URL in a prompt
        window.prompt("Copy this link:", window.location.href);
      });
    });
  }

  document.getElementById("reset-filters").addEventListener("click", () => {
    document.getElementById("search-input").value = "";
    document.getElementById("search-clear").style.display = "none";
    document.getElementById("filter-main-category").value = "";
    populateSubcategories("");   // hides sub-dropdown and clears it
    document.getElementById("filter-source").value = "";
    activeKeyword = "";
    activeBrand   = "";
    const _bi = document.getElementById("filter-brand"); if (_bi) _bi.value = "";
    document.querySelectorAll(".kw-pill").forEach(b => b.classList.remove("active"));
    if (kwClearBtn) kwClearBtn.style.display = "none";
    // Reset photos mode
    photosMode = false;
    if (photosToggle) {
      photosToggle.dataset.active = "0";
      photosToggle.textContent = _withImagesCount > 0
        ? `📷 Products with photos (${_withImagesCount.toLocaleString()})`
        : "Show only products with photos";
      photosToggle.classList.remove("avoid-btn-active");
    }
    // Reset history toggle — restore count label if known
    if (historyToggle) {
      historyToggle.dataset.active = "0";
      historyToggle.textContent = _withHistoryCount > 0
        ? `📈 Price history (${_withHistoryCount.toLocaleString()})`
        : "Show only products with history";
      historyToggle.classList.remove("avoid-btn-active");
    }
    // Reset price drop toggle — restore count label if known
    if (priceDropToggle) {
      priceDropToggle.dataset.active = "0";
      const dropCount = [...snapshotDeltaMap.values()].filter(v => v[2] != null && v[2] < 0).length;
      priceDropToggle.textContent = dropCount > 0 ? `💸 Price drops (${dropCount.toLocaleString()})` : "💸 Show only price drops";
      priceDropToggle.classList.remove("avoid-btn-active");
    }
    // Reset avoid mode
    avoidMode = false;
    if (avoidToggle) { avoidToggle.dataset.active = "0"; avoidToggle.textContent = "Show products to avoid"; avoidToggle.classList.remove("avoid-btn-active"); }
    if (avoidInfo) avoidInfo.style.display = "none";
    document.getElementById("sort-by").value = "cat_rank";  // restore default
    // Restore recommend filter (hidden when amazon_us was selected)
    const recGroup = document.getElementById("recommend-filter-group");
    if (recGroup) recGroup.style.display = "";
    starSlider.value = 0; document.getElementById("stars-val").textContent = "Any";
    returnSlider.value = 1.4; document.getElementById("return-val").textContent = "1.4%";
    reviewsSlider.value = 0; document.getElementById("reviews-val").textContent = "Any";
    recommendSlider.value = 0; document.getElementById("recommend-val").textContent = "Any";
    // Reset price range inputs
    if (minPriceInput) minPriceInput.value = "";
    if (maxPriceInput) maxPriceInput.value = "";
    document.querySelectorAll(".star-btn").forEach(b => b.classList.remove("active"));
    // Deactivate category quick-pills
    const cpRow = document.getElementById("cat-pills-row");
    if (cpRow) cpRow.querySelectorAll(".cat-pill").forEach(b => b.classList.remove("cat-pill-active"));
    const spRow = document.getElementById("sub-pills-row");
    if (spRow) { spRow.style.display = "none"; spRow.innerHTML = ""; }
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
    if (e.key === "Escape") { closeModal(); closeSidebar(); return; }
    // ← → to navigate between products when modal is open
    const overlayOpen = document.getElementById("modal-overlay")?.classList.contains("open");
    if (!overlayOpen) return;
    if (e.target.tagName === "INPUT" || e.target.tagName === "TEXTAREA") return;
    if (e.key === "ArrowRight") { e.preventDefault(); navigateModal(1); }
    if (e.key === "ArrowLeft")  { e.preventDefault(); navigateModal(-1); }
  });

  /** Navigate forward/backward through visible cards while modal is open. */
  window.navigateModal = function navigateModal(dir) {
    const cardIds = [...cardDataMap.keys()];
    if (!cardIds.length) return;
    let idx = currentModalCardId != null ? cardIds.indexOf(currentModalCardId) : -1;
    idx = Math.max(0, Math.min(cardIds.length - 1, idx + dir));
    const nextId = cardIds[idx];
    const p = cardDataMap.get(nextId);
    if (p) openModal(p, nextId);
  }

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
    // Sync category pill active state
    const row = document.getElementById("cat-pills-row");
    if (row) row.querySelectorAll(".cat-pill").forEach(b => b.classList.toggle("cat-pill-active", b.dataset.cat === this.value));
    // Sync sub-category pills
    renderSubCatPills(this.value);
    triggerSearchAndClose();
  });

  document.getElementById("filter-source").addEventListener("change", () => {
    const src = document.getElementById("filter-source").value;
    const isAmazonUS = src === "amazon_us";
    const isWarentest = src === "warentest";

    // Auto-switch sort to the most useful default for each source
    const sortSel = document.getElementById("sort-by");
    const wtSorts    = new Set(["AvgStarRating_desc","AvgStarRating"]);
    const nonWtSorts = new Set(["RecommendRate_pct_desc","RecommendRate_pct"]);
    if (isWarentest && nonWtSorts.has(sortSel.value)) {
      sortSel.value = "AvgStarRating_desc";
    } else if (isAmazonUS && sortSel.value === "RecommendRate_pct_desc") {
      sortSel.value = "ReviewsCount_desc";
    } else if (!isWarentest && !isAmazonUS && (wtSorts.has(sortSel.value) || sortSel.value === "ReviewsCount_desc")) {
      sortSel.value = "RecommendRate_pct_desc";
    }

    // Hide recommend rate filter for amazon_us (all products have NULL there)
    const recGroup = document.getElementById("recommend-filter-group");
    if (recGroup) recGroup.style.display = isAmazonUS ? "none" : "";

    // Return rate filter is only meaningful for Alza (only source with that data)
    const returnGroup = document.getElementById("return-rate-group");
    if (returnGroup) returnGroup.style.display = (src === "alza") ? "" : "none";

    // Re-fetch categories in the correct country when source changes
    document.getElementById("filter-main-category").value = "";
    populateSubcategories("");
    fetchCategories();
    triggerSearchAndClose();
  });
  document.getElementById("filter-category").addEventListener("change", function() {
    // Sync sub-pill active state when sub-category dropdown changes
    const row = document.getElementById("sub-pills-row");
    if (row) row.querySelectorAll(".sub-pill").forEach(b => b.classList.toggle("cat-pill-active", b.dataset.sub === this.value));
    triggerSearchAndClose();
  });
  document.getElementById("sort-by").addEventListener("change", triggerSearchAndClose);
  [starSlider, returnSlider, reviewsSlider, recommendSlider].forEach(sl => {
    sl.addEventListener("change", () => { if (window.innerWidth <= 900) closeSidebar(); });
  });

  // ── Cross-market view toggle ───────────────────────────────────────────────
  const cmBtn = document.getElementById("view-cross-market");
  if (cmBtn) {
    cmBtn.addEventListener("click", () => {
      const panel       = document.getElementById("cross-market-panel");
      const grid        = document.getElementById("product-grid");
      const pgn         = document.getElementById("pagination");
      const mvPanel     = document.getElementById("movers-panel");
      const brandsPanel = document.getElementById("brands-panel");
      const isOpen = panel.style.display !== "none";
      if (isOpen) {
        // Back to normal view
        panel.style.display = "none";
        grid.style.display  = "";
        pgn.style.display   = "";
        cmBtn.classList.remove("active");
      } else {
        // Close other panels
        if (mvPanel && mvPanel.style.display !== "none") {
          mvPanel.style.display = "none";
          document.getElementById("view-movers")?.classList.remove("active");
        }
        if (brandsPanel && brandsPanel.style.display !== "none") {
          brandsPanel.style.display = "none";
          document.getElementById("view-brands")?.classList.remove("active");
        }
        panel.style.display = "";
        grid.style.display  = "none";
        pgn.style.display   = "none";
        const _cmLmb = document.getElementById("load-more-btn");
        if (_cmLmb) _cmLmb.style.display = "none";
        cmBtn.classList.add("active");
        if (!document.getElementById("cross-market-grid").dataset.loaded) {
          loadCrossMarket();
        }
      }
    });
    document.getElementById("cm-refresh-btn")?.addEventListener("click", loadCrossMarket);
    document.getElementById("cm-min-markets")?.addEventListener("change", loadCrossMarket);
  }
});

// ── Cross-market data + rendering ─────────────────────────────────────────────
// Use FLAG_MAP (defined below in health panel section) for all country codes.
// Also keep this alias for any legacy references.
const FLAG = { CZ:"🇨🇿", DE:"🇩🇪", FR:"🇫🇷", PL:"🇵🇱", SK:"🇸🇰", US:"🇺🇸", GB:"🇬🇧",
               AT:"🇦🇹", CH:"🇨🇭", NL:"🇳🇱", SE:"🇸🇪", DK:"🇩🇰" };

async function loadCrossMarket() {
  const grid   = document.getElementById("cross-market-grid");
  const status = document.getElementById("cm-status");
  const minM   = document.getElementById("cm-min-markets")?.value || "2";
  grid.dataset.loaded = "";
  grid.innerHTML = '<div style="grid-column:1/-1;padding:20px;color:#888">Loading cross-market matches…</div>';
  if (status) status.textContent = "";

  try {
    const res  = await fetch(`${API_BASE}/api/cross-market?min_markets=${minM}`);
    const data = await res.json();
    if (data.error) throw new Error(data.error);

    grid.dataset.loaded = "1";
    if (status) status.textContent = `${data.length} product groups`;

    if (!data.length) {
      grid.innerHTML = '<div style="grid-column:1/-1;padding:20px;color:#888">No cross-market matches found.</div>';
      return;
    }

    grid.innerHTML = data.map(g => {
      // One best listing per market (most reviewed variant)
      const byCountry = {};
      for (const p of g.products) {
        if (!byCountry[p.country] || (p.reviews||0) > (byCountry[p.country].reviews||0)) {
          byCountry[p.country] = p;
        }
      }
      const listings = Object.values(byCountry)
        .sort((a,b) => a.country.localeCompare(b.country));

      // Price range display (use per-listing currency where possible)
      const prices = listings.filter(p => p.price != null);
      const hasCzk  = prices.some(p => p.currency === "CZK");
      const hasEur  = prices.some(p => p.currency !== "CZK");
      let priceRangeStr = "";
      if (hasCzk) {
        const czkPrices = prices.filter(p => p.currency === "CZK");
        const lo = Math.round(Math.min(...czkPrices.map(p=>p.price))).toLocaleString();
        const hi = Math.round(Math.max(...czkPrices.map(p=>p.price))).toLocaleString();
        priceRangeStr += `CZK ${lo === hi ? lo : lo + "–" + hi}`;
      }
      if (hasEur) {
        const eurPrices = prices.filter(p => p.currency !== "CZK");
        const lo = Math.round(Math.min(...eurPrices.map(p=>p.price))).toLocaleString();
        const hi = Math.round(Math.max(...eurPrices.map(p=>p.price))).toLocaleString();
        const curr = eurPrices[0].currency || "EUR";
        priceRangeStr += (priceRangeStr ? " · " : "") + `${curr} ${lo === hi ? lo : lo + "–" + hi}`;
      }

      const rates = listings.filter(p => p.rate != null).map(p => p.rate);
      const avgRate = rates.length ? Math.round(rates.reduce((a,b)=>a+b,0)/rates.length) : null;

      // Market flag chips for header
      const flagChips = listings.map(p =>
        `<span class="cm-flag-chip" title="${escHtml(p.source)}">${FLAG[p.country] || p.country}</span>`
      ).join("");

      // Brand logo
      const brandStr = g.brand.replace(/[A-Z]{1}[a-z]/g, s => " "+s).trim() || g.token;
      const _bLogoUrl = brandLogoUrl(brandStr);
      const bLogo = _bLogoUrl
        ? `<img class="cm-brand-logo" src="${escHtml(_bLogoUrl)}" alt="${escHtml(brandStr)}" onerror="this.style.display='none'">`
        : "";

      // Product rows
      const rows = listings.map(p => {
        const flag   = FLAG[p.country] || p.country;
        const name   = escHtml(p.name);
        const rate   = p.rate != null ? `<span class="cm-rate-val">${Math.round(p.rate)}%</span>` : "";
        const revN = p.reviews ? Math.round(p.reviews) : 0;
        const reviews = revN ? `<span class="cm-reviews-val">${revN >= 1000 ? (revN/1000).toFixed(1).replace('.0','') + 'k' : revN} rev</span>` : "";
        const price  = p.price != null
          ? `<span class="cm-price-val">${Math.round(p.price).toLocaleString()} ${p.currency || "CZK"}</span>`
          : "";
        const srcLabel = SOURCE_LABELS[p.source] || p.source;
        const link   = p.url
          ? `<a class="cm-product-link" href="${escHtml(p.url)}" target="_blank" title="${escHtml(p.name)}">→</a>`
          : "";
        return `<li class="cm-product-row">
          <span class="cm-product-flag">${flag}</span>
          <span class="cm-product-name" title="${escHtml(p.name)}">${name}</span>
          <span class="cm-product-stats">${rate}${reviews ? " · " + reviews : ""}${price ? " · " + price : ""}</span>
          <span class="cm-product-source">${escHtml(srcLabel)}</span>
          ${link}
        </li>`;
      }).join("");

      return `<div class="cm-card">
        <div class="cm-card-header">
          ${bLogo}
          <div class="cm-card-title">
            <span class="cm-token">${escHtml(g.token)}</span>
            <div class="cm-flag-row">${flagChips}</div>
          </div>
          <div class="cm-card-meta">
            ${avgRate != null ? `<span class="cm-avg-rate">${avgRate}%</span>` : ""}
            ${priceRangeStr ? `<span class="cm-price-range">💰 ${priceRangeStr}</span>` : ""}
          </div>
        </div>
        <ul class="cm-products">${rows}</ul>
      </div>`;
    }).join("");

  } catch (e) {
    grid.innerHTML = `<div style="grid-column:1/-1;padding:20px;color:#c62828">Error: ${escHtml(e.message)}</div>`;
    if (status) status.textContent = "Failed";
  }
}

// ── Data Health Panel ─────────────────────────────────────────────────────────

const FLAG_MAP = { CZ:"🇨🇿", SK:"🇸🇰", DE:"🇩🇪", AT:"🇦🇹", CH:"🇨🇭", FR:"🇫🇷", PL:"🇵🇱", NL:"🇳🇱", SE:"🇸🇪", DK:"🇩🇰", US:"🇺🇸", "??":"🌐" };

function initHealthPanel() {
  const btn       = document.getElementById("health-btn");
  const panel     = document.getElementById("health-panel");
  const backdrop  = document.getElementById("health-backdrop");
  const closeBtn  = document.getElementById("health-close");
  const indicator = document.getElementById("health-indicator");
  if (!btn || !panel) return;

  let loaded = false;

  function openPanel() {
    panel.style.display = "";
    backdrop.classList.add("open");
    document.body.style.overflow = "hidden";
    if (!loaded) { loaded = true; loadHealth(); }
  }
  function closePanel() {
    panel.style.display = "none";
    backdrop.classList.remove("open");
    document.body.style.overflow = "";
  }

  btn.addEventListener("click", openPanel);
  closeBtn.addEventListener("click", closePanel);
  backdrop.addEventListener("click", closePanel);

  // Quietly fetch health on page load to set the header indicator dot color
  fetch(`${API_BASE}/api/health`)
    .then(r => r.json())
    .then(data => {
      if (!data.sources) return;
      const hasSstale = data.sources.some(s => s.status === "stale");
      const hasWarn   = data.sources.some(s => s.status === "warn");
      if (hasSstale)      indicator.style.color = "#e53935";
      else if (hasWarn)   indicator.style.color = "#ffa000";
      else                indicator.style.color = "#43a047";
    })
    .catch(() => {});
}

function loadHealth() {
  const rowsEl   = document.getElementById("health-rows");
  const summEl   = document.getElementById("health-summary");
  if (!rowsEl) return;
  rowsEl.textContent = "Loading…";

  fetch(`${API_BASE}/api/health`)
    .then(r => r.json())
    .then(data => {
      if (data.error) { rowsEl.textContent = "Error: " + data.error; return; }

      const sources  = data.sources || [];
      const nOk      = sources.filter(s => s.status === "ok").length;
      const nWarn    = sources.filter(s => s.status === "warn").length;
      const nStale   = sources.filter(s => s.status === "stale").length;
      const nImport  = sources.filter(s => s.status === "imported").length;
      const total    = (data.total_products || 0).toLocaleString();

      summEl.innerHTML = `
        <span class="health-summary-pill health-pill-total">📦 ${total} products</span>
        ${nOk     ? `<span class="health-summary-pill health-pill-ok">✓ ${nOk} fresh</span>` : ""}
        ${nWarn   ? `<span class="health-summary-pill health-pill-warn">⚠ ${nWarn} ageing</span>` : ""}
        ${nStale  ? `<span class="health-summary-pill health-pill-stale">✕ ${nStale} stale</span>` : ""}
        ${nImport ? `<span class="health-summary-pill health-pill-imported">↑ ${nImport} imported</span>` : ""}
        <span class="health-summary-pill" style="background:#f5f5f5;color:#888">
          ${data.scheduler_running ? "🟢 Scheduler running" : "🔴 Scheduler stopped"}
        </span>`;

      rowsEl.innerHTML = sources.map(s => {
        const dotClass  = `health-dot-${s.status}`;
        const flag      = FLAG_MAP[s.market] || "🌐";
        const isImport  = s.status === "imported";
        const age       = s.days_since_ok != null
          ? (s.days_since_ok === 0 ? "today" : `${s.days_since_ok}d ago`)
          : (isImport ? "bulk import" : "never scraped");
        const lastOk    = s.last_ok
          ? new Date(s.last_ok).toLocaleDateString("en-GB", {day:"numeric", month:"short", year:"numeric"})
          : "—";
        const metaLabel = isImport ? "Imported:" : "Last OK:";
        const errBadge  = s.errors_30d > 0
          ? `<div class="health-row-errors">⚠ ${s.errors_30d} error${s.errors_30d > 1?"s":""} in last 30d</div>`
          : "";
        const addedStr  = s.total_added || s.total_updated
          ? `+${(s.total_added||0).toLocaleString()} added, ~${(s.total_updated||0).toLocaleString()} updated`
          : "";

        return `
          <div class="health-row">
            <span class="health-dot ${dotClass}">⬤</span>
            <div class="health-row-info">
              <div class="health-row-name">${flag} ${escHtml(s.scraper)}</div>
              <div class="health-row-meta">${metaLabel} ${lastOk} (${age})${addedStr ? " · " + addedStr : ""}</div>
              ${errBadge}
            </div>
            <div class="health-row-count">${(s.product_count||0).toLocaleString()}<br><span style="font-weight:400;color:#aaa;font-size:0.85em">products</span></div>
          </div>`;
      }).join("");
    })
    .catch(e => { rowsEl.textContent = "Failed to load: " + e.message; });
}

// Initialise health panel after DOM is ready
document.addEventListener("DOMContentLoaded", initHealthPanel);

// ── Movers panel ──────────────────────────────────────────────────────────────

function initMoversPanel() {
  const moversBtn = document.getElementById("view-movers");
  if (!moversBtn) return;

  moversBtn.addEventListener("click", () => {
    const panel       = document.getElementById("movers-panel");
    const grid        = document.getElementById("product-grid");
    const pgn         = document.getElementById("pagination");
    const cmPanel     = document.getElementById("cross-market-panel");
    const brandsPanel = document.getElementById("brands-panel");
    const isOpen = panel.style.display !== "none";

    if (isOpen) {
      panel.style.display = "none";
      grid.style.display  = "";
      pgn.style.display   = "";
      moversBtn.classList.remove("active");
    } else {
      // Close other panels
      if (cmPanel && cmPanel.style.display !== "none") {
        cmPanel.style.display = "none";
        document.getElementById("view-cross-market")?.classList.remove("active");
      }
      if (brandsPanel && brandsPanel.style.display !== "none") {
        brandsPanel.style.display = "none";
        document.getElementById("view-brands")?.classList.remove("active");
      }
      panel.style.display = "";
      grid.style.display  = "none";
      pgn.style.display   = "none";
      const _mvLmb = document.getElementById("load-more-btn");
      if (_mvLmb) _mvLmb.style.display = "none";
      moversBtn.classList.add("active");
      if (!document.getElementById("movers-grid").dataset.loaded) {
        loadMovers();
      }
    }
  });

  document.getElementById("movers-refresh-btn")?.addEventListener("click", loadMovers);
  document.getElementById("movers-metric")?.addEventListener("change", loadMovers);
  document.getElementById("movers-days")?.addEventListener("change", loadMovers);
}

async function loadMovers() {
  const grid    = document.getElementById("movers-grid");
  const status  = document.getElementById("movers-status");
  const metric  = document.getElementById("movers-metric")?.value || "recommend";
  const days    = document.getElementById("movers-days")?.value   || "24";
  if (!grid) return;

  grid.dataset.loaded = "";
  grid.innerHTML = '<div style="padding:24px;color:#888;text-align:center">Loading movers…</div>';
  if (status) status.textContent = "";

  try {
    const res  = await fetch(`${API_BASE}/api/snapshot-movers?metric=${metric}&days=${days}&limit=30`);
    const data = await res.json();
    if (data.error) throw new Error(data.error);

    grid.dataset.loaded = "1";
    const risers  = data.risers  || [];
    const fallers = data.fallers || [];

    const metricLabel = { recommend: "Recommend %", stars: "Stars", price: "Price" }[metric] || metric;

    // Infer currency symbol from source name for price formatting
    const _srcCurrency = src => {
      if (!src) return "Kč";
      const s = src.toLowerCase();
      if (s.includes("digitec")) return "CHF";   // Swiss franc
      if (s.includes("_de") || s.includes("alternate") || s.includes("coolblue") ||
          s.includes("fnac") || s.includes("darty") || s.includes("amazon_de") ||
          s.includes("_sk") || s.includes("geizhals"))
        return "€";
      if (s.includes("_uk") || s.includes("amazon_uk")) return "£";
      if (s.includes("prisjakt") || s.includes("elgiganten") || s.includes("pricerunner_se") ||
          s.includes("_se")) return "kr";
      if (s.includes("pricerunner.dk") || s.includes("pricerunner_dk")) return "kr"; // DKK
      if (s.includes("ceneo") || s.includes("_pl")) return "zł";
      return "Kč";
    };

    function fmtMoverVal(val, src, isPrice) {
      if (val == null) return "—";
      if (!isPrice) return val.toFixed(1);
      const sym = _srcCurrency(src);
      const n   = Math.round(val).toLocaleString("de-DE");
      return sym === "€" || sym === "£" ? `${sym} ${n}` : `${n} ${sym}`;
    }

    function fmtMoverDelta(delta, src, isPrice) {
      if (delta == null) return "—";
      if (!isPrice) {
        const sign = delta > 0 ? "+" : "";
        const cls  = delta > 0 ? "mover-up" : "mover-down";
        return `<span class="${cls}">${sign}${delta.toFixed(1)}</span>`;
      }
      const sym  = _srcCurrency(src);
      const sign = delta > 0 ? "+" : "";
      const cls  = delta > 0 ? "mover-up" : "mover-down";
      const abs  = Math.round(Math.abs(delta)).toLocaleString("de-DE");
      const str  = sym === "€" || sym === "£" ? `${sign}${sym} ${delta < 0 ? "-" : ""}${abs}` : `${sign}${delta < 0 ? "-" : ""}${abs} ${sym}`;
      return `<span class="${cls}">${str}</span>`;
    }

    function renderMoverTable(items, direction) {
      if (!items.length) return `<div style="padding:12px;color:#aaa;font-size:0.85em">No ${direction} data for this period.</div>`;
      const isPrice = metric === "price";
      // For price metric: sort by relative % change so EUR and CZK products are comparable
      const sorted = isPrice
        ? [...items].sort((a, b) => {
            const pa = a.old_val ? Math.abs(a.delta / a.old_val) : 0;
            const pb = b.old_val ? Math.abs(b.delta / b.old_val) : 0;
            return direction === "riser" ? pb - pa : pb - pa;
          })
        : items;
      return `<table class="movers-table">
        <thead><tr>
          <th>Product</th><th>Source</th><th>Category</th>
          <th>${metricLabel} (first)</th><th>${metricLabel} (now)</th><th>Change</th>
          ${isPrice ? "<th>%</th>" : ""}
        </tr></thead>
        <tbody>${sorted.map(r => {
          const pct    = (isPrice && r.old_val) ? (r.delta / r.old_val * 100) : null;
          const pctStr = pct != null
            ? `<span class="${pct < 0 ? "mover-down" : "mover-up"}">${pct > 0 ? "+" : ""}${pct.toFixed(1)}%</span>`
            : "";
          const delta  = fmtMoverDelta(r.delta, r.source, isPrice);
          const name   = escHtml((r.name || r.url || "").substring(0, 52));
          const cat    = escHtml(translateCat(r.category || ""));
          const oldVal = fmtMoverVal(r.old_val, r.source, isPrice);
          const newVal = fmtMoverVal(r.new_val, r.source, isPrice);
          const oldDate = r.old_date ? `<div class="mover-date">${r.old_date}</div>` : "";
          const newDate = r.new_date ? `<div class="mover-date">${r.new_date}</div>` : "";
          return `<tr>
            <td class="mover-name" title="${escHtml(r.name || '')}">
              ${r.url ? `<a href="${escHtml(r.url)}" target="_blank">${name}</a>` : name}
            </td>
            <td>${escHtml(SOURCE_LABELS[r.source] || r.source || "")}</td>
            <td>${cat}</td>
            <td class="mover-val">${oldVal}${oldDate}</td>
            <td class="mover-val">${newVal}${newDate}</td>
            <td class="mover-delta">${delta}</td>
            ${isPrice ? `<td class="mover-pct">${pctStr}</td>` : ""}
          </tr>`;
        }).join("")}</tbody>
      </table>`;
    }

    if (status) status.textContent = `${risers.length + fallers.length} movers found`;

    grid.innerHTML = `
      <div class="movers-section">
        <div class="movers-section-title mover-up-title">⬆ Biggest risers (${risers.length})</div>
        ${renderMoverTable(risers, "riser")}
      </div>
      <div class="movers-section">
        <div class="movers-section-title mover-down-title">⬇ Biggest fallers (${fallers.length})</div>
        ${renderMoverTable(fallers, "faller")}
      </div>`;

  } catch (e) {
    grid.innerHTML = `<div style="padding:20px;color:#c62828">Error loading movers: ${escHtml(e.message)}</div>`;
    if (status) status.textContent = "Failed";
  }
}

document.addEventListener("DOMContentLoaded", initMoversPanel);

// ── Product deep-link: ?p=ID opens the product's modal on page load ──────────

async function checkProductDeepLink() {
  const pid = new URLSearchParams(location.search).get("p");
  if (!pid || !/^\d+$/.test(pid)) return;
  try {
    const r = await fetch(`${API_BASE}/api/product?id=${pid}`);
    if (!r.ok) return;
    const d = await r.json();
    if (d && d.product) openModal(d.product);
  } catch (_) {}
}

// ── "Also available at" cross-market section ─────────────────────────────────

async function loadAlsoAt(name, source, el) {
  if (!el || !name) return;
  try {
    const url = `${API_BASE}/api/also-at?name=${encodeURIComponent(name)}&source=${encodeURIComponent(source)}`;
    const items = await (await fetch(url)).json();
    if (!Array.isArray(items) || items.length === 0) { el.style.display = "none"; return; }
    el.innerHTML = `<div class="also-at-title">🌍 Also available at</div>` +
      items.map(i => {
        const price = i.Price_CZK ? Math.round(i.Price_CZK).toLocaleString() + " Kč"
          : i.Price_EUR ? parseFloat(i.Price_EUR).toFixed(0) + " €" : "—";
        const srcLbl = SOURCE_LABELS[i.source] || i.source;
        const qual = i.RecommendRate_pct ? `${i.RecommendRate_pct}% rec`
          : i.AvgStarRating ? `${parseFloat(i.AvgStarRating).toFixed(1)} ★` : "";
        return `<a class="also-at-row" href="${escHtml(i.ProductURL || '#')}" target="_blank" rel="noopener">
          <span class="also-at-src">${escHtml(srcLbl)}</span>
          <span class="also-at-name">${escHtml((i.Name || '').slice(0, 50))}</span>
          <span class="also-at-right">${qual ? `<span class="also-at-qual">${escHtml(qual)}</span>` : ""}<span class="also-at-price">${escHtml(price)}</span></span>
        </a>`;
      }).join("");
    el.style.display = "";
  } catch(e) { el.style.display = "none"; }
}

// ── Category quick-filter pills ───────────────────────────────────────────────

const CAT_PILL_ICONS = {
  "Phones & Tablets": "📱", "Computers": "💻", "Audio": "🎧",
  "TV & Video": "📺", "Wearables": "⌚", "Cameras": "📷",
  "Gaming": "🎮", "Smart Home": "💡", "Home Appliances": "🏠",
  "Large Appliances": "🧺", "Small Appliances": "🍳",
  "Storage": "💾", "Networking": "📡", "Accessories": "🔌",
  "Toys & Games": "🧩", "Sports & Outdoor": "⚽",
  "Garden & Outdoors": "🌿", "Health & Beauty": "💊",
  "Baby & Kids": "👶", "Home & Garden": "🏡",
  "Vacuum Cleaners": "🧹", "Robot Vacuums": "🤖",
  "Ovens & Stoves": "🍳", "Refrigerators": "🧊", "Washing Machines": "🫧",
  "Coffee Makers": "☕", "Headphones": "🎧", "Speakers": "🔊",
  "Other": "🔮", "Miscellaneous": "🔮",
};

/** Filter by a specific normalized subcategory — finds its main category automatically. */
function filterByCategory(normalizedCat) {
  if (!normalizedCat) return;
  // Search categoriesTree for the main category that contains this sub
  let foundMain = null;
  for (const entry of categoriesTree) {
    if (entry.subs && entry.subs.some(s => s.sub === normalizedCat)) {
      foundMain = entry.main;
      break;
    }
  }
  const mcSel = document.getElementById("filter-main-category");
  const subSel = document.getElementById("filter-category");
  const cpRow  = document.getElementById("cat-pills-row");
  if (foundMain && mcSel) {
    mcSel.value = foundMain;
    populateSubcategories(foundMain);
    if (subSel) subSel.value = normalizedCat;
    // Sync cat pills active state
    if (cpRow) cpRow.querySelectorAll(".cat-pill").forEach(b => b.classList.toggle("cat-pill-active", b.dataset.cat === foundMain));
    renderSubCatPills(foundMain);
  } else if (subSel) {
    // Fallback: try setting subcategory directly if already visible
    const opt = [...subSel.options].find(o => o.value === normalizedCat);
    if (opt) { subSel.value = normalizedCat; }
  }
  currentPage = 1;
  triggerSearch();
}

function renderCatPills() {
  const row = document.getElementById("cat-pills-row");
  if (!row || !categoriesTree.length) return;
  const activeCat = document.getElementById("filter-main-category")?.value || "";
  // Show top 13 main categories (already sorted by count in categoriesTree)
  const top = categoriesTree.filter(c => c.main).slice(0, 13);
  row.innerHTML = top.map(c => {
    const icon = CAT_PILL_ICONS[c.main] || "📦";
    const totalCount = (c.subs || []).reduce((s, sub) => s + (sub.count || 0), 0);
    const isActive = activeCat === c.main;
    return `<button class="cat-pill${isActive ? " cat-pill-active" : ""}" data-cat="${escHtml(c.main)}" title="${escHtml(c.main)} (${totalCount.toLocaleString()} products)">${icon} ${escHtml(c.main)}</button>`;
  }).join("");
  row.style.display = "flex";

  row.querySelectorAll(".cat-pill").forEach(btn => {
    btn.addEventListener("click", () => {
      const mc = document.getElementById("filter-main-category");
      if (!mc) return;
      const wasActive = mc.value === btn.dataset.cat;
      mc.value = wasActive ? "" : btn.dataset.cat;
      populateSubcategories(mc.value);
      currentPage = 1;
      triggerSearch();
      // Update pill active states immediately
      row.querySelectorAll(".cat-pill").forEach(b => {
        b.classList.toggle("cat-pill-active", b === btn && !wasActive);
      });
      // Show/hide sub-category pills
      renderSubCatPills(wasActive ? "" : btn.dataset.cat);
    });
  });

  // Show sub-category pills for any already-active main category
  renderSubCatPills(activeCat);
}

function renderSubCatPills(mainCat) {
  const row = document.getElementById("sub-pills-row");
  if (!row) return;
  if (!mainCat) { row.style.display = "none"; row.innerHTML = ""; return; }

  const entry = categoriesTree.find(e => e.main === mainCat);
  if (!entry || !entry.subs || entry.subs.length < 2) { row.style.display = "none"; return; }

  const activeSub = document.getElementById("filter-category")?.value || "";
  // Show up to 12 sub-categories
  const subs = entry.subs.slice(0, 12);
  row.innerHTML = subs.map(s => {
    const isActive = activeSub === s.sub;
    return `<button class="cat-pill sub-pill${isActive ? " cat-pill-active" : ""}" data-sub="${escHtml(s.sub)}" title="${escHtml(s.sub)} (${(s.count||0).toLocaleString()} products)">${escHtml(s.sub)}</button>`;
  }).join("");
  row.style.display = "flex";

  row.querySelectorAll(".sub-pill").forEach(btn => {
    btn.addEventListener("click", () => {
      const sc = document.getElementById("filter-category");
      if (!sc) return;
      const wasActive = sc.value === btn.dataset.sub;
      sc.value = wasActive ? "" : btn.dataset.sub;
      currentPage = 1;
      triggerSearch();
      row.querySelectorAll(".sub-pill").forEach(b => {
        b.classList.toggle("cat-pill-active", b === btn && !wasActive);
      });
    });
  });
}

// ── Product history sparkline in modal ────────────────────────────────────────

async function loadModalHistory(productUrl, currency) {
  const section = document.getElementById("modal-history");
  const chart   = document.getElementById("modal-history-chart");
  if (!section || !chart) return;

  section.style.display = "";
  chart.innerHTML = '<span style="color:#aaa;font-size:0.8em">Loading history…</span>';

  try {
    const encoded = encodeURIComponent(productUrl);
    const res  = await fetch(`${API_BASE}/api/product-history?url=${encoded}`);
    const rows = await res.json();

    if (!Array.isArray(rows) || rows.length < 2) {
      section.style.display = "none";
      return;
    }

    // Build sparklines for recommend_pct, avg_star_rating, price, reviews
    const recData   = rows.filter(r => r.recommend_pct  != null).map(r => ({ d: r.snapshot_date, v: r.recommend_pct }));
    const starData  = rows.filter(r => r.avg_star_rating != null).map(r => ({ d: r.snapshot_date, v: r.avg_star_rating }));
    // Use price_czk when available (stores CZK, SEK, PLN, HUF etc.);
    // fall back to price_eur for EUR/GBP/CHF priced products
    const hasCzk = rows.some(r => r.price_czk != null);
    const priceField = hasCzk ? "price_czk" : "price_eur";
    // Build currency-aware label + formatter based on the product's currency
    const _cur = currency || "";
    let priceLabel, priceFmt;
    if (!hasCzk || _cur === "EUR" || _cur === "GBP" || _cur === "CHF" || _cur === "USD") {
      // Price stored in price_eur
      const sym = _cur === "GBP" ? "£" : _cur === "CHF" ? "CHF " : _cur === "USD" ? "$" : "";
      const suf = (!sym && _cur !== "GBP") ? " €" : "";
      priceLabel = `Price (${_cur || "€"})`;
      priceFmt   = v => sym + Math.round(v).toLocaleString("de-DE") + suf;
    } else {
      // Price stored in price_czk (CZK, SEK, NOK, DKK, PLN, HUF, …)
      const _labels = { SEK:"SEK", NOK:"NOK", DKK:"DKK", PLN:"PLN", HUF:"Ft" };
      const _locales = { SEK:"sv-SE", NOK:"nb-NO", DKK:"da-DK", PLN:"pl-PL", HUF:"hu-HU" };
      const _suffix  = { SEK:" kr", NOK:" kr", DKK:" kr", PLN:" zł", HUF:" Ft" };
      const lbl  = _labels[_cur]  || "Kč";
      const loc  = _locales[_cur] || "cs-CZ";
      const suf  = _suffix[_cur]  || " Kč";
      priceLabel = `Price (${lbl})`;
      priceFmt   = v => Math.round(v).toLocaleString(loc) + suf;
    }
    const priceData = rows.filter(r => r[priceField] != null).map(r => ({ d: r.snapshot_date, v: r[priceField] }));
    const revData   = rows.filter(r => r.review_count  != null).map(r => ({ d: r.snapshot_date, v: r.review_count }));

    const segments = [];
    if (recData.length   >= 2) segments.push({ label: "Recommend %", data: recData,   color: "#43a047", fmt: v => v.toFixed(1) + "%", higherIsBetter: true });
    if (starData.length  >= 2) segments.push({ label: "Stars",       data: starData,  color: "#f9a825", fmt: v => v.toFixed(2),       higherIsBetter: true });
    if (priceData.length >= 2) segments.push({ label: priceLabel,    data: priceData, color: "#1565c0", fmt: priceFmt,                higherIsBetter: false });
    if (revData.length   >= 2) segments.push({ label: "Reviews",     data: revData,   color: "#7b1fa2", fmt: v => Math.round(v).toLocaleString(), higherIsBetter: true });

    if (!segments.length) { section.style.display = "none"; return; }

    // Detect if ALL segments are flat (identical values across all snapshots)
    const allFlat = segments.every(seg => {
      const vals = seg.data.map(d => d.v);
      return Math.max(...vals) === Math.min(...vals);
    });
    const earliestDate = rows[0]?.snapshot_date || "";
    const latestDate   = rows[rows.length - 1]?.snapshot_date || "";
    const snapshotCount = rows.length;

    let sparkHtml;
    if (allFlat) {
      // Compact summary: show current values + single "no changes" note
      const metricItems = segments.map(seg => {
        const v = seg.fmt(seg.data[seg.data.length - 1].v);
        return `<div class="history-stable-metric">
          <span class="history-stable-label">${seg.label}</span>
          <span class="history-stable-val" style="color:${seg.color}">${v}</span>
        </div>`;
      }).join("");
      sparkHtml = `<div class="history-stable-banner">
        <div class="history-stable-icon">📊</div>
        <div class="history-stable-text">
          <strong>No changes detected</strong>
          <span>All metrics stable across ${snapshotCount} snapshots (${earliestDate} → ${latestDate})</span>
        </div>
      </div>
      <div class="history-stable-metrics">${metricItems}</div>`;
    } else {
      sparkHtml = segments.map((seg, i) => buildSparkline(seg, i)).join("");
    }

    // Note for early tracking (few real data points)
    const earlyNote = snapshotCount <= 3
      ? `<div class="history-early-note">📅 Tracking started recently — more data will appear as daily snapshots are collected.</div>`
      : "";

    chart.innerHTML = earlyNote + sparkHtml + buildHistoryTableToggle(rows);

    // Wire up hover interactions per sparkline + the table toggle.
    segments.forEach((seg, i) => attachSparkInteractions(chart, seg, i));
    const tbtn = chart.querySelector(".history-table-toggle");
    if (tbtn) tbtn.addEventListener("click", () => {
      const t = chart.querySelector(".history-table");
      const open = t.style.display !== "none";
      t.style.display = open ? "none" : "";
      tbtn.textContent = open ? "Show full table ▾" : "Hide table ▴";
    });

  } catch (e) {
    section.style.display = "none";
  }
}

function buildSparkline({ label, data, color, fmt, higherIsBetter = true }, sparkIdx) {
  const W = 460, H = 96, PAD = 8;
  const vals  = data.map(d => d.v);
  const dates = data.map(d => d.d);
  const minV  = Math.min(...vals);
  const maxV  = Math.max(...vals);
  const range = maxV - minV;

  const last     = fmt(vals[vals.length - 1]);
  const diff     = vals[vals.length - 1] - vals[0];
  const isFlat   = range === 0;
  const gradId   = `sg${color.replace('#','')}_${sparkIdx}`;

  // When all values are identical, show a "stable" card instead of a flat line
  if (isFlat) {
    const diffColor = "#888";
    return `<div class="sparkline-wrap sparkline-flat" data-spark-idx="${sparkIdx}">
    <div class="sparkline-label">${label}</div>
    <div class="sparkline-stable">
      <span class="sparkline-stable-val" style="color:${color}">${last}</span>
      <span class="sparkline-stable-tag">— stable since ${dates[0]}</span>
    </div>
    <div class="sparkline-footer">
      <span class="sparkline-range">${dates[0]} → ${dates[dates.length-1]}</span>
      <span class="sparkline-delta" style="color:${diffColor}">no change</span>
      <span class="sparkline-last">${last}</span>
    </div>
  </div>`;
  }

  const effectiveRange = range || 1;
  const xScale = i => PAD + (i / (data.length - 1)) * (W - PAD * 2);
  const yScale = v => H - PAD - ((v - minV) / effectiveRange) * (H - PAD * 2);

  const points = data.map((d, i) => `${xScale(i).toFixed(1)},${yScale(d.v).toFixed(1)}`).join(" ");

  // Fill polygon
  const fillPts = [
    `${xScale(0).toFixed(1)},${H}`,
    ...data.map((d, i) => `${xScale(i).toFixed(1)},${yScale(d.v).toFixed(1)}`),
    `${xScale(data.length-1).toFixed(1)},${H}`
  ].join(" ");

  const diffStr = diff >= 0 ? `+${fmt(diff)}` : fmt(diff);
  // Colour: green when change is good (↑ for higher-is-better; ↓ for price/lower-is-better)
  const diffColor = diff === 0 ? "#888"
    : (diff > 0) === higherIsBetter ? "#43a047"
    : "#e53935";
  const gradId2 = gradId;  // alias for compat

  return `<div class="sparkline-wrap" data-spark-idx="${sparkIdx}">
    <div class="sparkline-label">${label}</div>
    <div class="sparkline-canvas">
      <svg class="sparkline-svg" width="${W}" height="${H}" viewBox="0 0 ${W} ${H}">
        <defs>
          <linearGradient id="${gradId}" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stop-color="${color}" stop-opacity="0.25"/>
            <stop offset="100%" stop-color="${color}" stop-opacity="0.03"/>
          </linearGradient>
        </defs>
        <polygon points="${fillPts}" fill="url(#${gradId})" />
        <polyline points="${points}" fill="none" stroke="${color}" stroke-width="2"
          stroke-linecap="round" stroke-linejoin="round"/>
        <circle cx="${xScale(data.length-1).toFixed(1)}" cy="${yScale(vals[vals.length-1]).toFixed(1)}"
          r="3.5" fill="${color}"/>
        <line class="spark-guide" x1="0" y1="0" x2="0" y2="${H}" stroke="${color}" stroke-width="1"
          stroke-dasharray="3 3" opacity="0" pointer-events="none"/>
        <circle class="spark-cursor" cx="0" cy="0" r="4" fill="#fff" stroke="${color}" stroke-width="2"
          opacity="0" pointer-events="none"/>
      </svg>
      <div class="spark-tooltip" style="opacity:0"></div>
    </div>
    <div class="sparkline-footer">
      <span class="sparkline-range">${dates[0]} → ${dates[dates.length-1]}</span>
      <span class="sparkline-delta" style="color:${diffColor}">${diffStr}</span>
      <span class="sparkline-last">${last}</span>
    </div>
  </div>`;
}

function attachSparkInteractions(container, seg, sparkIdx) {
  const wrap = container.querySelector(`.sparkline-wrap[data-spark-idx="${sparkIdx}"]`);
  if (!wrap) return;
  const svg     = wrap.querySelector(".sparkline-svg");
  const guide   = wrap.querySelector(".spark-guide");
  const cursor  = wrap.querySelector(".spark-cursor");
  const tooltip = wrap.querySelector(".spark-tooltip");
  if (!svg || !guide || !cursor || !tooltip) return;

  const W = 460, H = 96, PAD = 8;
  const data = seg.data;
  const vals = data.map(d => d.v);
  const minV = Math.min(...vals);
  const maxV = Math.max(...vals);
  const range = maxV - minV || 1;
  const xScale = i => PAD + (i / (data.length - 1)) * (W - PAD * 2);
  const yScale = v => H - PAD - ((v - minV) / range) * (H - PAD * 2);

  svg.addEventListener("mousemove", e => {
    const rect = svg.getBoundingClientRect();
    const xPx  = e.clientX - rect.left;
    const xSvg = (xPx / rect.width) * W;
    // Find nearest data index
    let nearest = 0;
    let bestDist = Infinity;
    for (let i = 0; i < data.length; i++) {
      const d = Math.abs(xScale(i) - xSvg);
      if (d < bestDist) { bestDist = d; nearest = i; }
    }
    const cx = xScale(nearest);
    const cy = yScale(data[nearest].v);
    guide.setAttribute("x1", cx); guide.setAttribute("x2", cx);
    guide.setAttribute("opacity", "0.7");
    cursor.setAttribute("cx", cx); cursor.setAttribute("cy", cy);
    cursor.setAttribute("opacity", "1");
    tooltip.textContent = `${data[nearest].d}  ·  ${seg.fmt(data[nearest].v)}`;
    tooltip.style.opacity = "1";
    // Position tooltip near the cursor, clamped inside the canvas
    const tipLeftPct = Math.max(2, Math.min(98, (cx / W) * 100));
    tooltip.style.left = `${tipLeftPct}%`;
  });
  svg.addEventListener("mouseleave", () => {
    guide.setAttribute("opacity", "0");
    cursor.setAttribute("opacity", "0");
    tooltip.style.opacity = "0";
  });
}

function buildHistoryTableToggle(rows) {
  if (!Array.isArray(rows) || rows.length < 2) return "";
  const fmtNum = v => v == null ? "—" : (typeof v === "number" ? v.toFixed(2) : v);
  const fmtInt = v => v == null ? "—" : Math.round(v).toLocaleString();
  // Determine which price column has data and set label accordingly
  const hasEur = rows.some(r => r.price_eur != null);
  const hasCzk = rows.some(r => r.price_czk != null);
  const priceLabel = hasCzk ? "Price (Kč)" : hasEur ? "Price (€)" : "Price";
  const getPrice = r => {
    const v = hasCzk ? r.price_czk : r.price_eur;
    return v == null ? "—" : Math.round(v).toLocaleString();
  };
  const tableRows = [...rows].reverse().map(r => `
    <tr>
      <td>${r.snapshot_date || "—"}</td>
      <td>${r.recommend_pct == null ? "—" : r.recommend_pct.toFixed(1) + "%"}</td>
      <td>${fmtNum(r.avg_star_rating)}</td>
      <td>${getPrice(r)}</td>
      <td>${fmtInt(r.review_count)}</td>
    </tr>`).join("");
  return `
    <button class="history-table-toggle" type="button">Show full table ▾</button>
    <table class="history-table" style="display:none">
      <thead><tr><th>Date</th><th>Recommend</th><th>Stars</th><th>${priceLabel}</th><th>Reviews</th></tr></thead>
      <tbody>${tableRows}</tbody>
    </table>`;
}

// ── Brands leaderboard ────────────────────────────────────────────────────────

let _brandsData = [];
let _brandsSortCol = "avg_stars";
let _brandsSortAsc = false;

function initBrandsPanel() {
  const btn = document.getElementById("view-brands");
  if (!btn) return;

  btn.addEventListener("click", () => {
    const panel    = document.getElementById("brands-panel");
    const grid     = document.getElementById("product-grid");
    const pgn      = document.getElementById("pagination");
    const mvPanel  = document.getElementById("movers-panel");
    const cmPanel  = document.getElementById("cross-market-panel");
    const isOpen   = panel.style.display !== "none";

    if (isOpen) {
      panel.style.display = "none";
      grid.style.display  = "";
      pgn.style.display   = "";
      btn.classList.remove("active");
    } else {
      if (mvPanel && mvPanel.style.display !== "none") {
        mvPanel.style.display = "none";
        document.getElementById("view-movers")?.classList.remove("active");
      }
      if (cmPanel && cmPanel.style.display !== "none") {
        cmPanel.style.display = "none";
        document.getElementById("view-cross-market")?.classList.remove("active");
      }
      panel.style.display = "";
      grid.style.display  = "none";
      pgn.style.display   = "none";
      const _brLmb = document.getElementById("load-more-btn");
      if (_brLmb) _brLmb.style.display = "none";
      btn.classList.add("active");
      if (!_brandsData.length) loadBrands();
    }
  });

  document.getElementById("brands-min-products")?.addEventListener("change", () => {
    _brandsData = [];
    loadBrands();
  });

  document.getElementById("brands-search")?.addEventListener("input", renderBrandsTable);
}

async function loadBrands() {
  const gridEl  = document.getElementById("brands-grid");
  const status  = document.getElementById("brands-status");
  const minProd = document.getElementById("brands-min-products")?.value || "3";
  if (!gridEl) return;

  gridEl.innerHTML = '<div style="padding:20px;color:#888">Loading brand data…</div>';
  if (status) status.textContent = "";

  try {
    const res  = await fetch(`${API_BASE}/api/brands?min_products=${minProd}`);
    const data = await res.json();
    if (data.error) throw new Error(data.error);

    _brandsData = data.brands;
    if (status) status.textContent = `${_brandsData.length.toLocaleString()} brands`;
    renderBrandsTable();
  } catch (e) {
    gridEl.innerHTML = `<div style="padding:20px;color:#c62828">Error: ${escHtml(e.message)}</div>`;
    if (status) status.textContent = "Failed";
  }
}

function renderBrandsTable() {
  const gridEl  = document.getElementById("brands-grid");
  if (!gridEl) return;

  const query   = (document.getElementById("brands-search")?.value || "").toLowerCase();
  let rows = query
    ? _brandsData.filter(b => b.brand.toLowerCase().includes(query))
    : _brandsData;

  // Sort
  rows = [...rows].sort((a, b) => {
    const av = a[_brandsSortCol] ?? -Infinity;
    const bv = b[_brandsSortCol] ?? -Infinity;
    return _brandsSortAsc ? (av > bv ? 1 : -1) : (av < bv ? 1 : -1);
  });

  const colDefs = [
    { key: "brand",        label: "Brand",        fmt: v => escHtml(v) },
    { key: "avg_stars",    label: "⭐ Avg Stars",  fmt: v => v == null ? "—" : v.toFixed(2) },
    { key: "avg_recommend",label: "👍 Recommend",  fmt: v => v == null ? "—" : v.toFixed(1) + "%" },
    { key: "products",     label: "# Products",   fmt: v => v.toLocaleString() },
    { key: "total_reviews",label: "Total Reviews", fmt: v => v.toLocaleString() },
    { key: "sources",      label: "Sources",      fmt: v => v },
  ];

  const headerCells = colDefs.map(c => {
    const arrow = c.key === _brandsSortCol ? (_brandsSortAsc ? " ▲" : " ▼") : "";
    return `<th class="brands-th sortable${c.key === _brandsSortCol ? " sorted" : ""}" data-col="${c.key}">${c.label}${arrow}</th>`;
  }).join("");

  const bodyRows = rows.map((b, i) => {
    const starBar = b.avg_stars
      ? `<span class="brand-star-bar" style="--pct:${Math.min(b.avg_stars/5*100,100).toFixed(0)}%"></span>`
      : "";
    const cells = colDefs.map(c => {
      let val = c.fmt(b[c.key]);
      if (c.key === "brand") val = `<span class="brand-rank">#${i+1}</span> ${val}`;
      if (c.key === "avg_stars") val = `${val} ${starBar}`;
      return `<td>${val}</td>`;
    }).join("");
    return `<tr class="brands-row" data-brand="${escHtml(b.brand)}">${cells}</tr>`;
  }).join("");

  gridEl.innerHTML = `
    <div class="brands-count">${rows.length.toLocaleString()} brands shown</div>
    <div class="brands-table-wrap">
      <table class="brands-table">
        <thead><tr>${headerCells}</tr></thead>
        <tbody>${bodyRows}</tbody>
      </table>
    </div>`;

  // Sort click handlers
  gridEl.querySelectorAll(".brands-th.sortable").forEach(th => {
    th.addEventListener("click", () => {
      const col = th.dataset.col;
      if (_brandsSortCol === col) {
        _brandsSortAsc = !_brandsSortAsc;
      } else {
        _brandsSortCol = col;
        _brandsSortAsc = col === "brand";
      }
      renderBrandsTable();
    });
  });

  // Row click → filter main grid by exact brand match
  gridEl.querySelectorAll(".brands-row").forEach(row => {
    row.style.cursor = "pointer";
    row.title = "Click to browse products by this brand";
    row.addEventListener("click", () => {
      const brand = row.dataset.brand;
      // Close brands panel, show grid, set exact brand filter
      document.getElementById("brands-panel").style.display = "none";
      document.getElementById("product-grid").style.display = "";
      document.getElementById("pagination").style.display   = "";
      document.getElementById("view-brands")?.classList.remove("active");
      activeBrand = brand;
      currentPage = 1;
      fetchProducts();
    });
  });
}

document.addEventListener("DOMContentLoaded", initBrandsPanel);

// ── Institut Kvality auth ────────────────────────────────────────────────────
const AUTH_TOKEN_KEY = "ik_token";

function getAuthToken() { return localStorage.getItem(AUTH_TOKEN_KEY); }
function setAuthToken(t) { localStorage.setItem(AUTH_TOKEN_KEY, t); }
function clearAuthToken() { localStorage.removeItem(AUTH_TOKEN_KEY); }

function updateAuthButton(user) {
  const btn   = document.getElementById("auth-btn");
  const icon  = document.getElementById("auth-btn-icon");
  const label = document.getElementById("auth-btn-label");
  if (!btn) return;
  if (user) {
    btn.classList.add("logged-in");
    icon.textContent  = "✓";
    label.textContent = user.email.split("@")[0];
  } else {
    btn.classList.remove("logged-in");
    icon.textContent  = "🔑";
    label.textContent = "Log in";
  }
}

function openAuthModal(user) {
  const overlay = document.getElementById("auth-modal-overlay");
  if (!overlay) return;
  overlay.style.display = "flex";
  showAuthView(user ? "user" : "login");
  if (user) populateUserPanel(user);
}

function closeAuthModal() {
  const overlay = document.getElementById("auth-modal-overlay");
  if (overlay) overlay.style.display = "none";
}

function showAuthView(view) {
  document.getElementById("login-form").style.display    = view === "login"    ? "" : "none";
  document.getElementById("register-form").style.display = view === "register" ? "" : "none";
  document.getElementById("user-panel").style.display    = view === "user"     ? "" : "none";
  document.getElementById("tab-login").classList.toggle("active",    view === "login");
  document.getElementById("tab-register").classList.toggle("active", view === "register");
}

function populateUserPanel(user) {
  document.getElementById("user-email").textContent   = user.email || "";
  document.getElementById("user-country").textContent = user.country
    ? `Country: ${user.country}` : "";
  document.getElementById("user-contrib-count").textContent =
    (user.contrib_count ?? 0).toLocaleString();

  const token = getAuthToken() || "";
  const cmd   = document.getElementById("contrib-cmd");
  const span  = document.getElementById("user-token-display");
  if (span) span.textContent = token.slice(0, 8) + "…";

  // Load global stats
  fetch("/api/contrib-stats")
    .then(r => r.json())
    .then(s => {
      const staged = document.getElementById("contrib-total-staged");
      const merged = document.getElementById("contrib-total-merged");
      if (staged) staged.textContent = (s.total_staged  ?? 0).toLocaleString();
      if (merged) merged.textContent = (s.total_merged  ?? 0).toLocaleString();
    })
    .catch(() => {});
}

async function tryRestoreSession() {
  const token = getAuthToken();
  if (!token) return null;
  try {
    const r = await fetch("/api/me", {
      headers: { Authorization: `Bearer ${token}` }
    });
    const data = await r.json();
    if (data.ok && data.user) {
      updateAuthButton(data.user);
      return data.user;
    }
  } catch (_) {}
  clearAuthToken();
  updateAuthButton(null);
  return null;
}

function initAuthModal() {
  // Try restoring session on load
  tryRestoreSession();

  const overlay   = document.getElementById("auth-modal-overlay");
  const authBtn   = document.getElementById("auth-btn");
  const closeBtn  = document.getElementById("auth-modal-close");
  const tabLogin  = document.getElementById("tab-login");
  const tabReg    = document.getElementById("tab-register");
  const loginForm = document.getElementById("login-form");
  const regForm   = document.getElementById("register-form");
  const logoutBtn = document.getElementById("auth-logout");
  const copyBtn   = document.getElementById("copy-token-btn");

  if (!overlay || !authBtn) return;

  authBtn.addEventListener("click", async () => {
    const token = getAuthToken();
    let user = null;
    if (token) {
      try {
        const r = await fetch("/api/me", { headers: { Authorization: `Bearer ${token}` }});
        const d = await r.json();
        if (d.ok) user = d.user;
      } catch (_) {}
    }
    openAuthModal(user);
  });

  closeBtn?.addEventListener("click", closeAuthModal);
  overlay.addEventListener("click", e => { if (e.target === overlay) closeAuthModal(); });

  tabLogin?.addEventListener("click",  () => showAuthView("login"));
  tabReg?.addEventListener("click",    () => showAuthView("register"));

  // Login submit
  loginForm?.addEventListener("submit", async e => {
    e.preventDefault();
    const email    = document.getElementById("login-email").value.trim();
    const password = document.getElementById("login-password").value;
    const errEl    = document.getElementById("login-error");
    const btn      = document.getElementById("login-submit");
    errEl.textContent = "";
    btn.disabled = true;
    btn.textContent = "Logging in…";
    try {
      const r = await fetch("/api/login", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ email, password }),
      });
      const data = await r.json();
      if (data.ok) {
        setAuthToken(data.token);
        updateAuthButton(data.user);
        showAuthView("user");
        populateUserPanel(data.user);
      } else {
        errEl.textContent = data.error || "Login failed.";
      }
    } catch (err) {
      errEl.textContent = "Network error — is the server running?";
    } finally {
      btn.disabled = false;
      btn.textContent = "Log in";
    }
  });

  // Register submit
  regForm?.addEventListener("submit", async e => {
    e.preventDefault();
    const email    = document.getElementById("reg-email").value.trim();
    const password = document.getElementById("reg-password").value;
    const country  = document.getElementById("reg-country").value;
    const q1       = document.getElementById("reg-q1").value.trim();
    const q2       = document.getElementById("reg-q2").value.trim();
    const q3       = document.getElementById("reg-q3").value.trim();
    const errEl    = document.getElementById("reg-error");
    const btn      = document.getElementById("reg-submit");
    errEl.textContent = "";
    if (!country) { errEl.textContent = "Please select your country."; return; }
    btn.disabled = true;
    btn.textContent = "Creating account…";
    try {
      const r = await fetch("/api/register", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ email, password, country, q1, q2, q3 }),
      });
      const data = await r.json();
      if (data.ok) {
        setAuthToken(data.token);
        updateAuthButton(data.user);
        showAuthView("user");
        populateUserPanel(data.user);
      } else {
        errEl.textContent = data.error || "Registration failed.";
      }
    } catch (err) {
      errEl.textContent = "Network error — is the server running?";
    } finally {
      btn.disabled = false;
      btn.textContent = "Create account & join";
    }
  });

  logoutBtn?.addEventListener("click", () => {
    clearAuthToken();
    updateAuthButton(null);
    closeAuthModal();
  });

  copyBtn?.addEventListener("click", () => {
    const token = getAuthToken() || "";
    const text  = `python3 contrib_scraper.py --token ${token}`;
    navigator.clipboard?.writeText(text).then(() => {
      copyBtn.textContent = "✓";
      setTimeout(() => { copyBtn.textContent = "📋"; }, 1500);
    });
  });
}

document.addEventListener("DOMContentLoaded", initAuthModal);

// ── Dark / light mode toggle ──────────────────────────────────────────────────
(function initTheme() {
  // Restore saved preference (or respect OS setting — CSS handles that automatically)
  const saved = localStorage.getItem("theme");
  if (saved === "dark" || saved === "light") {
    document.documentElement.dataset.theme = saved;
  }
  function updateIcon() {
    const btn = document.getElementById("theme-toggle");
    if (!btn) return;
    const isDark = document.documentElement.dataset.theme === "dark"
      || (!document.documentElement.dataset.theme &&
          window.matchMedia("(prefers-color-scheme: dark)").matches);
    btn.textContent = isDark ? "☀️" : "🌙";
    btn.title = isDark ? "Switch to light mode" : "Switch to dark mode";
  }
  document.addEventListener("DOMContentLoaded", () => {
    updateIcon();
    const btn = document.getElementById("theme-toggle");
    if (!btn) return;
    btn.addEventListener("click", () => {
      const current = document.documentElement.dataset.theme;
      const isCurrentlyDark = current === "dark"
        || (!current && window.matchMedia("(prefers-color-scheme: dark)").matches);
      const next = isCurrentlyDark ? "light" : "dark";
      document.documentElement.dataset.theme = next;
      localStorage.setItem("theme", next);
      updateIcon();
    });
  });
})();
