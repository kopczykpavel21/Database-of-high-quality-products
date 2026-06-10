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
let avoidMode = false;
let categoriesTree = [];   // [{main, subs:[{sub,count}]}]

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

const SOURCE_LABELS = {
  alza: "Alza.cz", heureka: "Heureka.cz", zbozi: "Zbozi.cz",
  amazon: "Amazon.de", amazon_us: "Amazon.com", otto: "Otto.de", // warentest: hidden
  // dtest: hidden datart: "Datart.cz", ceneo: "Ceneo.pl",
  heureka_sk: "Heureka.sk", conrad: "Conrad.de", fnac: "Fnac.fr",
  fr_ir: "🏛️ Indice de Réparabilité", coolblue: "Coolblue.nl"
};

function repairabilityBadge(score, date) {
  if (score === null || score === undefined) return "";
  const num = parseFloat(score);
  const cls = num >= 7 ? "ir-good" : num >= 4 ? "ir-mid" : "ir-bad";
  const tip = date ? ` title="Indice de Réparabilité · ${date}"` : ' title="Indice de Réparabilité (French repairability score)"';
  return `<span class="ir-badge ${cls}"${tip}>🔧 ${num.toFixed(1)}/10</span>`;
}

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
    { value: "AvgStarRating_desc",           label: "⭐ WT Grade (best → worst)" },
    { value: "AvgStarRating",                label: "⭐ WT Grade (worst → best)" },
    { value: "RecommendRate_pct_desc",       label: "Recommend rate (high → low)" },
    { value: "RecommendRate_pct",            label: "Recommend rate (low → high)" },
    { value: "ReviewsCount_desc",            label: "Most reviewed" },
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
  // Default: WT Grade for warentest source, otherwise recommend rate
  const srcFilter = document.getElementById("filter-source");
  const src = srcFilter ? srcFilter.value : "";
  sel.value = src === "warentest" ? "AvgStarRating_desc" : "RecommendRate_pct_desc";
}

// When source filter changes, swap the default sort intelligently
function onSourceFilterChange() {
  const src = document.getElementById("filter-source").value;
  const sel = document.getElementById("sort-by");
  const cur = sel.value;
  // Only auto-switch if user hasn't manually chosen something meaningful for the new source
  const wtSorts    = new Set(["AvgStarRating_desc","AvgStarRating"]);
  const nonWtSorts = new Set(["RecommendRate_pct_desc","RecommendRate_pct","ReviewsCount_desc"]);
  // warentest sort hidden
  // warentest sort hidden
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
async function fetchProducts() {
  const grid = document.getElementById("product-grid");
  grid.innerHTML = '<div class="loading-state"><div class="spinner"></div><p>Loading products…</p></div>';

  const filters = getFilters();
  if (filters.source === "fr_ir") {
    await ensureIrData();
    renderFrGov(filters);
    return;
  }

  const params = new URLSearchParams(filters);
  const res = await fetch(`${API_BASE}/api/products?${params}`);
  const data = await res.json();
  renderProducts(data);
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
    return;
  }

  const SOURCE_COUNTRY_MAP = { otto:"DE", amazon:"DE", ceneo:"PL",
                                alza:"CZ", heureka:"CZ", zbozi:"CZ", datart:"CZ",
                                amazon_us:"US", heureka_sk:"SK", conrad:"DE",
                                fnac:"FR", fr_ir:"FR_IR", coolblue:"NL" };
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
  const catEn = translateCat(p.Category || "");
  const rankLabel = catEn
    ? "in " + (catEn.length > 18 ? catEn.slice(0, 16) + "…" : catEn)
    : "Rank";

  const keywords = p.keywords ? JSON.parse(p.keywords) : [];
  const cardTags = keywords.slice(0, 2).map(k =>
    `<span class="kw-tag">${escHtml(k)}</span>`
  ).join("");

  const badge = qualityBadge(p);

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
      : `<div class="metric">
          <div class="metric-label">${rankLabel}</div>
          <div class="metric-value ${rankClass}">${rankDisplay}</div>
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

  return `
  <div class="product-card" onclick="openModal(${JSON.stringify(JSON.stringify(p))})">
    <div class="card-top-row">${sourceBadge}${badge}</div>
    <div class="card-category">${escHtml(translateCat(p.Category || ""))}</div>
    <div class="card-name">${escHtml(p.Name || "Unnamed")}</div>
    <div class="card-metrics">
      ${firstMetric}
      ${secondMetric}
    </div>
    ${wtSubRow}
    ${irSubRow}
    ${cardTags ? `<div class="card-tags">${cardTags}</div>` : ""}
    ${!isFrGov ? (() => { const ir = getIR(p); return ir ? repairabilityBadge(ir.s, ir.d) : ""; })() : ""}
    ${starsRow}
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

  content.innerHTML = `
    <div class="modal-category">${escHtml(translateCat(p.Category || ""))}</div>
    <div class="modal-name">${escHtml(p.Name || "Unnamed")}</div>

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

    <div class="modal-actions">
      ${p.ProductURL ? `<a class="btn-primary" href="${escHtml(p.ProductURL)}" target="_blank">View on ${SOURCE_LABELS[p.source] || "Shop"} →</a>` : ""}
      <button class="btn-secondary" onclick="closeModal()">Close</button>
    </div>`;

  overlay.classList.add("open");

  // Async: load sparkline history after modal renders
  if (p.ProductURL) {
    const histSection = document.getElementById("modal-history");
    if (histSection) histSection.style.display = "none"; // reset
    loadModalHistory(p.ProductURL);
  }
}

function closeModal() {
  document.getElementById("modal-overlay").classList.remove("open");
  // Hide history section for next open
  const histSection = document.getElementById("modal-history");
  if (histSection) histSection.style.display = "none";
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
  ["filter-category", "sort-by"].forEach(id => {
    document.getElementById(id).addEventListener("change", triggerSearchAndClose);
  });
  [starSlider, returnSlider, reviewsSlider, recommendSlider].forEach(sl => {
    sl.addEventListener("change", () => { if (window.innerWidth <= 900) closeSidebar(); });
  });

  // ── Cross-market view toggle ───────────────────────────────────────────────
  const cmBtn = document.getElementById("view-cross-market");
  if (cmBtn) {
    cmBtn.addEventListener("click", () => {
      const panel    = document.getElementById("cross-market-panel");
      const grid     = document.getElementById("product-grid");
      const pgn      = document.getElementById("pagination");
      const mvPanel  = document.getElementById("movers-panel");
      const isOpen = panel.style.display !== "none";
      if (isOpen) {
        // Back to normal view
        panel.style.display = "none";
        grid.style.display  = "";
        pgn.style.display   = "";
        cmBtn.classList.remove("active");
      } else {
        // Close movers if open
        if (mvPanel && mvPanel.style.display !== "none") {
          mvPanel.style.display = "none";
          document.getElementById("view-movers")?.classList.remove("active");
        }
        panel.style.display = "";
        grid.style.display  = "none";
        pgn.style.display   = "none";
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
const FLAG = { CZ:"🇨🇿", DE:"🇩🇪", FR:"🇫🇷", PL:"🇵🇱", SK:"🇸🇰", US:"🇺🇸", GB:"🇬🇧" };

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
      const markets = g.countries.map(c => (FLAG[c] || c)).join(" ");
      const priceStr = g.price_min_czk != null
        ? `CZK ${Math.round(g.price_min_czk).toLocaleString()}–${Math.round(g.price_max_czk).toLocaleString()}`
        : "";
      const rateStr = g.rate_min != null
        ? `${g.rate_min}–${g.rate_max}% recommend`
        : "";

      // Deduplicate products by name, keep highest-review variant per market
      const byCountry = {};
      for (const p of g.products) {
        const key = p.country;
        if (!byCountry[key] || (p.reviews||0) > (byCountry[key].reviews||0)) {
          byCountry[key] = p;
        }
      }
      const rows = Object.values(byCountry)
        .sort((a,b) => a.country.localeCompare(b.country))
        .map(p => {
          const flag  = FLAG[p.country] || p.country;
          const name  = escHtml(p.name.length > 42 ? p.name.slice(0, 41) + "…" : p.name);
          const rate  = p.rate != null ? `${Math.round(p.rate)}%` : "";
          const price = p.price != null ? `${Math.round(p.price).toLocaleString()} ${p.currency || "CZK"}` : "";
          return `<li class="cm-product-row">
            <span class="cm-product-flag">${flag}</span>
            <span class="cm-product-name" title="${escHtml(p.name)}">${name}</span>
            <span class="cm-product-rate">${rate}</span>
            <span class="cm-product-price">${price}</span>
            <span class="cm-product-source">${escHtml(p.source)}</span>
          </li>`;
        }).join("");

      return `<div class="cm-card">
        <div class="cm-card-header">
          <span class="cm-token">${escHtml(g.token)}</span>
          <span class="cm-markets">${markets} · ${g.n_products} listings</span>
        </div>
        ${priceStr || rateStr ? `<div class="cm-range">
          ${priceStr ? `<span>💰 ${priceStr}</span>` : ""}
          ${rateStr  ? `<span>⭐ ${rateStr}</span>` : ""}
        </div>` : ""}
        <ul class="cm-products">${rows}</ul>
      </div>`;
    }).join("");

  } catch (e) {
    grid.innerHTML = `<div style="grid-column:1/-1;padding:20px;color:#c62828">Error: ${escHtml(e.message)}</div>`;
    if (status) status.textContent = "Failed";
  }
}

// ── Data Health Panel ─────────────────────────────────────────────────────────

const FLAG_MAP = { CZ:"🇨🇿", SK:"🇸🇰", DE:"🇩🇪", FR:"🇫🇷", PL:"🇵🇱", US:"🇺🇸", "??":"🌐" };

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

      const sources = data.sources || [];
      const nOk    = sources.filter(s => s.status === "ok").length;
      const nWarn  = sources.filter(s => s.status === "warn").length;
      const nStale = sources.filter(s => s.status === "stale").length;
      const total  = (data.total_products || 0).toLocaleString();

      summEl.innerHTML = `
        <span class="health-summary-pill health-pill-total">📦 ${total} products</span>
        ${nOk    ? `<span class="health-summary-pill health-pill-ok">✓ ${nOk} fresh</span>` : ""}
        ${nWarn  ? `<span class="health-summary-pill health-pill-warn">⚠ ${nWarn} ageing</span>` : ""}
        ${nStale ? `<span class="health-summary-pill health-pill-stale">✕ ${nStale} stale</span>` : ""}
        <span class="health-summary-pill" style="background:#f5f5f5;color:#888">
          ${data.scheduler_running ? "🟢 Scheduler running" : "🔴 Scheduler stopped"}
        </span>`;

      rowsEl.innerHTML = sources.map(s => {
        const dotClass = `health-dot-${s.status}`;
        const flag     = FLAG_MAP[s.market] || "🌐";
        const age      = s.days_since_ok != null
          ? (s.days_since_ok === 0 ? "today" : `${s.days_since_ok}d ago`)
          : "never scraped";
        const lastOk   = s.last_ok
          ? new Date(s.last_ok).toLocaleDateString("en-GB", {day:"numeric", month:"short", year:"numeric"})
          : "—";
        const errBadge = s.errors_30d > 0
          ? `<div class="health-row-errors">⚠ ${s.errors_30d} error${s.errors_30d > 1?"s":""} in last 30d</div>`
          : "";
        const addedStr = s.total_added || s.total_updated
          ? `+${(s.total_added||0).toLocaleString()} added, ~${(s.total_updated||0).toLocaleString()} updated`
          : "";

        return `
          <div class="health-row">
            <span class="health-dot ${dotClass}">⬤</span>
            <div class="health-row-info">
              <div class="health-row-name">${flag} ${escHtml(s.scraper)}</div>
              <div class="health-row-meta">Last OK: ${lastOk} (${age})${addedStr ? " · " + addedStr : ""}</div>
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
    const panel  = document.getElementById("movers-panel");
    const grid   = document.getElementById("product-grid");
    const pgn    = document.getElementById("pagination");
    const cmPanel = document.getElementById("cross-market-panel");
    const isOpen = panel.style.display !== "none";

    if (isOpen) {
      panel.style.display = "none";
      grid.style.display  = "";
      pgn.style.display   = "";
      moversBtn.classList.remove("active");
    } else {
      // Close cross-market if open
      if (cmPanel && cmPanel.style.display !== "none") {
        cmPanel.style.display = "none";
        document.getElementById("view-cross-market")?.classList.remove("active");
        grid.style.display = "";
        pgn.style.display  = "";
      }
      panel.style.display = "";
      grid.style.display  = "none";
      pgn.style.display   = "none";
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

    const metricLabel = { recommend: "Recommend %", stars: "Stars", price: "Price CZK" }[metric] || metric;

    function renderMoverTable(items, direction) {
      if (!items.length) return `<div style="padding:12px;color:#aaa;font-size:0.85em">No ${direction} data for this period.</div>`;
      return `<table class="movers-table">
        <thead><tr>
          <th>Product</th><th>Source</th><th>Category</th>
          <th>${metricLabel} before</th><th>${metricLabel} after</th><th>Change</th>
        </tr></thead>
        <tbody>${items.map(r => {
          const delta  = r.delta > 0 ? `<span class="mover-up">+${r.delta}</span>` : `<span class="mover-down">${r.delta}</span>`;
          const name   = escHtml((r.name || r.url || "").substring(0, 52));
          const cat    = escHtml(translateCat(r.category || ""));
          const oldVal = r.old_val != null ? r.old_val.toFixed(1) : "—";
          const newVal = r.new_val != null ? r.new_val.toFixed(1) : "—";
          return `<tr>
            <td class="mover-name" title="${escHtml(r.name || '')}">
              ${r.url ? `<a href="${escHtml(r.url)}" target="_blank">${name}</a>` : name}
            </td>
            <td>${escHtml(SOURCE_LABELS[r.source] || r.source || "")}</td>
            <td>${cat}</td>
            <td class="mover-val">${oldVal}</td>
            <td class="mover-val">${newVal}</td>
            <td class="mover-delta">${delta}</td>
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

// ── Product history sparkline in modal ────────────────────────────────────────

async function loadModalHistory(productUrl) {
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

    // Build sparklines for recommend_pct and avg_star_rating
    const recData   = rows.filter(r => r.recommend_pct    != null).map(r => ({ d: r.snapshot_date, v: r.recommend_pct }));
    const starData  = rows.filter(r => r.avg_star_rating   != null).map(r => ({ d: r.snapshot_date, v: r.avg_star_rating }));
    const priceData = rows.filter(r => r.price_czk        != null).map(r => ({ d: r.snapshot_date, v: r.price_czk }));
    const revData   = rows.filter(r => r.review_count     != null).map(r => ({ d: r.snapshot_date, v: r.review_count }));

    const segments = [];
    if (recData.length   >= 2) segments.push({ label: "Recommend %",  data: recData,   color: "#43a047", fmt: v => v.toFixed(1) + "%" });
    if (starData.length  >= 2) segments.push({ label: "Stars",        data: starData,  color: "#f9a825", fmt: v => v.toFixed(2) });
    if (priceData.length >= 2) segments.push({ label: "Price (CZK)",  data: priceData, color: "#1565c0", fmt: v => Math.round(v).toLocaleString() });
    if (revData.length   >= 2) segments.push({ label: "Reviews",      data: revData,   color: "#7b1fa2", fmt: v => Math.round(v).toLocaleString() });

    if (!segments.length) { section.style.display = "none"; return; }

    chart.innerHTML = segments.map(seg => buildSparkline(seg)).join("");

  } catch (e) {
    section.style.display = "none";
  }
}

function buildSparkline({ label, data, color, fmt }) {
  const W = 280, H = 52, PAD = 4;
  const vals  = data.map(d => d.v);
  const dates = data.map(d => d.d);
  const minV  = Math.min(...vals);
  const maxV  = Math.max(...vals);
  const range = maxV - minV || 1;

  const xScale = i => PAD + (i / (data.length - 1)) * (W - PAD * 2);
  const yScale = v => H - PAD - ((v - minV) / range) * (H - PAD * 2);

  const points = data.map((d, i) => `${xScale(i).toFixed(1)},${yScale(d.v).toFixed(1)}`).join(" ");

  // Fill polygon
  const fillPts = [
    `${xScale(0).toFixed(1)},${H}`,
    ...data.map((d, i) => `${xScale(i).toFixed(1)},${yScale(d.v).toFixed(1)}`),
    `${xScale(data.length-1).toFixed(1)},${H}`
  ].join(" ");

  const first = fmt(vals[0]);
  const last  = fmt(vals[vals.length - 1]);
  const diff  = vals[vals.length - 1] - vals[0];
  const diffStr = diff >= 0 ? `+${fmt(diff)}` : fmt(diff);
  const diffColor = diff > 0 ? "#43a047" : diff < 0 ? "#e53935" : "#888";

  return `<div class="sparkline-wrap">
    <div class="sparkline-label">${label}</div>
    <svg class="sparkline-svg" width="${W}" height="${H}" viewBox="0 0 ${W} ${H}">
      <defs>
        <linearGradient id="sg${color.replace('#','')}" x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stop-color="${color}" stop-opacity="0.25"/>
          <stop offset="100%" stop-color="${color}" stop-opacity="0.03"/>
        </linearGradient>
      </defs>
      <polygon points="${fillPts}" fill="url(#sg${color.replace('#','')})" />
      <polyline points="${points}" fill="none" stroke="${color}" stroke-width="1.8"
        stroke-linecap="round" stroke-linejoin="round"/>
      <circle cx="${xScale(data.length-1).toFixed(1)}" cy="${yScale(vals[vals.length-1]).toFixed(1)}"
        r="3" fill="${color}"/>
    </svg>
    <div class="sparkline-footer">
      <span class="sparkline-range">${dates[0]} → ${dates[dates.length-1]}</span>
      <span class="sparkline-delta" style="color:${diffColor}">${diffStr}</span>
      <span class="sparkline-last">${last}</span>
    </div>
  </div>`;
}
