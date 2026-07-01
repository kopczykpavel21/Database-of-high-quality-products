import sqlite3

import server


def _image_url(db_path, name):
    conn = sqlite3.connect(db_path)
    try:
        return conn.execute("SELECT image_url FROM products WHERE Name=?", (name,)).fetchone()[0]
    finally:
        conn.close()


# ── _extract_image_from_html (pure function) ────────────────────────────────

def test_extract_image_from_html_og_image():
    html = '<html><head><meta property="og:image" content="https://example.com/img.jpg"></head></html>'
    assert server._extract_image_from_html(html) == "https://example.com/img.jpg"


def test_extract_image_from_html_json_ld_product():
    html = """
    <script type="application/ld+json">
    {"@type": "Product", "image": "https://example.com/product.jpg"}
    </script>
    """
    assert server._extract_image_from_html(html) == "https://example.com/product.jpg"


def test_extract_image_from_html_coolblue_cdn_fallback():
    html = 'background-image: url(https://image.coolblue.nl/500x500/products/912345)'
    assert server._extract_image_from_html(html) == "https://image.coolblue.nl/500x500/products/912345"


def test_extract_image_from_html_no_match_returns_empty_string():
    assert server._extract_image_from_html("<html><body>nothing here</body></html>") == ""


# ── _populate_alza_images ────────────────────────────────────────────────────

def test_populate_alza_images_from_sku(db):
    n = server._populate_alza_images()
    assert n == 2  # iPhone 15 (SKU=APP0123) + Cheap Tablet X (SKU=CHP001)

    assert _image_url(db, "Apple iPhone 15 128GB") == "https://cdn.alza.cz/ImgW.ashx?fd=f10&cd=APP0123&i=1.jpg"
    assert _image_url(db, "Cheap Tablet X") == "https://cdn.alza.cz/ImgW.ashx?fd=f10&cd=CHP001&i=1.jpg"
    # Dell XPS already has an image and has no SKU -> untouched
    assert _image_url(db, "Dell XPS 13") == "https://cdn.alza.cz/ImgW.ashx?fd=f10&cd=DEL999&i=1.jpg"


def test_populate_alza_images_is_idempotent(db):
    assert server._populate_alza_images() == 2
    assert server._populate_alza_images() == 0  # nothing left to populate


# ── _populate_amazon_images ──────────────────────────────────────────────────

def test_populate_amazon_images_from_asin(db):
    n = server._populate_amazon_images()
    assert n == 1
    assert _image_url(db, "Sony WH-1000XM5") == \
        "https://images-eu.ssl-images-amazon.com/images/P/B09XS7JWHW.01.LZZZZZZZ.jpg"


# ── _populate_coolblue_images ────────────────────────────────────────────────

def test_populate_coolblue_images_from_product_id(db):
    n = server._populate_coolblue_images()
    assert n == 1
    assert _image_url(db, "Samsung Galaxy Buds") == "https://image.coolblue.nl/500x500/products/912345"


# ── _populate_prisjakt_images ────────────────────────────────────────────────

def test_populate_prisjakt_images_from_query_param(db):
    n = server._populate_prisjakt_images()
    assert n == 1
    assert _image_url(db, "LG OLED TV C3") == "https://pricespy-75b8.kxcdn.com/product/standard/800/4567890.jpg"


# ── _populate_pricerunner_images ─────────────────────────────────────────────

def test_populate_pricerunner_images_is_unimplemented_stub(db):
    assert server._populate_pricerunner_images() == 0


# ── _populate_images_from_sibling_products ──────────────────────────────────

def test_populate_images_from_sibling_products(db):
    # Sony WH-1000XM5 (amazon, no image) shares brand="Sony" +
    # NormalizedCategory="Headphones" with Sony WH-1000XM4 (heureka, has an
    # image) -> the XM5 row should inherit the XM4 image.
    n = server._populate_images_from_sibling_products()
    assert n == 1

    xm4_image = _image_url(db, "Sony WH-1000XM4 (refurb)")
    assert _image_url(db, "Sony WH-1000XM5") == xm4_image == "https://cdn.heureka.cz/sony-wh1000xm4.jpg"

    # No sibling with an image shares brand+category with these -> untouched
    assert _image_url(db, "LG OLED TV C3") is None
    assert _image_url(db, "Cheap Tablet X") is None
