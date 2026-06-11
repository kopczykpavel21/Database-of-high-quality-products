"""
Translate Warentest German category names to English in products.db
Run from the QualityDB folder: python3 translate_categories.py
"""
import sqlite3

TRANSLATIONS = {
    "Akkuschrauber":           "Cordless Drills",
    "Babynahrung (Pre)":       "Baby Formula (Pre)",
    "Backöfen":                "Ovens",
    "Blutdruckmessgeräte":     "Blood Pressure Monitors",
    "Bügeleisen":              "Clothes Irons",
    "Drucker":                 "Printers",
    "E-Bikes":                 "E-Bikes",
    "Elektrozahnbürsten":      "Electric Toothbrushes",
    "Fahrradhelme Erwachsene": "Adult Bicycle Helmets",
    "Fernseher":               "Televisions",
    "Gefriergeräte":           "Freezers",
    "Geschirrspüler":          "Dishwashers",
    "Haartrockner":            "Hair Dryers",
    "Heißluftfritteusen":      "Air Fryers",
    "Kaffeemaschinen":         "Coffee Machines",
    "Kameras":                 "Cameras",
    "Kindersitze":             "Child Car Seats",
    "Kinderwagen":             "Strollers",
    "Klimageräte":             "Air Conditioners",
    "Kopfhörer":               "Headphones",
    "Kühlschränke":            "Refrigerators",
    "Laptops & Notebooks":     "Laptops & Notebooks",
    "Luftreiniger":            "Air Purifiers",
    "Matratzen":               "Mattresses",
    "Mesh-WLAN":               "Mesh WiFi",
    "Mikrowellen":             "Microwaves",
    "Monitore":                "Monitors",
    "Mähroboter":              "Robot Lawn Mowers",
    "Router & Repeater":       "Routers & Repeaters",
    "Saugroboter":             "Robot Vacuums",
    "Smartphones":             "Smartphones",
    "Smartwatches":            "Smartwatches",
    "Sonnencreme":             "Sunscreen",
    "Soundbars":               "Soundbars",
    "Standmixer":              "Blenders",
    "Staubsauger":             "Vacuum Cleaners",
    "Streaming Devices":       "Streaming Devices",
    "Tablets":                 "Tablets",
    "Waschmaschinen":          "Washing Machines",
    "Wäschetrockner":          "Tumble Dryers",
}

conn = sqlite3.connect("products.db")
updated = 0
for de, en in TRANSLATIONS.items():
    cur = conn.execute(
        "UPDATE products SET Category = ? WHERE source = 'warentest' AND Category = ?",
        (en, de)
    )
    if cur.rowcount:
        print(f"  {cur.rowcount:4d}  {de!r} → {en!r}")
        updated += cur.rowcount

conn.commit()
conn.close()
print(f"\nDone: {updated} rows updated.")
