"""
Category Normalizer for QualityDB
==================================
Maps the per-source, multi-language Category + MainCategory values to a
canonical two-level English taxonomy stored in two new columns:

    NormalizedCategory   – specific product type  (e.g. "Smartphones")
    NormalizedMainGroup  – broad group             (e.g. "Phones & Tablets")

Run standalone:
    python3 scraper/normalize_categories.py [--dry-run]

Also callable from server.py on startup:
    from scraper.normalize_categories import run_normalization
    run_normalization(conn)
"""

from __future__ import annotations

import re
import os
import sys
import sqlite3
import argparse
import logging

log = logging.getLogger(__name__)

# ── Canonical taxonomy ────────────────────────────────────────────────────────
# Each entry: (NormalizedCategory, NormalizedMainGroup)
# Order matters — first match wins for keyword rules.

# Explicit exact-match overrides  (Category value → canonical)
EXACT: dict[str, tuple[str, str]] = {
    # Phones
    "Mobilní telefony":        ("Smartphones",          "Phones & Tablets"),
    "Chytré telefony":         ("Smartphones",          "Phones & Tablets"),
    "Smartphones":             ("Smartphones",          "Phones & Tablets"),
    "Smartfony":               ("Smartphones",          "Phones & Tablets"),
    "Mobile Phones":           ("Smartphones",          "Phones & Tablets"),
    "Telefony":                ("Smartphones",          "Phones & Tablets"),
    "Telefony komórkowe":      ("Smartphones",          "Phones & Tablets"),
    "Handy":                   ("Smartphones",          "Phones & Tablets"),
    "Smartphones & Handys":    ("Smartphones",          "Phones & Tablets"),
    # Tablets
    "Tablety":                 ("Tablets",              "Phones & Tablets"),
    "Tablets":                 ("Tablets",              "Phones & Tablets"),
    "Tablet":                  ("Tablets",              "Phones & Tablets"),
    "Tablettes":               ("Tablets",              "Phones & Tablets"),
    # Gaming Laptops (must come BEFORE Laptops)
    "Gaming Notebooks":        ("Gaming Laptops",       "Computers"),
    "Gaming Laptops":          ("Gaming Laptops",       "Computers"),
    "Gaming laptop":           ("Gaming Laptops",       "Computers"),
    "Herní notebooky":         ("Gaming Laptops",       "Computers"),
    "Herní notebook":          ("Gaming Laptops",       "Computers"),
    "Gaming-Notebooks":        ("Gaming Laptops",       "Computers"),
    "Gaming-Laptop":           ("Gaming Laptops",       "Computers"),
    "Laptops voor gaming":     ("Gaming Laptops",       "Computers"),
    # Laptops
    "Laptops":                 ("Laptops",              "Computers"),
    "Notebooky":               ("Laptops",              "Computers"),
    "Notebooky a PC":          ("Laptops",              "Computers"),
    "Notebooks":               ("Laptops",              "Computers"),
    # Desktops
    "Počítače":                ("Desktop PCs",          "Computers"),
    "Desktop PCs":             ("Desktop PCs",          "Computers"),
    # Gaming Monitors (must come BEFORE Monitors)
    "Gaming Monitors":         ("Gaming Monitors",      "Computers"),
    "Herní monitory":          ("Gaming Monitors",      "Computers"),
    "Gaming-Monitore":         ("Gaming Monitors",      "Computers"),
    "Gaming monitors":         ("Gaming Monitors",      "Computers"),
    # Monitors
    "Monitors":                ("Monitors",             "Computers"),
    "Monitory":                ("Monitors",             "Computers"),
    "Monitore":                ("Monitors",             "Computers"),
    # Headphones
    "Sluchátka":               ("Headphones",           "Audio"),
    "Headphones":              ("Headphones",           "Audio"),
    "Zvuk a sluchátka":        ("Headphones",           "Audio"),
    "Kopfhörer":               ("Headphones",           "Audio"),
    "Casques audio":           ("Headphones",           "Audio"),
    "Słuchawki":               ("Headphones",           "Audio"),
    "Auriculares":             ("Headphones",           "Audio"),
    "Écouteurs":               ("Headphones",           "Audio"),
    "Gaming Headsets":         ("Gaming Headsets",      "Gaming"),
    # Earbuds
    "Earbuds":                 ("Earbuds",              "Audio"),
    "True Wireless Earbuds":   ("Earbuds",              "Audio"),
    "In-Ear Kopfhörer":        ("Earbuds",              "Audio"),
    # Speakers
    "Reproduktory":            ("Speakers",             "Audio"),
    "Speakers":                ("Speakers",             "Audio"),
    "Lautsprecher":            ("Speakers",             "Audio"),
    "Enceintes":               ("Speakers",             "Audio"),
    "Bluetooth Speakers":      ("Speakers",             "Audio"),
    # Soundbars
    "Soundbars":               ("Soundbars",            "Audio"),
    "Soundbar":                ("Soundbars",            "Audio"),
    # TVs
    "TVs":                     ("TVs",                  "TV & Video"),
    "Televisions":             ("TVs",                  "TV & Video"),
    "Televize":                ("TVs",                  "TV & Video"),
    "Televizory":              ("TVs",                  "TV & Video"),
    "Fernseher":               ("TVs",                  "TV & Video"),
    "Téléviseurs":             ("TVs",                  "TV & Video"),
    "TV & Video":              ("TVs",                  "TV & Video"),
    # Projectors
    "Projectors":              ("Projectors",           "TV & Video"),
    "Projektory":              ("Projectors",           "TV & Video"),
    # Streaming devices
    "Streaming zařízení":      ("Streaming Devices",    "TV & Video"),
    # Smartwatches
    "Smartwatches":            ("Smartwatches",         "Wearables"),
    "Chytré hodinky":          ("Smartwatches",         "Wearables"),
    "Montres connectées":      ("Smartwatches",         "Wearables"),
    "Smartwatch":              ("Smartwatches",         "Wearables"),
    # Fitness trackers
    "Fitness Trackers":        ("Fitness Trackers",     "Wearables"),
    "Fitness náramky":         ("Fitness Trackers",     "Wearables"),
    "Aktivitätstracker":       ("Fitness Trackers",     "Wearables"),
    # Cameras
    "Digital Cameras":             ("Digital Cameras",      "Cameras"),
    "Fotoaparáty":                 ("Digital Cameras",      "Cameras"),
    "Appareils photo":             ("Digital Cameras",      "Cameras"),
    "Kameras":                     ("Digital Cameras",      "Cameras"),
    "Kamera & Foto":               ("Digital Cameras",      "Cameras"),
    "Aparaty fotograficzne":       ("Digital Cameras",      "Cameras"),  # Polish (ceneo)
    "Cyfrowe aparaty fotograficzne": ("Digital Cameras",    "Cameras"),  # Polish long form
    "Lustrzanki i hybrydowe":      ("Digital Cameras",      "Cameras"),  # Polish mirrorless/DSLR
    "Action Cameras":          ("Action Cameras",       "Cameras"),
    "IP Cameras":              ("IP Cameras",           "Cameras"),
    "IP kamery":               ("IP Cameras",           "Cameras"),
    "Camera Accessories":      ("Camera Accessories",   "Cameras"),
    "Webcams":                 ("Webcams",              "Computers"),
    # Air purifiers (Czech/DE)
    "Čističky vzduchu":        ("Air Purifiers",        "Home Appliances"),
    "Luftreiniger":            ("Air Purifiers",        "Home Appliances"),
    # Storage (additional Czech)
    "Pevné disky":             ("HDDs",                 "Storage"),
    "NAS úložiště":            ("NAS Storage",          "Storage"),
    "Externe SSDs & Festplatten": ("External Drives",   "Storage"),
    "Paměťová média":          ("SD Cards",             "Storage"),
    "Paměťové karty":          ("SD Cards",             "Storage"),
    # Fridges/freezers (additional Czech)
    "Chladničky a mrazničky":  ("Refrigerators",        "Home Appliances"),
    "Gefrierschränke":         ("Freezers",             "Home Appliances"),
    "Freezers":                ("Freezers",             "Home Appliances"),
    "Mrazničky":               ("Freezers",             "Home Appliances"),
    # Large appliances (Czech groups)
    "Vytápění a klimatizace":  ("Air Conditioners",     "Home Appliances"),
    "Klimatizace":             ("Air Conditioners",     "Home Appliances"),
    # Networking (additional)
    "Extendery":               ("Networking",           "Networking"),
    "Síťové prvky":            ("Networking",           "Networking"),
    # Cameras (more variants)
    "Videokamery":             ("Video Cameras",        "Cameras"),
    "Akční kamery":            ("Action Cameras",       "Cameras"),
    "Webkamery":               ("Webcams",              "Computers"),
    "Cameras":                 ("Digital Cameras",      "Cameras"),
    # Toys (additional)
    "Deskové hry":             ("Board Games",          "Toys & Games"),
    # Audio (additional)
    "Gramofony":               ("Hi-Fi & Turntables",   "Audio"),
    "Domácí kino":             ("Home Cinema",          "TV & Video"),
    # Gaming (additional)
    "Herní konzole":           ("Gaming Consoles",      "Gaming"),
    "Spielkonsolen":           ("Gaming Consoles",      "Gaming"),
    # Sport
    "Sekačky a péče o trávník":("Lawn Mowers",          "Garden & Outdoors"),
    "Elektrické nářadí":       ("Power Tools",          "Garden & Outdoors"),
    "Cordless Drills":         ("Power Tools",          "Garden & Outdoors"),
    # Kitchen / small appliances catch-all (Czech)
    "Kuchyňské spotřebiče":    ("Kitchen Appliances",   "Home Appliances"),
    "Malé domácí spotřebiče":  ("Kitchen Appliances",   "Home Appliances"),
    "Küche & Haushalt":        ("Kitchen Appliances",   "Home Appliances"),
    # Vacuum cleaners
    "Vacuum Cleaners":         ("Vacuum Cleaners",      "Home Appliances"),
    "Vysavače":                ("Vacuum Cleaners",      "Home Appliances"),
    "Staubsauger":             ("Vacuum Cleaners",      "Home Appliances"),
    "Aspirateurs":             ("Vacuum Cleaners",      "Home Appliances"),
    "Odkurzacze":              ("Vacuum Cleaners",      "Home Appliances"),
    "Robot Vacuums":           ("Robot Vacuums",        "Home Appliances"),
    "Stick Vacuums":           ("Stick Vacuums",        "Home Appliances"),
    "Robotické vysavače":      ("Robot Vacuums",        "Home Appliances"),
    # Washing machines
    "Washing Machines":        ("Washing Machines",     "Home Appliances"),
    "Pračky":                  ("Washing Machines",     "Home Appliances"),
    "Pračky a péče o prádlo":  ("Washing Machines",     "Home Appliances"),
    "Waschmaschinen":          ("Washing Machines",     "Home Appliances"),
    "Lave-linge":              ("Washing Machines",     "Home Appliances"),
    "Pralki":                  ("Washing Machines",     "Home Appliances"),
    # Dryers
    "Tumble Dryers":           ("Dryers",               "Home Appliances"),
    "Sušičky":                 ("Dryers",               "Home Appliances"),
    "Wäschetrockner":          ("Dryers",               "Home Appliances"),
    "Sèche-linge":             ("Dryers",               "Home Appliances"),
    # Dishwashers
    "Dishwashers":             ("Dishwashers",          "Home Appliances"),
    "Myčky nádobí":            ("Dishwashers",          "Home Appliances"),
    "Geschirrspüler":          ("Dishwashers",          "Home Appliances"),
    "Lave-vaisselle":          ("Dishwashers",          "Home Appliances"),
    "Zmywarki":                ("Dishwashers",          "Home Appliances"),
    # Refrigerators
    "Refrigerators":           ("Refrigerators",        "Home Appliances"),
    "Ledničky":                ("Refrigerators",        "Home Appliances"),
    "Kühlschränke":            ("Refrigerators",        "Home Appliances"),
    "Réfrigérateurs":          ("Refrigerators",        "Home Appliances"),
    "Lodówki":                 ("Refrigerators",        "Home Appliances"),
    # Ovens/stoves
    "Sporáky":                 ("Ovens & Stoves",       "Home Appliances"),
    "Vaření a pečení":         ("Ovens & Stoves",       "Home Appliances"),
    "Ovens":                   ("Ovens & Stoves",       "Home Appliances"),
    "Backöfen":                ("Ovens & Stoves",       "Home Appliances"),
    # Microwaves
    "Microwaves":              ("Microwaves",           "Home Appliances"),
    "Mikrovlnky":              ("Microwaves",           "Home Appliances"),
    "Mikrowellen":             ("Microwaves",           "Home Appliances"),
    "Fours micro-ondes":       ("Microwaves",           "Home Appliances"),
    # Coffee machines
    "Coffee Machines":         ("Coffee Machines",      "Home Appliances"),
    "Ekspresy do kawy":        ("Coffee Machines",      "Home Appliances"),
    "Kaffeemaschinen":         ("Coffee Machines",      "Home Appliances"),
    "Machines à café":         ("Coffee Machines",      "Home Appliances"),
    "Kávovary":                ("Coffee Machines",      "Home Appliances"),
    # Air fryers
    "Air Fryers":              ("Air Fryers",           "Home Appliances"),
    "Friteuses à air":         ("Air Fryers",           "Home Appliances"),
    # Blenders
    "Blenders":                ("Blenders & Mixers",    "Home Appliances"),
    "Kitchen Robots":          ("Blenders & Mixers",    "Home Appliances"),
    "Mixers":                  ("Blenders & Mixers",    "Home Appliances"),
    # Kettles
    "Kettles":                 ("Kettles",              "Home Appliances"),
    "Wasserkocher":            ("Kettles",              "Home Appliances"),
    "Bouilloires":             ("Kettles",              "Home Appliances"),
    # Toasters
    "Toasters":                ("Toasters",             "Home Appliances"),
    # Irons
    "Irons":                   ("Irons",                "Home Appliances"),
    "Bügeleisen":              ("Irons",                "Home Appliances"),
    "Fers à repasser":         ("Irons",                "Home Appliances"),
    # Hair care
    "Hair Dryers":             ("Hair Dryers",          "Home Appliances"),
    "Haartrockner":            ("Hair Dryers",          "Home Appliances"),
    "Sèche-cheveux":           ("Hair Dryers",          "Home Appliances"),
    # Personal care
    "Electric Shavers":        ("Electric Shavers",     "Home Appliances"),
    "Electric Toothbrush":     ("Electric Toothbrushes","Home Appliances"),
    "Elektrische Zahnbürsten": ("Electric Toothbrushes","Home Appliances"),
    # Air quality
    "Air Purifiers":           ("Air Purifiers",        "Home Appliances"),
    "Air Conditioners":        ("Air Conditioners",     "Home Appliances"),
    "Klimaanlagen":            ("Air Conditioners",     "Home Appliances"),
    # Printers
    "Printers":                ("Printers",             "Computers"),
    "Tiskárny":                ("Printers",             "Computers"),
    "Drucker":                 ("Printers",             "Computers"),
    "Imprimantes":             ("Printers",             "Computers"),
    "Drukarki":                ("Printers",             "Computers"),
    # Keyboards
    "Keyboards":               ("Keyboards",            "Computers"),
    "Klávesnice":              ("Keyboards",            "Computers"),
    "Tastaturen":              ("Keyboards",            "Computers"),
    "Claviers":                ("Keyboards",            "Computers"),
    # Mice
    "Mice":                    ("Mice",                 "Computers"),
    "Myši":                    ("Mice",                 "Computers"),
    "Mäuse":                   ("Mice",                 "Computers"),
    "Gaming Mice":             ("Gaming Mice",          "Gaming"),
    # Storage
    "SSD":                     ("SSDs",                 "Storage"),
    "HDD":                     ("HDDs",                 "Storage"),
    "Pevné disky a SSD":       ("SSDs & HDDs",          "Storage"),
    "Úložiště":                ("SSDs & HDDs",          "Storage"),
    "Flash disky":             ("Flash Drives",         "Storage"),
    "USB Flash Drives":        ("Flash Drives",         "Storage"),
    "SD Cards":                ("SD Cards",             "Storage"),
    "SD karty":                ("SD Cards",             "Storage"),
    "RAM":                     ("RAM",                  "Computers"),
    "RAM paměti":              ("RAM",                  "Computers"),
    "Externí disky":           ("External Drives",      "Storage"),
    "External Drives":         ("External Drives",      "Storage"),
    # Networking
    "Routers":                 ("Routers",              "Networking"),
    "Routery":                 ("Routers",              "Networking"),
    "Router":                  ("Routers",              "Networking"),
    "Síťové prvky":            ("Networking",           "Networking"),
    "Kabely a rozbočovače":    ("Cables & Accessories", "Accessories"),
    "Kabely a adaptéry":       ("Cables & Accessories", "Accessories"),
    # Smart Home
    "Smart Home":              ("Smart Home",           "Smart Home"),
    "Smart Lighting":          ("Smart Lighting",       "Smart Home"),
    "Chytrá domácnost":        ("Smart Home",           "Smart Home"),
    # PC components
    "PC skříně":               ("PC Cases",             "Computers"),
    "Grafické karty":          ("Graphics Cards",       "Computers"),
    "Chlazení":                ("PC Cooling",           "Computers"),
    "Napájení":                ("Power Supplies",       "Computers"),
    "Základní desky":          ("Motherboards",         "Computers"),
    "Procesory":               ("CPUs",                 "Computers"),
    # Gaming
    "Game Controllers":        ("Game Controllers",     "Gaming"),
    "Herní ovladače":          ("Game Controllers",     "Gaming"),
    "Herní příslušenství":     ("Gaming Accessories",   "Gaming"),
    "Herní sedačky":           ("Gaming Chairs",        "Gaming"),
    "Videospiele":             ("Video Games",          "Gaming"),
    "Video Games":             ("Video Games",          "Gaming"),
    # Toys
    "Hry a hračky":            ("Toys & Games",         "Toys & Games"),
    "Hračky":                  ("Toys & Games",         "Toys & Games"),
    "Panenky":                 ("Dolls",                "Toys & Games"),
    "LEGO a stavebnice":       ("LEGO",                 "Toys & Games"),
    "Deskové a karetní hry":   ("Board Games",          "Toys & Games"),
    # Laptop/phone accessories
    "Příslušenství k notebookům": ("Laptop Accessories","Accessories"),
    "Laptop Accessories":      ("Laptop Accessories",   "Accessories"),
    "Dokovací stanice":        ("Docking Stations",     "Accessories"),
    "Pouzdra a kryty":         ("Phone Cases",          "Accessories"),
    "Ochranné fólie":          ("Screen Protectors",    "Accessories"),
    "Phone Cases":             ("Phone Cases",          "Accessories"),
    "Phone Chargers":          ("Chargers",             "Accessories"),
    "Nabíječky":               ("Chargers",             "Accessories"),
    "Powerbanky":              ("Power Banks",          "Accessories"),
    "Držáky a stojany":        ("Mounts & Stands",      "Accessories"),
    # Microphones
    "Mikrofony":               ("Microphones",          "Audio"),
    "Microphones":             ("Microphones",          "Audio"),
    # Home cinema
    "Domácí kino":             ("Home Cinema",          "TV & Video"),
    # Portable speakers (Polish)
    "Głośniki przenośne":      ("Portable Speakers",    "Audio"),
    "Głośniki":                ("Speakers",             "Audio"),
    # Small appliances (Czech)
    "Malé spotřebiče":         ("Small Appliances",     "Home Appliances"),
    "Malé domácí spotřebiče":  ("Kitchen Appliances",   "Home Appliances"),
    "Domácí spotřebiče":       ("Home Appliances",      "Home Appliances"),
    # Small appliances — specific (Czech)
    "Mixéry a roboty":         ("Blenders & Mixers",    "Home Appliances"),
    "Varné konvice":           ("Kettles",              "Home Appliances"),
    "Toustovače":              ("Toasters",             "Home Appliances"),
    "Fény a stylingové přístroje": ("Hair Dryers",      "Home Appliances"),
    "Žehličky":                ("Irons",                "Home Appliances"),
    "Ventilátory":             ("Fans",                 "Home Appliances"),
    # Gaming accessories (Czech)
    "Závodní příslušenství":   ("Racing Accessories",   "Gaming"),
    "Herní technika":          ("Gaming Accessories",   "Gaming"),
    # German coffee machines
    "Kaffeevollautomaten":     ("Coffee Machines",      "Home Appliances"),
    # Music/audio (Czech)
    "Hudební nástroje":        ("Musical Instruments",  "Audio"),
    "Hudební příslušenství":   ("Music Accessories",    "Audio"),
    "Zvuk a hudba":            ("Music & Sound",        "Audio"),
    "Rádia a Hi-Fi":           ("Hi-Fi & Radio",        "Audio"),
    "Zvukové karty":           ("Sound Cards",          "Computers"),
    # Networking (Czech)
    "Sítě a konektivita":      ("Networking",           "Networking"),
    "Anténní příslušenství":   ("Accessories",          "Accessories"),
    # Others (Czech)
    "Ostatní příslušenství":   ("Accessories",          "Accessories"),
    "Brýle na počítač":        ("Computer Glasses",     "Accessories"),
    # Missing Czech categories (added 2026-05)
    "Ostatní spotřebiče":      ("Other Appliances",     "Home Appliances"),
    "Dětské autosedačky":      ("Car Seats",            "Baby & Kids"),
    "Opalovací krémy":         ("Sunscreens",           "Health & Beauty"),
    "Dětské kočárky":          ("Baby Strollers",       "Baby & Kids"),
    "Holicí strojky":          ("Shavers",              "Health & Beauty"),
    "Dentální hygiena":        ("Dental Care",          "Health & Beauty"),
    "Baterie a nabíječky":     ("Batteries & Chargers", "Accessories"),
    "Zdravotnické pomůcky":    ("Medical Devices",      "Health & Beauty"),
    "Běhání a atletika":       ("Running & Athletics",  "Sports & Outdoor"),
    "Grilování":               ("Grills & BBQ",         "Home & Garden"),
    "Příslušenství":           ("Accessories",          "Accessories"),
    # Misc
    "Software":                ("Software",             "Other"),
    "Sport":                   ("Sports & Outdoor",     "Other"),
    "Kancelářské potřeby":     ("Office Supplies",      "Other"),
    # German PC components (saturn_de)
    "Grafikkarten":            ("Graphics Cards",       "Computers"),
    "Prozessoren":             ("CPUs",                 "Computers"),
    "Arbeitsspeicher":         ("RAM",                  "Computers"),
    "Mainboards":              ("Motherboards",         "Computers"),
    "PC-Gehäuse":              ("PC Cases",             "Computers"),
    "Netzteile":               ("Power Supplies",       "Computers"),
    "CPU Kühler":              ("PC Cooling",           "Computers"),
    # Czech storage (alza/heureka)
    "SSD disky":               ("SSDs",                 "Storage"),
    "Ostatní úložiště":        ("Other Storage",        "Storage"),
    # Czech gaming
    "Hry":                     ("Video Games",          "Gaming"),
    "Controller":              ("Game Controllers",     "Gaming"),
    "Arcade Stick":            ("Game Controllers",     "Gaming"),
    "Letecké pedály":          ("Gaming Accessories",   "Gaming"),
    "Letecké Pedály":          ("Gaming Accessories",   "Gaming"),
    "Letecké Pedály Pro Pc A Xbox": ("Gaming Accessories", "Gaming"),
    "Pedály K Volantu Pro Volanty Logitech G Pro Racing": ("Gaming Accessories", "Gaming"),
    "Nabíjecí Stanice Ipega Pg": ("Gaming Accessories",  "Gaming"),
    "Příslušenství K Vr Brýlím": ("VR Headsets",        "Gaming"),
    # Czech audio/instruments
    "Klasická Kytara":         ("Musical Instruments",  "Audio"),
    "Akustická Kytara":        ("Musical Instruments",  "Audio"),
    "Elektronické Bicí":       ("Musical Instruments",  "Audio"),
    "Midi Kontroler":          ("Musical Instruments",  "Audio"),
    "Midi Kontroler Třetí Generace": ("Musical Instruments", "Audio"),
    "Midi Kontroler Pad Kontroler":  ("Musical Instruments", "Audio"),
    "Midi Kontroler Obsahuje Software Ableton Live Lite": ("Musical Instruments", "Audio"),
    "Klička Na Navíjení Strun": ("Musical Instruments", "Audio"),
    "Okarína":                 ("Musical Instruments",  "Audio"),
    "Stojan Na Ukulele":       ("Musical Instruments",  "Audio"),
    "Nářadí Pro Hudební Nástroje": ("Musical Instruments", "Audio"),
    "Čistič Na Vinylové Desky": ("Hi-Fi & Turntables",  "Audio"),
    "Discman":                 ("Hi-Fi & Turntables",   "Audio"),
    "Radiobudík":              ("Radios",               "Audio"),
    # Czech earphones/earbuds
    "Špunty Do Uší Vhodné Do Letadla": ("Earbuds",      "Audio"),
    "Špunty Do Uší Pro Hudebníky":     ("Earbuds",      "Audio"),
    "Špunty Do Uší Na Spaní Navržené Pro Maximální Pohodlí": ("Earbuds", "Audio"),
    "Špunty Do Uší Zabránění Proniknutí Vody Do Uší": ("Earbuds", "Audio"),
    "Špunty Do Uší Vhodné Pro Celodenní Nošení":       ("Earbuds", "Audio"),
    "Špunty Do Uší Vhodné Na Spaní":                   ("Earbuds", "Audio"),
    "Špunty Do Uší":           ("Earbuds",              "Audio"),
    # Czech home cinema/AV
    "Domácí Kino 5.1 Zvukový Systém": ("Home Cinema",  "TV & Video"),
    "Anténní Zesilovač Pro Pozemní Analogový A Digitální Příjem Tv/Fm Signálů": ("TV Accessories", "Accessories"),
    "Anténní Zesilovač 1 Vstupy/2 Výstup": ("TV Accessories", "Accessories"),
    "Anténní Zesilovač Set 40 Db Anténního Zesilovače Evercon Am": ("TV Accessories", "Accessories"),
    "Anténní Rozbočovač 1X Koaxiální Zásuvka (F)": ("TV Accessories", "Accessories"),
    "Slučovač Anténní":        ("TV Accessories",       "Accessories"),
    "Dvd Mechanika Sata":      ("Computers",            "Computers"),
    "Dvd Mechanika Černá":     ("Computers",            "Computers"),
    # Czech accessories
    "Cables & Hubs":           ("Cables & Accessories", "Accessories"),
    "Zástrčka":                ("Cables & Accessories", "Accessories"),
    "Spojka":                  ("Cables & Accessories", "Accessories"),
    "Konektor Usb":            ("Cables & Accessories", "Accessories"),
    "Konektor Micro Usb":      ("Cables & Accessories", "Accessories"),
    "Konektor Robustní":       ("Cables & Accessories", "Accessories"),
    "Záslepka":                ("Cables & Accessories", "Accessories"),
    "1M":                      ("Cables & Accessories", "Accessories"),
    "Filtr":                   ("Accessories",          "Accessories"),
    "Prachový Filtr Černý":    ("Accessories",          "Accessories"),
    "Nabíječka":               ("Chargers",             "Accessories"),
    "Nabíjecí Stanice":        ("Chargers",             "Accessories"),
    "Nabíjecí Baterie Pro Ovladač Dualshock 3": ("Gaming Accessories", "Gaming"),
    "Baterie Kit":             ("Batteries & Chargers", "Accessories"),
    "Powerbanka 10000 Mah":    ("Power Banks",          "Accessories"),
    "Držák":                   ("Mounts & Stands",      "Accessories"),
    "Klip Na Brýle Na Brýle":  ("Accessories",          "Accessories"),
    "Peněženka Na Hesla Offline": ("Accessories",       "Accessories"),
    "Peněženka Na Hesla Offline Hardwarová Peněženka": ("Accessories", "Accessories"),
    "Hardware Peněženka Šifrovací": ("Accessories",     "Accessories"),
    "Arduino":                 ("Computers",            "Computers"),
    "Záznamové Zařízení Rozlišení Až 1080P/60Fps": ("Action Cameras", "Cameras"),
    "Záznamové Zařízení Pro Záznam": ("Action Cameras", "Cameras"),
    # Czech appliances/kitchen
    "Hrnek Materiál Keramika": ("Kitchen Appliances",   "Home Appliances"),
    "Parní Mop Určen K Vytírání Podlah A Čištění Oken": ("Steam Mops", "Home Appliances"),
    "Parní Mop Určen K Vytírání Podlah": ("Steam Mops", "Home Appliances"),
    "Polštář":                 ("Home & Garden",        "Home & Garden"),
    "Vozík Skládací Vozík S Výsuvnou Rukojetí S Aretací": ("Garden & Outdoors", "Garden & Outdoors"),
    "Láhev Na Pití":           ("Accessories",          "Accessories"),
    "Nástrojová Kosmetika":    ("Health & Beauty",      "Health & Beauty"),
    # Czech misc
    "Nezařazeno":              ("Other",                "Other"),
    "Adventní Kalendář":       ("Toys & Games",         "Toys & Games"),
    # German specialized (catch extra patterns)
    "Chromebooks im Test":     ("Laptops",              "Computers"),
    "Gaming maus":             ("Gaming Mice",          "Gaming"),
    "SSD disky":               ("SSDs",                 "Storage"),
    "E-Bikes":                 ("E-Bikes",              "Sports & Outdoor"),
    "E Scooter":               ("E-Scooters",           "Sports & Outdoor"),
    "Outdoor a turistika":     ("Outdoor & Hiking",     "Sports & Outdoor"),
    "Cyklistika":              ("Cycling",              "Sports & Outdoor"),
    "Zavazadla a kufry":       ("Luggage",              "Accessories"),
    "Drony":                   ("Drones",               "Cameras"),
}

# Keyword-based fallback rules (checked on lowercased Category + MainCategory)
# Each tuple: (keyword_pattern, NormalizedCategory, NormalizedMainGroup)
KEYWORD_RULES: list[tuple[str, str, str]] = [
    # Phones — must run before tablets (keyword order matters)
    (r"smartphone|mobile.?phone|mobilní.?tel|chytré?.?tel|handy|telefon.?kom",
     "Smartphones", "Phones & Tablets"),
    # Tablets — use explicit language forms to avoid false-positive on "Telefony a tablety"
    (r"\btablets?\b|\btablety\b|\btablettes?\b",
     "Tablets", "Phones & Tablets"),
    # Gaming Laptops (BEFORE generic laptop rule)
    (r"gaming.{0,4}(notebook|laptop)|herní.{0,4}notebook",
     "Gaming Laptops", "Computers"),
    # Laptops
    (r"laptop|notebook",
     "Laptops", "Computers"),
    # Gaming Monitors (BEFORE generic monitor rule)
    (r"gaming.{0,4}monitor|herní.{0,4}monitor",
     "Gaming Monitors", "Computers"),
    # Monitors
    (r"monitor\b",
     "Monitors", "Computers"),
    # Headphones (broad — must come before earbuds/gaming-headset rules)
    (r"sluchátk|headphone|kopfhörer|écouteur|casque|słuchawk|auricular",
     "Headphones", "Audio"),
    # Earbuds
    (r"earbud|in.?ear|true.?wireless",
     "Earbuds", "Audio"),
    # Gaming headsets
    (r"gaming.?headset",
     "Gaming Headsets", "Gaming"),
    # Speakers
    (r"reproduktor|speaker|lautsprecher|enceinte\b|bluetooth.?speak",
     "Speakers", "Audio"),
    # Soundbars
    (r"soundbar",
     "Soundbars", "Audio"),
    # TVs
    (r"\btv\b|televis|fernseher|téléviseur",
     "TVs", "TV & Video"),
    # Smartwatches
    (r"smartwatch|chytré.?hodin|montre.?connect",
     "Smartwatches", "Wearables"),
    # Fitness trackers
    (r"fitness.?track|aktivitätstracker|band.*fitness",
     "Fitness Trackers", "Wearables"),
    # Robot vacuums
    (r"robot.?vac|robotick.*vysav|saugroboter",
     "Robot Vacuums", "Home Appliances"),
    # Vacuum cleaners
    (r"vacuum|vysavač|staubsauger|aspirateur|odkurzacz",
     "Vacuum Cleaners", "Home Appliances"),
    # Washing machines
    (r"washing.?mach|pračk|waschmasch|lave.?linge|pralki",
     "Washing Machines", "Home Appliances"),
    # Dryers
    (r"tumble.?dry|sušičk|wäschetrock|sèche.?linge",
     "Dryers", "Home Appliances"),
    # Dishwashers
    (r"dishwash|myčk|geschirrspül|lave.?vaissel|zmywark",
     "Dishwashers", "Home Appliances"),
    # Refrigerators
    (r"refrigerat|ledničk|kühlschrank|réfrigérat|lodówk",
     "Refrigerators", "Home Appliances"),
    # Coffee
    (r"coffee|espresso|kávov|kaffeemasch|expres.?do.?kaw",
     "Coffee Machines", "Home Appliances"),
    # Microwaves
    (r"microwave|mikrovln|mikrowell|micro.?ondes",
     "Microwaves", "Home Appliances"),
    # Air fryers
    (r"air.?fry|heißluft",
     "Air Fryers", "Home Appliances"),
    # Blenders
    (r"blend|kitchen.?robot|food.?process|mixér",
     "Blenders & Mixers", "Home Appliances"),
    # Kettles
    (r"kettle|wasserkocher|bouilloir",
     "Kettles", "Home Appliances"),
    # Toasters
    (r"toast",
     "Toasters", "Home Appliances"),
    # Irons
    (r"\biron\b|bügeleisen|fer.?à.?repasser",
     "Irons", "Home Appliances"),
    # Hair care
    (r"hair.?dry|haartrockner|sèche.?cheveux",
     "Hair Dryers", "Home Appliances"),
    # Shavers
    (r"shaver|rasier|rasoir",
     "Electric Shavers", "Home Appliances"),
    # Toothbrush
    (r"toothbrush|zahnbürste|brosse.?à.?dent",
     "Electric Toothbrushes", "Home Appliances"),
    # Air purifiers
    (r"air.?purif|luftreini|purificat",
     "Air Purifiers", "Home Appliances"),
    # AC
    (r"air.?condition|klimaanlag|climatiseur",
     "Air Conditioners", "Home Appliances"),
    # Printers
    (r"print|tiskárn|drucker|imprimant|drukark",
     "Printers", "Computers"),
    # Cameras
    (r"camera|fotoaparát|appareil.?photo",
     "Digital Cameras", "Cameras"),
    # Keyboards
    (r"keyboard|klávesnic|tastatur|clavier",
     "Keyboards", "Computers"),
    # Mice
    (r"\bmice\b|\bmouse\b|\bmyši\b|gaming.?mice",
     "Mice", "Computers"),
    # Storage
    (r"\bssd\b",   "SSDs",         "Storage"),
    (r"\bhdd\b",   "HDDs",         "Storage"),
    (r"flash.?disk|usb.?flash|flash.?drive",
     "Flash Drives", "Storage"),
    # RAM
    (r"\bram\b|\bpaměti\b",
     "RAM", "Computers"),
    # Routers
    (r"router|síťov|netzwerk",
     "Routers", "Networking"),
    # Smart home
    (r"smart.?home|chytré.?dom|smart.?light|smarthome",
     "Smart Home", "Smart Home"),
    # Game controllers
    (r"game.?control|herní.?ovladač|gamepad",
     "Game Controllers", "Gaming"),
    # Toys
    (r"hračk|toy|spielzeug|jouet",
     "Toys & Games", "Toys & Games"),
    # LEGO
    (r"lego",
     "LEGO", "Toys & Games"),
    # Laptop accessories
    (r"notebook.*příslušen|laptop.?access|zubehör.*laptop",
     "Laptop Accessories", "Accessories"),
    # Phone accessories
    (r"pouzdra|phone.?case|ochranná.?fólie|screen.?protect",
     "Phone Cases", "Accessories"),
    # Microphones
    (r"mikrofon|microphone",
     "Microphones", "Audio"),
    # Streaming
    (r"streaming",
     "Streaming Devices", "TV & Video"),
    # Projectors
    (r"projector|projektor",
     "Projectors", "TV & Video"),
    # Webcams
    (r"webcam",
     "Webcams", "Computers"),
    # PC components
    (r"grafick|graphics.?card|gpu\b|grafikkarte",
     "Graphics Cards", "Computers"),
    (r"pc.?skříně|pc.?case",
     "PC Cases", "Computers"),
    (r"chlazen|cooling|cpu.?cool",
     "PC Cooling", "Computers"),
    # Chromebooks
    (r"chromebook",
     "Laptops", "Computers"),
    # E-Bikes / E-Scooters
    (r"\be-?bikes?\b|pedelec|elektrofahrrad|e-?kolo\b",
     "E-Bikes", "Sports & Outdoor"),
    (r"\be-?scooter|elektroscooter|elektrická koloběžka",
     "E-Scooters", "Sports & Outdoor"),
    # Outdoor / cycling
    (r"outdoor.?turistik|turistika\b|hiking\b",
     "Outdoor & Hiking", "Sports & Outdoor"),
    (r"\bcyklistik|\bcycling\b",
     "Cycling", "Sports & Outdoor"),
    # Luggage
    (r"zavazadl|kufry|luggage|suitcase",
     "Luggage", "Accessories"),
    # Drones
    (r"\bdroh?ne?n?\b|\bdrony\b|\bdrón\b",
     "Drones", "Cameras"),
    # Gaming Mice
    (r"gaming.?maus|gaming.?m(?:ice|yš|äuse)\b",
     "Gaming Mice", "Gaming"),
    # Musical Instruments keywords
    (r"\bgitar(?:re|a)\b|\bkytara\b|\bguitar\b",
     "Musical Instruments", "Audio"),
    (r"midi.?kontrol|midi.?control",
     "Musical Instruments", "Audio"),
    # Ear plugs → Earbuds
    (r"špunty.?do.?uší|ear.?plug|earplugs",
     "Earbuds", "Audio"),
    # Home cinema (broader)
    (r"domácí.?kino|heimkino|home.?cinema",
     "Home Cinema", "TV & Video"),
    # Radios
    (r"radiobudík|digital.*radio|dab.?radio|internet.*radio",
     "Radios", "Audio"),
    # Steam mops
    (r"parní.?mop|steam.?mop",
     "Steam Mops", "Home Appliances"),
]

_compiled = [(re.compile(pat, re.IGNORECASE), nc, mg) for pat, nc, mg in KEYWORD_RULES]


def normalize(category: str | None, main_category: str | None) -> tuple[str, str]:
    """Return (NormalizedCategory, NormalizedMainGroup) for given raw values."""
    cat  = (category or "").strip()
    main = (main_category or "").strip()

    # 1. Exact match on Category
    if cat in EXACT:
        return EXACT[cat]

    # 2. Keyword rules on combined text
    combined = f"{cat} {main}"
    for pattern, nc, mg in _compiled:
        if pattern.search(combined):
            return nc, mg

    # 3. Partial exact match on MainCategory fallback
    if main in EXACT:
        return EXACT[main]

    # 4. Keep original Category as-is, assign a sensible group
    if cat:
        return cat, main or "Other"

    return "Other", "Other"


# ── DB operations ─────────────────────────────────────────────────────────────

def ensure_columns(conn: sqlite3.Connection) -> None:
    cols = {r[1] for r in conn.execute("PRAGMA table_info(products)").fetchall()}
    if "NormalizedCategory" not in cols:
        conn.execute("ALTER TABLE products ADD COLUMN NormalizedCategory TEXT")
        log.info("Added NormalizedCategory column")
    if "NormalizedMainGroup" not in cols:
        conn.execute("ALTER TABLE products ADD COLUMN NormalizedMainGroup TEXT")
        log.info("Added NormalizedMainGroup column")
    conn.commit()


def _build_sql_case(when_col: str = "Category") -> tuple[str, str]:
    """
    Build two SQL CASE WHEN expressions for NormalizedCategory and NormalizedMainGroup
    from the EXACT map.  Returns (nc_case_sql, mg_case_sql).
    """
    nc_parts = []
    mg_parts = []
    for raw, (nc, mg) in EXACT.items():
        escaped = raw.replace("'", "''")
        nc_parts.append(f"    WHEN {when_col} = '{escaped}' THEN '{nc.replace(chr(39), chr(39)*2)}'")
        mg_parts.append(f"    WHEN {when_col} = '{escaped}' THEN '{mg.replace(chr(39), chr(39)*2)}'")
    nc_sql = "CASE\n" + "\n".join(nc_parts) + "\n    ELSE NULL\nEND"
    mg_sql = "CASE\n" + "\n".join(mg_parts) + "\n    ELSE NULL\nEND"
    return nc_sql, mg_sql


# LIKE-based keyword rules for SQL (ordered; first match wins via multiple UPDATEs)
# Each tuple: (like_pattern_on_combined, NormalizedCategory, NormalizedMainGroup)
_SQL_KEYWORD_RULES: list[tuple[str, str, str]] = [
    # Phones first
    ("%smartphone%",         "Smartphones",          "Phones & Tablets"),
    ("%mobile%phone%",       "Smartphones",          "Phones & Tablets"),
    ("%mobiln%tel%",         "Smartphones",          "Phones & Tablets"),
    ("%chytré%tel%",         "Smartphones",          "Phones & Tablets"),
    ("%handy%",              "Smartphones",          "Phones & Tablets"),
    # Tablets (explicit forms only — avoid matching "Telefony a tablety")
    ("% tablets %",          "Tablets",              "Phones & Tablets"),
    ("% tablets",            "Tablets",              "Phones & Tablets"),
    ("tablets %",            "Tablets",              "Phones & Tablets"),
    ("%tablety%",            "Tablets",              "Phones & Tablets"),
    ("%tablettes%",          "Tablets",              "Phones & Tablets"),
    # Gaming Laptops (BEFORE generic laptop rules)
    ("%gaming%laptop%",      "Gaming Laptops",       "Computers"),
    ("%gaming%notebook%",    "Gaming Laptops",       "Computers"),
    ("%herní%notebook%",     "Gaming Laptops",       "Computers"),
    # Laptops
    ("%laptop%",             "Laptops",              "Computers"),
    ("%notebook%",           "Laptops",              "Computers"),
    # Gaming Monitors (BEFORE generic monitor rule)
    ("%gaming%monitor%",     "Gaming Monitors",      "Computers"),
    ("%herní%monitor%",      "Gaming Monitors",      "Computers"),
    # Monitors
    ("%monitor%",            "Monitors",             "Computers"),
    # Headphones
    ("%sluchátk%",           "Headphones",           "Audio"),
    ("%headphone%",          "Headphones",           "Audio"),
    ("%kopfhörer%",          "Headphones",           "Audio"),
    ("%écouteur%",           "Headphones",           "Audio"),
    ("%słuchawk%",           "Headphones",           "Audio"),
    ("%gaming headset%",     "Gaming Headsets",      "Gaming"),
    # Earbuds
    ("%earbud%",             "Earbuds",              "Audio"),
    ("%in-ear%",             "Earbuds",              "Audio"),
    # Speakers
    ("%reproduktor%",        "Speakers",             "Audio"),
    ("%speaker%",            "Speakers",             "Audio"),
    ("%lautsprecher%",       "Speakers",             "Audio"),
    ("%enceinte%",           "Speakers",             "Audio"),
    # Soundbars
    ("%soundbar%",           "Soundbars",            "Audio"),
    # TVs
    ("% tv %",               "TVs",                  "TV & Video"),
    ("% tv",                 "TVs",                  "TV & Video"),
    ("%televisi%",           "TVs",                  "TV & Video"),
    ("%televize%",           "TVs",                  "TV & Video"),
    ("%televizor%",          "TVs",                  "TV & Video"),
    ("%fernseher%",          "TVs",                  "TV & Video"),
    # Smartwatches
    ("%smartwatch%",         "Smartwatches",         "Wearables"),
    ("%chytré hodin%",       "Smartwatches",         "Wearables"),
    # Fitness trackers
    ("%fitness track%",      "Fitness Trackers",     "Wearables"),
    ("%fitness náramk%",     "Fitness Trackers",     "Wearables"),
    # Cameras
    ("%digital camera%",     "Digital Cameras",      "Cameras"),
    ("%fotoaparát%",         "Digital Cameras",      "Cameras"),
    ("%action camera%",      "Action Cameras",       "Cameras"),
    ("%ip kamer%",           "IP Cameras",           "Cameras"),
    ("%ip camera%",          "IP Cameras",           "Cameras"),
    ("%webcam%",             "Webcams",              "Computers"),
    # Vacuums
    ("%robot vac%",          "Robot Vacuums",        "Home Appliances"),
    ("%robotick% vysav%",    "Robot Vacuums",        "Home Appliances"),
    ("%vacuum%",             "Vacuum Cleaners",      "Home Appliances"),
    ("%vysavač%",            "Vacuum Cleaners",      "Home Appliances"),
    ("%staubsauger%",        "Vacuum Cleaners",      "Home Appliances"),
    ("%odkurzacz%",          "Vacuum Cleaners",      "Home Appliances"),
    # Washing
    ("%washing machine%",    "Washing Machines",     "Home Appliances"),
    ("%pračk%",              "Washing Machines",     "Home Appliances"),
    ("%waschmasch%",         "Washing Machines",     "Home Appliances"),
    # Dryers
    ("%tumble dry%",         "Dryers",               "Home Appliances"),
    ("%sušičk%",             "Dryers",               "Home Appliances"),
    # Dishwashers
    ("%dishwash%",           "Dishwashers",          "Home Appliances"),
    ("%myčk%",               "Dishwashers",          "Home Appliances"),
    ("%geschirrspül%",       "Dishwashers",          "Home Appliances"),
    # Fridges
    ("%refrigerat%",         "Refrigerators",        "Home Appliances"),
    ("%ledničk%",            "Refrigerators",        "Home Appliances"),
    ("%kühlschrank%",        "Refrigerators",        "Home Appliances"),
    # Coffee
    ("%coffee%",             "Coffee Machines",      "Home Appliances"),
    ("%kávov%",              "Coffee Machines",      "Home Appliances"),
    ("%espresso%",           "Coffee Machines",      "Home Appliances"),
    ("%kaffeemasch%",        "Coffee Machines",      "Home Appliances"),
    # Air fryers
    ("%air fry%",            "Air Fryers",           "Home Appliances"),
    # Kitchen appliances
    ("%kuchyňské spotřebiče%","Kitchen Appliances",  "Home Appliances"),
    ("%küche%haushalt%",     "Kitchen Appliances",   "Home Appliances"),
    # Microwaves
    ("%microwave%",          "Microwaves",           "Home Appliances"),
    ("%mikrovln%",           "Microwaves",           "Home Appliances"),
    # Air purifiers
    ("%air purif%",          "Air Purifiers",        "Home Appliances"),
    ("%čističky vzduchu%",   "Air Purifiers",        "Home Appliances"),
    # Printers
    ("%printer%",            "Printers",             "Computers"),
    ("%tiskárn%",            "Printers",             "Computers"),
    ("%drucker%",            "Printers",             "Computers"),
    # Keyboards
    ("%keyboard%",           "Keyboards",            "Computers"),
    ("%klávesnic%",          "Keyboards",            "Computers"),
    # Mice
    ("% mice %",             "Mice",                 "Computers"),
    ("% mice",               "Mice",                 "Computers"),
    ("% mouse %",            "Mice",                 "Computers"),
    ("%gaming mice%",        "Gaming Mice",          "Gaming"),
    # Routers
    ("%router%",             "Routers",              "Networking"),
    ("%síťov%",              "Networking",           "Networking"),
    # Smart home
    ("%smart home%",         "Smart Home",           "Smart Home"),
    ("%smart lighting%",     "Smart Lighting",       "Smart Home"),
    ("%chytré dom%",         "Smart Home",           "Smart Home"),
    # Gaming
    ("%game control%",       "Game Controllers",     "Gaming"),
    ("%herní ovladač%",      "Game Controllers",     "Gaming"),
    # Toys
    ("%lego%",               "LEGO",                 "Toys & Games"),
    ("%hračk%",              "Toys & Games",         "Toys & Games"),
    ("%toy%",                "Toys & Games",         "Toys & Games"),
    # Microphones
    ("%mikrofon%",           "Microphones",          "Audio"),
    ("%microphone%",         "Microphones",          "Audio"),
    # Streaming
    ("%streaming%",          "Streaming Devices",    "TV & Video"),
    # Projectors
    ("%projector%",          "Projectors",           "TV & Video"),
    ("%projektor%",          "Projectors",           "TV & Video"),
    ("%beamer%",             "Projectors",           "TV & Video"),
    # Graphics cards (German)
    ("%grafikkarte%",        "Graphics Cards",       "Computers"),
    # Chromebooks
    ("%chromebook%",         "Laptops",              "Computers"),
    # E-Bikes / E-Scooters
    ("%e-bike%",             "E-Bikes",              "Sports & Outdoor"),
    ("%e-scooter%",          "E-Scooters",           "Sports & Outdoor"),
    # Drones
    ("%drohne%",             "Drones",               "Cameras"),
    ("%drony%",              "Drones",               "Cameras"),
    # Gaming mice (German)
    ("%gaming maus%",        "Gaming Mice",          "Gaming"),
    # Musical Instruments
    ("%kytara%",             "Musical Instruments",  "Audio"),
    ("%gitarre%",            "Musical Instruments",  "Audio"),
    ("%midi kontrol%",       "Musical Instruments",  "Audio"),
    # Ear plugs → Earbuds
    ("%špunty do uší%",      "Earbuds",              "Audio"),
    # Home cinema
    ("%domácí kino%",        "Home Cinema",          "TV & Video"),
    # Radios
    ("%digitalradio%",       "Radios",               "Audio"),
    ("%radiobudík%",         "Radios",               "Audio"),
    # Steam mops
    ("%parní mop%",          "Steam Mops",           "Home Appliances"),
    # Czech storage
    ("%ssd disk%",           "SSDs",                 "Storage"),
    # Czech games
    ("hry %",                "Video Games",          "Gaming"),
    ("% hry",                "Video Games",          "Gaming"),
]


def run_normalization(conn: sqlite3.Connection, force: bool = False) -> int:
    """
    Populate NormalizedCategory / NormalizedMainGroup for all products using
    pure SQL (CASE WHEN + LIKE) for speed — no row-by-row Python iteration.
    Returns number of rows updated.
    """
    ensure_columns(conn)

    scope = "" if force else "WHERE NormalizedCategory IS NULL OR NormalizedCategory = ''"

    # Count rows to process
    n_todo = conn.execute(f"SELECT COUNT(*) FROM products {scope}").fetchone()[0]
    if n_todo == 0:
        log.info("All rows already normalized — nothing to do")
        return 0

    log.info(f"Normalizing {n_todo} rows …")

    # ── Step 1: exact Category matches (very fast CASE WHEN) ─────────────────
    nc_case, mg_case = _build_sql_case("Category")
    scope_cond = ("WHERE NormalizedCategory IS NULL OR NormalizedCategory = ''"
                  if not force else "WHERE 1=1")
    conn.execute(f"""
        UPDATE products SET
            NormalizedCategory = {nc_case},
            NormalizedMainGroup = {mg_case}
        {scope_cond}
    """)
    log.info("  Exact-match pass done")

    # ── Step 2: LIKE keyword fallback for still-unmatched rows ───────────────
    for like_pat, nc, mg in _SQL_KEYWORD_RULES:
        nc_esc = nc.replace("'", "''")
        mg_esc = mg.replace("'", "''")
        pat_esc = like_pat.replace("'", "''")
        conn.execute(f"""
            UPDATE products SET NormalizedCategory='{nc_esc}', NormalizedMainGroup='{mg_esc}'
            WHERE (NormalizedCategory IS NULL OR NormalizedCategory = '')
              AND LOWER(COALESCE(Category,'') || ' ' || COALESCE(MainCategory,'')) LIKE '{pat_esc}'
        """)

    log.info("  Keyword-LIKE pass done")

    # ── Step 3: remaining rows — use original Category as fallback ───────────
    conn.execute("""
        UPDATE products SET
            NormalizedCategory  = COALESCE(NULLIF(TRIM(Category),''), 'Other'),
            NormalizedMainGroup = COALESCE(NULLIF(TRIM(MainCategory),''), 'Other')
        WHERE NormalizedCategory IS NULL OR NormalizedCategory = ''
    """)
    log.info("  Fallback pass done")

    # ── Step 4: consistency sweep — fix NormalizedMainGroup for all known categories
    # This corrects fallback rows whose NormalizedMainGroup ended up as a Czech string.
    # Build mapping: NormalizedCategory → NormalizedMainGroup from EXACT dict.
    cat_to_group: dict[str, str] = {}
    for _raw, (nc, mg) in EXACT.items():
        cat_to_group[nc] = mg
    # Also cover extra entries from this function's own EXACT additions
    extra = {
        "Smartphones": "Phones & Tablets", "Tablets": "Phones & Tablets",
        "Laptops": "Computers", "Desktop PCs": "Computers", "Monitors": "Computers",
        "Headphones": "Audio", "Earbuds": "Audio", "Speakers": "Audio",
        "Soundbars": "Audio", "Microphones": "Audio", "Hi-Fi & Turntables": "Audio",
        "TVs": "TV & Video", "Smartwatches": "Wearables", "Fitness Trackers": "Wearables",
        "Digital Cameras": "Cameras", "Action Cameras": "Cameras", "IP Cameras": "Cameras",
        "Video Cameras": "Cameras", "Camera Accessories": "Cameras", "Webcams": "Computers",
        "Vacuum Cleaners": "Home Appliances", "Robot Vacuums": "Home Appliances",
        "Washing Machines": "Home Appliances", "Dryers": "Home Appliances",
        "Dishwashers": "Home Appliances", "Refrigerators": "Home Appliances",
        "Freezers": "Home Appliances", "Coffee Machines": "Home Appliances",
        "Microwaves": "Home Appliances", "Air Fryers": "Home Appliances",
        "Blenders & Mixers": "Home Appliances", "Kettles": "Home Appliances",
        "Toasters": "Home Appliances", "Irons": "Home Appliances",
        "Hair Dryers": "Home Appliances", "Electric Shavers": "Home Appliances",
        "Electric Toothbrushes": "Home Appliances", "Air Purifiers": "Home Appliances",
        "Air Conditioners": "Home Appliances", "Kitchen Appliances": "Home Appliances",
        "Ovens & Stoves": "Home Appliances", "Stick Vacuums": "Home Appliances",
        "Printers": "Computers", "Keyboards": "Computers", "Mice": "Computers",
        "Gaming Mice": "Gaming", "RAM": "Computers", "PC Cases": "Computers",
        "PC Cooling": "Computers", "Graphics Cards": "Computers",
        "Power Supplies": "Computers", "Motherboards": "Computers", "CPUs": "Computers",
        "SSDs": "Storage", "HDDs": "Storage", "SSDs & HDDs": "Storage",
        "External Drives": "Storage", "Flash Drives": "Storage", "SD Cards": "Storage",
        "RAM": "Computers", "NAS Storage": "Storage",
        "Routers": "Networking", "Networking": "Networking",
        "Smart Home": "Smart Home", "Smart Lighting": "Smart Home",
        "Game Controllers": "Gaming", "Gaming Headsets": "Gaming",
        "Gaming Accessories": "Gaming", "Gaming Chairs": "Gaming",
        "Video Games": "Gaming", "Gaming Consoles": "Gaming",
        "Toys & Games": "Toys & Games", "LEGO": "Toys & Games",
        "Dolls": "Toys & Games", "Board Games": "Toys & Games",
        "Streaming Devices": "TV & Video", "Projectors": "TV & Video",
        "Home Cinema": "TV & Video",
        "Laptop Accessories": "Accessories", "Phone Cases": "Accessories",
        "Screen Protectors": "Accessories", "Chargers": "Accessories",
        "Power Banks": "Accessories", "Mounts & Stands": "Accessories",
        "Docking Stations": "Accessories", "Cables & Accessories": "Accessories",
        "Power Tools": "Garden & Outdoors", "Lawn Mowers": "Garden & Outdoors",
        "E-Bikes": "Sports & Outdoor", "E-Scooters": "Sports & Outdoor",
        "Cycling": "Sports & Outdoor", "Outdoor & Hiking": "Sports & Outdoor",
        "Drones": "Cameras", "VR Headsets": "Gaming",
        "Gaming Mice": "Gaming", "Steam Mops": "Home Appliances",
        "TV Accessories": "Accessories", "Luggage": "Accessories",
        "Radios": "Audio", "Home Cinema": "TV & Video",
        "Musical Instruments": "Audio", "Hi-Fi & Turntables": "Audio",
    }
    cat_to_group.update(extra)

    if cat_to_group:
        case_parts = [
            f"    WHEN NormalizedCategory = '{nc.replace(chr(39),chr(39)*2)}' "
            f"THEN '{mg.replace(chr(39),chr(39)*2)}'"
            for nc, mg in cat_to_group.items()
        ]
        conn.execute(
            "UPDATE products SET NormalizedMainGroup = CASE\n"
            + "\n".join(case_parts)
            + "\n    ELSE NormalizedMainGroup\nEND"
        )
        log.info("  Group consistency sweep done")

    conn.commit()

    n_done = conn.execute(
        "SELECT COUNT(*) FROM products WHERE NormalizedCategory IS NOT NULL AND NormalizedCategory != ''"
    ).fetchone()[0]
    log.info(f"Normalization complete — {n_done} rows have NormalizedCategory")
    return n_todo


# ── Standalone ────────────────────────────────────────────────────────────────

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "products.db")


def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    ap = argparse.ArgumentParser(description="Normalize product categories")
    ap.add_argument("--dry-run", action="store_true",
                    help="Print mapping without touching the DB")
    ap.add_argument("--force", action="store_true",
                    help="Re-normalize all rows, not just NULL ones")
    ap.add_argument("--show-unmapped", action="store_true",
                    help="Print categories that map to their original value")
    args = ap.parse_args()

    conn = sqlite3.connect(DB_PATH, timeout=60)

    if args.dry_run:
        rows = conn.execute(
            "SELECT DISTINCT Category, MainCategory, COUNT(*) as cnt "
            "FROM products GROUP BY Category, MainCategory ORDER BY cnt DESC"
        ).fetchall()
        print(f"\n{'Category':<40} {'MainCategory':<35} {'→ NormCat':<30} {'NormGroup':<25} {'count':>6}")
        print("─" * 140)
        for cat, main, cnt in rows:
            nc, mg = normalize(cat, main)
            if args.show_unmapped and nc != (cat or "Other"):
                continue
            print(f"{str(cat):<40} {str(main):<35} {nc:<30} {mg:<25} {cnt:>6}")
        conn.close()
        return

    n = run_normalization(conn, force=args.force)

    # Print summary
    summary = conn.execute("""
        SELECT NormalizedMainGroup, NormalizedCategory, COUNT(*) as cnt
        FROM products
        WHERE NormalizedCategory IS NOT NULL
        GROUP BY NormalizedMainGroup, NormalizedCategory
        ORDER BY NormalizedMainGroup, cnt DESC
    """).fetchall()
    conn.close()

    print(f"\nNormalized {n} rows.\n")
    cur_group = None
    for mg, nc, cnt in summary:
        if mg != cur_group:
            print(f"\n  {mg}")
            cur_group = mg
        print(f"    {nc:<35} {cnt:>6} products")


if __name__ == "__main__":
    main()
