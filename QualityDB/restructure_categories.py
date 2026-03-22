"""
restructure_categories.py — QualityDB

Adds a MainCategory column to products.db and maps every product to one of
15 main categories + a cleaned subcategory.  Also fixes the most common
misclassification: phones/tablets landing in "Home Appliances".

Run:
    python3 restructure_categories.py            # live run
    python3 restructure_categories.py --dry-run  # preview only (no writes)
"""

import sqlite3
import re
import os
import sys
from collections import defaultdict

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "products.db")
DRY_RUN = "--dry-run" in sys.argv


# ─────────────────────────────────────────────────────────────────────────────
# CATEGORY MAP  (current Category value → (MainCategory, new Subcategory))
# ─────────────────────────────────────────────────────────────────────────────
CATEGORY_MAP = {
    # ── Telefony a tablety ────────────────────────────────────────────────────
    "Mobilní telefony":             ("Telefony a tablety",          "Mobilní telefony"),
    "Mobile Phones":                ("Telefony a tablety",          "Mobilní telefony"),
    "Tablet":                       ("Telefony a tablety",          "Tablety"),
    "Tablety":                      ("Telefony a tablety",          "Tablety"),
    "Tablets":                      ("Telefony a tablety",          "Tablety"),

    # ── Počítače a notebooky ───────────────────────────────────────────────────
    "Notebooky":                    ("Počítače a notebooky",        "Notebooky"),
    "Monitory":                     ("Počítače a notebooky",        "Monitory"),
    "Monitors":                     ("Počítače a notebooky",        "Monitory"),
    "Příslušenství k notebookům":   ("Počítače a notebooky",        "Příslušenství k notebookům"),
    "Laptop Accessories":           ("Počítače a notebooky",        "Příslušenství k notebookům"),
    "Kancelářské vybavení":         ("Počítače a notebooky",        "Kancelářské vybavení"),
    "Tašky a batohy":               ("Počítače a notebooky",        "Tašky a batohy"),
    "Mini počítače":                ("Počítače a notebooky",        "Mini počítače"),
    "Dokovací Stanice":             ("Počítače a notebooky",        "Dokovací stanice"),

    # ── PC komponenty ─────────────────────────────────────────────────────────
    "Grafické karty":               ("PC komponenty",               "Grafické karty"),
    "Procesory":                    ("PC komponenty",               "Procesory"),
    "RAM":                          ("PC komponenty",               "RAM"),
    "Základní desky":               ("PC komponenty",               "Základní desky"),
    "PC skříně":                    ("PC komponenty",               "PC skříně"),
    "Chlazení procesorů":           ("PC komponenty",               "Chlazení"),
    "Napájení":                     ("PC komponenty",               "Napájení"),

    # ── Herní technika ────────────────────────────────────────────────────────
    "Herní Konzole":                ("Herní technika",              "Herní konzole"),
    "Herní příslušenství":          ("Herní technika",              "Herní příslušenství"),
    "Herní ovladače":               ("Herní technika",              "Herní ovladače"),
    "Závodní příslušenství":        ("Herní technika",              "Závodní příslušenství"),
    "Herní Židle":                  ("Herní technika",              "Herní sedačky"),
    "Herní sedačky":                ("Herní technika",              "Herní sedačky"),
    "Herní Křeslo":                 ("Herní technika",              "Herní sedačky"),
    "Gaming Keyboards":             ("Herní technika",              "Herní příslušenství"),
    "Herní klávesnice":             ("Herní technika",              "Herní příslušenství"),
    "Gaming Mice":                  ("Herní technika",              "Herní příslušenství"),
    "Herní myši":                   ("Herní technika",              "Herní příslušenství"),

    # ── Zvuk a hudba ─────────────────────────────────────────────────────────
    "Sluchátka":                    ("Zvuk a hudba",                "Sluchátka"),
    "Headphones":                   ("Zvuk a hudba",                "Sluchátka"),
    "Gaming Headsets":              ("Zvuk a hudba",                "Sluchátka"),
    "Herní sluchátka":              ("Zvuk a hudba",                "Sluchátka"),
    "Chrániče sluchu":              ("Zvuk a hudba",                "Sluchátka"),
    "Bluetooth Headset":            ("Zvuk a hudba",                "Sluchátka"),
    "Reproduktory":                 ("Zvuk a hudba",                "Reproduktory"),
    "Speakers":                     ("Zvuk a hudba",                "Reproduktory"),
    "Reproduktor":                  ("Zvuk a hudba",                "Reproduktory"),
    "Reprosoustava":                ("Zvuk a hudba",                "Reproduktory"),
    "Soundbary":                    ("Zvuk a hudba",                "Soundbary"),
    "Soundbars":                    ("Zvuk a hudba",                "Soundbary"),
    "Subwoofer":                    ("Zvuk a hudba",                "Soundbary"),
    "Mikrofony":                    ("Zvuk a hudba",                "Mikrofony"),
    "Hudební nástroje":             ("Zvuk a hudba",                "Hudební nástroje"),
    "Hudební příslušenství":        ("Zvuk a hudba",                "Hudební příslušenství"),
    "Gramofony":                    ("Zvuk a hudba",                "Gramofony"),
    "Rádia":                        ("Zvuk a hudba",                "Rádia a Hi-Fi"),
    "Mikrosystémy":                 ("Zvuk a hudba",                "Rádia a Hi-Fi"),
    "Digitální Piano":              ("Zvuk a hudba",                "Hudební nástroje"),

    # ── Televize a video ──────────────────────────────────────────────────────
    "Televize":                     ("Televize a video",            "Televize"),
    "TVs":                          ("Televize a video",            "Televize"),
    "Projektory":                   ("Televize a video",            "Projektory"),
    "Projectors":                   ("Televize a video",            "Projektory"),
    "Streamovací zařízení":         ("Televize a video",            "Streamovací zařízení"),
    "Multimediální přehrávače":     ("Televize a video",            "Multimediální přehrávače"),

    # ── Velké domácí spotřebiče ───────────────────────────────────────────────
    "Pračky":                       ("Velké domácí spotřebiče",     "Pračky"),
    "Sušičky prádla":               ("Velké domácí spotřebiče",     "Sušičky prádla"),
    "Ledničky":                     ("Velké domácí spotřebiče",     "Ledničky"),
    "Americké ledničky":            ("Velké domácí spotřebiče",     "Ledničky"),
    "Mrazáky":                      ("Velké domácí spotřebiče",     "Mrazáky"),
    "Myčky nádobí":                 ("Velké domácí spotřebiče",     "Myčky nádobí"),
    "Sporáky":                      ("Velké domácí spotřebiče",     "Sporáky"),
    "Trouby":                       ("Velké domácí spotřebiče",     "Trouby"),

    # ── Malé domácí spotřebiče ────────────────────────────────────────────────
    "Kávovary":                     ("Malé domácí spotřebiče",      "Kávovary"),
    "Coffee Machines":              ("Malé domácí spotřebiče",      "Kávovary"),
    "Varné konvice":                ("Malé domácí spotřebiče",      "Varné konvice"),
    "Kettles":                      ("Malé domácí spotřebiče",      "Varné konvice"),
    "Mixéry a roboty":              ("Malé domácí spotřebiče",      "Mixéry a roboty"),
    "Kitchen Robots":               ("Malé domácí spotřebiče",      "Mixéry a roboty"),
    "Blenders":                     ("Malé domácí spotřebiče",      "Mixéry a roboty"),
    "Toustovače":                   ("Malé domácí spotřebiče",      "Toustovače"),
    "Toasters":                     ("Malé domácí spotřebiče",      "Toustovače"),
    "Mikrovlnné trouby":            ("Malé domácí spotřebiče",      "Mikrovlnné trouby"),
    "Microwaves":                   ("Malé domácí spotřebiče",      "Mikrovlnné trouby"),
    "Žehličky":                     ("Malé domácí spotřebiče",      "Žehličky"),
    "Irons":                        ("Malé domácí spotřebiče",      "Žehličky"),
    "Fény a stylingové přístroje":  ("Malé domácí spotřebiče",      "Fény a stylingové přístroje"),
    "Fritézy":                      ("Malé domácí spotřebiče",      "Fritézy"),
    "Ventilátory":                  ("Malé domácí spotřebiče",      "Ventilátory"),
    "Parní čističe":                ("Malé domácí spotřebiče",      "Parní čističe"),
    "Epilátory a holicí strojky":   ("Malé domácí spotřebiče",      "Péče o tělo"),

    # ── Vysavače a úklid ──────────────────────────────────────────────────────
    "Vysavače":                     ("Vysavače a úklid",            "Vysavače"),
    "Vacuum Cleaners":              ("Vysavače a úklid",            "Vysavače"),
    "Tyčové vysavače":              ("Vysavače a úklid",            "Tyčové vysavače"),
    "Robotické vysavače":           ("Vysavače a úklid",            "Robotické vysavače"),
    "Robot Vacuums":                ("Vysavače a úklid",            "Robotické vysavače"),

    # ── Chytré zařízení ───────────────────────────────────────────────────────
    "Chytré hodinky":               ("Chytré zařízení",             "Chytré hodinky"),
    "Smartwatches":                 ("Chytré zařízení",             "Chytré hodinky"),
    "Fitness Náramek":              ("Chytré zařízení",             "Fitness náramky"),
    "Fitness náramky":              ("Chytré zařízení",             "Fitness náramky"),
    "Fitness Trackers":             ("Chytré zařízení",             "Fitness náramky"),
    "Čističky vzduchu":             ("Chytré zařízení",             "Čističky vzduchu"),
    "Air Purifiers":                ("Chytré zařízení",             "Čističky vzduchu"),
    "Chytrá domácnost":             ("Chytré zařízení",             "Chytrá domácnost"),
    "Smart Home":                   ("Chytré zařízení",             "Chytrá domácnost"),
    "IP kamery":                    ("Chytré zařízení",             "IP kamery"),
    "IP Cameras":                   ("Chytré zařízení",             "IP kamery"),

    # ── Foto a kamery ─────────────────────────────────────────────────────────
    "Fotoaparáty":                  ("Foto a kamery",               "Fotoaparáty"),
    "Digital Cameras":              ("Foto a kamery",               "Fotoaparáty"),
    "Akční kamery":                 ("Foto a kamery",               "Akční kamery"),
    "Action Cameras":               ("Foto a kamery",               "Akční kamery"),
    "Webkamery":                    ("Foto a kamery",               "Webkamery"),
    "Webcams":                      ("Foto a kamery",               "Webkamery"),

    # ── Datová úložiště ───────────────────────────────────────────────────────
    "SSD":                          ("Datová úložiště",             "SSD"),
    "Pevné disky":                  ("Datová úložiště",             "Pevné disky"),
    "HDD":                          ("Datová úložiště",             "Pevné disky"),
    "Flash disky":                  ("Datová úložiště",             "Flash disky"),
    "USB Flash Drives":             ("Datová úložiště",             "Flash disky"),
    "Externí disky":                ("Datová úložiště",             "Externí disky"),
    "NAS úložiště":                 ("Datová úložiště",             "NAS úložiště"),

    # ── Sítě a konektivita ────────────────────────────────────────────────────
    "Kabely a rozbočovače":         ("Sítě a konektivita",          "Kabely a rozbočovače"),
    "Rozbočovače":                  ("Sítě a konektivita",          "Kabely a rozbočovače"),
    "Konektory a adaptéry":         ("Sítě a konektivita",          "Kabely a rozbočovače"),
    "Routery":                      ("Sítě a konektivita",          "Routery"),
    "Routers":                      ("Sítě a konektivita",          "Routery"),
    "Síťové přepínače":             ("Sítě a konektivita",          "Síťové přepínače"),
    "Síťové komponenty":            ("Sítě a konektivita",          "Síťové komponenty"),
    "Síťová Karta":                 ("Sítě a konektivita",          "Síťové komponenty"),
    "Extendery":                    ("Sítě a konektivita",          "Extendery"),
    "Anténní příslušenství":        ("Sítě a konektivita",          "Anténní příslušenství"),
    "Síťová karta":                 ("Sítě a konektivita",          "Síťové komponenty"),

    # ── Periferie a příslušenství ─────────────────────────────────────────────
    "Myši":                         ("Periferie a příslušenství",   "Myši"),
    "Mice":                         ("Periferie a příslušenství",   "Myši"),
    "Trackball":                    ("Periferie a příslušenství",   "Myši"),
    "Trackpad":                     ("Periferie a příslušenství",   "Myši"),
    "Klávesnice":                   ("Periferie a příslušenství",   "Klávesnice"),
    "Keyboards":                    ("Periferie a příslušenství",   "Klávesnice"),
    "Brýle Na Počítač":             ("Periferie a příslušenství",   "Brýle na počítač"),
    "Přepěťové ochrany":            ("Periferie a příslušenství",   "Ostatní příslušenství"),
    "Dobíjecí Stanice":             ("Periferie a příslušenství",   "Ostatní příslušenství"),
    "Dobíjecí Karta":               ("Periferie a příslušenství",   "Ostatní příslušenství"),
    "Hardware peněženky":           ("Periferie a příslušenství",   "Ostatní příslušenství"),
    "Autentizační Token":           ("Periferie a příslušenství",   "Ostatní příslušenství"),
    "Bezpečnostní zámky":           ("Periferie a příslušenství",   "Ostatní příslušenství"),
    "Stolní lampy":                 ("Periferie a příslušenství",   "Ostatní příslušenství"),
    "Držáky":                       ("Periferie a příslušenství",   "Ostatní příslušenství"),
    "PC příslušenství":             ("Periferie a příslušenství",   "Ostatní příslušenství"),
    "Dotykové Pero (Stylus)":       ("Periferie a příslušenství",   "Ostatní příslušenství"),
    "Ochranné Sklo":                ("Periferie a příslušenství",   "Ostatní příslušenství"),
    "Ochranná skla a fólie":        ("Periferie a příslušenství",   "Ostatní příslušenství"),
    "Kancelářské Křeslo":           ("Periferie a příslušenství",   "Kancelářské vybavení"),
    "Herní příslušenství":          ("Herní technika",              "Herní příslušenství"),

    # ── Hry a hračky ─────────────────────────────────────────────────────────
    "Hry a hračky":                 ("Hry a hračky",                "Hry a hračky"),
    "Hra Na Pc A Xbox":             ("Hry a hračky",                "Hry"),
    "Hra Na Pc":                    ("Hry a hračky",                "Hry"),
    "Herní Doplněk / Dlc":          ("Hry a hračky",                "Hry"),
    "Karetní Hra":                  ("Hry a hračky",                "Hry a hračky"),
    "Příslušenství K Ovladači":     ("Herní technika",              "Herní příslušenství"),
    "Herní příslušenství":          ("Herní technika",              "Herní příslušenství"),
    "Závodní příslušenství":        ("Herní technika",              "Závodní příslušenství"),
    "Sada Herního Příslušenství":   ("Herní technika",              "Herní příslušenství"),
    "Kryt Na Herní Konzoli":        ("Herní technika",              "Herní příslušenství"),
    "Brašna Pro Xbox Series S/X":   ("Herní technika",              "Herní příslušenství"),
    "Gamepad":                      ("Herní technika",              "Herní ovladače"),
    "Obal Na Ovladač":              ("Herní technika",              "Herní příslušenství"),

    # ── Zbývající → správné kategorie ────────────────────────────────────────
    "Externí vypalovačky":          ("Periferie a příslušenství",   "Ostatní příslušenství"),
    "Externí Mechanika":            ("Periferie a příslušenství",   "Ostatní příslušenství"),
    "Externí Box":                  ("Datová úložiště",             "Externí disky"),
    "Čtečka Karet":                 ("Periferie a příslušenství",   "Ostatní příslušenství"),
    "Čisticí příslušenství":        ("Vysavače a úklid",            "Čisticí příslušenství"),
    "Pouzdro Na Tablet S Klávesnicí": ("Telefony a tablety",        "Příslušenství"),
    "Blu":                          ("Televize a video",            "Multimediální přehrávače"),
    "Popruh Na Kytaru":             ("Zvuk a hudba",                "Hudební příslušenství"),
    "Obal Na Kytaru":               ("Zvuk a hudba",                "Hudební příslušenství"),
    "Klavírní Stolička":            ("Zvuk a hudba",                "Hudební příslušenství"),
    "Bubenická Stolička":           ("Zvuk a hudba",                "Hudební příslušenství"),
    "Kytarový Efekt":               ("Zvuk a hudba",                "Hudební příslušenství"),
    "Zásuvka":                      ("Periferie a příslušenství",   "Ostatní příslušenství"),
    "Přepínač":                     ("Sítě a konektivita",          "Síťové přepínače"),
    "Přijímač":                     ("Televize a video",            "Multimediální přehrávače"),
    "Dac Převodník":                ("Zvuk a hudba",                "Soundbary"),
    "Baterie a akumulátory":        ("Periferie a příslušenství",   "Ostatní příslušenství"),
    "Chladič Pevného Disku":        ("PC komponenty",               "Chlazení"),
    "Chladič Pevného Disku Pro M.2 2280 Disky": ("PC komponenty",  "Chlazení"),
    "Řadič":                        ("PC komponenty",               "Ostatní příslušenství"),
    "Řadič Do Serial Ata":          ("PC komponenty",               "Ostatní příslušenství"),
    "Řadič Do Usb 3.2 Gen 2 Header":("PC komponenty",              "Ostatní příslušenství"),
    "Serverová Paměť":              ("PC komponenty",               "RAM"),
    "Paměťová Karta 128 Gb":        ("Datová úložiště",             "Flash disky"),
    "Paměťová Karta 256 Gb":        ("Datová úložiště",             "Flash disky"),
    "Paměťová Karta 512 Gb":        ("Datová úložiště",             "Flash disky"),
    "Datové Úložiště":              ("Datová úložiště",             "Ostatní úložiště"),
    "Webkamera S Rozlišením Full Hd (1920 × 1080 Px)": ("Foto a kamery", "Webkamery"),
    "Konferenční Zařízení":         ("Periferie a příslušenství",   "Ostatní příslušenství"),
    "Spínač":                       ("Sítě a konektivita",          "Síťové přepínače"),
    "Tuner":                        ("Televize a video",            "Multimediální přehrávače"),
    "Vzdálený Přehrávač":           ("Televize a video",            "Streamovací zařízení"),
    "Cd Přehrávač":                 ("Televize a video",            "Multimediální přehrávače"),
    "Handsfree Do Auta":            ("Periferie a příslušenství",   "Ostatní příslušenství"),
    "Ochranné Sklo Pro Nintendo Switch": ("Herní technika",         "Herní příslušenství"),
    "Ochranné Sklo Pro Nintendo Switch 2": ("Herní technika",       "Herní příslušenství"),
    "Zásuvková Lišta":              ("Periferie a příslušenství",   "Ostatní příslušenství"),
    "Přepěťové ochrany":            ("Periferie a příslušenství",   "Ostatní příslušenství"),
    "Led Pásek":                    ("Periferie a příslušenství",   "Ostatní příslušenství"),
    "Usb Lampička":                 ("Periferie a příslušenství",   "Ostatní příslušenství"),
    "Stopky":                       ("Periferie a příslušenství",   "Ostatní příslušenství"),
    "Dokovací Stanice Propojující Usb": ("Počítače a notebooky",   "Dokovací stanice"),
    "Externí Dokovací Stanice":     ("Počítače a notebooky",        "Dokovací stanice"),
    "Dárková Sada Oficiální Dárkový Set Pro Fanoušky Herní Konzole Playstation": ("Herní technika", "Herní příslušenství"),
    "Uncategorized":                ("Ostatní",                     "Nezařazeno"),
    "Ostatní":                      ("Ostatní",                     "Nezařazeno"),
    "Externí Zvuková Karta":        ("Zvuk a hudba",                "Zvukové karty"),
    "Příslušenství Pro Hudební Nástroje": ("Zvuk a hudba",          "Hudební příslušenství"),
    "Počítačový Zdroj 120W":        ("PC komponenty",               "Napájení"),
    "Počítačový Zdroj 200W":        ("PC komponenty",               "Napájení"),
    "Rámeček Na Disk":              ("Datová úložiště",             "Ostatní úložiště"),
    "Zesilovač Pro Pozemní Analogový A Digitální Příjem Tv/Fm Signálů": ("Sítě a konektivita", "Anténní příslušenství"),
    "Bleskojistka F Konektory":     ("Sítě a konektivita",          "Anténní příslušenství"),
    "Příslušenství Pro Lokátor Gps Anténa S 5M Pigtailem S Sma Konektorem": ("Sítě a konektivita", "Anténní příslušenství"),
    "Autentizační Token Univerzální Bezpečnostní Token S Usb": ("Periferie a příslušenství", "Ostatní příslušenství"),
    "Příslušenství K Ovladači Sada Příslušenství Pro Ovladač Xbox Elite Series 2": ("Herní technika", "Herní příslušenství"),
    "Příslušenství K Vr Brýlím Ipega Ochranné Krytky Objektivů Pro Playstation Vr2": ("Herní technika", "Herní příslušenství"),
    "Cestovní Pouzdro":             ("Periferie a příslušenství",   "Ostatní příslušenství"),
    "Dětský Psací Stůl S Židlí":    ("Periferie a příslušenství",   "Kancelářské vybavení"),
    "Zametač Všechny Druhy Podlah": ("Vysavače a úklid",            "Čisticí příslušenství"),
}


# ─────────────────────────────────────────────────────────────────────────────
# ENGLISH CATEGORY NAMES (original Alza export)
# ─────────────────────────────────────────────────────────────────────────────
CATEGORY_MAP.update({
    "Cables & Hubs":            ("Sítě a konektivita",         "Kabely a rozbočovače"),
    "Games & Toys":             ("Hry a hračky",               "Hry a hračky"),
    "Headphones":               ("Zvuk a hudba",               "Sluchátka"),
    "Laptop Accessories":       ("Počítače a notebooky",       "Příslušenství k notebookům"),
    "Speakers":                 ("Zvuk a hudba",               "Reproduktory"),
    "Keyboards":                ("Periferie a příslušenství",  "Klávesnice"),
    "PC Cases":                 ("PC komponenty",              "PC skříně"),
    "Kitchen – Ovens & Hobs":   ("Velké domácí spotřebiče",   "Sporáky"),
    "Refrigerators":            ("Velké domácí spotřebiče",   "Ledničky"),
    "Graphics Cards":           ("PC komponenty",              "Grafické karty"),
    "Bags & Backpacks":         ("Počítače a notebooky",       "Tašky a batohy"),
    "External Drives":          ("Datová úložiště",            "Externí disky"),
    "Digital Cameras":          ("Foto a kamery",              "Fotoaparáty"),
    "Blenders":                 ("Malé domácí spotřebiče",     "Mixéry a roboty"),
    "Irons":                    ("Malé domácí spotřebiče",     "Žehličky"),
    "Kitchen Robots":           ("Malé domácí spotřebiče",     "Mixéry a roboty"),
    "Toasters":                 ("Malé domácí spotřebiče",     "Toustovače"),
    "Gaming Headsets":          ("Zvuk a hudba",               "Sluchátka"),
    "Microwaves":               ("Malé domácí spotřebiče",     "Mikrovlnné trouby"),
    "Action Cameras":           ("Foto a kamery",              "Akční kamery"),
    "Robot Vacuums":            ("Vysavače a úklid",           "Robotické vysavače"),
    "Fitness Trackers":         ("Chytré zařízení",            "Fitness náramky"),
    "Tablets":                  ("Telefony a tablety",         "Tablety"),
    "USB Flash Drives":         ("Datová úložiště",            "Flash disky"),
    "PC Cooling":               ("PC komponenty",              "Chlazení"),
    "Soundbars":                ("Zvuk a hudba",               "Soundbary"),
    "Webcams":                  ("Foto a kamery",              "Webkamery"),
    "Air Purifiers":            ("Chytré zařízení",            "Čističky vzduchu"),
    "Kettles":                  ("Malé domácí spotřebiče",     "Varné konvice"),
    "Coffee Machines":          ("Malé domácí spotřebiče",     "Kávovary"),
    "IP Cameras":               ("Chytré zařízení",            "IP kamery"),
    "Routers":                  ("Sítě a konektivita",         "Routery"),
    "Projectors":               ("Televize a video",           "Projektory"),
    "Game Controllers":         ("Herní technika",             "Herní ovladače"),
    "Gaming Mice":              ("Herní technika",             "Herní příslušenství"),
    "Gaming Keyboards":         ("Herní technika",             "Herní příslušenství"),
    "Smart Home":               ("Chytré zařízení",            "Chytrá domácnost"),
    "Laptops":                  ("Počítače a notebooky",       "Notebooky"),
    "NAS":                      ("Datová úložiště",            "NAS úložiště"),
    "HDD":                      ("Datová úložiště",            "Pevné disky"),
    "Vacuum Cleaners":          ("Vysavače a úklid",           "Vysavače"),
    "Smartwatches":             ("Chytré zařízení",            "Chytré hodinky"),
    "Mice":                     ("Periferie a příslušenství",  "Myši"),
    "Mobile Phones":            ("Telefony a tablety",         "Mobilní telefony"),
    "Microphones":              ("Zvuk a hudba",               "Mikrofony"),
    "Chrániče Sluchu":          ("Zvuk a hudba",               "Sluchátka"),
    "Chrániče sluchu":          ("Zvuk a hudba",               "Sluchátka"),
    "Chrániče Sluchu Pro Děti": ("Zvuk a hudba",               "Sluchátka"),
    "Špunty Do Uší Vhodné Na Koncerty": ("Zvuk a hudba",       "Sluchátka"),
    "Hardware Peněženka":       ("Periferie a příslušenství",  "Ostatní příslušenství"),
    "Hardware peněženky":       ("Periferie a příslušenství",  "Ostatní příslušenství"),
    "Bezpečnostní Zámek":       ("Periferie a příslušenství",  "Ostatní příslušenství"),
    "Ochranná Fólie":           ("Periferie a příslušenství",  "Ostatní příslušenství"),
    "Antivibrační Sloupky":     ("PC komponenty",              "Chlazení"),
    "Příslušenství Pro Pc Skříně": ("PC komponenty",           "PC skříně"),
    "Střihová Karta Externí":   ("Foto a kamery",              "Akční kamery"),
    "Záznamové Zařízení Externí": ("Zvuk a hudba",             "Zvukové karty"),
    "Čistič Koberců":           ("Vysavače a úklid",           "Čisticí příslušenství"),
})


# ─────────────────────────────────────────────────────────────────────────────
# REGEX FALLBACK for over-specific Czech category names
# Applied when a category is NOT found in CATEGORY_MAP.
# Each entry: (MainCategory, Subcategory, compiled regex)
# ─────────────────────────────────────────────────────────────────────────────
REGEX_RULES = [
    # PC komponenty
    ("PC komponenty",              "Flash disky",               re.compile(r'^Flash Disk \d', re.I)),
    ("PC komponenty",              "Chlazení",                  re.compile(r'^Ventilátor Do Pc\b', re.I)),
    ("PC komponenty",              "Chlazení",                  re.compile(r'^Chladič Na Procesor\b', re.I)),
    ("PC komponenty",              "Chlazení",                  re.compile(r'^Chladič Pevného Disku\b', re.I)),
    ("PC komponenty",              "Chlazení",                  re.compile(r'^Chránič Ventilátorů\b', re.I)),
    ("PC komponenty",              "Chlazení",                  re.compile(r'^PC Cooling\b', re.I)),
    ("PC komponenty",              "Napájení",                  re.compile(r'^Počítačový Zdroj\b', re.I)),
    ("PC komponenty",              "Procesory",                 re.compile(r'^Procesor \d+', re.I)),
    ("PC komponenty",              "Základní desky",            re.compile(r'^Základní Deska\b', re.I)),
    ("PC komponenty",              "Napájení",                  re.compile(r'^Zdroj \d+W\b', re.I)),
    ("PC komponenty",              "Ostatní příslušenství",     re.compile(r'^Řadič Do Pcie\b', re.I)),
    ("PC komponenty",              "Mini počítače",             re.compile(r'^(Mini Počítač|Raspberry Pi|Pouzdro Na Minipočítač)', re.I)),

    # Datová úložiště
    ("Datová úložiště",            "Flash disky",               re.compile(r'^Flash Disk\b', re.I)),
    ("Datová úložiště",            "Flash disky",               re.compile(r'^Flash Disk', re.I)),
    ("Datová úložiště",            "Pevné disky",               re.compile(r'^Pevný Disk\b', re.I)),
    ("Datová úložiště",            "Pevné disky",               re.compile(r'^Pevný Disk', re.I)),
    ("Datová úložiště",            "Externí disky",             re.compile(r'^Externí Disk\b', re.I)),
    ("Datová úložiště",            "Ostatní úložiště",          re.compile(r'^Rámeček Na Disk\b', re.I)),

    # Sítě a konektivita
    ("Sítě a konektivita",         "Kabely a rozbočovače",      re.compile(r'^Konektor Typu\b', re.I)),
    ("Sítě a konektivita",         "Kabely a rozbočovače",      re.compile(r'^Keystone\b', re.I)),
    ("Sítě a konektivita",         "Kabely a rozbočovače",      re.compile(r'^Rozbočovač\b', re.I)),
    ("Sítě a konektivita",         "Kabely a rozbočovače",      re.compile(r'^Zásuvka\b', re.I)),
    ("Sítě a konektivita",         "Síťové přepínače",          re.compile(r'^Switch\b', re.I)),
    ("Sítě a konektivita",         "Síťové přepínače",          re.compile(r'^Přepínač\b', re.I)),
    ("Sítě a konektivita",         "Síťové přepínače",          re.compile(r'^Přepínač Datový\b', re.I)),
    ("Sítě a konektivita",         "Extendery",                 re.compile(r'^Extender\b', re.I)),
    ("Sítě a konektivita",         "Anténní příslušenství",     re.compile(r'^Zesilovač Pro', re.I)),

    # Vysavače a úklid
    ("Vysavače a úklid",           "Robotické vysavače",        re.compile(r'^Robotický Vysavač\b', re.I)),
    ("Vysavače a úklid",           "Tyčové vysavače",           re.compile(r'^Tyčový Vysavač\b', re.I)),
    ("Vysavače a úklid",           "Vysavače",                  re.compile(r'^(Bezsáčkový|Sáčkový|Ruční|Průmyslový|Autovysavač|Vysavač Popela)\b', re.I)),
    ("Vysavače a úklid",           "Vysavače",                  re.compile(r'Vysavač', re.I)),
    ("Vysavače a úklid",           "Čisticí příslušenství",     re.compile(r'^Zametač\b', re.I)),

    # Herní technika
    ("Herní technika",             "Herní ovladače",            re.compile(r'^Gamepad\b', re.I)),
    ("Herní technika",             "Herní ovladače",            re.compile(r'^Herní Ovladač\b', re.I)),
    ("Herní technika",             "Herní ovladače",            re.compile(r'^Volant\b', re.I)),
    ("Herní technika",             "Herní sedačky",             re.compile(r'^Herní (Závodní Sedačka|Křeslo|Sedačka)\b', re.I)),
    ("Herní technika",             "Závodní příslušenství",     re.compile(r'^Stojan Na Volant\b', re.I)),
    ("Herní technika",             "Herní příslušenství",       re.compile(r'^(Obal Na Nintendo|Stojan Na Herní|Gripy Na Ovladač|Streamdeck|RGB Příslušenství)\b', re.I)),
    ("Herní technika",             "Herní konzole",             re.compile(r'^Herní Konzole\b', re.I)),
    ("Herní technika",             "Herní příslušenství",       re.compile(r'^Stojan Na Herní Konzoli\b', re.I)),
    ("Herní technika",             "Herní příslušenství",       re.compile(r'^Obal Na (Ovladač|Klávesy)\b', re.I)),

    # Zvuk a hudba
    ("Zvuk a hudba",               "Reproduktory",              re.compile(r'^Reproduktor\b', re.I)),
    ("Zvuk a hudba",               "Soundbary",                 re.compile(r'^Subwoofer\b', re.I)),
    ("Zvuk a hudba",               "Rádia a Hi-Fi",             re.compile(r'^(Radiomagnetofon|Rádio|Mikrosystém|Minisystém|Multimediální Centrum)\b', re.I)),
    ("Zvuk a hudba",               "Hudební nástroje",          re.compile(r'^(Klávesy|Midi Klávesy|Syntezátor|Digitální Piano|Ukulele|Perkuse|Kazoo|Foukací Harmonika|Zobcová Flétna|Kombo)\b', re.I)),
    ("Zvuk a hudba",               "Hudební příslušenství",     re.compile(r'^(Stojan Na (Kytaru|Noty|Klávesy)|Trsátko|Struny|Kapodastr|Ladička|Lampička Na Noty|Paličky Na Bicí|Metronom|Příslušenství Pro Hudební)\b', re.I)),
    ("Zvuk a hudba",               "Gramofony",                 re.compile(r'^Gramofon\b', re.I)),
    ("Zvuk a hudba",               "Zvukové karty",             re.compile(r'^(Dac Převodník|Dac/Amp|Zvuková Karta)\b', re.I)),

    # Televize a video
    ("Televize a video",           "Multimediální přehrávače",  re.compile(r'^(Blu|Dvd Přehrávač|Mp4 Přehrávač|Síťový Přehrávač|Video Grabber|Multimediální Centrum)\b', re.I)),

    # Periferie a příslušenství
    ("Periferie a příslušenství",  "Kancelářské vybavení",      re.compile(r'^(Kancelářská Židle|Kancelářské Křeslo|Stojan Na Pc|Držák Na Pc|Dětský Psací Stůl|Dětská Židle)\b', re.I)),
    ("Periferie a příslušenství",  "Ostatní příslušenství",     re.compile(r'^(Nabíječka Do Sítě|Přepěťová Ochrana|Rgb Příslušenství|Stolní Lampa|Lampička)\b', re.I)),

    # Telefony a tablety
    ("Telefony a tablety",         "Příslušenství",             re.compile(r'^(Držák Na Mobil|Pouzdro Na Tablet|Obal Na Tablet)\b', re.I)),
    ("Telefony a tablety",         "Ostatní",                   re.compile(r'^Dobíjecí (Karta|Stanice)\b', re.I)),
    ("Periferie a příslušenství",  "Ostatní příslušenství",     re.compile(r'^Externí Vypalovačka\b', re.I)),
    ("Periferie a příslušenství",  "Ostatní příslušenství",     re.compile(r'^Příslušenství Pro Pc Skříně\b', re.I)),
    ("Vysavače a úklid",           "Čisticí příslušenství",     re.compile(r'^Čistič Koberců\b', re.I)),
]


# ─────────────────────────────────────────────────────────────────────────────
# NAME-BASED DETECTION for "Home Appliances" products
# ─────────────────────────────────────────────────────────────────────────────

# Phone brands / model patterns — match = Mobilní telefony
PHONE_RE = re.compile(
    r'\biphone\b'
    r'|\bsamsung galaxy [a-z]'
    r'|\bgalaxy (z|a|s|m|f)\d'
    r'|\bgalaxy fold\b|\bgalaxy flip\b'
    r'|\boneplus\b'
    r'|\bgoogle pixel\b'
    r'|\brealme\b'
    r'|\baligator\b'
    r'|\bulefone\b'
    r'|\bmotorola moto\b'
    r'|\bhonor \d'
    r'|\bhuawei (p|mate|nova)\d'
    r'|\bxiaomi [0-9]'
    r'|\bxiaomi redmi\b|\bredmi\b'
    r'|\bnokia [gcgt]\d'
    r'|\boppo (a|find|reno)\d'
    r'|\bsony xperia\b',
    re.IGNORECASE
)

# Tablet patterns — match = Tablety
TABLET_RE = re.compile(
    r'\bipad\b'
    r'|\bsamsung tab\b|\bgalaxy tab\b'
    r'|\bxiaomi pad\b|\blenovo tab\b'
    r'|\bhuawei matepad\b'
    r'|\bumax u-one\b',
    re.IGNORECASE
)

# Gaming handheld — match = Herní konzole
HANDHELD_RE = re.compile(
    r'\bmsi claw\b|\bsteam deck\b|\basus rog ally\b',
    re.IGNORECASE
)

# Appliance sub-rules for remaining "Home Appliances"
# Each entry: (MainCategory, Subcategory), regex pattern
APPLIANCE_RULES = [
    (("Vysavače a úklid",           "Robotické vysavače"),    re.compile(r'\brobot\b|\broomba\b', re.I)),
    (("Vysavače a úklid",           "Tyčové vysavače"),       re.compile(r'\btyčov[áy]\b|\baquatrio\b|\baquaforce\b|\bhandy force\b|\bhandy\b|\bfreedom\b', re.I)),
    (("Vysavače a úklid",           "Vysavače"),              re.compile(r'\bvysava[cč]\b|\bcyclone\b|\bturbovac\b|\bbbhf\b|\baquawash\b|\bh-energy\b|\bhe\d{3}\b', re.I)),
    (("Velké domácí spotřebiče",    "Pračky"),                re.compile(r'\bpra[cč]k[ay]\b|\bwf[0-9]\b|\bww\d\b|\bwashing machine\b', re.I)),
    (("Velké domácí spotřebiče",    "Myčky nádobí"),          re.compile(r'\bmy[cč]k[ay]\b|\bdishwash\b|\bbdin\b|\bbdfn\b|\bgi67\b|\bsgr-dw\b', re.I)),
    (("Velké domácí spotřebiče",    "Ledničky"),              re.compile(r'\bledni[cč]k\b|\brcna\b|\bnrc6\b|\bfridge\b|\brefrigerator\b|\bamerická\b|\bb5rcna\b', re.I)),
    (("Velké domácí spotřebiče",    "Mrazáky"),               re.compile(r'\bmrazák\b|\bfreezer\b', re.I)),
    (("Velké domácí spotřebiče",    "Sušičky prádla"),        re.compile(r'\bsu[sš]i[cč]k[ay]\b|\bdryer\b', re.I)),
    (("Velké domácí spotřebiče",    "Sporáky"),               re.compile(r'\bsporák\b|\brange\b', re.I)),
    (("Velké domácí spotřebiče",    "Trouby"),                re.compile(r'\btroub[ay]\b|\boven\b|\bim 6435\b', re.I)),
    (("Malé domácí spotřebiče",     "Kávovary"),              re.compile(r'\bkávovar\b|\bcoffee\b|\bespresso\b|\bnanopresso\b|\bnespresso\b|\bdolce\b|\bbarista\b|\bka\s?5[0-9]{3}\b|\bke 550\b|\bnk2w\b', re.I)),
    (("Malé domácí spotřebiče",     "Varné konvice"),         re.compile(r'\bkonvic\b|\bkettle\b|\bwatercooker\b|\bphwk\b|\brk-0\b|\brohnson r-7\b|\bcatler ke\b|\bphilco ph\b|\beta.*adagio\b|\borava.*retro konvice\b', re.I)),
    (("Malé domácí spotřebiče",     "Mixéry a roboty"),       re.compile(r'\bmixér\b|\bblender\b|\bkitchen.*robot\b|\bfoodprocessor\b', re.I)),
    (("Malé domácí spotřebiče",     "Parní čističe"),         re.compile(r'\bparní\b|\bsteam clean\b|\bpáry\b', re.I)),
    (("Malé domácí spotřebiče",     "Ventilátory"),           re.compile(r'\baerostar\b|\bventilátor\b|\bfan\b|\bair.*cooler\b|\btesla.*t[57]\d{2}\b', re.I)),
    (("Malé domácí spotřebiče",     "Žehličky"),              re.compile(r'\bžehli[cč]k\b|\biron\b', re.I)),
    (("Malé domácí spotřebiče",     "Fény a stylingové přístroje"), re.compile(r'\bfén\b|\bhair.*dryer\b|\bstyling\b|\bsencor shd\b', re.I)),
]


def classify_home_appliance(name: str) -> tuple:
    """
    Given a product name from the 'Home Appliances' bucket, return
    (MainCategory, Subcategory).  Falls back to Ostatní if nothing matches.
    """
    if PHONE_RE.search(name):
        return ("Telefony a tablety", "Mobilní telefony")
    if TABLET_RE.search(name):
        return ("Telefony a tablety", "Tablety")
    if HANDHELD_RE.search(name):
        return ("Herní technika", "Herní konzole")
    for (main, sub), pattern in APPLIANCE_RULES:
        if pattern.search(name):
            return (main, sub)
    # Generic fallback for genuine appliances we couldn't classify more specifically
    return ("Malé domácí spotřebiče", "Ostatní spotřebiče")


def classify_product(name: str, current_cat: str) -> tuple:
    """Return (MainCategory, Subcategory) for a given product."""
    if current_cat == "Home Appliances":
        return classify_home_appliance(name)
    # 1. Exact match in CATEGORY_MAP
    if current_cat in CATEGORY_MAP:
        return CATEGORY_MAP[current_cat]
    # 2. Regex fallback for over-specific Czech categories
    for main, sub, pattern in REGEX_RULES:
        if pattern.search(current_cat):
            return (main, sub)
    # 3. Nothing matched
    return ("Ostatní", current_cat or "Nezařazeno")


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def run():
    import shutil, tempfile

    if not os.path.exists(DB_PATH):
        print(f"✗  Database not found at {DB_PATH}")
        sys.exit(1)

    # The DB lives on a FUSE-mounted filesystem that doesn't support SQLite WAL.
    # Work on a /tmp copy, then write back.
    tmp = "/tmp/qualitydb_restructure3.db"; import subprocess; subprocess.run(["chmod", "644", tmp], capture_output=True)
    shutil.copy2(DB_PATH, tmp)
    print(f"Working on copy: {tmp}")

    conn = sqlite3.connect(tmp)
    conn.row_factory = sqlite3.Row

    # Add MainCategory column if missing
    existing_cols = {r[1] for r in conn.execute("PRAGMA table_info(products)").fetchall()}
    if "MainCategory" not in existing_cols:
        if DRY_RUN:
            print("[dry-run] Would ALTER TABLE products ADD COLUMN MainCategory TEXT")
        else:
            conn.execute("ALTER TABLE products ADD COLUMN MainCategory TEXT")
            print("✓  Added MainCategory column")

    rows = conn.execute("SELECT ProductURL, Name, Category FROM products").fetchall()
    print(f"Processing {len(rows):,} products…\n")

    updates = []           # (main_cat, new_sub, product_url)
    skipped_unmapped = defaultdict(int)

    for row in rows:
        url  = row["ProductURL"] or ""
        name = row["Name"] or ""
        cat  = row["Category"] or ""

        main_cat, new_sub = classify_product(name, cat)
        updates.append((main_cat, new_sub, url, cat))
        if main_cat == "Ostatní":
            skipped_unmapped[cat] += 1

    # ── Summary before writing ────────────────────────────────────────────────
    main_counts = defaultdict(int)
    sub_changes = defaultdict(int)
    home_fixed  = 0

    for main, sub, url, old_cat in updates:
        main_counts[main] += 1
        if old_cat == "Home Appliances" and main != "Malé domácí spotřebiče":
            home_fixed += 1
        if sub != old_cat:
            sub_changes[f"{old_cat} → {sub}"] += 1

    print("=== Proposed MainCategory distribution ===")
    for cat, cnt in sorted(main_counts.items(), key=lambda x: -x[1]):
        print(f"  {cnt:>5}  {cat}")

    print(f"\n  Phones/tablets rescued from Home Appliances: {home_fixed}")

    if skipped_unmapped:
        print(f"\n  Categories → 'Ostatní' (no mapping found):")
        for cat, cnt in sorted(skipped_unmapped.items(), key=lambda x: -x[1])[:20]:
            print(f"    {cnt:>4}  {cat!r}")

    if DRY_RUN:
        print("\n[dry-run] No changes written.")
        conn.close()
        return

    # ── Apply updates ─────────────────────────────────────────────────────────
    print("\nWriting updates…")
    cur = conn.cursor()
    for main_cat, new_sub, url, _old_cat in updates:
        if url:
            cur.execute(
                "UPDATE products SET MainCategory=?, Category=? WHERE ProductURL=?",
                (main_cat, new_sub, url)
            )
        else:
            # For products without a URL (rare), match by name+old_category
            cur.execute(
                "UPDATE products SET MainCategory=?, Category=? WHERE Name=? AND Category=?",
                (main_cat, new_sub, _old_cat, _old_cat)
            )

    conn.commit()

    # Verify
    total = conn.execute("SELECT COUNT(*) FROM products").fetchone()[0]
    mapped = conn.execute("SELECT COUNT(*) FROM products WHERE MainCategory IS NOT NULL").fetchone()[0]
    print(f"\n✓  Done. {mapped:,}/{total:,} products have MainCategory set.")

    print("\n=== Final MainCategory counts ===")
    for row in conn.execute(
        "SELECT MainCategory, COUNT(*) FROM products GROUP BY MainCategory ORDER BY COUNT(*) DESC"
    ).fetchall():
        print(f"  {row[1]:>5}  {row[0]}")

    conn.close()

    if not DRY_RUN:
        shutil.copy2(tmp, DB_PATH)
        print(f"\n✓  Copied updated DB back to {DB_PATH}")
    os.remove(tmp)


if __name__ == "__main__":
    run()
