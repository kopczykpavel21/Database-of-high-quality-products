"""
Split amazon_us products into more specific subcategories using
keyword matching on product names.

Updates the Category column in-place. MainCategory is unchanged.

Run once after importing:
  python3 scraper/split_amazon_subcategories.py
  python3 scraper/split_amazon_subcategories.py --dry-run
  python3 scraper/split_amazon_subcategories.py --category Telefony --dry-run
"""

import argparse
import logging
import os
import re
import sqlite3

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s %(message)s")
log = logging.getLogger(__name__)

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "products.db")


# ── Keyword rules ─────────────────────────────────────────────────────────────
# Each entry: (new_subcategory, [keywords_any_of])
# Rules are evaluated IN ORDER — first match wins.
# Keywords are case-insensitive substring matches against the product Name.

RULES = {

    # ── Telefony a tablety (was: "Telefony") ─────────────────────────────────
    "Telefony": [
        ("Příslušenství Apple Watch",["Apple Watch Band","Apple Watch Strap","Watch Band","Watch Strap","iWatch Band"]),
        ("Ochranné fólie",           ["Screen Protector","Tempered Glass","Screen Film","Glass Protector","Protective Film","Screen Guard"]),
        ("Pouzdra a kryty",          ["Phone Case","Phone Cover"," Case for iPhone"," Case for Samsung"," Case for Galaxy",
                                      " Case for Google Pixel"," Case for Motorola"," Case for LG "," Case for Moto",
                                      " Case for OnePlus"," Case for Xiaomi","Phone Shell","Phone Bumper",
                                      "Pouch for","Holster for","Back Cover for","Flip Case","Wallet Case","Leather Case",
                                      "Shockproof Case","Clear Case","Silicone Case","Bumper Case","Hard Case",
                                      "Protective Case","Rugged Case","Armor Case",
                                      "iPhone Case","iPhone Cover","Galaxy Case","Galaxy Cover",
                                      "Pixel Case","Pixel Cover","Moto Case","OnePlus Case",
                                      "for iPhone","for Samsung Galaxy","for Google Pixel",
                                      "for Motorola Moto","Compatible with iPhone","Compatible with Samsung"]),
        ("Nabíječky",                ["Wireless Charger","Fast Charger","USB Charger","Wall Charger","Car Charger",
                                      "Charging Pad","Charging Station","MagSafe Charger","Qi Charger",
                                      "Power Adapter","Charging Adapter","Charging Dock"]),
        ("Kabely",                   ["Lightning Cable","USB-C Cable","USB C Cable","Micro USB Cable","Charging Cable",
                                      "Sync Cable","Data Cable","Phone Cable"]),
        ("Powerbanky",               ["Power Bank","Portable Charger","Battery Pack","Portable Battery"]),
        ("Sluchátka",                ["Earbud","Earphone","Earpiece","AirPod","Headphone","Headset","In-Ear","In Ear",
                                      "Wireless Earbuds","Bluetooth Earbuds","Bluetooth Headphone"]),
        ("Smartwatch",               ["Smartwatch","Smart Watch","Fitness Tracker","Fitness Band","Activity Tracker",
                                      "Garmin","Fitbit","Apple Watch","Galaxy Watch","Mi Band"]),
        ("Držáky a stojany",         ["Phone Mount","Phone Holder","Car Mount","Dashboard Mount","Windshield Mount",
                                      "Phone Stand","Desk Stand","Ring Stand","PopSocket","Phone Grip","Phone Ring"]),
        ("Tablety",                  ["iPad","Tablet","Fire HD","Fire Tablet","Samsung Tab","Galaxy Tab"]),
        ("Smartphones",              ["iPhone","Samsung Galaxy","Galaxy S","Galaxy A","Galaxy Note","Galaxy Z",
                                      "Google Pixel","OnePlus","Xiaomi","Motorola Moto","Moto G","Moto E",
                                      "LG G","LG V","Nokia","Sony Xperia","Huawei","ZTE","Alcatel"]),
    ],

    # ── Elektro (was: "Elektronika") ─────────────────────────────────────────
    "Elektronika": [
        ("Sluchátka",        ["Headphone","Earbud","Earphone","Headset","AirPod","In-Ear","Over-Ear","On-Ear",
                              "Wireless Earbuds","Bluetooth Headphone","Noise Cancelling"]),
        ("Televizory",       [" TV ","Smart TV","4K TV","OLED TV","QLED TV","Television","HDTV","LED TV"]),
        ("Reproduktory",     ["Bluetooth Speaker","Portable Speaker","Soundbar","Sound Bar","Subwoofer",
                              "Home Theater Speaker","Bookshelf Speaker","Studio Monitor"]),
        ("Chytrá domácnost", ["Echo ","Alexa","Google Home","Nest Hub","Smart Plug","Smart Switch",
                              "Smart Bulb","Smart Light","Smart Sensor","Smart Lock","Ring Doorbell",
                              "Security Camera","Smart Hub","Home Automation"]),
        ("Baterie",          ["AA Battery","AAA Battery","9V Battery","CR2032","Lithium Battery","Alkaline Battery",
                              "Rechargeable Battery","Battery Pack","NiMH","Duracell","Energizer"]),
        ("Kabely a adaptéry",["HDMI Cable","HDMI Adapter","DisplayPort","Optical Cable","Coaxial Cable",
                              "Toslink","RCA Cable","AUX Cable","Audio Cable","Ethernet Cable",
                              "USB Hub","USB Adapter","USB Cable","USB-C Adapter","Lightning Adapter",
                              "Power Strip","Surge Protector","Extension Cord"]),
        ("Paměťová média",   ["Flash Drive","USB Drive","Thumb Drive","Pen Drive","Jump Drive",
                              "Memory Card","SD Card","MicroSD","micro SD","CFexpress","CompactFlash"]),
        ("Přehrávače",       ["DVD Player","Blu-ray","Blu Ray","CD Player","Media Player","Streaming Player",
                              "Fire TV Stick","Roku","Chromecast","Apple TV","Android TV Box","TV Box"]),
        ("Projektory",       ["Projector","Mini Projector","Portable Projector","Home Theater Projector"]),
        ("Dálkové ovladače", ["Remote Control","Universal Remote","TV Remote","Replacement Remote"]),
        ("Herní příslušenství",["Gaming Headset","Gaming Mouse","Gaming Keyboard","Game Controller",
                                "Gamepad","Joystick","Gaming Chair"]),
    ],

    # ── Počítače a notebooky (was: "Počítače") ───────────────────────────────
    "Počítače": [
        ("Notebooky",         ["Laptop","Notebook","Netbook","MacBook","Chromebook","Ultrabook","Gaming Laptop"]),
        ("Tablety",           ["iPad","Tablet","Fire HD","Fire Tablet","Samsung Tab","Galaxy Tab","Surface Pro"]),
        ("Monitory",          ["Monitor","IPS Monitor","4K Monitor","Gaming Monitor","Display","LED Monitor",
                               "Curved Monitor","Ultrawide"]),
        ("Pevné disky a SSD", ["SSD","NVMe","M.2 Drive","Hard Drive","HDD","Solid State","Internal Drive",
                               "External Drive","External Hard"]),
        ("Operační paměti",   ["RAM","DDR4","DDR5","DDR3","DIMM","SO-DIMM","Memory Module","PC RAM"]),
        ("Grafické karty",    ["Graphics Card","GPU","Video Card","GeForce","Radeon","RTX","GTX"]),
        ("Klávesnice",        ["Keyboard","Mechanical Keyboard","Wireless Keyboard","Gaming Keyboard",
                               "Bluetooth Keyboard"]),
        ("Myši",              [" Mouse "," Mice ","Gaming Mouse","Wireless Mouse","Bluetooth Mouse",
                               "Optical Mouse","Trackball","Trackpad"]),
        ("Tašky a batohy",    ["Laptop Bag","Laptop Case","Laptop Sleeve","Laptop Backpack",
                               "Notebook Bag","Notebook Case","Computer Bag"]),
        ("Síťové prvky",      ["Router","WiFi Router","Modem","Network Switch","Ethernet Switch",
                               "WiFi Extender","Network Adapter","WiFi Adapter","Network Card"]),
        ("Webkamery",         ["Webcam","Web Camera","Webcam HD","Conference Camera","PC Camera"]),
        ("Dokovací stanice",  ["Docking Station","USB Dock","USB-C Hub","USB Hub","Thunderbolt Dock"]),
        ("Tiskárny",          ["Printer","Inkjet","Laser Printer","3D Printer","Ink Cartridge",
                               "Toner Cartridge"]),
        ("Úložiště",          ["SD Card","MicroSD","micro SD","Memory Card","Flash Drive","USB Drive",
                               "Thumb Drive","Pen Drive","External SSD","Portable SSD"]),
        ("Komponenty",        ["CPU","Processor","Motherboard","Power Supply","PSU","CPU Cooler",
                               "PC Case","ATX Case","ATX Tower","Micro-ATX","MicroATX","Mini-ITX",
                               "Tower Case","Computer Case","Heatsink","PC Fan"]),
    ],

    # ── Foto a video (was: "Fotoaparáty") ────────────────────────────────────
    "Fotoaparáty": [
        ("Fotoaparáty",     ["DSLR","Mirrorless Camera","Digital Camera","Point and Shoot","Point-and-Shoot",
                             "Film Camera","Compact Camera","Instant Camera","Polaroid"]),
        ("Akční kamery",    ["GoPro","Action Camera","Action Cam","360 Camera","360° Camera","Insta360"]),
        ("Drony",           ["Drone","DJI","Quadcopter","UAV","FPV Drone"]),
        ("Videokamery",     ["Camcorder","Video Camera","Videocamera"]),
        ("Webkamery",       ["Webcam","Web Camera","Conference Camera","Streaming Camera","4K Webcam"]),
        ("Objektivy",       ["Camera Lens","DSLR Lens","Mirrorless Lens","Telephoto Lens","Wide Angle Lens",
                             "Prime Lens","Zoom Lens","Macro Lens","Fish Eye","Fisheye"]),
        ("Stativy a stab.", ["Tripod","Monopod","Gimbal","Stabilizer","Camera Rig","Video Rig","Selfie Stick"]),
        ("Blesky",          ["Camera Flash","Speedlight","Strobe","Ring Light","Ring Flash","LED Panel"]),
        ("Filtry",          ["Camera Filter","ND Filter","UV Filter","Polarizing Filter","CPL Filter"]),
        ("Tašky a pouzdra", ["Camera Bag","Camera Case","Camera Backpack","Camera Strap","Camera Holster"]),
        ("Dalekohledce",    ["Binocular","Telescope","Monocular","Spotting Scope","Night Vision"]),
        ("Paměťové karty",  ["SD Card","MicroSD","micro SD","CFexpress","CompactFlash","Memory Card for Camera"]),
        ("Držáky a rigy",   ["Camera Mount","Camera Bracket","Camera Cage","Hot Shoe","Cold Shoe Mount",
                             "Camera Stand","Camera Arm","Camera Desk","Camera Holder","Tripod Mount",
                             "Phone Tripod","iPad Tripod","Tablet Tripod"]),
    ],

    # ── Děti a hračky (was: "Hračky") ────────────────────────────────────────
    "Hračky": [
        ("LEGO a stavebnice",    ["LEGO","Lego ","Building Blocks","Building Bricks","Construction Set",
                                  "Building Set","Brick Set","Mega Bloks","K'nex"]),
        ("Deskové a karetní hry",["Board Game","Card Game","Tabletop Game","Trading Card","MTG","Magic: the Gathering",
                                  "Pokémon","Pokemon Card","Yugioh","Yu-Gi-Oh","Game of Thrones Game",
                                  "Puzzle Game","Trivia","Strategy Game","Dungeon"]),
        ("Puzzle",               ["Jigsaw Puzzle","Jigsaw","Floor Puzzle","3D Puzzle","Wooden Puzzle"]),
        ("Figurky a sběratelství",["Action Figure","Figurine","Funko Pop","Pop Vinyl","Funko ","Statue",
                                   "Collectible Figure","Marvel Figure","DC Figure","Star Wars Figure",
                                   "Anime Figure","Piñata","Mezco","NECA Figure"]),
        ("Panenky",              ["Doll","Barbie","Baby Doll","Fashion Doll","Dollhouse","Doll Accessories"]),
        ("Plyšové hračky",       ["Plush","Stuffed Animal","Stuffed Toy","Teddy Bear","Plushie","Soft Toy"]),
        ("RC modely",            ["Remote Control Car","RC Car","RC Truck","Remote Control Truck",
                                  "RC Drone","Remote Drone","Radio Control","RC Helicopter","RC Boat"]),
        ("Venkovní hračky",      ["Sprinkler","Water Toy","Outdoor Toy","Trampoline","Swing Set",
                                  "Slip and Slide","Sandbox","Water Gun","Super Soaker","Frisbee",
                                  "Kite","Jump Rope","Hula Hoop"]),
        ("Vzdělávací hračky",    ["Educational Toy","Learning Toy","Science Kit","Science Experiment",
                                  "STEM Toy","Math Toy","Alphabet Toy","Number Toy","Flash Card",
                                  "Montessori","Coding Toy"]),
        ("Tvoření a výtvarno",   ["Art Set","Craft Kit","Drawing Set","Paint Set","Coloring","Sketch Pad",
                                  "Slime Kit","Kinetic Sand","Air Dry Clay","Modeling Clay","Bead Kit"]),
        ("Kostýmy a party",      ["Costume","Party Supplies","Party Kit","Party Banner","Tiara","Sash",
                                  "Halloween Costume","Dress Up","Role Play"]),
    ],

    # ── Hudba (was: "Hudební nástroje") ──────────────────────────────────────
    "Hudební nástroje": [
        ("Kytary",       ["Guitar","Acoustic Guitar","Electric Guitar","Bass Guitar","Classical Guitar",
                          "Ukulele","Banjo","Mandolin"]),
        ("Klávesy",      ["Piano","Keyboard","Digital Piano","Electric Piano","Synthesizer","MIDI Controller",
                          "MIDI Keyboard","Organ"]),
        ("Bicí",         ["Drum","Drum Kit","Drum Set","Snare","Cymbal","Hi-Hat","Drum Pad","Electronic Drum"]),
        ("Dechové nástroje",["Flute","Ocarina","Harmonica","Trumpet","Saxophone","Clarinet","Recorder",
                             "Trombone","Tuba"]),
        ("Struny a příslušenství",["Guitar String","Guitar Pick","Guitar Strap","Guitar Tuner","Capo",
                                   "Guitar Cable","Instrument Cable","Music Stand","Sheet Music"]),
        ("Mikrofony",    ["Microphone","Condenser Mic","Dynamic Mic","USB Mic","Vocal Mic","Wireless Mic",
                          "Lavalier","Shotgun Mic"]),
        ("Audio rozhraní",["Audio Interface","Recording Interface","USB Audio","Sound Card","Mixer",
                            "Audio Mixer","Mixing Board","Preamp"]),
        ("Zesilovače",   ["Amplifier","Guitar Amp","Bass Amp","Combo Amp","Tube Amp","Speaker Cab"]),
        ("Sluchátka",    ["Studio Headphone","Monitor Headphone","DJ Headphone","Audio-Technica","Sennheiser",
                          "Beyerdynamic","AKG Headphone"]),
        ("Elektronické hud. nástroje",["Vacuum Tube","Synthesizer","Drum Machine","Sampler","Looper"]),
    ],

    # ── Kreativní práce / Hobby ───────────────────────────────────────────────
    "Kreativní práce": [
        ("Malování a kreslení",  ["Paint Set","Acrylic Paint","Oil Paint","Watercolor","Gouache","Painting Set",
                                   "Drawing Set","Sketch Pad","Sketchbook","Charcoal","Pastel","Colored Pencil",
                                   "Pencil Set","Art Pencil","Marker Set","Copic","Posca","Brush Set",
                                   "Canvas Board","Drawing Tablet","Art Easel"]),
        ("Šití a pletení",       ["Sewing Machine","Sewing Kit","Embroidery","Cross Stitch","Needlepoint",
                                   "Knitting","Crochet","Yarn","Thread","Fabric","Quilting","Felt Fabric",
                                   "Hand Sewing","Needle Set","Sewing Thread","Bobbin"]),
        ("Scrapbooking",         ["Scrapbook","Scrapbooking","Washi Tape","Sticker Sheet","Die Cut",
                                   "Stamp Set","Ink Pad","Stamping","Cardstock"]),
        ("Dřevo a řemesla",      ["Woodworking","Wood Carving","Wood Burning","Pyrography","Wood Craft",
                                   "Balsa Wood","Craft Wood","Resin Kit","Epoxy Resin","Jewelry Making",
                                   "Macrame","Beading","Leather Craft","Basket Weaving"]),
        ("3D tisk a modelování", ["3D Print","3D Filament","PLA Filament","ABS Filament","Resin Printer",
                                   "Air Dry Clay","Polymer Clay","Modeling Clay","Sculpting","Pottery"]),
        ("Háčkování a haptika",  ["Diamond Painting","Paint by Number","Mosaic","Felt Kit","Foam Craft",
                                   "Origami","Paper Craft","Paper Quilling","Decoupage"]),
        ("Tvoření s dětmi",      ["Kids Craft","Children Craft","Craft Kit","Slime Kit","Kinetic Sand",
                                   "Bath Bomb Kit","Soap Making","Candle Making","Tie Dye"]),
    ],

    # ── Průmyslové zboží ──────────────────────────────────────────────────────
    "Průmyslové zboží": [
        ("Měřicí přístroje",     ["Multimeter","Digital Multimeter","Oscilloscope","Clamp Meter",
                                   "Caliper","Micrometer","Tachometer","Thermometer","Hygrometer",
                                   "Anemometer","Flow Meter","Data Logger"]),
        ("Pájení a elektronika", ["Soldering Iron","Soldering Station","Solder Wire","Flux","Desoldering",
                                   "Breadboard","Arduino","Raspberry Pi","ESP32","Jumper Wire",
                                   "Resistor Kit","Capacitor Kit","LED Strip","PCB Board","Power Supply"]),
        ("Pneumatika",           ["Air Compressor","Pneumatic","Air Hose","Air Fitting","Air Gun",
                                   "Spray Gun","Air Tool","Tire Inflator"]),
        ("Bezpečnost a ochrana", ["Safety Glove","Safety Glasses","Hard Hat","Respirator","Face Shield",
                                   "Ear Protection","Safety Vest","Work Boot","Safety Shoe","First Aid"]),
        ("Elektroinstalace",     ["Electrical Tape","Wire Connector","Terminal Block","Cable Tie",
                                   "Heat Shrink","Conduit","Junction Box","Circuit Breaker","Fuse"]),
        ("Skladování",           ["Storage Cabinet","Tool Cabinet","Parts Bin","Parts Organizer",
                                   "Label Maker","Shelving Unit","Warehouse"]),
        ("Čerpadla a motory",    ["Water Pump","Submersible Pump","Electric Motor","Gear Motor",
                                   "Servo Motor","Stepper Motor","Fan Motor"]),
    ],

    # ── Nástroje (tools / hardware) ───────────────────────────────────────────
    "Nástroje": [
        ("Ruční nářadí",         ["Hammer","Wrench","Screwdriver","Plier","Socket Set","Hex Key","Allen Key",
                                   "Torx","Ratchet","Spanner","Tape Measure","Level","Hand Saw","Chisel",
                                   "Box Cutter","Utility Knife","Putty Knife"]),
        ("Elektrické nářadí",    ["Drill","Cordless Drill","Impact Driver","Jigsaw","Circular Saw",
                                   "Reciprocating Saw","Angle Grinder","Sander","Rotary Tool","Dremel",
                                   "Heat Gun","Nail Gun","Power Tool"]),
        ("Zahradní nářadí",      ["Garden Hoe","Garden Rake","Garden Spade","Pruner","Hedge Trimmer",
                                   "Lawn Mower","Leaf Blower","Garden Tool","Weed Puller","Trowel"]),
        ("Šrouby a spojovací mat.",["Screw Set","Bolt Set","Nut Set","Anchor","Fastener","Nail","Washer",
                                    "Rivet","Wood Screw","Machine Screw","Drywall Screw"]),
        ("Lepidla a těsnicí látky",["Wood Glue","Super Glue","Epoxy","Caulk","Sealant","Adhesive",
                                    "Construction Adhesive","Silicone Sealant","Threadlocker"]),
        ("Žebříky a lešení",     ["Ladder","Step Ladder","Extension Ladder","Scaffolding","Step Stool"]),
    ],

    # ── Móda a oblečení ───────────────────────────────────────────────────────
    "Móda": [
        ("Pánské oblečení",      ["Men's Shirt","Men's T-Shirt","Men's Hoodie","Men's Jacket","Men's Pants",
                                   "Men's Shorts","Men's Vest","Men's Sweater","Men's Polo","Men's Suit"]),
        ("Dámské oblečení",      ["Women's Dress","Women's Top","Women's Blouse","Women's Skirt",
                                   "Women's Legging","Women's Jacket","Women's Hoodie","Women's Cardigan",
                                   "Women's Shirt","Women's Sweater"]),
        ("Boty",                 ["Sneaker","Running Shoe","Athletic Shoe","Boot","Sandal","Loafer",
                                   "Oxford Shoe","Ballet Flat","Heel","Flip Flop","Slipper","Mule"]),
        ("Doplňky a šperky",     ["Necklace","Bracelet","Ring","Earring","Pendant","Anklet","Choker",
                                   "Watch Band","Bangle","Brooch","Cufflink"]),
        ("Tašky a batohy",       ["Handbag","Purse","Tote Bag","Shoulder Bag","Backpack","Crossbody",
                                   "Clutch","Wallet","Fanny Pack","Messenger Bag","Duffel Bag"]),
        ("Spodní prádlo a ponožky",["Underwear","Bra","Boxer","Brief","Sock","Sports Bra","Pantie",
                                    "Lingerie","Nightgown","Pajama"]),
        ("Sportovní oblečení",   ["Yoga Pant","Running Shorts","Compression","Athletic Wear","Workout",
                                   "Gym Shirt","Training Jacket","Track Suit","Sports Legging"]),
    ],

    # ── Sport a outdoor ───────────────────────────────────────────────────────
    "Sport": [
        ("Fitness a posilování", ["Dumbbell","Barbell","Kettlebell","Weight Plate","Pull Up Bar","Resistance Band",
                                   "Yoga Mat","Jump Rope","Push Up Board","Ab Roller","Foam Roller",
                                   "Bench Press","Pull-up","Exercise Band"]),
        ("Cyklistika",           ["Bicycle","Bike","Cycling","Helmet","Bike Lock","Bike Pump","Bike Light",
                                   "Bike Stand","Bike Rack","Cycling Glove","Cycling Short","Mountain Bike"]),
        ("Vodní sporty",         ["Kayak","Paddle","Swimming","Snorkel","Wetsuit","Life Jacket","Swim Goggle",
                                   "Pool Float","Inflatable Boat","Surfboard","Paddleboard"]),
        ("Camping a turistika",  ["Tent","Sleeping Bag","Hiking","Backpacking","Trekking Pole","Camping",
                                   "Headlamp","Survival","Paracord","Fire Starter","Hammock","Camp Chair"]),
        ("Zimní sporty",         ["Ski","Snowboard","Snow Boot","Ski Glove","Ski Goggle","Snow Pants",
                                   "Ice Skate","Sledge","Sled"]),
        ("Bojové sporty",        ["Martial Art","Boxing Glove","MMA","Jiu Jitsu","Wrestling","Karate",
                                   "Taekwondo","Punch Bag","Punching Bag","Heavy Bag"]),
        ("Míčové hry",           ["Basketball","Football","Soccer Ball","Tennis Racket","Badminton",
                                   "Ping Pong","Table Tennis","Volleyball","Baseball Bat","Softball"]),
    ],

    # ── Kosmetika a péče o tělo ───────────────────────────────────────────────
    "Kosmetika": [
        ("Péče o pleť",          ["Moisturizer","Face Cream","Serum","Face Wash","Cleanser","Toner",
                                   "Sunscreen","SPF","Eye Cream","Sheet Mask","Face Mask","Exfoliant",
                                   "Retinol","Hyaluronic","Vitamin C Serum"]),
        ("Líčení a make-up",     ["Foundation","Concealer","BB Cream","Mascara","Eyeliner","Eye Shadow",
                                   "Lipstick","Lip Gloss","Blush","Bronzer","Setting Powder","Primer",
                                   "Makeup Brush","Beauty Blender"]),
        ("Vlasová kosmetika",    ["Shampoo","Conditioner","Hair Mask","Hair Oil","Hair Serum","Hair Dye",
                                   "Hair Color","Hairspray","Hair Gel","Mousse","Dry Shampoo","Hair Brush",
                                   "Hair Dryer","Curling Iron","Flat Iron","Straightener"]),
        ("Parfumy",              ["Perfume","Cologne","Eau de Parfum","Eau de Toilette","Fragrance",
                                   "Body Spray","Deodorant Spray"]),
        ("Ústní hygiena",        ["Toothpaste","Toothbrush","Electric Toothbrush","Mouthwash","Floss",
                                   "Whitening Strip","Teeth Whitening","Tongue Scraper"]),
        ("Deodoranty a antiperspiranty",["Deodorant","Antiperspirant","Natural Deodorant"]),
        ("Holení a depilace",    ["Razor","Shaving Cream","Shaving Gel","Aftershave","Electric Shaver",
                                   "Hair Removal","Epilator","Wax Kit","Threading"]),
        ("Manikúra a pedikúra",  ["Nail Polish","Nail Gel","Nail Art","Nail File","Nail Clipper",
                                   "Cuticle","Foot File","Pumice Stone"]),
    ],

    # ── Kancelářské potřeby ───────────────────────────────────────────────────
    "Kancelářské potřeby": [
        ("Psací potřeby",        ["Pen ","Ballpoint","Gel Pen","Fountain Pen","Marker","Highlighter",
                                   "Pencil ","Mechanical Pencil","Eraser","Pencil Case","Pen Set"]),
        ("Papír a notesy",       ["Notebook","Notepad","Sticky Note","Post-it","Graph Paper","Printer Paper",
                                   "Sketchbook","Planner","Journal","Binder","Spiral Notebook"]),
        ("Organizace kanceláře", ["Desk Organizer","File Folder","Binder Clip","Paper Clip","Stapler",
                                   "Tape Dispenser","Scissors","Ruler","Calculator","Hole Punch",
                                   "Inbox Tray","Letter Tray"]),
        ("Tisk a kopírování",    ["Ink Cartridge","Toner","Printer Ink","Inkjet Cartridge","Label Printer",
                                   "Thermal Label","Laminator","Laminating Pouch","Shredder"]),
        ("Školní potřeby",       ["School Supply","Backpack","School Bag","Lunch Box","Pencil Box",
                                   "Index Card","Flash Card","Protractor","Compass Set"]),
    ],

    # ── Elektro / Domácí kino (was: "Domácí kino") ───────────────────────────
    "Domácí kino": [
        ("Soundbary a reproduktory",["Soundbar","Sound Bar","Home Theater Speaker","Subwoofer",
                                     "Surround Sound","5.1","7.1","Bookshelf Speaker","Floor Speaker"]),
        ("Přehrávače",              ["Blu-ray Player","DVD Player","4K Player","Media Player","Fire TV Stick",
                                     "Streaming Stick","Roku","Chromecast","Apple TV","Android TV Box"]),
        ("Projektory",              ["Projector","Home Theater Projector","4K Projector","Mini Projector"]),
        ("Sluchátka",               ["Headphone","Headset","Earbud","Earphone","Noise Cancelling"]),
        ("Příslušenství",           ["Remote Control","HDMI Cable","HDMI Switch","AV Receiver",
                                     "Wall Mount","TV Bracket","Cable Management"]),
    ],

    # ── Dům a zahrada / Domácí potřeby (was: "Domácí potřeby") ───────────────
    "Domácí potřeby": [
        ("Kuchyňské nádobí",  ["Cookware","Pan","Pot","Skillet","Wok","Baking","Cutting Board","Knife",
                                "Kitchen Knife","Chef Knife","Utensil","Spatula","Ladle"]),
        ("Malé spotřebiče",   ["Blender","Mixer","Coffee Maker","Toaster","Microwave","Air Fryer",
                               "Instant Pot","Pressure Cooker","Rice Cooker","Juicer","Food Processor"]),
        ("Úklid",             ["Vacuum","Mop","Broom","Cleaning","Dustpan","Trash Can","Garbage Bag"]),
        ("Svítidla",          ["LED Light","LED Bulb","Smart Bulb","Ring Light","Desk Lamp","Floor Lamp",
                               "Night Light","Ceiling Light"]),
        ("Dekorace",          ["Wall Decor","Wall Art","Picture Frame","Candle","Vase","Figurine",
                               "Home Decor","Shelf","Display Case","Funko","Piñata","Banner","Pennant"]),
        ("Organizace",        ["Storage Box","Storage Bin","Organizer","Drawer","Closet","Shelf Organizer",
                               "Cabinet","Rack","Hanger","Hook"]),
        ("Koupelna",          ["Towel","Bath Mat","Shower","Toilet","Bathroom","Soap Dispenser",
                               "Toothbrush Holder"]),
        ("Zahrada a outdoor", ["Outdoor","Garden","Patio","BBQ","Grill","Lawn","Plant","Planter",
                               "Bird Feeder","Camping","Hammock","Outdoor Movie"]),
    ],
}


# ── Classifier ────────────────────────────────────────────────────────────────
def classify(name: str, category: str):
    """Return new subcategory string, or None if no rule matches."""
    rules = RULES.get(category)
    if not rules:
        return None
    name_lower = name.lower()
    for sub, keywords in rules:
        for kw in keywords:
            if kw.lower() in name_lower:
                return sub
    return None


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Split amazon_us products into subcategories by keyword")
    parser.add_argument("--dry-run", action="store_true", help="Show counts without writing")
    parser.add_argument("--category", help="Only process this Category (e.g. Telefony)")
    args = parser.parse_args()

    conn = sqlite3.connect(DB_PATH, timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=OFF")  # faster for bulk updates

    categories = [args.category] if args.category else list(RULES.keys())

    grand_total = 0
    grand_classified = 0

    for cat in categories:
        if args.category and cat != args.category:
            continue

        rows = conn.execute(
            "SELECT rowid, Name FROM products WHERE source='amazon_us' AND Category=?",
            (cat,)
        ).fetchall()

        if not rows:
            log.info(f"  {cat}: 0 products — skipping")
            continue

        sub_counts: dict[str, int] = {}
        updates: list[tuple[str, int]] = []

        for row_id, name in rows:
            sub = classify(name or "", cat)
            if sub:
                sub_counts[sub] = sub_counts.get(sub, 0) + 1
                updates.append((sub, row_id))
            else:
                sub_counts[f"[{cat}]"] = sub_counts.get(f"[{cat}]", 0) + 1

        classified = sum(v for k, v in sub_counts.items() if not k.startswith("["))
        log.info(f"\n── {cat}  ({len(rows):,} products, {classified:,} classified, {len(rows)-classified:,} unchanged)")
        for sub, n in sorted(sub_counts.items(), key=lambda x: -x[1]):
            pct = 100 * n // len(rows)
            log.info(f"   {sub:45} {n:>7,}  ({pct}%)")

        if not args.dry_run and updates:
            conn.executemany(
                "UPDATE products SET Category=? WHERE rowid=?",
                updates
            )
            conn.commit()

        grand_total += len(rows)
        grand_classified += classified

    if not args.dry_run:
        log.info(f"\n{'='*60}")
        log.info(f"DONE: {grand_classified:,} / {grand_total:,} products reclassified into subcategories")
    else:
        log.info(f"\nDRY RUN — {grand_classified:,} / {grand_total:,} would be reclassified")


if __name__ == "__main__":
    main()
