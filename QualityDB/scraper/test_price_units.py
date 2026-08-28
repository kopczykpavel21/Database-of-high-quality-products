#!/usr/bin/env python3
"""
test_price_units.py
───────────────────
Regression guard for the EUR price-scale bug that corrupted otto_de,
mediamarkt and saturn_de rows in products.db.

Every literal below was captured from a LIVE page on 2026-08-26, so these are
not invented fixtures -- they are the exact shapes the retailers actually emit:

  MediaMarkt / Saturn  (JSON-LD offers.price on both search and product pages)
      399      int    -> EUR 399.00   BAUKNECHT BPW 9A114
      279      int    -> EUR 279.00   HAIER HW70-BP14929
      19       int    -> EUR 19.00    APPLE EarPods (USB-C)
      579.99   float  -> EUR 579.99   SIEMENS WU14UT28
      189.99   float  -> EUR 189.99   OK. OWM 6112 A

  Otto  (JSON-LD offers.price -- always a 2-decimal STRING)
      "439.00"  str   -> EUR 439.00   BAUKNECHT Super Eco 9464
      "15.10"   str   -> EUR 15.10    APPLE EarPods (USB-C)
      "1309.99" str   -> EUR 1309.99  Apple iPhone 17 Pro Max

The two historic failure modes both showed up as a power-of-ten slip:
  * dividing an int by 100 on the theory it was cents:  399   -> 3.99
  * stripping every dot out of a decimal string:       "579.99" -> 57999

Run:  python3 test_price_units.py
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import mediamarkt_scraper as mm
import otto_scraper_v2 as otto_v2
import otto_scraper as otto_v1

FAILURES = []


def check(label, got, want):
    ok = (got == want) or (
        got is not None and want is not None and abs(got - want) < 0.005
    )
    if not ok:
        FAILURES.append(f"{label}: got {got!r}, expected {want!r}")
    print(f"  {'ok  ' if ok else 'FAIL'}  {label:<52} -> {got!r}")


def main():
    print("MediaMarkt / Saturn -- JSON-LD offers.price is EUROS (int or float)")
    for raw, want in [(399, 399.0), (279, 279.0), (19, 19.0),
                      (449, 449.0), (333, 333.0), (499, 499.0),
                      (579.99, 579.99), (189.99, 189.99), (299.99, 299.99),
                      ("399", 399.0), ("579.99", 579.99)]:
        check(f"mm.parse_eur({raw!r})", mm.parse_eur(raw), want)

    print("\nMediaMarkt -- German formatted strings")
    for raw, want in [("939,99 €", 939.99), ("1.234,56 €", 1234.56),
                      ("1,234.56", 1234.56), ("62,90", 62.90)]:
        check(f"mm.parse_eur({raw!r})", mm.parse_eur(raw), want)

    print("\nOtto -- offers.price is a 2-decimal EURO string")
    for mod, tag in ((otto_v2, "otto_v2"), (otto_v1, "otto_v1")):
        for raw, want in [("439.00", 439.0), ("15.10", 15.10),
                          ("1309.99", 1309.99), ("106.00", 106.0),
                          ("1.019,00", 1019.0), ("62,90", 62.90),
                          ("1299", 1299.0)]:
            check(f"{tag}.parse_eur({raw!r})", mod.parse_eur(raw), want)

    print("\nThe two historic corruptions must NOT reappear")
    # int euros must not be read as cents
    check("399 is not 3.99", mm.parse_eur(399), 399.0)
    check("19 is not 0.19", mm.parse_eur(19), 19.0)
    check("'439.00' is not 43900", otto_v2.parse_eur("439.00"), 439.0)
    check("'439.00' is not 43900 (v1)", otto_v1.parse_eur("439.00"), 439.0)
    # decimal strings must keep their decimal point
    check("'579.99' is not 57999", mm.parse_eur("579.99"), 579.99)
    check("'107.2' is not 1072", mm.parse_eur("107.2"), 107.2)

    print("\nThe sanity band is a backstop -- it catches the obvious slips")
    for mod, tag in ((mm, "mm"), (otto_v2, "otto_v2"), (otto_v1, "otto_v1")):
        check(f"{tag}.sane_eur(43900.0) dot-strip of '439.00'", mod.sane_eur(43900.0, "x"), None)
        check(f"{tag}.sane_eur(57999.0) dot-strip of '579.99'", mod.sane_eur(57999.0, "x"), None)
        check(f"{tag}.sane_eur(0.19) int 19 read as cents", mod.sane_eur(0.19, "x"), None)
        check(f"{tag}.sane_eur(439.0) legitimate", mod.sane_eur(439.0, "x"), 439.0)
        check(f"{tag}.sane_eur(7928.94) dearest real item", mod.sane_eur(7928.94, "x"), 7928.94)

    print("\n...but it cannot catch every slip, and does not pretend to:")
    # 59.99 EUR inflated by 100 lands at 5999, inside any usable retail band.
    # Only the parser prevents this one, which is why parse_eur carries the fix.
    check("mm.sane_eur(5999.0) undetectable by magnitude", mm.sane_eur(5999.0, "x"), 5999.0)

    print()
    if FAILURES:
        print(f"{len(FAILURES)} FAILURE(S):")
        for f in FAILURES:
            print("  -", f)
        return 1
    print("all price-unit checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
