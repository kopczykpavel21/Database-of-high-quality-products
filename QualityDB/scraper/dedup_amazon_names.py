"""
Name-based deduplication for amazon_us products.

Collapses product variants (same cable in 3ft/6ft/10ft, same tablet in
Pink/Blue/Purple, etc.) into a single representative product per name family.

Strategy:
  - Normalize each product name to its first PREFIX_LEN characters
    (lowercased, whitespace-collapsed)
  - Within each (name_prefix, Category) group, keep the row with the
    highest ReviewsCount; delete the rest
  - PREFIX_LEN=70 is the sweet spot: long enough to distinguish genuinely
    different products, short enough to catch color/length variants

Run:
  python3 scraper/dedup_amazon_names.py --dry-run   # preview
  python3 scraper/dedup_amazon_names.py             # apply
  python3 scraper/dedup_amazon_names.py --prefix 60 # tune prefix length
  python3 scraper/dedup_amazon_names.py --show-groups 20  # inspect groups
"""

import argparse
import logging
import os
import re
import sqlite3

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s %(message)s")
log = logging.getLogger(__name__)

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "products.db")

DEFAULT_PREFIX = 70   # characters


def normalize(name: str, prefix_len: int) -> str:
    """Lowercase, collapse whitespace, truncate to prefix_len chars."""
    name = re.sub(r"\s+", " ", (name or "").strip().lower())
    return name[:prefix_len]


def main():
    parser = argparse.ArgumentParser(description="Name-based dedup of amazon_us product variants")
    parser.add_argument("--dry-run",     action="store_true", help="Show counts without deleting")
    parser.add_argument("--prefix",      type=int, default=DEFAULT_PREFIX,
                        help=f"Name prefix length for grouping (default: {DEFAULT_PREFIX})")
    parser.add_argument("--show-groups", type=int, default=0, metavar="N",
                        help="Print N largest duplicate groups then exit (for tuning)")
    args = parser.parse_args()

    conn = sqlite3.connect(DB_PATH, timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=OFF")

    log.info(f"Loading all amazon_us products (prefix_len={args.prefix})...")
    rows = conn.execute(
        "SELECT rowid, Name, Category, ReviewsCount FROM products WHERE source='amazon_us'"
    ).fetchall()
    log.info(f"  {len(rows):,} products loaded")

    # Group by (name_prefix, category)
    groups: dict[tuple, list[tuple]] = {}
    for row_id, name, category, reviews in rows:
        key = (normalize(name, args.prefix), category or "")
        groups.setdefault(key, []).append((row_id, reviews or 0))

    # Find groups with more than one member
    dup_groups = {k: v for k, v in groups.items() if len(v) > 1}
    total_to_delete = sum(len(v) - 1 for v in dup_groups.values())

    log.info(f"  Duplicate name groups found: {len(dup_groups):,}")
    log.info(f"  Rows to delete (keeping best per group): {total_to_delete:,}")
    log.info(f"  Products remaining after dedup: {len(rows) - total_to_delete:,}")

    if args.show_groups:
        log.info(f"\nTop {args.show_groups} largest duplicate groups:")
        sorted_groups = sorted(dup_groups.items(), key=lambda x: -len(x[1]))
        for (prefix, cat), members in sorted_groups[:args.show_groups]:
            best_id = max(members, key=lambda x: x[1])[0]
            best_reviews = max(m[1] for m in members)
            # Get full names for display (using rowids)
            ids = [m[0] for m in members]
            placeholders = ",".join("?" * len(ids))
            full_names = conn.execute(
                f"SELECT Name FROM products WHERE rowid IN ({placeholders})", ids
            ).fetchall()
            log.info(f"\n  [{cat}] {len(members)} variants — keeping rowid={best_id} ({best_reviews:,} reviews)")
            for fn in full_names[:5]:
                log.info(f"    • {fn[0][:100]}")
            if len(full_names) > 5:
                log.info(f"    … and {len(full_names)-5} more")
        return

    if total_to_delete == 0:
        log.info("Nothing to deduplicate.")
        conn.close()
        return

    if args.dry_run:
        log.info("\nDRY RUN — no changes written.")
        # Show sample of what would be deleted
        log.info("\nSample groups (5 largest):")
        sorted_groups = sorted(dup_groups.items(), key=lambda x: -len(x[1]))
        for (prefix, cat), members in sorted_groups[:5]:
            best_reviews = max(m[1] for m in members)
            ids = [m[0] for m in members]
            placeholders = ",".join("?" * len(ids))
            full_names = conn.execute(
                f"SELECT Name FROM products WHERE rowid IN ({placeholders})", ids
            ).fetchall()
            log.info(f"\n  [{cat}] {len(members)} variants, best has {best_reviews:,} reviews")
            for fn in full_names[:4]:
                log.info(f"    {fn[0][:100]}")
        conn.close()
        return

    # Build set of ids to DELETE (all group members EXCEPT the one with max reviews)
    ids_to_delete = []
    for members in dup_groups.values():
        # Keep the member with the highest review count (highest id as tiebreak)
        keep_id = max(members, key=lambda x: (x[1], x[0]))[0]
        for row_id, _ in members:
            if row_id != keep_id:
                ids_to_delete.append(row_id)

    log.info(f"\nDeleting {len(ids_to_delete):,} duplicate variant rows...")

    # Delete in chunks to avoid SQLite variable limit
    CHUNK = 900
    deleted = 0
    for i in range(0, len(ids_to_delete), CHUNK):
        chunk = ids_to_delete[i:i + CHUNK]
        placeholders = ",".join("?" * len(chunk))
        conn.execute(f"DELETE FROM products WHERE rowid IN ({placeholders})", chunk)
        deleted += len(chunk)
        if deleted % 50000 == 0:
            conn.commit()
            log.info(f"  …{deleted:,} deleted")

    conn.commit()
    remaining = conn.execute(
        "SELECT COUNT(*) FROM products WHERE source='amazon_us'"
    ).fetchone()[0]
    log.info(f"Done. Deleted {deleted:,} rows. amazon_us products remaining: {remaining:,}")
    conn.close()


if __name__ == "__main__":
    main()
