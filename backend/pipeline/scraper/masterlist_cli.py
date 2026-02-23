"""CLI entry point for the masterlist scraper."""

import argparse
import asyncio
import logging
import sys
from pathlib import Path

from backend.common.logging import setup_logging
from backend.pipeline.scraper.masterlist_scraper import DEFAULT_DATA_DIR, MasterlistScraper
from backend.pipeline.scraper.models import MASTERLIST_CATEGORY_MAP, MASTERLIST_PRESIDENT_SLUGS

logger = logging.getLogger(__name__)


def parse_proxy_file(path: Path) -> list[str]:
    """Read a proxy file and convert Webshare format to httpx-compatible URLs.

    Each non-empty, non-comment line should be: host:port:user:pass
    Returns list of URLs like: http://user:pass@host:port
    """
    proxies: list[str] = []
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split(":")
        if len(parts) != 4:
            logger.warning("Skipping malformed proxy line: %s", line)
            continue
        host, port, user, password = parts
        proxies.append(f"http://{user}:{password}@{host}:{port}")
    return proxies


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="talasuri-scrape-masterlist",
        description="Scrape presidential documents from the Official Gazette masterlist generator",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-scrape all documents, ignoring previous results",
    )
    parser.add_argument(
        "--index-only",
        action="store_true",
        help="Only run Phase A (collect metadata indexes, no content fetching)",
    )
    parser.add_argument(
        "--content-only",
        action="store_true",
        help="Only run Phase B (fetch content from existing indexes)",
    )
    parser.add_argument(
        "--categories",
        nargs="+",
        choices=list(MASTERLIST_CATEGORY_MAP.keys()),
        metavar="CATEGORY",
        help="Only scrape specific categories (default: all 15)",
    )
    parser.add_argument(
        "--presidents",
        nargs="+",
        choices=MASTERLIST_PRESIDENT_SLUGS,
        metavar="PRESIDENT",
        help="Only scrape specific presidents (default: all)",
    )
    parser.add_argument(
        "-c", "--concurrency",
        type=int,
        default=3,
        help="Number of concurrent workers for Phase B content fetching (default: 3)",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=5.0,
        help="Base delay in seconds per worker between requests, with ±50%% jitter (default: 5.0)",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=DEFAULT_DATA_DIR,
        help=f"Output directory (default: {DEFAULT_DATA_DIR})",
    )
    parser.add_argument(
        "--proxy-file",
        type=Path,
        default=None,
        help="Path to proxy list file (one per line: host:port:user:pass)",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose (DEBUG) logging",
    )
    return parser.parse_args(argv)


async def async_main(args: argparse.Namespace) -> int:
    proxy_urls: list[str] = []
    if args.proxy_file:
        proxy_urls = parse_proxy_file(args.proxy_file)
        if not proxy_urls:
            print(f"Error: no valid proxies found in {args.proxy_file}", file=sys.stderr)
            return 1
        # Auto-set concurrency to match proxy count (one worker per IP)
        args.concurrency = len(proxy_urls)
        logger.info(
            "Loaded %d proxies, setting concurrency to %d",
            len(proxy_urls), args.concurrency,
        )

    scraper = MasterlistScraper(
        data_dir=args.data_dir,
        delay=args.delay,
        force=args.force,
        categories=args.categories,
        presidents=args.presidents,
        concurrency=args.concurrency,
        proxies=proxy_urls,
    )

    if args.index_only:
        total = await scraper.run_phase_a_only()
        print(f"\nPhase A complete: {total} entries indexed")
        return 0

    if args.content_only:
        summary = await scraper.run_phase_b_only()
    else:
        summary = await scraper.run()

    print("\nScraping complete:")
    print(f"  Total:          {summary.total}")
    print(f"  Success:        {summary.success}")
    print(f"  Failed:         {summary.failed}")
    print(f"  Pending:        {summary.pending}")
    print(f"  PDF-only:       {summary.pdf_only}")
    print(f"  HTML with text: {summary.html_with_text}")
    print(f"  Total words:    {summary.total_words:,}")

    if summary.by_category:
        print("\n  By category:")
        for cat, count in sorted(summary.by_category.items()):
            print(f"    {cat}: {count}")

    if summary.by_president:
        print("\n  By president:")
        for pres, count in sorted(summary.by_president.items()):
            print(f"    {pres}: {count}")

    return 0 if summary.failed == 0 else 1


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    setup_logging(verbose=args.verbose)
    sys.exit(asyncio.run(async_main(args)))
