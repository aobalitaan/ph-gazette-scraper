"""CLI entry point for the SONA scraper."""

import argparse
import asyncio
import sys
from pathlib import Path

from backend.common.logging import setup_logging
from backend.pipeline.scraper.sona_scraper import DEFAULT_DATA_DIR, SONAScraper


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="talasuri-scrape-sona",
        description="Scrape State of the Nation Addresses from the Official Gazette",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-scrape all documents, ignoring previous results",
    )
    parser.add_argument(
        "--index-only",
        action="store_true",
        help="Only fetch and display the SONA index (no scraping)",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=5.0,
        help="Delay in seconds between requests (default: 5.0)",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=DEFAULT_DATA_DIR,
        help=f"Output directory (default: {DEFAULT_DATA_DIR})",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose (DEBUG) logging",
    )
    return parser.parse_args(argv)


async def async_main(args: argparse.Namespace) -> int:
    scraper = SONAScraper(
        data_dir=args.data_dir,
        delay=args.delay,
        force=args.force,
    )

    if args.index_only:
        entries = await scraper.fetch_index_only()
        print(f"Found {len(entries)} SONAs:")
        for entry in entries:
            print(f"  {entry.date}  {entry.president:30s}  {entry.title}")
        return 0

    summary = await scraper.run()
    print("\nScraping complete:")
    print(f"  Total:   {summary.total}")
    print(f"  Success: {summary.success}")
    print(f"  Failed:  {summary.failed}")
    print(f"  Pending: {summary.pending}")
    print(f"  Words:   {summary.total_words:,}")
    if summary.date_range:
        print(f"  Range:   {summary.date_range[0]} to {summary.date_range[1]}")

    return 0 if summary.failed == 0 else 1


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    setup_logging(verbose=args.verbose)
    sys.exit(asyncio.run(async_main(args)))
