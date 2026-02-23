"""SONA scraper orchestration.

Coordinates fetching the index, parsing individual pages, and saving results.
"""

import asyncio
import logging
import signal
from datetime import UTC, datetime
from pathlib import Path

from backend.pipeline.scraper.http_client import GazetteClient
from backend.pipeline.scraper.models import (
    ScrapeStatus,
    SONACorpusSummary,
    SONADocument,
    SONAIndexEntry,
)
from backend.pipeline.scraper.parsers import parse_sona_index, parse_sona_page
from backend.pipeline.scraper.storage import SONAStorage

logger = logging.getLogger(__name__)

SONA_INDEX_URL = "https://www.officialgazette.gov.ph/past-sona-speeches/"
DEFAULT_DATA_DIR = Path("data/documents-raw/sona")


class SONAScraper:
    """Orchestrates the end-to-end SONA scraping pipeline.

    1. Fetch and parse the index page (always fresh)
    2. Load existing manifest for resumability
    3. Merge: skip already-successful, retry failed/pending
    4. Scrape each pending SONA with rate limiting
    5. Save raw HTML + text + metadata after each document
    6. Save complete manifest at the end
    7. Graceful shutdown on Ctrl+C
    """

    def __init__(
        self,
        data_dir: Path = DEFAULT_DATA_DIR,
        delay: float = 5.0,
        force: bool = False,
    ) -> None:
        self.storage = SONAStorage(data_dir)
        self.delay = delay
        self.force = force
        self._shutdown_requested = False
        self._documents: list[SONADocument] = []

    async def run(self) -> SONACorpusSummary:
        """Execute the full scraping pipeline. Returns a corpus summary."""
        self.storage.ensure_dirs()
        self._setup_signal_handlers()

        async with GazetteClient(delay=self.delay) as client:
            # Step 1: Fetch and parse index
            logger.info("Fetching SONA index from %s", SONA_INDEX_URL)
            index_html = await client.fetch(SONA_INDEX_URL)
            entries = parse_sona_index(index_html)
            logger.info("Found %d SONAs in index", len(entries))

            # Step 2: Build document list with resumability
            self._documents = self._build_document_list(entries)
            pending = [d for d in self._documents if d.scrape_status != ScrapeStatus.SUCCESS]
            logger.info(
                "%d total documents, %d already scraped, %d to scrape",
                len(self._documents),
                len(self._documents) - len(pending),
                len(pending),
            )

            # Step 3: Scrape each pending document
            for i, doc in enumerate(pending):
                if self._shutdown_requested:
                    logger.info("Shutdown requested, saving state...")
                    break

                logger.info(
                    "[%d/%d] Scraping: %s (%s, %s)",
                    i + 1,
                    len(pending),
                    doc.doc_id,
                    doc.president,
                    doc.date,
                )
                await self._scrape_one(client, doc)

        # Step 4: Save manifest
        self.storage.save_manifest(self._documents)
        summary = self._build_summary()
        logger.info(
            "Done. %d success, %d failed, %d pending, %d skipped",
            summary.success,
            summary.failed,
            summary.pending,
            summary.skipped,
        )
        return summary

    async def fetch_index_only(self) -> list[SONAIndexEntry]:
        """Fetch and parse the index without scraping individual pages."""
        async with GazetteClient(delay=self.delay) as client:
            index_html = await client.fetch(SONA_INDEX_URL)
            return parse_sona_index(index_html)

    def _build_document_list(self, entries: list[SONAIndexEntry]) -> list[SONADocument]:
        """Build document list, merging with existing manifest for resumability."""
        existing: dict[str, SONADocument] = {}
        if not self.force:
            for doc in self.storage.load_manifest():
                existing[doc.doc_id] = doc

        documents: list[SONADocument] = []
        for entry in entries:
            if (
                entry.doc_id in existing
                and existing[entry.doc_id].scrape_status == ScrapeStatus.SUCCESS
            ):
                documents.append(existing[entry.doc_id])
            else:
                documents.append(SONADocument.from_index_entry(entry))
        return documents

    async def _scrape_one(self, client: GazetteClient, doc: SONADocument) -> None:
        """Scrape a single SONA page. Errors are non-fatal."""
        try:
            html = await client.fetch(doc.source_url)
            parsed = parse_sona_page(html)

            doc.full_title = parsed["full_title"]
            doc.text = parsed["text"]
            doc.word_count = parsed["word_count"]
            doc.paragraph_count = parsed["paragraph_count"]
            doc.pdf_url = parsed["pdf_url"]
            doc.scrape_status = ScrapeStatus.SUCCESS
            doc.scraped_at = datetime.now(UTC)

            self.storage.save_document(doc, html)
            logger.info(
                "  OK: %d words, %d paragraphs",
                doc.word_count or 0,
                doc.paragraph_count or 0,
            )
        except Exception as e:
            doc.scrape_status = ScrapeStatus.FAILED
            doc.scrape_error = str(e)
            doc.scraped_at = datetime.now(UTC)
            self.storage.save_metadata(doc)
            logger.error("  FAILED: %s — %s", doc.doc_id, e)

    def _build_summary(self) -> SONACorpusSummary:
        """Build corpus summary from current document list."""
        presidents = list(dict.fromkeys(d.president for d in self._documents))
        dates = [d.date for d in self._documents]
        total_words = sum(d.word_count or 0 for d in self._documents)

        return SONACorpusSummary(
            total=len(self._documents),
            success=sum(1 for d in self._documents if d.scrape_status == ScrapeStatus.SUCCESS),
            failed=sum(1 for d in self._documents if d.scrape_status == ScrapeStatus.FAILED),
            pending=sum(1 for d in self._documents if d.scrape_status == ScrapeStatus.PENDING),
            skipped=sum(1 for d in self._documents if d.scrape_status == ScrapeStatus.SKIPPED),
            presidents=presidents,
            date_range=(min(dates), max(dates)) if dates else None,
            total_words=total_words,
        )

    def _setup_signal_handlers(self) -> None:
        """Register signal handlers for graceful shutdown."""
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self._handle_shutdown)
            except NotImplementedError:
                # Windows doesn't support add_signal_handler
                pass

    def _handle_shutdown(self) -> None:
        """Handle Ctrl+C by flagging shutdown (save state before exiting)."""
        if self._shutdown_requested:
            logger.warning("Force shutdown requested, exiting immediately")
            raise SystemExit(1)
        logger.info("Shutdown requested, finishing current document...")
        self._shutdown_requested = True
