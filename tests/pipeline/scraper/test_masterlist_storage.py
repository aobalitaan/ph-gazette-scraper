"""Tests for masterlist storage layer."""

import json
from datetime import date, datetime
from pathlib import Path

from backend.pipeline.scraper.masterlist_storage import MasterlistStorage
from backend.pipeline.scraper.models import (
    DocumentCategory,
    MasterlistDocument,
    MasterlistEntry,
    ScrapeStatus,
)


def _make_entry(**overrides) -> MasterlistEntry:
    defaults = {
        "title": "Proclamation No. 1, s. 2016",
        "content_url": "https://www.officialgazette.gov.ph/2016/06/30/proclamation-no-1-s-2016/",
        "president_slug": "rodrigo-roa-duterte",
        "date": date(2016, 6, 30),
        "category_slug": "proclamations",
    }
    defaults.update(overrides)
    return MasterlistEntry(**defaults)


def _make_doc(
    doc_id: str = "proclamation-no-1-s-2016",
    status: ScrapeStatus = ScrapeStatus.SUCCESS,
    text: str | None = "WHEREAS the people...",
) -> MasterlistDocument:
    return MasterlistDocument(
        doc_id=doc_id,
        category=DocumentCategory.PROCLAMATION,
        category_slug="proclamations",
        president_slug="rodrigo-roa-duterte",
        date=date(2016, 6, 30),
        title="Proclamation No. 1, s. 2016",
        content_url="https://example.com/proclamation",
        text=text,
        word_count=4 if text else None,
        has_html_content=text is not None,
        is_pdf_only=text is None,
        scrape_status=status,
        scraped_at=datetime(2025, 1, 1) if status == ScrapeStatus.SUCCESS else None,
    )


class TestMasterlistStorage:
    def test_ensure_dirs(self, tmp_path: Path):
        storage = MasterlistStorage(tmp_path / "masterlist")
        storage.ensure_dirs()
        assert storage.raw_html_dir.exists()
        assert storage.text_dir.exists()
        assert storage.metadata_dir.exists()
        assert storage.pdf_dir.exists()
        assert storage.index_dir.exists()

    def test_save_and_read_raw_html(self, tmp_path: Path):
        storage = MasterlistStorage(tmp_path / "masterlist")
        storage.ensure_dirs()
        doc = _make_doc()
        html = "<html><body>Hello</body></html>"
        path = storage.save_raw_html(doc, html)
        assert path.exists()
        assert path.read_text() == html
        assert "proclamations" in str(path)
        assert "rodrigo-roa-duterte" in str(path)

    def test_save_and_read_text(self, tmp_path: Path):
        storage = MasterlistStorage(tmp_path / "masterlist")
        storage.ensure_dirs()
        doc = _make_doc()
        path = storage.save_text(doc, "WHEREAS the people...")
        assert path.exists()
        assert path.read_text() == "WHEREAS the people..."

    def test_save_metadata(self, tmp_path: Path):
        storage = MasterlistStorage(tmp_path / "masterlist")
        storage.ensure_dirs()
        doc = _make_doc()
        path = storage.save_metadata(doc)
        assert path.exists()
        loaded = json.loads(path.read_text())
        assert loaded["doc_id"] == "proclamation-no-1-s-2016"
        assert loaded["category"] == "proclamation"
        assert loaded["category_slug"] == "proclamations"

    def test_save_document_all_artifacts(self, tmp_path: Path):
        storage = MasterlistStorage(tmp_path / "masterlist")
        storage.ensure_dirs()
        doc = _make_doc()
        storage.save_document(doc, "<html>content</html>")
        cat, pres = "proclamations", "rodrigo-roa-duterte"
        doc_id = "proclamation-no-1-s-2016"
        assert (storage.raw_html_dir / cat / pres / f"{doc_id}.html").exists()
        assert (storage.text_dir / cat / pres / f"{doc_id}.txt").exists()
        assert (storage.metadata_dir / cat / pres / f"{doc_id}.json").exists()

    def test_save_document_no_text(self, tmp_path: Path):
        storage = MasterlistStorage(tmp_path / "masterlist")
        storage.ensure_dirs()
        doc = _make_doc(status=ScrapeStatus.SUCCESS, text=None)
        storage.save_document(doc, "<html>pdf viewer</html>")
        cat, pres = "proclamations", "rodrigo-roa-duterte"
        doc_id = "proclamation-no-1-s-2016"
        assert (storage.raw_html_dir / cat / pres / f"{doc_id}.html").exists()
        assert not (storage.text_dir / cat / pres / f"{doc_id}.txt").exists()
        assert (storage.metadata_dir / cat / pres / f"{doc_id}.json").exists()


class TestMasterlistIndex:
    def test_save_and_load_index(self, tmp_path: Path):
        storage = MasterlistStorage(tmp_path / "masterlist")
        storage.ensure_dirs()
        entries = [_make_entry(), _make_entry(title="Proc No. 2")]
        storage.save_index("proclamations", "rodrigo-roa-duterte", entries)
        loaded = storage.load_index("proclamations", "rodrigo-roa-duterte")
        assert loaded is not None
        assert len(loaded) == 2
        assert loaded[0].title == "Proclamation No. 1, s. 2016"

    def test_load_index_missing(self, tmp_path: Path):
        storage = MasterlistStorage(tmp_path / "masterlist")
        assert storage.load_index("speeches", "nobody") is None

    def test_load_all_index_entries(self, tmp_path: Path):
        storage = MasterlistStorage(tmp_path / "masterlist")
        storage.ensure_dirs()
        entries1 = [_make_entry()]
        entries2 = [_make_entry(category_slug="speeches", president_slug="fidel-v-ramos")]
        storage.save_index("proclamations", "rodrigo-roa-duterte", entries1)
        storage.save_index("speeches", "fidel-v-ramos", entries2)
        all_entries = storage.load_all_index_entries()
        assert len(all_entries) == 2

    def test_load_all_index_empty(self, tmp_path: Path):
        storage = MasterlistStorage(tmp_path / "masterlist")
        assert storage.load_all_index_entries() == []


class TestMasterlistManifest:
    def test_manifest_roundtrip(self, tmp_path: Path):
        storage = MasterlistStorage(tmp_path / "masterlist")
        storage.ensure_dirs()
        docs = [_make_doc(), _make_doc(doc_id="proclamation-no-2")]
        storage.save_manifest(docs)
        loaded = storage.load_manifest()
        assert len(loaded) == 2
        assert loaded[0].doc_id == "proclamation-no-1-s-2016"

    def test_load_manifest_missing_file(self, tmp_path: Path):
        storage = MasterlistStorage(tmp_path / "masterlist")
        assert storage.load_manifest() == []

    def test_manifest_preserves_all_fields(self, tmp_path: Path):
        storage = MasterlistStorage(tmp_path / "masterlist")
        storage.ensure_dirs()
        doc = _make_doc()
        storage.save_manifest([doc])
        loaded = storage.load_manifest()[0]
        assert loaded.category_slug == doc.category_slug
        assert loaded.has_html_content == doc.has_html_content
        assert loaded.scrape_status == doc.scrape_status


class TestMasterlistResumability:
    def test_get_scraped_doc_ids(self, tmp_path: Path):
        storage = MasterlistStorage(tmp_path / "masterlist")
        storage.ensure_dirs()
        docs = [
            _make_doc(doc_id="doc-1", status=ScrapeStatus.SUCCESS),
            _make_doc(doc_id="doc-2", status=ScrapeStatus.FAILED),
            _make_doc(doc_id="doc-3", status=ScrapeStatus.PENDING),
        ]
        storage.save_manifest(docs)
        scraped = storage.get_scraped_doc_ids()
        assert scraped == {"doc-1"}

    def test_get_scraped_doc_ids_empty(self, tmp_path: Path):
        storage = MasterlistStorage(tmp_path / "masterlist")
        assert storage.get_scraped_doc_ids() == set()


class TestMasterlistPdfStorage:
    def test_save_and_load_pdf(self, tmp_path: Path):
        storage = MasterlistStorage(tmp_path / "masterlist")
        storage.ensure_dirs()
        doc = _make_doc()
        pdf_bytes = b"%PDF-1.4 fake pdf content"
        path = storage.save_pdf(doc, pdf_bytes)
        assert path.exists()
        assert "proclamations" in str(path)
        assert "rodrigo-roa-duterte" in str(path)
        assert path.suffix == ".pdf"
        loaded = storage.load_pdf(doc)
        assert loaded == pdf_bytes

    def test_load_pdf_missing(self, tmp_path: Path):
        storage = MasterlistStorage(tmp_path / "masterlist")
        storage.ensure_dirs()
        doc = _make_doc()
        assert storage.load_pdf(doc) is None

    def test_has_pdf(self, tmp_path: Path):
        storage = MasterlistStorage(tmp_path / "masterlist")
        storage.ensure_dirs()
        doc = _make_doc()
        assert storage.has_pdf(doc) is False
        storage.save_pdf(doc, b"%PDF-1.4 content")
        assert storage.has_pdf(doc) is True

    def test_save_pdf_creates_subdirs(self, tmp_path: Path):
        storage = MasterlistStorage(tmp_path / "masterlist")
        storage.ensure_dirs()
        doc = _make_doc()
        storage.save_pdf(doc, b"data")
        expected = (
            storage.pdf_dir / "proclamations" / "rodrigo-roa-duterte"
            / "proclamation-no-1-s-2016.pdf"
        )
        assert expected.exists()
