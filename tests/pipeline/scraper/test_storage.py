"""Tests for SONA storage layer."""

import json
from datetime import date, datetime
from pathlib import Path

from backend.pipeline.scraper.models import ScrapeStatus, SONADocument
from backend.pipeline.scraper.storage import SONAStorage, president_slug


def _make_doc(
    doc_id: str = "sona_2016_07_25",
    president: str = "Rodrigo Duterte",
    status: ScrapeStatus = ScrapeStatus.SUCCESS,
    text: str | None = "Fellow Filipinos...",
) -> SONADocument:
    return SONADocument(
        doc_id=doc_id,
        president=president,
        date=date(2016, 7, 25),
        title="SONA 2016",
        source_url="https://example.com/sona",
        text=text,
        word_count=2 if text else None,
        paragraph_count=1 if text else None,
        scrape_status=status,
        scraped_at=datetime(2025, 1, 1) if status == ScrapeStatus.SUCCESS else None,
    )


class TestPresidentSlug:
    def test_simple_name(self):
        assert president_slug("Manuel L. Quezon") == "manuel-l-quezon"

    def test_junior(self):
        assert president_slug("Ferdinand R. Marcos Jr.") == "ferdinand-r-marcos-jr"

    def test_hyphenated(self):
        assert president_slug("Gloria Macapagal-Arroyo") == "gloria-macapagal-arroyo"

    def test_roman_numeral(self):
        assert president_slug("Benigno S. Aquino III") == "benigno-s-aquino-iii"


class TestSONAStorage:
    def test_ensure_dirs(self, tmp_path: Path):
        storage = SONAStorage(tmp_path / "sona")
        storage.ensure_dirs()
        assert storage.raw_html_dir.exists()
        assert storage.text_dir.exists()
        assert storage.metadata_dir.exists()

    def test_save_and_read_raw_html(self, tmp_path: Path):
        storage = SONAStorage(tmp_path / "sona")
        storage.ensure_dirs()
        doc = _make_doc()
        html = "<html><body>Hello</body></html>"
        path = storage.save_raw_html(doc, html)
        assert path.exists()
        assert path.read_text() == html
        assert "rodrigo-duterte" in str(path)

    def test_save_and_read_text(self, tmp_path: Path):
        storage = SONAStorage(tmp_path / "sona")
        storage.ensure_dirs()
        doc = _make_doc()
        text = "Fellow Filipinos..."
        path = storage.save_text(doc, text)
        assert path.exists()
        assert path.read_text() == text
        assert "rodrigo-duterte" in str(path)

    def test_save_metadata(self, tmp_path: Path):
        storage = SONAStorage(tmp_path / "sona")
        storage.ensure_dirs()
        doc = _make_doc()
        path = storage.save_metadata(doc)
        assert path.exists()
        loaded = json.loads(path.read_text())
        assert loaded["doc_id"] == "sona_2016_07_25"
        assert loaded["scrape_status"] == "success"
        assert "rodrigo-duterte" in str(path)

    def test_save_document_all_artifacts(self, tmp_path: Path):
        storage = SONAStorage(tmp_path / "sona")
        storage.ensure_dirs()
        doc = _make_doc()
        slug = "rodrigo-duterte"
        storage.save_document(doc, "<html>content</html>")
        assert (storage.raw_html_dir / slug / "sona_2016_07_25.html").exists()
        assert (storage.text_dir / slug / "sona_2016_07_25.txt").exists()
        assert (storage.metadata_dir / slug / "sona_2016_07_25.json").exists()

    def test_save_document_no_text(self, tmp_path: Path):
        storage = SONAStorage(tmp_path / "sona")
        storage.ensure_dirs()
        doc = _make_doc(status=ScrapeStatus.FAILED, text=None)
        slug = "rodrigo-duterte"
        storage.save_document(doc, "<html>error page</html>")
        assert (storage.raw_html_dir / slug / "sona_2016_07_25.html").exists()
        assert not (storage.text_dir / slug / "sona_2016_07_25.txt").exists()
        assert (storage.metadata_dir / slug / "sona_2016_07_25.json").exists()

    def test_different_presidents_different_dirs(self, tmp_path: Path):
        storage = SONAStorage(tmp_path / "sona")
        storage.ensure_dirs()
        doc1 = _make_doc(president="Manuel L. Quezon")
        doc2 = _make_doc(doc_id="sona_2022_07_25", president="Ferdinand R. Marcos Jr.")
        storage.save_metadata(doc1)
        storage.save_metadata(doc2)
        assert (storage.metadata_dir / "manuel-l-quezon" / "sona_2016_07_25.json").exists()
        assert (storage.metadata_dir / "ferdinand-r-marcos-jr" / "sona_2022_07_25.json").exists()


class TestManifest:
    def test_manifest_roundtrip(self, tmp_path: Path):
        storage = SONAStorage(tmp_path / "sona")
        storage.ensure_dirs()
        docs = [
            _make_doc("sona_2016_07_25"),
            _make_doc("sona_1935_11_25"),
        ]
        storage.save_manifest(docs)
        loaded = storage.load_manifest()
        assert len(loaded) == 2
        assert loaded[0].doc_id == "sona_2016_07_25"
        assert loaded[1].doc_id == "sona_1935_11_25"

    def test_load_manifest_missing_file(self, tmp_path: Path):
        storage = SONAStorage(tmp_path / "sona")
        assert storage.load_manifest() == []

    def test_manifest_preserves_all_fields(self, tmp_path: Path):
        storage = SONAStorage(tmp_path / "sona")
        storage.ensure_dirs()
        doc = _make_doc()
        storage.save_manifest([doc])
        loaded = storage.load_manifest()[0]
        assert loaded.president == doc.president
        assert loaded.scrape_status == doc.scrape_status
        assert loaded.scraped_at == doc.scraped_at
        assert loaded.text == doc.text


class TestResumability:
    def test_is_already_scraped_success(self, tmp_path: Path):
        storage = SONAStorage(tmp_path / "sona")
        storage.ensure_dirs()
        doc = _make_doc(status=ScrapeStatus.SUCCESS)
        storage.save_manifest([doc])
        assert storage.is_already_scraped("sona_2016_07_25") is True

    def test_is_already_scraped_failed(self, tmp_path: Path):
        storage = SONAStorage(tmp_path / "sona")
        storage.ensure_dirs()
        doc = _make_doc(status=ScrapeStatus.FAILED)
        storage.save_manifest([doc])
        assert storage.is_already_scraped("sona_2016_07_25") is False

    def test_is_already_scraped_missing(self, tmp_path: Path):
        storage = SONAStorage(tmp_path / "sona")
        storage.ensure_dirs()
        assert storage.is_already_scraped("sona_2016_07_25") is False
