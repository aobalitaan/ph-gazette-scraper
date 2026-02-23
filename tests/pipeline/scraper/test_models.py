"""Tests for scraper data models."""

import json
from datetime import date, datetime

from backend.pipeline.scraper.models import (
    MASTERLIST_CATEGORY_MAP,
    MASTERLIST_PRESIDENT_SLUGS,
    DocumentCategory,
    MasterlistCorpusSummary,
    MasterlistDocument,
    MasterlistEntry,
    ScrapeStatus,
    SONACorpusSummary,
    SONADocument,
    SONAIndexEntry,
)


class TestSONAIndexEntry:
    def test_doc_id_generation(self):
        entry = SONAIndexEntry(
            president="Manuel L. Quezon",
            date=date(1935, 11, 25),
            title="First SONA",
            url="https://www.officialgazette.gov.ph/1935/11/25/first-sona/",
        )
        assert entry.doc_id == "sona_1935_11_25"

    def test_optional_fields(self):
        entry = SONAIndexEntry(
            president="Rodrigo Duterte",
            date=date(2016, 7, 25),
            title="SONA 2016",
            url="https://example.com/sona",
            venue="Batasang Pambansa",
            legislature="17th Congress",
        )
        assert entry.venue == "Batasang Pambansa"
        assert entry.legislature == "17th Congress"

    def test_serialization_roundtrip(self):
        entry = SONAIndexEntry(
            president="Gloria Macapagal Arroyo",
            date=date(2001, 7, 23),
            title="SONA 2001",
            url="https://example.com/sona",
            venue="Batasang Pambansa",
            legislature="12th Congress",
        )
        data = json.loads(entry.model_dump_json())
        restored = SONAIndexEntry(**data)
        assert restored.president == entry.president
        assert restored.date == entry.date
        assert restored.doc_id == entry.doc_id


class TestSONADocument:
    def test_from_index_entry(self):
        entry = SONAIndexEntry(
            president="Benigno Aquino III",
            date=date(2010, 7, 26),
            title="SONA 2010",
            url="https://example.com/sona",
            venue="Batasang Pambansa",
            legislature="15th Congress",
        )
        doc = SONADocument.from_index_entry(entry)
        assert doc.doc_id == "sona_2010_07_26"
        assert doc.category == DocumentCategory.SONA
        assert doc.president == "Benigno Aquino III"
        assert doc.scrape_status == ScrapeStatus.PENDING
        assert doc.text is None

    def test_serialization_roundtrip(self):
        doc = SONADocument(
            doc_id="sona_2016_07_25",
            president="Rodrigo Duterte",
            date=date(2016, 7, 25),
            title="SONA 2016",
            source_url="https://example.com/sona",
            full_title="First State of the Nation Address of President Rodrigo Duterte",
            text="Fellow Filipinos...",
            word_count=5000,
            paragraph_count=50,
            scrape_status=ScrapeStatus.SUCCESS,
            scraped_at=datetime(2025, 1, 1, 12, 0, 0),
        )
        data = json.loads(doc.model_dump_json())
        restored = SONADocument(**data)
        assert restored.doc_id == doc.doc_id
        assert restored.scrape_status == ScrapeStatus.SUCCESS
        assert restored.word_count == 5000
        assert restored.scraped_at == doc.scraped_at

    def test_default_category_is_sona(self):
        doc = SONADocument(
            doc_id="sona_1935_11_25",
            president="Manuel L. Quezon",
            date=date(1935, 11, 25),
            title="First SONA",
            source_url="https://example.com/sona",
        )
        assert doc.category == DocumentCategory.SONA


class TestSONACorpusSummary:
    def test_defaults(self):
        summary = SONACorpusSummary()
        assert summary.total == 0
        assert summary.success == 0
        assert summary.presidents == []
        assert summary.date_range is None

    def test_with_data(self):
        summary = SONACorpusSummary(
            total=85,
            success=83,
            failed=2,
            presidents=["Quezon", "Marcos"],
            date_range=(date(1935, 11, 25), date(2025, 7, 28)),
            total_words=500000,
        )
        assert summary.total == 85
        assert summary.date_range[0].year == 1935


# ---------------------------------------------------------------------------
# Masterlist models
# ---------------------------------------------------------------------------


class TestMasterlistConstants:
    def test_category_map_has_15_entries(self):
        assert len(MASTERLIST_CATEGORY_MAP) == 15

    def test_all_category_map_values_are_document_categories(self):
        for slug, cat in MASTERLIST_CATEGORY_MAP.items():
            assert isinstance(cat, DocumentCategory), f"{slug} maps to non-category {cat}"

    def test_president_slugs_has_entries(self):
        assert len(MASTERLIST_PRESIDENT_SLUGS) == 15
        assert "rodrigo-roa-duterte" in MASTERLIST_PRESIDENT_SLUGS
        assert "ferdinand-e-marcos" in MASTERLIST_PRESIDENT_SLUGS
        assert "ferdinand-r-marcos" in MASTERLIST_PRESIDENT_SLUGS


class TestMasterlistEntry:
    def _make_entry(self, **overrides) -> MasterlistEntry:
        defaults = {
            "title": "Administrative Order No. 48, s. 2022",
            "content_url": "https://www.officialgazette.gov.ph/2022/05/11/administrative-order-no-48-s-2022/",
            "president_slug": "ferdinand-r-marcos",
            "date": date(2022, 5, 11),
            "pdf_url": "https://www.officialgazette.gov.ph/downloads/2022/05may/20220511-AO-48-RRD.pdf",
            "category_slug": "administrative-orders",
        }
        defaults.update(overrides)
        return MasterlistEntry(**defaults)

    def test_doc_id_from_url(self):
        entry = self._make_entry()
        assert entry.doc_id == "administrative-order-no-48-s-2022"

    def test_doc_id_no_trailing_slash(self):
        entry = self._make_entry(
            content_url="https://www.officialgazette.gov.ph/2022/05/11/some-doc"
        )
        assert entry.doc_id == "some-doc"

    def test_category_computed(self):
        entry = self._make_entry(category_slug="executive-orders")
        assert entry.category == DocumentCategory.EXECUTIVE_ORDER

    def test_pdf_url_optional(self):
        entry = self._make_entry(pdf_url=None)
        assert entry.pdf_url is None

    def test_serialization_roundtrip(self):
        entry = self._make_entry()
        data = json.loads(entry.model_dump_json())
        restored = MasterlistEntry(**data)
        assert restored.doc_id == entry.doc_id
        assert restored.category == entry.category
        assert restored.date == entry.date


class TestMasterlistDocument:
    def test_from_entry(self):
        entry = MasterlistEntry(
            title="Proclamation No. 1",
            content_url="https://www.officialgazette.gov.ph/1935/11/15/proclamation-no-1/",
            president_slug="manuel-l-quezon",
            date=date(1935, 11, 15),
            category_slug="proclamations",
        )
        doc = MasterlistDocument.from_entry(entry)
        assert doc.doc_id == "proclamation-no-1"
        assert doc.category == DocumentCategory.PROCLAMATION
        assert doc.category_slug == "proclamations"
        assert doc.president_slug == "manuel-l-quezon"
        assert doc.scrape_status == ScrapeStatus.PENDING
        assert doc.text is None
        assert doc.has_html_content is False
        assert doc.is_pdf_only is False

    def test_serialization_roundtrip(self):
        doc = MasterlistDocument(
            doc_id="executive-order-no-1-s-2022",
            category=DocumentCategory.EXECUTIVE_ORDER,
            category_slug="executive-orders",
            president_slug="ferdinand-r-marcos",
            date=date(2022, 6, 30),
            title="Executive Order No. 1, s. 2022",
            content_url="https://example.com/eo-1",
            text="WHEREAS...",
            word_count=500,
            has_html_content=True,
            is_pdf_only=False,
            scrape_status=ScrapeStatus.SUCCESS,
            scraped_at=datetime(2025, 1, 1, 12, 0, 0),
        )
        data = json.loads(doc.model_dump_json())
        restored = MasterlistDocument(**data)
        assert restored.doc_id == doc.doc_id
        assert restored.category == DocumentCategory.EXECUTIVE_ORDER
        assert restored.has_html_content is True
        assert restored.is_pdf_only is False
        assert restored.word_count == 500


class TestMasterlistCorpusSummary:
    def test_defaults(self):
        summary = MasterlistCorpusSummary()
        assert summary.total == 0
        assert summary.pdf_only == 0
        assert summary.html_with_text == 0
        assert summary.by_category == {}

    def test_with_data(self):
        summary = MasterlistCorpusSummary(
            total=10000,
            success=9500,
            failed=100,
            pending=400,
            pdf_only=6000,
            html_with_text=3500,
            by_category={"executive-orders": 3000, "proclamations": 2000},
            by_president={"rodrigo-roa-duterte": 1500},
            total_words=5000000,
        )
        assert summary.total == 10000
        assert summary.by_category["executive-orders"] == 3000
