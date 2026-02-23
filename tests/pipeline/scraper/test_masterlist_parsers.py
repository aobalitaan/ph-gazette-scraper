"""Tests for masterlist HTML parsers."""

from datetime import date
from pathlib import Path

import pytest

from backend.pipeline.scraper.masterlist_parsers import (
    parse_masterlist_content_page,
    parse_masterlist_page,
    parse_total_records,
)
from backend.pipeline.scraper.models import DocumentCategory

FIXTURES = Path(__file__).parent.parent.parent / "fixtures"


class TestParseMasterlistPage:
    @pytest.fixture
    def results_html(self) -> str:
        return (FIXTURES / "masterlist_results_page.html").read_text()

    def test_parses_correct_count(self, results_html: str):
        entries = parse_masterlist_page(results_html, "speeches")
        assert len(entries) == 4

    def test_first_entry_no_pdf(self, results_html: str):
        entries = parse_masterlist_page(results_html, "speeches")
        first = entries[0]
        assert first.title == "Inaugural address of President Rodrigo Roa Duterte"
        assert "inaugural-address" in first.content_url
        assert first.president_slug == "rodrigo-roa-duterte"
        assert first.date == date(2016, 6, 30)
        assert first.pdf_url is None
        assert first.category_slug == "speeches"
        assert first.category == DocumentCategory.SPEECH

    def test_entry_with_pdf(self, results_html: str):
        entries = parse_masterlist_page(results_html, "speeches")
        sona = entries[2]
        assert "First State of the Nation" in sona.title
        assert sona.date == date(2016, 7, 25)
        assert sona.pdf_url is not None
        assert sona.pdf_url.endswith(".pdf")

    def test_doc_id_generated(self, results_html: str):
        entries = parse_masterlist_page(results_html, "speeches")
        expected = "inaugural-address-of-president-rodrigo-roa-duterte-june-30-2016"
        assert entries[0].doc_id == expected

    def test_empty_table_returns_empty(self):
        html = "<html><body><p>No results</p></body></html>"
        entries = parse_masterlist_page(html, "speeches")
        assert entries == []


class TestParseTotalRecords:
    def test_extracts_count(self):
        html = '<div class="alert-box">There are 30 total number of records found.</div>'
        assert parse_total_records(html) == 30

    def test_large_count(self):
        html = "There are 4523 total number of records found."
        assert parse_total_records(html) == 4523

    def test_no_match_returns_zero(self):
        assert parse_total_records("<html><body>Nothing here</body></html>") == 0

    def test_extracts_from_full_page(self):
        html = (FIXTURES / "masterlist_results_page.html").read_text()
        assert parse_total_records(html) == 30


class TestParseMasterlistContentPageFulltext:
    @pytest.fixture
    def fulltext_html(self) -> str:
        return (FIXTURES / "masterlist_content_fulltext.html").read_text()

    def test_has_html_content(self, fulltext_html: str):
        result = parse_masterlist_content_page(fulltext_html)
        assert result["has_html_content"] is True
        assert result["is_pdf_only"] is False

    def test_text_extracted(self, fulltext_html: str):
        result = parse_masterlist_content_page(fulltext_html)
        assert result["text"] is not None
        assert "fellow Filipinos" in result["text"].lower() or "vision" in result["text"].lower()
        assert result["word_count"] > 20

    def test_boilerplate_stripped(self, fulltext_html: str):
        result = parse_masterlist_content_page(fulltext_html)
        assert "MALACAÑAN PALACE" not in result["text"]
        assert "MANILA" not in result["text"]
        assert "BY THE PRESIDENT" not in result["text"]

    def test_resources_section_excluded(self, fulltext_html: str):
        result = parse_masterlist_content_page(fulltext_html)
        assert "Resources" not in result["text"]
        assert "Download PDF" not in result["text"]

    def test_pdf_url_extracted(self, fulltext_html: str):
        result = parse_masterlist_content_page(fulltext_html)
        assert result["pdf_url"] is not None
        assert result["pdf_url"].endswith(".pdf")


class TestParseMasterlistContentPagePdfOnly:
    @pytest.fixture
    def pdfonly_html(self) -> str:
        return (FIXTURES / "masterlist_content_pdfonly.html").read_text()

    def test_is_pdf_only(self, pdfonly_html: str):
        result = parse_masterlist_content_page(pdfonly_html)
        assert result["is_pdf_only"] is True
        assert result["has_html_content"] is False

    def test_no_text_for_pdf_only(self, pdfonly_html: str):
        result = parse_masterlist_content_page(pdfonly_html)
        assert result["text"] is None
        assert result["word_count"] is None

    def test_pdf_url_extracted(self, pdfonly_html: str):
        result = parse_masterlist_content_page(pdfonly_html)
        assert result["pdf_url"] is not None
        assert result["pdf_url"].endswith(".pdf")

    def test_no_article_raises(self):
        with pytest.raises(ValueError, match="No <article>"):
            parse_masterlist_content_page("<html><body><p>No article</p></body></html>")
