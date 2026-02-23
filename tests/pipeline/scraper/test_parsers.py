"""Tests for SONA HTML parsers."""

from datetime import date
from pathlib import Path

import pytest

from backend.pipeline.scraper.parsers import parse_sona_index, parse_sona_page

FIXTURES = Path(__file__).parent.parent.parent / "fixtures"


class TestParseSONAIndex:
    @pytest.fixture
    def index_html(self) -> str:
        return (FIXTURES / "sona_index_snippet.html").read_text()

    def test_parses_correct_count(self, index_html: str):
        entries = parse_sona_index(index_html)
        assert len(entries) == 9

    def test_first_entry_with_rowspan(self, index_html: str):
        entries = parse_sona_index(index_html)
        first = entries[0]
        assert first.president == "Manuel L. Quezon"
        assert first.date == date(1935, 11, 25)
        assert first.title == "Message to the First Assembly on National Defense"
        assert "1935/11/25" in first.url
        assert first.venue == "Legislative Building, Manila"
        assert first.legislature == "First National Assembly, First Session"

    def test_president_carryforward_with_rowspan(self, index_html: str):
        entries = parse_sona_index(index_html)
        # Second and third entries should carry forward Quezon
        assert entries[1].president == "Manuel L. Quezon"
        assert entries[1].date == date(1936, 6, 16)
        assert entries[2].president == "Manuel L. Quezon"
        assert entries[2].date == date(1937, 10, 18)

    def test_new_president_after_rowspan(self, index_html: str):
        entries = parse_sona_index(index_html)
        osmena = entries[3]
        assert osmena.president == "Sergio Osmeña"
        assert osmena.date == date(1945, 6, 9)

    def test_empty_legislature_martial_law(self, index_html: str):
        entries = parse_sona_index(index_html)
        marcos_73 = entries[4]
        assert marcos_73.president == "Ferdinand E. Marcos"
        assert marcos_73.date == date(1973, 9, 21)
        assert marcos_73.legislature is None

    def test_president_carryforward_empty_cells(self, index_html: str):
        """Marcos Jr. uses empty admin cells instead of rowspan."""
        entries = parse_sona_index(index_html)
        jr_entries = [e for e in entries if "Marcos Jr" in e.president]
        assert len(jr_entries) == 3
        assert jr_entries[0].date == date(2022, 7, 25)
        assert jr_entries[1].date == date(2023, 7, 24)
        assert jr_entries[2].date == date(2024, 7, 22)

    def test_url_cleaned_of_html_entities(self, index_html: str):
        entries = parse_sona_index(index_html)
        jr_2022 = [e for e in entries if e.date == date(2022, 7, 25)][0]
        # URL should not end with &quot; or "
        assert not jr_2022.url.endswith('"')
        assert "&quot;" not in jr_2022.url

    def test_doc_id_generation(self, index_html: str):
        entries = parse_sona_index(index_html)
        assert entries[0].doc_id == "sona_1935_11_25"
        assert entries[3].doc_id == "sona_1945_06_09"

    def test_empty_html_raises(self):
        with pytest.raises(ValueError, match="No table found"):
            parse_sona_index("<html><body><p>No table here</p></body></html>")


class TestParseSONAPageModern:
    @pytest.fixture
    def page_html(self) -> str:
        return (FIXTURES / "sona_page_modern.html").read_text()

    def test_extracts_title(self, page_html: str):
        result = parse_sona_page(page_html)
        assert result["full_title"] == (
            "Rodrigo Roa Duterte, First State of the Nation Address, July 25, 2016"
        )

    def test_extracts_paragraphs_across_pages(self, page_html: str):
        result = parse_sona_page(page_html)
        assert result["paragraph_count"] == 5
        assert "informality" in result["paragraphs"][1]
        # Page 2 paragraphs should also be included
        assert "concerns" in result["paragraphs"][3]

    def test_has_text_and_word_count(self, page_html: str):
        result = parse_sona_page(page_html)
        assert result["word_count"] > 0
        assert len(result["text"]) > 0

    def test_extracts_pdf_url(self, page_html: str):
        result = parse_sona_page(page_html)
        assert result["pdf_url"] is not None
        assert result["pdf_url"].endswith(".pdf")

    def test_excludes_resources_section(self, page_html: str):
        result = parse_sona_page(page_html)
        for p in result["paragraphs"]:
            assert "Resources" not in p


class TestParseSONAPageHistorical:
    @pytest.fixture
    def page_html(self) -> str:
        return (FIXTURES / "sona_page_historical.html").read_text()

    def test_extracts_title(self, page_html: str):
        result = parse_sona_page(page_html)
        assert "Manuel L. Quezon" in result["full_title"]
        assert "1935" in result["full_title"]

    def test_extracts_paragraphs(self, page_html: str):
        result = parse_sona_page(page_html)
        assert result["paragraph_count"] == 5
        assert "Assembly Hall" in result["paragraphs"][1]

    def test_no_pdf_url(self, page_html: str):
        result = parse_sona_page(page_html)
        assert result["pdf_url"] is None

    def test_no_article_raises(self):
        with pytest.raises(ValueError, match="No <article>"):
            parse_sona_page("<html><body><p>No article</p></body></html>")
