"""Pure HTML parsing functions for Official Gazette masterlist pages.

No I/O — these operate on HTML strings only.
"""

import re

from bs4 import BeautifulSoup, Tag

from backend.pipeline.scraper.models import MasterlistEntry
from backend.pipeline.scraper.parsers import parse_gazette_date


def parse_masterlist_page(html: str, category_slug: str) -> list[MasterlistEntry]:
    """Parse the masterlist results table into a list of MasterlistEntry objects.

    Each row has 5 columns: Title | Content URL | President slug | Date | PDF (optional).
    """
    soup = BeautifulSoup(html, "lxml")
    table = soup.find("table")
    if not table:
        return []

    rows = table.find_all("tr")
    entries: list[MasterlistEntry] = []

    for row in rows:
        cells = row.find_all("td")
        if len(cells) < 4:
            continue

        title = cells[0].get_text(strip=True)
        if not title:
            continue

        # Content URL is in a link inside the second cell
        content_link = cells[1].find("a", href=True)
        if not content_link:
            continue
        content_url = content_link["href"]

        president_slug = cells[2].get_text(strip=True)
        date_text = cells[3].get_text(strip=True)
        parsed_date = parse_gazette_date(date_text)
        if parsed_date is None:
            continue

        # PDF URL is optional (5th column may be empty or absent)
        pdf_url = None
        if len(cells) >= 5:
            pdf_link = cells[4].find("a", href=True)
            if pdf_link and pdf_link["href"].endswith(".pdf"):
                pdf_url = pdf_link["href"]

        entries.append(
            MasterlistEntry(
                title=title,
                content_url=content_url,
                president_slug=president_slug,
                date=parsed_date,
                pdf_url=pdf_url,
                category_slug=category_slug,
            )
        )

    return entries


def parse_total_records(html: str) -> int:
    """Extract total record count from 'There are X total number of records found.' text."""
    match = re.search(r"There are (\d+) total number of records found", html)
    if match:
        return int(match.group(1))
    return 0


def parse_masterlist_content_page(html: str) -> dict:
    """Parse an individual content page and extract text + classify page type.

    Returns dict with: text, word_count, has_html_content, is_pdf_only, pdf_url
    """
    soup = BeautifulSoup(html, "lxml")
    article = soup.find("article")
    if not article:
        raise ValueError("No <article> element found in content page HTML")

    # Extract PDF URL before removing Resources section
    pdf_url = _extract_pdf_url(article)

    # Find content area
    content = article.find(class_="entry-content")
    if content is None:
        content = article

    # Remove Resources section before extracting paragraphs
    _remove_resources_section(content)

    # Extract paragraphs
    paragraphs = _extract_paragraphs(content)
    text = "\n\n".join(paragraphs)
    word_count = len(text.split()) if text else 0

    # Many gazette pages only show the document header (title + number)
    # with no substantive text — the actual content is in the PDF.
    # 100 words filters out these boilerplate-only pages.
    has_html_content = word_count >= 100
    is_pdf_only = not has_html_content

    return {
        "text": text if has_html_content else None,
        "word_count": word_count if has_html_content else None,
        "has_html_content": has_html_content,
        "is_pdf_only": is_pdf_only,
        "pdf_url": pdf_url,
    }


# Boilerplate header lines commonly found at the top of gazette documents.
_BOILERPLATE_PATTERNS = frozenset({
    "malacañan palace",
    "malacañang palace",
    "malacanan palace",
    "manila",
    "by the president of the philippines",
})


def _extract_paragraphs(content: Tag) -> list[str]:
    """Extract meaningful paragraphs, stripping boilerplate header lines."""
    paragraphs = []
    for p in content.find_all("p", recursive=True):
        text = p.get_text(separator=" ", strip=True)
        if not text or text == "\xa0":
            continue
        if text.lower() in _BOILERPLATE_PATTERNS:
            continue
        paragraphs.append(text)
    return paragraphs


def _extract_pdf_url(article: Tag) -> str | None:
    """Extract PDF URL from links ending in .pdf."""
    for link in article.find_all("a", href=True):
        href = link["href"]
        if href.endswith(".pdf"):
            return href
    return None


def _remove_resources_section(content: Tag) -> None:
    """Remove the Resources section from content to avoid polluting text extraction."""
    for heading in content.find_all("h5"):
        if heading.get_text(strip=True) == "Resources":
            parent = heading.find_parent("section") or heading.find_parent(
                "div", class_="row"
            )
            if parent:
                parent.decompose()
            else:
                heading.decompose()
