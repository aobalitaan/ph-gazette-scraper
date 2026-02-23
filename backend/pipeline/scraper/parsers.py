"""Pure HTML parsing functions for Official Gazette SONA pages.

No I/O — these operate on HTML strings only.
"""

from datetime import date, datetime

from bs4 import BeautifulSoup, Tag

from backend.pipeline.scraper.models import SONAIndexEntry


def parse_sona_index(html: str) -> list[SONAIndexEntry]:
    """Parse the SONA index table into a list of SONAIndexEntry objects.

    Handles two patterns for president grouping:
    - rowspan on the first cell (most presidents)
    - Empty admin cells for continuation rows (Marcos Jr.)
    """
    soup = BeautifulSoup(html, "lxml")
    table = soup.find("table")
    if not table:
        raise ValueError("No table found in SONA index HTML")

    rows = table.find_all("tr")
    entries: list[SONAIndexEntry] = []
    current_president: str | None = None

    for row in rows:
        cells = row.find_all("td")
        if not cells:
            continue

        # Determine if this row has a president cell.
        # Rows with rowspan or a non-empty first cell that contains text
        # have a president cell. Rows where the first cell's text is a date
        # (continuation rows from rowspan) skip the admin column entirely.
        first_cell_text = cells[0].get_text(strip=True)

        # Header row detection: skip if first cell is bold header text
        if cells[0].find("b") and first_cell_text in ("Administration", "President"):
            continue

        # Determine column offset. If this row was created by a rowspan,
        # the first cell IS the date (no admin cell). If it has an admin cell
        # (even empty), it has 5 cells.
        if len(cells) == 5:
            admin_text = cells[0].get_text(strip=True)
            if admin_text:
                current_president = admin_text
            date_text = cells[1].get_text(strip=True)
            title_cell = cells[2]
            venue_text = cells[3].get_text(strip=True) or None
            legislature_text = cells[4].get_text(strip=True) or None
        elif len(cells) == 4:
            # Rowspan continuation: no admin cell, cells are date/title/venue/legislature
            date_text = cells[0].get_text(strip=True)
            title_cell = cells[1]
            venue_text = cells[2].get_text(strip=True) or None
            legislature_text = cells[3].get_text(strip=True) or None
        else:
            continue

        if current_president is None:
            continue

        parsed_date = parse_gazette_date(date_text)
        if parsed_date is None:
            continue

        link = title_cell.find("a")
        if link is None:
            continue

        title = link.get_text(strip=True)
        url = _clean_url(link["href"])

        entries.append(
            SONAIndexEntry(
                president=current_president,
                date=parsed_date,
                title=title,
                url=url,
                venue=venue_text,
                legislature=legislature_text,
            )
        )

    return entries


def parse_sona_page(html: str) -> dict:
    """Parse an individual SONA page and extract its content.

    Handles two layouts:
    - Modern (paginated): content in div.page[title="Page N"] elements
    - Historical: paragraphs directly under .entry-content

    Returns dict with: full_title, paragraphs, text, word_count,
    paragraph_count, pdf_url
    """
    soup = BeautifulSoup(html, "lxml")
    article = soup.find("article")
    if not article:
        raise ValueError("No <article> element found in SONA page HTML")

    # Extract title
    h1 = article.find("h1")
    full_title = h1.get_text(strip=True) if h1 else None

    # Extract PDF URL before removing Resources section
    pdf_url = _extract_pdf_url(article)

    # Find content area
    content = article.find(class_="entry-content")
    if content is None:
        content = article

    # Remove Resources section before extracting paragraphs
    for resources_heading in content.find_all("h5"):
        if resources_heading.get_text(strip=True) == "Resources":
            parent = resources_heading.find_parent("section") or resources_heading.find_parent(
                "div", class_="row"
            )
            if parent:
                parent.decompose()
            else:
                resources_heading.decompose()

    # Try modern layout first: look for div.page elements
    page_divs = content.find_all("div", class_="page")
    if page_divs:
        paragraphs = _extract_paginated_paragraphs(page_divs)
    else:
        # Historical layout: paragraphs directly under entry-content
        paragraphs = _extract_direct_paragraphs(content)

    text = "\n\n".join(paragraphs)
    word_count = len(text.split()) if text else 0

    return {
        "full_title": full_title,
        "paragraphs": paragraphs,
        "text": text,
        "word_count": word_count,
        "paragraph_count": len(paragraphs),
        "pdf_url": pdf_url,
    }


def _extract_paginated_paragraphs(page_divs: list[Tag]) -> list[str]:
    """Extract paragraphs from modern paginated layout."""
    paragraphs = []
    for page_div in page_divs:
        for p in page_div.find_all("p"):
            text = p.get_text(separator=" ", strip=True)
            if text:
                paragraphs.append(text)
    return paragraphs


def _extract_direct_paragraphs(content: Tag) -> list[str]:
    """Extract paragraphs from historical non-paginated layout."""
    paragraphs = []
    for p in content.find_all("p", recursive=True):
        text = p.get_text(separator=" ", strip=True)
        if text and text != "\xa0":
            paragraphs.append(text)
    return paragraphs


def _extract_pdf_url(article: Tag) -> str | None:
    """Extract PDF URL from the Resources section."""
    for link in article.find_all("a", href=True):
        href = link["href"]
        if href.endswith(".pdf"):
            return href
    return None


def parse_gazette_date(date_str: str) -> date | None:
    """Parse dates in 'Month DD, YYYY' format."""
    try:
        return datetime.strptime(date_str, "%B %d, %Y").date()
    except ValueError:
        return None


def _clean_url(url: str) -> str:
    """Clean URLs that may have HTML entity artifacts."""
    # Some URLs on the Gazette have a trailing &quot; encoded as literal text
    url = url.rstrip('"').rstrip("&quot;")
    return url
