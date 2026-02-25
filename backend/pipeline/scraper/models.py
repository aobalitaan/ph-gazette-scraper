"""Data models for the scraper pipeline."""

from datetime import date, datetime
from enum import StrEnum
from urllib.parse import urlparse

from pydantic import BaseModel, computed_field


class DocumentCategory(StrEnum):
    """All document types from the Official Gazette."""

    SONA = "sona"
    EXECUTIVE_ORDER = "executive_order"
    ADMINISTRATIVE_ORDER = "administrative_order"
    PROCLAMATION = "proclamation"
    MEMORANDUM_ORDER = "memorandum_order"
    MEMORANDUM_CIRCULAR = "memorandum_circular"
    PRESIDENTIAL_DECREE = "presidential_decree"
    SPEECH = "speech"
    REPUBLIC_ACT = "republic_act"
    GENERAL_ORDER = "general_order"
    LETTER_OF_INSTRUCTION = "letter_of_instruction"
    LETTER_OF_IMPLEMENTATION = "letter_of_implementation"
    OTHER_ISSUANCE = "other_issuance"
    SPECIAL_ORDER = "special_order"
    IRR_EXECUTIVE_ORDER = "irr_executive_order"
    IRR_REPUBLIC_ACT = "irr_republic_act"


class ScrapeStatus(StrEnum):
    """Status of a document scrape attempt."""

    PENDING = "pending"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


class PdfStatus(StrEnum):
    """Status of PDF text extraction for a document."""

    PENDING = "pending"
    TEXT_EXTRACTED = "text_extracted"
    OCR_EXTRACTED = "ocr_extracted"
    FAILED = "failed"
    SKIPPED = "skipped"
    NOT_APPLICABLE = "not_applicable"


class SONAIndexEntry(BaseModel):
    """One row from the Official Gazette SONA index table."""

    president: str
    date: date
    title: str
    url: str
    venue: str | None = None
    legislature: str | None = None

    @computed_field  # type: ignore[prop-decorator]
    @property
    def doc_id(self) -> str:
        return f"sona_{self.date.strftime('%Y_%m_%d')}"


class SONADocument(BaseModel):
    """Complete SONA document with metadata, content, and scrape status."""

    doc_id: str
    category: DocumentCategory = DocumentCategory.SONA
    president: str
    date: date
    title: str
    source_url: str
    venue: str | None = None
    legislature: str | None = None

    full_title: str | None = None
    text: str | None = None
    word_count: int | None = None
    paragraph_count: int | None = None
    pdf_url: str | None = None

    scrape_status: ScrapeStatus = ScrapeStatus.PENDING
    scrape_error: str | None = None
    scraped_at: datetime | None = None

    @classmethod
    def from_index_entry(cls, entry: SONAIndexEntry) -> "SONADocument":
        return cls(
            doc_id=entry.doc_id,
            president=entry.president,
            date=entry.date,
            title=entry.title,
            source_url=entry.url,
            venue=entry.venue,
            legislature=entry.legislature,
        )


class SONACorpusSummary(BaseModel):
    """Aggregate statistics for reporting on the SONA corpus."""

    total: int = 0
    success: int = 0
    failed: int = 0
    pending: int = 0
    skipped: int = 0
    presidents: list[str] = []
    date_range: tuple[date, date] | None = None
    total_words: int = 0


# ---------------------------------------------------------------------------
# Masterlist generator constants and models
# ---------------------------------------------------------------------------

MASTERLIST_CATEGORY_MAP: dict[str, DocumentCategory] = {
    "executive-orders": DocumentCategory.EXECUTIVE_ORDER,
    "administrative-orders": DocumentCategory.ADMINISTRATIVE_ORDER,
    "proclamations": DocumentCategory.PROCLAMATION,
    "memorandum-orders": DocumentCategory.MEMORANDUM_ORDER,
    "memorandum-circulars": DocumentCategory.MEMORANDUM_CIRCULAR,
    "presidential-decrees-executive-issuances": DocumentCategory.PRESIDENTIAL_DECREE,
    "speeches": DocumentCategory.SPEECH,
    "republic-acts": DocumentCategory.REPUBLIC_ACT,
    "general-orders": DocumentCategory.GENERAL_ORDER,
    "letters-of-instruction": DocumentCategory.LETTER_OF_INSTRUCTION,
    "letters-of-implementation": DocumentCategory.LETTER_OF_IMPLEMENTATION,
    "other-issuances": DocumentCategory.OTHER_ISSUANCE,
    "special-orders": DocumentCategory.SPECIAL_ORDER,
    "implementing-rules-and-regulations-executive-orders": DocumentCategory.IRR_EXECUTIVE_ORDER,
    "implementing-rules-and-regulations": DocumentCategory.IRR_REPUBLIC_ACT,
}

MASTERLIST_PRESIDENT_SLUGS: list[str] = [
    "manuel-l-quezon",
    "sergio-osmena",
    "manuel-roxas",
    "elpidio-quirino",
    "ramon-magsaysay",
    "carlos-p-garcia",
    "diosdado-macapagal",
    "ferdinand-e-marcos",
    "corazon-c-aquino",
    "fidel-v-ramos",
    "joseph-ejercito-estrada",
    "gloria-macapagal-arroyo",
    "benigno-s-aquino-iii",
    "rodrigo-roa-duterte",
    "ferdinand-r-marcos",
]


def _doc_id_from_url(url: str) -> str:
    """Extract a human-readable doc_id from a Gazette content URL.

    Example:
        "https://www.officialgazette.gov.ph/2022/05/11/administrative-order-no-48-s-2022/"
        -> "administrative-order-no-48-s-2022"
    """
    path = urlparse(url).path.rstrip("/")
    return path.rsplit("/", 1)[-1]


class MasterlistEntry(BaseModel):
    """One row from a masterlist results table (Phase A output)."""

    title: str
    content_url: str
    president_slug: str
    date: date
    pdf_url: str | None = None
    category_slug: str

    @computed_field  # type: ignore[prop-decorator]
    @property
    def doc_id(self) -> str:
        return _doc_id_from_url(self.content_url)

    @computed_field  # type: ignore[prop-decorator]
    @property
    def category(self) -> DocumentCategory:
        return MASTERLIST_CATEGORY_MAP[self.category_slug]


class MasterlistDocument(BaseModel):
    """Full document after content page fetch (Phase B output)."""

    doc_id: str
    category: DocumentCategory
    category_slug: str
    president_slug: str
    date: date
    title: str
    content_url: str
    pdf_url: str | None = None

    text: str | None = None
    word_count: int | None = None
    has_html_content: bool = False
    is_pdf_only: bool = False

    scrape_status: ScrapeStatus = ScrapeStatus.PENDING
    scrape_error: str | None = None
    scraped_at: datetime | None = None

    pdf_status: PdfStatus = PdfStatus.NOT_APPLICABLE
    pdf_error: str | None = None
    pdf_processed_at: datetime | None = None

    @classmethod
    def from_entry(cls, entry: MasterlistEntry) -> "MasterlistDocument":
        return cls(
            doc_id=entry.doc_id,
            category=entry.category,
            category_slug=entry.category_slug,
            president_slug=entry.president_slug,
            date=entry.date,
            title=entry.title,
            content_url=entry.content_url,
            pdf_url=entry.pdf_url,
        )


class MasterlistCorpusSummary(BaseModel):
    """Aggregate statistics for the masterlist corpus."""

    total: int = 0
    success: int = 0
    failed: int = 0
    pending: int = 0
    skipped: int = 0
    pdf_only: int = 0
    html_with_text: int = 0
    by_category: dict[str, int] = {}
    by_president: dict[str, int] = {}
    total_words: int = 0
    pdf_text_extracted: int = 0
    pdf_ocr_extracted: int = 0
    pdf_failed: int = 0
    pdf_skipped: int = 0
