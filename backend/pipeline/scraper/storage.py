"""Storage layer for SONA scraper output.

Manages raw HTML, extracted text, per-document metadata, and the combined manifest.
"""

import json
from pathlib import Path

from backend.pipeline.scraper.models import SONADocument


def president_slug(president: str) -> str:
    """Convert a president name to a filesystem-safe slug.

    Examples:
        "Manuel L. Quezon" -> "manuel-l-quezon"
        "Ferdinand R. Marcos Jr." -> "ferdinand-r-marcos-jr"
        "Gloria Macapagal-Arroyo" -> "gloria-macapagal-arroyo"
    """
    slug = president.lower().replace(".", "").replace(",", "")
    return "-".join(slug.split())


class SONAStorage:
    """Manages file I/O for scraped SONA documents.

    Directory layout:
        base_dir/
            raw_html/{president_slug}/{doc_id}.html
            text/{president_slug}/{doc_id}.txt
            metadata/{president_slug}/{doc_id}.json
            manifest.json
    """

    def __init__(self, base_dir: Path) -> None:
        self.base_dir = base_dir
        self.raw_html_dir = base_dir / "raw_html"
        self.text_dir = base_dir / "text"
        self.metadata_dir = base_dir / "metadata"
        self.manifest_path = base_dir / "manifest.json"

    def ensure_dirs(self) -> None:
        """Create top-level output directories if they don't exist."""
        for d in (self.raw_html_dir, self.text_dir, self.metadata_dir):
            d.mkdir(parents=True, exist_ok=True)

    def save_raw_html(self, doc: SONADocument, html: str) -> Path:
        """Save the original HTML page."""
        path = self._doc_path(self.raw_html_dir, doc, ".html")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(html, encoding="utf-8")
        return path

    def save_text(self, doc: SONADocument, text: str) -> Path:
        """Save the extracted plain text."""
        path = self._doc_path(self.text_dir, doc, ".txt")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
        return path

    def save_metadata(self, doc: SONADocument) -> Path:
        """Save per-document JSON metadata."""
        path = self._doc_path(self.metadata_dir, doc, ".json")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(doc.model_dump_json(indent=2), encoding="utf-8")
        return path

    def save_document(self, doc: SONADocument, html: str) -> None:
        """Save all artifacts for a single document."""
        self.save_raw_html(doc, html)
        if doc.text:
            self.save_text(doc, doc.text)
        self.save_metadata(doc)

    def load_manifest(self) -> list[SONADocument]:
        """Load the manifest file. Returns empty list if it doesn't exist."""
        if not self.manifest_path.exists():
            return []
        data = json.loads(self.manifest_path.read_text(encoding="utf-8"))
        return [SONADocument(**entry) for entry in data]

    def save_manifest(self, documents: list[SONADocument]) -> Path:
        """Save the combined manifest with all documents."""
        data = [json.loads(doc.model_dump_json()) for doc in documents]
        self.manifest_path.write_text(
            json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8"
        )
        return self.manifest_path

    def is_already_scraped(self, doc_id: str) -> bool:
        """Check if a document has already been successfully scraped via manifest."""
        for doc in self.load_manifest():
            if doc.doc_id == doc_id and doc.scrape_status == "success":
                return True
        return False

    def _doc_path(self, base: Path, doc: SONADocument, ext: str) -> Path:
        """Build the full path for a document artifact."""
        return base / president_slug(doc.president) / f"{doc.doc_id}{ext}"
