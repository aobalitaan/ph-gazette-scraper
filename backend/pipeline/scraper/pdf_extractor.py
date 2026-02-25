"""PDF text extraction with quality gate and OCR fallback.

Pure synchronous module — no I/O, no network calls. Called from async code
via ``asyncio.to_thread()`` since OCR is CPU-bound.

Pipeline:
  1. Try pymupdf text-layer extraction.
  2. Run quality gate on the result.
  3. If quality gate fails, OCR via Tesseract (pymupdf renders pages → pytesseract).
  4. Run quality gate on OCR result.
  5. Return the best result or a failure.
"""

import logging
import re
from dataclasses import dataclass

import fitz  # pymupdf
import pytesseract
from PIL import Image

logger = logging.getLogger(__name__)

# Quality gate thresholds
MIN_ALPHA_RATIO = 0.70
MIN_AVG_WORD_LEN = 2.0
MAX_AVG_WORD_LEN = 15.0
MIN_WORD_COUNT = 50

# OCR rendering: 200 DPI balances quality vs speed for Tesseract
OCR_DPI = 200


@dataclass(frozen=True)
class QualityResult:
    """Result of the text quality gate check."""

    passed: bool
    word_count: int
    alpha_ratio: float
    avg_word_len: float
    reason: str | None = None


@dataclass(frozen=True)
class ExtractionResult:
    """Result of PDF text extraction."""

    text: str | None
    word_count: int
    method: str  # "text_extracted", "ocr_extracted", or "failed"
    error: str | None = None


def check_quality(text: str) -> QualityResult:
    """Language-agnostic quality gate for extracted text.

    Checks:
      - Alphabetic token ratio >= 70% (filters OCR garbage)
      - Average word length between 2 and 15 characters
      - Minimum 50 words
    """
    tokens = text.split()
    word_count = len(tokens)

    if word_count < MIN_WORD_COUNT:
        return QualityResult(
            passed=False,
            word_count=word_count,
            alpha_ratio=0.0,
            avg_word_len=0.0,
            reason=f"too few words ({word_count} < {MIN_WORD_COUNT})",
        )

    alpha_tokens = sum(1 for t in tokens if re.search(r"[a-zA-Z]", t))
    alpha_ratio = alpha_tokens / word_count

    avg_word_len = sum(len(t) for t in tokens) / word_count

    if alpha_ratio < MIN_ALPHA_RATIO:
        return QualityResult(
            passed=False,
            word_count=word_count,
            alpha_ratio=alpha_ratio,
            avg_word_len=avg_word_len,
            reason=f"low alphabetic ratio ({alpha_ratio:.2f} < {MIN_ALPHA_RATIO})",
        )

    if avg_word_len < MIN_AVG_WORD_LEN or avg_word_len > MAX_AVG_WORD_LEN:
        return QualityResult(
            passed=False,
            word_count=word_count,
            alpha_ratio=alpha_ratio,
            avg_word_len=avg_word_len,
            reason=(
                f"avg word length {avg_word_len:.1f} outside "
                f"[{MIN_AVG_WORD_LEN}, {MAX_AVG_WORD_LEN}]"
            ),
        )

    return QualityResult(
        passed=True,
        word_count=word_count,
        alpha_ratio=alpha_ratio,
        avg_word_len=avg_word_len,
    )


def extract_text_from_pdf(pdf_bytes: bytes) -> str:
    """Extract text layer from a PDF using pymupdf.

    Returns concatenated text from all pages, separated by newlines.
    """
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    try:
        pages = []
        for page in doc:
            text = page.get_text()
            if text.strip():
                pages.append(text)
        return "\n".join(pages)
    finally:
        doc.close()


def ocr_pdf(pdf_bytes: bytes) -> str:
    """Render PDF pages to images and OCR with Tesseract.

    Uses pymupdf to render each page at OCR_DPI, then feeds the image
    to pytesseract for text recognition.
    """
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    try:
        pages = []
        mat = fitz.Matrix(OCR_DPI / 72, OCR_DPI / 72)
        for page in doc:
            pix = page.get_pixmap(matrix=mat)
            img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
            text = pytesseract.image_to_string(img)
            if text.strip():
                pages.append(text)
        return "\n".join(pages)
    finally:
        doc.close()


def extract_pdf_text(pdf_bytes: bytes) -> ExtractionResult:
    """Main entry point: extract text from PDF with quality gate and OCR fallback.

    1. Try pymupdf text extraction → quality gate.
    2. If that fails, try Tesseract OCR → quality gate.
    3. Return the best result or a failure.
    """
    # Step 1: pymupdf text layer
    try:
        text = extract_text_from_pdf(pdf_bytes)
    except Exception as e:
        logger.warning("pymupdf text extraction failed: %s", e)
        text = ""

    if text.strip():
        qr = check_quality(text)
        if qr.passed:
            return ExtractionResult(
                text=text.strip(),
                word_count=qr.word_count,
                method="text_extracted",
            )
        logger.debug("Text layer failed quality gate: %s", qr.reason)

    # Step 2: OCR fallback
    try:
        ocr_text = ocr_pdf(pdf_bytes)
    except Exception as e:
        logger.warning("OCR failed: %s", e)
        # If we had some text from step 1 but it failed quality, report that
        if text.strip():
            qr = check_quality(text)
            return ExtractionResult(
                text=None,
                word_count=qr.word_count,
                method="failed",
                error=f"text quality gate failed ({qr.reason}), OCR also failed ({e})",
            )
        return ExtractionResult(
            text=None,
            word_count=0,
            method="failed",
            error=f"no text layer, OCR failed ({e})",
        )

    if ocr_text.strip():
        qr = check_quality(ocr_text)
        if qr.passed:
            return ExtractionResult(
                text=ocr_text.strip(),
                word_count=qr.word_count,
                method="ocr_extracted",
            )
        return ExtractionResult(
            text=None,
            word_count=qr.word_count,
            method="failed",
            error=f"OCR quality gate failed ({qr.reason})",
        )

    return ExtractionResult(
        text=None,
        word_count=0,
        method="failed",
        error="no text from text layer or OCR",
    )
