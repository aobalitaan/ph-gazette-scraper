"""Tests for the PDF text extractor module."""

from unittest.mock import MagicMock, patch

from backend.pipeline.scraper.pdf_extractor import (
    ExtractionResult,
    QualityResult,
    check_quality,
    extract_pdf_text,
    extract_text_from_pdf,
    ocr_pdf,
)


class TestCheckQuality:
    def test_good_english_text(self):
        text = " ".join(["hello"] * 100)
        result = check_quality(text)
        assert result.passed is True
        assert result.word_count == 100
        assert result.reason is None

    def test_too_few_words(self):
        text = "hello world only three words here"
        result = check_quality(text)
        assert result.passed is False
        assert "too few words" in result.reason

    def test_exactly_min_words_passes(self):
        text = " ".join(["word"] * 50)
        result = check_quality(text)
        assert result.passed is True
        assert result.word_count == 50

    def test_low_alphabetic_ratio(self):
        # 30 alpha tokens + 70 numeric tokens = 30% alpha ratio
        tokens = ["hello"] * 30 + ["12345"] * 70
        text = " ".join(tokens)
        result = check_quality(text)
        assert result.passed is False
        assert "alphabetic ratio" in result.reason

    def test_avg_word_length_too_short(self):
        # Single-char tokens
        text = " ".join(["a"] * 100)
        result = check_quality(text)
        assert result.passed is False
        assert "avg word length" in result.reason

    def test_avg_word_length_too_long(self):
        text = " ".join(["a" * 20] * 100)
        result = check_quality(text)
        assert result.passed is False
        assert "avg word length" in result.reason

    def test_mixed_content_passes(self):
        # Simulate real document text with some numbers and short words
        words = ["the", "president", "signed", "executive", "order", "no", "123"]
        text = " ".join(words * 10)
        result = check_quality(text)
        assert result.passed is True
        assert result.alpha_ratio > 0.7

    def test_returns_quality_result_dataclass(self):
        text = " ".join(["word"] * 100)
        result = check_quality(text)
        assert isinstance(result, QualityResult)
        assert isinstance(result.word_count, int)
        assert isinstance(result.alpha_ratio, float)
        assert isinstance(result.avg_word_len, float)


class TestExtractTextFromPdf:
    def test_extracts_text_from_pages(self):
        mock_page1 = MagicMock()
        mock_page1.get_text.return_value = "Page one text"
        mock_page2 = MagicMock()
        mock_page2.get_text.return_value = "Page two text"

        mock_doc = MagicMock()
        mock_doc.__iter__ = lambda self: iter([mock_page1, mock_page2])
        mock_doc.close = MagicMock()

        with patch("backend.pipeline.scraper.pdf_extractor.fitz.open", return_value=mock_doc):
            result = extract_text_from_pdf(b"fake-pdf-bytes")

        assert "Page one text" in result
        assert "Page two text" in result
        mock_doc.close.assert_called_once()

    def test_skips_empty_pages(self):
        mock_page1 = MagicMock()
        mock_page1.get_text.return_value = "Content here"
        mock_page2 = MagicMock()
        mock_page2.get_text.return_value = "   "  # whitespace only

        mock_doc = MagicMock()
        mock_doc.__iter__ = lambda self: iter([mock_page1, mock_page2])
        mock_doc.close = MagicMock()

        with patch("backend.pipeline.scraper.pdf_extractor.fitz.open", return_value=mock_doc):
            result = extract_text_from_pdf(b"fake-pdf-bytes")

        assert result == "Content here"

    def test_closes_doc_on_error(self):
        mock_page = MagicMock()
        mock_page.get_text.side_effect = RuntimeError("corrupt page")

        mock_doc = MagicMock()
        mock_doc.__iter__ = lambda self: iter([mock_page])
        mock_doc.close = MagicMock()

        with patch("backend.pipeline.scraper.pdf_extractor.fitz.open", return_value=mock_doc):
            try:
                extract_text_from_pdf(b"bad-pdf")
            except RuntimeError:
                pass
        mock_doc.close.assert_called_once()


class TestOcrPdf:
    def test_ocrs_pages(self):
        mock_pix = MagicMock()
        mock_pix.width = 100
        mock_pix.height = 100
        mock_pix.samples = b"\x00" * (100 * 100 * 3)

        mock_page = MagicMock()
        mock_page.get_pixmap.return_value = mock_pix

        mock_doc = MagicMock()
        mock_doc.__iter__ = lambda self: iter([mock_page])
        mock_doc.close = MagicMock()

        with (
            patch("backend.pipeline.scraper.pdf_extractor.fitz.open", return_value=mock_doc),
            patch(
                "backend.pipeline.scraper.pdf_extractor.pytesseract.image_to_string",
                return_value="OCR text here",
            ),
        ):
            result = ocr_pdf(b"fake-pdf")

        assert result == "OCR text here"
        mock_doc.close.assert_called_once()


class TestExtractPdfText:
    def _good_text(self):
        return " ".join(["The president signed an executive order on governance"] * 10)

    def test_text_extraction_succeeds(self):
        text = self._good_text()
        with (
            patch(
                "backend.pipeline.scraper.pdf_extractor.extract_text_from_pdf",
                return_value=text,
            ),
        ):
            result = extract_pdf_text(b"fake-pdf")

        assert isinstance(result, ExtractionResult)
        assert result.method == "text_extracted"
        assert result.text is not None
        assert result.word_count > 0
        assert result.error is None

    def test_falls_back_to_ocr(self):
        ocr_text = self._good_text()
        with (
            patch(
                "backend.pipeline.scraper.pdf_extractor.extract_text_from_pdf",
                return_value="",  # no text layer
            ),
            patch(
                "backend.pipeline.scraper.pdf_extractor.ocr_pdf",
                return_value=ocr_text,
            ),
        ):
            result = extract_pdf_text(b"fake-pdf")

        assert result.method == "ocr_extracted"
        assert result.text is not None

    def test_falls_back_to_ocr_when_quality_fails(self):
        bad_text = " ".join(["x"] * 100)  # single-char words fail quality gate
        ocr_text = self._good_text()
        with (
            patch(
                "backend.pipeline.scraper.pdf_extractor.extract_text_from_pdf",
                return_value=bad_text,
            ),
            patch(
                "backend.pipeline.scraper.pdf_extractor.ocr_pdf",
                return_value=ocr_text,
            ),
        ):
            result = extract_pdf_text(b"fake-pdf")

        assert result.method == "ocr_extracted"

    def test_both_fail(self):
        with (
            patch(
                "backend.pipeline.scraper.pdf_extractor.extract_text_from_pdf",
                return_value="",
            ),
            patch(
                "backend.pipeline.scraper.pdf_extractor.ocr_pdf",
                return_value="",
            ),
        ):
            result = extract_pdf_text(b"fake-pdf")

        assert result.method == "failed"
        assert result.text is None
        assert result.error is not None

    def test_text_extraction_exception_falls_through(self):
        ocr_text = self._good_text()
        with (
            patch(
                "backend.pipeline.scraper.pdf_extractor.extract_text_from_pdf",
                side_effect=RuntimeError("corrupt PDF"),
            ),
            patch(
                "backend.pipeline.scraper.pdf_extractor.ocr_pdf",
                return_value=ocr_text,
            ),
        ):
            result = extract_pdf_text(b"fake-pdf")

        assert result.method == "ocr_extracted"
        assert result.text is not None

    def test_ocr_exception_with_no_text(self):
        with (
            patch(
                "backend.pipeline.scraper.pdf_extractor.extract_text_from_pdf",
                return_value="",
            ),
            patch(
                "backend.pipeline.scraper.pdf_extractor.ocr_pdf",
                side_effect=RuntimeError("tesseract not found"),
            ),
        ):
            result = extract_pdf_text(b"fake-pdf")

        assert result.method == "failed"
        assert "OCR failed" in result.error

    def test_ocr_exception_with_bad_text(self):
        bad_text = " ".join(["x"] * 100)
        with (
            patch(
                "backend.pipeline.scraper.pdf_extractor.extract_text_from_pdf",
                return_value=bad_text,
            ),
            patch(
                "backend.pipeline.scraper.pdf_extractor.ocr_pdf",
                side_effect=RuntimeError("tesseract not found"),
            ),
        ):
            result = extract_pdf_text(b"fake-pdf")

        assert result.method == "failed"
        assert "quality gate failed" in result.error
        assert "OCR also failed" in result.error
