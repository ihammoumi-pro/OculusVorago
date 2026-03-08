"""
Unit tests for OCRExtractor.

``pytesseract.image_to_string`` is mocked so the tests run without a
real Tesseract installation.  The ``pdf2image.convert_from_path`` call is
also mocked to avoid needing a working ``pdftoppm`` binary.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from vorago.extractors.ocr_extractor import OCRExtractor

# ---------------------------------------------------------------------------
# Interface compliance
# ---------------------------------------------------------------------------


class TestOCRExtractorInterface:
    def test_implements_iextractor(self) -> None:
        from vorago.core.interfaces import IExtractor

        assert issubclass(OCRExtractor, IExtractor)


# ---------------------------------------------------------------------------
# Image extraction
# ---------------------------------------------------------------------------


class TestOCRExtractorImage:
    def _write_image(self, tmp_path: Path, filename: str = "test.png") -> Path:
        """Create a minimal 10x10 white PNG using Pillow."""
        from PIL import Image

        img_path = tmp_path / filename
        img = Image.new("RGB", (10, 10), color=(255, 255, 255))
        img.save(str(img_path))
        return img_path

    def test_yields_one_record_for_image(self, tmp_path: Path) -> None:
        img_path = self._write_image(tmp_path)
        with patch("pytesseract.image_to_string", return_value="Hello World"):
            records = list(OCRExtractor().extract(str(img_path)))
        assert len(records) == 1

    def test_record_has_required_keys(self, tmp_path: Path) -> None:
        img_path = self._write_image(tmp_path)
        with patch("pytesseract.image_to_string", return_value="OCR text"):
            record = list(OCRExtractor().extract(str(img_path)))[0]
        assert "page_number" in record
        assert "text_content" in record
        assert "source_uri" in record
        assert "lang" in record

    def test_page_number_is_one_for_image(self, tmp_path: Path) -> None:
        img_path = self._write_image(tmp_path)
        with patch("pytesseract.image_to_string", return_value="text"):
            record = list(OCRExtractor().extract(str(img_path)))[0]
        assert record["page_number"] == 1

    def test_text_content_matches_ocr_output(self, tmp_path: Path) -> None:
        img_path = self._write_image(tmp_path)
        with patch("pytesseract.image_to_string", return_value="Extracted text"):
            record = list(OCRExtractor().extract(str(img_path)))[0]
        assert record["text_content"] == "Extracted text"

    def test_source_uri_in_record(self, tmp_path: Path) -> None:
        img_path = self._write_image(tmp_path)
        with patch("pytesseract.image_to_string", return_value=""):
            record = list(OCRExtractor().extract(str(img_path)))[0]
        assert record["source_uri"] == str(img_path)

    def test_lang_in_record(self, tmp_path: Path) -> None:
        img_path = self._write_image(tmp_path)
        with patch("pytesseract.image_to_string", return_value=""):
            record = list(OCRExtractor(lang="fra").extract(str(img_path)))[0]
        assert record["lang"] == "fra"

    def test_jpeg_extension_handled(self, tmp_path: Path) -> None:
        img_path = self._write_image(tmp_path, "photo.jpg")
        with patch("pytesseract.image_to_string", return_value="jpeg text"):
            records = list(OCRExtractor().extract(str(img_path)))
        assert len(records) == 1

    def test_tiff_extension_handled(self, tmp_path: Path) -> None:
        img_path = self._write_image(tmp_path, "scan.tiff")
        with patch("pytesseract.image_to_string", return_value="tiff text"):
            records = list(OCRExtractor().extract(str(img_path)))
        assert len(records) == 1


# ---------------------------------------------------------------------------
# PDF extraction
# ---------------------------------------------------------------------------


class TestOCRExtractorPDF:
    def _make_fake_images(self, n: int):
        """Return a list of n minimal Pillow images."""
        from PIL import Image

        return [Image.new("RGB", (10, 10), color=(255, 255, 255)) for _ in range(n)]

    def _write_dummy_pdf(self, tmp_path: Path) -> Path:
        """Write an empty file with .pdf extension (content doesn't matter; convert is mocked)."""
        pdf_path = tmp_path / "test.pdf"
        pdf_path.write_bytes(b"%PDF-1.4 dummy")
        return pdf_path

    def test_yields_one_record_per_page(self, tmp_path: Path) -> None:
        pdf_path = self._write_dummy_pdf(tmp_path)
        images = self._make_fake_images(3)
        with (
            patch("pdf2image.convert_from_path", return_value=images),
            patch("pytesseract.image_to_string", return_value="page text"),
        ):
            records = list(OCRExtractor().extract(str(pdf_path)))
        assert len(records) == 3

    def test_page_numbers_are_sequential(self, tmp_path: Path) -> None:
        pdf_path = self._write_dummy_pdf(tmp_path)
        images = self._make_fake_images(2)
        with (
            patch("pdf2image.convert_from_path", return_value=images),
            patch("pytesseract.image_to_string", return_value="text"),
        ):
            records = list(OCRExtractor().extract(str(pdf_path)))
        assert records[0]["page_number"] == 1
        assert records[1]["page_number"] == 2

    def test_pdf_record_has_required_keys(self, tmp_path: Path) -> None:
        pdf_path = self._write_dummy_pdf(tmp_path)
        images = self._make_fake_images(1)
        with (
            patch("pdf2image.convert_from_path", return_value=images),
            patch("pytesseract.image_to_string", return_value="content"),
        ):
            record = list(OCRExtractor().extract(str(pdf_path)))[0]
        assert {"page_number", "text_content", "source_uri", "lang"} <= set(record.keys())

    def test_pdf_source_uri_in_record(self, tmp_path: Path) -> None:
        pdf_path = self._write_dummy_pdf(tmp_path)
        images = self._make_fake_images(1)
        with (
            patch("pdf2image.convert_from_path", return_value=images),
            patch("pytesseract.image_to_string", return_value=""),
        ):
            record = list(OCRExtractor().extract(str(pdf_path)))[0]
        assert record["source_uri"] == str(pdf_path)

    def test_dpi_forwarded_to_convert(self, tmp_path: Path) -> None:
        pdf_path = self._write_dummy_pdf(tmp_path)
        with (
            patch("pdf2image.convert_from_path", return_value=[]) as mock_convert,
            patch("pytesseract.image_to_string", return_value=""),
        ):
            list(OCRExtractor(dpi=300).extract(str(pdf_path)))
        call_kwargs = mock_convert.call_args[1]
        assert call_kwargs.get("dpi") == 300

    def test_empty_pdf_yields_nothing(self, tmp_path: Path) -> None:
        pdf_path = self._write_dummy_pdf(tmp_path)
        with (
            patch("pdf2image.convert_from_path", return_value=[]),
            patch("pytesseract.image_to_string", return_value=""),
        ):
            records = list(OCRExtractor().extract(str(pdf_path)))
        assert records == []


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


class TestOCRExtractorErrors:
    def test_missing_file_raises_file_not_found(self, tmp_path: Path) -> None:
        extractor = OCRExtractor()
        with pytest.raises(FileNotFoundError):
            list(extractor.extract(str(tmp_path / "nonexistent.png")))

    def test_convert_error_propagates(self, tmp_path: Path) -> None:
        pdf_path = tmp_path / "bad.pdf"
        pdf_path.write_bytes(b"dummy")
        with patch("pdf2image.convert_from_path", side_effect=RuntimeError("boom")):
            with pytest.raises(RuntimeError, match="boom"):
                list(OCRExtractor().extract(str(pdf_path)))

    def test_ocr_error_on_page_is_skipped(self, tmp_path: Path) -> None:
        """An OCR failure on a single page is logged and skipped (not raised)."""
        pdf_path = tmp_path / "test.pdf"
        pdf_path.write_bytes(b"dummy")
        from PIL import Image

        images = [
            Image.new("RGB", (10, 10)),
            Image.new("RGB", (10, 10)),
        ]
        side_effects = [RuntimeError("ocr fail"), "good page"]
        with (
            patch("pdf2image.convert_from_path", return_value=images),
            patch("pytesseract.image_to_string", side_effect=side_effects),
        ):
            records = list(OCRExtractor().extract(str(pdf_path)))
        # Only the successful page is yielded
        assert len(records) == 1
        assert records[0]["text_content"] == "good page"
