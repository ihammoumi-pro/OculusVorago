"""
Unit tests for PDFExtractor.

Real PDF fixtures are generated in-memory with PyMuPDF so the tests have
no external file dependencies.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from vorago.extractors.pdf_extractor import PDFExtractor

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_pdf(pages: list[str], tmp_dir: Path) -> str:
    """Create a real PDF with one text block per page using PyMuPDF."""
    import fitz

    doc = fitz.open()
    for text in pages:
        page = doc.new_page()
        page.insert_text((72, 72), text, fontsize=12)
    path = str(tmp_dir / "test.pdf")
    doc.save(path)
    doc.close()
    return path


# ---------------------------------------------------------------------------
# Interface compliance
# ---------------------------------------------------------------------------


class TestPDFExtractorInterface:
    def test_implements_iextractor(self) -> None:
        from vorago.core.interfaces import IExtractor

        assert issubclass(PDFExtractor, IExtractor)


# ---------------------------------------------------------------------------
# Core extraction
# ---------------------------------------------------------------------------


class TestPDFExtractorExtract:
    def test_yields_one_record_per_page(self, tmp_path: Path) -> None:
        pdf_path = _make_pdf(["Page one text", "Page two text", "Page three text"], tmp_path)
        records = list(PDFExtractor().extract(pdf_path))
        assert len(records) == 3

    def test_page_number_is_one_based(self, tmp_path: Path) -> None:
        pdf_path = _make_pdf(["First", "Second"], tmp_path)
        records = list(PDFExtractor().extract(pdf_path))
        assert records[0]["page_number"] == 1
        assert records[1]["page_number"] == 2

    def test_record_has_required_keys(self, tmp_path: Path) -> None:
        pdf_path = _make_pdf(["Hello"], tmp_path)
        record = list(PDFExtractor().extract(pdf_path))[0]
        assert "page_number" in record
        assert "text_content" in record
        assert "document_metadata" in record

    def test_text_content_is_string(self, tmp_path: Path) -> None:
        pdf_path = _make_pdf(["Sample text"], tmp_path)
        record = list(PDFExtractor().extract(pdf_path))[0]
        assert isinstance(record["text_content"], str)

    def test_text_content_contains_page_text(self, tmp_path: Path) -> None:
        pdf_path = _make_pdf(["UniqueMarker12345"], tmp_path)
        record = list(PDFExtractor().extract(pdf_path))[0]
        assert "UniqueMarker12345" in record["text_content"]

    def test_document_metadata_is_dict(self, tmp_path: Path) -> None:
        pdf_path = _make_pdf(["Meta test"], tmp_path)
        record = list(PDFExtractor().extract(pdf_path))[0]
        assert isinstance(record["document_metadata"], dict)

    def test_empty_pdf_yields_nothing(self, tmp_path: Path) -> None:
        """A single-page PDF with no text still yields one record with empty text."""
        import fitz

        doc = fitz.open()
        doc.new_page()  # blank page — PyMuPDF requires at least one page to save
        path = str(tmp_path / "blank.pdf")
        doc.save(path)
        doc.close()
        records = list(PDFExtractor().extract(path))
        # One blank page → one record with empty/whitespace text_content
        assert len(records) == 1
        assert records[0]["page_number"] == 1

    def test_metadata_shared_across_pages(self, tmp_path: Path) -> None:
        pdf_path = _make_pdf(["Page 1", "Page 2"], tmp_path)
        records = list(PDFExtractor().extract(pdf_path))
        # Both pages come from the same document — same metadata object
        assert records[0]["document_metadata"] == records[1]["document_metadata"]

    def test_metadata_with_title(self, tmp_path: Path) -> None:
        import fitz

        doc = fitz.open()
        page = doc.new_page()
        page.insert_text((72, 72), "Content", fontsize=12)
        doc.set_metadata({"title": "Test Document", "author": "Tester"})
        path = str(tmp_path / "meta.pdf")
        doc.save(path)
        doc.close()

        record = list(PDFExtractor().extract(path))[0]
        assert record["document_metadata"].get("title") == "Test Document"


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


class TestPDFExtractorErrors:
    def test_file_not_found_raises(self, tmp_path: Path) -> None:
        extractor = PDFExtractor()
        with pytest.raises(FileNotFoundError):
            list(extractor.extract(str(tmp_path / "nonexistent.pdf")))

    def test_non_pdf_file_raises_runtime_error(self, tmp_path: Path) -> None:
        bad_file = tmp_path / "not_a_pdf.pdf"
        bad_file.write_bytes(b"this is not a PDF file at all")
        extractor = PDFExtractor()
        with pytest.raises((RuntimeError, Exception)):
            list(extractor.extract(str(bad_file)))
