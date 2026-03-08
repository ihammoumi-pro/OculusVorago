"""
PDF Extractor plugin for OculusVorago.

Streams text from PDF files one page at a time using PyMuPDF (``fitz``).
Each yielded record contains the page number, the extracted text content,
and the document-level metadata (author, title, creation date, etc.).

Memory usage is proportional to a single page rather than to the whole
document, regardless of file size.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from vorago.core.interfaces import IExtractor

logger = logging.getLogger(__name__)


class PDFExtractor(IExtractor):
    """
    Streaming PDF text extractor backed by PyMuPDF (``fitz``).

    Each PDF page is opened, processed, and released individually so that
    large multi-hundred-page documents can be handled without loading the
    entire file into memory.

    Args:
        password:
            Optional decryption password for encrypted PDFs.
        text_flags:
            ``fitz.TEXT_*`` flag bitmask passed to ``Page.get_text()``.
            Defaults to ``0`` (plain text, preserving whitespace).
    """

    def __init__(
        self,
        password: str | None = None,
        text_flags: int = 0,
    ) -> None:
        self.password = password
        self.text_flags = text_flags

    # ------------------------------------------------------------------
    # IExtractor implementation
    # ------------------------------------------------------------------

    def extract(self, source_uri: str) -> Iterator[dict[str, Any]]:
        """
        Open the PDF at *source_uri* and yield one record per page.

        Args:
            source_uri: Path to the PDF file on the local filesystem.

        Yields:
            A ``dict`` with the following keys:

            * ``page_number`` (int): 1-based page index.
            * ``text_content`` (str): Plain text extracted from the page.
            * ``document_metadata`` (dict): Document-level metadata
              (title, author, creation date, etc.) from the PDF header.

        Raises:
            FileNotFoundError: When *source_uri* does not exist.
            RuntimeError: When PyMuPDF cannot open or decrypt the file.
        """
        import fitz  # PyMuPDF — imported lazily to keep startup fast

        logger.info("PDFExtractor: opening '%s'", source_uri)
        if not Path(source_uri).exists():
            logger.error("PDFExtractor: file not found — '%s'", source_uri)
            raise FileNotFoundError(f"PDFExtractor: file not found — '{source_uri}'")
        try:
            doc = fitz.open(source_uri)
        except Exception as exc:
            logger.error("PDFExtractor: cannot open '%s' — %s", source_uri, exc)
            raise RuntimeError(
                f"PDFExtractor: cannot open '{source_uri}': {exc}"
            ) from exc

        try:
            if doc.needs_pass:
                if not self.password or not doc.authenticate(self.password):
                    raise RuntimeError(
                        f"PDFExtractor: '{source_uri}' is encrypted and the "
                        "supplied password is incorrect or missing."
                    )

            metadata = self._build_metadata(doc)
            total_pages = doc.page_count
            logger.debug(
                "PDFExtractor: '%s' — %d pages, metadata=%s",
                source_uri,
                total_pages,
                metadata,
            )

            for page_index in range(total_pages):
                try:
                    page = doc.load_page(page_index)
                    text = page.get_text(flags=self.text_flags)
                    yield {
                        "page_number": page_index + 1,
                        "text_content": text,
                        "document_metadata": metadata,
                    }
                except Exception as exc:  # noqa: BLE001
                    logger.warning(
                        "PDFExtractor: error on page %d of '%s' — %s",
                        page_index + 1,
                        source_uri,
                        exc,
                    )
                    continue
        finally:
            doc.close()
            logger.info("PDFExtractor: closed '%s'", source_uri)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _build_metadata(doc: Any) -> dict[str, Any]:
        """Extract and clean the document-level metadata dictionary."""
        raw: dict[str, Any] = doc.metadata or {}
        # Filter out empty strings so the payload stays clean
        return {k: v for k, v in raw.items() if v}
