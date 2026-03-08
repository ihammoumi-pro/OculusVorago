"""
OCR Extractor plugin for OculusVorago.

Extracts text from scanned image files (JPEG, PNG, TIFF, …) and scanned PDF
documents using ``pytesseract`` and ``Pillow``.  Scanned PDFs are first
converted to images with ``pdf2image`` before OCR is applied.

Each page / image is processed and yielded individually so the pipeline
handles large multi-page documents without blowing up memory.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from vorago.core.interfaces import IExtractor

logger = logging.getLogger(__name__)

# Extensions treated as single-image files (not multi-page PDFs)
_IMAGE_EXTENSIONS = frozenset(
    {".jpg", ".jpeg", ".png", ".tiff", ".tif", ".bmp", ".gif", ".webp"}
)


class OCRExtractor(IExtractor):
    """
    Streaming OCR extractor backed by ``pytesseract`` and ``Pillow``.

    * **Image files** (JPEG, PNG, TIFF, …): a single record is yielded.
    * **PDF files**: each page is rasterised with ``pdf2image`` and OCR'd
      separately; one record is yielded per page.

    Args:
        lang:
            Tesseract language string (e.g. ``'eng'``, ``'fra+eng'``).
            Defaults to ``'eng'``.
        dpi:
            Resolution (dots per inch) used when converting PDF pages to
            images.  Higher values improve OCR accuracy at the cost of
            speed and memory.  Defaults to ``200``.
        tesseract_config:
            Additional flags passed verbatim to Tesseract via
            ``pytesseract.image_to_string``.  Example: ``'--psm 6'``.
        pdf_password:
            Password for encrypted PDFs passed to ``pdf2image``.
    """

    def __init__(
        self,
        lang: str = "eng",
        dpi: int = 200,
        tesseract_config: str = "",
        pdf_password: str | None = None,
    ) -> None:
        self.lang = lang
        self.dpi = dpi
        self.tesseract_config = tesseract_config
        self.pdf_password = pdf_password

    # ------------------------------------------------------------------
    # IExtractor implementation
    # ------------------------------------------------------------------

    def extract(self, source_uri: str) -> Iterator[dict[str, Any]]:
        """
        Apply OCR to *source_uri* and yield one record per page / image.

        Args:
            source_uri: Path to a PDF file or an image file on the local
                        filesystem.

        Yields:
            A ``dict`` with:

            * ``page_number`` (int): 1-based index (always ``1`` for
              single-image inputs).
            * ``text_content`` (str): OCR'd text for that page/image.
            * ``source_uri`` (str): The original file path.
            * ``lang`` (str): Tesseract language code used.

        Raises:
            FileNotFoundError: When *source_uri* does not exist.
        """
        path = Path(source_uri)
        if not path.exists():
            logger.error("OCRExtractor: file not found — '%s'", source_uri)
            raise FileNotFoundError(
                f"OCRExtractor: file not found — '{source_uri}'"
            )

        suffix = path.suffix.lower()
        if suffix == ".pdf":
            yield from self._extract_pdf(source_uri)
        elif suffix in _IMAGE_EXTENSIONS:
            yield from self._extract_image(source_uri)
        else:
            # Try treating unknown extensions as images
            logger.warning(
                "OCRExtractor: unknown extension '%s', attempting image OCR", suffix
            )
            yield from self._extract_image(source_uri)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _extract_image(self, source_uri: str) -> Iterator[dict[str, Any]]:
        """OCR a single image file and yield one record."""
        import pytesseract
        from PIL import Image

        logger.info("OCRExtractor: processing image '%s'", source_uri)
        try:
            with Image.open(source_uri) as img:
                text = pytesseract.image_to_string(
                    img, lang=self.lang, config=self.tesseract_config
                )
            yield {
                "page_number": 1,
                "text_content": text,
                "source_uri": source_uri,
                "lang": self.lang,
            }
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "OCRExtractor: error processing image '%s' — %s", source_uri, exc
            )

    def _extract_pdf(self, source_uri: str) -> Iterator[dict[str, Any]]:
        """Convert each PDF page to an image, OCR it, and yield one record."""
        import pytesseract
        from pdf2image import convert_from_path

        logger.info("OCRExtractor: converting PDF '%s' to images (dpi=%d)", source_uri, self.dpi)
        kwargs: dict[str, Any] = {"dpi": self.dpi}
        if self.pdf_password:
            kwargs["userpw"] = self.pdf_password

        try:
            images = convert_from_path(source_uri, **kwargs)
        except Exception as exc:
            logger.error(
                "OCRExtractor: failed to convert PDF '%s' — %s", source_uri, exc
            )
            raise

        logger.debug("OCRExtractor: PDF has %d page(s)", len(images))
        for page_number, img in enumerate(images, start=1):
            try:
                text = pytesseract.image_to_string(
                    img, lang=self.lang, config=self.tesseract_config
                )
                yield {
                    "page_number": page_number,
                    "text_content": text,
                    "source_uri": source_uri,
                    "lang": self.lang,
                }
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "OCRExtractor: error on page %d of '%s' — %s",
                    page_number,
                    source_uri,
                    exc,
                )
                continue
