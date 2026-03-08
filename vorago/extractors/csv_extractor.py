"""
CSV Extractor plugin for OculusVorago.

Streams rows from a CSV file one at a time, keeping memory usage constant
regardless of file size.  Malformed lines are logged and skipped rather
than crashing the pipeline.
"""

from __future__ import annotations

import csv
import logging
from collections.abc import Iterator
from typing import Any

from vorago.core.interfaces import IExtractor

logger = logging.getLogger(__name__)


class CsvExtractor(IExtractor):
    """
    Streaming CSV extractor.

    Each row is yielded as a plain ``dict`` keyed by the CSV header names.
    Empty rows and rows that raise a parsing error are silently skipped
    after logging a warning so that the pipeline keeps running.

    Args:
        encoding: File encoding passed to ``open()``.  Defaults to
                  ``'utf-8-sig'`` which also strips the UTF-8 BOM that
                  Excel sometimes adds to CSV exports.
        delimiter: Column delimiter character.  Defaults to ``','``.
        skip_blank_lines: When ``True`` (default) rows where every value
                          is empty / whitespace are not yielded.
    """

    def __init__(
        self,
        encoding: str = "utf-8-sig",
        delimiter: str = ",",
        skip_blank_lines: bool = True,
    ) -> None:
        self.encoding = encoding
        self.delimiter = delimiter
        self.skip_blank_lines = skip_blank_lines

    def extract(self, source_uri: str) -> Iterator[dict[str, Any]]:
        """
        Open *source_uri* as a CSV file and yield rows as dicts.

        Args:
            source_uri: Path to the CSV file on the local filesystem.

        Yields:
            One ``dict`` per data row, using the header row as keys.
        """
        logger.info("CsvExtractor: opening '%s'", source_uri)
        row_number = 0
        try:
            with open(source_uri, newline="", encoding=self.encoding) as fh:
                reader = csv.DictReader(fh, delimiter=self.delimiter)
                for row_number, raw_row in enumerate(reader, start=1):
                    try:
                        record: dict[str, Any] = dict(raw_row)

                        if self.skip_blank_lines and all(
                            (v is None or str(v).strip() == "")
                            for v in record.values()
                        ):
                            logger.debug(
                                "CsvExtractor: skipping blank row %d", row_number
                            )
                            continue

                        yield record

                    except Exception as exc:  # noqa: BLE001
                        logger.warning(
                            "CsvExtractor: error parsing row %d — %s",
                            row_number,
                            exc,
                        )
                        continue

        except FileNotFoundError:
            logger.error("CsvExtractor: file not found — '%s'", source_uri)
            raise
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "CsvExtractor: unexpected error after row %d — %s", row_number, exc
            )
            raise

        logger.info("CsvExtractor: finished reading '%s' (%d rows)", source_uri, row_number)
