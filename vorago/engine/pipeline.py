"""
Pipeline orchestrator for OculusVorago.

The :class:`Pipeline` class ties an IExtractor, a DynamicMapper, and an
ILoader together into a single streaming ETL pass.  Key design goals:

* **Memory efficiency** — records flow through the pipeline one at a time
  using Python generators; the full dataset is never held in RAM.
* **Resilience** — mapping errors on individual rows are caught, logged,
  and the row is sent to the Dead-Letter Queue (DLQ).  One bad row cannot
  crash a multi-million-row job.
* **Observability** — comprehensive Python logging including rows/second
  throughput, DLQ counts, and per-batch progress.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Iterator
from typing import Any

from vorago.core.config_models import MappingConfig
from vorago.core.interfaces import IExtractor, ILoader
from vorago.mappers.dynamic_mapper import DynamicMapper

logger = logging.getLogger(__name__)

# Default rows-between-progress-logs (can be overridden per Pipeline instance).
_DEFAULT_PROGRESS_INTERVAL = 10_000


class Pipeline:
    """
    Streaming ETL pipeline.

    Args:
        extractor:         A concrete IExtractor instance.
        config:            A loaded :class:`~vorago.core.config_models.MappingConfig`.
        loader:            A concrete ILoader instance.
        mapper:            Optional custom IMapper.  Defaults to
                           :class:`~vorago.mappers.dynamic_mapper.DynamicMapper`.
        progress_interval: Emit a progress log every N rows.  Defaults to
                           10 000.  Set to 0 to disable progress logging.
    """

    def __init__(
        self,
        extractor: IExtractor,
        config: MappingConfig,
        loader: ILoader,
        mapper: DynamicMapper | None = None,
        progress_interval: int = _DEFAULT_PROGRESS_INTERVAL,
    ) -> None:
        self.extractor = extractor
        self.config = config
        self.loader = loader
        self.mapper = mapper or DynamicMapper()
        self.progress_interval = progress_interval

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(self, source_uri: str) -> dict[str, int]:
        """
        Execute the full ETL pipeline for *source_uri*.

        Args:
            source_uri: The source location forwarded to the extractor
                        (e.g. a file path or API endpoint URL).

        Returns:
            A summary dict with keys:
            ``rows_read``, ``rows_mapped``, ``payloads_produced``,
            ``rows_failed``, ``elapsed_seconds``, ``rows_per_second``.
        """
        logger.info(
            "Pipeline.run() starting — source='%s', system='%s'",
            source_uri,
            self.config.source_system_name,
        )
        start_time = time.monotonic()

        # Use single-element lists as mutable counter references that can be
        # updated inside the generator and read back here after exhaustion.
        rows_read_ref: list[int] = [0]
        rows_mapped_ref: list[int] = [0]
        payloads_ref: list[int] = [0]
        rows_failed_ref: list[int] = [0]

        self.loader.load(
            self._mapped_records(
                source_uri=source_uri,
                rows_read_ref=rows_read_ref,
                rows_mapped_ref=rows_mapped_ref,
                payloads_ref=payloads_ref,
                rows_failed_ref=rows_failed_ref,
            )
        )

        elapsed = time.monotonic() - start_time
        rows_read = rows_read_ref[0]
        rows_per_second = rows_read / elapsed if elapsed > 0 else 0.0

        summary = {
            "rows_read": rows_read,
            "rows_mapped": rows_mapped_ref[0],
            "payloads_produced": payloads_ref[0],
            "rows_failed": rows_failed_ref[0],
            "elapsed_seconds": round(elapsed, 3),
            "rows_per_second": round(rows_per_second, 1),
        }
        logger.info("Pipeline finished — %s", summary)
        return summary

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _mapped_records(
        self,
        source_uri: str,
        rows_read_ref: list[int],
        rows_mapped_ref: list[int],
        payloads_ref: list[int],
        rows_failed_ref: list[int],
    ) -> Iterator[dict[str, Any]]:
        """
        Generator that extracts, maps, and yields payloads one at a time.

        Counter lists are mutated in-place so that the outer ``run()``
        method can read the final values after the generator is exhausted.
        """
        start_time = time.monotonic()

        for raw_record in self.extractor.extract(source_uri):
            rows_read_ref[0] += 1

            try:
                payloads = self.mapper.map_record(raw_record, self.config)
            except Exception as exc:  # noqa: BLE001
                rows_failed_ref[0] += 1
                logger.warning(
                    "Pipeline: DLQ — row %d mapping failed (%s). Record: %r",
                    rows_read_ref[0],
                    exc,
                    raw_record,
                )
                continue

            if not payloads:
                # No output from this record (e.g. no entity mappings configured).
                continue

            rows_mapped_ref[0] += 1
            for payload in payloads:
                payloads_ref[0] += 1
                yield payload

            # Progress log
            if (
                self.progress_interval > 0
                and rows_read_ref[0] % self.progress_interval == 0
            ):
                elapsed = time.monotonic() - start_time
                rps = rows_read_ref[0] / elapsed if elapsed > 0 else 0.0
                logger.info(
                    "Pipeline progress: %d rows read, %d mapped, "
                    "%d failed, %.1f rows/s",
                    rows_read_ref[0],
                    rows_mapped_ref[0],
                    rows_failed_ref[0],
                    rps,
                )

        # Propagate final counts back to the outer scope.
        # (The list mutation already did this; this statement silences
        #  linters about unused variables.)
        logger.debug(
            "Pipeline generator exhausted: %d read, %d mapped, %d failed",
            rows_read_ref[0],
            rows_mapped_ref[0],
            rows_failed_ref[0],
        )
