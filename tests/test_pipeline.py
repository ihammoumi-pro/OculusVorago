"""
Integration-style tests for the Pipeline engine.

These tests use real CsvExtractor + DynamicMapper but a stub ILoader that
captures output in memory, so no Kafka broker is required.
"""

from __future__ import annotations

import csv
import os
import tempfile
from collections.abc import Iterator
from typing import Any

from vorago.core.config_models import EntityMapping, MappingConfig, PropertyMapping
from vorago.core.interfaces import ILoader
from vorago.engine.pipeline import Pipeline
from vorago.extractors.csv_extractor import CsvExtractor

# ---------------------------------------------------------------------------
# Stub loader
# ---------------------------------------------------------------------------


class CapturingLoader(ILoader):
    """Collects all payloads into a list for assertion."""

    def __init__(self) -> None:
        self.records: list[dict[str, Any]] = []

    def load(self, records: Iterator[dict[str, Any]]) -> None:
        for record in records:
            self.records.append(record)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _temp_csv(rows: list[dict[str, Any]]) -> str:
    fd, path = tempfile.mkstemp(suffix=".csv")
    with os.fdopen(fd, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    return path


def _simple_config() -> MappingConfig:
    return MappingConfig(
        source_system_name="PIPE_TEST",
        default_classification="UNCLASSIFIED",
        entities=[
            EntityMapping(
                target_entity_type="PERSON",
                property_mappings={
                    "name": PropertyMapping(source_field="full_name"),
                    "age": PropertyMapping(source_field="age", cast_as="int"),
                },
            )
        ],
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestPipelineRun:
    def test_end_to_end(self) -> None:
        rows = [
            {"full_name": "Alice", "age": "30"},
            {"full_name": "Bob", "age": "25"},
        ]
        path = _temp_csv(rows)
        try:
            loader = CapturingLoader()
            pipeline = Pipeline(CsvExtractor(), _simple_config(), loader)
            summary = pipeline.run(path)

            assert summary["rows_read"] == 2
            assert summary["rows_mapped"] == 2
            assert summary["payloads_produced"] == 2
            assert summary["rows_failed"] == 0
            assert len(loader.records) == 2
            assert loader.records[0]["properties"]["name"] == "Alice"
            assert loader.records[1]["properties"]["age"] == 25
        finally:
            os.unlink(path)

    def test_bad_row_goes_to_dlq_pipeline_continues(self) -> None:
        """A row that makes the mapper raise should be counted in rows_failed."""
        rows = [
            {"full_name": "Alice", "age": "30"},
            {"full_name": "Bob", "age": "30"},
        ]
        path = _temp_csv(rows)
        try:
            loader = CapturingLoader()

            # Monkey-patch mapper to fail on second call
            pipeline = Pipeline(CsvExtractor(), _simple_config(), loader)
            original_map = pipeline.mapper.map_record
            call_count = [0]

            def failing_map(record, config):
                call_count[0] += 1
                if call_count[0] == 2:
                    raise ValueError("Simulated mapping error")
                return original_map(record, config)

            pipeline.mapper.map_record = failing_map  # type: ignore[method-assign]
            summary = pipeline.run(path)

            assert summary["rows_read"] == 2
            assert summary["rows_failed"] == 1
            assert len(loader.records) == 1
        finally:
            os.unlink(path)

    def test_summary_contains_elapsed_and_rps(self) -> None:
        path = _temp_csv([{"full_name": "X", "age": "1"}])
        try:
            loader = CapturingLoader()
            summary = Pipeline(CsvExtractor(), _simple_config(), loader).run(path)
            assert "elapsed_seconds" in summary
            assert "rows_per_second" in summary
            assert summary["elapsed_seconds"] >= 0
        finally:
            os.unlink(path)

    def test_empty_csv_produces_zero_counts(self) -> None:
        fd, path = tempfile.mkstemp(suffix=".csv")
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write("full_name,age\n")  # header only
        try:
            loader = CapturingLoader()
            summary = Pipeline(CsvExtractor(), _simple_config(), loader).run(path)
            assert summary["rows_read"] == 0
            assert summary["payloads_produced"] == 0
        finally:
            os.unlink(path)
