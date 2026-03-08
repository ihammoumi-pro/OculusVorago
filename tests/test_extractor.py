"""
Unit tests for CsvExtractor.
"""

from __future__ import annotations

import csv
import os
import tempfile
from pathlib import Path

import pytest

from vorago.extractors.csv_extractor import CsvExtractor


def _write_csv(rows: list[dict], delimiter: str = ",") -> str:
    """Write rows to a temp CSV file and return the file path."""
    fd, path = tempfile.mkstemp(suffix=".csv")
    try:
        with os.fdopen(fd, "w", newline="", encoding="utf-8") as fh:
            if rows:
                writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()), delimiter=delimiter)
                writer.writeheader()
                writer.writerows(rows)
    except Exception:
        os.unlink(path)
        raise
    return path


class TestCsvExtractor:
    def test_yields_all_rows(self) -> None:
        path = _write_csv([
            {"name": "Alice", "age": "30"},
            {"name": "Bob", "age": "25"},
        ])
        try:
            extractor = CsvExtractor()
            rows = list(extractor.extract(path))
            assert len(rows) == 2
            assert rows[0]["name"] == "Alice"
            assert rows[1]["name"] == "Bob"
        finally:
            os.unlink(path)

    def test_row_is_dict(self) -> None:
        path = _write_csv([{"col_a": "1", "col_b": "2"}])
        try:
            rows = list(CsvExtractor().extract(path))
            assert isinstance(rows[0], dict)
        finally:
            os.unlink(path)

    def test_skips_blank_rows(self) -> None:
        fd, path = tempfile.mkstemp(suffix=".csv")
        try:
            with os.fdopen(fd, "w", newline="", encoding="utf-8") as fh:
                fh.write("name,age\n")
                fh.write("Alice,30\n")
                fh.write(",\n")        # blank values
                fh.write("Bob,25\n")
            rows = list(CsvExtractor(skip_blank_lines=True).extract(path))
            assert len(rows) == 2
        finally:
            os.unlink(path)

    def test_does_not_skip_blank_rows_when_disabled(self) -> None:
        fd, path = tempfile.mkstemp(suffix=".csv")
        try:
            with os.fdopen(fd, "w", newline="", encoding="utf-8") as fh:
                fh.write("name,age\n")
                fh.write(",\n")
            rows = list(CsvExtractor(skip_blank_lines=False).extract(path))
            assert len(rows) == 1
        finally:
            os.unlink(path)

    def test_custom_delimiter(self) -> None:
        path = _write_csv([{"a": "1", "b": "2"}], delimiter=";")
        try:
            rows = list(CsvExtractor(delimiter=";").extract(path))
            assert rows[0]["a"] == "1"
        finally:
            os.unlink(path)

    def test_file_not_found_raises(self, tmp_path: Path) -> None:
        extractor = CsvExtractor()
        nonexistent = str(tmp_path / "vorago_nonexistent_file_xyz.csv")
        with pytest.raises(FileNotFoundError):
            list(extractor.extract(nonexistent))

    def test_empty_file_yields_nothing(self) -> None:
        fd, path = tempfile.mkstemp(suffix=".csv")
        try:
            os.close(fd)  # empty file
            rows = list(CsvExtractor().extract(path))
            assert rows == []
        finally:
            os.unlink(path)

    def test_header_only_yields_nothing(self) -> None:
        fd, path = tempfile.mkstemp(suffix=".csv")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                fh.write("name,age\n")
            rows = list(CsvExtractor().extract(path))
            assert rows == []
        finally:
            os.unlink(path)

    def test_large_file_is_streamed(self, tmp_path: Path) -> None:
        """Write 10 000 rows and assert memory is not blown (basic smoke test)."""
        csv_path = tmp_path / "large.csv"
        with csv_path.open("w", encoding="utf-8", newline="") as fh:
            writer = csv.writer(fh)
            writer.writerow(["id", "value"])
            for i in range(10_000):
                writer.writerow([str(i), f"val_{i}"])

        count = sum(1 for _ in CsvExtractor().extract(str(csv_path)))
        assert count == 10_000
