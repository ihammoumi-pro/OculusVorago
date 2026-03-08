"""
Unit tests for SQLExtractor.

Uses an in-memory SQLite database (via SQLAlchemy) so no external database
server is required.
"""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine, text

from vorago.extractors.sql_extractor import SQLExtractor

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def sqlite_uri(tmp_path) -> str:
    """Create an in-memory-like SQLite DB and return the file URI."""
    db_path = tmp_path / "test.db"
    engine = create_engine(f"sqlite:///{db_path}")
    with engine.connect() as conn:
        conn.execute(
            text(
                "CREATE TABLE users "
                "(id INTEGER PRIMARY KEY, name TEXT, score REAL)"
            )
        )
        conn.execute(
            text("INSERT INTO users VALUES (1, 'Alice', 9.5)"),
        )
        conn.execute(
            text("INSERT INTO users VALUES (2, 'Bob', 8.0)"),
        )
        conn.execute(
            text("INSERT INTO users VALUES (3, 'Carol', 7.5)"),
        )
        conn.commit()
    engine.dispose()
    return f"sqlite:///{db_path}"


# ---------------------------------------------------------------------------
# Interface compliance
# ---------------------------------------------------------------------------


class TestSQLExtractorInterface:
    def test_implements_iextractor(self) -> None:
        from vorago.core.interfaces import IExtractor

        assert issubclass(SQLExtractor, IExtractor)

    def test_raises_if_both_query_and_table_given(self) -> None:
        with pytest.raises(ValueError, match="not both"):
            SQLExtractor(query="SELECT 1", table="users")


# ---------------------------------------------------------------------------
# Query-based extraction
# ---------------------------------------------------------------------------


class TestSQLExtractorQuery:
    def test_yields_all_rows(self, sqlite_uri: str) -> None:
        extractor = SQLExtractor(query="SELECT * FROM users")
        rows = list(extractor.extract(sqlite_uri))
        assert len(rows) == 3

    def test_rows_are_dicts(self, sqlite_uri: str) -> None:
        extractor = SQLExtractor(query="SELECT * FROM users")
        rows = list(extractor.extract(sqlite_uri))
        for row in rows:
            assert isinstance(row, dict)

    def test_column_names_are_keys(self, sqlite_uri: str) -> None:
        extractor = SQLExtractor(query="SELECT id, name FROM users")
        rows = list(extractor.extract(sqlite_uri))
        assert set(rows[0].keys()) == {"id", "name"}

    def test_values_are_correct(self, sqlite_uri: str) -> None:
        extractor = SQLExtractor(query="SELECT name, score FROM users ORDER BY id")
        rows = list(extractor.extract(sqlite_uri))
        assert rows[0]["name"] == "Alice"
        assert rows[0]["score"] == pytest.approx(9.5)

    def test_filtered_query(self, sqlite_uri: str) -> None:
        extractor = SQLExtractor(query="SELECT * FROM users WHERE score > 8.0")
        rows = list(extractor.extract(sqlite_uri))
        assert len(rows) == 1
        assert rows[0]["name"] == "Alice"

    def test_empty_result_yields_nothing(self, sqlite_uri: str) -> None:
        extractor = SQLExtractor(query="SELECT * FROM users WHERE 1=0")
        rows = list(extractor.extract(sqlite_uri))
        assert rows == []

    def test_streaming_chunk_size(self, sqlite_uri: str) -> None:
        """chunk_size=1 still yields all rows correctly."""
        extractor = SQLExtractor(query="SELECT * FROM users", chunk_size=1)
        rows = list(extractor.extract(sqlite_uri))
        assert len(rows) == 3


# ---------------------------------------------------------------------------
# Table-based extraction
# ---------------------------------------------------------------------------


class TestSQLExtractorTable:
    def test_table_yields_all_rows(self, sqlite_uri: str) -> None:
        extractor = SQLExtractor(table="users")
        rows = list(extractor.extract(sqlite_uri))
        assert len(rows) == 3

    def test_table_rows_are_dicts(self, sqlite_uri: str) -> None:
        extractor = SQLExtractor(table="users")
        rows = list(extractor.extract(sqlite_uri))
        assert all(isinstance(r, dict) for r in rows)

    def test_no_query_or_table_raises(self, sqlite_uri: str) -> None:
        extractor = SQLExtractor()
        with pytest.raises(ValueError, match="requires either"):
            list(extractor.extract(sqlite_uri))

    def test_unsafe_table_name_raises(self, sqlite_uri: str) -> None:
        extractor = SQLExtractor(table="users; DROP TABLE users")
        with pytest.raises(ValueError, match="unsafe"):
            list(extractor.extract(sqlite_uri))


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


class TestSQLExtractorErrors:
    def test_bad_connection_string_raises(self) -> None:
        from sqlalchemy.exc import OperationalError

        # SQLite will raise OperationalError when the parent directory doesn't exist
        extractor = SQLExtractor(query="SELECT 1")
        with pytest.raises(OperationalError):
            list(extractor.extract("sqlite:////nonexistent/path/that/wont/work/db.sqlite"))

    def test_bad_query_raises(self, sqlite_uri: str) -> None:
        from sqlalchemy.exc import OperationalError

        extractor = SQLExtractor(query="SELECT * FROM nonexistent_table_xyz")
        with pytest.raises(OperationalError):
            list(extractor.extract(sqlite_uri))


# ---------------------------------------------------------------------------
# URI redaction helper
# ---------------------------------------------------------------------------


class TestRedactUri:
    def test_password_is_redacted(self) -> None:
        uri = "postgresql+psycopg2://user:s3cr3t@localhost:5432/mydb"
        result = SQLExtractor._redact_uri(uri)
        assert "s3cr3t" not in result
        assert "***" in result

    def test_no_password_unchanged(self) -> None:
        uri = "sqlite:///./data/local.db"
        assert SQLExtractor._redact_uri(uri) == uri
