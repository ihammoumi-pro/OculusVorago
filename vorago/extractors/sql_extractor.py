"""
SQL Extractor plugin for OculusVorago.

Streams rows from any SQLAlchemy-supported relational database (PostgreSQL,
MySQL, SQLite, MSSQL, Oracle, …) one row at a time using server-side cursors
(``yield_per``), keeping memory usage constant regardless of table size.

The caller supplies either a raw SQL query or a table name.  The connection is
managed via a context manager so it is always cleanly closed, even if an error
occurs mid-stream.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from typing import Any

from sqlalchemy import create_engine, text

from vorago.core.interfaces import IExtractor

logger = logging.getLogger(__name__)

_DEFAULT_CHUNK_SIZE = 1_000


class SQLExtractor(IExtractor):
    """
    Streaming SQL extractor backed by SQLAlchemy.

    Rows are fetched in configurable chunks using ``yield_per`` (server-side
    cursor semantics) and yielded individually as plain ``dict`` objects so
    that the pipeline never holds more than one row in memory at a time.

    Args:
        query:
            A raw SQL SELECT statement to execute.  Mutually exclusive with
            *table*.  At least one of *query* or *table* must be provided when
            :meth:`extract` is called.
        table:
            Name of the table to perform a full ``SELECT *`` scan on.
            Ignored when *query* is set.
        chunk_size:
            Number of rows to fetch from the database per round-trip.
            This controls memory usage on the client side; it does not limit
            the total number of rows returned.  Defaults to ``1 000``.
        connect_args:
            Extra keyword arguments forwarded verbatim to
            :func:`sqlalchemy.create_engine` as ``connect_args``.  Use this
            for SSL certificates, timeouts, etc.
        engine_kwargs:
            Extra keyword arguments forwarded verbatim to
            :func:`sqlalchemy.create_engine`.

    Note:
        The *source_uri* passed to :meth:`extract` must be a valid
        `SQLAlchemy connection URL
        <https://docs.sqlalchemy.org/en/20/core/engines.html>`_, e.g.
        ``postgresql+psycopg2://user:pass@host:5432/dbname`` or
        ``sqlite:///./data/local.db``.
    """

    def __init__(
        self,
        query: str | None = None,
        table: str | None = None,
        chunk_size: int = _DEFAULT_CHUNK_SIZE,
        connect_args: dict[str, Any] | None = None,
        engine_kwargs: dict[str, Any] | None = None,
    ) -> None:
        if query and table:
            raise ValueError("Provide either 'query' or 'table', not both.")
        self.query = query
        self.table = table
        self.chunk_size = chunk_size
        self._connect_args: dict[str, Any] = connect_args or {}
        self._engine_kwargs: dict[str, Any] = engine_kwargs or {}

    # ------------------------------------------------------------------
    # IExtractor implementation
    # ------------------------------------------------------------------

    def extract(self, source_uri: str) -> Iterator[dict[str, Any]]:
        """
        Connect to *source_uri* and stream rows one at a time.

        Args:
            source_uri: A SQLAlchemy connection URL
                        (e.g. ``sqlite:///./path/to/db.sqlite3``).

        Yields:
            One row as a plain ``dict`` (column name → value).

        Raises:
            ValueError: When neither *query* nor *table* was supplied.
        """
        sql = self._resolve_sql()
        logger.info(
            "SQLExtractor: connecting to '%s'",
            self._redact_uri(source_uri),
        )
        engine = create_engine(
            source_uri,
            connect_args=self._connect_args,
            **self._engine_kwargs,
        )
        row_count = 0
        try:
            with engine.connect() as conn:
                result = conn.execution_options(yield_per=self.chunk_size).execute(
                    text(sql)
                )
                columns = list(result.keys())
                logger.debug("SQLExtractor: columns = %s", columns)
                for row in result:
                    yield dict(zip(columns, row, strict=False))
                    row_count += 1
        except Exception:
            logger.error(
                "SQLExtractor: error after %d rows — re-raising", row_count
            )
            raise
        finally:
            engine.dispose()
            logger.info(
                "SQLExtractor: finished — %d rows streamed from '%s'",
                row_count,
                self._redact_uri(source_uri),
            )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _resolve_sql(self) -> str:
        """Return the SQL string to execute, derived from *query* or *table*."""
        if self.query:
            return self.query
        if self.table:
            # Basic identifier safety: reject if table name contains
            # semicolons or comment markers that could indicate injection.
            safe_name = self.table.strip()
            if any(c in safe_name for c in (";", "--", "/*", "*/")):
                raise ValueError(
                    f"Potentially unsafe table name rejected: '{self.table}'"
                )
            return f"SELECT * FROM {safe_name}"  # noqa: S608
        raise ValueError(
            "SQLExtractor requires either 'query' or 'table' to be set."
        )

    @staticmethod
    def _redact_uri(uri: str) -> str:
        """Remove the password component from a connection URI for safe logging."""
        try:
            from urllib.parse import urlparse, urlunparse

            parsed = urlparse(uri)
            if parsed.password:
                redacted = parsed._replace(
                    netloc=parsed.netloc.replace(f":{parsed.password}@", ":***@")
                )
                return urlunparse(redacted)
        except Exception:  # noqa: BLE001
            pass
        return uri
