"""
API Extractor plugin for OculusVorago.

Streams records from RESTful JSON APIs one at a time, handling pagination
(cursor, offset/limit, or ``next`` URL) so that arbitrarily large datasets
can be processed with constant memory usage.

Authentication is supported via Bearer tokens or arbitrary header key/value
pairs.  Transient failures (rate-limits, timeouts, 5xx responses) are retried
with exponential back-off before raising.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Iterator
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from vorago.core.interfaces import IExtractor

logger = logging.getLogger(__name__)

_DEFAULT_TIMEOUT = 30  # seconds
_DEFAULT_MAX_RETRIES = 3
_DEFAULT_BACKOFF_FACTOR = 0.5


def _build_session(
    headers: dict[str, str] | None,
    bearer_token: str | None,
    max_retries: int,
    backoff_factor: float,
) -> requests.Session:
    """Return a :class:`requests.Session` pre-configured with auth and retries."""
    session = requests.Session()

    # Authentication
    merged_headers: dict[str, str] = {}
    if bearer_token:
        merged_headers["Authorization"] = f"Bearer {bearer_token}"
    if headers:
        merged_headers.update(headers)
    if merged_headers:
        session.headers.update(merged_headers)

    # Retry strategy: retry on connection errors, 429, and 5xx responses
    retry = Retry(
        total=max_retries,
        backoff_factor=backoff_factor,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


class APIExtractor(IExtractor):
    """
    Streaming REST API extractor with pagination support.

    Records are fetched page by page and yielded individually so that the
    pipeline processes only one record at a time regardless of how many
    pages the API returns.

    Three pagination styles are supported (auto-detected or specified via
    *pagination_style*):

    ``next_url``
        The response body contains a ``next`` (or ``next_url``) key whose
        value is the URL of the next page.  Iteration stops when that key
        is absent or ``null``.

    ``offset``
        The extractor appends ``?offset=N&limit=<page_size>`` to the base
        URL, incrementing ``offset`` by ``page_size`` until a page contains
        fewer records than ``page_size``.

    ``cursor``
        The response body contains a ``next_cursor`` key.  The extractor
        passes ``?cursor=<value>`` on subsequent requests until
        ``next_cursor`` is absent or ``null``.

    Args:
        records_key:
            Key in the JSON response body that holds the list of records.
            Defaults to ``None``, which means the extractor expects the
            response body itself to be a JSON array.
        pagination_style:
            ``'next_url'``, ``'offset'``, or ``'cursor'``.
            Defaults to ``'next_url'``.
        page_size:
            Number of records to request per page (used for ``offset``
            pagination).  Defaults to ``100``.
        bearer_token:
            Value for the ``Authorization: Bearer <token>`` header.
        headers:
            Arbitrary extra HTTP headers (e.g. ``{"X-API-Key": "secret"}``).
        timeout:
            HTTP request timeout in seconds.  Defaults to ``30``.
        max_retries:
            Number of automatic retries on transient errors.  Defaults to
            ``3``.
        backoff_factor:
            Back-off multiplier passed to :class:`urllib3.util.retry.Retry`.
            Defaults to ``0.5``.
        rate_limit_pause:
            Extra sleep in seconds after a 429 response if the adapter's
            built-in retry did not resolve it.  Defaults to ``1.0``.
    """

    def __init__(
        self,
        records_key: str | None = None,
        pagination_style: str = "next_url",
        page_size: int = 100,
        bearer_token: str | None = None,
        headers: dict[str, str] | None = None,
        timeout: int = _DEFAULT_TIMEOUT,
        max_retries: int = _DEFAULT_MAX_RETRIES,
        backoff_factor: float = _DEFAULT_BACKOFF_FACTOR,
        rate_limit_pause: float = 1.0,
    ) -> None:
        self.records_key = records_key
        self.pagination_style = pagination_style.lower()
        self.page_size = page_size
        self.bearer_token = bearer_token
        self.headers = headers
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.rate_limit_pause = rate_limit_pause

    # ------------------------------------------------------------------
    # IExtractor implementation
    # ------------------------------------------------------------------

    def extract(self, source_uri: str) -> Iterator[dict[str, Any]]:
        """
        Fetch *source_uri* (a REST endpoint) page by page, yielding one
        record at a time.

        Args:
            source_uri: Base URL of the REST API endpoint.

        Yields:
            One record dict at a time.
        """
        logger.info("APIExtractor: starting extraction from '%s'", source_uri)
        session = _build_session(
            self.headers, self.bearer_token, self.max_retries, self.backoff_factor
        )
        total = 0
        try:
            if self.pagination_style == "offset":
                yield from self._extract_offset(session, source_uri)
            elif self.pagination_style == "cursor":
                yield from self._extract_cursor(session, source_uri)
            else:
                yield from self._extract_next_url(session, source_uri)
            logger.info(
                "APIExtractor: finished '%s' — %d records emitted", source_uri, total
            )
        finally:
            session.close()

    # ------------------------------------------------------------------
    # Pagination strategies
    # ------------------------------------------------------------------

    def _fetch_page(
        self, session: requests.Session, url: str, params: dict[str, Any] | None = None
    ) -> Any:
        """Perform a GET request and return the parsed JSON body."""
        logger.debug("APIExtractor: GET %s params=%s", url, params)
        response = session.get(url, params=params, timeout=self.timeout)
        if response.status_code == 429:
            logger.warning(
                "APIExtractor: rate-limited (429) on %s — pausing %.1fs",
                url,
                self.rate_limit_pause,
            )
            time.sleep(self.rate_limit_pause)
            response = session.get(url, params=params, timeout=self.timeout)
        response.raise_for_status()
        return response.json()

    def _records_from_page(self, body: Any) -> list[dict[str, Any]]:
        """Extract the list of records from a page body."""
        if self.records_key:
            records = body.get(self.records_key, [])
        elif isinstance(body, list):
            records = body
        else:
            records = []
        return [r for r in records if isinstance(r, dict)]

    def _extract_next_url(
        self, session: requests.Session, url: str
    ) -> Iterator[dict[str, Any]]:
        """Paginate by following a ``next`` / ``next_url`` key in the response."""
        current_url: str | None = url
        while current_url:
            body = self._fetch_page(session, current_url)
            records = self._records_from_page(body)
            yield from records
            # Resolve next page URL
            if isinstance(body, dict):
                current_url = body.get("next") or body.get("next_url") or None
            else:
                current_url = None
            logger.debug(
                "APIExtractor: next_url page done (%d records), next=%s",
                len(records),
                current_url,
            )

    def _extract_offset(
        self, session: requests.Session, url: str
    ) -> Iterator[dict[str, Any]]:
        """Paginate using offset/limit query parameters."""
        offset = 0
        while True:
            params = {"offset": offset, "limit": self.page_size}
            body = self._fetch_page(session, url, params=params)
            records = self._records_from_page(body)
            yield from records
            logger.debug(
                "APIExtractor: offset page offset=%d, got %d records",
                offset,
                len(records),
            )
            if len(records) < self.page_size:
                break
            offset += self.page_size

    def _extract_cursor(
        self, session: requests.Session, url: str
    ) -> Iterator[dict[str, Any]]:
        """Paginate using a server-provided cursor token."""
        cursor: str | None = None
        while True:
            params: dict[str, Any] = {}
            if cursor:
                params["cursor"] = cursor
            body = self._fetch_page(session, url, params=params or None)
            records = self._records_from_page(body)
            yield from records
            next_cursor = body.get("next_cursor") if isinstance(body, dict) else None
            logger.debug(
                "APIExtractor: cursor page done (%d records), next_cursor=%s",
                len(records),
                next_cursor,
            )
            if not next_cursor:
                break
            cursor = next_cursor
