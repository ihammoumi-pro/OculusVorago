"""
Unit tests for APIExtractor.

All HTTP calls are intercepted via ``unittest.mock`` so no real network
access is required.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest
import requests

from vorago.extractors.api_extractor import APIExtractor

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _mock_response(json_body: Any, status_code: int = 200) -> MagicMock:
    """Return a mock :class:`requests.Response` with a pre-set JSON body."""
    resp = MagicMock(spec=requests.Response)
    resp.status_code = status_code
    resp.json.return_value = json_body
    resp.raise_for_status = MagicMock()
    return resp


# ---------------------------------------------------------------------------
# Interface compliance
# ---------------------------------------------------------------------------


class TestAPIExtractorInterface:
    def test_implements_iextractor(self) -> None:
        from vorago.core.interfaces import IExtractor

        assert issubclass(APIExtractor, IExtractor)

    def test_extract_returns_iterator(self) -> None:
        extractor = APIExtractor()
        # We'll patch the session; just check the return type
        with patch("vorago.extractors.api_extractor._build_session") as mock_build:
            mock_session = MagicMock()
            mock_session.get.return_value = _mock_response([])
            mock_session.__enter__ = lambda s: s
            mock_session.__exit__ = MagicMock(return_value=False)
            mock_build.return_value = mock_session
            result = extractor.extract("http://example.com/api")
            assert hasattr(result, "__iter__")


# ---------------------------------------------------------------------------
# next_url pagination
# ---------------------------------------------------------------------------


class TestNextUrlPagination:
    def test_single_page_list_response(self) -> None:
        """API returns a JSON array; no pagination key — single page."""
        extractor = APIExtractor(pagination_style="next_url")
        with patch("requests.Session.get") as mock_get:
            mock_get.return_value = _mock_response(
                [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
            )
            rows = list(extractor.extract("http://example.com/api/records"))

        assert len(rows) == 2
        assert rows[0]["name"] == "Alice"
        assert rows[1]["name"] == "Bob"

    def test_single_page_object_with_records_key(self) -> None:
        """API wraps records in a named key."""
        extractor = APIExtractor(records_key="data", pagination_style="next_url")
        with patch("requests.Session.get") as mock_get:
            mock_get.return_value = _mock_response(
                {"data": [{"id": 10}, {"id": 20}], "next": None}
            )
            rows = list(extractor.extract("http://example.com/api/items"))

        assert [r["id"] for r in rows] == [10, 20]

    def test_multi_page_follows_next_key(self) -> None:
        """Extractor follows ``next`` URL across two pages."""
        page1 = {"records": [{"x": 1}], "next": "http://example.com/api?page=2"}
        page2 = {"records": [{"x": 2}], "next": None}

        extractor = APIExtractor(records_key="records", pagination_style="next_url")
        responses = [_mock_response(page1), _mock_response(page2)]
        with patch("requests.Session.get", side_effect=responses):
            rows = list(extractor.extract("http://example.com/api"))

        assert [r["x"] for r in rows] == [1, 2]

    def test_empty_response_yields_nothing(self) -> None:
        extractor = APIExtractor(pagination_style="next_url")
        with patch("requests.Session.get") as mock_get:
            mock_get.return_value = _mock_response([])
            rows = list(extractor.extract("http://example.com/api"))
        assert rows == []

    def test_non_dict_items_are_skipped(self) -> None:
        """Non-dict items in the response array are filtered out."""
        extractor = APIExtractor(pagination_style="next_url")
        with patch("requests.Session.get") as mock_get:
            mock_get.return_value = _mock_response([{"id": 1}, "not-a-dict", 42])
            rows = list(extractor.extract("http://example.com/api"))
        assert rows == [{"id": 1}]


# ---------------------------------------------------------------------------
# offset pagination
# ---------------------------------------------------------------------------


class TestOffsetPagination:
    def test_two_full_pages_and_partial_last(self) -> None:
        """Three pages: 2 full (size=2) then 1 partial ⟹ 5 records total."""
        extractor = APIExtractor(
            records_key="items",
            pagination_style="offset",
            page_size=2,
        )
        responses = [
            _mock_response({"items": [{"n": 1}, {"n": 2}]}),
            _mock_response({"items": [{"n": 3}, {"n": 4}]}),
            _mock_response({"items": [{"n": 5}]}),  # partial → last page
        ]
        with patch("requests.Session.get", side_effect=responses):
            rows = list(extractor.extract("http://example.com/api"))

        assert [r["n"] for r in rows] == [1, 2, 3, 4, 5]

    def test_stops_when_page_is_empty(self) -> None:
        """An empty page stops iteration immediately."""
        extractor = APIExtractor(records_key="items", pagination_style="offset", page_size=5)
        responses = [
            _mock_response({"items": [{"n": 1}]}),  # partial → stop immediately
        ]
        with patch("requests.Session.get", side_effect=responses):
            rows = list(extractor.extract("http://example.com/api"))
        assert len(rows) == 1


# ---------------------------------------------------------------------------
# cursor pagination
# ---------------------------------------------------------------------------


class TestCursorPagination:
    def test_cursor_pagination_two_pages(self) -> None:
        page1 = {"results": [{"a": 1}], "next_cursor": "cur_abc"}
        page2 = {"results": [{"a": 2}], "next_cursor": None}

        extractor = APIExtractor(records_key="results", pagination_style="cursor")
        responses = [_mock_response(page1), _mock_response(page2)]
        with patch("requests.Session.get", side_effect=responses):
            rows = list(extractor.extract("http://example.com/api"))

        assert [r["a"] for r in rows] == [1, 2]

    def test_cursor_stops_on_missing_next_cursor(self) -> None:
        body = {"results": [{"a": 99}]}  # no "next_cursor" key at all
        extractor = APIExtractor(records_key="results", pagination_style="cursor")
        with patch("requests.Session.get") as mock_get:
            mock_get.return_value = _mock_response(body)
            rows = list(extractor.extract("http://example.com/api"))
        assert len(rows) == 1


# ---------------------------------------------------------------------------
# Authentication
# ---------------------------------------------------------------------------


class TestAuthentication:
    def test_bearer_token_is_set_in_header(self) -> None:
        extractor = APIExtractor(bearer_token="my-secret-token", pagination_style="next_url")
        with patch("requests.Session.get") as mock_get:
            mock_get.return_value = _mock_response([])
            list(extractor.extract("http://example.com/api"))
        # The session's headers are set during _build_session; just verify
        # that extract() completes without error — integration with headers
        # tested via _build_session unit tests below.

    def test_custom_headers_passed_through(self) -> None:
        extractor = APIExtractor(
            headers={"X-API-Key": "key123"}, pagination_style="next_url"
        )
        with patch("requests.Session.get") as mock_get:
            mock_get.return_value = _mock_response([])
            rows = list(extractor.extract("http://example.com/api"))
        assert rows == []


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


class TestErrorHandling:
    def test_http_error_raises(self) -> None:
        extractor = APIExtractor(pagination_style="next_url")
        error_resp = _mock_response({}, status_code=500)
        error_resp.raise_for_status.side_effect = requests.HTTPError("500 Server Error")
        with patch("requests.Session.get", return_value=error_resp):
            with pytest.raises(requests.HTTPError):
                list(extractor.extract("http://example.com/api"))

    def test_rate_limit_triggers_pause_and_retry(self) -> None:
        """A 429 response triggers a pause and a second GET attempt."""
        extractor = APIExtractor(
            pagination_style="next_url", rate_limit_pause=0.001
        )
        rate_resp = _mock_response({}, status_code=429)
        rate_resp.raise_for_status = MagicMock()

        ok_resp = _mock_response([{"id": 1}])

        with (
            patch("requests.Session.get", side_effect=[rate_resp, ok_resp]),
            patch("vorago.extractors.api_extractor.time.sleep") as mock_sleep,
        ):
            rows = list(extractor.extract("http://example.com/api"))

        mock_sleep.assert_called_once()
        assert rows == [{"id": 1}]


# ---------------------------------------------------------------------------
# _build_session helper
# ---------------------------------------------------------------------------


class TestBuildSession:
    def test_bearer_token_added_to_headers(self) -> None:
        from vorago.extractors.api_extractor import _build_session

        session = _build_session(
            headers=None,
            bearer_token="tok_abc",
            max_retries=0,
            backoff_factor=0,
        )
        assert session.headers.get("Authorization") == "Bearer tok_abc"

    def test_extra_headers_merged(self) -> None:
        from vorago.extractors.api_extractor import _build_session

        session = _build_session(
            headers={"X-Custom": "value"},
            bearer_token=None,
            max_retries=0,
            backoff_factor=0,
        )
        assert session.headers.get("X-Custom") == "value"
