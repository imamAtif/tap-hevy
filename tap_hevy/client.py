"""REST client handling, including HevyStream base class."""

from __future__ import annotations

import datetime
import email.utils
import sys
import typing as t
from http import HTTPStatus

import requests
from singer_sdk.authenticators import APIKeyAuthenticator
from singer_sdk.pagination import BaseAPIPaginator
from singer_sdk.streams import RESTStream

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Context


DEFAULT_API_URL = "https://api.hevyapp.com"
DEFAULT_TIMEOUT = 30
DEFAULT_PAGE_SIZE = 10


class HevyPageNumberPaginator(BaseAPIPaginator[int]):
    """Paginator for Hevy endpoints using page/page_count."""

    def __init__(self, start_value: int = 1, *args: t.Any, **kwargs: t.Any) -> None:
        super().__init__(start_value, *args, **kwargs)

    def _get_data(self, response: requests.Response) -> dict | None:
        # Cache parsed json on response to avoid double parse per page
        cached = getattr(response, "_hevy_data", None)
        if cached is not None:
            return cached  # type: ignore[no-any-return]
        try:
            data = response.json()
        except Exception:
            return None
        try:
            response._hevy_data = data  # type: ignore[attr-defined]
        except Exception:
            pass
        return data  # type: ignore[no-any-return]

    @override
    def has_more(self, response: requests.Response) -> bool:
        data = self._get_data(response)
        if data is None:
            return False

        page = data.get("page")
        page_count = data.get("page_count")
        if page is None or page_count is None:
            # No pagination metadata -> assume single page
            return False
        try:
            return int(page) < int(page_count)
        except (ValueError, TypeError):
            return False

    @override
    def get_next(self, response: requests.Response) -> int | None:
        # Value is current page number; next is +1 if has_more
        data = self._get_data(response)
        if data is None:
            return None
        try:
            page = data.get("page")
            page_count = data.get("page_count")
            if page is None or page_count is None:
                return None
            if int(page) >= int(page_count):
                return None
        except Exception:
            return None
        return int(self.current_value) + 1


class HevyStream(RESTStream[int]):
    """Hevy stream class."""

    records_jsonpath = "$[*]"
    next_page_token_jsonpath = None

    # Hevy uses api-key header
    @property
    @override
    def url_base(self) -> str:
        return str(self.config.get("api_url", DEFAULT_API_URL)).rstrip("/")

    @property
    @override
    def authenticator(self) -> APIKeyAuthenticator:
        return APIKeyAuthenticator(
            stream=self,
            key="api-key",
            value=self.config["api_key"],
            location="header",
        )

    @property
    @override
    def http_headers(self) -> dict:
        return {
            "Accept": "application/json",
        }

    @override
    def get_new_paginator(self) -> BaseAPIPaginator | None:
        # Default to page-number paginator; single-page streams override.
        return HevyPageNumberPaginator(start_value=1)

    @property
    def timeout(self) -> int:
        try:
            return int(self.config.get("request_timeout", DEFAULT_TIMEOUT))
        except (TypeError, ValueError):
            return DEFAULT_TIMEOUT

    @override
    def backoff_max_tries(self) -> int:
        # Allow config override
        try:
            value = int(self.config.get("max_retries", 5))
        except (TypeError, ValueError):
            return 5
        # Clamp to sensible range
        if value < 0:
            return 0
        if value > 20:
            return 20
        return value

    @override
    def backoff_wait_generator(self) -> t.Generator[float, None, None]:
        """Handle Retry-After header if present, else exponential.

        Respects Retry-After (seconds or HTTP-date) when a 429 response
        includes it, falling back to exponential backoff otherwise.
        Also respects X-RateLimit-Reset if present.
        """

        def _parse_retry_after(value: str) -> float | None:
            if value is None:  # type: ignore[unreachable]
                return None
            value = value.strip()
            try:
                return float(int(value))
            except ValueError:
                pass
            try:
                return float(value)
            except ValueError:
                pass
            try:
                dt = email.utils.parsedate_to_datetime(value)
                if dt is not None:
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=datetime.timezone.utc)
                    now = datetime.datetime.now(datetime.timezone.utc)
                    diff = (dt - now).total_seconds()
                    return max(0.0, float(diff))
            except Exception:
                pass
            return None

        def _get_wait_from_response(
            response: requests.Response | None,
        ) -> float | None:
            if response is None:
                return None
            headers = getattr(response, "headers", {}) or {}
            retry_after = headers.get("Retry-After") or headers.get("retry-after")
            if retry_after is not None:
                parsed = _parse_retry_after(str(retry_after))
                if parsed is not None:
                    return parsed
            reset = headers.get("X-RateLimit-Reset") or headers.get("x-ratelimit-reset")
            if reset is not None:
                parsed = _parse_retry_after(str(reset))
                if parsed is not None:
                    return parsed
            return None

        # Use runtime generator that receives exception via send()
        # and returns wait based on Retry-After or expo fallback.
        # We need a counter for expo fallback.
        tries = {"count": 0}

        def _get_wait(exception: t.Any) -> float:
            # Check header first
            response = getattr(exception, "response", None) if exception else None
            header_wait = _get_wait_from_response(response)
            if header_wait is not None:
                return float(header_wait)
            # Fallback expo: factor 2, base 2
            tries["count"] += 1
            # backoff.expo with factor 2: wait = 2 * (2 ** (tries-1)) roughly
            # Use same as backoff.expo(factor=2) first values: 1,2,4,8,16 with jitter later
            # Actually backoff.expo yields 0.5*2,1*2,...approx 2*(2**(tries-1))
            # To avoid needing generator state, compute directly
            return float(2 * (2 ** (tries["count"] - 1)))

        return self.backoff_runtime(value=_get_wait)  # type: ignore[return-value]

    # Ensure extra_retry_statuses includes 429 (default) and we retry 5xx automatically
    # No need to override validate_response unless we want custom.
    @override
    def validate_response(self, response: requests.Response) -> None:
        # Use parent validation but ensure we don't retry 4xx except 429
        if (
            response.status_code == HTTPStatus.TOO_MANY_REQUESTS
            or response.status_code >= HTTPStatus.INTERNAL_SERVER_ERROR
        ):
            msg = self.response_error_message(response)
            from singer_sdk.exceptions import RetriableAPIError

            raise RetriableAPIError(msg, response)

        if HTTPStatus.BAD_REQUEST <= response.status_code < HTTPStatus.INTERNAL_SERVER_ERROR:
            from singer_sdk.exceptions import FatalAPIError

            msg = self.response_error_message(response)
            raise FatalAPIError(msg)

    def get_page_size(self, max_size: int = 10) -> int:
        """Return configured page size clamped to API hard limit."""
        try:
            configured = int(self.config.get("page_size", DEFAULT_PAGE_SIZE))
        except (TypeError, ValueError):
            configured = DEFAULT_PAGE_SIZE
        # Clamp to valid range 1..max_size
        if configured < 1:
            return 1
        if configured > max_size:
            return max_size
        return configured

    def get_url_params(
        self,
        context: Context | None,
        next_page_token: int | None,
    ) -> dict[str, t.Any]:
        params: dict[str, t.Any] = {}
        if next_page_token is not None:
            params["page"] = next_page_token
        else:
            params["page"] = 1
        # Default pageSize; subclasses may override max
        params["pageSize"] = self.get_page_size(max_size=10)
        return params
