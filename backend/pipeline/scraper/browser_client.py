"""HTTP client using curl_cffi for fetching pages behind Cloudflare protection.

The Official Gazette uses Cloudflare bot detection that blocks standard HTTP
clients (httpx, requests) via TLS fingerprinting. curl_cffi impersonates a real
browser's TLS fingerprint (JA3/JA4) so Cloudflare treats us like a real browser.
"""

import asyncio
import logging
import random

from curl_cffi.requests import AsyncSession
from tenacity import (
    retry,
    retry_if_exception,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)

DEFAULT_DELAY = 5.0
DEFAULT_TIMEOUT = 30


class BrowserFetchError(Exception):
    """HTTP-level error from a curl_cffi response."""

    def __init__(self, status: int, url: str) -> None:
        self.status = status
        self.url = url
        super().__init__(f"HTTP {status} for {url}")


def _is_retryable_status(status: int) -> bool:
    return status == 429 or status >= 500


class CurlCffiClient:
    """Async HTTP client that impersonates a browser's TLS fingerprint.

    Uses curl_cffi's `impersonate` feature to send requests with a real
    Chrome TLS fingerprint, bypassing Cloudflare's bot detection without
    needing an actual browser.

    Usage::

        async with CurlCffiClient(delay=3.0) as client:
            html = await client.fetch("https://example.com")
    """

    def __init__(
        self,
        delay: float = DEFAULT_DELAY,
        timeout: int = DEFAULT_TIMEOUT,
        proxy: str | None = None,
    ) -> None:
        self.delay = delay
        self.timeout = timeout
        self._proxy = proxy
        self._session: AsyncSession | None = None
        self._last_request_time: float = 0.0

    async def __aenter__(self) -> "CurlCffiClient":
        kwargs: dict = {
            "impersonate": "chrome",
            "timeout": self.timeout,
        }
        if self._proxy:
            kwargs["proxy"] = self._proxy
        self._session = AsyncSession(**kwargs)
        return self

    async def __aexit__(self, *exc: object) -> None:
        if self._session:
            await self._session.close()
            self._session = None

    async def _rate_limit(self) -> None:
        """Enforce minimum delay between requests with random jitter."""
        jittered_delay = self.delay * random.uniform(0.5, 1.5)
        now = asyncio.get_event_loop().time()
        elapsed = now - self._last_request_time
        if elapsed < jittered_delay:
            await asyncio.sleep(jittered_delay - elapsed)
        self._last_request_time = asyncio.get_event_loop().time()

    async def fetch(self, url: str) -> str:
        """Fetch a URL and return its HTML content.

        Rate-limits requests and retries on transient errors (429, 5xx).
        """
        if self._session is None:
            raise RuntimeError(
                "Client not initialized. Use 'async with CurlCffiClient()' context."
            )
        await self._rate_limit()
        return await self._fetch_with_retry(url)

    # only retry on 429/5xx and network errors, not on 403/404 etc.
    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2, min=4, max=60),
        retry=(
            retry_if_exception(
                lambda e: isinstance(e, BrowserFetchError)
                and _is_retryable_status(e.status)
            )
            | retry_if_exception_type(ConnectionError)
        ),
        reraise=True,
    )
    async def _fetch_with_retry(self, url: str) -> str:
        """Inner fetch with tenacity retry on transient errors."""
        assert self._session is not None  # guaranteed by fetch()

        response = await self._session.get(url)
        status = response.status_code

        if _is_retryable_status(status):
            logger.warning("HTTP %d for %s, will retry", status, url)
            raise BrowserFetchError(status, url)

        if status >= 400:
            raise BrowserFetchError(status, url)

        return response.text
