"""Playwright-based browser client for fetching pages behind Cloudflare protection.

The Official Gazette uses Cloudflare bot detection that blocks non-browser HTTP
clients via TLS fingerprinting.  PlaywrightClient launches a real Chromium browser
whose TLS fingerprint passes Cloudflare checks automatically.
"""

import asyncio
import logging
import random

from playwright.async_api import Browser, BrowserContext, Page
from playwright.async_api import Error as PlaywrightError
from tenacity import (
    retry,
    retry_if_exception,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)

DEFAULT_DELAY = 5.0
DEFAULT_TIMEOUT = 30_000  # Playwright uses milliseconds

# Resource types to block — we only need the HTML document.
_BLOCKED_RESOURCE_TYPES = {"image", "stylesheet", "font", "media"}


class BrowserFetchError(Exception):
    """HTTP-level error from a Playwright navigation response."""

    def __init__(self, status: int, url: str) -> None:
        self.status = status
        self.url = url
        super().__init__(f"HTTP {status} for {url}")


def _is_retryable_status(status: int) -> bool:
    return status == 429 or status >= 500


class PlaywrightClient:
    """Async browser client with rate limiting, retry, and resource blocking.

    Wraps a shared Playwright Browser instance.  Each client creates its own
    BrowserContext (isolated cookies/session) with a single Page that is reused
    across navigations so Cloudflare challenge cookies persist.

    Usage::

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            async with PlaywrightClient(browser, delay=1.5) as client:
                html = await client.fetch("https://example.com")
            await browser.close()
    """

    def __init__(
        self,
        browser: Browser,
        delay: float = DEFAULT_DELAY,
        timeout: int = DEFAULT_TIMEOUT,
        proxy: dict | None = None,
    ) -> None:
        self._browser = browser
        self.delay = delay
        self.timeout = timeout
        self._proxy = proxy
        self._context: BrowserContext | None = None
        self._page: Page | None = None
        self._last_request_time: float = 0.0

    async def __aenter__(self) -> "PlaywrightClient":
        kwargs: dict = {}
        if self._proxy:
            kwargs["proxy"] = self._proxy
        # each client gets its own context so cookies/sessions are isolated per worker
        self._context = await self._browser.new_context(**kwargs)
        self._page = await self._context.new_page()
        # intercept all requests and drop images/css/fonts — saves bandwidth
        await self._page.route(
            "**/*",
            lambda route: (
                route.abort()
                if route.request.resource_type in _BLOCKED_RESOURCE_TYPES
                else route.fallback()
            ),
        )
        return self

    async def __aexit__(self, *exc: object) -> None:
        if self._context:
            await self._context.close()
            self._context = None
            self._page = None

    async def _rate_limit(self) -> None:
        """Enforce minimum delay between requests with random jitter."""
        jittered_delay = self.delay * random.uniform(0.5, 1.5)
        now = asyncio.get_event_loop().time()
        elapsed = now - self._last_request_time
        if elapsed < jittered_delay:
            await asyncio.sleep(jittered_delay - elapsed)
        self._last_request_time = asyncio.get_event_loop().time()

    async def fetch(self, url: str) -> str:
        """Navigate to *url* and return the page HTML.

        Rate-limits requests and retries on transient errors (429, 5xx)
        and network failures.
        """
        if self._page is None:
            raise RuntimeError(
                "Client not initialized. Use 'async with PlaywrightClient()' context."
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
            | retry_if_exception_type(PlaywrightError)
        ),
        reraise=True,
    )
    async def _fetch_with_retry(self, url: str) -> str:
        """Inner fetch with tenacity retry on transient errors."""
        assert self._page is not None  # guaranteed by fetch()

        response = await self._page.goto(
            url, timeout=self.timeout, wait_until="domcontentloaded",
        )

        if response is None:
            raise BrowserFetchError(0, url)

        status = response.status
        if _is_retryable_status(status):
            logger.warning("HTTP %d for %s, will retry", status, url)
            raise BrowserFetchError(status, url)

        if status >= 400:
            raise BrowserFetchError(status, url)

        return await self._page.content()
