"""Tests for the Playwright browser client."""

import time
from unittest.mock import AsyncMock, MagicMock

import pytest

from backend.pipeline.scraper.browser_client import (
    _BLOCKED_RESOURCE_TYPES,
    BrowserFetchError,
    PlaywrightClient,
)
from backend.pipeline.scraper.masterlist_scraper import _proxy_url_to_playwright


def _make_mock_browser(
    *,
    status: int = 200,
    content: str = "<html>Hello</html>",
    response_sequence: list[int] | None = None,
) -> tuple[MagicMock, MagicMock, MagicMock]:
    """Build mock Browser → BrowserContext → Page chain.

    If *response_sequence* is provided, successive goto() calls return those
    status codes in order (the last one sticks for any further calls).
    """
    mock_response = AsyncMock()
    mock_response.status = status

    mock_page = AsyncMock()
    mock_page.content = AsyncMock(return_value=content)
    mock_page.route = AsyncMock()

    if response_sequence is not None:
        call_idx = 0

        async def _goto_side_effect(*_args, **_kwargs):
            nonlocal call_idx
            idx = min(call_idx, len(response_sequence) - 1)
            call_idx += 1
            resp = AsyncMock()
            resp.status = response_sequence[idx]
            return resp

        mock_page.goto = AsyncMock(side_effect=_goto_side_effect)
    else:
        mock_page.goto = AsyncMock(return_value=mock_response)

    mock_context = AsyncMock()
    mock_context.new_page = AsyncMock(return_value=mock_page)

    mock_browser = AsyncMock()
    mock_browser.new_context = AsyncMock(return_value=mock_context)

    return mock_browser, mock_context, mock_page


class TestPlaywrightClient:
    async def test_fetch_success(self):
        browser, _ctx, _page = _make_mock_browser(content="<html>OK</html>")
        async with PlaywrightClient(browser, delay=0) as client:
            result = await client.fetch("https://example.com/page")
        assert result == "<html>OK</html>"

    async def test_context_manager_required(self):
        browser, _ctx, _page = _make_mock_browser()
        client = PlaywrightClient(browser, delay=0)
        with pytest.raises(RuntimeError, match="not initialized"):
            await client.fetch("https://example.com")

    async def test_4xx_raises_immediately(self):
        browser, _ctx, page = _make_mock_browser(status=403)
        async with PlaywrightClient(browser, delay=0) as client:
            with pytest.raises(BrowserFetchError) as exc_info:
                await client.fetch("https://example.com/blocked")
        assert exc_info.value.status == 403
        # 4xx (non-429) must NOT retry
        assert page.goto.call_count == 1

    async def test_429_retries_then_succeeds(self):
        # 429, 429, then 200
        browser, _ctx, page = _make_mock_browser(response_sequence=[429, 429, 200])
        async with PlaywrightClient(browser, delay=0) as client:
            result = await client.fetch("https://example.com/rate-limited")
        assert result == "<html>Hello</html>"
        assert page.goto.call_count == 3

    async def test_5xx_retries_then_succeeds(self):
        browser, _ctx, page = _make_mock_browser(response_sequence=[502, 200])
        async with PlaywrightClient(browser, delay=0) as client:
            result = await client.fetch("https://example.com/flaky")
        assert result == "<html>Hello</html>"
        assert page.goto.call_count == 2

    async def test_rate_limiting(self):
        browser, _ctx, _page = _make_mock_browser()
        async with PlaywrightClient(browser, delay=0.3) as client:
            start = time.monotonic()
            await client.fetch("https://example.com/a")
            await client.fetch("https://example.com/b")
            elapsed = time.monotonic() - start
        # Second request should wait at least delay * 0.5 = 0.15s
        assert elapsed >= 0.10

    async def test_resource_blocking_route_installed(self):
        browser, _ctx, page = _make_mock_browser()
        async with PlaywrightClient(browser, delay=0) as _client:
            pass  # just need __aenter__
        # route() should have been called with "**/*" pattern
        page.route.assert_called_once()
        args = page.route.call_args
        assert args[0][0] == "**/*"

    async def test_resource_blocking_callback(self):
        """Verify the route callback aborts blocked types and falls back for others."""
        browser, _ctx, page = _make_mock_browser()
        async with PlaywrightClient(browser, delay=0) as _client:
            pass
        # Extract the callback registered with page.route
        route_callback = page.route.call_args[0][1]

        # Blocked resource type → abort
        for rtype in _BLOCKED_RESOURCE_TYPES:
            mock_route = AsyncMock()
            mock_route.request.resource_type = rtype
            await route_callback(mock_route)
            mock_route.abort.assert_called_once()
            mock_route.fallback.assert_not_called()

        # Allowed resource type → fallback
        mock_route = AsyncMock()
        mock_route.request.resource_type = "document"
        await route_callback(mock_route)
        mock_route.fallback.assert_called_once()
        mock_route.abort.assert_not_called()

    async def test_context_closed_on_exit(self):
        browser, ctx, _page = _make_mock_browser()
        async with PlaywrightClient(browser, delay=0) as _client:
            pass
        ctx.close.assert_called_once()

    async def test_proxy_passed_to_browser_context(self):
        browser, _ctx, _page = _make_mock_browser()
        proxy = {"server": "http://proxy.example.com:8080"}
        async with PlaywrightClient(browser, delay=0, proxy=proxy) as _client:
            pass
        browser.new_context.assert_called_once_with(proxy=proxy)


class TestBrowserFetchError:
    def test_attributes(self):
        err = BrowserFetchError(403, "https://example.com")
        assert err.status == 403
        assert err.url == "https://example.com"
        assert "403" in str(err)


class TestProxyUrlToPlaywright:
    def test_simple_proxy(self):
        result = _proxy_url_to_playwright("http://proxy.example.com:8080")
        assert result == {"server": "http://proxy.example.com:8080"}

    def test_proxy_with_auth(self):
        result = _proxy_url_to_playwright("http://user:pass@proxy.example.com:8080")
        assert result == {
            "server": "http://proxy.example.com:8080",
            "username": "user",
            "password": "pass",
        }

    def test_https_proxy(self):
        result = _proxy_url_to_playwright("https://secure.proxy.io:443")
        assert result == {"server": "https://secure.proxy.io:443"}
