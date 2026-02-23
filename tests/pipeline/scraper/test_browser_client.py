"""Tests for the curl_cffi browser client."""

import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from backend.pipeline.scraper.browser_client import (
    BrowserFetchError,
    CurlCffiClient,
)


def _make_mock_response(*, status_code: int = 200, text: str = "<html>Hello</html>"):
    """Build a mock curl_cffi Response."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.text = text
    return resp


class TestCurlCffiClient:
    async def test_fetch_success(self):
        with patch("backend.pipeline.scraper.browser_client.AsyncSession") as mock_cls:
            mock_session = AsyncMock()
            mock_session.get = AsyncMock(
                return_value=_make_mock_response(text="<html>OK</html>")
            )
            mock_session.close = AsyncMock()
            mock_cls.return_value = mock_session

            async with CurlCffiClient(delay=0) as client:
                result = await client.fetch("https://example.com/page")
            assert result == "<html>OK</html>"

    async def test_context_manager_required(self):
        client = CurlCffiClient(delay=0)
        with pytest.raises(RuntimeError, match="not initialized"):
            await client.fetch("https://example.com")

    async def test_4xx_raises_immediately(self):
        with patch("backend.pipeline.scraper.browser_client.AsyncSession") as mock_cls:
            mock_session = AsyncMock()
            mock_session.get = AsyncMock(
                return_value=_make_mock_response(status_code=403)
            )
            mock_session.close = AsyncMock()
            mock_cls.return_value = mock_session

            async with CurlCffiClient(delay=0) as client:
                with pytest.raises(BrowserFetchError) as exc_info:
                    await client.fetch("https://example.com/blocked")
            assert exc_info.value.status == 403
            # 4xx (non-429) must NOT retry
            assert mock_session.get.call_count == 1

    async def test_429_retries_then_succeeds(self):
        with patch("backend.pipeline.scraper.browser_client.AsyncSession") as mock_cls:
            mock_session = AsyncMock()
            mock_session.get = AsyncMock(
                side_effect=[
                    _make_mock_response(status_code=429),
                    _make_mock_response(status_code=429),
                    _make_mock_response(status_code=200, text="<html>OK</html>"),
                ]
            )
            mock_session.close = AsyncMock()
            mock_cls.return_value = mock_session

            async with CurlCffiClient(delay=0) as client:
                result = await client.fetch("https://example.com/rate-limited")
            assert result == "<html>OK</html>"
            assert mock_session.get.call_count == 3

    async def test_5xx_retries_then_succeeds(self):
        with patch("backend.pipeline.scraper.browser_client.AsyncSession") as mock_cls:
            mock_session = AsyncMock()
            mock_session.get = AsyncMock(
                side_effect=[
                    _make_mock_response(status_code=502),
                    _make_mock_response(status_code=200, text="<html>OK</html>"),
                ]
            )
            mock_session.close = AsyncMock()
            mock_cls.return_value = mock_session

            async with CurlCffiClient(delay=0) as client:
                result = await client.fetch("https://example.com/flaky")
            assert result == "<html>OK</html>"
            assert mock_session.get.call_count == 2

    async def test_rate_limiting(self):
        with patch("backend.pipeline.scraper.browser_client.AsyncSession") as mock_cls:
            mock_session = AsyncMock()
            mock_session.get = AsyncMock(
                return_value=_make_mock_response()
            )
            mock_session.close = AsyncMock()
            mock_cls.return_value = mock_session

            async with CurlCffiClient(delay=0.3) as client:
                start = time.monotonic()
                await client.fetch("https://example.com/a")
                await client.fetch("https://example.com/b")
                elapsed = time.monotonic() - start
            # Second request should wait at least delay * 0.5 = 0.15s
            assert elapsed >= 0.10

    async def test_session_closed_on_exit(self):
        with patch("backend.pipeline.scraper.browser_client.AsyncSession") as mock_cls:
            mock_session = AsyncMock()
            mock_session.close = AsyncMock()
            mock_cls.return_value = mock_session

            async with CurlCffiClient(delay=0) as _client:
                pass
            mock_session.close.assert_called_once()

    async def test_proxy_passed_to_session(self):
        with patch("backend.pipeline.scraper.browser_client.AsyncSession") as mock_cls:
            mock_session = AsyncMock()
            mock_session.close = AsyncMock()
            mock_cls.return_value = mock_session

            async with CurlCffiClient(delay=0, proxy="http://proxy:8080") as _client:
                pass
            mock_cls.assert_called_once_with(
                impersonate="chrome",
                timeout=30,
                proxy="http://proxy:8080",
            )

    async def test_impersonate_chrome(self):
        with patch("backend.pipeline.scraper.browser_client.AsyncSession") as mock_cls:
            mock_session = AsyncMock()
            mock_session.close = AsyncMock()
            mock_cls.return_value = mock_session

            async with CurlCffiClient(delay=0) as _client:
                pass
            call_kwargs = mock_cls.call_args[1]
            assert call_kwargs["impersonate"] == "chrome"


class TestBrowserFetchError:
    def test_attributes(self):
        err = BrowserFetchError(403, "https://example.com")
        assert err.status == 403
        assert err.url == "https://example.com"
        assert "403" in str(err)
