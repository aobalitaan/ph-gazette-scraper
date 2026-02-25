"""Tests for the curl_cffi browser client."""

import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from backend.pipeline.scraper.browser_client import (
    _429_WAIT_CYCLE,
    BrowserFetchError,
    CurlCffiClient,
)


def _make_mock_response(
    *,
    status_code: int = 200,
    text: str = "<html>Hello</html>",
    content: bytes = b"<html>Hello</html>",
    headers: dict | None = None,
):
    """Build a mock curl_cffi Response."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.text = text
    resp.content = content
    resp.headers = headers or {}
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

    async def test_429_uses_wait_cycle(self):
        """429 responses cycle through _429_WAIT_CYCLE sleeps until success."""
        with patch("backend.pipeline.scraper.browser_client.AsyncSession") as mock_cls:
            mock_session = AsyncMock()
            mock_session.get = AsyncMock(
                side_effect=[
                    _make_mock_response(status_code=429),
                    _make_mock_response(status_code=429),
                    _make_mock_response(status_code=429),
                    _make_mock_response(status_code=200, text="<html>OK</html>"),
                ]
            )
            mock_session.close = AsyncMock()
            mock_cls.return_value = mock_session

            sleep_durations: list[float] = []

            async def fake_sleep(duration: float) -> None:
                sleep_durations.append(duration)

            with patch("backend.pipeline.scraper.browser_client.asyncio.sleep", fake_sleep):
                async with CurlCffiClient(delay=0) as client:
                    result = await client.fetch("https://example.com/rate-limited")

            assert result == "<html>OK</html>"
            assert mock_session.get.call_count == 4
            assert sleep_durations == list(_429_WAIT_CYCLE[:3])

    async def test_429_cycle_wraps_around(self):
        """After exhausting the cycle, 429 retries wrap back to the start."""
        num_429s = len(_429_WAIT_CYCLE) + 2
        responses = [_make_mock_response(status_code=429) for _ in range(num_429s)]
        responses.append(_make_mock_response(status_code=200, text="<html>OK</html>"))

        with patch("backend.pipeline.scraper.browser_client.AsyncSession") as mock_cls:
            mock_session = AsyncMock()
            mock_session.get = AsyncMock(side_effect=responses)
            mock_session.close = AsyncMock()
            mock_cls.return_value = mock_session

            sleep_durations: list[float] = []

            async def fake_sleep(duration: float) -> None:
                sleep_durations.append(duration)

            with patch("backend.pipeline.scraper.browser_client.asyncio.sleep", fake_sleep):
                async with CurlCffiClient(delay=0) as client:
                    result = await client.fetch("https://example.com/rate-limited")

            assert result == "<html>OK</html>"
            expected = list(_429_WAIT_CYCLE) + list(_429_WAIT_CYCLE[:2])
            assert sleep_durations == expected

    async def test_429_respects_retry_after_header(self):
        """429 with Retry-After header uses header value instead of cycle."""
        with patch("backend.pipeline.scraper.browser_client.AsyncSession") as mock_cls:
            mock_session = AsyncMock()
            mock_session.get = AsyncMock(
                side_effect=[
                    _make_mock_response(
                        status_code=429, headers={"retry-after": "90"}
                    ),
                    _make_mock_response(
                        status_code=429  # no header → fallback to cycle
                    ),
                    _make_mock_response(status_code=200, text="<html>OK</html>"),
                ]
            )
            mock_session.close = AsyncMock()
            mock_cls.return_value = mock_session

            sleep_durations: list[float] = []

            async def fake_sleep(duration: float) -> None:
                sleep_durations.append(duration)

            with patch("backend.pipeline.scraper.browser_client.asyncio.sleep", fake_sleep):
                async with CurlCffiClient(delay=0) as client:
                    result = await client.fetch("https://example.com/rate-limited")

            assert result == "<html>OK</html>"
            assert sleep_durations == [90, _429_WAIT_CYCLE[1]]

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


class TestFetchBytes:
    async def test_fetch_bytes_success(self):
        pdf_bytes = b"%PDF-1.4 binary content"
        with patch("backend.pipeline.scraper.browser_client.AsyncSession") as mock_cls:
            mock_session = AsyncMock()
            mock_session.get = AsyncMock(
                return_value=_make_mock_response(content=pdf_bytes)
            )
            mock_session.close = AsyncMock()
            mock_cls.return_value = mock_session

            async with CurlCffiClient(delay=0) as client:
                result = await client.fetch_bytes("https://example.com/doc.pdf")
        assert result == pdf_bytes

    async def test_fetch_bytes_context_required(self):
        client = CurlCffiClient(delay=0)
        with pytest.raises(RuntimeError, match="not initialized"):
            await client.fetch_bytes("https://example.com/doc.pdf")

    async def test_fetch_bytes_4xx_raises(self):
        with patch("backend.pipeline.scraper.browser_client.AsyncSession") as mock_cls:
            mock_session = AsyncMock()
            mock_session.get = AsyncMock(
                return_value=_make_mock_response(status_code=404)
            )
            mock_session.close = AsyncMock()
            mock_cls.return_value = mock_session

            async with CurlCffiClient(delay=0) as client:
                with pytest.raises(BrowserFetchError) as exc_info:
                    await client.fetch_bytes("https://example.com/missing.pdf")
            assert exc_info.value.status == 404

    async def test_fetch_bytes_429_retries(self):
        pdf_bytes = b"%PDF-1.4 content"
        with patch("backend.pipeline.scraper.browser_client.AsyncSession") as mock_cls:
            mock_session = AsyncMock()
            mock_session.get = AsyncMock(
                side_effect=[
                    _make_mock_response(status_code=429),
                    _make_mock_response(content=pdf_bytes),
                ]
            )
            mock_session.close = AsyncMock()
            mock_cls.return_value = mock_session

            async def fake_sleep(duration: float) -> None:
                pass

            with patch("backend.pipeline.scraper.browser_client.asyncio.sleep", fake_sleep):
                async with CurlCffiClient(delay=0) as client:
                    result = await client.fetch_bytes("https://example.com/doc.pdf")

            assert result == pdf_bytes
            assert mock_session.get.call_count == 2

    async def test_fetch_bytes_5xx_retries(self):
        pdf_bytes = b"%PDF-1.4 content"
        with patch("backend.pipeline.scraper.browser_client.AsyncSession") as mock_cls:
            mock_session = AsyncMock()
            mock_session.get = AsyncMock(
                side_effect=[
                    _make_mock_response(status_code=503),
                    _make_mock_response(content=pdf_bytes),
                ]
            )
            mock_session.close = AsyncMock()
            mock_cls.return_value = mock_session

            async with CurlCffiClient(delay=0) as client:
                result = await client.fetch_bytes("https://example.com/doc.pdf")

        assert result == pdf_bytes
        assert mock_session.get.call_count == 2


class TestBrowserFetchError:
    def test_attributes(self):
        err = BrowserFetchError(403, "https://example.com")
        assert err.status == 403
        assert err.url == "https://example.com"
        assert "403" in str(err)
