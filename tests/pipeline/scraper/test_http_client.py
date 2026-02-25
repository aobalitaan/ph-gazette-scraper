"""Tests for the Gazette HTTP client."""

import time
from unittest.mock import patch

import httpx
import pytest
import respx

from backend.pipeline.scraper.http_client import (
    _429_WAIT_CYCLE,
    _BROWSER_PROFILES,
    GazetteClient,
)


class TestGazetteClient:
    async def test_fetch_success(self):
        with respx.mock:
            respx.get("https://example.com/page").mock(
                return_value=httpx.Response(200, text="<html>Hello</html>")
            )
            async with GazetteClient(delay=0) as client:
                result = await client.fetch("https://example.com/page")
            assert result == "<html>Hello</html>"

    async def test_user_agent_header(self):
        with respx.mock:
            route = respx.get("https://example.com/page").mock(
                return_value=httpx.Response(200, text="ok")
            )
            async with GazetteClient(delay=0) as client:
                await client.fetch("https://example.com/page")
            ua = route.calls[0].request.headers["user-agent"]
            valid_uas = [p["User-Agent"] for p in _BROWSER_PROFILES]
            assert ua in valid_uas

    async def test_follows_redirects(self):
        with respx.mock:
            respx.get("https://example.com/short").mock(
                return_value=httpx.Response(
                    301,
                    headers={"Location": "https://example.com/full-page"},
                )
            )
            respx.get("https://example.com/full-page").mock(
                return_value=httpx.Response(200, text="redirected content")
            )
            async with GazetteClient(delay=0) as client:
                result = await client.fetch("https://example.com/short")
            assert result == "redirected content"

    async def test_retry_on_500(self):
        call_count = 0

        def side_effect(request: httpx.Request) -> httpx.Response:
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                return httpx.Response(500, text="server error")
            return httpx.Response(200, text="ok")

        with respx.mock:
            respx.get("https://example.com/flaky").mock(side_effect=side_effect)
            async with GazetteClient(delay=0) as client:
                result = await client.fetch("https://example.com/flaky")
            assert result == "ok"
            assert call_count == 3

    async def test_raises_after_max_retries(self):
        with respx.mock:
            respx.get("https://example.com/down").mock(
                return_value=httpx.Response(500, text="down")
            )
            async with GazetteClient(delay=0) as client:
                with pytest.raises(httpx.HTTPStatusError):
                    await client.fetch("https://example.com/down")

    async def test_raises_on_4xx(self):
        with respx.mock:
            respx.get("https://example.com/missing").mock(
                return_value=httpx.Response(404, text="not found")
            )
            async with GazetteClient(delay=0) as client:
                with pytest.raises(httpx.HTTPStatusError):
                    await client.fetch("https://example.com/missing")

    async def test_rate_limiting(self):
        with respx.mock:
            respx.get("https://example.com/a").mock(
                return_value=httpx.Response(200, text="a")
            )
            respx.get("https://example.com/b").mock(
                return_value=httpx.Response(200, text="b")
            )
            async with GazetteClient(delay=0.3) as client:
                start = time.monotonic()
                await client.fetch("https://example.com/a")
                await client.fetch("https://example.com/b")
                elapsed = time.monotonic() - start
            # Second request should have waited at least 0.3 * 0.5 = 0.15s (jittered)
            assert elapsed >= 0.10

    async def test_context_manager_required(self):
        client = GazetteClient(delay=0)
        with pytest.raises(RuntimeError, match="not initialized"):
            await client.fetch("https://example.com")

    async def test_429_uses_wait_cycle(self):
        """429 responses cycle through _429_WAIT_CYCLE sleeps until success."""
        call_count = 0

        def side_effect(request: httpx.Request) -> httpx.Response:
            nonlocal call_count
            call_count += 1
            if call_count <= 3:
                return httpx.Response(429, text="rate limited")
            return httpx.Response(200, text="ok")

        sleep_durations: list[float] = []

        async def fake_sleep(duration: float) -> None:
            sleep_durations.append(duration)

        with respx.mock:
            respx.get("https://example.com/limited").mock(side_effect=side_effect)
            with patch("backend.pipeline.scraper.http_client.asyncio.sleep", fake_sleep):
                async with GazetteClient(delay=0) as client:
                    result = await client.fetch("https://example.com/limited")

        assert result == "ok"
        assert call_count == 4  # 3 x 429, then 200
        assert sleep_durations == list(_429_WAIT_CYCLE[:3])

    async def test_429_cycle_wraps_around(self):
        """After exhausting the cycle, 429 retries wrap back to the start."""
        num_429s = len(_429_WAIT_CYCLE) + 2  # full cycle + 2 more
        call_count = 0

        def side_effect(request: httpx.Request) -> httpx.Response:
            nonlocal call_count
            call_count += 1
            if call_count <= num_429s:
                return httpx.Response(429, text="rate limited")
            return httpx.Response(200, text="ok")

        sleep_durations: list[float] = []

        async def fake_sleep(duration: float) -> None:
            sleep_durations.append(duration)

        with respx.mock:
            respx.get("https://example.com/limited").mock(side_effect=side_effect)
            with patch("backend.pipeline.scraper.http_client.asyncio.sleep", fake_sleep):
                async with GazetteClient(delay=0) as client:
                    result = await client.fetch("https://example.com/limited")

        assert result == "ok"
        expected = list(_429_WAIT_CYCLE) + list(_429_WAIT_CYCLE[:2])
        assert sleep_durations == expected

    async def test_429_respects_retry_after_header(self):
        """429 with Retry-After header uses header value instead of cycle."""
        call_count = 0

        def side_effect(request: httpx.Request) -> httpx.Response:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return httpx.Response(429, text="rate limited", headers={"Retry-After": "90"})
            if call_count == 2:
                return httpx.Response(429, text="rate limited")  # no header → fallback
            return httpx.Response(200, text="ok")

        sleep_durations: list[float] = []

        async def fake_sleep(duration: float) -> None:
            sleep_durations.append(duration)

        with respx.mock:
            respx.get("https://example.com/limited").mock(side_effect=side_effect)
            with patch("backend.pipeline.scraper.http_client.asyncio.sleep", fake_sleep):
                async with GazetteClient(delay=0) as client:
                    result = await client.fetch("https://example.com/limited")

        assert result == "ok"
        assert sleep_durations == [90, _429_WAIT_CYCLE[1]]

    async def test_429_ignores_non_numeric_retry_after(self):
        """Non-numeric Retry-After falls back to wait cycle."""
        call_count = 0

        def side_effect(request: httpx.Request) -> httpx.Response:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return httpx.Response(
                    429, text="rate limited",
                    headers={"Retry-After": "Wed, 21 Oct 2025 07:28:00 GMT"},
                )
            return httpx.Response(200, text="ok")

        sleep_durations: list[float] = []

        async def fake_sleep(duration: float) -> None:
            sleep_durations.append(duration)

        with respx.mock:
            respx.get("https://example.com/limited").mock(side_effect=side_effect)
            with patch("backend.pipeline.scraper.http_client.asyncio.sleep", fake_sleep):
                async with GazetteClient(delay=0) as client:
                    result = await client.fetch("https://example.com/limited")

        assert result == "ok"
        assert sleep_durations == [_429_WAIT_CYCLE[0]]

    async def test_429_does_not_count_as_tenacity_attempt(self):
        """429 retries don't consume tenacity attempts; a 500 after 429 still retries."""
        call_count = 0

        def side_effect(request: httpx.Request) -> httpx.Response:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return httpx.Response(429, text="rate limited")
            if call_count == 2:
                return httpx.Response(200, text="ok after 429")
            return httpx.Response(200, text="ok")

        async def fake_sleep(duration: float) -> None:
            pass

        with respx.mock:
            respx.get("https://example.com/mixed").mock(side_effect=side_effect)
            with patch("backend.pipeline.scraper.http_client.asyncio.sleep", fake_sleep):
                async with GazetteClient(delay=0) as client:
                    result = await client.fetch("https://example.com/mixed")

        assert result == "ok after 429"
        assert call_count == 2
