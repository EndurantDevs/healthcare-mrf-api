"""Small deterministic fakes shared by claims-pricing contract tests."""

from __future__ import annotations

from typing import Any


class AsyncChunkStream:
    def __init__(self, chunks: list[bytes]) -> None:
        self.chunks = chunks

    async def iter_chunked(self, _chunk_size: int):
        """Yield configured response chunks without network access."""

        for response_chunk in self.chunks:
            yield response_chunk


class FakeHttpResponse:
    def __init__(
        self,
        *,
        text_payload: str = "",
        chunks: list[bytes] | None = None,
        status_error: Exception | None = None,
    ) -> None:
        self.text_payload = text_payload
        self.content = AsyncChunkStream(chunks or [])
        self.status_error = status_error
        self.status_checks = 0

    async def text(self) -> str:
        """Return the configured response body."""

        return self.text_payload

    def raise_for_status(self) -> None:
        """Record validation and raise the configured HTTP failure."""

        self.status_checks += 1
        if self.status_error is not None:
            raise self.status_error


class AsyncResponseContext:
    def __init__(self, response: FakeHttpResponse) -> None:
        self.response = response

    async def __aenter__(self) -> FakeHttpResponse:
        """Return the configured response from an async context."""

        return self.response

    async def __aexit__(self, *_exc_info: Any) -> None:
        """Leave the response context without suppressing failures."""

        return None


class DownloadHttpClient:
    def __init__(self, response: FakeHttpResponse) -> None:
        self.response = response
        self.requests: list[tuple[str, Any]] = []

    async def __aenter__(self) -> "DownloadHttpClient":
        """Enter the fake client context."""

        return self

    async def __aexit__(self, *_exc_info: Any) -> None:
        """Leave the fake client context."""

        return None

    def get(self, url: str, *, timeout: Any) -> AsyncResponseContext:
        """Record a streaming GET and return its response context."""

        self.requests.append((url, timeout))
        return AsyncResponseContext(self.response)


class CatalogHttpClient:
    def __init__(self, outcomes: list[FakeHttpResponse | Exception]) -> None:
        self.outcomes = list(outcomes)
        self.requests: list[tuple[str, Any]] = []

    async def __aenter__(self) -> "CatalogHttpClient":
        """Enter the fake catalog client context."""

        return self

    async def __aexit__(self, *_exc_info: Any) -> None:
        """Leave the fake catalog client context."""

        return None

    async def get(self, url: str, *, timeout: Any) -> FakeHttpResponse:
        """Return or raise the next configured catalog outcome."""

        self.requests.append((url, timeout))
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


class RecordingRedis:
    def __init__(self) -> None:
        self.values_by_key: dict[str, Any] = {}
        self.members_by_key: dict[str, set[str]] = {}
        self.jobs: list[dict[str, Any]] = []
        self.expired_keys: list[tuple[str, int]] = []
        self.deleted_keys: list[str] = []

    async def delete(self, *keys: str) -> None:
        """Delete each configured scalar or set key."""

        self.deleted_keys.extend(keys)
        for redis_key in keys:
            self.values_by_key.pop(redis_key, None)
            self.members_by_key.pop(redis_key, None)

    async def set(
        self,
        key: str,
        value: Any,
        *,
        ex: int | None = None,
        nx: bool = False,
    ) -> int:
        """Store a scalar value and honor the lock-style NX contract."""

        if nx and key in self.values_by_key:
            return 0
        self.values_by_key[key] = value
        if ex is not None:
            self.expired_keys.append((key, ex))
        return 1

    async def get(self, key: str) -> Any:
        """Return a configured scalar value."""

        return self.values_by_key.get(key)

    async def expire(self, key: str, seconds: int) -> None:
        """Record TTL refreshes."""

        self.expired_keys.append((key, seconds))

    async def sadd(self, key: str, member: str) -> None:
        """Add one set member."""

        self.members_by_key.setdefault(key, set()).add(member)

    async def srem(self, key: str, member: str) -> None:
        """Remove one set member."""

        self.members_by_key.setdefault(key, set()).discard(member)

    async def scard(self, key: str) -> int:
        """Return a set cardinality."""

        return len(self.members_by_key.get(key, set()))

    async def incrby(self, key: str, delta: int) -> None:
        """Increment an integer-like scalar value."""

        current = int(self.values_by_key.get(key, 0))
        self.values_by_key[key] = str(current + delta)

    async def enqueue_job(self, function_name: str, task_by_field: dict[str, Any], **options: Any) -> None:
        """Record one ARQ job using its public payload and options."""

        self.jobs.append(
            {
                "function": function_name,
                "task": task_by_field,
                "options": options,
            }
        )


class AsyncTransaction:
    async def __aenter__(self) -> "AsyncTransaction":
        """Enter a no-op database transaction."""

        return self

    async def __aexit__(self, *_exc_info: Any) -> None:
        """Leave a no-op transaction without suppressing failures."""

        return None
