# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from types import SimpleNamespace

import aiohttp

class _ChunkStream:
    def __init__(self, chunks):
        self._chunks = list(chunks)

    async def iter_chunked(self, _chunk_size):
        for chunk in self._chunks:
            yield chunk


class _Response:
    def __init__(
        self,
        *,
        status=200,
        body=b"",
        headers=None,
        content_length=None,
    ):
        self.status = status
        self._body = body
        self.headers = dict(headers or {})
        self.content_length = (
            len(body) if content_length is None else content_length
        )
        self.content = _ChunkStream([body] if body else [])

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        return False

    async def read(self):
        return self._body

    async def text(self):
        return self._body.decode()


class _Request:
    def __init__(self, response=None, error=None):
        self._response = response
        self._error = error

    def __await__(self):
        async def resolve():
            if self._error is not None:
                raise self._error
            return self._response

        return resolve().__await__()

    async def __aenter__(self):
        if self._error is not None:
            raise self._error
        return self._response

    async def __aexit__(self, exc_type, exc, traceback):
        return False


class _Client:
    def __init__(self, *, get_requests=(), head_requests=()):
        self.get_requests = list(get_requests)
        self.head_requests = list(head_requests)
        self.calls = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        return False

    def get(self, url, **kwargs):
        self.calls.append(("get", url, kwargs))
        return self.get_requests.pop(0)

    def head(self, url, **kwargs):
        self.calls.append(("head", url, kwargs))
        return self.head_requests.pop(0)


class _Acquire:
    def __init__(self, driver):
        self._connection = SimpleNamespace(
            raw_connection=SimpleNamespace(driver_connection=driver)
        )

    async def __aenter__(self):
        return self._connection

    async def __aexit__(self, exc_type, exc, traceback):
        return False


class _CopyDriver:
    def __init__(self, outcome=None):
        self.outcome = outcome
        self.calls = []

    async def copy_records_to_table(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        if isinstance(self.outcome, BaseException):
            raise self.outcome
        return self.outcome


class _InsertStatement:
    def __init__(self, status):
        self._status = status
        self.excluded = SimpleNamespace(
            id="excluded-id",
            value="excluded-value",
        )
        self.payload = None
        self.conflict = None

    def values(self, payload):
        self.payload = payload
        return self

    def on_conflict_do_update(self, **kwargs):
        self.conflict = ("update", kwargs)
        return self

    def on_conflict_do_nothing(self, **kwargs):
        self.conflict = ("nothing", kwargs)
        return self

    async def status(self):
        return await self._status(self)


def _fake_table(*, schema=None, include_value=True):
    columns = [SimpleNamespace(name="id", primary_key=True)]
    if include_value:
        columns.append(SimpleNamespace(name="value", primary_key=False))
    return SimpleNamespace(schema=schema, c=columns)


async def _fixed_download_info(*_args, **_kwargs):
    return 8, False


async def _fixed_download_timeout(*_args, **_kwargs):
    return aiohttp.ClientTimeout(total=10, sock_read=10)


class _BrokenHeaders:
    def get(self, _key):
        request_info = SimpleNamespace(real_url="https://example.test/error")
        raise aiohttp.ClientResponseError(
            request_info=request_info,
            history=(),
            status=503,
            message="unavailable",
        )


class _BrokenChunkStream:
    def __init__(self, error):
        self._error = error

    async def iter_chunked(self, _chunk_size):
        raise self._error
        yield b""
