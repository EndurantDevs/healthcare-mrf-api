# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""API reads resolve the published routing metadata before any other verification."""

import json

import pytest

from process import network_published_api_pin as published_pin
from process import network_serving_read as serving


class _Connection:
    def is_in_transaction(self):
        return True


@pytest.fixture
def unverifiable(monkeypatch):
    """A manifest whose original verification refuses in this database."""

    async def read_manifest(_connection, _namespace, _generation_id):
        return {"candidate_json": json.dumps({})}

    async def refuse(*_arguments, **_options):
        raise ValueError("custody unavailable")

    monkeypatch.setattr(serving, "_read_manifest", read_manifest)
    monkeypatch.setattr(serving, "_candidate_custody", refuse)


async def test_api_read_serves_the_published_pin_without_other_verification(monkeypatch):
    manifest, calls = object(), []

    async def read_published(_connection, generation_id, schema):
        calls.append((generation_id, schema))
        return manifest, None, None

    async def unexpected(*_arguments, **_options):
        raise AssertionError("a published pin needs no further verification")

    monkeypatch.setattr(published_pin, "read_published_api_pin", read_published)
    monkeypatch.setattr(serving, "_read_manifest", unexpected)
    monkeypatch.setattr(serving, "_candidate_custody", unexpected)
    resolved = await serving.resolve_network_serving_manifest(
        _Connection(), generation_id=3, control_schema="registry", api_read=True
    )
    assert resolved is manifest and calls == [(3, "registry")]


@pytest.mark.parametrize(
    ("failure", "message"),
    [
        (None, "verification failed"),
        (serving.NetworkServingReadUnavailable("missing"), "missing"),
        (KeyError("candidate"), "verification failed"),
    ],
)
async def test_api_read_refuses_without_a_published_pin(monkeypatch, unverifiable, failure, message):
    """A generation without published routing metadata keeps its original verification."""

    async def read_published(_connection, _generation_id, _schema):
        if failure is not None:
            raise failure
        return None

    monkeypatch.setattr(published_pin, "read_published_api_pin", read_published)
    with pytest.raises(serving.NetworkServingReadUnavailable, match=message):
        await serving.resolve_network_serving_manifest(_Connection(), control_schema="registry", api_read=True)


async def test_other_reads_never_use_the_published_pin(monkeypatch, unverifiable):
    async def read_published(*_arguments):
        raise AssertionError("only API reads may use the published pin")

    monkeypatch.setattr(published_pin, "read_published_api_pin", read_published)
    with pytest.raises(serving.NetworkServingReadUnavailable, match="verification failed"):
        await serving.resolve_network_serving_manifest(_Connection(), control_schema="registry")
