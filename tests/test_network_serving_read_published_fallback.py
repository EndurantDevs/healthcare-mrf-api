# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""API reads use publication evidence when custody cannot be re-proved in this database."""

import json

import pytest

from process import network_published_api_pin as published_pin
from process import network_serving_read as serving


class _Connection:
    def is_in_transaction(self):
        return True


@pytest.fixture
def unverifiable(monkeypatch):
    """A manifest whose custody was sealed on another database."""

    async def read_manifest(_connection, _namespace, _generation_id):
        return {"candidate_json": json.dumps({})}

    async def refuse(*_arguments, **_options):
        raise ValueError("custody unavailable")

    monkeypatch.setattr(serving, "_read_manifest", read_manifest)
    monkeypatch.setattr(serving, "_candidate_custody", refuse)


async def test_api_read_serves_published_evidence_when_custody_is_unavailable(monkeypatch, unverifiable):
    manifest, calls = object(), []

    async def read_published(_connection, generation_id, schema):
        calls.append((generation_id, schema))
        return manifest, None, None

    monkeypatch.setattr(published_pin, "read_published_api_pin", read_published)
    resolved = await serving.resolve_network_serving_manifest(
        _Connection(), generation_id=3, control_schema="registry", api_read=True
    )
    assert resolved is manifest and calls == [(3, "registry")]


@pytest.mark.parametrize("failure", [None, serving.NetworkServingReadUnavailable("missing"), KeyError("candidate")])
async def test_api_read_keeps_the_refusal_without_usable_published_evidence(monkeypatch, unverifiable, failure):
    async def read_published(_connection, _generation_id, _schema):
        if failure is not None:
            raise failure
        return None

    monkeypatch.setattr(published_pin, "read_published_api_pin", read_published)
    with pytest.raises(serving.NetworkServingReadUnavailable, match="verification failed"):
        await serving.resolve_network_serving_manifest(_Connection(), control_schema="registry", api_read=True)


async def test_other_reads_never_use_published_evidence(monkeypatch, unverifiable):
    async def read_published(*_arguments):
        raise AssertionError("only API reads may use publication evidence")

    monkeypatch.setattr(published_pin, "read_published_api_pin", read_published)
    with pytest.raises(serving.NetworkServingReadUnavailable, match="verification failed"):
        await serving.resolve_network_serving_manifest(_Connection(), control_schema="registry")


async def test_missing_generation_is_refused_without_reading_published_evidence(monkeypatch):
    async def read_manifest(_connection, _namespace, _generation_id):
        return None

    async def read_published(*_arguments):
        raise AssertionError("a missing generation has no publication evidence")

    monkeypatch.setattr(serving, "_read_manifest", read_manifest)
    monkeypatch.setattr(published_pin, "read_published_api_pin", read_published)
    with pytest.raises(serving.NetworkServingReadUnavailable, match="manifest is unavailable"):
        await serving.resolve_network_serving_manifest(_Connection(), control_schema="registry", api_read=True)


@pytest.mark.parametrize(
    ("selector", "message"),
    [
        ({"api_read": 1}, "verification mode is invalid"),
        ({"generation_id": 0}, "generation is invalid"),
        ({"generation_id": True}, "generation is invalid"),
        ({"generation_id": 2**63}, "generation is invalid"),
    ],
)
async def test_invalid_selector_is_refused_before_the_connection_is_used(selector, message):
    with pytest.raises(serving.NetworkServingReadUnavailable, match=message):
        await serving.resolve_network_serving_manifest(object(), control_schema="registry", **selector)


async def test_read_outside_a_transaction_is_refused(monkeypatch):
    monkeypatch.setattr(_Connection, "is_in_transaction", lambda _connection: False)
    with pytest.raises(serving.NetworkServingReadUnavailable, match="caller-owned transaction"):
        await serving.resolve_network_serving_manifest(_Connection(), control_schema="registry", api_read=True)
