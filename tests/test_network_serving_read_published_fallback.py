# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""API reads resolve only the published routing metadata; other readers never use it."""

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

    async def read_published(_connection, generation_id, schema, *, approved):
        calls.append((generation_id, schema, approved))
        return manifest, None, None

    async def unexpected(*_arguments, **_options):
        raise AssertionError("a published pin needs no further verification")

    monkeypatch.setattr(published_pin, "read_published_api_pin", read_published)
    monkeypatch.setattr(serving, "_read_manifest", unexpected)
    monkeypatch.setattr(serving, "_candidate_custody", unexpected)
    resolved = await serving.resolve_network_serving_manifest(
        _Connection(), generation_id=3, control_schema="registry", api_read=True
    )
    assert resolved is manifest and calls == [(3, "registry", False)]


@pytest.mark.parametrize(
    ("failure", "message"),
    [
        (serving.NetworkServingReadUnavailable("missing"), "missing"),
        (KeyError("candidate"), "verification failed"),
    ],
)
async def test_api_read_has_no_verification_besides_the_published_pin(monkeypatch, failure, message):
    async def read_published(_connection, _generation_id, _schema, *, approved):
        raise failure

    async def unexpected(*_arguments, **_options):
        raise AssertionError("an API read never falls back to the sealed manifest")

    monkeypatch.setattr(published_pin, "read_published_api_pin", read_published)
    monkeypatch.setattr(serving, "_read_manifest", unexpected)
    with pytest.raises(serving.NetworkServingReadUnavailable, match=message):
        await serving.resolve_network_serving_manifest(_Connection(), control_schema="registry", api_read=True)


async def test_other_reads_never_use_the_published_pin(monkeypatch, unverifiable):
    async def read_published(*_arguments, **_options):
        raise AssertionError("only API reads may use the published pin")

    monkeypatch.setattr(published_pin, "read_published_api_pin", read_published)
    with pytest.raises(serving.NetworkServingReadUnavailable, match="verification failed"):
        await serving.resolve_network_serving_manifest(_Connection(), control_schema="registry")


async def test_missing_generation_is_refused_without_reading_published_evidence(monkeypatch):
    """Readers other than API reads look the generation up directly."""

    async def read_manifest(_connection, _namespace, _generation_id):
        return None

    async def read_published(*_arguments, **_options):
        raise AssertionError("a missing generation has no publication evidence")

    monkeypatch.setattr(serving, "_read_manifest", read_manifest)
    monkeypatch.setattr(published_pin, "read_published_api_pin", read_published)
    with pytest.raises(serving.NetworkServingReadUnavailable, match="manifest is unavailable"):
        await serving.resolve_network_serving_manifest(_Connection(), control_schema="registry")


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
