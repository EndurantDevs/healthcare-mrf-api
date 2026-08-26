# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Focused branch coverage for immutable pricing projection contracts."""

from __future__ import annotations

from contextlib import asynccontextmanager
import hashlib
from types import SimpleNamespace

import pytest

from api import plan_pricing_projection_build as projection_build
from api import plan_pricing_projection_contract as projection_contract

from .test_plan_pricing_projection import PROJECTION_ID


class _Result:
    def __init__(self, rows=()):
        self.rows = list(rows)

    def mappings(self):
        return self

    def one_or_none(self):
        return self.rows[0] if self.rows else None

    def one(self):
        return self.rows[0]


class _ResultSession:
    def __init__(self, *results):
        self.results = list(results)
        self.statements = []

    async def execute(self, statement, params=None):
        self.statements.append((str(statement), params))
        return self.results.pop(0) if self.results else _Result()


def _candidate(**updates):
    candidate_by_field = {
        "projection_id": PROJECTION_ID,
        "binding_manifest_digest": "b" * 64,
        "binding_manifest": [{"snapshot_id": "snapshot"}],
        "provider_signature": "c" * 64,
        "content_digest": "d" * 64,
        "card_row_count": 2,
        "aggregate_row_count": 1,
        "fragment_byte_count": 80,
        "build_seconds": 1.5,
        "state": "ready",
    }
    candidate_by_field.update(updates)
    return candidate_by_field


@pytest.mark.asyncio
async def test_existing_projection_candidates_are_reused_or_replaced():
    binding_manifest_list = [{"snapshot_id": "snapshot"}]
    assert await projection_build._existing_candidate_receipt(
        _ResultSession(_Result()),
        PROJECTION_ID,
        binding_manifest_list,
        "b" * 64,
        "c" * 64,
    ) is None

    ready = await projection_build._existing_candidate_receipt(
        _ResultSession(_Result([_candidate()])),
        PROJECTION_ID,
        binding_manifest_list,
        "b" * 64,
        "c" * 64,
    )
    assert ready["state"] == "ready"
    assert ready["card_row_count"] == 2

    with pytest.raises(ValueError, match="identity collision"):
        await projection_build._existing_candidate_receipt(
            _ResultSession(_Result([_candidate(provider_signature="e" * 64)])),
            PROJECTION_ID,
            binding_manifest_list,
            "b" * 64,
            "c" * 64,
        )

    stale_session = _ResultSession(_Result([_candidate(state="building")]))
    assert await projection_build._existing_candidate_receipt(
        stale_session,
        PROJECTION_ID,
        binding_manifest_list,
        "b" * 64,
        "c" * 64,
    ) is None
    assert "DELETE FROM" in stale_session.statements[-1][0]


@pytest.mark.asyncio
async def test_projection_candidate_insert_and_seal_keep_receipt_counts():
    session = _ResultSession(
        _Result(),
        _Result([_candidate()]),
    )
    await projection_build._insert_candidate(
        session,
        PROJECTION_ID,
        [{"snapshot_id": "snapshot"}],
        "b" * 64,
        "c" * 64,
    )
    receipt = await projection_build._seal_candidate(
        session,
        PROJECTION_ID,
        hashlib.sha256(b"content"),
        (2, 1, 80),
        1.5,
    )
    assert "INSERT INTO" in session.statements[0][0]
    assert session.statements[0][1]["binding_manifest"] == (
        '[{"snapshot_id":"snapshot"}]'
    )
    assert receipt["aggregate_row_count"] == 1
    assert len(session.statements[1][1]["content_digest"]) == 64


@pytest.mark.asyncio
async def test_materialize_all_codes_requires_rates_and_sorts_identity(
    monkeypatch,
):
    with pytest.raises(ValueError, match="in-network binding"):
        await projection_build._materialize_all_codes(
            object(), PROJECTION_ID, [{"role": "allowed_amounts"}]
        )

    async def binding_projection(_session, binding):
        return SimpleNamespace(
            binding=binding,
            code_rows_by_identity={
                ("HCPCS", "G0439"): [{}],
                ("CPT", "27447"): [{}],
            },
        )

    calls = []

    async def project_code(_session, _candidate_id, code_identity, *_args):
        calls.append(code_identity)
        return (1, 2, 3)

    monkeypatch.setattr(projection_build, "binding_projection", binding_projection)
    monkeypatch.setattr(projection_build, "project_code", project_code)
    _digest, card_count, aggregate_count, fragment_bytes = (
        await projection_build._materialize_all_codes(
            object(), PROJECTION_ID, [{"role": "in_network"}]
        )
    )
    assert calls == [("CPT", "27447"), ("HCPCS", "G0439")]
    assert (card_count, aggregate_count, fragment_bytes) == (2, 4, 6)


@pytest.mark.asyncio
async def test_session_builder_reuses_or_builds_exact_candidate(monkeypatch):
    with pytest.raises(ValueError, match="digest is invalid"):
        await projection_build.build_in_session(
            object(), binding_manifest_digest="bad", bindings=[]
        )

    binding_by_field = {
        "snapshot_id": "snapshot",
        "source_key": "source",
        "plan_id": "plan",
        "role": "in_network",
        "ordinal": 0,
    }

    async def provider_signature(_session):
        return "c" * 64

    async def existing(*_args):
        return {"state": "ready"}

    monkeypatch.setattr(projection_build, "provider_signature", provider_signature)
    monkeypatch.setattr(projection_build, "_existing_candidate_receipt", existing)
    assert await projection_build.build_in_session(
        _ResultSession(),
        binding_manifest_digest="b" * 64,
        bindings=[binding_by_field],
    ) == {"state": "ready"}

    calls = []

    async def absent(*_args):
        return None

    async def insert(*_args):
        calls.append("insert")

    async def materialize(*_args):
        calls.append("materialize")
        return hashlib.sha256(), 1, 2, 3

    async def seal(*_args):
        calls.append("seal")
        return {"state": "ready"}

    monkeypatch.setattr(projection_build, "_existing_candidate_receipt", absent)
    monkeypatch.setattr(projection_build, "_insert_candidate", insert)
    monkeypatch.setattr(projection_build, "_materialize_all_codes", materialize)
    monkeypatch.setattr(projection_build, "_seal_candidate", seal)
    assert await projection_build.build_in_session(
        _ResultSession(),
        binding_manifest_digest="b" * 64,
        bindings=[binding_by_field],
    ) == {"state": "ready"}
    assert calls == ["insert", "materialize", "seal"]


@pytest.mark.asyncio
async def test_public_builder_locks_provider_generation_before_build(monkeypatch):
    session = object()
    calls = []

    @asynccontextmanager
    async def transaction():
        yield session

    async def lock(actual_session):
        calls.append(("lock", actual_session))

    async def build(actual_session, **kwargs):
        calls.append(("build", actual_session, kwargs))
        return {"state": "ready"}

    monkeypatch.setattr(projection_build.db, "transaction", transaction)
    monkeypatch.setattr(projection_build, "lock_provider_generation", lock)
    monkeypatch.setattr(projection_build, "build_in_session", build)
    assert await projection_build.build_plan_pricing_projection(
        binding_manifest_digest="b" * 64, bindings=[]
    ) == {"state": "ready"}
    assert [call[0] for call in calls] == ["lock", "build"]


@pytest.mark.parametrize(
    "bindings",
    (
        None,
        [],
        ["not-an-object"],
        [{"snapshot_id": "s", "source_key": "k", "plan_id": "p"}],
        [
            {
                "snapshot_id": "s",
                "source_key": "k",
                "plan_id": "p",
                "role": "in_network",
                "ordinal": "bad",
            }
        ],
        [
            {
                "snapshot_id": "s",
                "source_key": "k",
                "plan_id": "p",
                "role": "in_network",
                "ordinal": -1,
            }
        ],
    ),
)
def test_projection_bindings_reject_incomplete_identity(bindings):
    with pytest.raises(ValueError, match="pricing projection binding"):
        projection_contract.normalized_bindings(bindings)


def test_projection_contract_identity_and_valid_binding_are_stable():
    binding_by_field = {
        "snapshot_id": "s",
        "source_key": "k",
        "plan_id": "p",
        "role": "in_network",
        "binding_ordinal": 0,
    }
    assert projection_contract.normalized_bindings([binding_by_field]) == [
        binding_by_field
    ]
    assert len(projection_contract.projection_id("b" * 64, "c" * 64)) == 64
    assert projection_contract.projection_code_identity(None, "27447") is None

    for malformed in ("not-json", "[]"):
        with pytest.raises(ValueError, match="relations are incomplete"):
            projection_contract._validated_provider_signature(malformed)
