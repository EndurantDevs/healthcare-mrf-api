# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Projection upgrades participate in existing materialization equivalence."""

from dataclasses import replace
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process.custom_import import query_projection_finality as finality
from process.custom_import.publication import _Materialization
from process.custom_import.query_projection_binding import QueryProjectionUnavailable
from process.custom_import.query_projection_evidence import ProjectionDigest
from process.custom_import.query_projection_finality import (
    bind_generation_materialization,
    bind_materialization,
    load_generation_evidence,
)
from tests.test_custom_import_query_projection_evidence import _fixture_digest, _generation, _manifest, _seal
from tests.test_custom_import_query_projection_storage import _definition


def _materialization():
    return _Materialization(b"s" * 32, b"m" * 32, b"e" * 32, 1, 1, 1, 1, 1, 2, 3, 4)


def _evidence(value="2.5"):
    digest = ProjectionDigest(_definition(enabled=("combined_score",)))
    digest.add_root("1234567893", 10, {"combined_score": Decimal(value)})
    return digest.finish()


def test_identical_projection_content_converges_and_legacy_upgrade_is_observable():
    materialization = _materialization()
    evidence = _evidence()
    projected = bind_materialization(materialization, evidence)
    assert bind_materialization(materialization, None) is materialization
    assert projected == bind_materialization(materialization, _evidence("2.500000000000"))
    assert projected.query_projection_evidence == evidence
    assert projected.materialization_sha256 != materialization.materialization_sha256
    assert projected.effective_output_sha256 != materialization.effective_output_sha256
    assert projected.root_count == materialization.root_count


def test_changed_values_or_capability_change_both_commitments():
    materialization = _materialization()
    evidence = _evidence()
    projected = bind_materialization(materialization, evidence)
    for changed in (_evidence("3"), replace(evidence, capability_sha256=b"c" * 32)):
        actual = bind_materialization(materialization, changed)
        assert actual.materialization_sha256 != projected.materialization_sha256
        assert actual.effective_output_sha256 != projected.effective_output_sha256


@pytest.mark.asyncio
async def test_disabled_definition_still_rejects_partial_or_advertised_seal_evidence():
    definition = _definition(enabled=())
    generation = SimpleNamespace(generation_id=1)
    evidence = ProjectionDigest(definition).finish()
    for markers_by_name in (evidence.seal_values(), evidence.seal_values() | {"query_projection_contract": None}):
        session = SimpleNamespace(get=AsyncMock(return_value=SimpleNamespace(**markers_by_name)))
        with pytest.raises(QueryProjectionUnavailable):
            await load_generation_evidence(session, generation, definition)


@pytest.mark.asyncio
@pytest.mark.parametrize("is_partial", [False, True])
async def test_disabled_materialization_checks_advertised_seal_evidence(is_partial):
    definition = _definition(enabled=())
    markers_by_name = ProjectionDigest(definition).finish().seal_values()
    if is_partial:
        markers_by_name["query_projection_contract"] = None
    session = SimpleNamespace(
        get=AsyncMock(return_value=SimpleNamespace(**markers_by_name)),
        in_transaction=lambda: True,
    )
    with pytest.raises(QueryProjectionUnavailable):
        await bind_generation_materialization(session, SimpleNamespace(generation_id=1), _materialization(), definition)
    session.get.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("is_legacy", [False, True])
@pytest.mark.parametrize("in_transaction", [False, True])
async def test_disabled_materialization_accepts_legacy_or_unsealed_generation(is_legacy, in_transaction):
    definition = _definition(enabled=())
    markers_by_name = {name: None for name in ProjectionDigest(definition).finish().seal_values()}
    transaction = AsyncMock()
    session = SimpleNamespace(
        get=AsyncMock(return_value=SimpleNamespace(**markers_by_name) if is_legacy else None),
        in_transaction=lambda: in_transaction,
        begin=Mock(return_value=transaction),
    )
    materialization = _materialization()
    assert (
        await bind_generation_materialization(session, SimpleNamespace(generation_id=1), materialization, definition)
        is materialization
    )
    session.get.assert_awaited_once()
    assert transaction.__aenter__.await_count == (0 if in_transaction else 1)
    assert transaction.__aexit__.await_count == (0 if in_transaction else 1)


@pytest.mark.asyncio
async def test_missing_definition_revision_fails_before_manifest_or_table_access():
    session = SimpleNamespace(get=AsyncMock(return_value=None), scalars=AsyncMock())
    with pytest.raises(QueryProjectionUnavailable, match="definition revision is missing"):
        await load_generation_evidence(session, _generation())
    assert session.get.await_count == 2
    session.scalars.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("sealed", [False, True])
async def test_configured_generation_never_falls_back_when_manifest_is_missing(monkeypatch, sealed):
    definition = _definition()
    evidence = _fixture_digest(definition).finish()
    session = SimpleNamespace(
        get=AsyncMock(
            side_effect=[
                _seal(evidence) if sealed else None,
                SimpleNamespace(canonical_definition=definition.canonical),
            ]
        ),
        scalars=AsyncMock(return_value=SimpleNamespace(one_or_none=lambda: None)),
    )
    lock = AsyncMock()
    monkeypatch.setattr(finality, "lock_projection_tables", lock)
    with pytest.raises(QueryProjectionUnavailable, match="required query projection manifest is missing"):
        await load_generation_evidence(session, _generation())
    lock.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("frozen", [False, True])
async def test_finality_binds_exact_generation_and_seal_before_locking_manifest(monkeypatch, frozen):
    definition = _definition()
    generation = _generation()
    evidence = _fixture_digest(definition).finish()
    manifest = _manifest(definition, evidence, generation)
    seal = _seal(evidence, generation)
    session = SimpleNamespace(
        get=AsyncMock(return_value=seal), scalars=AsyncMock(return_value=SimpleNamespace(one_or_none=lambda: manifest))
    )
    lock = AsyncMock()
    monkeypatch.setattr(finality, "lock_projection_tables", lock)
    assert await load_generation_evidence(session, generation, definition, frozen=frozen) == evidence
    lock.assert_awaited_once_with(session, manifest, definition, require_complete=frozen)
    lock.reset_mock()
    seal.sealing_token_sha256 = b"x" * 32
    with pytest.raises(QueryProjectionUnavailable, match="producer binding differs"):
        await load_generation_evidence(session, generation, definition, frozen=frozen)
    lock.assert_not_awaited()


@pytest.mark.asyncio
async def test_explicit_legacy_seal_skips_lookup_but_advertised_seal_requires_revalidation(monkeypatch):
    evidence = _evidence()
    legacy_seal = SimpleNamespace(**dict.fromkeys(evidence.seal_values()))
    load = AsyncMock(return_value=evidence)
    monkeypatch.setattr(finality, "load_generation_evidence", load)
    session, generation, definition = object(), _generation(), _definition()
    assert await finality.validate_sealed_projection(session, generation, legacy_seal, definition) is None
    load.assert_not_awaited()
    assert await finality.validate_sealed_projection(session, generation, _seal(evidence), definition) == evidence
    load.assert_awaited_once_with(session, generation, definition)
