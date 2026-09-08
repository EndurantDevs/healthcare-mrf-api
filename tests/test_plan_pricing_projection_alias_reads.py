# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Keep complete release authority while reading identical snapshots once."""

from copy import deepcopy
import hashlib
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import plan_pricing_projection_build as build
from api import plan_pricing_projection_contract as contract
from api import plan_pricing_projection_source as source
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError
from .test_plan_pricing_projection_coverage import _binding
from .test_plan_release_serving_readiness import _serving_table_descriptor


class _ScopeSession:
    def __init__(self, scopes):
        self.scopes = scopes
        self.calls = []

    async def execute(self, statement, parameters):
        self.calls.append((str(statement), parameters))
        matches = sorted(
            market for plan, market in self.scopes.get(parameters["snapshot_id"], ())
            if plan == parameters["plan_id"]
            and (not parameters["market_type"] or market == parameters["market_type"])
        )
        return SimpleNamespace(scalar_one_or_none=lambda: matches[0] if matches else None)


def _install_sources(monkeypatch, bindings, *, alias_fields=None):
    descriptors, scopes = {}, {}
    for binding in bindings:
        snapshot_id = binding["snapshot_id"]
        descriptors[snapshot_id] = _serving_table_descriptor(
            snapshot_id=snapshot_id, source_key=binding["source_key"],
            coverage_scope_id="c" * 64,
        )
        scopes.setdefault(snapshot_id, []).append((
            binding["plan_id"].strip(),
            (binding.get("market_type") or binding.get("plan_market_type") or "").strip().lower(),
        ))
    if alias_fields:
        from dataclasses import replace

        snapshot_id = bindings[-1]["snapshot_id"]
        descriptors[snapshot_id] = replace(descriptors[snapshot_id], **alias_fields)
    loader = AsyncMock(side_effect=lambda _session, snapshot: descriptors[snapshot])
    monkeypatch.setattr(source, "snapshot_serving_tables", loader)
    return _ScopeSession(scopes), loader


@pytest.mark.asyncio
@pytest.mark.parametrize("distinct_snapshot", [False, True])
async def test_alias_reads_preserve_manifest_and_first_ordinal(monkeypatch, distinct_snapshot):
    first = _binding(ordinal=4, source_file_id="first-logical-file")
    alias = _binding(ordinal=7, source_file_id="second-logical-file")
    alias.pop("market_type")
    alias["plan_market_type"] = " GROUP "
    alias["plan_id"] = " plan "
    alias.update(shared_snapshot_key=999, coverage_scope_id="e" * 64, storage_generation="untrusted")
    if distinct_snapshot:
        alias.update(snapshot_id="alias-snapshot", source_key="alias-source")
    bindings = [first, alias]
    original = deepcopy(bindings)
    session, loader = _install_sources(monkeypatch, bindings)
    reader = AsyncMock(side_effect=lambda _session, binding, **_kwargs: SimpleNamespace(
        binding=binding, raw_code_row_count=build.MAX_PROJECTION_CODE_ROWS,
    ))
    materialize = AsyncMock(return_value=SimpleNamespace())
    monkeypatch.setattr(build, "binding_projection", reader)
    monkeypatch.setattr(build, "materialize_factorized_projection", materialize)

    await build._materialize_all_codes(session, "candidate", bindings)

    assert loader.await_count == len(bindings)
    assert len(session.calls) == len(bindings)
    assert session.calls[-1][1]["plan_id"] == "plan"
    assert session.calls[-1][1]["market_type"] == "group"
    assert "plan_id = :plan_id" in session.calls[-1][0]
    assert "plan_market_type = :market_type" in session.calls[-1][0]
    reader.assert_awaited_once()
    assert reader.call_args.kwargs["serving_tables"].snapshot_id == first["snapshot_id"]
    assert reader.call_args.kwargs["serving_tables"].plan_id != "plan"
    assert materialize.call_args.args[2][0].binding["ordinal"] == 4
    assert len(materialize.call_args.args[2]) == 1
    assert bindings == original
    assert contract.normalized_bindings(bindings) == original


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["plan_id", "market_type"])
async def test_distinct_read_scope_is_not_collapsed(monkeypatch, field):
    bindings = [_binding(), _binding(ordinal=1, **{field: "different"})]
    session, _loader = _install_sources(monkeypatch, bindings)
    reader = AsyncMock(side_effect=lambda _session, binding, **_kwargs: SimpleNamespace(
        binding=binding, raw_code_row_count=1,
    ))
    materialize = AsyncMock(return_value=SimpleNamespace())
    monkeypatch.setattr(build, "binding_projection", reader)
    monkeypatch.setattr(build, "materialize_factorized_projection", materialize)

    await build._materialize_all_codes(session, "candidate", bindings)

    assert reader.await_count == 2
    assert len(materialize.call_args.args[2]) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("alias_fields", [
    {"shared_snapshot_key": 18},
    {"coverage_scope_id": "d" * 64},
    {"storage_generation": "shared_blocks_v4", "shared_block_layout": "packed_snapshot_maps_v4"},
])
async def test_distinct_sealed_layout_is_not_collapsed(monkeypatch, alias_fields):
    bindings = [_binding(), _binding(ordinal=1, snapshot_id="alias", source_key="alias-source")]
    session, _loader = _install_sources(monkeypatch, bindings, alias_fields=alias_fields)
    reader = AsyncMock(return_value=SimpleNamespace(raw_code_row_count=1))
    monkeypatch.setattr(build, "binding_projection", reader)
    monkeypatch.setattr(build, "materialize_factorized_projection", AsyncMock())

    await build._materialize_all_codes(session, "candidate", bindings)

    assert reader.await_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid_scope", ["source_key", "snapshot_id", "plan", "market"])
async def test_trailing_alias_is_validated_before_any_code_read(monkeypatch, invalid_scope):
    bindings = [_binding(), _binding(ordinal=1, snapshot_id="alias", source_key="alias-source")]
    alias_fields = {invalid_scope: "wrong"} if invalid_scope in {"source_key", "snapshot_id"} else None
    session, loader = _install_sources(monkeypatch, bindings, alias_fields=alias_fields)
    if invalid_scope in {"plan", "market"}:
        session.scopes["alias"] = [("wrong", "group")] if invalid_scope == "plan" else [("plan", "individual")]
    reader = AsyncMock()
    materialize = AsyncMock()
    monkeypatch.setattr(build, "binding_projection", reader)
    monkeypatch.setattr(build, "materialize_factorized_projection", materialize)

    with pytest.raises(ValueError, match="scope"):
        await build._materialize_all_codes(session, "candidate", bindings)

    assert loader.await_count == 2
    reader.assert_not_awaited()
    materialize.assert_not_awaited()


@pytest.mark.asyncio
async def test_unspecified_market_uses_each_snapshots_effective_scope(monkeypatch):
    bindings = [_binding(market_type=""), _binding(ordinal=1, snapshot_id="alias", market_type="")]
    session, _loader = _install_sources(monkeypatch, bindings)
    session.scopes = {"snapshot": [("plan", "group")], "alias": [("plan", "individual")]}
    reader = AsyncMock(return_value=SimpleNamespace(raw_code_row_count=1))
    monkeypatch.setattr(build, "binding_projection", reader)
    monkeypatch.setattr(build, "materialize_factorized_projection", AsyncMock())

    await build._materialize_all_codes(session, "candidate", bindings)

    assert reader.await_count == 2


@pytest.mark.asyncio
async def test_unpublished_trailing_alias_cannot_reuse_a_published_layout(monkeypatch):
    bindings = [_binding(), _binding(ordinal=1, snapshot_id="alias")]
    session, loader = _install_sources(monkeypatch, bindings)
    published_descriptor = loader.side_effect(session, "snapshot")
    loader.side_effect = [published_descriptor, PTG2ManifestArtifactError("unavailable")]
    reader = AsyncMock()
    monkeypatch.setattr(build, "binding_projection", reader)

    with pytest.raises(PTG2ManifestArtifactError, match="unavailable"):
        await build._materialize_all_codes(session, "candidate", bindings)

    assert loader.await_count == 2
    reader.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("ordinal", [0, False, float("inf")])
async def test_alias_cannot_hide_invalid_raw_ordinal(monkeypatch, ordinal):
    reader = AsyncMock()
    monkeypatch.setattr(build, "binding_projection", reader)
    with pytest.raises(ValueError, match="ordinal"):
        await build._materialize_all_codes(object(), "candidate", [
            _binding(), _binding(ordinal=ordinal),
        ])
    reader.assert_not_awaited()


def test_new_build_identity_preserves_storage_contract():
    digest, provider = "b" * 64, "c" * 64
    old_identity = hashlib.sha256(
        f"plan_pricing_factorized_v4\0{digest}\0{provider}".encode("ascii")
    ).hexdigest()
    assert contract.PROJECTION_CONTRACT == "plan_pricing_factorized_v4"
    assert contract.projection_id(digest, provider) != old_identity
    previous_build_identity = hashlib.sha256(
        f"plan_pricing_factorized_v4\0physical-read-dedup-v1\0{digest}\0{provider}".encode("ascii")
    ).hexdigest()
    assert contract.projection_id(digest, provider) != previous_build_identity


@pytest.mark.asyncio
async def test_role_scoped_ordinals_preserve_mixed_manifest(monkeypatch):
    bindings = [_binding(role="allowed_amounts"), _binding()]
    session, loader = _install_sources(monkeypatch, bindings)
    reader = AsyncMock(return_value=SimpleNamespace(raw_code_row_count=1))
    monkeypatch.setattr(build, "binding_projection", reader)
    monkeypatch.setattr(build, "materialize_factorized_projection", AsyncMock())

    assert contract.normalized_bindings(bindings) == bindings
    await build._materialize_all_codes(session, "candidate", bindings)

    loader.assert_awaited_once()
    reader.assert_awaited_once()
    assert reader.call_args.args[1] == bindings[1]
