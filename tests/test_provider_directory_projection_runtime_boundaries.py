# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Lease-fenced workset and admission runtime boundary tests."""

from __future__ import annotations

import asyncio

import pytest

import process.provider_directory_projection_workset as workset_module
from process.provider_directory_projection_admission_contract import (
    admission_registration_inputs,
)
from process.provider_directory_projection_contract import (
    projection_input_block,
    projection_shard_spec,
)
from process.provider_directory_projection_types import (
    ProviderDirectoryProjectionError,
    ProviderDirectoryProjectionLeaseLost,
)
from process.provider_directory_projection_workset import (
    _assert_sealed_admission,
    _is_registered_workset_replay,
    _normalized_workset,
    _seal_registered_workset,
    _validate_workset_resource_coverage,
    _validated_block_fields,
    claim_projection_shard,
    heartbeat_projection_shard,
    register_projection_workset,
)
from tests.provider_directory_projection_semantic_support import digest
from tests.test_provider_directory_projection_admission_v2 import (
    _ordered_admission,
)
from tests.test_provider_directory_projection_workset_contract import _workset


class _Transaction:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc_info):
        return False


class _RuntimeDatabase:
    def __init__(
        self,
        *,
        first_values=(),
        all_values=(),
        scalar_values=(),
        status_values=(),
    ):
        self.first_values = list(first_values)
        self.all_values = list(all_values)
        self.scalar_values = list(scalar_values)
        self.status_values = list(status_values)
        self.statements: list[str] = []

    def transaction(self):
        return _Transaction()

    async def first(self, statement: str, **_parameters):
        self.statements.append(statement)
        return self.first_values.pop(0) if self.first_values else None

    async def all(self, statement: str, **_parameters):
        self.statements.append(statement)
        return self.all_values.pop(0) if self.all_values else []

    async def scalar(self, statement: str, **_parameters):
        self.statements.append(statement)
        return self.scalar_values.pop(0) if self.scalar_values else None

    async def status(self, statement: str, **_parameters):
        self.statements.append(statement)
        return self.status_values.pop(0) if self.status_values else 1


def _stored_recipe_fields(recipe, **overrides):
    field_map = {
        "decoder_contract_id": recipe.decoder_contract_id,
        "input_set_sha256": recipe.input_set_sha256,
        "transform_contract_id": recipe.transform_contract_id,
        "scope_contract_id": recipe.scope_contract_id,
        "transform_context_hash": recipe.transform_context_hash,
        "transform_context_json": dict(recipe.transform_context),
        "resource_profile_hash": recipe.resource_profile_hash,
        "selected_resources_json": list(recipe.selected_resources),
        "required_resources_json": list(recipe.required_resources),
        "status": "building",
        "attempt": 1,
        "lease_token": digest("lease"),
        "lease_expires_at": "future",
        "workset_registered_at": "now",
    }
    field_map.update(overrides)
    return field_map


def _registration_fixture():
    block, recipe, binding, terminals = _ordered_admission()
    shard = projection_shard_spec(
        recipe=recipe.physical,
        partition_ordinal=0,
        input_block=block,
    )
    registration = admission_registration_inputs(
        recipe,
        (binding,),
        (shard,),
        1,
        900,
        terminal_zeros=terminals,
    )
    return recipe, registration


def test_workset_boundary_rejects_nonblocks_and_nonmixed_resource_sets():
    recipe, _lease, _block, _admission, _shard = _workset()
    with pytest.raises(ProviderDirectoryProjectionError, match="workset_invalid"):
        _validated_block_fields(object())
    with pytest.raises(ProviderDirectoryProjectionError, match="workset_invalid"):
        _normalized_workset(recipe.physical, (object(),), ())
    with pytest.raises(ProviderDirectoryProjectionError, match="resource_mismatch"):
        _validate_workset_resource_coverage(recipe.physical, ())
    with pytest.raises(ProviderDirectoryProjectionError, match="resource_mismatch"):
        _validate_workset_resource_coverage(
            recipe.physical,
            ({"resource_type": "Organization"},),
        )


def test_registered_workset_replay_requires_exact_stored_census(monkeypatch):
    recipe, lease, block, _admission, shard = _workset()
    block_fields, shard_fields, _block_hash, _shard_hash = _normalized_workset(
        recipe.physical,
        (block,),
        (shard,),
    )
    assert (
        asyncio.run(
            _is_registered_workset_replay(
                _RuntimeDatabase(),
                "mrf",
                lease,
                {},
                block_fields,
                shard_fields,
            )
        )
        is False
    )

    async def exact_stored_workset(_database, _schema, _lease):
        return block_fields, shard_fields

    monkeypatch.setattr(workset_module, "_stored_workset", exact_stored_workset)
    assert asyncio.run(
        _is_registered_workset_replay(
            _RuntimeDatabase(),
            "mrf",
            lease,
            {"workset_registered_at": "now"},
            block_fields,
            shard_fields,
        )
    )

    async def mismatched_stored_workset(_database, _schema, _lease):
        return [], []

    monkeypatch.setattr(workset_module, "_stored_workset", mismatched_stored_workset)
    with pytest.raises(ProviderDirectoryProjectionError, match="replay_mismatch"):
        asyncio.run(
            _is_registered_workset_replay(
                _RuntimeDatabase(),
                "mrf",
                lease,
                {"workset_registered_at": "now"},
                block_fields,
                shard_fields,
            )
        )


def test_workset_seal_detects_lost_recipe_generation(monkeypatch):
    recipe, lease, block, _admission, shard = _workset()
    block_fields, shard_fields, block_hash, shard_hash = _normalized_workset(
        recipe.physical,
        (block,),
        (shard,),
    )

    async def accept_action(*_args, **_kwargs):
        return None

    async def accept_insert(*_args, **_kwargs):
        return None

    monkeypatch.setattr(workset_module, "set_local_projection_action", accept_action)
    monkeypatch.setattr(workset_module, "_insert_workset", accept_insert)
    database = _RuntimeDatabase(status_values=(0,))
    with pytest.raises(ProviderDirectoryProjectionLeaseLost, match="lease_lost"):
        asyncio.run(
            _seal_registered_workset(
                database,
                "mrf",
                lease,
                block_fields,
                shard_fields,
                block_hash,
                shard_hash,
            )
        )


def test_workset_registration_recomputes_input_manifest_hash():
    recipe, lease, _block, _admission, _shard = _workset()
    substitute = projection_input_block(
        upstream_artifact_id=digest("substitute-artifact"),
        source_object_id=digest("substitute-object"),
        block_kind="ndjson",
        input_contract_id="test-input.v1",
        record_start=0,
        record_count=2,
        content_sha256=digest("substitute-content"),
        payload_sha256=digest("substitute-payload"),
        payload_bytes=128,
        summary={"resource_count": 2},
    )
    substitute_shard = projection_shard_spec(
        recipe=recipe.physical,
        partition_ordinal=0,
        input_block=substitute,
    )
    with pytest.raises(ProviderDirectoryProjectionError, match="manifest_hash_mismatch"):
        asyncio.run(
            register_projection_workset(
                lease,
                (substitute,),
                (substitute_shard,),
                database=_RuntimeDatabase(),
            )
        )


def test_exact_registered_workset_replay_returns_without_resealing(monkeypatch):
    _recipe, lease, block, _admission, shard = _workset()

    async def active_recipe(*_args, **_kwargs):
        return {"workset_registered_at": "now"}

    async def is_exact_replay(*_args, **_kwargs):
        return True

    async def unexpected_seal(*_args, **_kwargs):
        raise AssertionError("exact replay must not reseal")

    monkeypatch.setattr(workset_module, "locked_active_recipe", active_recipe)
    monkeypatch.setattr(
        workset_module,
        "_is_registered_workset_replay",
        is_exact_replay,
    )
    monkeypatch.setattr(workset_module, "_seal_registered_workset", unexpected_seal)
    asyncio.run(
        register_projection_workset(
            lease,
            (block,),
            (shard,),
            database=_RuntimeDatabase(),
        )
    )


def test_shard_claim_requires_sealed_admission_and_valid_identifiers(monkeypatch):
    _recipe, lease, _block, _admission, shard = _workset()
    with pytest.raises(ProviderDirectoryProjectionError, match="lease_seconds_invalid"):
        asyncio.run(
            claim_projection_shard(
                lease,
                admission_id=digest("admission"),
                lease_seconds=29,
            )
        )
    with pytest.raises(ProviderDirectoryProjectionError, match="admission_id_invalid"):
        asyncio.run(claim_projection_shard(lease, admission_id="invalid"))
    with pytest.raises(ProviderDirectoryProjectionError, match="partition_id_invalid"):
        asyncio.run(
            claim_projection_shard(
                lease,
                admission_id=digest("admission"),
                partition_id="invalid",
            )
        )

    with pytest.raises(ProviderDirectoryProjectionError, match="admission_not_sealed"):
        asyncio.run(
            _assert_sealed_admission(
                _RuntimeDatabase(first_values=(None,)),
                "mrf",
                lease,
                digest("admission"),
            )
        )
    assert shard.partition_ordinal == 0


def test_shard_claim_rejects_missing_workset_and_returns_when_none_claimable(
    monkeypatch,
):
    _recipe, lease, _block, _admission, _shard = _workset()

    async def missing_workset(*_args, **_kwargs):
        return {}

    monkeypatch.setattr(workset_module, "shared_active_recipe", missing_workset)
    with pytest.raises(ProviderDirectoryProjectionError, match="workset_missing"):
        asyncio.run(
            claim_projection_shard(
                lease,
                admission_id=digest("admission"),
                database=_RuntimeDatabase(),
            )
        )

    async def registered_workset(*_args, **_kwargs):
        return {"workset_registered_at": "now"}

    async def sealed_admission(*_args, **_kwargs):
        return None

    async def no_candidate(*_args, **_kwargs):
        return {}

    monkeypatch.setattr(workset_module, "shared_active_recipe", registered_workset)
    monkeypatch.setattr(workset_module, "_assert_sealed_admission", sealed_admission)
    monkeypatch.setattr(workset_module, "_claimable_shard_row", no_candidate)
    assert (
        asyncio.run(
            claim_projection_shard(
                lease,
                admission_id=digest("admission"),
                database=_RuntimeDatabase(),
            )
        )
        is None
    )


def test_shard_claim_returns_none_when_update_loses_candidate(monkeypatch):
    _recipe, lease, _block, _admission, _shard = _workset()

    async def registered_workset(*_args, **_kwargs):
        return {"workset_registered_at": "now"}

    async def sealed_admission(*_args, **_kwargs):
        return None

    async def candidate(*_args, **_kwargs):
        return {"partition_id": digest("partition"), "partition_attempt": 0}

    async def lost_update(*_args, **_kwargs):
        return None

    monkeypatch.setattr(workset_module, "shared_active_recipe", registered_workset)
    monkeypatch.setattr(workset_module, "_assert_sealed_admission", sealed_admission)
    monkeypatch.setattr(workset_module, "_claimable_shard_row", candidate)
    monkeypatch.setattr(workset_module, "_claim_shard_row", lost_update)
    assert (
        asyncio.run(
            claim_projection_shard(
                lease,
                admission_id=digest("admission"),
                database=_RuntimeDatabase(),
            )
        )
        is None
    )


def test_shard_heartbeat_enforces_bounds_and_exact_generation(monkeypatch):
    _recipe, lease, _block, _admission, shard = _workset()
    claim = workset_module._shard_claim(
        lease,
        digest("admission"),
        {
            "partition_id": shard.partition_id,
            "partition_ordinal": shard.partition_ordinal,
            "partition_key": shard.partition_key,
            "input_block_id": shard.input_block_id,
            "resource_type": shard.resource_type,
            "input_sha256": shard.input_sha256,
            "partition_attempt": 1,
            "lease_token": digest("shard-lease"),
        },
    )
    with pytest.raises(ProviderDirectoryProjectionError, match="lease_seconds_invalid"):
        asyncio.run(heartbeat_projection_shard(claim, lease_seconds=29))

    async def accept_action(*_args, **_kwargs):
        return None

    monkeypatch.setattr(workset_module, "set_local_projection_action", accept_action)
    asyncio.run(
        heartbeat_projection_shard(
            claim,
            lease_seconds=300,
            database=_RuntimeDatabase(status_values=(1,)),
        )
    )
    with pytest.raises(ProviderDirectoryProjectionLeaseLost, match="shard_lease_lost"):
        asyncio.run(
            heartbeat_projection_shard(
                claim,
                lease_seconds=300,
                database=_RuntimeDatabase(status_values=(0,)),
            )
        )
