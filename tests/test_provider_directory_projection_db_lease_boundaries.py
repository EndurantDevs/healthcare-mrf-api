# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Database-setting and recipe-lease fail-closed boundary tests."""

from __future__ import annotations

import asyncio
import json

import pytest

import process.provider_directory_projection_lease as lease_module
from process.provider_directory_projection_db import (
    assert_stage_trigger,
    json_value,
    locked_active_recipe,
    projection_maintenance_work_mem_mb,
    quoted_identifier,
    relation_oid,
    row_mapping,
    set_local_projection_action,
    set_local_projection_maintenance_work_mem,
    set_local_projection_wal_compression,
    shared_active_recipe,
)
from process.provider_directory_projection_lease import (
    _claim_locked_recipe,
    _locked_recipe,
    claim_projection_recipe,
    heartbeat_projection_lease,
)
from process.provider_directory_projection_types import (
    ProjectionLease,
    ProviderDirectoryProjectionBusy,
    ProviderDirectoryProjectionError,
    ProviderDirectoryProjectionLeaseLost,
)
from tests.provider_directory_projection_semantic_support import digest
from tests.test_provider_directory_physical_projection import (
    _StageCatalogDatabase,
    _exact_stage_trigger_fields,
    _prepared_stage,
)
from tests.test_provider_directory_projection_workset_contract import _workset


class _Transaction:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc_info):
        return False


class _SettingDatabase:
    def __init__(self, *, fail_setting: str | None = None):
        self.fail_setting = fail_setting
        self.settings: dict[str, str] = {}

    async def scalar(self, statement: str, **parameters):
        assert "set_config" in statement
        setting_name = statement.split("provider_directory_projection_", 1)[1]
        setting_name = setting_name.split("'", 1)[0]
        value = parameters["setting_value"]
        self.settings[setting_name] = value
        return "mismatch" if setting_name == self.fail_setting else value


class _StatusDatabase:
    def __init__(self, *, result=1):
        self.result = result
        self.statements: list[str] = []

    def transaction(self):
        return _Transaction()

    async def status(self, statement: str, **_parameters):
        self.statements.append(statement)
        return self.result

    async def scalar(self, _statement: str, **_parameters):
        return 0


class _RecipeDatabase:
    def __init__(self, fields, *, live=True):
        self.fields = fields
        self.live = live

    async def first(self, _statement: str, **_parameters):
        return self.fields

    async def scalar(self, _statement: str, **_parameters):
        return self.live


def _stored_recipe_fields(recipe, **overrides):
    field_map = {
        "recipe_id": recipe.recipe_id,
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
    }
    field_map.update(overrides)
    return field_map


def test_database_value_helpers_preserve_mapping_and_decode_json():
    class Row:
        _mapping = {"value": 1}

    assert row_mapping(None) == {}
    assert row_mapping(Row()) == {"value": 1}
    assert json_value('{"value":1}') == {"value": 1}
    assert json_value({"value": 1}) == {"value": 1}
    with pytest.raises(ProviderDirectoryProjectionError, match="identifier_invalid"):
        quoted_identifier('unsafe"name')


def test_maintenance_work_mem_is_bounded_and_applied(monkeypatch):
    monkeypatch.delenv(
        "HLTHPRT_PROVIDER_DIRECTORY_PROJECTION_MAINTENANCE_WORK_MEM_MB",
        raising=False,
    )
    assert projection_maintenance_work_mem_mb() == 256

    for invalid_value in ("not-a-number", "63", "1025"):
        monkeypatch.setenv(
            "HLTHPRT_PROVIDER_DIRECTORY_PROJECTION_MAINTENANCE_WORK_MEM_MB",
            invalid_value,
        )
        with pytest.raises(ProviderDirectoryProjectionError, match="work_mem_invalid"):
            projection_maintenance_work_mem_mb()

    monkeypatch.setenv(
        "HLTHPRT_PROVIDER_DIRECTORY_PROJECTION_MAINTENANCE_WORK_MEM_MB",
        "512",
    )
    database = _StatusDatabase()
    asyncio.run(set_local_projection_maintenance_work_mem(database))
    assert database.statements == ["SET LOCAL maintenance_work_mem = '512MB';"]


def test_database_action_rejects_invalid_identity_and_cross_action_fields():
    database = _SettingDatabase()
    with pytest.raises(ProviderDirectoryProjectionError, match="identity_invalid"):
        asyncio.run(
            set_local_projection_action(
                database,
                "recipe_insert",
                recipe_id="not-a-hash",
                recipe_attempt=1,
            )
        )
    with pytest.raises(ProviderDirectoryProjectionError, match="identity_invalid"):
        asyncio.run(
            set_local_projection_action(
                database,
                "admission_insert",
                recipe_id=digest("recipe"),
                recipe_attempt=1,
            )
        )
    with pytest.raises(ProviderDirectoryProjectionError, match="identity_invalid"):
        asyncio.run(
            set_local_projection_action(
                database,
                "reference_insert",
                recipe_id=digest("recipe"),
                recipe_attempt=1,
                physical_projection_id=digest("projection"),
                reference_owner_kind="dataset",
                reference_owner_id="owner",
                reference_identity_hash=digest("reference"),
                reference_lease_token=digest("reference-lease"),
                previous_reference_lease_token=digest("unexpected-previous"),
            )
        )
    with pytest.raises(ProviderDirectoryProjectionError, match="identity_invalid"):
        asyncio.run(
            set_local_projection_action(
                database,
                "recipe_insert",
                recipe_id=digest("recipe"),
                recipe_attempt=1,
                reference_owner_kind="dataset",
            )
        )


def test_database_action_sets_exact_reference_fence_and_detects_setting_failure():
    database = _SettingDatabase()
    asyncio.run(
        set_local_projection_action(
            database,
            "reference_reclaim",
            recipe_id=digest("recipe"),
            recipe_attempt=1,
            physical_projection_id=digest("projection"),
            reference_owner_kind="artifact",
            reference_owner_id="artifact-1",
            reference_identity_hash=digest("reference"),
            reference_lease_token=digest("new-lease"),
            previous_reference_lease_token=digest("old-lease"),
        )
    )
    assert database.settings["reference_owner_kind"] == "artifact"
    assert database.settings["reference_previous_lease_token"] == digest("old-lease")

    failing_database = _SettingDatabase(fail_setting="recipe_id")
    with pytest.raises(ProviderDirectoryProjectionError, match="setting_failed"):
        asyncio.run(
            set_local_projection_action(
                failing_database,
                "recipe_insert",
                recipe_id=digest("recipe"),
                recipe_attempt=1,
            )
        )


def test_wal_compression_off_and_post_set_verification(monkeypatch):
    class WalDatabase:
        async def scalar(self, statement: str):
            if "server_version_num" in statement:
                return 180000
            if "has_parameter_privilege" in statement:
                return True
            if "ANY(enumvals)" in statement:
                return "zstd"
            if "current_setting('wal_compression')" in statement:
                return "off"
            raise AssertionError(statement)

        async def status(self, _statement: str):
            return None

    monkeypatch.setenv(
        "HLTHPRT_PROVIDER_DIRECTORY_PROJECTION_WAL_COMPRESSION",
        "off",
    )
    assert asyncio.run(set_local_projection_wal_compression(WalDatabase())) is None

    monkeypatch.setenv(
        "HLTHPRT_PROVIDER_DIRECTORY_PROJECTION_WAL_COMPRESSION",
        "auto",
    )
    with pytest.raises(ProviderDirectoryProjectionError, match="unavailable"):
        asyncio.run(set_local_projection_wal_compression(WalDatabase()))


def test_relation_oid_handles_missing_and_present_catalog_rows():
    class OidDatabase:
        def __init__(self, oid):
            self.oid = oid

        async def scalar(self, _statement: str, **parameters):
            assert parameters["qualified_relation"] == "mrf.projection"
            return self.oid

    assert asyncio.run(relation_oid(OidDatabase(None), "mrf", "projection")) is None
    assert asyncio.run(relation_oid(OidDatabase("123"), "mrf", "projection")) == 123


def test_active_recipe_fences_reject_stale_or_missing_database_identity():
    recipe, lease, _block, _admission, _shard = _workset()
    stored = _stored_recipe_fields(recipe.physical, status="failed")
    with pytest.raises(ProviderDirectoryProjectionLeaseLost, match="lease_lost"):
        asyncio.run(
            locked_active_recipe(
                lease,
                database=_RecipeDatabase(stored),
                schema="mrf",
            )
        )
    stored = _stored_recipe_fields(recipe.physical, status="proof_ready")
    with pytest.raises(ProviderDirectoryProjectionLeaseLost, match="lease_lost"):
        asyncio.run(
            shared_active_recipe(
                lease,
                database=_RecipeDatabase(stored),
                schema="mrf",
            )
        )


def test_stage_trigger_accepts_driver_bytes_for_enabled_state():
    trigger_fields = _exact_stage_trigger_fields()
    trigger_fields["tgenabled"] = b"O"
    asyncio.run(
        assert_stage_trigger(
            _StageCatalogDatabase(trigger_fields),
            _prepared_stage(),
        )
    )


def test_locked_recipe_rejects_hash_collision_against_stored_identity():
    recipe, _lease, _block, _admission, _shard = _workset()
    stored = _stored_recipe_fields(
        recipe.physical,
        decoder_contract_id="other-decoder.v1",
    )
    with pytest.raises(ProviderDirectoryProjectionError, match="identity_collision"):
        asyncio.run(
            _locked_recipe(
                _RecipeDatabase(stored),
                '"mrf"."provider_directory_projection_recipe"',
                recipe.physical,
            )
        )


def test_claim_locked_recipe_rejects_terminal_failed_busy_and_unknown_states():
    recipe, _lease, _block, _admission, _shard = _workset()
    table = '"mrf"."provider_directory_projection_recipe"'
    token = digest("replacement")
    for status, expected_error in (
        ("sealed", "resolution_required"),
        ("retired", "not_reclaimable"),
        ("failed", "classification_required"),
    ):
        with pytest.raises(ProviderDirectoryProjectionError, match=expected_error):
            asyncio.run(
                _claim_locked_recipe(
                    _StatusDatabase(),
                    table,
                    recipe.physical,
                    _stored_recipe_fields(recipe.physical, status=status),
                    token,
                    900,
                )
            )

    class BusyDatabase(_StatusDatabase):
        async def scalar(self, _statement: str, **_parameters):
            return 0

    with pytest.raises(ProviderDirectoryProjectionBusy, match="recipe_busy"):
        asyncio.run(
            _claim_locked_recipe(
                BusyDatabase(),
                table,
                recipe.physical,
                _stored_recipe_fields(recipe.physical),
                token,
                900,
            )
        )


def test_expired_recipe_reclaim_is_fenced_to_replacement_token(monkeypatch):
    recipe, _lease, _block, _admission, _shard = _workset()
    actions = []

    async def record_action(_database, action, **fields):
        actions.append((action, fields))

    class ExpiredDatabase(_StatusDatabase):
        async def scalar(self, _statement: str, **_parameters):
            return 1

    monkeypatch.setattr(lease_module, "set_local_projection_action", record_action)
    token = digest("replacement")
    claim = asyncio.run(
        _claim_locked_recipe(
            ExpiredDatabase(),
            '"mrf"."provider_directory_projection_recipe"',
            recipe.physical,
            _stored_recipe_fields(recipe.physical),
            token,
            900,
        )
    )
    assert claim.lease.lease_token == token
    assert actions[0][0] == "recipe_reclaim"


def test_recipe_claim_and_heartbeat_validate_lease_bounds(monkeypatch):
    recipe, lease, _block, _admission, _shard = _workset()
    with pytest.raises(ProviderDirectoryProjectionError, match="lease_seconds_invalid"):
        asyncio.run(claim_projection_recipe(recipe, lease_seconds=29))
    with pytest.raises(ProviderDirectoryProjectionError, match="lease_seconds_invalid"):
        asyncio.run(heartbeat_projection_lease(lease, lease_seconds=86_401))

    async def accept_action(_database, _action, **_fields):
        return None

    monkeypatch.setattr(lease_module, "set_local_projection_action", accept_action)
    database = _StatusDatabase(result=1)
    asyncio.run(
        heartbeat_projection_lease(
            lease,
            lease_seconds=300,
            database=database,
        )
    )
    assert any("lease_heartbeat_at" in statement for statement in database.statements)

    lost_database = _StatusDatabase(result=0)
    with pytest.raises(ProviderDirectoryProjectionLeaseLost, match="lease_lost"):
        asyncio.run(
            heartbeat_projection_lease(
                lease,
                lease_seconds=300,
                database=lost_database,
            )
        )


def test_recipe_lease_stage_oid_is_preserved_as_an_integer(monkeypatch):
    recipe, _lease, _block, _admission, _shard = _workset()

    async def accept_action(_database, _action, **_fields):
        return None

    monkeypatch.setattr(lease_module, "set_local_projection_action", accept_action)
    fields = _stored_recipe_fields(
        recipe.physical,
        lease_token=digest("inserted"),
        stage_schema="mrf",
        stage_relation="projection_stage",
        stage_relation_oid="123",
    )
    claim = asyncio.run(
        _claim_locked_recipe(
            _StatusDatabase(),
            '"mrf"."provider_directory_projection_recipe"',
            recipe.physical,
            fields,
            digest("inserted"),
            900,
        )
    )
    assert claim.lease.stage_relation_oid == 123
    assert json.dumps(claim.lease.recipe.identity_payload, sort_keys=True)
