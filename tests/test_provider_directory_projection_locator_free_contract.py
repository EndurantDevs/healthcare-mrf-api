# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Pure guards for retained-reader-bound projection inputs."""

from __future__ import annotations

from dataclasses import fields
import hashlib
import importlib.util
import inspect
from pathlib import Path

import pytest
import sqlalchemy as sa

from process.provider_directory_projection_contract import (
    projection_completeness_manifest,
    projection_input_block,
    projection_input_set_sha256,
)
from process.provider_directory_projection_types import (
    ProjectionCompletenessManifest,
    ProjectionInputBlock,
    ProviderDirectoryProjectionError,
)


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
MIGRATION_PATH = (
    REPOSITORY_ROOT
    / "alembic"
    / "versions"
    / "20260721170000_provider_directory_physical_projection.py"
)
CONTRACT_PATHS = (
    REPOSITORY_ROOT / "process/provider_directory_projection_types.py",
    REPOSITORY_ROOT / "process/provider_directory_projection_contract.py",
    MIGRATION_PATH,
)
BINDING_FIELDS = (
    "retained_campaign_id",
    "retained_campaign_sha256",
    "retained_source_item_id",
    "retained_range_ordinal",
)


def _digest(label: str) -> str:
    return hashlib.sha256(label.encode("utf-8")).hexdigest()


def _input_block(**overrides):
    input_map = {
        "block_ordinal": 0,
        "upstream_artifact_id": _digest("artifact"),
        "source_object_id": _digest("object"),
        "block_kind": "ndjson",
        "input_contract_id": "example.decoder-input.v1",
        "source_partition_ordinal": 0,
        "record_start": 0,
        "record_count": 2,
        "content_sha256": _digest("canonical-content"),
        "payload_sha256": _digest("retained-bytes"),
        "payload_bytes": 128,
        "summary": {"resource_count": 2},
        "retained_campaign_id": _digest("campaign-id"),
        "retained_campaign_sha256": _digest("campaign-proof"),
        "retained_source_item_id": _digest("source-item"),
        "retained_range_ordinal": 0,
    }
    input_map.update(overrides)
    return projection_input_block(**input_map)


def _completeness_manifest() -> ProjectionCompletenessManifest:
    return projection_completeness_manifest(
        endpoint_campaign_hash=_digest("campaign"),
        partition_strategy_contract_id="example.partition.v1",
        selected_resources=("Organization",),
        required_resources=(),
        terminal_partitions=(
            {
                "resource_type": "Organization",
                "partition_key_hash": _digest("partition"),
                "source_partition_ordinal": 0,
                "terminal": True,
                "block_count": 1,
                "row_count": 2,
                "byte_count": 128,
            },
        ),
        complete=True,
    )


def _load_migration():
    spec = importlib.util.spec_from_file_location(
        "provider_directory_physical_projection_migration",
        MIGRATION_PATH,
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize(
    "field_name",
    (
        "retained_campaign_id",
        "retained_campaign_sha256",
        "retained_source_item_id",
    ),
)
def test_retained_binding_digests_are_required(field_name: str) -> None:
    with pytest.raises(
        ProviderDirectoryProjectionError,
        match=f"provider_directory_projection_{field_name}_invalid",
    ):
        _input_block(**{field_name: "not-a-digest"})


@pytest.mark.parametrize("invalid_ordinal", (-1, True, 1.5, "0"))
def test_retained_range_ordinal_is_nullable_or_nonnegative(invalid_ordinal) -> None:
    with pytest.raises(
        ProviderDirectoryProjectionError,
        match="provider_directory_projection_retained_range_ordinal_invalid",
    ):
        _input_block(retained_range_ordinal=invalid_ordinal)

    assert _input_block(retained_range_ordinal=None).retained_range_ordinal is None
    assert _input_block(retained_range_ordinal=0).retained_range_ordinal == 0


def test_equivalent_retained_binding_does_not_change_content_proofs() -> None:
    first = _input_block()
    rebound = _input_block(
        retained_campaign_id=_digest("replacement-campaign-id"),
        retained_campaign_sha256=_digest("replacement-campaign-proof"),
        retained_source_item_id=_digest("replacement-source-item"),
        retained_range_ordinal=None,
    )

    assert tuple(getattr(first, name) for name in BINDING_FIELDS) != tuple(
        getattr(rebound, name) for name in BINDING_FIELDS
    )
    assert rebound.block_id == first.block_id
    assert rebound.block_proof_sha256 == first.block_proof_sha256
    first_set = projection_input_set_sha256(
        (first,),
        decoder_contract_id="example.decoder.v1",
        completeness_manifest=_completeness_manifest(),
    )
    rebound_set = projection_input_set_sha256(
        (rebound,),
        decoder_contract_id="example.decoder.v1",
        completeness_manifest=_completeness_manifest(),
    )
    assert rebound_set == first_set


def test_projection_input_contract_has_no_storage_address_surface() -> None:
    assert tuple(field.name for field in fields(ProjectionInputBlock))[-5:-1] == (
        *BINDING_FIELDS,
    )
    assert set(inspect.signature(projection_input_block).parameters).issuperset(
        BINDING_FIELDS
    )
    combined_source = "\n".join(
        source_path.read_text(encoding="utf-8") for source_path in CONTRACT_PATHS
    ).lower()
    assert "locator" not in combined_source
    assert "path" not in combined_source
    assert "urllib.parse" not in combined_source


def test_migration_declares_scalar_retained_binding_checks(monkeypatch) -> None:
    migration = _load_migration()
    recorded_table_by_name = {}

    def record_table(_operations, table_name, *elements, schema=None):
        recorded_table_by_name[table_name] = (elements, schema)

    monkeypatch.setattr(migration, "create_table_or_validate", record_table)
    monkeypatch.setattr(migration, "create_index_if_missing", lambda *_a, **_k: None)
    monkeypatch.setattr(
        migration,
        "_create_partitioned_resource_table",
        lambda _schema: None,
    )
    monkeypatch.setattr(
        migration,
        "_create_partitioned_membership_table",
        lambda _schema: None,
    )
    monkeypatch.setattr(
        migration,
        "_create_partitioned_profile_contribution_table",
        lambda _schema: None,
    )

    migration._create_tables("mrf")

    elements, schema = recorded_table_by_name[
        "provider_directory_projection_input_block"
    ]
    column_by_name = {
        element.name: element
        for element in elements
        if isinstance(element, sa.Column)
    }
    assert schema == "mrf"
    for digest_field in BINDING_FIELDS[:3]:
        assert column_by_name[digest_field].nullable is False
        assert column_by_name[digest_field].type.length == 64
    assert column_by_name["retained_range_ordinal"].nullable is True
    assert "storage_locator_json" not in column_by_name

    checks = migration._CHECKS_BY_TABLE[
        "provider_directory_projection_input_block"
    ]
    digest_check = checks["pd_projection_input_block_digest_check"]
    values_check = checks["pd_projection_input_block_values_check"]
    for digest_field in BINDING_FIELDS[:3]:
        assert f"{digest_field} ~ '^[0-9a-f]{{64}}$'" in digest_check
    assert "retained_range_ordinal IS NULL" in values_check
    assert "retained_range_ordinal >= 0" in values_check


def test_migration_refresh_gate_can_change_only_retained_binding(monkeypatch) -> None:
    migration = _load_migration()

    class _OperationRecorder:
        def __init__(self) -> None:
            self.statements = []

        def execute(self, statement) -> None:
            self.statements.append(str(statement))

    recorder = _OperationRecorder()
    monkeypatch.setattr(migration, "op", recorder)
    monkeypatch.setattr(migration, "_validate_trigger", lambda *_args: None)

    migration._create_fences("mrf")

    guard_sql = next(
        statement
        for statement in recorder.statements
        if "guard_provider_directory_projection_input_block()" in statement
        and "RETURNS trigger" in statement
    )
    for fence_fragment in (
        "healthporta.provider_directory_projection_action",
        "healthporta.provider_directory_projection_recipe_id",
        "healthporta.provider_directory_projection_recipe_attempt",
        "healthporta.provider_directory_projection_recipe_lease_token",
        "action_setting = 'retained_rebind'",
        "provider_directory_retained_artifact_consumer_reference",
        "provider_directory_retained_artifact_range",
        "FOR SHARE OF campaign, consumer, reference, campaign_item",
    ):
        assert fence_fragment in guard_sql
    immutable_fields = (
        "recipe_id",
        "block_id",
        "block_ordinal",
        "upstream_artifact_id",
        "source_object_id",
        "block_kind",
        "input_contract_id",
        "source_partition_ordinal",
        "record_start",
        "record_count",
        "content_sha256",
        "payload_sha256",
        "payload_bytes",
        "summary_json",
        "block_proof_sha256",
        "created_at",
    )
    for field_name in immutable_fields:
        assert f"OLD.{field_name} = NEW.{field_name}" in guard_sql
    for field_name in BINDING_FIELDS:
        assert f"OLD.{field_name} = NEW.{field_name}" not in guard_sql
        assert f"NEW.{field_name}" in guard_sql
    assert migration.down_revision == (
        "20260721160000_provider_directory_retained_artifact_acquisition"
    )
