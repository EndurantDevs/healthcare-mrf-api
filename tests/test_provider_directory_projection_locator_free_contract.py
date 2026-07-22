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

from process import provider_directory_physical_projection as facade
from process.provider_directory_projection_contract import (
    projection_admission_input_block,
    projection_completeness_manifest,
    projection_input_block,
    projection_input_set_sha256,
)
from process.provider_directory_projection_types import (
    ProjectionAdmissionInputBlock,
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
ADMISSION_BINDING_FIELDS = (
    "retained_campaign_id",
    "retained_campaign_sha256",
    "retained_source_item_id",
    "retained_range_ordinal",
)
ADMISSION_IDENTITY_FIELDS = (
    "retained_campaign_id",
    "retained_campaign_sha256",
    "retained_consumer_recipe_id",
    "claim_generation",
)
PROJECTION_INPUT_BLOCK_TABLE = "provider_directory_projection_input_block"
RETAINED_ACQUISITION_REVISION = (
    "20260721160000_provider_directory_retained_artifact_acquisition"
)


def _digest(label: str) -> str:
    return hashlib.sha256(label.encode("utf-8")).hexdigest()


def _input_block(**overrides):
    input_map = {
        "upstream_artifact_id": _digest("artifact"),
        "source_object_id": _digest("object"),
        "block_kind": "ndjson",
        "input_contract_id": "example.decoder-input.v1",
        "record_start": 0,
        "record_count": 2,
        "content_sha256": _digest("canonical-content"),
        "payload_sha256": _digest("retained-bytes"),
        "payload_bytes": 128,
        "summary": {"resource_count": 2},
    }
    input_map.update(overrides)
    return projection_input_block(**input_map)


def _admission_block(block=None, **overrides):
    binding_map = {
        "retained_campaign_id": _digest("campaign-id"),
        "retained_campaign_sha256": _digest("campaign-proof"),
        "retained_source_item_id": _digest("source-item"),
        "retained_range_ordinal": 0,
        "stream_identity_sha256": _digest("stream-item"),
        "sequence_ordinal": 0,
        "resource_type": "Organization",
        "partition_key_hash": _digest("partition"),
        "source_partition_ordinal": 0,
    }
    binding_map.update(overrides)
    return projection_admission_input_block(block or _input_block(), **binding_map)


def _completeness_manifest() -> ProjectionCompletenessManifest:
    return projection_completeness_manifest(
        endpoint_campaign_hash=_digest("campaign-proof"),
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
        _admission_block(**{field_name: "not-a-digest"})


@pytest.mark.parametrize("invalid_ordinal", (-1, True, 1.5, "0"))
def test_retained_range_ordinal_is_nullable_or_nonnegative(invalid_ordinal) -> None:
    with pytest.raises(
        ProviderDirectoryProjectionError,
        match="provider_directory_projection_admission_coordinate_invalid",
    ):
        _admission_block(retained_range_ordinal=invalid_ordinal)

    assert _admission_block(retained_range_ordinal=None).retained_range_ordinal is None
    assert _admission_block(retained_range_ordinal=0).retained_range_ordinal == 0


def test_equivalent_retained_binding_does_not_change_content_proofs() -> None:
    physical_block = _input_block()
    first = _admission_block(physical_block)
    rebound = _admission_block(
        physical_block,
        retained_campaign_id=_digest("replacement-campaign-id"),
        retained_campaign_sha256=_digest("replacement-campaign-proof"),
        retained_source_item_id=_digest("replacement-source-item"),
        retained_range_ordinal=None,
    )

    assert tuple(getattr(first, name) for name in ADMISSION_BINDING_FIELDS) != tuple(
        getattr(rebound, name) for name in ADMISSION_BINDING_FIELDS
    )
    assert rebound.block.block_id == first.block.block_id
    assert rebound.block.block_proof_sha256 == first.block.block_proof_sha256
    first_set = projection_input_set_sha256(
        (first.block,),
        decoder_contract_id="example.decoder.v1",
    )
    rebound_set = projection_input_set_sha256(
        (rebound.block,),
        decoder_contract_id="example.decoder.v1",
    )
    assert rebound_set == first_set


def test_projection_input_contract_has_no_storage_address_surface() -> None:
    physical_fields = {field.name for field in fields(ProjectionInputBlock)}
    admission_fields = {
        field.name for field in fields(ProjectionAdmissionInputBlock)
    }
    assert not physical_fields.intersection(ADMISSION_BINDING_FIELDS)
    assert set(ADMISSION_BINDING_FIELDS).issubset(admission_fields)
    assert not set(inspect.signature(projection_input_block).parameters).intersection(
        ADMISSION_BINDING_FIELDS
    )
    assert set(
        inspect.signature(projection_admission_input_block).parameters
    ).issuperset(ADMISSION_BINDING_FIELDS)
    combined_source = "\n".join(
        source_path.read_text(encoding="utf-8") for source_path in CONTRACT_PATHS
    ).lower()
    assert "storage_locator" not in combined_source
    assert "urllib.parse" not in combined_source


def test_public_facade_exports_physical_and_admission_split() -> None:
    for export_name in (
        "PhysicalProjectionRecipeIdentity",
        "ProjectionAdmissionInputBlock",
        "projection_admission_input_block",
        "validated_physical_projection_recipe_identity",
    ):
        assert export_name in facade.__all__
        assert getattr(facade, export_name) is not None


def test_migration_declares_scalar_retained_binding_checks(monkeypatch) -> None:
    """Keep retained coordinates out of source-neutral core relations."""

    migration = _load_migration()
    recorded_table_by_name = {}

    def record_table(_operations, table_name, *elements, schema=None):
        recorded_table_by_name[table_name] = (elements, schema)

    monkeypatch.setattr(migration, "create_table_or_validate", record_table)
    monkeypatch.setattr(migration, "create_index_if_missing", lambda *_a, **_k: None)
    monkeypatch.setattr(
        migration, "_create_partitioned_resource_table", lambda _schema: None
    )

    migration._create_tables("mrf")

    core_elements, schema = recorded_table_by_name[PROJECTION_INPUT_BLOCK_TABLE]
    core_column_by_name = {
        element.name: element
        for element in core_elements
        if isinstance(element, sa.Column)
    }
    assert schema == "mrf"
    assert not set(ADMISSION_BINDING_FIELDS).intersection(core_column_by_name)
    assert "storage_locator_json" not in core_column_by_name

    admission_elements, _ = recorded_table_by_name[
        "provider_directory_projection_admission"
    ]
    admission_column_by_name = {
        element.name: element
        for element in admission_elements
        if isinstance(element, sa.Column)
    }
    assert set(ADMISSION_IDENTITY_FIELDS).issubset(admission_column_by_name)

    mapping_elements, _ = recorded_table_by_name[
        "provider_directory_projection_admission_input_block"
    ]
    mapping_column_by_name = {
        element.name: element
        for element in mapping_elements
        if isinstance(element, sa.Column)
    }
    for field_name in (
        "retained_campaign_id",
        "retained_consumer_recipe_id",
        "retained_source_item_id",
        "retained_artifact_sha256",
        "retained_layout_sha256",
        "retained_range_ordinal",
        "claim_generation",
        "resource_type",
        "partition_key_hash",
        "source_partition_ordinal",
    ):
        assert field_name in mapping_column_by_name
    assert mapping_column_by_name["retained_range_ordinal"].nullable is True
    assert "storage_locator_json" not in mapping_column_by_name


def test_migration_keeps_core_blocks_immutable_and_admissions_exact(monkeypatch) -> None:
    """Fence neutral blocks separately from exact retained admissions."""

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

    core_guard_sql = next(
        statement for statement in recorder.statements
        if "guard_provider_directory_projection_input_block()" in statement
        and "RETURNS trigger" in statement
    )
    for core_fragment in (
        "healthporta.provider_directory_projection_action",
        "healthporta.provider_directory_projection_recipe_id",
        "healthporta.provider_directory_projection_recipe_attempt",
        "healthporta.provider_directory_projection_recipe_lease_token",
        "action_setting = 'workset_register'",
        "provider_directory_projection_input_block_immutable",
    ):
        assert core_fragment in core_guard_sql
    assert "retained_rebind" not in core_guard_sql

    mapping_guard_sql = next(
        statement for statement in recorder.statements
        if "guard_provider_directory_projection_admission_block()" in statement
        and "RETURNS trigger" in statement
    )
    for mapping_fragment in (
        "action_setting = 'admission_map'",
        "provider_directory_projection_admission_mapping_is_exact",
        "provider_directory_projection_admission_block_immutable",
    ):
        assert mapping_fragment in mapping_guard_sql

    admission_guard_sql = next(
        statement for statement in recorder.statements
        if "guard_provider_directory_projection_admission()" in statement
        and "RETURNS trigger" in statement
    )
    for admission_fragment in (
        "action_setting <> 'admission_insert'",
        "action_setting = 'admission_reclaim'",
        "action_setting = 'admission_seal'",
        "provider_directory_projection_admission_identity_immutable",
    ):
        assert admission_fragment in admission_guard_sql
    assert migration.down_revision == RETAINED_ACQUISITION_REVISION
