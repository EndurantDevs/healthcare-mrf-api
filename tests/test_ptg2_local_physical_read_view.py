# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Named LOCAL readers preserve content and logical publication boundaries."""

from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from process.ptg_parts import ptg2_physical_binding as native
from process.ptg_parts import result_archive_candidate_validation as validation
from tests.test_ptg2_local_preparation_authority import (
    _control_fixture,
    _ownership,
    _published_control_fixture,
    _stage_audit_identity,
)


def _view_fixture():
    binding, candidate, _scope, _initialized = _control_fixture()
    _binding, published, prior, _publication = _published_control_fixture()
    model_sha = native.local_data_model_digest()
    audit = _stage_audit_identity(candidate, binding, [{"source_key": 0, "raw_container_sha256": (b"r" * 32).hex()}])
    evidence_by_field = {
        "contract": "ptg_result.postgres.v2",
        "metadata_sha256": "a" * 64,
        "ownership": {
            "importer_id": native.local_data_family_spec().importer_id,
            "dataset_id": str(binding.dataset_id),
            "schema_name": binding.schema_name,
            "relations": [{"schema": schema, "name": name} for schema, name in binding.relations],
            "sequences": [
                {"schema": schema, "name": name, "owner_table": table, "owner_column": column}
                for schema, name, table, column in binding.sequences
            ],
        },
        "initialization": {
            "destination_snapshot_id": binding.snapshot_id,
            "destination_import_run_id": published["import_run_id"],
            "destination_layout_key": binding.destination_layout_key,
        },
        "data": {
            "model_sha256": model_sha,
            "payload_snapshot_id": binding.payload_snapshot_id,
            "payload_snapshot_key": binding.payload_snapshot_key,
        },
        "control_sha256": prior["control_sha256"],
        "native_audit": {
            "contract": "ptg-local-data.native-set-audit.v1",
            "model_sha256": model_sha,
            "identity": audit["identity"],
        },
        "activation_evidence": {**prior["activation_evidence"], "control_sha256": prior["control_sha256"]},
    }
    return binding, {
        "contract": "ptg.prepared-physical-binding-read.v1",
        "destination_snapshot_id": binding.snapshot_id,
        "package_id": "a" * 64,
        "manifest_sha256": "a" * 64,
        "artifact_sha256": None,
        "native_validation": evidence_by_field,
        "relation_inventory": [
            {"ordinal": ordinal, "schema_name": schema, "relation_name": name}
            for ordinal, (schema, name) in enumerate(binding.relations)
        ],
        "sequence_inventory": [
            {
                "ordinal": ordinal,
                "schema_name": schema,
                "sequence_name": name,
                "owner_table": table,
                "owner_column": column,
            }
            for ordinal, (schema, name, table, column) in enumerate(binding.sequences)
        ],
    }


def _installed_fixture():
    binding, row = _view_fixture()
    _binding, control, _evidence, publication = _published_control_fixture()
    row.update(
        contract="ptg.installed-physical-binding-read.v1",
        generation_id=str(UUID(int=3)),
        publication_fence=str(UUID(int=4)),
        publication_sha256="d" * 64,
        generation_inventory={
            "relations": [{"schema_name": schema, "relation_name": name} for schema, name in binding.relations]
        },
        native_publication=publication,
    )
    return binding, row, control


def _legacy_view_fixture(is_prepared):
    binding, row_by_field = _view_fixture() if is_prepared else _installed_fixture()[:2]
    row_by_field = deepcopy(row_by_field)
    legacy = _ownership(binding)
    row_by_field.update(owner_oid=102, validation_sha256="b" * 64, inventory_sha256="c" * 64)
    evidence = row_by_field["native_validation"]
    evidence["ownership"] = {
        "importer_id": legacy.importer_id,
        "dataset_id": str(legacy.dataset_id),
        "schema_name": legacy.schema_name,
        "schema_oid": legacy.schema_oid,
        "relation_oids": [list(pair) for pair in legacy.relation_oids],
        "sequence_oids": [list(entry) for entry in legacy.sequence_oids],
    }
    evidence["catalog_sha256"] = evidence["native_audit"]["catalog_sha256"] = "e" * 64
    row_by_field["relation_inventory"] = [
        {"ordinal": ordinal, "table_name": name, "relation_oid": oid}
        for ordinal, (name, oid) in enumerate(legacy.relation_oids)
    ]
    row_by_field["sequence_inventory"] = [
        {"ordinal": ordinal, "sequence_name": name, "sequence_oid": oid, "owner_table": table, "owner_column": column}
        for ordinal, (name, oid, table, column) in enumerate(legacy.sequence_oids)
    ]
    if not is_prepared:
        row_by_field["registration_sha256"] = "f" * 64
        row_by_field["generation_inventory"] = {
            "database_oid": 88,
            "relations": [
                {
                    "schema_name": binding.schema_name,
                    "relation_name": name,
                    "relation_oid": oid,
                    "schema_oid": 101,
                    "schema_owner_oid": 102,
                    "owner_oid": 102,
                    "relfilenode": 700 + ordinal,
                }
                for ordinal, (name, oid) in enumerate(legacy.relation_oids)
            ],
        }
        publication = row_by_field["native_publication"]
        for field in ("schema_name", "relations", "sequences"):
            publication.pop(field)
        publication.update(
            schema_oid=101,
            owner_oid=102,
            relation_oids=[list(pair) for pair in legacy.relation_oids],
            sequence_oids=[list(entry) for entry in legacy.sequence_oids],
        )
    return binding, row_by_field


@pytest.mark.parametrize("is_prepared", [False, True])
def test_known_legacy_read_projection_matches_named_content(is_prepared):
    binding, legacy = _legacy_view_fixture(is_prepared)
    expected = (_view_fixture() if is_prepared else _installed_fixture())[1]
    resolved, normalized = validation._local_read_authority_binding(
        legacy, binding.snapshot_id, is_prepared=is_prepared
    )
    assert resolved == binding
    assert normalized == expected
    assert set(normalized["native_validation"]["native_audit"]["identity"]) == native._LOCAL_AUDIT_IDENTITY_FIELDS


@pytest.mark.parametrize("is_prepared", [False, True])
def test_copied_legacy_metadata_ignores_only_retired_physical_values(is_prepared):
    binding, legacy = _legacy_view_fixture(is_prepared)
    before = validation._local_read_authority_binding(legacy, binding.snapshot_id, is_prepared=is_prepared)
    legacy["owner_oid"] = "retired"
    legacy["validation_sha256"] = legacy["inventory_sha256"] = None
    evidence = legacy["native_validation"]
    evidence["ownership"]["schema_oid"] = "different_database"
    evidence["ownership"]["relation_oids"] = [[name, None] for name, _oid in evidence["ownership"]["relation_oids"]]
    evidence["catalog_sha256"] = evidence["native_audit"]["catalog_sha256"] = None
    assert validation._local_read_authority_binding(legacy, binding.snapshot_id, is_prepared=is_prepared) == before


@pytest.mark.parametrize(
    "fault", ["contract", "missing", "duplicate", "schema", "ordinal", "unknown", "model", "selection"]
)
def test_named_read_rejects_incomplete_names_or_changed_content(fault):
    binding, row = _view_fixture()
    match fault:
        case "contract":
            row["contract"] = "unknown"
        case "missing":
            row["relation_inventory"].pop()
        case "duplicate":
            row["relation_inventory"][-1]["relation_name"] = row["relation_inventory"][0]["relation_name"]
        case "schema":
            row["relation_inventory"][0]["schema_name"] = "other"
        case "ordinal":
            row["relation_inventory"][0]["ordinal"] = True
        case "unknown":
            row["extra"] = None
        case "model":
            row["native_validation"]["data"]["model_sha256"] = "0" * 64
        case "selection":
            row["native_validation"]["native_audit"]["identity"].pop("provider_identifier_quarantine_evidence")
    with pytest.raises(native.PTG2PhysicalBindingError):
        validation._local_read_authority_binding(row, binding.snapshot_id, is_prepared=True)


@pytest.mark.parametrize(
    "field,value,refuses",
    [
        ("artifact_sha256", None, False),
        ("artifact_sha256", "a" * 64, False),
        ("artifact_sha256", "", True),
        ("manifest_sha256", None, True),
        ("publication_sha256", "g" * 64, True),
    ],
)
def test_named_content_checksums_keep_transport_artifact_optional(field, value, refuses):
    binding, row, _control = _installed_fixture()
    row[field] = value
    if refuses:
        with pytest.raises(native.PTG2PhysicalBindingError):
            validation._local_read_authority_binding(row, binding.snapshot_id, is_prepared=False)
    else:
        assert validation._local_read_authority_binding(row, binding.snapshot_id, is_prepared=False)[0] == binding


@pytest.mark.asyncio
@pytest.mark.parametrize("does_refuse", [False, True])
async def test_called_read_pins_names_then_checks_logical_postimage(monkeypatch, does_refuse):
    binding, row_by_field = _view_fixture()
    statements = []

    async def execute(query, parameters=None):
        sql = str(query)
        statements.append(sql)
        rows = [row_by_field] if sql.startswith("SELECT *") else []
        return SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: rows))

    async def control(_session, _schema, selected, *, lock_controls):
        assert len([sql for sql in statements if sql.startswith("LOCK TABLE")]) == len(binding.relations)
        assert selected == binding and lock_controls is False
        return {"snapshot_id": binding.snapshot_id}

    monkeypatch.setattr(native, "_local_candidate_control", control)

    def check(candidate, *_args):
        if does_refuse:
            raise native.PTG2PhysicalBindingError("postimage drift")
        return candidate

    monkeypatch.setattr(native, "_require_local_control_postimage", check)
    session = SimpleNamespace(execute=execute, in_transaction=lambda: True, info={})
    if does_refuse:
        with pytest.raises(native.PTG2PhysicalBindingError, match="postimage drift"):
            await validation.local_data_physical_read_state(session, binding.snapshot_id, is_prepared=True)
        assert session.info == {}
    else:
        read_state = await validation.local_data_physical_read_state(session, binding.snapshot_id, is_prepared=True)
        assert read_state[2] == binding
        assert session.info == {"ptg2_local_read_bindings": {binding.schema_name: binding}}
    assert all("pg_class" not in sql and "pg_roles" not in sql and "FOR SHARE" not in sql for sql in statements)


@pytest.mark.asyncio
async def test_installed_api_reader_uses_actual_payload_schema_and_preserved_key(monkeypatch):
    """The called API branch reuses strict manifest parsing after genuine installed resolution."""
    from api import ptg2_tables
    from tests.ptg2_manifest_tables_support import (
        FakeResult,
        strict_snapshot_row,
        strict_source_identity_rows,
        strict_v4_root_row,
        strict_v4_serving_index,
    )

    physical_binding, _authority = _view_fixture()
    serving_index = strict_v4_serving_index(physical_binding.payload_snapshot_key)
    snapshot_by_field = strict_snapshot_row(serving_index, has_local_physical_binding=True)
    candidate_by_field = {
        "manifest": {"serving_index": serving_index},
        "layout_manifest": {"serving_index": serving_index},
        "plan_id": snapshot_by_field["snapshot_plan_id"],
        "plan_market_type": snapshot_by_field["snapshot_plan_market_type"],
        "coverage_scope_id": bytes.fromhex(snapshot_by_field["snapshot_coverage_scope_id"]),
        **{key: field_value for key, field_value in snapshot_by_field.items() if key.startswith("attested_")},
    }
    resolver = AsyncMock(return_value=({}, {}, physical_binding, candidate_by_field))
    monkeypatch.setattr(validation, "local_data_physical_read_state", resolver)
    finalizer = AsyncMock()
    monkeypatch.setattr(ptg2_tables, "_validate_v4_finalizer_map_root", finalizer)
    calls = []

    async def execute(query, parameters_by_name):
        sql = str(query)
        calls.append((sql, parameters_by_name))
        if "LIMIT 257" in sql:
            return SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: strict_source_identity_rows()))
        return FakeResult(strict_v4_root_row(serving_index) if "root.representation" in sql else snapshot_by_field)

    tables = await ptg2_tables.snapshot_serving_tables(SimpleNamespace(execute=execute), physical_binding.snapshot_id)
    assert (
        tables.physical_binding == physical_binding
        and tables.shared_snapshot_key == physical_binding.payload_snapshot_key
    )
    resolver.assert_awaited_once_with(SimpleNamespace(execute=execute), physical_binding.snapshot_id, is_prepared=False)
    source_query = next((sql, parameters) for sql, parameters in calls if "LIMIT 257" in sql)
    assert physical_binding.relation("ptg2_v3_snapshot_source") in source_query[0]
    assert source_query[1]["snapshot_id"] == physical_binding.payload_snapshot_id
    assert f"{physical_binding.schema_name}.ptg2_v4_snapshot_map_root" in calls[-1][0]
    assert finalizer.await_args.kwargs["physical_binding"] == physical_binding


@pytest.mark.asyncio
@pytest.mark.parametrize("does_refuse", [False, True])
@pytest.mark.parametrize(
    "reader_name",
    ["fetch_snapshot_source_provenance", "fetch_snapshot_source_set_identity", "fetch_snapshot_source_set_metadata"],
)
async def test_local_provenance_reauthenticates_before_reading_selected_payload(monkeypatch, does_refuse, reader_name):
    """Source readers must authenticate the family before using its payload ID."""
    from api import ptg2_shared_blocks, ptg2_tables
    from tests.test_ptg2_shared_blocks_guards import _provenance_row

    physical_binding, _authority = _view_fixture()
    descriptor = SimpleNamespace(physical_binding=physical_binding)
    read = AsyncMock(
        side_effect=native.PTG2PhysicalBindingError("binding drift") if does_refuse else None, return_value=descriptor
    )
    monkeypatch.setattr(ptg2_tables, "read_serving_tables", read)
    execute = AsyncMock(return_value=[_provenance_row()])
    session = SimpleNamespace(execute=execute)
    access = object()
    arguments_by_name = {
        "schema_name": "mrf",
        "logical_snapshot_id": physical_binding.snapshot_id,
        "expected_source_count": 1,
        "serving_tables": descriptor,
        "candidate_audit_access": access,
    }
    if reader_name == "fetch_snapshot_source_provenance":
        arguments_by_name["source_keys"] = (0,)
    reader = getattr(ptg2_shared_blocks, reader_name)
    if does_refuse:
        with pytest.raises(native.PTG2PhysicalBindingError, match="binding drift"):
            await reader(session, **arguments_by_name)
        execute.assert_not_awaited()
    else:
        observed_read = await reader(session, **arguments_by_name)
        if reader_name == "fetch_snapshot_source_provenance":
            assert set(observed_read) == {0}
        elif reader_name == "fetch_snapshot_source_set_identity":
            assert observed_read[2] == ("2" * 64,)
        else:
            assert observed_read["source_count"] == 1
        sql, parameters_by_name = execute.await_args.args
        assert physical_binding.relation("ptg2_v3_snapshot_source") in str(sql)
        if reader_name == "fetch_snapshot_source_provenance":
            assert all(
                physical_binding.relation(table) in str(sql) for table in ("ptg2_source_trace_set", "ptg2_source_trace")
            )
        assert parameters_by_name["snapshot_id"] == physical_binding.payload_snapshot_id
    read.assert_awaited_once_with(
        session, physical_binding.snapshot_id, serving_tables=descriptor, candidate_audit_access=access
    )
