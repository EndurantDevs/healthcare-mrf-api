# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Host layout order and native-catalog transport tests; no PostgreSQL proof."""

import importlib
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

fhir = importlib.import_module("process.provider_directory_fhir")


@pytest.fixture
def recording(monkeypatch):
    statements, reservations, windows = [], [], []
    admission = SimpleNamespace(run_id="run_" + "a" * 32)
    monkeypatch.setattr(fhir, "_provider_directory_profile_capacity_admission", lambda: admission)
    monkeypatch.setattr(fhir, "_provider_directory_profile_control_operation_count", lambda *_: 29)

    async def reserve(actual, *, control_operation_counts):
        assert actual is admission
        reservations.append(control_operation_counts["artifact_scope_layout"])

    @asynccontextmanager
    async def window(relation):
        assert relation is None
        windows.append("enter")
        yield
        windows.append("exit")

    async def execute(_directory, operation, relation, statement):
        assert operation == "artifact_scope_layout" and relation.startswith('"fixture".')
        statements.append(statement)

    monkeypatch.setattr(fhir, "_reserve_provider_directory_profile_wal_budget", reserve)
    monkeypatch.setattr(fhir, "_profile_capacity_mutation_window", window)
    monkeypatch.setattr(fhir.profile_artifact_custody, "status", execute)
    return statements, reservations, windows


async def test_nine_heaps_precede_payload_and_all_twenty_index_statements(recording):
    statements, reservations, windows = recording
    plan = fhir._artifact_scope_materialization_plan("run_" + "a" * 32)
    await fhir._create_artifact_scope_layouts("fixture", plan)
    assert len(statements) == 9 and all(sql.startswith("CREATE UNLOGGED TABLE") for sql in statements)
    assert reservations == [9] and windows == ["enter", "exit"]
    assert plan.created_tables == [value for _key, value in sorted(plan.relation_by_table.items())]
    statements.append("all nine COPY payloads completed")
    await fhir._finish_artifact_scope_indexes("fixture", plan.relation_by_table, plan.model_by_table_name)
    assert statements[9] == "all nine COPY payloads completed"
    assert len(statements[10:]) == 20
    assert sum(sql.startswith("CREATE UNIQUE INDEX") for sql in statements[10:]) == 9
    assert sum(sql.startswith("ALTER TABLE") and "PRIMARY KEY USING INDEX" in sql for sql in statements[10:]) == 9
    assert sum(sql.startswith("CREATE INDEX") for sql in statements[10:]) == 2
    assert reservations == [9, 20] and sum(reservations) == 29
    assert windows == ["enter", "exit", "enter", "exit"]
    assert not any("IF NOT EXISTS" in sql or "ANALYZE" in sql for sql in statements)


async def test_index_failure_is_not_reported_complete(recording, monkeypatch):
    plan = fhir._artifact_scope_materialization_plan("run_" + "a" * 32)
    failure = OSError("synthetic index build refusal")
    monkeypatch.setattr(fhir.profile_artifact_custody, "status", AsyncMock(side_effect=failure))
    with pytest.raises(OSError) as caught:
        await fhir._finish_artifact_scope_indexes("fixture", plan.relation_by_table, plan.model_by_table_name)
    assert caught.value is failure
    assert recording[1] == [20] and recording[2] == ["enter"]


async def test_recovery_creation_is_heap_only_and_keeps_exact_cleanup_names(monkeypatch):
    plan = fhir._artifact_scope_materialization_plan("run_" + "b" * 32)
    status = AsyncMock()
    monkeypatch.setattr(fhir.profile_artifact_custody, "recovery_status", status)
    await fhir._create_recovery_artifact_scope_layouts("fixture", plan)
    assert len(status.await_args_list) == 9
    assert all(call.args[1].startswith("CREATE UNLOGGED TABLE") for call in status.await_args_list)
    assert set(plan.created_tables) == set(plan.relation_by_table.values())


@pytest.mark.parametrize("count", [True, 0, 10, 29])
async def test_phase_reservation_cannot_duplicate_full_signed_layout(recording, count):
    with pytest.raises(RuntimeError, match="layout_phase_invalid"):
        await fhir._reserve_artifact_scope_layout_wal(statement_count=count)
    assert recording[1] == []


def _native_indexes(phase):
    model = fhir.ProviderDirectoryPractitionerRole
    relation = "scratch_role"
    build, primary = fhir._artifact_scope_pk_names(relation)
    bucket, _ = fhir._provider_directory_profile_bucket_index_sql("fixture", relation)
    names = [] if phase == "heap" else [build if phase == "build" else primary]
    if phase == "bucket":
        names.append(bucket)
    indexes = [
        dict(relation_oid=100, index_name=name, index_oid=101 + n, reloptions=[]) for n, name in enumerate(names)
    ]
    constraints = (
        []
        if phase in {"heap", "build"}
        else [
            dict(
                relation_oid=100,
                constraint_type="p",
                condeferrable=False,
                condeferred=False,
                convalidated=True,
                connoinherit=True,
                constraint_definition="PRIMARY KEY (source_id, resource_id)",
            )
        ]
    )
    expression = (
        "mod(hashtextextended(resource_id::text, 0::bigint) & '9223372036854775807'::bigint, "
        f"{fhir.profile_artifact.PROFILE_AFFILIATION_ROLE_BUCKETS}::bigint)"
    )
    native_by_name = {}
    for index in indexes:
        is_bucket = index["index_name"] == bucket
        native_by_name[index["index_name"]] = dict(
            table_oid=100,
            table_kind="r",
            table_persistence="u",
            index_oid=index["index_oid"],
            index_am="btree",
            indnatts=2,
            indnkeyatts=2,
            indisunique=not is_bucket,
            indnullsnotdistinct=False,
            indisprimary=phase not in {"heap", "build"} and not is_bucket,
            indisexclusion=False,
            indimmediate=True,
            indisclustered=False,
            indisvalid=True,
            indcheckxmin=False,
            indisready=True,
            indislive=True,
            indisreplident=False,
            first_key="source_id",
            second_key=expression if is_bucket else "resource_id",
            index_predicate="",
        )
    return model, relation, indexes, constraints, native_by_name


@pytest.mark.parametrize("phase", ["heap", "build", "attached", "bucket"])
async def test_exact_prior_index_prefix_is_authenticated(monkeypatch, phase):
    model, relation, indexes, constraints, native_by_name = _native_indexes(phase)

    async def row(schema, table, name, _relation):
        assert schema == "fixture" and table == relation
        return native_by_name[name]

    monkeypatch.setattr(fhir, "_profile_bucket_index_row", row)
    await fhir._artifact_scope_recovery_index_phase("fixture", relation, 100, model, indexes, constraints)


@pytest.mark.parametrize("is_attached", [False, True])
@pytest.mark.parametrize("second_key", ["", None, "resource_id"])
def test_source_recovery_primary_key_requires_native_absent_second_key(is_attached, second_key):
    _model, _relation, _indexes, _constraints, native_by_name = _native_indexes("attached" if is_attached else "build")
    native = next(iter(native_by_name.values()))
    native.update(indnatts=1, indnkeyatts=1, second_key=second_key)
    if second_key == "":
        fhir._artifact_scope_recovery_pk_shape(native, fhir.ProviderDirectorySource, is_attached)
    else:
        with pytest.raises(fhir.ProviderDirectoryArtifactBuildStale, match="recovery_pk_changed"):
            fhir._artifact_scope_recovery_pk_shape(native, fhir.ProviderDirectorySource, is_attached)


@pytest.mark.parametrize(
    "fault", ["extra", "definition", "invalid", "constraint", "options", "oid", "bucket_before_pk"]
)
async def test_changed_prior_index_or_constraint_is_refused(monkeypatch, fault):
    model, relation, indexes, constraints, native_by_name = _native_indexes("bucket")
    if fault == "extra":
        indexes.append(dict(relation_oid=100, index_name="foreign", index_oid=999, reloptions=[]))
    elif fault == "constraint":
        constraints[0]["condeferrable"] = True
    elif fault == "options":
        indexes[0]["reloptions"] = ["fillfactor=80"]
    elif fault == "bucket_before_pk":
        indexes.pop(0)
        constraints.clear()
    else:
        first = native_by_name[indexes[0]["index_name"]]
        first[{"definition": "second_key", "invalid": "indisvalid", "oid": "table_oid"}[fault]] = {
            "definition": "other_id",
            "invalid": False,
            "oid": 999,
        }[fault]
    monkeypatch.setattr(
        fhir, "_profile_bucket_index_row", AsyncMock(side_effect=lambda _s, _t, name, _r: native_by_name[name])
    )
    with pytest.raises(fhir.ProviderDirectoryArtifactBuildStale):
        await fhir._artifact_scope_recovery_index_phase("fixture", relation, 100, model, indexes, constraints)


def test_actual_signed_control_ledger_accepts_nine_then_twenty_only():
    from tests.test_provider_directory_profile_control_capacity import _bound_control_wal_projection

    _geometry, projection = _bound_control_wal_projection()
    operations_by_name = {operation.operation_name: operation for operation in projection.operations}
    assert operations_by_name["artifact_scope_layout"].operation_count == 29
    assert operations_by_name["artifact_scope_analyze"].operation_count == 9
    tracker = SimpleNamespace(accounted_control_operation_counts={})
    counts, first_wal, _remaining = fhir._profile_control_wal_candidate(
        tracker, projection, {"artifact_scope_layout": 9}
    )
    tracker.accounted_control_operation_counts = counts
    counts, second_wal, _remaining = fhir._profile_control_wal_candidate(
        tracker, projection, {"artifact_scope_layout": 20}
    )
    assert counts == {"artifact_scope_layout": 29}
    assert first_wal + second_wal == 29 * operations_by_name["artifact_scope_layout"].wal_bytes_per_operation
    tracker.accounted_control_operation_counts = counts
    with pytest.raises(ValueError, match="completed_operation_exceeded"):
        fhir._profile_control_wal_candidate(tracker, projection, {"artifact_scope_layout": 1})
