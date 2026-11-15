# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Saved NUCC names bind generation to the selected partition without old IDs."""

import json
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import nucc_protected_control as control
from process import reference_family_archive as archive
from process import reference_family_result_generation as generation
from tests.test_nucc_protected_bootstrap import baseline
from tests.test_nucc_protected_control import publication, source_generation
from tests.test_nucc_snapshot_partition import parent_catalog
from tests.test_reference_named_generation import _migration


def _transition_row(*, legacy=False):
    saved, names, authority = baseline(legacy=legacy)
    serving = authority["serving_generation"]
    generation_by_field = dict(
        importer_id="nucc",
        local_lineage_id=authority["local_lineage_id"],
        local_generation=authority["local_generation"],
        **serving,
        relation_oids="opaque original edition",
        nucc_kind="p",
        nucc_baseline=json.dumps(saved),
    )
    return generation_by_field, names


@pytest.mark.parametrize("legacy", (False, True))
def test_partition_transition_uses_only_saved_child_and_full_logical_generation(legacy):
    migration = _migration("20261114180000_reference_family_named_generation.py")
    row, names = _transition_row(legacy=legacy)
    before = deepcopy(row)
    assert migration._nucc_recorded_relation(row, "mrf") == names["child"]
    assert row == before


def test_ordinary_heap_transition_uses_explicit_configured_name_without_saved_catalog():
    migration = _migration("20261114180000_reference_family_named_generation.py")
    row, _ = _transition_row()
    row.update(nucc_kind="r", nucc_baseline=None)
    assert migration._nucc_recorded_relation(row, "serving") == ("serving", "nucc_taxonomy")
    row["origin_lineage_id"] = None
    assert migration._nucc_recorded_relation(row, "serving") is None


@pytest.mark.parametrize(
    "fault",
    ("missing", "missing-name", "conflicting-name", "wrong-child", "counter", "lineage", "origin", "time", "kind"),
)
def test_partition_transition_refuses_missing_ambiguous_or_stale_saved_declaration(fault):
    migration = _migration("20261114180000_reference_family_named_generation.py")
    generation_by_field, _ = _transition_row()
    saved = json.loads(generation_by_field["nucc_baseline"])
    saved_changes_by_fault = {
        "missing-name": (("child",), None),
        "conflicting-name": (("child_schema",), "reference_family_archive_" + "b" * 32),
        "wrong-child": (("source_authority", "relations", 0, "schema_name"), "reference_family_archive_" + "b" * 32),
    }
    generation_changes_by_fault = {
        "counter": ("local_generation", generation_by_field["local_generation"] + 1),
        "lineage": ("local_lineage_id", "55555555-5555-4555-8555-555555555555"),
        "origin": ("origin_generation", generation_by_field["origin_generation"] + 1),
        "time": ("published_at", "2026-10-17T06:00:00Z"),
        "kind": ("nucc_kind", None),
    }
    if fault == "missing":
        saved = None
    elif fault in saved_changes_by_fault:
        path, replacement = saved_changes_by_fault[fault]
        parent = saved
        for field in path[:-1]:
            parent = parent[field]
        if replacement is None:
            del parent[path[-1]]
        else:
            parent[path[-1]] = replacement
    else:
        field, replacement = generation_changes_by_fault[fault]
        generation_by_field[field] = replacement
    generation_by_field["nucc_baseline"] = json.dumps(saved)
    with pytest.raises(RuntimeError, match="transition-legacy-nucc-generation"):
        migration._nucc_recorded_relation(generation_by_field, "mrf")


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", (False, True))
async def test_partition_capture_refuses_new_child_with_old_generation_before_ack(monkeypatch, drift):
    authority = generation._nucc_result_authority(source_generation(4))
    child = authority.relations[0]
    if drift:
        child = ("reference_family_archive_" + "b" * 32, child[1])
    monkeypatch.setattr(
        generation, "read_reference_family_result_generation_authority", AsyncMock(return_value=authority)
    )
    monkeypatch.setattr(generation, "require_nucc_native_predecessor_storage", AsyncMock(return_value={"child": child}))
    if drift:
        with pytest.raises(RuntimeError, match="selected child changed"):
            await generation.capture_reference_family_serving_generation(
                object(), importer_id="nucc", schema_name="mrf"
            )
    else:
        assert (
            await generation.capture_reference_family_serving_generation(
                object(), importer_id="nucc", schema_name="mrf"
            )
            == authority.serving_generation
        )


@pytest.mark.asyncio
async def test_ack_updates_child_name_and_complete_generation_in_one_cas(monkeypatch):
    receipt = publication()
    prior = generation._nucc_result_authority(receipt["source_predecessor"])
    monkeypatch.setattr(generation, "read_reference_family_result_generation_authority", AsyncMock(return_value=prior))
    session = SimpleNamespace(scalar=AsyncMock(return_value="nucc"))
    await control._project_generation(session, receipt)
    statement, parameters = session.scalar.await_args.args
    assert parameters["schema"] == receipt["child"][0]
    assert parameters["names"] == [receipt["child"][1]]
    assert parameters["prior"] == prior.local_generation and parameters["next"] == prior.local_generation + 1
    assert "local_lineage_id=CAST(:lineage AS uuid) AND local_generation=:prior" in str(statement)
    assert "serving_schema=:schema,serving_relation_names=CAST(:names AS text[])" in str(statement)
    assert "relation_oid" not in str(statement)


@pytest.mark.asyncio
async def test_generation_lock_refuses_switched_child_before_reading_prior_ack(monkeypatch):
    expected = source_generation(4)
    current = generation._nucc_result_authority(expected)
    monkeypatch.setattr(control, "_require_publisher_fence", AsyncMock())
    monkeypatch.setattr(control, "_require_source_controls", AsyncMock())
    ack = AsyncMock()
    monkeypatch.setattr(control, "_require_prior_owner_ack", ack)
    monkeypatch.setattr(
        generation, "read_reference_family_result_generation_authority", AsyncMock(return_value=current)
    )
    session = SimpleNamespace(scalar=AsyncMock(return_value=parent_catalog()))
    with pytest.raises(control.NuccProtectedControlError, match="prior Source projection is not ACKed"):
        await control._lock_generation_eligibility(
            session,
            expected,
            ("reference_family_archive_" + "b" * 32, "nucc_taxonomy"),
            {},
            {},
            publisher_session=object(),
        )
    ack.assert_not_awaited()


@pytest.mark.parametrize("field", ("source_authority", "source_predecessor"))
def test_new_receipt_cannot_bind_correct_counters_to_wrong_child(field):
    receipt = publication()
    receipt[field]["relations"][0]["schema_name"] = "reference_family_archive_" + "c" * 32
    with pytest.raises(control.NuccProtectedControlError, match="Source predecessor differs"):
        control.validate_publication(receipt)


def test_legacy_generation_without_recorded_name_requires_explicit_transition():
    value = source_generation(4)
    del value["relations"]
    value["relation_oids"] = "opaque original physical token"
    before = deepcopy(value)
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="transition-legacy-nucc-generation"):
        generation._nucc_result_authority(value)
    assert generation._nucc_result_authority(value, declared_schema="saved_snapshot").relations == (
        ("saved_snapshot", "nucc_taxonomy"),
    )
    assert value == before


def _legacy_publication():
    value = publication()
    value["child_schema"] = value["child"][0]
    for key in control._NAMED_FIELDS:
        del value[key]
    for key in control._LEGACY_PUBLICATION_FIELDS - value.keys():
        value[key] = "opaque original " + key
    for key in ("source_authority", "source_predecessor"):
        del value[key]["relations"]
        value[key]["relation_oids"] = "opaque original generation token"
    return value


def test_legacy_publication_read_preserves_raw_tokens_without_inventing_old_child():
    value = _legacy_publication()
    before = deepcopy(value)
    assert control.validate_publication(value) == before == value
    prior = generation._nucc_result_authority(value["source_predecessor"], legacy_read=True)
    assert prior.relations is None and prior.serving_generation is not None
    assert value == before


@pytest.mark.asyncio
async def test_legacy_unacknowledged_publication_without_recorded_predecessor_fails_transition(monkeypatch):
    value = _legacy_publication()
    current = generation._nucc_result_authority(source_generation(4))
    monkeypatch.setattr(
        generation, "read_reference_family_result_generation_authority", AsyncMock(return_value=current)
    )
    session = SimpleNamespace(scalar=AsyncMock())
    with pytest.raises(control.NuccProtectedControlError, match="transition-legacy-nucc-generation"):
        await control._project_generation(session, value)
    session.scalar.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "hooks",
    (
        [],
        ["cms_native_input_mutation"],
        ["reference_source_generation_revision_guard"],
        ["foreign"],
        ["cms_native_input_mutation", "foreign"],
    ),
)
async def test_predecessor_accepts_only_immutable_or_existing_semantic_counter_boundary(monkeypatch, hooks):
    from process import reference_source_generation as tracking

    monkeypatch.setattr(generation, "_require_sealed_nucc_storage", AsyncMock())
    storage = AsyncMock(return_value={"table_name": "nucc_taxonomy"})
    revision = AsyncMock()
    monkeypatch.setattr(generation, "require_immutable_nucc_storage", storage)
    monkeypatch.setattr(tracking, "require_reference_revision_tracking", revision)
    session = SimpleNamespace(scalar=AsyncMock(return_value=hooks))
    if hooks in ([], ["cms_native_input_mutation"]):
        assert await generation.require_nucc_native_predecessor_storage(session, schema_name="mrf") == {
            "table_name": "nucc_taxonomy"
        }
        storage.assert_awaited_once()
        revision.assert_not_awaited()
    elif hooks == [tracking.REVISION_TRIGGER]:
        await generation.require_nucc_native_predecessor_storage(session, schema_name="mrf")
        revision.assert_awaited_once_with(session, importer_id="nucc", schema_name="mrf")
    else:
        with pytest.raises(RuntimeError, match="hooks differ"):
            await generation.require_nucc_native_predecessor_storage(session, schema_name="mrf")
        storage.assert_not_awaited()
        revision.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("registered", (False, True))
async def test_named_native_hook_requires_existing_adoption_semantics_before_model_observation(monkeypatch, registered):
    from process import provider_directory_cms_native_registration as registration

    driver = object()
    monkeypatch.setattr(archive, "_native_model_copy_driver", AsyncMock(return_value=driver))
    capture = AsyncMock(return_value={"revision": 4} if registered else None)
    monkeypatch.setattr(registration, "capture_native_input_adoption", capture)
    columns, dependencies = AsyncMock(), AsyncMock()
    monkeypatch.setattr(archive, "_require_nucc_native_columns", columns)
    monkeypatch.setattr(archive, "require_native_read_catalog", dependencies)
    result = SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: []))
    session = SimpleNamespace(scalar=AsyncMock(side_effect=[registered, 42]), execute=AsyncMock(return_value=result))
    assert await archive._observe_nucc_native_stage(session, "mrf", "nucc_taxonomy", native_revision=True) == {
        "table_name": "nucc_taxonomy",
        "indexes": [],
    }
    if registered:
        capture.assert_awaited_once_with(
            driver,
            ledger_schema="mrf",
            candidate=registration.NativeInputCandidate("mrf", "nucc_taxonomy", "mrf", "nucc_taxonomy"),
        )
    else:
        capture.assert_not_awaited()
    assert session.scalar.await_args.args[1]["hook"] == ("cms_native_input_mutation" if registered else None)
    assert "tgname IS DISTINCT FROM :hook" in str(session.scalar.await_args.args[0])
    columns.assert_awaited_once_with(session, 42)


@pytest.mark.parametrize("missing", (False, True))
def test_real_transition_order_maps_saved_names_before_retiring_old_columns(monkeypatch, missing):
    migration = _migration("20261114180000_reference_family_named_generation.py")
    generation_by_field, names = _transition_row(legacy=True)
    if missing:
        generation_by_field["nucc_baseline"] = None
    before = deepcopy(generation_by_field)
    statements, reads = [], []

    def execute(statement, parameters):
        reads.append((str(statement), parameters))
        return SimpleNamespace(mappings=lambda: SimpleNamespace(one_or_none=lambda: generation_by_field))

    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "mrf")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    monkeypatch.setattr(migration.op, "get_bind", lambda: SimpleNamespace(execute=execute))
    monkeypatch.setattr(migration.op, "execute", statements.append)
    if missing:
        with pytest.raises(RuntimeError, match="transition-legacy-nucc-generation"):
            migration.upgrade()
        assert not any("DROP COLUMN" in str(statement) for statement in statements)
    else:
        migration.upgrade()
        mapping = next(
            statement
            for statement in statements
            if hasattr(statement, "compile") and statement.compile().params.get("importer") == "nucc"
        )
        assert mapping.compile().params == {
            "schema": names["child"][0],
            "names": [names["child"][1]],
            "importer": "nucc",
        }
        assert statements.index(mapping) < next(
            i for i, statement in enumerate(statements) if "DROP COLUMN relation_oids" in str(statement)
        )
    assert generation_by_field == before
    sql, parameters = reads[0]
    assert parameters == {"schema": "mrf"} and "FOR UPDATE OF g" in sql
    assert "relation_oids" not in sql and "relowner" not in sql and "relfilenode" not in sql
    assert "n.nspname=:schema" in sql and "c.relname='nucc_taxonomy'" in sql


@pytest.mark.asyncio
async def test_stage_retirement_locks_named_generation_before_native_drop(monkeypatch):
    from tests.test_reference_family_dictionary import _session
    from tests.test_reference_family_source_copy import _nucc_stage_receipt

    stage = _nucc_stage_receipt()
    session = _session()
    session.first = AsyncMock(
        return_value=dict(
            importer_id="nucc",
            local_lineage_id="11111111-1111-4111-8111-111111111111",
            local_generation=0,
            origin_lineage_id=None,
            origin_generation=None,
            published_at=None,
            serving_schema=None,
            serving_relation_names=None,
        )
    )
    metadata_queries = (
        'SELECT local_generation FROM "mrf"."reference_family_result_generation" WHERE FALSE',
        "SELECT attname FROM pg_catalog.pg_attribute WHERE attrelid=to_regclass(:relation) "
        "AND attnum>0 AND NOT attisdropped AND attname IN ('serving_schema','serving_relation_names')",
    )

    async def execute(statement, parameters=None):
        sql = str(statement)
        if sql == metadata_queries[0]:
            assert parameters == {}
            return SimpleNamespace(all=lambda: [])
        if sql == metadata_queries[1]:
            assert parameters == {"relation": '"mrf"."reference_family_result_generation"'}
            return SimpleNamespace(all=lambda: [("serving_schema",), ("serving_relation_names",)])
        assert sql.startswith("LOCK TABLE ONLY ") and sql.endswith(" IN ACCESS SHARE MODE NOWAIT")
        return None

    session.execute.side_effect = execute
    session.scalar.return_value = True
    monkeypatch.setattr(archive, "_nucc_native_stage", AsyncMock(return_value=stage["stage"]))
    assert await generation._require_nucc_stage_cleanup_storage(session, stage, None) == stage["stage"]
    assert str(session.first.await_args.args[0]).endswith("WHERE importer_id=:importer_id FOR UPDATE")
    assert session.first.await_args.kwargs == {"importer_id": "nucc"}
    queries = [str(call.args[0]) for call in session.execute.await_args_list]
    assert len(queries) == 4 and queries[1:3] == list(metadata_queries)
    assert all("IN ACCESS SHARE MODE NOWAIT" in sql for sql in (queries[0], queries[-1]))
    assert not any("UPDATE " in sql or "GRANT " in sql or "DROP " in sql for sql in queries)


@pytest.mark.asyncio
async def test_cleanup_digest_ignores_opaque_old_physical_tokens_and_preserves_raw_document(monkeypatch):
    from contextlib import asynccontextmanager

    from tests.test_reference_family_dictionary import _session
    from tests.test_reference_family_source_copy import (
        _nucc_completed_handoff,
        _nucc_owning_transaction,
        _nucc_stage_receipt,
    )
    from tests.test_reference_handoff_boundary import _cleanup_inputs

    @asynccontextmanager
    async def path(_session):
        yield

    monkeypatch.setattr(archive, "_nucc_catalog_search_path", path)
    for name in ("_require_nucc_cleanup_run", "_require_nucc_handoff_stage", "_require_nucc_cleanup_custody"):
        monkeypatch.setattr(archive, name, AsyncMock())
    digests = []
    for token in ("first original token", "second original token"):
        handoff = _nucc_completed_handoff(_nucc_stage_receipt(legacy=True))
        handoff["database_oid"] = token
        handoff["stage"]["relation_oid"] = token
        _, _, abandonment = _cleanup_inputs()
        abandonment["candidate_custody"] = {
            key: handoff[key]
            for key in (
                "schema_name",
                "attempt_id",
                "attempt_started_at",
                "handoff_sha256",
                "source_contract_sha256",
                "stage",
                "database_oid",
                "import_run_oid",
            )
        }
        before = deepcopy((handoff, abandonment))
        receipt = await archive.cleanup_nucc_native_handoff(
            _nucc_owning_transaction(_session()),
            handoff,
            abandonment=abandonment,
            assert_unreferenced=AsyncMock(return_value=True),
        )
        assert (handoff, abandonment) == before
        digests.append(receipt["abandonment_sha256"])
    assert digests[0] == digests[1]
