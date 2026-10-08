# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Host regressions for the shared bounded SOURCE copier; PostgreSQL custody is separate."""

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from process import mrf_address_publication as address
from process import reference_family_archive as archive
from process import reference_family_result_generation as generation
from process import tiger_captured_epoch as captured


def _nucc_stage_receipt():
    """Synthetic closed metadata exercises decoders, not native authority."""
    ctx_by_field = {
        "control_run_id": "synthetic",
        "context": {
            "_control_attempt_id": "synthetic:" + "1" * 32,
            "_control_attempt_started_at": "2026-10-01T00:00:00+00:00",
        },
    }
    run_id, attempt, started, suffix = archive._nucc_attempt(ctx_by_field)
    stage_by_field = {
        "contract": "nucc-native-stage.v2",
        "run_id": run_id,
        "attempt_id": attempt,
        "attempt_started_at": started,
        "schema_name": "mrf",
        "import_date": suffix,
        "node_id": "synthetic",
        "database_oid": 41,
        "import_run_oid": 42,
        "source_contract_sha256": "a" * 64,
        "stage": {
            "table_name": "nucc_taxonomy_" + suffix,
            "relation_oid": 43,
            "relfilenode": 44,
            "owner_oid": 45,
            "indexes": [],
        },
        "incumbent": generation.ReferenceFamilyResultGenerationAuthority(
            "nucc", "c8f27af1-56ba-4cda-82d8-0fc67650918f", 0, None, None
        ).as_dict(),
        "incumbent_relation_oid": 46,
    }
    stage_by_field["stage_sha256"] = archive.nucc_native_digest(stage_by_field)
    return stage_by_field


def test_nucc_v2_receipt_preserves_original_zero_and_exact_precreated_identity():
    stage_by_field = _nucc_stage_receipt()
    assert archive._nucc_precreated_stage_value(stage_by_field) == stage_by_field
    assert stage_by_field["incumbent"]["local_generation"] == 0
    assert stage_by_field["incumbent"]["serving_generation"] is None
    corrupt_by_field = {**stage_by_field, "incumbent_relation_oid": 47}
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="digest"):
        archive._nucc_precreated_stage_value(corrupt_by_field)
    foreign_authority_by_field = {
        **stage_by_field["incumbent"],
        "serving_generation": {
            "origin_lineage_id": "edfc559e-0067-43ed-b7fa-983fdb7077fe",
            "origin_generation": 9,
            "published_at": "2026-10-01T00:00:00Z",
        },
        "relation_oids": [46],
    }
    assert archive._nucc_result_authority(foreign_authority_by_field, allow_untracked=True).local_generation == 0
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="predecessor"):
        archive._nucc_result_authority(foreign_authority_by_field)


@pytest.mark.parametrize("importer_id", ["nucc", "label", "mrf"])
def test_nucc_zero_decoder_rejects_other_registered_family_identities(importer_id):
    authority_by_field = {**_nucc_stage_receipt()["incumbent"], "importer_id": importer_id}
    if importer_id == "nucc":
        assert archive._nucc_result_authority(authority_by_field, allow_untracked=True).as_dict() == authority_by_field
    else:
        with pytest.raises(archive.ReferenceFamilyArchiveError, match="predecessor differs"):
            archive._nucc_result_authority(authority_by_field, allow_untracked=True)


@pytest.mark.asyncio
async def test_owner_sealed_zero_still_requires_genuine_initial_source_adoption(monkeypatch):
    original = _nucc_stage_receipt()["incumbent"]
    authority = archive._nucc_result_authority(original, allow_untracked=True)
    monkeypatch.setattr(archive, "is_nucc_native_handoff_required", AsyncMock(return_value=True))
    monkeypatch.setattr(archive, "read_reference_family_result_generation_authority", AsyncMock(return_value=authority))
    monkeypatch.setattr(archive, "_relation_oid", AsyncMock(return_value=46))
    create = AsyncMock()
    monkeypatch.setattr(archive, "_precreate_nucc_attempt", create)
    stage_by_field = _nucc_stage_receipt()
    ctx_by_field = {
        "control_run_id": stage_by_field["run_id"],
        "context": {
            "_control_attempt_id": stage_by_field["attempt_id"],
            "_control_attempt_started_at": stage_by_field["attempt_started_at"],
        },
    }
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="untracked"):
        await archive.bind_nucc_native_attempt(object(), ctx_by_field, schema_name="mrf")
    create.assert_not_awaited()


@pytest.mark.asyncio
async def test_nucc_precreation_reserves_before_ddl_and_records_exact_oid(monkeypatch):
    stage_by_field = _nucc_stage_receipt()
    observed_operations = []

    async def scalar(statement, _parameters=None):
        query = str(statement)
        if query.startswith("UPDATE"):
            assert "jsonb_build_object(CAST(:slot AS text),CAST(:receipt AS jsonb))" in query
            observed_operations.append(
                ("reservation" if _parameters["slot"].endswith("reservation") else "receipt", query)
            )
            return stage_by_field["run_id"]
        return 41

    session = SimpleNamespace(
        scalar=scalar, connection=AsyncMock(return_value=SimpleNamespace(exec_driver_sql=AsyncMock()))
    )
    monkeypatch.setattr(
        archive,
        "_nucc_locked_attempt",
        AsyncMock(
            return_value={"finished_at": None, "metrics": {}, "node_id": "synthetic", "params": {"source": "official"}}
        ),
    )
    monkeypatch.setattr(generation, "_require_nucc_native_builder", AsyncMock())
    monkeypatch.setattr(archive, "_relation_oid", AsyncMock(side_effect=[42, None, 42]))

    async def create(*_args, **_kwargs):
        observed_operations.append(("DDL", "heap"))

    monkeypatch.setattr(archive, "_create_model_heaps", create)
    monkeypatch.setattr(archive, "_nucc_native_stage", AsyncMock(return_value=stage_by_field["stage"]))
    receipt = await archive._precreate_nucc_attempt(
        session,
        "mrf",
        stage_by_field["run_id"],
        stage_by_field["attempt_id"],
        stage_by_field["attempt_started_at"],
        stage_by_field["import_date"],
        incumbent=stage_by_field["incumbent"],
        incumbent_relation_oid=46,
    )
    assert [event[0] for event in observed_operations] == ["reservation", "DDL", "receipt"]
    assert archive._nucc_precreated_stage_value(receipt) == receipt
    assert all(
        "metrics->'nucc_native_stage' IS NULL" in query for event, query in observed_operations if event != "DDL"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status,late_reference,completed",
    [("running", False, False), ("failed", True, False), ("canceled", False, False), ("canceled", False, True)],
)
async def test_nucc_stage_cleanup_fences_active_runs_and_late_references(
    monkeypatch, status, late_reference, completed
):
    """Only actual abandoned, unreferenced custody can drop its exact empty or completed heap."""
    stage_by_field = _nucc_stage_receipt()
    run_by_field = _nucc_abandoned_run(stage_by_field, status)
    handoff_by_field = _nucc_completed_handoff(stage_by_field) if completed else None
    if completed:
        run_by_field["metrics"]["nucc_handoff"] = handoff_by_field
    physical_stage = handoff_by_field["stage"] if completed else stage_by_field["stage"]
    queries = []

    async def execute(statement, _parameters=None):
        queries.append(str(statement))
        return SimpleNamespace(mappings=lambda: SimpleNamespace(one_or_none=lambda: run_by_field))

    async def scalar(statement, _parameters=None):
        query = str(statement)
        if "obj_description" in query:
            return archive._canonical_json(stage_by_field).decode("ascii")
        if query.startswith("UPDATE"):
            assert "jsonb_build_object(CAST(:digest AS text),CAST(:receipt AS jsonb))" in query
        return stage_by_field["run_id"] if query.startswith("UPDATE") else "transaction"

    session = SimpleNamespace(in_transaction=lambda: True, execute=execute, scalar=scalar)
    monkeypatch.setattr(archive, "_require_nucc_native_location", AsyncMock())
    monkeypatch.setattr(archive, "_nucc_native_publisher_owner", AsyncMock(return_value=50))
    monkeypatch.setattr(archive, "_nucc_native_stage", AsyncMock(return_value=stage_by_field["stage"]))
    monkeypatch.setattr(archive, "_require_nucc_cleanup_custody", AsyncMock())
    check_handoff = AsyncMock()
    monkeypatch.setattr(archive, "_require_nucc_handoff_stage", check_handoff)
    assert_unreferenced = AsyncMock(side_effect=[True, not late_reference])
    if status == "running" or late_reference:
        with pytest.raises(archive.ReferenceFamilyArchiveError, match="abandoned|transaction"):
            await archive.cleanup_nucc_native_stage(
                session, stage_by_field, runtime_owner_oids=(45,), assert_unreferenced=assert_unreferenced
            )
        assert not any(query.startswith("DROP") for query in queries)
    else:
        receipt = await archive.cleanup_nucc_native_stage(
            session, stage_by_field, runtime_owner_oids=(45,), assert_unreferenced=assert_unreferenced
        )
        assert receipt["stage"] == physical_stage and receipt["physical_cleanup_completed"] is True
        assert check_handoff.await_count == int(completed)
        if completed:
            assert receipt["handoff_sha256"] == handoff_by_field["handoff_sha256"]
        assert [query for query in queries if query.startswith("DROP")] == [
            f'DROP TABLE "mrf"."{stage_by_field["stage"]["table_name"]}" RESTRICT'
        ]
        assert assert_unreferenced.await_count == 2


def _nucc_abandoned_run(stage_by_field, status):
    """Keep the genuine attempt fields explicit in cleanup scenarios."""
    return {
        "engine": "healthcare-mrf-api",
        "importer": "nucc",
        "node_id": "synthetic",
        "status": status,
        "finished_at": None if status == "running" else "2026-10-01T00:01:00Z",
        "error": None,
        "params": {},
        "progress": {
            "attempt_id": stage_by_field["attempt_id"],
            "attempt_started_at": stage_by_field["attempt_started_at"],
        },
        "metrics": {"nucc_native_stage": stage_by_field},
    }


def _nucc_completed_handoff(stage_by_field):
    """Pure decoder metadata; native storage remains independently checked."""
    handoff_by_field = {
        **{
            key: value
            for key, value in stage_by_field.items()
            if key not in {"contract", "stage_sha256", "incumbent_relation_oid"}
        },
        "contract": archive.NUCC_IMMUTABLE_HANDOFF_CONTRACT,
        "stage": {
            **stage_by_field["stage"],
            "indexes": [{"index_oid": 47, "relfilenode": 48, "definition": "synthetic metadata"}],
        },
        "row_count": 1,
        "precreated_stage": stage_by_field,
    }
    handoff_by_field["handoff_sha256"] = archive.nucc_native_digest(handoff_by_field)
    return archive.validate_nucc_native_handoff(handoff_by_field)


@pytest.mark.asyncio
@pytest.mark.parametrize("has_equal_sets", (True, False, None))
async def test_model_set_equality_uses_exact_payload_probe_and_reverse_keys(has_equal_sets):
    """Keep wide JSON out of joined storage without dropping missing/extra/changed-row checks."""
    model = archive.models.ProviderProfileSourceRecord
    session = SimpleNamespace(scalar=AsyncMock(return_value=has_equal_sets), in_transaction=lambda: True)
    assert await archive._is_model_table_equal(
        session,
        model,
        left_schema="synthetic_left",
        left_name="source_rows",
        right_schema="synthetic_right",
        right_name="published_rows",
        scope=("run_id", ("a" * 64,)),
    ) is (has_equal_sets is True)
    session.scalar.assert_awaited_once()
    query, parameters = session.scalar.await_args.args
    query = str(query)
    assert "FULL OUTER JOIN" not in query and " JOIN " not in query
    assert "IS DISTINCT FROM (SELECT ROW(" in query
    assert 'FROM (SELECT * FROM "synthetic_right"."published_rows" WHERE' in query
    assert 'r WHERE l."record_id"=r."record_id"))' in query
    assert 'WHERE NOT EXISTS(SELECT 1 FROM (SELECT * FROM "synthetic_left"."source_rows" WHERE' in query
    assert query.count('"run_id"=ANY(CAST(:scope_values AS text[]))') == 4
    assert parameters == {"scope_values": ["a" * 64]}
    for column in model.__table__.columns:
        cast = "::jsonb" if column.name in {"raw_payload", "normalized_payload", "match_evidence"} else ""
        assert f'l."{column.name}"{cast}' in query and f'r."{column.name}"{cast}' in query


@pytest.mark.asyncio
@pytest.mark.parametrize("model_name,column_name", (("Plan", "benefits"), ("PlanNPIRaw", "addresses")))
async def test_model_set_equality_uses_native_equality_for_json_arrays(model_name, column_name):
    """Preserve array shape/SQL NULLs while giving composite row comparison native equality."""
    session = SimpleNamespace(scalar=AsyncMock(return_value=True), in_transaction=lambda: True)
    assert await archive._is_model_table_equal(
        session,
        getattr(archive.models, model_name),
        left_schema="synthetic_left",
        left_name="source_rows",
        right_schema="synthetic_right",
        right_name="published_rows",
    )
    query = str(session.scalar.await_args.args[0])
    assert f'l."{column_name}"::jsonb[]' in query and f'r."{column_name}"::jsonb[]' in query
    assert "to_jsonb" not in query
    session.scalar.assert_awaited_once()


@pytest.mark.asyncio
async def test_model_set_equality_preserves_composite_keys_and_left_canonical_filter():
    """The exact same authenticated source filter applies to comparison and reverse containment."""
    model = archive.models.NPIAddress
    session = SimpleNamespace(scalar=AsyncMock(return_value=True), in_transaction=lambda: True)
    predicate = "WHERE canonical.\"type\"='primary'"
    assert await archive._is_model_table_equal(
        session,
        model,
        left_schema="synthetic_left",
        left_name="source_rows",
        right_schema="synthetic_right",
        right_name="published_rows",
        left_predicate=predicate,
    )
    query = str(session.scalar.await_args.args[0])
    keys = 'l."npi"=r."npi" AND l."type"=r."type" AND l."checksum"=r."checksum"'
    assert query.count(keys) == 2 and query.count(predicate) == 2
    assert query.count('FROM "synthetic_right"."published_rows"') == 2
    assert session.scalar.await_args.args[1] == {}


@pytest.mark.asyncio
async def test_model_set_equality_preserves_spatial_text_and_requires_primary_keys():
    """Keep the existing spatial representation and refuse models without exact lookup keys."""
    model = next(
        model for model in archive.reference_family_spec("tiger").model_types if model.__tablename__ == "zcta5"
    )
    session = SimpleNamespace(scalar=AsyncMock(return_value=True), in_transaction=lambda: True)
    assert await archive._is_model_table_equal(
        session,
        model,
        left_schema="synthetic_left",
        left_name="source_rows",
        right_schema="synthetic_right",
        right_name="published_rows",
    )
    query = str(session.scalar.await_args.args[0])
    assert 'l."the_geom"::text' in query and 'r."the_geom"::text' in query
    assert query.count('l."zcta5ce"=r."zcta5ce" AND l."statefp"=r."statefp"') == 2
    session.scalar.reset_mock()
    keyless = SimpleNamespace(__table__=SimpleNamespace(primary_key=SimpleNamespace(columns=())))
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="model primary key"):
        await archive._is_model_table_equal(
            session,
            keyless,
            left_schema="synthetic_left",
            left_name="source_rows",
            right_schema="synthetic_right",
            right_name="published_rows",
        )
    session.scalar.assert_not_awaited()


def _clone_case(monkeypatch, importer="mrf"):
    spec = archive.reference_family_spec(importer, canonical=importer == "mrf")
    events = []
    ownership = SimpleNamespace(importer_id=importer, dataset_id=uuid4())

    async def execute(statement, _parameters=None):
        events.append(str(statement.compile(dialect=archive.postgresql.dialect())))

    async def is_clone_equal(statement, _parameters=None):
        return "FULL OUTER JOIN" in str(statement)

    session = SimpleNamespace(execute=execute, scalar=is_clone_equal, in_transaction=lambda: True)
    monkeypatch.setattr(archive, "capture_reference_family_stage_ownership", AsyncMock(return_value=ownership))
    monkeypatch.setattr(archive, "_capture_model_family_ownership", AsyncMock(return_value=ownership))
    monkeypatch.setattr(archive, "_rebase_owned_sequences", AsyncMock())
    monkeypatch.setattr(archive, "_is_model_table_equal", AsyncMock(return_value=True))
    capture = archive.ReferenceFamilySourceCapture(
        SimpleNamespace(
            importer_id=importer,
            tables=[SimpleNamespace(table_name=name) for name in spec.table_names],
            as_dict=lambda: {
                "importer_id": importer,
                "contract": archive.TYPED_MRF_CONTRACT if importer == "mrf" else archive.CONTRACT,
                "tables": [SimpleNamespace(table_name=name) for name in spec.table_names],
            },
        ),
        "synthetic_source",
        "00000001-00000001-1",
    )
    return session, capture, ownership, events


@pytest.mark.asyncio
@pytest.mark.parametrize("importer", ["mrf", "mrf-address", "tiger"])
async def test_ordinary_heaps_preserve_every_declared_owned_sequence_without_indexes(importer):
    spec = archive.reference_family_spec(importer)
    originals = [
        str(archive.CreateTable(model.__table__).compile(dialect=archive.postgresql.dialect()))
        for model in spec.model_types
    ]
    session = SimpleNamespace(execute=AsyncMock())
    await archive._create_model_heaps(session, spec, "synthetic_source", create_indexes=False, ordinary_heaps=True)
    statements = [str(call.args[0]) for call in session.execute.await_args_list]
    definitions = "\n".join(statements)
    for sequence, table, column in archive._OWNED_SEQUENCES[importer]:
        if importer == "tiger":
            assert sequence in definitions and f'"{table}"."{column}"' in definitions
        else:
            assert f"{column} {'BIGSERIAL' if column == 'evidence_checksum' else 'SERIAL'}" in definitions
    assert not any("CREATE INDEX" in sql or "PRIMARY KEY" in sql or "UNIQUE" in sql for sql in statements)
    assert [
        str(archive.CreateTable(model.__table__).compile(dialect=archive.postgresql.dialect()))
        for model in spec.model_types
    ] == originals


@pytest.mark.asyncio
@pytest.mark.parametrize("entrypoint", ["clone", "prepare", "convenience", "captured-tiger"])
async def test_new_source_creation_refuses_absent_protected_copy_before_database_access(monkeypatch, entrypoint):
    capture = AsyncMock(side_effect=AssertionError("source read without native custody"))
    monkeypatch.setattr(archive, "_capture_reference_family_source", capture)
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="protected source.*capability"):
        if entrypoint == "clone":
            await archive._clone_source(object(), object(), "synthetic_stage")
        elif entrypoint == "captured-tiger":
            await captured.prepare_captured_tiger_epoch(object(), object(), epoch_id=uuid4(), on_prepared=AsyncMock())
        else:
            options_by_field = {
                "importer_id": "geo",
                "schema_name": "synthetic",
                "source_metadata": {},
                "dataset_id": uuid4(),
            }
            if entrypoint == "prepare":
                await archive.prepare_reference_family_archive_source(
                    object(), **options_by_field, on_prepared=AsyncMock()
                )
            else:
                await archive.export_reference_family_archive(object(), **options_by_field, archive_copy=AsyncMock())
    capture.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("missing", ["copy", "seal", "deadline"])
async def test_direct_clone_requires_complete_native_capability_before_sql(monkeypatch, missing):
    session, source, ownership, events = _clone_case(monkeypatch)
    options_by_field = {
        "source_copy": archive.ReferenceFamilySourceCopy(AsyncMock(), 4096, 30),
        "on_precreated": AsyncMock(),
        "deadline": asyncio.get_running_loop().time() + 30,
    }
    options_by_field[{"copy": "source_copy", "seal": "on_precreated", "deadline": "deadline"}[missing]] = None
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="COPY (capability|deadline)"):
        await archive._clone_source(
            session, source, archive.reference_family_stage_schema(ownership.dataset_id), **options_by_field
        )
    assert events == []
    archive.capture_reference_family_stage_ownership.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("importer", [importer for importer in archive._SPECS if importer != "nucc"])
async def test_native_source_seals_all_heaps_then_copies_then_builds_indexes(monkeypatch, importer):
    session, capture, ownership, events = _clone_case(monkeypatch, importer)
    copy_options = []
    deadline = asyncio.get_running_loop().time() + 100

    async def seal(observed_session, observed_ownership):
        assert observed_session is session and observed_ownership is ownership
        assert sum(statement.lstrip().startswith("CREATE TABLE") for statement in events) == len(
            archive.reference_family_spec(importer, canonical=importer == "mrf").archive_names
        )
        events.append("seal")

    async def copy_rows(observed_session, query, **options):
        assert observed_session is session
        copy_options.append(options)
        events.append("copy:" + options["table_name"])
        assert "INSERT" not in query
        return 1 if len(copy_options) == 1 else 2 if len(copy_options) == 2 else 0

    await archive._clone_source(
        session,
        capture,
        archive.reference_family_stage_schema(ownership.dataset_id),
        source_copy=archive.ReferenceFamilySourceCopy(copy_rows, 3, 100),
        deadline=deadline,
        on_precreated=seal,
    )
    assert len(copy_options) == len(archive.reference_family_spec(importer, canonical=importer == "mrf").archive_names)
    assert [options["max_bytes"] for options in copy_options] == [3, 2, *([0] * (len(copy_options) - 2))][
        : len(copy_options)
    ]
    assert all(0 < options["timeout"] <= 100 for options in copy_options)
    spec = archive.reference_family_spec(importer, canonical=importer == "mrf")
    for model in spec.model_types:
        ddl = next(sql for sql in events if "CREATE TABLE" in sql and model.__tablename__ + " " in sql)
        assert "PARTITION BY" not in ddl
        if not archive._declared_owner_columns(spec, model.__tablename__):
            assert "SERIAL" not in ddl
    if importer == "mrf":
        assert copy_options[-1]["columns"] == tuple(
            column.name for column in address.AddressArchiveV2.__table__.columns
        )
        assert "payload" not in copy_options[-1]["columns"]
    copy_positions = [position for position, statement in enumerate(events) if statement.startswith("copy:")]
    index_positions = [
        position
        for position, statement in enumerate(events)
        if statement.startswith(("CREATE INDEX", "CREATE UNIQUE INDEX"))
        or statement.startswith("ALTER TABLE")
        and " ADD " in statement
    ]
    assert events.index("seal") < min(copy_positions) <= max(copy_positions) < min(index_positions)
    assert not any(statement.startswith("INSERT") for statement in events)
    assert not any("INCLUDING ALL" in statement for statement in events)
    assert archive._is_model_table_equal.await_count == len(
        archive.reference_family_spec(importer, canonical=importer == "mrf").model_types
    )


@pytest.mark.asyncio
async def test_expired_source_deadline_creates_nothing(monkeypatch):
    session, source, ownership, events = _clone_case(monkeypatch)
    with pytest.raises(TimeoutError, match="deadline expired"):
        await archive._clone_source(
            session,
            source,
            archive.reference_family_stage_schema(ownership.dataset_id),
            source_copy=archive.ReferenceFamilySourceCopy(AsyncMock(), 4096, 30),
            on_precreated=AsyncMock(),
            deadline=asyncio.get_running_loop().time() - 1,
        )
    assert events == []


@pytest.mark.asyncio
@pytest.mark.parametrize("copied", [True, -1, 4, None])
async def test_invalid_native_copy_accounting_never_completes_candidate(monkeypatch, copied):
    session, capture, ownership, events = _clone_case(monkeypatch)
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="COPY accounting"):
        await archive._clone_source(
            session,
            capture,
            archive.reference_family_stage_schema(ownership.dataset_id),
            source_copy=archive.ReferenceFamilySourceCopy(AsyncMock(return_value=copied), 3, 100),
            deadline=asyncio.get_running_loop().time() + 100,
            on_precreated=AsyncMock(),
        )
    assert not any(statement.startswith(("CREATE INDEX", "CREATE UNIQUE INDEX")) for statement in events)
    archive._rebase_owned_sequences.assert_not_awaited()


@pytest.mark.asyncio
async def test_native_source_deadline_is_shared_between_tables(monkeypatch):
    session, capture, ownership, _events = _clone_case(monkeypatch)
    deadlines = []

    async def copy_rows(_session, _query, **options):
        deadlines.append(options["timeout"])
        await asyncio.sleep(0)
        return 0

    await archive._clone_source(
        session,
        capture,
        archive.reference_family_stage_schema(ownership.dataset_id),
        source_copy=archive.ReferenceFamilySourceCopy(copy_rows, 100, 10),
        deadline=asyncio.get_running_loop().time() + 10,
        on_precreated=AsyncMock(),
    )
    assert all(left >= right > 0 for left, right in zip(deadlines, deadlines[1:]))


@pytest.mark.asyncio
async def test_native_auxiliary_proof_does_not_call_legacy_row_hash(monkeypatch):
    monkeypatch.setattr(archive, "_relation_oid", AsyncMock(return_value=10))
    monkeypatch.setattr(archive, "_projected_row_identity", AsyncMock(side_effect=AssertionError("legacy row hash")))
    session = SimpleNamespace(scalar=AsyncMock(return_value=0))
    publication_by_field = {
        "attempt_id": str(uuid4()),
        "generation": {},
        "address_content": {},
        "native_address_coverage": {},
    }
    auxiliary = await archive._mrf_auxiliary_receipt(
        session, "synthetic", is_source=True, publication=publication_by_field, native_set=True
    )
    assert auxiliary["contract"] == archive._AUX_NATIVE_SET_CONTRACT
    assert "content_sha256" not in auxiliary and auxiliary["row_count"] == 0
    assert archive._validate_mrf_auxiliary_receipt(auxiliary) == auxiliary
    archive._projected_row_identity.assert_not_awaited()


@pytest.mark.asyncio
async def test_native_address_coverage_uses_counts_and_indexed_anti_joins(monkeypatch):
    monkeypatch.setattr(address, "_projected_row_identity", AsyncMock(side_effect=AssertionError("legacy row hash")))
    statements = []

    async def scalar(statement, _parameters=None):
        statements.append(str(statement))
        return 10 if "to_regclass" in str(statement) else 0

    session = SimpleNamespace(scalar=scalar, execute=AsyncMock())
    coverage = await address.capture_address_content(
        session, "synthetic", lambda schema, name: f'"{schema}"."{name}"', native_set=True
    )
    assert coverage["contract"] == "mrf-address-coverage.indexed-set.v2"
    assert all("sha256" not in table for table in coverage["tables"].values())
    assert sum("NOT EXISTS" in statement for statement in statements) == len(address.REFERENCES)
    address.require_address_coverage(coverage)


@pytest.mark.asyncio
async def test_failed_source_authority_record_rolls_back_both_transactions(monkeypatch):
    events = []
    sessions = []

    @asynccontextmanager
    async def session_factory():
        ordinal = len(sessions)

        @asynccontextmanager
        async def transaction():
            try:
                yield
            except BaseException:
                events.append((ordinal, "rollback"))
                raise
            else:
                events.append((ordinal, "commit"))

        session = SimpleNamespace(begin=transaction, execute=AsyncMock())
        sessions.append(session)
        yield session

    capture = SimpleNamespace(
        manifest=SimpleNamespace(
            importer_id="geo", as_dict=lambda: {"importer_id": "geo", "contract": archive.CONTRACT}
        )
    )
    monkeypatch.setattr(archive, "_capture_reference_family_source", AsyncMock(return_value=capture))
    monkeypatch.setattr(archive, "_clone_source", AsyncMock())
    monkeypatch.setattr(archive, "_validate_stage_manifest", AsyncMock())
    monkeypatch.setattr(archive, "capture_reference_family_stage_ownership", AsyncMock(return_value=object()))
    monkeypatch.setattr(archive, "_capture_model_family_ownership", AsyncMock(return_value=object()))
    on_prepared = AsyncMock(side_effect=RuntimeError("lease changed"))
    copier = archive.ReferenceFamilySourceCopy(AsyncMock(), 1024**3, 3600)
    with pytest.raises(RuntimeError, match="lease changed"):
        await archive.prepare_reference_family_archive_source(
            session_factory,
            importer_id="geo",
            schema_name="synthetic",
            source_metadata={},
            dataset_id=uuid4(),
            on_prepared=on_prepared,
            source_copy=copier,
            on_precreated=AsyncMock(),
        )
    assert events == [(1, "rollback"), (0, "rollback")]
    assert archive._clone_source.call_args.kwargs["source_copy"] is copier
    assert archive._clone_source.call_args.kwargs["deadline"] > asyncio.get_running_loop().time()


@pytest.mark.asyncio
@pytest.mark.parametrize("max_bytes,timeout", [(8 * 1024**2 + 1, 30), (4096, 121), (1024**3, 3600)])
async def test_nucc_keeps_its_smaller_fixed_limits(max_bytes, timeout):
    copier = archive.ReferenceFamilySourceCopy(AsyncMock(), max_bytes, timeout)
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="NUCC source COPY capability"):
        await archive.prepare_nucc_reference_archive_source(
            None,
            dataset_id=uuid4(),
            on_prepared=AsyncMock(),
            source_copy=copier,
            source_metadata_factory=AsyncMock(),
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("contract", [None, archive.IMMUTABLE_NUCC_SOURCE_CAPTURE_CONTRACT, "unknown"])
async def test_nucc_source_mode_follows_authenticated_metadata_in_the_same_transaction(monkeypatch, contract):
    observed_operations = []

    @asynccontextmanager
    async def transaction():
        observed_operations.append("begin")
        yield

    session = SimpleNamespace(begin=transaction)

    @asynccontextmanager
    async def session_factory():
        yield session

    async def metadata(received):
        assert received is session
        observed_operations.append("metadata")
        return {"official": "proof"}

    async def selected_mode(received):
        assert received is session
        observed_operations.append("mode")
        return contract

    capture = AsyncMock(side_effect=RuntimeError("stop before COPY"))
    monkeypatch.setattr(archive, "_capture_reference_family_source", capture)
    with pytest.raises(RuntimeError, match="contract differs|stop before COPY"):
        await archive.prepare_nucc_reference_archive_source(
            session_factory,
            dataset_id=uuid4(),
            on_prepared=AsyncMock(),
            on_precreated=AsyncMock(),
            source_copy=archive.ReferenceFamilySourceCopy(AsyncMock(), 8 * 1024**2, 120),
            source_metadata_factory=metadata,
            source_capture_contract_factory=selected_mode,
        )
    assert observed_operations == ["begin", "metadata", "mode"]
    assert capture.await_count == int(contract != "unknown")
    if contract != "unknown":
        assert capture.await_args.args[0] is session
        assert capture.await_args.kwargs["source_capture_contract"] == contract


@pytest.mark.asyncio
@pytest.mark.parametrize("entrypoint", ["clone", "prepare"])
async def test_generic_source_cannot_bypass_dedicated_nucc_policy(monkeypatch, entrypoint):
    session, source, ownership, events = _clone_case(monkeypatch, "nucc")
    copier = archive.ReferenceFamilySourceCopy(AsyncMock(), 1024**3, 3600)
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="NUCC protected source requires dedicated"):
        if entrypoint == "clone":
            await archive._clone_source(
                session,
                source,
                archive.reference_family_stage_schema(ownership.dataset_id),
                source_copy=copier,
                on_precreated=AsyncMock(),
                deadline=asyncio.get_running_loop().time() + 3600,
            )
        else:
            await archive.prepare_reference_family_archive_source(
                object(),
                importer_id="nucc",
                schema_name="synthetic",
                source_metadata={},
                dataset_id=uuid4(),
                on_prepared=AsyncMock(),
                source_copy=copier,
                on_precreated=AsyncMock(),
            )
    assert events == []
