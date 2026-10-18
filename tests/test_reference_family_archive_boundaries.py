# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Reject drifted source and restored ownership before publication."""

import asyncio
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import reference_family_archive as archive
from tests.test_reference_family_archive import (
    _incumbent,
    _manifest,
    _ownership,
    _serving_generation,
    _source_creation_options,
)


@pytest.mark.asyncio
@pytest.mark.parametrize("importer_id", ["label", "geo", "mrf"])
async def test_source_lock_preserves_other_families_and_label_select_only_reader(importer_id):
    spec = SimpleNamespace(importer_id=importer_id, table_names=("label" if importer_id == "label" else "synthetic",))
    session = SimpleNamespace(execute=AsyncMock())
    await archive._lock_source_family(session, spec, "synthetic")
    names = archive.RELATION_NAMES_BY_IMPORTER["mrf"] if importer_id == "mrf" else spec.table_names
    relations = ", ".join(f'"synthetic"."{name}"' for name in names)
    mode = "ACCESS SHARE MODE NOWAIT" if importer_id == "label" else "ACCESS SHARE MODE"
    assert str(session.execute.await_args.args[0]) == f"LOCK TABLE {relations} IN {mode}"


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["valid", "incomplete", "drifted", "locked"])
async def test_label_source_reader_keeps_snapshot_and_generation_fences(monkeypatch, state):
    @asynccontextmanager
    async def bounded(_session):
        yield

    async def execute(statement):
        if state == "locked" and str(statement).startswith("LOCK TABLE"):
            raise RuntimeError("synthetic source lock unavailable")
        return SimpleNamespace(scalar_one=lambda: "00000001-00000001-1")

    session = SimpleNamespace(in_transaction=lambda: True, execute=AsyncMock(side_effect=execute))
    spec = SimpleNamespace(importer_id="label", table_names=("label",))
    serving = _serving_generation()
    authority = SimpleNamespace(serving_generation=None if state == "incomplete" else serving, relation_oids=(99,))
    tables = (replace(_manifest().tables[0], model_name="Label", table_name="label"),)
    manifest = replace(
        _manifest(),
        importer_id="label",
        tables=tables,
        schema_sha256=archive._schema_digest(tables),
        publication_authority="tracked-generation",
        source_serving_generation=serving,
    )
    manifest_capture = AsyncMock(return_value=manifest)
    monkeypatch.setattr(archive, "reference_family_spec", lambda _importer: spec)
    monkeypatch.setattr(archive, "_bounded_capture", bounded)
    monkeypatch.setattr(archive, "read_reference_family_result_generation_authority", AsyncMock(return_value=authority))
    monkeypatch.setattr(
        archive,
        "capture_reference_family_serving_generation",
        AsyncMock(return_value=serving, side_effect=RuntimeError("drifted") if state == "drifted" else None),
    )
    monkeypatch.setattr(archive, "_family_manifest", manifest_capture)
    capture_argument_map = dict(importer_id="label", schema_name="synthetic", source_metadata={"release": "synthetic"})
    if state == "valid":
        capture = await archive.capture_reference_family_source(session, **capture_argument_map)
        assert capture.manifest == manifest
        assert capture.postgres_snapshot == "00000001-00000001-1"
        assert manifest_capture.await_args.kwargs["source_serving_generation"] is serving
    else:
        with pytest.raises(RuntimeError, match="source generation|source lock"):
            await archive.capture_reference_family_source(session, **capture_argument_map)
        manifest_capture.assert_not_awaited()
    statements = [str(call.args[0]) for call in session.execute.await_args_list]
    assert statements[:2] == [
        "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ",
        'LOCK TABLE "synthetic"."label" IN ACCESS SHARE MODE NOWAIT',
    ]
    assert ("SELECT pg_export_snapshot()" in statements) is (state == "valid")


@pytest.mark.parametrize("authority,generation", [("unknown", None), ("tracked-generation", None), ("manual-only", 1)])
def test_manifest_serialization_cannot_invent_generation_authority(authority, generation):
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="authority is invalid"):
        replace(_manifest(), publication_authority=authority, source_serving_generation=generation).as_dict()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["relation", "columns", "primary-key", "payload-key"])
async def test_restored_canonical_address_requires_exact_identity(monkeypatch, failure):
    monkeypatch.setattr(archive, "_relation_oid", AsyncMock(return_value=None if failure == "relation" else 10))
    columns = [
        {"attname": name, "type": kind, "attnotnull": True, "default_expression": None}
        for name, kind in [("address_key", "uuid"), ("payload", "jsonb")]
    ]
    monkeypatch.setattr(
        archive.catalog_identity, "_catalog_columns", AsyncMock(return_value=[] if failure == "columns" else columns)
    )
    session = SimpleNamespace(scalar=AsyncMock(side_effect=[0 if failure == "primary-key" else 1, 1]))
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="MRF canonical auxiliary"):
        await archive._restored_mrf_auxiliary_identity(session, "synthetic", {"archive_name": "address_archive_v2"})


@pytest.mark.asyncio
@pytest.mark.parametrize("missing", ["relation", "publication"])
async def test_canonical_source_requires_both_relation_and_publication(monkeypatch, missing):
    monkeypatch.setattr(archive, "_relation_oid", AsyncMock(return_value=None if missing == "relation" else 10))
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="source or publication receipt"):
        await archive._mrf_auxiliary_receipt(
            object(), "synthetic", is_source=True, publication=None if missing == "publication" else {}
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("schema,expected", [("tiger", True), ("synthetic", False)])
async def test_tiger_generation_authority_is_native_schema_only(schema, expected):
    assert (
        await archive._has_source_generation_authority(object(), archive.reference_family_spec("tiger"), schema)
        is expected
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["missing", "wrong-column", "wrong-sequence", "duplicate"])
async def test_sequence_identity_rejects_absent_or_ambiguous_defaults(monkeypatch, failure):
    columns = [{"attname": "id", "default_expression": "nextval('synthetic.owned_seq'::regclass)"}]
    if failure == "missing":
        columns[0]["default_expression"] = None
    if failure == "wrong-column":
        columns[0]["attname"] = "unrelated"
    if failure == "wrong-sequence":
        columns[0]["default_expression"] = "nextval('synthetic.other_seq'::regclass)"
    if failure == "duplicate":
        columns.append(dict(columns[0]))
    monkeypatch.setattr(archive, "_OWNED_SEQUENCES", {"synthetic": (("owned_seq", "owned_table", "id"),)})
    monkeypatch.setattr(archive, "_source_owned_sequence", AsyncMock(return_value="owned_seq"))
    monkeypatch.setattr(archive.catalog_identity, "_catalog_columns", AsyncMock(return_value=columns))
    monkeypatch.setattr(archive.catalog_identity, "_catalog_constraints", AsyncMock(return_value=[]))
    monkeypatch.setattr(archive.catalog_identity, "_catalog_indexes", AsyncMock(return_value=[]))
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="owned sequence default"):
        await archive._family_schema_identity(object(), "synthetic", 10, "synthetic", "owned_table")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "records", [[], [{"sequence_name": "bad-name"}], [{"sequence_name": "a"}, {"sequence_name": "b"}]]
)
async def test_owned_sequence_requires_one_exact_catalog_record(records):
    session = SimpleNamespace(execute=AsyncMock(return_value=SimpleNamespace(mappings=lambda: records)))
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="owned sequence is unavailable"):
        await archive._source_owned_sequence(session, 10, "id")


@pytest.mark.parametrize("auxiliary", [None, {}, {"table_name": "other"}])
def test_canonical_auxiliary_receipt_is_mandatory_and_closed(auxiliary):
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="auxiliary receipt is invalid"):
        archive._validate_mrf_auxiliary_receipt(auxiliary)


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["untracked", "incomplete", "drifted", "invalid-snapshot"])
async def test_capture_rejects_incomplete_or_drifted_source_generation(monkeypatch, state):
    @asynccontextmanager
    async def bounded(_session):
        yield

    session = SimpleNamespace(
        in_transaction=lambda: True,
        execute=AsyncMock(
            return_value=SimpleNamespace(
                scalar_one=lambda: "bad snapshot" if state == "invalid-snapshot" else "00000001-00000001-1"
            )
        ),
    )
    monkeypatch.setattr(archive, "_bounded_capture", bounded)
    monkeypatch.setattr(archive, "_lock_source_family", AsyncMock())
    monkeypatch.setattr(archive, "_has_source_generation_authority", AsyncMock(return_value=state != "untracked"))
    authority = SimpleNamespace(
        serving_generation=None if state == "incomplete" else _serving_generation(), relation_oids=(99,)
    )
    monkeypatch.setattr(archive, "read_reference_family_result_generation_authority", AsyncMock(return_value=authority))
    capture_generation = AsyncMock(
        return_value=_serving_generation(), side_effect=RuntimeError("drifted") if state == "drifted" else None
    )
    monkeypatch.setattr(archive, "capture_reference_family_serving_generation", capture_generation)
    manifest = AsyncMock(return_value=_manifest())
    monkeypatch.setattr(archive, "_family_manifest", manifest)
    capture_argument_map = dict(
        importer_id="places-zcta",
        schema_name="synthetic",
        source_metadata={"release": "synthetic"},
    )
    if state == "untracked":
        capture = await archive._capture_reference_family_source(session, **capture_argument_map)
        assert capture.manifest == _manifest()
        assert manifest.await_args.kwargs["source_serving_generation"] is None
    else:
        with pytest.raises(archive.ReferenceFamilyArchiveError, match="source generation|source snapshot"):
            await archive._capture_reference_family_source(session, **capture_argument_map)
        assert manifest.await_count == int(state == "invalid-snapshot")
    if state in {"untracked", "incomplete"}:
        capture_generation.assert_not_awaited()
    else:
        capture_generation.assert_awaited_once_with(session, importer_id="places-zcta", schema_name="synthetic")


@pytest.mark.asyncio
@pytest.mark.parametrize("wrong_scope", [False, True])
async def test_sequence_rebase_locks_and_verifies_retained_ownership(monkeypatch, wrong_scope):
    ownership = replace(_ownership("tiger"), sequence_oids=(("zcta5_gid_seq", 99, "zcta5", "gid"),))
    session = SimpleNamespace(in_transaction=lambda: True, execute=AsyncMock(), scalar=AsyncMock(return_value=17))
    lock = AsyncMock()
    verify = AsyncMock()
    monkeypatch.setattr(archive, "_lock_family", lock)
    monkeypatch.setattr(archive, "verify_reference_family_stage_ownership", verify)
    if wrong_scope:
        with pytest.raises(archive.ReferenceFamilyArchiveError, match="ownership scope differs"):
            await archive._rebase_owned_sequences(session, "other", "tiger", ownership=ownership)
        lock.assert_not_awaited()
    else:
        await archive._rebase_owned_sequences(session, ownership.schema_name, "tiger", ownership=ownership)
        verify.assert_awaited_once_with(session, ownership)
        assert session.execute.await_args.args[1] == {"sequence": "99", "value": 17, "called": True}
        assert "SELECT last_value" in str(session.execute.await_args_list[0].args[0])


@pytest.mark.asyncio
async def test_capture_rejects_missing_mrf_auxiliary_stage(monkeypatch):
    monkeypatch.setattr(archive, "_schema_oid", AsyncMock(return_value=10))
    monkeypatch.setattr(
        archive,
        "_relation_oid",
        AsyncMock(side_effect=lambda _session, _schema, name: None if name == archive.STAGE_TABLE else 11),
    )
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="auxiliary relation is missing"):
        await archive.capture_reference_family_stage_ownership(
            SimpleNamespace(in_transaction=lambda: True), importer_id="mrf", dataset_id=_ownership().dataset_id
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["archive-name", "archive-missing", "invalid-source", "strict-bits"])
async def test_canonical_merge_rejects_invalid_sources_and_preserves_strict_bits(monkeypatch, failure):
    monkeypatch.setattr(archive, "_relation_oid", AsyncMock(return_value=None if failure == "archive-missing" else 10))
    monkeypatch.setattr(archive, "_lock_family", AsyncMock())
    session = SimpleNamespace(
        scalar=AsyncMock(side_effect=[0, int(failure == "invalid-source"), True]), execute=AsyncMock()
    )
    auxiliary_receipt_map = {"archive_name": "other" if failure == "archive-name" else archive.archive_table_name()}
    if failure != "strict-bits":
        with pytest.raises(
            archive.ReferenceFamilyArchiveError, match="archive name|archive is unavailable|source contribution"
        ):
            await archive._merge_mrf_canonical_address(session, _ownership("mrf"), "synthetic", auxiliary_receipt_map)
        session.execute.assert_not_awaited()
    else:
        await archive._merge_mrf_canonical_address(session, _ownership("mrf"), "synthetic", auxiliary_receipt_map)
        statements = [str(call.args[0]) for call in session.execute.await_args_list]
        assert "strict_source_bits=target.strict_source_bits |" in statements[2]
        assert statements[-1].startswith("DROP TABLE")


@pytest.mark.asyncio
async def test_validated_cutover_rejects_post_rotation_oid_drift(monkeypatch):
    monkeypatch.setattr(archive, "_rotate_family_relations", AsyncMock(return_value=None))
    monkeypatch.setattr(archive, "_drop_empty_stage_schema", AsyncMock())
    monkeypatch.setattr(archive, "_incumbent_pairs", AsyncMock(return_value=[("pricing_places_zcta", 99)]))
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="activated relation OID differs"):
        await archive._complete_validated_stage_activation(
            object(),
            archive.reference_family_spec("places-zcta"),
            _ownership(),
            SimpleNamespace(schema_name="synthetic"),
            _manifest(),
        )


def test_declared_family_cannot_repeat_a_table(monkeypatch):
    spec = archive.reference_family_spec("places-zcta")
    monkeypatch.setitem(archive._SPECS, "places-zcta", replace(spec, model_types=spec.model_types * 2))
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="model declaration is invalid"):
        archive.reference_family_spec("places-zcta")


@pytest.mark.asyncio
async def test_export_preparation_failure_does_not_claim_stage_cleanup(monkeypatch):
    prepare = AsyncMock(side_effect=RuntimeError("preparation failed"))
    cleanup = AsyncMock()
    monkeypatch.setattr(archive, "prepare_reference_family_archive_source", prepare)
    monkeypatch.setattr(archive, "_shielded_cleanup", cleanup)
    with pytest.raises(RuntimeError, match="preparation failed"):
        await archive.export_reference_family_archive(
            object(),
            importer_id="places-zcta",
            schema_name="synthetic",
            source_metadata={"release": "synthetic"},
            dataset_id=_ownership().dataset_id,
            archive_copy=AsyncMock(),
            **_source_creation_options(),
        )
    cleanup.assert_not_awaited()


@pytest.mark.asyncio
async def test_cancelled_cleanup_finishes_before_propagating_cancellation(monkeypatch):
    entered = asyncio.Event()
    release = asyncio.Event()
    finished = asyncio.Event()

    @asynccontextmanager
    async def transaction():
        yield

    @asynccontextmanager
    async def sessions():
        yield SimpleNamespace(begin=transaction)

    async def cleanup(_session, _owned):
        entered.set()
        await release.wait()
        finished.set()

    monkeypatch.setattr(archive, "cleanup_reference_family_stage", cleanup)
    task = asyncio.create_task(archive._shielded_cleanup(sessions, _ownership()))
    try:
        await asyncio.wait_for(entered.wait(), timeout=1)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=1)
        assert finished.is_set()
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_mrf_activation_rechecks_table_receipts_after_rotation(monkeypatch):
    owned = _ownership("mrf")
    monkeypatch.setattr(archive, "_incumbent_pairs", AsyncMock(return_value=owned.relation_oids))
    monkeypatch.setattr(archive, "_table_receipt", AsyncMock(return_value=None))
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="activated receipt differs"):
        await archive._activation_receipt(
            object(), archive.reference_family_spec("mrf"), owned, _incumbent("mrf"), _manifest(), (), None
        )


@pytest.mark.parametrize("include_identity", [False, True])
def test_owned_sequence_inventory_keeps_serials_and_optional_identities(monkeypatch, include_identity):
    declared_serials = (("synthetic_id_seq", "synthetic", "id"),)
    columns = (SimpleNamespace(name="id", identity=None), SimpleNamespace(name="other_id", identity=object()))
    model = SimpleNamespace(__tablename__="synthetic", __table__=SimpleNamespace(columns=columns))
    spec = SimpleNamespace(importer_id="synthetic", model_types=(model,))
    monkeypatch.setattr(archive, "_OWNED_SEQUENCES", {"synthetic": declared_serials})
    expected = declared_serials
    if include_identity:
        expected = (*declared_serials, ("synthetic_other_id_seq", "synthetic", "other_id"))
    assert archive._expected_model_family_sequences(spec, include_identity=include_identity) == expected


@pytest.mark.asyncio
@pytest.mark.parametrize("include_identity", [False, True])
@pytest.mark.parametrize("drift", [None, "extra", "wrong-owner"])
async def test_captured_owned_sequence_set_still_rejects_drift(monkeypatch, include_identity, drift):
    declared_serials = (("synthetic_id_seq", "synthetic", "id"),)
    columns = (SimpleNamespace(name="id", identity=None), SimpleNamespace(name="other_id", identity=object()))
    model = SimpleNamespace(__tablename__="synthetic", __table__=SimpleNamespace(columns=columns))
    spec = SimpleNamespace(importer_id="synthetic", model_types=(model,), table_names=("synthetic",))
    monkeypatch.setattr(archive, "_OWNED_SEQUENCES", {"synthetic": declared_serials})
    expected = archive._expected_model_family_sequences(spec, include_identity=include_identity)
    sequence_oids = tuple((name, 20 + index, owner, column) for index, (name, owner, column) in enumerate(expected))
    if drift == "extra":
        sequence_oids = (*sequence_oids, ("unexpected_seq", 30, "synthetic", "id"))
    elif drift == "wrong-owner":
        sequence_oids = ((sequence_oids[0][0], 20, "synthetic", "unrelated"), *sequence_oids[1:])
    monkeypatch.setattr(archive, "_schema_oid", AsyncMock(return_value=10))
    monkeypatch.setattr(archive, "_relation_oid", AsyncMock(return_value=11))
    owned_sequences = AsyncMock(return_value=sequence_oids)
    monkeypatch.setattr(archive, "_owned_sequences", owned_sequences)
    namespace_relations = [
        {"relkind": b"r", "oid": 11},
        *({"relkind": b"S", "oid": sequence_oid} for _name, sequence_oid, _owner, _column in sequence_oids),
    ]
    monkeypatch.setattr(archive, "_namespace_relations", AsyncMock(return_value=namespace_relations))
    session = SimpleNamespace(in_transaction=lambda: True)
    if drift is None:
        captured = await archive._capture_model_family_ownership(
            session, spec, _ownership().dataset_id, include_identity=include_identity
        )
        assert captured.sequence_oids == sequence_oids
        assert captured.relation_oids == (("synthetic", 11),)
    else:
        with pytest.raises(archive.ReferenceFamilyArchiveError, match="owned sequence set is invalid"):
            await archive._capture_model_family_ownership(
                session, spec, _ownership().dataset_id, include_identity=include_identity
            )
    owned_sequences.assert_awaited_once_with(session, 10, include_identity=include_identity)


def _finite_tiger_capture():
    projection = (
        ("zip_state", (("tiger", "zip_state", 101, 1101), ("tiger_data", "fixed_zip", 103, 1103))),
        ("zcta5", (("tiger", "zcta5", 102, 1102), ("tiger_data", "fixed_zcta", 104, 1104))),
    )
    tables = tuple(
        archive.ReferenceTableReceipt(model.__name__, model.__tablename__, "a" * 64, 1)
        for model in archive.reference_family_spec("tiger").model_types
    )
    metadata = dict(
        contract="tiger.captured-origin.v2",
        epoch_id="00000000-0000-0000-0000-000000000001",
        source_graph_sha256="b" * 64,
    )
    _, digest = archive._source_metadata(metadata)
    manifest = archive.ReferenceFamilyManifest(
        "tiger",
        tables,
        metadata,
        digest,
        archive._schema_digest(tables),
        publication_authority="captured-epoch",
        source_capture_contract=archive.CAPTURED_TIGER_SNAPSHOT_CONTRACT,
    )
    return archive.ReferenceFamilySourceCapture(
        manifest, "tiger", "00000001-00000001-1", frozen_source_projection=projection
    )


def test_frozen_projection_uses_only_exact_physical_relations_and_contract():
    capture = _finite_tiger_capture()
    archive._require_frozen_source_capture(capture)
    query = archive._frozen_source_query(
        capture.frozen_source_projection, archive.reference_family_spec("tiger").model_types[0]
    )
    assert (
        query
        == 'SELECT "zip", "stusps", "statefp" FROM ONLY "tiger"."zip_state" UNION ALL SELECT "zip", "stusps", "statefp" FROM ONLY "tiger_data"."fixed_zip"'
    )
    assert archive.validate_reference_family_manifest(capture.manifest.as_dict()) == capture.manifest
    for changed in [
        replace(capture, frozen_source_projection=None),
        replace(capture, manifest=replace(capture.manifest, source_capture_contract=archive.CAPTURED_TIGER_CONTRACT)),
    ]:
        with pytest.raises(archive.ReferenceFamilyArchiveError, match="contract"):
            archive._require_frozen_source_capture(changed)


@pytest.mark.asyncio
async def test_frozen_count_copy_and_multiset_validation_share_one_projection(monkeypatch):
    capture = _finite_tiger_capture()
    spec = archive.reference_family_spec("tiger")
    sql_calls, copied_queries = [], []

    async def scalar(statement, parameters=None):
        sql_calls.append(str(statement))
        return 1 if "count(*)" in str(statement) else True

    session = SimpleNamespace(in_transaction=lambda: True, scalar=scalar)
    monkeypatch.setattr(archive, "_relation_oid", AsyncMock(return_value=101))
    monkeypatch.setattr(archive, "_family_schema_identity", AsyncMock(return_value="a" * 64))

    async def copy(_session, _source_copy, query, *_args):
        copied_queries.append(query)
        return 4000

    monkeypatch.setattr(archive, "_copy_source_projection", copy)
    await archive._table_receipt(
        session,
        importer_id="tiger",
        schema_name="tiger",
        model_type=spec.model_types[0],
        frozen_source_projection=capture.frozen_source_projection,
    )
    await archive._copy_source_tables(
        session, capture, "fixture_stage", spec, archive.ReferenceFamilySourceCopy(AsyncMock(), 4096, 30), 123
    )
    await archive._validate_source_clone_sets(session, capture, "fixture_stage")
    assert len(copied_queries) == 2 and len(sql_calls) == 3
    assert copied_queries[0] in sql_calls[0] and copied_queries[0] in sql_calls[1]
    assert copied_queries[1] in sql_calls[2]
    assert all(sql.count("EXCEPT ALL") == 2 for sql in sql_calls[1:])
    assert 'FROM "tiger".' not in " ".join(copied_queries)
    session.scalar = AsyncMock(return_value=False)
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="differs"):
        await archive._validate_source_clone_sets(session, capture, "fixture_stage")


@pytest.mark.asyncio
async def test_finite_clone_imports_snapshot_before_authorization_or_creation(monkeypatch):
    capture = _finite_tiger_capture()
    events = []
    session = SimpleNamespace(
        in_transaction=lambda: True, execute=AsyncMock(side_effect=lambda sql: events.append(str(sql)))
    )

    async def authorize(actual):
        assert actual is session
        events.append("authorize")

    async def create(*_args):
        events.append("create")
        return "owned"

    monkeypatch.setattr(archive, "create_model_family_export_clone", create)
    for name in [
        "_copy_source_tables",
        "_create_model_indexes",
        "_rebase_owned_sequences",
        "_validate_source_clone_sets",
    ]:
        monkeypatch.setattr(archive, name, AsyncMock())
    precreated = AsyncMock()
    await archive._clone_source(
        session,
        capture,
        "fixture_stage",
        source_copy=archive.ReferenceFamilySourceCopy(AsyncMock(), 4096, 30),
        on_precreated=precreated,
        authorize_creation=authorize,
        deadline=asyncio.get_running_loop().time() + 30,
    )
    assert events == [
        "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ",
        "SET TRANSACTION SNAPSHOT '00000001-00000001-1'",
        "authorize",
        "create",
    ]
    precreated.assert_awaited_once_with(session, "owned")


@pytest.mark.parametrize(
    "fault", ["mutable", "empty", "wrong_root", "duplicate_oid", "duplicate_name", "bool_oid", "zero_file", "reordered"]
)
def test_finite_projection_refuses_ambiguous_or_unbound_physical_identity(fault):
    capture = _finite_tiger_capture()
    projection = capture.frozen_source_projection
    table, relations = projection[0]
    if fault == "mutable":
        bad_relations = list(projection)
    elif fault == "empty":
        bad_relations = ((table, ()), projection[1])
    else:
        first_relation_by_fault = {
            "wrong_root": ("other", "zip_state", 101, 1101),
            "duplicate_oid": ("tiger", "zip_state", 102, 1101),
            "duplicate_name": relations[0],
            "bool_oid": ("tiger", "zip_state", True, 1101),
            "zero_file": ("tiger", "zip_state", 101, 0),
            "reordered": relations[1],
        }
        second = relations[0] if fault in {"duplicate_name", "reordered"} else relations[1]
        bad_relations = ((table, (first_relation_by_fault[fault], second)), projection[1])
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="frozen TIGER"):
        archive._require_frozen_source_capture(replace(capture, frozen_source_projection=bad_relations))


def test_captured_origin_versions_cannot_relabel_snapshot_or_writer_fenced_evidence():
    from process import tiger_captured_epoch as captured

    manifest = _finite_tiger_capture().manifest
    old_origin_by_field = dict(manifest.source_metadata, contract=captured.ORIGIN_CONTRACT)
    assert captured.validate_captured_origin(old_origin_by_field)["contract"] == captured.ORIGIN_CONTRACT
    assert captured.validate_captured_origin(manifest.source_metadata)["contract"] == captured.SNAPSHOT_ORIGIN_CONTRACT
    for changed in [
        replace(manifest, source_metadata=old_origin_by_field),
        replace(manifest, source_capture_contract=archive.CAPTURED_TIGER_CONTRACT),
    ]:
        with pytest.raises(archive.ReferenceFamilyArchiveError, match="origin and snapshot"):
            changed.as_dict()
