# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native audit contracts are checked without claiming database publication proof."""

from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.asyncio import AsyncSession

from process.ptg_parts import ptg2_physical_binding as native
from process.ptg_parts import ptg2_shared_audit as sample_store
from process.ptg_parts import ptg2_source_witness as witness_codec
from process.ptg_parts import ptg2_source_witness_store as witness_store
from process.ptg_parts import result_archive_candidate_initialization as initialization
from process.ptg_parts import result_archive_candidate_preparation as preparation
from process.ptg_parts import result_archive_closure as closure
from tests.test_ptg2_candidate_attestation import _audit_sample
from tests.test_ptg2_local_preparation_authority import _control_fixture, _ownership
from tests.test_ptg2_physical_binding import _serving_scope
from tests.test_ptg2_source_witness import _bundle, _occurrence_record
from tests.test_result_archive_closure_contracts import _layout_by_field


class _SqlRows(list):
    def __init__(self, rows=(), *, scalar_value=None):
        super().__init__(rows)
        self.scalar_value = scalar_value

    def mappings(self):
        return self

    def all(self):
        return list(self)

    def scalar(self):
        return self.scalar_value


def _witness_observation(tmp_path, raw_digest):
    bundle = _bundle(
        tmp_path,
        source_digest=raw_digest,
        name="source-witness.bin",
        compressed_records=[_occurrence_record(0)],
        occurrence_population_count=1,
    )
    payload, metadata = witness_codec.build_persisted_source_witness([bundle], expected_raw_source_sha256=[raw_digest])
    database_by_field = {
        key: bytes.fromhex(value) if key.endswith("digest") or key == "payload_sha256" else value
        for key, value in metadata.items()
        if key
        in {
            "contract",
            "selection_method",
            "source_set_digest",
            "sample_digest",
            "queryable_occurrence_population_count",
            "provider_population_count",
            "occurrence_witness_count",
            "provider_witness_count",
            "payload_sha256",
        }
    }
    return metadata, {**database_by_field, "payload": payload}


def _sample_observation():
    occurrence_by_field = {
        "occurrence_id": b"a" * 32,
        "code_key": 1,
        "provider_set_key": 2,
        "price_key": 3,
        "source_key": 0,
        "npi": 1234567890,
        "atom_ordinal": 0,
        "atom_key": 4,
    }
    occurrence = sample_store._stored_audit_occurrence(occurrence_by_field)
    metadata = _audit_sample(sample_store._sample_digest([occurrence]).hex())
    return metadata, [occurrence_by_field]


def _catalog_columns():
    return [
        {
            "table_name": model.__tablename__,
            "column_name": column.name,
            "data_type": str(column.type.compile(dialect=postgresql.dialect())).lower(),
            "attnotnull": not column.nullable,
            "attidentity": "d" if column.identity else "",
            "attgenerated": "",
        }
        for model in native.local_data_family_spec().model_types
        for column in model.__table__.columns
    ]


def _audit_observations(tmp_path):
    physical, candidate, control_scope, initialized = _control_fixture()
    ownership = _ownership(physical)
    scope = _serving_scope()
    scope.update({key: control_scope[key] for key in control_scope if key != "source_assignments"})
    raw_digest = (b"r" * 32).hex()
    scope["source_assignments"][0]["raw_container_sha256"] = raw_digest
    witness_metadata, witness_row = _witness_observation(tmp_path, raw_digest)
    sample_metadata, occurrence_rows = _sample_observation()
    serving = candidate["layout_manifest"]["serving_index"]
    serving.update(source_witness=witness_metadata, audit_sample=sample_metadata)
    layout = _layout_by_field()
    layout.update(
        snapshot_id=physical.payload_snapshot_id,
        snapshot_key=physical.payload_snapshot_key,
        manifest={"serving_index": deepcopy(serving)},
        layout_manifest=deepcopy(candidate["layout_manifest"]),
    )
    metadata = {
        "source_snapshot_key": physical.payload_snapshot_key,
        "closure_metadata": {"serving_scope": scope},
    }
    plans = [dict(zip(("plan_id", "plan_market_type"), plan, strict=True)) for plan in scope["plan_scopes"]]
    control_digest = initialization._local_control_sha256(
        candidate["manifest"], candidate["options"], tuple(tuple(plan) for plan in scope["plan_scopes"])
    )
    return SimpleNamespace(
        physical=physical,
        ownership=ownership,
        candidate=candidate,
        initialized=initialized,
        scope=scope,
        metadata=metadata,
        control_digest=control_digest,
        plans=plans,
        layout=layout,
        sample_manifest=deepcopy(layout["layout_manifest"]),
        occurrences=occurrence_rows,
        sources=[{"source_key": 0, "raw_container_sha256": raw_digest}],
        witness_row=witness_row,
        source_keys=[{"source_key": 0}],
        schema_oid=ownership.schema_oid,
    )


def _query_observation(observed, statement, parameters=None):
    sql = str(statement).strip()
    if sql.startswith("SELECT sequence.relname"):
        return _SqlRows(
            [
                dict(zip(("sequence_name", "sequence_oid", "table_name", "column_name"), sequence, strict=True))
                for sequence in observed.ownership.sequence_oids
            ]
        )
    if sql.startswith("SELECT relation.oid, relation.relname"):
        return _SqlRows(
            [{"oid": oid, "relkind": "r"} for _name, oid in observed.ownership.relation_oids]
            + [{"oid": oid, "relkind": "S"} for _name, oid, _table, _column in observed.ownership.sequence_oids]
        )
    if sql.startswith("SELECT snapshot.*,run.options"):
        assert parameters == {"snapshot_id": observed.initialized.destination_snapshot_id}
        assert "FOR UPDATE OF snapshot,run,scope" in sql
        return _SqlRows([] if observed.candidate is None else [observed.candidate])
    if sql.startswith("SELECT plan_id, lower(plan_market_type)"):
        assert parameters == {"snapshot_id": observed.initialized.destination_snapshot_id}
        assert "FOR SHARE" in sql
        return _SqlRows(observed.plans)
    if sql.startswith("SELECT snapshot.snapshot_id, snapshot.status"):
        assert parameters == {
            "snapshot_id": observed.scope["snapshot_id"],
            "payload_snapshot_key": observed.metadata["source_snapshot_key"],
        }
        assert "ptg2_v3_snapshot_binding" not in sql and "FOR KEY SHARE OF snapshot, layout" in sql
        return _SqlRows([observed.layout])
    if sql.startswith("SELECT layout_manifest"):
        return _SqlRows(scalar_value=observed.sample_manifest)
    if sql.startswith("SELECT occurrence_id"):
        return _SqlRows(observed.occurrences)
    if sql.startswith("SELECT source_key"):
        return _SqlRows(observed.source_keys)
    if sql.startswith("SELECT source.source_key"):
        assert parameters == {"snapshot_id": observed.scope["snapshot_id"]}
        return _SqlRows(observed.sources)
    if sql.startswith("SELECT 'relation' AS kind"):
        return _SqlRows(
            [
                {
                    "kind": "relation",
                    "oid": oid,
                    "name": name,
                    "owner": observed.physical.owner_oid,
                    "definition": "r:p:1:0",
                }
                for name, oid in observed.ownership.relation_oids
            ]
        )
    if sql.startswith("SELECT c.relname AS table_name"):
        return _SqlRows(_catalog_columns())
    raise AssertionError("unexpected audit query: " + sql)


def _scalar_observation(observed, statement, parameters):
    sql = str(statement)
    if sql.startswith("SELECT oid FROM pg_catalog.pg_namespace"):
        assert parameters == {"schema_name": observed.ownership.schema_name}
        return observed.schema_oid
    if sql.startswith("SELECT relation.oid FROM pg_catalog.pg_class"):
        assert parameters["schema_name"] == observed.ownership.schema_name
        return dict(observed.ownership.relation_oids).get(parameters["table_name"])
    raise AssertionError("unexpected audit scalar: " + sql)


def _audit_session(monkeypatch, observed):
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "mrf")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    session = MagicMock(spec=AsyncSession)
    session.in_transaction.return_value = True
    session.in_nested_transaction.return_value = False
    session.bind = SimpleNamespace(url=SimpleNamespace(database=None))
    session.execute = AsyncMock(
        side_effect=lambda statement, parameters=None: _query_observation(observed, statement, parameters)
    )
    session.scalar = AsyncMock(
        side_effect=lambda statement, parameters: _scalar_observation(observed, statement, parameters)
    )

    async def witness_row(statement, **parameters):
        assert ".ptg2_v3_source_audit_witness" in statement
        assert parameters == {"snapshot_key": observed.physical.payload_snapshot_key}
        return observed.witness_row

    async def witness_parts(statement, **parameters):
        assert ".ptg2_v3_source_audit_witness_part" in statement
        assert parameters == {"snapshot_key": observed.physical.payload_snapshot_key}
        return []

    monkeypatch.setattr(witness_store.db, "first", AsyncMock(side_effect=witness_row))
    monkeypatch.setattr(witness_store.db, "all", AsyncMock(side_effect=witness_parts))
    return session


async def _audit(session, observed):
    return await preparation.audit_local_data_candidate(
        session,
        ownership=observed.ownership,
        metadata=observed.metadata,
        initialized=observed.initialized,
        control_sha256=observed.control_digest,
    )


@pytest.mark.asyncio
async def test_native_audit_binds_real_decoded_evidence_without_publishing(monkeypatch, tmp_path):
    observed = _audit_observations(tmp_path)
    session = _audit_session(monkeypatch, observed)
    receipt = await _audit(session, observed)
    assert receipt["contract"] == "ptg-local-data.native-set-audit.v1"
    assert receipt["control_sha256"] == observed.control_digest
    assert receipt["model_sha256"] == native.local_data_model_digest()
    assert receipt["identity"]["snapshot_key"] == observed.physical.payload_snapshot_key
    assert receipt["identity"]["snapshot_key"] != observed.initialized.destination_layout_key
    assert receipt["identity"]["source_witness_digest"] == observed.witness_row["payload_sha256"].hex()
    assert (
        receipt["identity"]["audit_sample_digest"]
        == observed.sample_manifest["serving_index"]["audit_sample"]["sample_digest"]
    )
    assert len(receipt["catalog_sha256"]) == 64
    assert all(
        not str(call.args[0]).strip().startswith(("INSERT", "UPDATE", "DELETE", "DROP"))
        for call in session.execute.await_args_list
    )
    session.commit.assert_not_awaited()
    session.rollback.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("transaction", [False, None, "not-callable"])
async def test_native_audit_requires_caller_transaction_before_custody(transaction):
    session = SimpleNamespace(in_transaction=transaction, execute=AsyncMock())
    if transaction is False:
        session.in_transaction = lambda: False
    with pytest.raises(preparation.ResultArchiveCandidatePreparationError, match="caller transaction"):
        await preparation.audit_local_data_candidate(
            session, ownership=None, metadata={}, initialized=None, control_sha256="a" * 64
        )
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("status", "validated"),
        ("run_status", "validated"),
        ("import_run_id", "another-run"),
        ("plan_id", "another-plan"),
        ("plan_market_type", "individual"),
        ("coverage_scope_id", b"x" * 32),
    ],
)
async def test_native_audit_refuses_rebound_destination_controls(monkeypatch, tmp_path, field, value):
    observed = _audit_observations(tmp_path)
    observed.candidate[field] = value
    session = _audit_session(monkeypatch, observed)
    with pytest.raises(preparation.ResultArchiveCandidatePreparationError, match="controls differ"):
        await _audit(session, observed)
    assert not any("FOR KEY SHARE" in str(call.args[0]) for call in session.execute.await_args_list)
    witness_store.db.first.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["source", "plans", "manifest", "digest", "absent", "custody"])
async def test_native_audit_stops_before_payload_when_control_preimage_changes(monkeypatch, tmp_path, fault):
    observed = _audit_observations(tmp_path)
    mutation_by_fault = {
        "source": lambda: observed.candidate["options"].update(source_key="another-source"),
        "plans": lambda: observed.plans.clear(),
        "manifest": lambda: observed.candidate["manifest"].update(changed=True),
        "digest": lambda: setattr(observed, "control_digest", "0" * 64),
        "absent": lambda: setattr(observed, "candidate", None),
        "custody": lambda: setattr(observed, "schema_oid", observed.schema_oid + 1),
    }
    mutation_by_fault[fault]()
    session = _audit_session(monkeypatch, observed)
    with pytest.raises(
        (
            preparation.ResultArchiveCandidatePreparationError,
            initialization.ResultArchiveCandidateInitializationError,
            native.PTG2PhysicalBindingError,
        ),
        match="controls differ|missing or ambiguous|ownership changed",
    ):
        await _audit(session, observed)
    assert not any("FOR KEY SHARE" in str(call.args[0]) for call in session.execute.await_args_list)
    witness_store.db.first.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("payload_key", [False, 0, -1, 2**63, "19"])
async def test_native_audit_rejects_unbounded_isolated_layout_keys_before_query(monkeypatch, tmp_path, payload_key):
    observed = _audit_observations(tmp_path)
    observed.metadata["source_snapshot_key"] = payload_key
    session = _audit_session(monkeypatch, observed)
    with pytest.raises(closure.ResultArchiveClosureError, match="payload snapshot key is invalid"):
        await _audit(session, observed)
    assert not any("FOR KEY SHARE" in str(call.args[0]) for call in session.execute.await_args_list)
    witness_store.db.first.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["layout", "sample", "dictionary", "witness", "missing-witness", "source", "scope"])
async def test_native_audit_preserves_layout_sample_witness_and_identity_refusals(monkeypatch, tmp_path, fault):
    observed = _audit_observations(tmp_path)
    mutation_by_fault = {
        "layout": lambda: observed.layout.update(state="building"),
        "sample": lambda: observed.occurrences[0].update(atom_key=5),
        "dictionary": lambda: observed.source_keys.clear(),
        "witness": lambda: observed.witness_row.update(payload=observed.witness_row["payload"] + b"x"),
        "missing-witness": lambda: setattr(observed, "witness_row", None),
        "source": lambda: observed.sources[0].update(raw_container_sha256="f" * 64),
        "scope": lambda: observed.layout["layout_manifest"]["serving_index"].update(coverage_scope_id="f" * 64),
    }
    mutation_by_fault[fault]()
    session = _audit_session(monkeypatch, observed)
    with pytest.raises(
        (RuntimeError, ValueError),
        match="sealed layout|audit rows disagree|complete and dense|payload digest|persisted source witness|bindings",
    ):
        await _audit(session, observed)
    session.commit.assert_not_awaited()
    session.rollback.assert_not_awaited()


@pytest.mark.asyncio
async def test_native_audit_compares_separately_authenticated_sample_to_locked_identity(monkeypatch, tmp_path):
    observed = _audit_observations(tmp_path)
    observed.occurrences[0]["atom_key"] += 1
    occurrence = sample_store._stored_audit_occurrence(observed.occurrences[0])
    observed.sample_manifest["serving_index"]["audit_sample"]["sample_digest"] = sample_store._sample_digest(
        [occurrence]
    ).hex()
    session = _audit_session(monkeypatch, observed)
    with pytest.raises(preparation.ResultArchiveCandidatePreparationError, match="persisted audit evidence differs"):
        await _audit(session, observed)
    assert any("SELECT source.source_key" in str(call.args[0]) for call in session.execute.await_args_list)
    assert not any("SELECT 'relation' AS kind" in str(call.args[0]) for call in session.execute.await_args_list)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure",
    [
        "reference family stage ownership is invalid",
        "reference family stage ownership differs",
        "index creation failed",
    ],
)
async def test_local_family_completion_preserves_native_failure_translation(monkeypatch, failure):
    from process import reference_family_archive as archive

    complete = AsyncMock(side_effect=archive.ReferenceFamilyArchiveError(failure))
    monkeypatch.setattr(archive, "complete_model_family_stage", complete)
    expected = (
        native.PTG2PhysicalBindingError
        if failure.startswith("reference family")
        else archive.ReferenceFamilyArchiveError
    )
    with pytest.raises(expected):
        await native.complete_local_data_family(SimpleNamespace(), _ownership(_control_fixture()[0]))
    assert complete.await_args.kwargs == {"include_identity": True}
