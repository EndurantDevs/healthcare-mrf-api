# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Host custody refusals; native PostgreSQL privileges and transactional effects are qualified separately."""

import asyncio
import json
from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from process import mrf_address_publication as canonical
from process import reference_family_archive as archive
from process.ptg_parts import ptg2_physical_binding as native
from process.ptg_parts.ptg2_candidate_attestation import _candidate_identity
from process.ptg_parts.source_pointers import candidate_snapshot_attributes
from tests.test_ptg2_local_preparation_authority import (
    _ownership,
    _protected_preparation_fixture,
    _published_control_fixture,
)
from tests.test_ptg2_physical_binding import _binding, _serving_scope


class _query_result(list):
    def mappings(self):
        return self

    def all(self):
        return list(self)

    def one(self):
        assert len(self) == 1
        return self[0]

    def one_or_none(self):
        assert len(self) <= 1
        return self[0] if self else None

    def first(self):
        return self[0] if self else None


def _observe_family(monkeypatch, ownership):
    """Substitute only fixed catalog reads; family completeness validation remains real."""
    namespace_rows = [{"relkind": "r", "oid": oid} for _name, oid in ownership.relation_oids]
    namespace_rows.extend({"relkind": "S", "oid": oid} for _name, oid, _table, _column in ownership.sequence_oids)
    monkeypatch.setattr(archive, "_schema_oid", AsyncMock(return_value=ownership.schema_oid))
    relations = AsyncMock(side_effect=lambda _session, _schema, name: dict(ownership.relation_oids).get(name))
    monkeypatch.setattr(archive, "_relation_oid", relations)
    monkeypatch.setattr(archive, "_owned_sequences", AsyncMock(return_value=ownership.sequence_oids))
    monkeypatch.setattr(archive, "_namespace_relations", AsyncMock(return_value=namespace_rows))
    return relations


def _staged_control_fixture():
    physical, published, evidence, _publication = _published_control_fixture()
    physical = replace(physical, snapshot_id="snapshot-archive-" + str(UUID(int=1)))
    candidate = candidate_snapshot_attributes(published, source_key="source_a", previous_snapshot_id=None)
    candidate["snapshot_id"] = physical.snapshot_id
    candidate["run_report"] = deepcopy(candidate["manifest"])
    sources = [{"source_key": 0, "raw_container_sha256": (b"r" * 32).hex()}]
    candidate["frozen_source_records"] = sources
    evidence["initialization"]["destination_snapshot_id"] = physical.snapshot_id
    identity = _candidate_identity(candidate, physical_binding=physical)
    evidence["native_audit"]["identity"] = {
        key: value.hex() if isinstance(value, bytes) else value for key, value in identity.items()
    }
    evidence["activation_evidence"]["control_sha256"] = evidence["control_sha256"]
    return physical, candidate, evidence, sources


def _preparation_observations():
    """Bind an installed-model inventory, staged controls and audit to one protected header."""
    physical, candidate, evidence, frozen_sources = _staged_control_fixture()
    _original, header = _protected_preparation_fixture()
    protected = header["validation"]["evidence"]
    for key in ("ownership", "initialization", "activation_evidence", "control_sha256"):
        protected[key] = evidence[key]
    protected["native_audit"]["identity"] = evidence["native_audit"]["identity"]
    ownership = _ownership(physical)
    relations = [
        {"ordinal": index, "table_name": name, "relation_oid": oid}
        for index, (name, oid) in enumerate(ownership.relation_oids)
    ]
    sequences = [
        {"ordinal": index, "sequence_name": name, "sequence_oid": oid, "owner_table": table, "owner_column": column}
        for index, (name, oid, table, column) in enumerate(ownership.sequence_oids)
    ]
    inventory_by_field = {
        "schema_name": ownership.schema_name,
        "schema_oid": ownership.schema_oid,
        "relations": [
            {key: field_value for key, field_value in inventory_row.items() if key != "ordinal"}
            for inventory_row in relations
        ],
        "sequences": [
            {key: field_value for key, field_value in inventory_row.items() if key != "ordinal"}
            for inventory_row in sequences
        ],
    }
    columns = _local_catalog_columns()
    objects = [
        {"kind": "relation", "oid": oid, "name": name, "owner": physical.owner_oid, "definition": "r:p:1:0"}
        for name, oid in ownership.relation_oids
    ]
    protected["catalog_sha256"] = native._local_catalog_digest(ownership, objects, columns)
    protected["native_audit"]["catalog_sha256"] = protected["catalog_sha256"]
    protected["metadata_sha256"] = "b" * 64
    header.update(
        operation_id=UUID(int=1),
        package_id="a" * 64,
        inventory_sha256=native._native_metadata_digest(inventory_by_field),
    )
    header["validation"]["inventory_sha256"] = header["inventory_sha256"]
    header["validation_sha256"] = native._native_metadata_digest(header["validation"])
    return SimpleNamespace(
        physical=physical,
        ownership=ownership,
        candidate=candidate,
        sources=frozen_sources,
        header=header,
        evidence=protected,
        relations=relations,
        sequences=sequences,
        objects=objects,
        columns=columns,
    )


def _local_catalog_columns():
    return [
        {
            "table_name": model.__tablename__,
            "column_name": column.name,
            "data_type": str(column.type),
            "attnotnull": not column.nullable,
            "attidentity": "d" if column.identity else "",
            "attgenerated": "",
        }
        for model in native.local_data_family_spec().model_types
        for column in model.__table__.columns
    ]


def _preparation_session(monkeypatch, observed):
    """Fail on unknown SQL while exercising the real protected-header and postimage chain."""
    _observe_family(monkeypatch, observed.ownership)

    async def execute(statement, parameters=None):
        sql = str(statement)
        if sql.startswith("LOCK TABLE"):
            return _query_result()
        if "c.relowner::bigint AS owner_oid" in sql:
            return _query_result([{"owner_oid": observed.physical.owner_oid, "protected": True}])
        if sql.startswith("SELECT * FROM hp_snapshot_retention.reference_preparation"):
            return _query_result([observed.header])
        if sql.startswith("SELECT ordinal,table_name"):
            return _query_result(observed.relations)
        if sql.startswith("SELECT ordinal,sequence_name"):
            return _query_result(observed.sequences)
        if "AS object_count,bool_and" in sql:
            return _query_result([{"object_count": len(observed.relations) + len(observed.sequences), "closed": True}])
        if sql.startswith("SELECT 'relation' AS kind"):
            return _query_result(observed.objects)
        if sql.startswith("SELECT c.relname AS table_name"):
            return _query_result(observed.columns)
        if sql.startswith("SELECT snapshot.*,run.options"):
            return _query_result([] if observed.candidate is None else [observed.candidate])
        if ".ptg2_v3_snapshot_plan_scope" in sql:
            return _query_result([{"plan_id": "12-3456789", "plan_market_type": "group"}])
        if "SELECT source.source_key" in sql:
            return _query_result(observed.sources)
        raise AssertionError("unexpected query: " + sql)

    return SimpleNamespace(in_transaction=lambda: True, execute=AsyncMock(side_effect=execute), scalar=AsyncMock())


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", (None, "operation", "package", "metadata", "control", "identity"))
async def test_preparation_replay_authenticates_catalog_inventory_controls_and_audit(monkeypatch, fault):
    observed = _preparation_observations()
    session = _preparation_session(monkeypatch, observed)
    operation_by_field = {"operation_id": UUID(int=1), "package_id": "a" * 64}
    if fault == "operation":
        observed.header["operation_id"] = UUID(int=2)
    elif fault == "package":
        operation_by_field["package_id"] = "c" * 64
    elif fault == "control":
        observed.candidate["options"]["source_key"] = "other_source"
    elif fault == "identity":
        observed.evidence["native_audit"]["identity"]["snapshot_key"] += 1
        observed.header["validation_sha256"] = native._native_metadata_digest(observed.header["validation"])
    if fault:
        with pytest.raises(
            native.PTG2PhysicalBindingError, match="replay differs|control changed|audit identity changed"
        ):
            await native.revalidate_local_preparation(
                session, operation=operation_by_field, metadata_sha256="c" * 64 if fault == "metadata" else "b" * 64
            )
    else:
        assert (
            await native.revalidate_local_preparation(session, operation=operation_by_field, metadata_sha256="b" * 64)
            is observed.evidence
        )
    queries = [str(call.args[0]) for call in session.execute.await_args_list]
    assert sum(query.startswith(f'LOCK TABLE ONLY "{observed.physical.schema_name}".') for query in queries) == len(
        observed.relations
    )
    assert not any(query.startswith(("INSERT", "UPDATE", "DELETE", "DROP")) for query in queries)
    if fault in {"operation", "package", "metadata"}:
        assert not any(query.startswith("SELECT snapshot.*,run.options") for query in queries)
    if fault == "control":
        assert not any("SELECT source.source_key" in query for query in queries)


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", (None, "missing", "state", "report", "source", "previous", "layout", "digest"))
async def test_staged_control_requires_exact_replayed_preimage_before_audit(monkeypatch, fault):
    observed = _preparation_observations()
    session = _preparation_session(monkeypatch, observed)
    changes_by_fault = {
        "state": ("status", "published"),
        "report": ("run_report", {}),
        "previous": ("previous_snapshot_id", "other"),
        "layout": ("snapshot_key", 702),
    }
    if fault == "missing":
        observed.candidate = None
    elif fault in changes_by_fault:
        field, field_value = changes_by_fault[fault]
        observed.candidate[field] = field_value
    elif fault == "source":
        observed.candidate["options"]["source_key"] = "other"
    elif fault == "digest":
        observed.evidence["control_sha256"] = "0" * 64
    if fault:
        with pytest.raises(native.PTG2PhysicalBindingError, match="control is unavailable|control changed"):
            await native._require_local_control(session, "mrf", observed.evidence, observed.physical)
    else:
        assert (
            await native._require_local_control(session, "mrf", observed.evidence, observed.physical)
            == observed.candidate
        )
    query, parameters = session.execute.await_args_list[0].args
    assert "FOR SHARE OF snapshot,run,binding,scope,layout NOWAIT" in str(query)
    assert parameters == {
        "snapshot_id": observed.physical.snapshot_id,
        "payload_key": observed.physical.payload_snapshot_key,
    }
    assert session.execute.await_count == (1 if fault == "missing" else 2)


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ("count", "missing-table", "sequence", "digest"))
async def test_prepared_header_rejects_unbound_inventory_before_any_heap_lock(monkeypatch, fault):
    observed = _preparation_observations()
    session = _preparation_session(monkeypatch, observed)
    if fault == "count":
        session.execute.side_effect = lambda *_args: _query_result([])
    elif fault == "missing-table":
        observed.relations.pop()
    elif fault == "sequence":
        observed.sequences[0]["sequence_oid"] += 1
    else:
        observed.header["inventory_sha256"] = observed.header["validation"]["inventory_sha256"] = "0" * 64
        observed.header["validation_sha256"] = native._native_metadata_digest(observed.header["validation"])
    with pytest.raises(native.PTG2PhysicalBindingError, match="preparation is unavailable|protected inventory"):
        await native._prepared_local_header(
            session, observed.physical.snapshot_id, owner_oid=observed.physical.owner_oid
        )
    assert not any(str(call.args[0]).startswith("LOCK TABLE") for call in session.execute.await_args_list)


@pytest.mark.parametrize("fault", ("extra", "uuid", "importer", "schema", "relations", "sequence"))
def test_prepared_descriptor_cannot_relabel_or_truncate_a_family(fault):
    physical, header = _protected_preparation_fixture()
    evidence = header["validation"]["evidence"]
    ownership = evidence["ownership"]
    mutations_by_fault = {
        "extra": ("extra", True),
        "uuid": ("dataset_id", "invalid"),
        "importer": ("importer_id", "nucc"),
        "schema": ("schema_name", "other"),
        "relations": ("relation_oids", ownership["relation_oids"][:-1]),
        "sequence": ("sequence_oids", ()),
    }
    field, value = mutations_by_fault[fault]
    ownership[field] = value
    with pytest.raises(native.PTG2PhysicalBindingError, match="descriptor differs|sequence inventory differs"):
        native._prepared_local_binding(evidence, physical.snapshot_id, physical.owner_oid)


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", (None, "write-grant", "missing-object"))
async def test_session_custody_checks_both_heap_and_sequence_authority(fault):
    ownership = _ownership(_binding())
    object_count = len(ownership.relation_oids) + len(ownership.sequence_oids)
    session = SimpleNamespace(
        execute=AsyncMock(
            return_value=_query_result(
                [{"object_count": object_count - int(fault == "missing-object"), "closed": fault != "write-grant"}]
            )
        )
    )
    if fault:
        with pytest.raises(native.PTG2PhysicalBindingError, match="not closed"):
            await native._require_closed_local_custody(session, ownership, 102)
    else:
        await native._require_closed_local_custody(session, ownership, 102)
    query, parameters = session.execute.await_args.args
    assert parameters["objects"] == parameters["heaps"] + parameters["sequences"]
    assert parameters["sequences"] == [300] and parameters["owner_oid"] == 102
    assert "a.is_grantable OR c.relkind='S'" in str(query) and "contype='f'" in str(query)


@pytest.mark.asyncio
@pytest.mark.parametrize("in_transaction", (False, True))
async def test_driver_custody_refuses_no_transaction_or_missing_catalog_row(in_transaction):
    connection = SimpleNamespace(is_in_transaction=lambda: in_transaction, fetchrow=AsyncMock(return_value=None))
    with pytest.raises(native.PTG2PhysicalBindingError, match="requires a transaction|custody is unavailable"):
        await native.require_closed_local_driver_custody(connection, _ownership(_binding()), 102)
    assert connection.fetchrow.await_count == int(in_transaction)


@pytest.mark.asyncio
@pytest.mark.parametrize("in_transaction", (False, True))
async def test_published_control_refuses_missing_transaction_or_actual_control(monkeypatch, in_transaction):
    physical, _candidate, evidence, publication = _published_control_fixture()
    ownership = _ownership(physical)
    observed = _preparation_observations()
    evidence["catalog_sha256"] = native._local_catalog_digest(ownership, observed.objects, observed.columns)
    connection = SimpleNamespace(
        is_in_transaction=lambda: in_transaction,
        fetchrow=AsyncMock(side_effect=[{"object_count": 43, "closed": True}, None]),
        fetch=AsyncMock(side_effect=[observed.objects, observed.columns]),
    )
    with pytest.raises(native.PTG2PhysicalBindingError, match="requires a transaction|control is unavailable"):
        await native.require_local_published_control(connection, evidence=evidence, publication=publication)
    assert connection.fetchrow.await_count == 2 * int(in_transaction)
    assert connection.fetch.await_count == 2 * int(in_transaction)


@pytest.mark.parametrize("fault", ("contract", "extra", "payload-key", "schema-oid", "dataset"))
def test_publication_binding_rejects_valid_shape_with_different_physical_custody(fault):
    _physical, _candidate, evidence, publication = _published_control_fixture()
    changes_by_fault = {
        "contract": ("contract", "other"),
        "extra": ("extra", True),
        "payload-key": ("payload_snapshot_key", 20),
        "schema-oid": ("schema_oid", 103),
        "dataset": ("dataset_id", str(UUID(int=2))),
    }
    field, value = changes_by_fault[fault]
    publication[field] = value
    with pytest.raises(
        native.PTG2PhysicalBindingError, match="publication receipt differs|published inventory differs"
    ):
        native._local_publication_binding(evidence, publication)


@pytest.mark.parametrize("field", ("manifest", "options", "run_report"))
@pytest.mark.parametrize("encoding", ("json", "non-mapping"))
def test_published_postimage_decodes_json_but_refuses_non_mapping_controls(field, encoding):
    physical, candidate, evidence, publication = _published_control_fixture()
    candidate[field] = json.dumps(candidate[field]) if encoding == "json" else []
    plans = [{"plan_id": "12-3456789", "plan_market_type": "group"}]
    if encoding == "json":
        native._require_local_published_postimage(candidate, plans, evidence, publication, physical)
    else:
        with pytest.raises(native.PTG2PhysicalBindingError, match="control encoding differs"):
            native._require_local_published_postimage(candidate, plans, evidence, publication, physical)


@pytest.mark.asyncio
@pytest.mark.parametrize("selector", ("missing", "run-absent", "snapshot-absent", "ambiguous", "wrong-contract"))
async def test_audit_selector_never_chooses_ambiguous_or_foreign_local_authority(selector):
    rows = []
    if selector in {"ambiguous", "wrong-contract"}:
        rows = [
            {"snapshot_id": "synthetic", "import_run_id": "run", "manifest": {"physical_binding_contract": "other"}}
        ]
        if selector == "ambiguous":
            rows *= 2
    session = SimpleNamespace(
        in_transaction=lambda: True, execute=AsyncMock(return_value=_query_result(rows)), scalar=AsyncMock()
    )
    selector_by_field = {"candidate_run_id": "run"} if selector == "run-absent" else {"snapshot_id": "synthetic"}
    if selector == "missing":
        selector_by_field = {}
    if selector == "run-absent":
        assert await native.local_candidate_audit_state(session, **selector_by_field) is None
    else:
        with pytest.raises(
            native.PTG2PhysicalBindingError, match="selector is missing|selector differs|declaration differs"
        ):
            await native.local_candidate_audit_state(session, **selector_by_field)
    assert session.execute.await_count == int(selector != "missing")
    session.scalar.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("safe", (False, None, 1))
async def test_read_view_cannot_promote_owner_membership_or_schema_create(monkeypatch, safe):
    session = SimpleNamespace(
        in_transaction=lambda: True,
        execute=AsyncMock(return_value=_query_result([{"owner_oid": 102, "protected": True}])),
        scalar=AsyncMock(return_value=safe),
    )
    with pytest.raises(native.PTG2PhysicalBindingError, match="reader privileges differ"):
        await native.require_local_physical_read_view(session, is_prepared=False)
    assert session.execute.await_count == 3
    assert not any(str(call.args[0]).startswith("LOCK") for call in session.execute.await_args_list)
    assert "'MEMBER'" in str(session.scalar.await_args.args[0])
    assert "publication_sha256" in native._local_read_view_columns(False)
    assert "publication_sha256" not in native._local_read_view_columns(True)


@pytest.mark.asyncio
async def test_local_family_wrappers_keep_identity_and_restrictive_cleanup(monkeypatch):
    ownership = _ownership(_binding())
    _observe_family(monkeypatch, ownership)
    session = SimpleNamespace(in_transaction=lambda: True, execute=AsyncMock(), scalar=AsyncMock(side_effect=[101, 0]))
    assert await native.precreate_local_data_family(session, ownership.dataset_id) == ownership
    created_statements = [
        str(call.args[0].compile(dialect=archive.postgresql.dialect())) for call in session.execute.await_args_list
    ]
    assert len(created_statements) == len(ownership.relation_oids) + 1
    assert "GENERATED BY DEFAULT AS IDENTITY" in "\n".join(created_statements)
    assert "FOREIGN KEY" not in "\n".join(created_statements) and "CREATE INDEX" not in "\n".join(created_statements)
    assert set(native.physical_family_tables()) <= set(native.local_data_family_spec().table_names)
    session.execute.reset_mock()
    await native.cleanup_local_data_family(session, ownership)
    statements = [str(call.args[0]) for call in session.execute.await_args_list]
    assert "ACCESS EXCLUSIVE MODE NOWAIT" in statements[0]
    assert statements[1].endswith(" RESTRICT") and "CASCADE" not in "\n".join(statements)
    assert statements[2] == f'DROP SCHEMA "{ownership.schema_name}"'


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ("invalid", "changed"))
async def test_local_ownership_verification_rejects_non_token_and_rebound_schema(monkeypatch, fault):
    ownership = _ownership(_binding())
    relation = _observe_family(monkeypatch, ownership)
    if fault == "changed":
        monkeypatch.setattr(archive, "_schema_oid", AsyncMock(return_value=ownership.schema_oid + 1))
    session = SimpleNamespace(in_transaction=lambda: True)
    with pytest.raises(native.PTG2PhysicalBindingError, match="ownership is invalid|ownership changed"):
        await native.verify_local_data_family(session, object() if fault == "invalid" else ownership)
    assert relation.await_count == (0 if fault == "invalid" else len(ownership.relation_oids))


@pytest.mark.asyncio
async def test_local_payload_scope_checks_coverage_as_a_set_before_next_model():
    statements = []

    async def scalar(statement, parameters=None):
        statements.append((str(statement), parameters))
        return (
            "coverage_scope_id IS DISTINCT FROM" in str(statement) if str(statement).startswith("SELECT EXISTS") else 1
        )

    scope = _serving_scope()
    with pytest.raises(native.PTG2PhysicalBindingError, match="payload coverage differs"):
        await native._local_payload_counts(
            SimpleNamespace(scalar=scalar),
            _ownership(_binding()),
            snapshot_id=scope["snapshot_id"],
            snapshot_key=19,
            scope=scope,
        )
    assert statements[-1][1] == {"expected": bytes.fromhex(scope["coverage_scope_id"])}
    assert not any("FOR UPDATE" in query for query, _parameters in statements)


def _canonical_observations():
    dataset_id = UUID(int=42)
    ownership = archive.ReferenceFamilyStageOwnership(
        "address-canonical",
        dataset_id,
        archive.reference_family_stage_schema(dataset_id),
        41,
        (("address_archive_v2", 43),),
    )
    columns = [
        {"attnum": number, "attname": column.name, "data_type": str(column.type), "attnotnull": not column.nullable}
        for number, column in enumerate(canonical.AddressArchiveV2.__table__.columns, 1)
    ]
    relation_by_field = {
        "relation_name": "address_archive_v2",
        "relation_oid": 43,
        "schema_name": ownership.schema_name,
        "schema_oid": 41,
        "schema_owner_oid": 45,
        "owner_oid": 45,
        "relfilenode": 44,
    }
    receipt_by_field = {
        "contract": "canonical-retained-base.v1",
        "dataset_id": str(dataset_id),
        "inventory": {"database_oid": 7, "relations": [dict(relation_by_field)]},
        "catalog_sha256": canonical.catalog_identity._canonical_digest(
            canonical.normalized_canonical_catalog(columns, [], [])
        ),
    }
    return SimpleNamespace(
        ownership=ownership,
        columns=columns,
        relation=relation_by_field,
        receipt=receipt_by_field,
        publisher_oid=45,
        is_custody_closed=True,
        is_enum_accepted=True,
        is_shape_changed=False,
        is_code_owned=False,
        is_relationship_invalid=False,
        is_publication_allowed=True,
        is_merge_conflicted=False,
        is_publication_incomplete=False,
        is_drop_failed=False,
    )


def _canonical_execute(observed, statement, parameters=None):
    sql = str(statement)
    if sql.startswith("DROP TABLE") and observed.is_drop_failed:
        raise RuntimeError("drop failed")
    if sql.startswith(("SET LOCAL", "SELECT set_config", "DROP TABLE", "LOCK TABLE", "INSERT INTO", "UPDATE")):
        return _query_result()
    if sql.lstrip().startswith(("CREATE TEMPORARY TABLE", "CREATE INDEX", "CREATE UNIQUE INDEX")):
        return _query_result()
    if "AS owner_safe" in sql:
        return _query_result([{"owner_oid": observed.publisher_oid, "owner_safe": True, "caller_safe": True}])
    if sql.startswith("SELECT c.relname,c.oid::bigint"):
        return _query_result([{"relname": "address_archive_v2", "oid": 43, "relfilenode": 44}])
    if "AS relation_oid,c.relname::text AS relation_name" in sql:
        return _query_result([{**observed.relation, "closed": True}])
    if "AS object_count,bool_and" in sql:
        return _query_result([{"object_count": 1, "closed": observed.is_custody_closed}])
    if "t.typtype='e'" in sql:
        return _query_result([{"oid": 500, "accepted": observed.is_enum_accepted}])
    raise AssertionError("unexpected query: " + sql)


def _canonical_scalar(observed, statement, parameters=None):
    sql = str(statement)
    fixed_by_sql = {
        "SHOW search_path": "public",
        "SELECT relowner::bigint FROM pg_class WHERE oid=:oid": observed.relation["owner_oid"],
        "SELECT oid::bigint FROM pg_database WHERE datname=current_database()": 7,
        "SELECT pg_catalog.pg_relation_filenode(CAST(:oid AS oid))::bigint": 44,
        "SELECT to_regclass(:relation)::oid": 144,
        "SELECT to_regclass(:archive)::oid": 43,
        "SELECT to_regclass(:stage)::oid": 46,
    }
    if sql in fixed_by_sql:
        return fixed_by_sql[sql]
    if sql.startswith(("SELECT count(*)=cardinality", "SELECT NOT EXISTS(SELECT 1 FROM pg_index")):
        return True
    if sql.startswith("WITH objects(classid,objid)"):
        return observed.is_code_owned
    if sql.startswith("SELECT to_regtype('public.geography')"):
        return False
    if sql.startswith("SELECT (SELECT count(*) FROM pg_catalog.pg_constraint"):
        return observed.is_relationship_invalid
    if sql.startswith("SELECT pg_catalog.has_table_privilege"):
        return observed.is_publication_allowed
    if "contribution WHERE NOT EXISTS" in sql:
        return observed.is_publication_incomplete
    if "contribution JOIN" in sql:
        return observed.is_merge_conflicted
    raise AssertionError("unexpected scalar: " + sql)


def _canonical_session(monkeypatch, observed):
    _observe_family(monkeypatch, observed.ownership)

    async def columns(_session, oid):
        rows = deepcopy(observed.columns)
        if oid == 144 and observed.is_shape_changed:
            rows[0]["attnotnull"] = not rows[0]["attnotnull"]
        return rows

    monkeypatch.setattr(canonical.catalog_identity, "_catalog_columns", AsyncMock(side_effect=columns))
    monkeypatch.setattr(canonical.catalog_identity, "_catalog_constraints", AsyncMock(return_value=[]))
    monkeypatch.setattr(canonical.catalog_identity, "_catalog_indexes", AsyncMock(return_value=[]))
    return SimpleNamespace(
        in_transaction=lambda: True,
        execute=AsyncMock(side_effect=lambda *args: _canonical_execute(observed, *args)),
        scalar=AsyncMock(side_effect=lambda *args: _canonical_scalar(observed, *args)),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("is_transaction_active", (False, True))
async def test_canonical_source_fence_is_captured_from_the_fixed_live_lock(is_transaction_active):
    transaction = object()
    session = SimpleNamespace(
        in_transaction=lambda: is_transaction_active,
        get_transaction=lambda: transaction,
        execute=AsyncMock(
            side_effect=[
                _query_result(),
                _query_result(
                    [
                        {"pid": 21, "database": 22, "relation": 23, "virtualtransaction": "24/25"},
                    ]
                ),
            ]
        ),
    )
    if not is_transaction_active:
        with pytest.raises(RuntimeError, match="transaction is unavailable"):
            await canonical.capture_canonical_source_fence(session)
        session.execute.assert_not_awaited()
        return
    assert await canonical.capture_canonical_source_fence(session) == canonical.CanonicalSourceFence(
        session, transaction, 21, 22, 23, "24/25"
    )
    assert str(session.execute.await_args_list[0].args[0]) == (
        'LOCK TABLE "mrf"."address_archive_v2" IN SHARE ROW EXCLUSIVE MODE NOWAIT'
    )
    assert "l.pid=pg_catalog.pg_backend_pid()" in str(session.execute.await_args.args[0])


@pytest.mark.parametrize(
    "field,value", (("key_columns", "{1}"), ("referenced_table", "foreign"), ("referenced_in_archive_schema", False))
)
def test_canonical_catalog_does_not_normalize_away_foreign_relationships(field, value):
    columns = [{"attnum": 1, "attname": "address_key"}, {"attnum": 2, "attname": "merged_into"}]
    constraint_by_field = {
        "contype": "f",
        "key_columns": "{2}",
        "referenced_table": "address_archive_v2",
        "referenced_in_archive_schema": True,
    }
    constraint_by_field[field] = value
    with pytest.raises(RuntimeError, match="relationship catalog is unsupported"):
        canonical.normalized_canonical_catalog(columns, [constraint_by_field], [])


@pytest.mark.asyncio
@pytest.mark.parametrize("oid", (None, 0, True))
async def test_canonical_source_missing_oid_refuses_before_enum_or_shape(oid):
    session = SimpleNamespace(scalar=AsyncMock(return_value=oid), execute=AsyncMock())
    with pytest.raises(RuntimeError, match="source model is unavailable"):
        await canonical.require_canonical_source_model(session, "mrf", canonical._qualified)
    session.execute.assert_not_awaited()
    assert session.scalar.await_args.args[1] == {"archive": '"mrf"."address_archive_v2"'}


@pytest.mark.asyncio
@pytest.mark.parametrize("row", (None, {"oid": 500, "accepted": False}, {"oid": 500, "accepted": 1}))
async def test_canonical_enum_requires_exact_native_labels_and_non_member_owner(row):
    session = SimpleNamespace(execute=AsyncMock(return_value=_query_result([] if row is None else [row])))
    with pytest.raises(RuntimeError, match="source enum differs"):
        await canonical._canonical_enum_oid(session)
    query = str(session.execute.await_args.args[0])
    assert "array_agg(e.enumlabel ORDER BY e.enumsortorder)" in query
    assert "NOT pg_catalog.pg_has_role(current_user,t.typowner,'MEMBER')" in query


@pytest.mark.asyncio
@pytest.mark.parametrize("oids", ((), (True,), (0,), (43, -1)))
async def test_native_source_catalog_rejects_incomplete_oid_scope_before_queries(oids):
    session = SimpleNamespace(scalar=AsyncMock())
    with pytest.raises(RuntimeError, match="catalog is incomplete"):
        await canonical.require_native_read_catalog(session, oids)
    session.scalar.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ("relationships", "code-owner"))
async def test_canonical_source_refuses_relationship_or_callable_ownership_before_temp_ddl(monkeypatch, fault):
    observed = _canonical_observations()
    observed.is_relationship_invalid = fault == "relationships"
    observed.is_code_owned = fault == "code-owner"
    session = _canonical_session(monkeypatch, observed)
    with pytest.raises(RuntimeError, match="relationship differs|executable ownership is unsupported"):
        await canonical.require_canonical_source_model(session, "mrf", canonical._qualified)
    assert not any("CREATE" in str(call.args[0]) for call in session.execute.await_args_list)
    assert canonical.catalog_identity._catalog_columns.await_count == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", (None, "shape", "cancel", "cancel-and-drop", "query-and-drop"))
async def test_canonical_model_witness_is_temporary_and_cleanup_preserves_original_failure(monkeypatch, fault):
    observed = _canonical_observations()
    observed.is_shape_changed = fault == "shape"
    observed.is_drop_failed = fault in {"cancel-and-drop", "query-and-drop"}
    session = _canonical_session(monkeypatch, observed)
    failure = asyncio.CancelledError() if fault and fault.startswith("cancel") else RuntimeError("catalog read failed")
    if fault in {"cancel", "cancel-and-drop", "query-and-drop"}:
        canonical.catalog_identity._catalog_columns.side_effect = failure
    if fault:
        with pytest.raises(type(failure) if fault != "shape" else RuntimeError) as caught:
            await canonical._require_canonical_shape(session, "mrf", 43, canonical._qualified)
        if fault == "shape":
            assert str(caught.value) == "canonical source native catalog differs"
        else:
            assert caught.value is failure
    else:
        await canonical._require_canonical_shape(session, "mrf", 43, canonical._qualified)
    statements = [
        str(call.args[0].compile(dialect=archive.postgresql.dialect())) for call in session.execute.await_args_list
    ]
    created = next(sql for sql in statements if "CREATE TEMPORARY TABLE" in sql)
    assert "ON COMMIT DROP" in created and "FOREIGN KEY" not in created
    assert sum(sql.startswith('DROP TABLE "pg_temp"."address_model_') for sql in statements) == 1
    assert statements[-1].startswith("SELECT set_config('search_path'")
    assert session.execute.await_args.args[1] == {"path": "public"}


@pytest.mark.asyncio
@pytest.mark.parametrize("contributions", ({}, {"foreign": '"stage"."address"'}))
async def test_canonical_contribution_scope_refuses_unknown_importers_without_payload_queries(contributions):
    session = SimpleNamespace(scalar=AsyncMock())
    with pytest.raises(RuntimeError, match="contribution scope differs"):
        await canonical.validate_canonical_contributions(session, contributions, "incumbent")
    with pytest.raises(RuntimeError, match="contribution scope differs"):
        canonical.canonical_archive_projection(contributions, "incumbent")
    with pytest.raises(RuntimeError, match="importer is unsupported"):
        canonical.canonical_reference_filter("foreign", "stage", canonical._qualified)
    session.scalar.assert_not_awaited()


@pytest.mark.parametrize("importer_id,bit,priority", (("npi", 1, 0), ("mrf", 16, 5)))
def test_resolution_projection_preserves_incumbents_alias_identity_and_noop_timestamps(importer_id, bit, priority):
    sql = canonical.canonical_resolution_projection(
        importer_id,
        archive='"mrf"."address_archive_v2"',
        deduplicated="incoming_rows",
        strict="strict_rows",
        formatted="formatted_rows",
    )
    assert 'FROM "mrf"."address_archive_v2" incumbent FULL JOIN incoming_rows incoming USING(address_key)' in sql
    assert "strict.identity_key=COALESCE(incumbent.identity_key,incoming.identity_key)" in sql
    for column in canonical.AddressArchiveV2.__table__.columns:
        assert f" AS {column.name}" in sql
    assert f"THEN {bit} ELSE 0 END AS source_bits" in sql
    assert f"THEN {bit} ELSE 0 END AS strict_source_bits" in sql
    assert "ELSE incumbent.last_seen_at END AS last_seen_at" in sql
    assert f"least(incumbent.display_priority,{priority})" in sql
    assert "ELSE incumbent.formatted_address END AS formatted_address" in sql
    assert "THEN NULL ELSE incumbent.merged_into END AS merged_into" in sql
    assert "THEN incoming.identity_key ELSE incumbent.identity_key END AS identity_key" in sql


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", (None, "rebound", "custody", "enum"))
async def test_canonical_output_custody_authenticates_whole_heap_before_native_reads(monkeypatch, fault):
    observed = _canonical_observations()
    observed.is_custody_closed = fault != "custody"
    observed.is_enum_accepted = fault != "enum"
    session = _canonical_session(monkeypatch, observed)
    ownership = replace(observed.ownership, schema_oid=42) if fault == "rebound" else observed.ownership
    if fault:
        with pytest.raises(RuntimeError, match="inventory changed|not closed|source enum differs"):
            await canonical._require_canonical_custody(session, ownership, 45)
        session.scalar.assert_not_awaited()
    else:
        await canonical._require_canonical_custody(session, ownership, 45)
        assert session.scalar.await_count == 3
        assert session.scalar.await_args_list[1].args[1]["allowed_type_oids"] == [500]
    assert session.execute.await_count == {"rebound": 0, "custody": 3, "enum": 4, None: 4}[fault]


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", (None, "contract", "scope", "inventory", "owner", "catalog"))
async def test_canonical_retained_base_requires_live_custody_and_exact_receipt_before_shape(monkeypatch, fault):
    observed = _canonical_observations()
    session = _canonical_session(monkeypatch, observed)
    evidence = observed.receipt
    if fault == "contract":
        evidence["contract"] = "foreign"
    elif fault == "inventory":
        evidence["inventory"]["relations"][0]["relfilenode"] += 1
    elif fault == "owner":
        observed.publisher_oid += 1
    elif fault == "catalog":
        evidence["catalog_sha256"] = "0" * 64
    qualified = canonical._qualified(
        "foreign" if fault == "scope" else observed.ownership.schema_name, "address_archive_v2"
    )
    if fault:
        with pytest.raises(RuntimeError, match="retained base .* differs"):
            await canonical.require_canonical_output_base(session, qualified, evidence)
    else:
        await canonical.require_canonical_output_base(session, qualified, evidence)
    queries = [str(call.args[0]) for call in session.execute.await_args_list]
    assert not any(sql.startswith(("INSERT", "UPDATE", "DELETE")) for sql in queries)
    assert sum("CREATE TEMPORARY TABLE" in sql for sql in queries) == int(fault is None)
    if fault in {"contract", "scope"}:
        session.execute.assert_not_awaited()
        session.scalar.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "qualified",
    ('mrf."address_archive_v2"', '"mrf"."other"', '""."address_archive_v2"', '"mrf;other"."address_archive_v2"'),
)
async def test_canonical_publication_cannot_redirect_fixed_relation_or_schema(qualified):
    session = SimpleNamespace(scalar=AsyncMock())
    with pytest.raises(RuntimeError, match="publication relation differs|publication schema differs"):
        await canonical.require_canonical_publication_catalog(session, qualified)
    session.scalar.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", (None, "capability", "identity", "coverage"))
async def test_canonical_merge_refusals_leave_transaction_decision_with_caller(monkeypatch, fault):
    observed = _canonical_observations()
    observed.is_publication_allowed = fault != "capability"
    observed.is_merge_conflicted = fault == "identity"
    observed.is_publication_incomplete = fault == "coverage"
    session = _canonical_session(monkeypatch, observed)
    session.commit, session.rollback = AsyncMock(), AsyncMock()
    args = session, "mrf", '"stage"."mrf_canonical_address"', '"mrf"."address_archive_v2"'
    if fault:
        with pytest.raises(RuntimeError, match="capability is unavailable|identity conflicts|publication differs"):
            await canonical.merge_canonical_contribution(*args)
    else:
        assert (
            canonical.validate_canonical_publication(await canonical.merge_canonical_contribution(*args), "mrf", 46)[
                "archive_oid"
            ]
            == 43
        )
    queries = [str(call.args[0]) for call in session.execute.await_args_list]
    assert queries[0] == 'LOCK TABLE "mrf"."address_archive_v2" IN SHARE ROW EXCLUSIVE MODE NOWAIT'
    writes = [sql for sql in queries if sql.startswith(("INSERT", "UPDATE"))]
    assert len(writes) == (2 if fault in {None, "coverage"} else 0)
    assert not any("FOR UPDATE" in sql for sql in queries)
    session.commit.assert_not_awaited()
    session.rollback.assert_not_awaited()
    if fault == "coverage":
        assert "WHERE NOT EXISTS" in str(session.scalar.await_args.args[0])


@pytest.mark.asyncio
@pytest.mark.parametrize("digest", (None, "a" * 64, b"short"))
async def test_native_publication_wrapper_requires_exact_held_attestation_before_sql(digest):
    session = SimpleNamespace(execute=AsyncMock(), scalar=AsyncMock(), commit=AsyncMock(), rollback=AsyncMock())
    with pytest.raises(native.PTG2PhysicalBindingError, match="exact held attestation"):
        await native.publish_local_data_candidate_in_transaction(
            session,
            operation={"operation_id": UUID(int=1)},
            expected_attestation_digest=digest,
            rollback_owner_id="synthetic-retention",
        )
    session.execute.assert_not_awaited()
    session.scalar.assert_not_awaited()
    session.commit.assert_not_awaited()
    session.rollback.assert_not_awaited()


@pytest.mark.asyncio
async def test_native_binding_resolver_refuses_unowned_reader_transaction_before_authority_queries():
    session = SimpleNamespace(in_transaction=lambda: False, execute=AsyncMock(), scalar=AsyncMock())
    with pytest.raises(native.PTG2PhysicalBindingError, match="caller transaction"):
        await native.resolve_local_physical_binding(session, "synthetic-snapshot")
    session.execute.assert_not_awaited()
    session.scalar.assert_not_awaited()


@pytest.mark.asyncio
async def test_native_audit_declaration_cannot_fall_back_when_prepared_view_is_missing():
    snapshot_id = "snapshot-archive-" + str(UUID(int=1))
    session = SimpleNamespace(
        in_transaction=lambda: True,
        execute=AsyncMock(
            return_value=_query_result(
                [
                    {
                        "snapshot_id": snapshot_id,
                        "manifest": {"physical_binding_contract": native.PHYSICAL_BINDING_CONTRACT},
                    }
                ]
            )
        ),
        scalar=AsyncMock(return_value=False),
    )
    with pytest.raises(native.PTG2PhysicalBindingError, match="preparation authority is unavailable"):
        await native.local_candidate_audit_state(session, snapshot_id=snapshot_id)
    assert session.execute.await_count == session.scalar.await_count == 1
    assert session.scalar.await_args.args[1] == {"table": "mrf.ptg2_prepared_physical_binding"}


@pytest.mark.asyncio
async def test_graph_read_refuses_authenticated_root_for_another_payload_before_io():
    from api import ptg2_v4_graph as graph

    session = SimpleNamespace(execute=AsyncMock())
    request = graph._V4RelationLookupRequest(
        snapshot_key=19,
        relation="npi_groups_exact",
        owner_keys=(1,),
        schema_name="mrf",
        max_members=8,
        authenticated_root=graph.V4GraphRoot(20, "pattern_v1", b"m" * 32),
    )
    with pytest.raises(graph.PTG2SharedBlockError, match="changed snapshot identity"):
        await graph._prepare_v4_relation_lookup(session, request)
    session.execute.assert_not_awaited()


@pytest.mark.parametrize("value", (None, "invalid"))
def test_graph_manifest_count_refuses_non_numeric_metadata_with_original_cause(value):
    from api import ptg2_v4_graph as graph

    with pytest.raises(graph.PTG2SharedBlockError, match="invalid entry_count") as caught:
        graph._strict_manifest_int({"entry_count": value}, "entry_count")
    assert isinstance(caught.value.__cause__, (TypeError, ValueError))


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ("unexpected", "duplicate", "count"))
async def test_graph_cas_refuses_unselected_duplicate_or_miscounted_authenticated_blocks(monkeypatch, fault):
    from api import ptg2_v4_graph as graph
    from process.ptg_parts.ptg2_shared_blocks import SharedBlock

    block = SharedBlock("v4_npi_groups_exact_members_v1", 0, 0, 1, "none", 4, b"\x01\x00\x00\x00")
    block_by_field = {
        **vars(block),
        "block_hash": block.block_hash,
        "block_entry_count": block.entry_count,
        "stored_byte_count": block.stored_byte_count,
    }
    coordinate = replace(block.reference(), entry_count=2) if fault == "count" else block.reference()
    selected_hash = b"x" * 32 if fault == "unexpected" else block.block_hash
    session = SimpleNamespace(
        execute=AsyncMock(return_value=_query_result([block_by_field] * (2 if fault == "duplicate" else 1)))
    )
    monkeypatch.setattr(graph, "_PHYSICAL_BLOCK_CACHE", graph._ByteLRU(1024))
    with (
        graph.v4_graph_request_scope(),
        pytest.raises(graph.PTG2SharedBlockError, match="unexpected block|count is inconsistent"),
    ):
        await graph._fetch_missing_physical_blocks(
            session, "mrf", block.object_kind, 4, {selected_hash: coordinate}, {}, [selected_hash]
        )
    query, parameters = session.execute.await_args.args
    assert '"mrf".ptg2_v3_block' in str(query)
    assert parameters == {"block_hashes": [selected_hash]}


def test_graph_map_pack_refuses_authenticated_payload_with_wrong_coordinate_census():
    from api import ptg2_v4_graph as graph
    from process.ptg_parts.ptg2_shared_blocks import SharedBlock
    from process.ptg_parts.ptg2_v4_snapshot_maps import encode_v4_snapshot_map_pack

    target = SharedBlock("v4_npi_groups_exact_members_v1", 0, 0, 1, "none", 4, b"\x01\x00\x00\x00")
    payload = encode_v4_snapshot_map_pack(target.object_kind, (target.reference(),))
    block = SharedBlock(graph.PTG2_V4_MAP_BLOCK_KIND, 0, 0, 1, "none", len(payload), payload)
    block_by_field = {
        **vars(block),
        "block_hash": block.block_hash,
        "block_entry_count": block.entry_count,
        "stored_byte_count": block.stored_byte_count,
        "coordinate_count": 2,
    }
    retained_by_pair = {}
    with (
        graph.v4_graph_request_scope(),
        pytest.raises(graph.PTG2SharedBlockError, match="count does not match its root"),
    ):
        graph._retain_map_pack_coordinates(
            block_by_field,
            schema_name="mrf",
            snapshot_key=19,
            object_kind=target.object_kind,
            missing_pairs={(0, 0)},
            coordinates_by_pair=retained_by_pair,
        )
    assert retained_by_pair == {}


def test_graph_bitmap_fragment_refuses_framed_count_that_disagrees_with_actual_bits():
    from api import ptg2_v4_graph as graph
    from tests.test_ptg2_v4_graph import _HeavyBitmapFragmentFixture

    observed = _HeavyBitmapFragmentFixture()
    logical_payload = observed.fragment_payloads[0][graph.PTG2_V4_HEAVY_BITMAP_FRAGMENT_HEADER_BYTES :]
    frame = observed._frame(0, 2, logical_payload)
    with pytest.raises(graph.PTG2SharedBlockError, match="fragment entry count changed"):
        graph._unframe_heavy_bitmap_fragment(
            frame, heavy_owner=observed.owner, fragment_no=0, entry_count=2, logical_offset=0
        )


@pytest.mark.parametrize("selection", ("full", "prefix"))
def test_graph_bitmap_materialization_refuses_authentic_but_incomplete_owner_fragments(selection):
    from api import ptg2_v4_graph as graph
    from tests.test_ptg2_v4_graph import _HeavyBitmapFragmentFixture

    observed = _HeavyBitmapFragmentFixture()
    coordinate = observed.coordinates_by_key[(7, 0)]
    blocks_by_hash = {
        coordinate.block_hash: graph._CachedPhysicalBlock(
            coordinate.block_hash, observed.owner.object_kind, coordinate.entry_count, observed.fragment_payloads[0]
        )
    }
    with pytest.raises(
        graph.PTG2SharedBlockError, match="member count changed|does not prove its requested members"
    ) as caught:
        if selection == "full":
            graph._assemble_heavy_bitmap_payloads(
                {7: replace(observed.owner, fragment_count=1)}, {(7, 0): coordinate}, blocks_by_hash
            )
        else:
            graph._decode_heavy_owner_prefix(observed.owner, (coordinate,), blocks_by_hash, observed.owner.member_count)
    assert caught.traceback[-1].name == (
        "_assemble_heavy_bitmap_payloads" if selection == "full" else "_decode_heavy_owner_prefix"
    )


def test_graph_bitmap_prefix_refuses_more_members_than_its_validated_fragment_proves():
    from api import ptg2_v4_graph as graph
    from tests.test_ptg2_v4_graph import _HeavyBitmapFragmentFixture

    observed = _HeavyBitmapFragmentFixture()
    coordinate = observed.coordinates_by_key[(7, 0)]
    logical_prefix = graph._unframe_heavy_bitmap_fragment(
        observed.fragment_payloads[0],
        heavy_owner=observed.owner,
        fragment_no=0,
        entry_count=coordinate.entry_count,
        logical_offset=0,
    )
    assert graph._decode_heavy_bitmap_prefix(logical_prefix, heavy_owner=observed.owner, limit=3) == (100, 101, 102)
    with pytest.raises(graph.PTG2SharedBlockError, match="does not prove its requested members") as caught:
        graph._decode_heavy_bitmap_prefix(logical_prefix, heavy_owner=observed.owner, limit=4)
    assert caught.traceback[-1].name == "_decode_heavy_bitmap_prefix"


def test_graph_bitmap_page_refuses_capacity_that_cannot_hold_its_logical_header():
    from api import ptg2_v4_graph as graph
    from tests.test_ptg2_v4_graph import _heavy_bitmap_relation_manifest

    manifest = replace(
        _heavy_bitmap_relation_manifest(),
        member_page_bytes=graph.PTG2_V4_HEAVY_BITMAP_FRAGMENT_HEADER_BYTES + graph.PTG2_V4_HEAVY_BITMAP_HEADER_BYTES,
    )
    with pytest.raises(graph.PTG2SharedBlockError, match="cannot contain its logical header"):
        graph._heavy_bitmap_fragment_content_bytes(manifest)


@pytest.mark.asyncio
async def test_graph_selected_bitmap_cannot_combine_intersection_and_prefix_before_io():
    from api import ptg2_v4_graph as graph
    from tests.test_ptg2_v4_graph import _heavy_bitmap_relation_manifest, _HeavyBitmapFragmentFixture

    session = SimpleNamespace(execute=AsyncMock())
    owner = _HeavyBitmapFragmentFixture().owner
    with pytest.raises(graph.PTG2SharedBlockError, match="cannot combine with owner prefixes"):
        await graph._lookup_v4_selected_heavy_members(
            session,
            snapshot_key=19,
            schema_name="mrf",
            relation_manifest=_heavy_bitmap_relation_manifest(),
            heavy_owners={owner.owner_key: owner},
            per_owner_limit=1,
            allowed_member_keys=(101,),
        )
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_graph_ordered_prefix_refuses_negative_budget_before_io():
    from api import ptg2_v4_graph as graph

    session = SimpleNamespace(execute=AsyncMock())
    with pytest.raises(graph.PTG2SharedBlockError, match="maximum cannot be negative"):
        await graph.lookup_v4_ordered_prefixes(
            session, snapshot_key=19, provider_set_keys=(1,), schema_name="mrf", max_members=-1
        )
    session.execute.assert_not_awaited()


@pytest.mark.parametrize("pages", ({}, {0: (1,)}))
def test_graph_owner_locator_cannot_read_missing_or_truncated_member_page(pages):
    from api import ptg2_v4_graph as graph
    from tests.test_ptg2_v4_graph import _regular_lookup_fixture

    with pytest.raises(graph.PTG2SharedBlockError, match="locator points outside its member page"):
        graph._decode_v4_owner_members(_regular_lookup_fixture(), 1, 1, pages)
