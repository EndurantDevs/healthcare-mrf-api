# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Synthetic custody failures and read-only native catalog statement contracts."""

import datetime as dt
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from sqlalchemy.dialects import postgresql

from process.custom_import import query_projection_binding as binding
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.query_projection_sources import capability_sha256
from process.custom_import.query_projection_storage import QUERY_PROJECTION_BINDING_INDEX, QUERY_PROJECTION_CONTRACT
from process.custom_import.runner_types import CandidateRunnerError
from tests.test_custom_import_derived_query import definition, derived_document

_NOW = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)
_ROOT = "custom_import_derived_root"
_CHILD = "custom_import_derived_child"
_DEFINITION = definition()


def _manifest(**changes):
    has_complete_proof = changes.get("phase", "complete") == "complete"
    return SimpleNamespace(
        **dict(
            family_id=17,
            generation_id=13,
            dataset_id=11,
            definition_revision_id=7,
            schema_revision_id=5,
            execution_id=3,
            producing_fence=2,
            producing_token_sha256=b"t" * 32,
            projection_contract=QUERY_PROJECTION_CONTRACT,
            definition_sha256=bytes.fromhex(_DEFINITION.digest),
            capability_sha256=capability_sha256(_DEFINITION),
            root_table_oid=101,
            root_table_owner=42,
            root_columns_sha256=b"r" * 32,
            child_table_oid=102,
            child_table_owner=42,
            child_columns_sha256=b"k" * 32,
            after_entity_binding_id=999,
            root_count=10,
            child_count=20,
            projection_sha256=b"p" * 32,
            complete_at=_NOW,
            phase="complete",
            index_count=4,
            verified_root_count=10 if has_complete_proof else 0,
            verified_child_count=20 if has_complete_proof else 0,
            digested_root_count=10 if has_complete_proof else 0,
            digested_child_count=20 if has_complete_proof else 0,
            digest_chain_sha256=b"c" * 32 if has_complete_proof else None,
        )
        | changes
    )


@pytest.mark.parametrize(
    "changes",
    [
        {"verified_root_count": True},
        {"verified_root_count": -1},
        {"verified_root_count": 11},
        {"digested_child_count": 19},
        {"digest_chain_sha256": None},
        {"digest_chain_sha256": b"short"},
        {"phase": "root", "verified_root_count": 1},
        {"phase": "digest_child", "digested_child_count": 1},
        {
            "phase": "verify_root",
            "root_count": 2**63 - 1,
            "child_count": 2**63 - 1,
            "verified_root_count": 2**63 - 1,
            "verified_child_count": 1,
        },
    ],
)
def test_manifest_proof_accounting_fails_closed(changes):
    with pytest.raises(binding.QueryProjectionUnavailable):
        binding._manifest_shape(_manifest(**changes), require_complete=False)


@pytest.mark.parametrize("phase", ["verify_root", "verify_child", "digest_root", "digest_child"])
def test_bounded_proof_phases_allow_unfinished_control_evidence(phase):
    manifest = _manifest(phase=phase, complete_at=None, projection_sha256=None)
    binding._manifest_shape(manifest, require_complete=False)


def test_projection_failures_follow_the_candidate_runner_failure_contract():
    assert issubclass(binding.QueryProjectionUnavailable, CandidateRunnerError)


def test_maximum_field_and_support_index_count_can_be_retained_before_completion():
    binding._manifest_shape(
        _manifest(phase="indexes", complete_at=None, projection_sha256=None, index_count=33), require_complete=False
    )


def _parents(**parent_changes):
    values = vars(_manifest()) | {"capture_bundle_id": 8, "frozen_at": _NOW}
    definition = SimpleNamespace(
        **{name: values[name] for name in ("definition_revision_id", "dataset_id", "schema_revision_id")},
        contract_version="custom-import/v1",
        definition_sha256=bytes.fromhex(_DEFINITION.digest),
    )
    return SimpleNamespace(**(values | parent_changes)), SimpleNamespace(**values), definition


def _catalog(name=_ROOT, **changes):
    specifications = [
        ("entity_binding_id", "bigint", True, None),
        ("selected_value", "bigint", True, None),
    ]
    primary_key_names = ["entity_binding_id", "selected_value"]
    if name == _CHILD:
        specifications.extend((("child_key_sha256", "bytea", True, None), ("canonical_child_key", "text", True, "C")))
        primary_key_names.append("child_key_sha256")
        specifications.extend((("field_2", "numeric(31,12)", False, None), ("field_3", "numeric(31,12)", False, None)))
    else:
        specifications.append(("field_1", "numeric(31,12)", False, None))
    columns = [
        [index, column, kind, required, "", "", None, None if collation is None else "pg_catalog", collation]
        for index, (column, kind, required, collation) in enumerate(specifications, 1)
    ]
    return (
        dict(
            table_oid=102 if name == _CHILD else 101,
            table_owner=42,
            schema_owner=42,
            relkind="r",
            relpersistence="p",
            inherited=False,
            foreign_keys=False,
            triggers=False,
            columns_sha256=(b"k" if name == _CHILD else b"r") * 32,
            columns=columns,
            primary_key=primary_key_names,
        )
        | changes
    )


def _result(value, *, mapping=False):
    result = Mock()
    result.one_or_none.return_value = value
    result.all.return_value = value
    if mapping:
        result.mappings.return_value = result
    return result


def _session(*results):
    return SimpleNamespace(in_transaction=lambda: True, execute=AsyncMock(side_effect=results))


def _indexes():
    flags_by_name = dict(
        valid=True,
        ready=True,
        live=True,
        plain=True,
        complete=True,
        default_options=True,
        uncollated=True,
        default_ops=True,
        unique_index=False,
        primary_index=False,
        exclusion_index=False,
    )
    return [
        dict(
            flags_by_name,
            index_name=f"{table}_field_{slot}_idx",
            table_oid=oid,
            index_owner=42,
            index_kind="i",
            access_method="btree",
            key_names=["selected_value", *child_keys, f"field_{slot}", "entity_binding_id"],
            key_count=3 + len(child_keys),
            attribute_count=3 + len(child_keys),
            collation_names=[None] * (3 + len(child_keys)),
        )
        for table, slot, oid, child_keys in (
            (_ROOT, 1, 101, []),
            (_CHILD, 2, 102, ["child_key_sha256"]),
            (_CHILD, 3, 102, ["child_key_sha256"]),
        )
    ] + [
        dict(
            flags_by_name,
            index_name=QUERY_PROJECTION_BINDING_INDEX,
            table_oid=103,
            index_owner=42,
            index_kind="i",
            access_method="btree",
            key_names=["dataset_id", "adapter_id", "canonical_value", "entity_binding_id"],
            key_count=4,
            attribute_count=4,
            collation_names=[None, "pg_catalog.default", "pg_catalog.C", None],
            uncollated=False,
        )
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("name", [_ROOT, _CHILD])
async def test_native_catalog_identity_validates_the_model_before_returning_custody(name):
    row = _catalog(name)
    session = _session(_result(row, mapping=True))
    assert await binding.projection_table_identity(session, 17, name, _DEFINITION) == (
        row["table_oid"],
        row["table_owner"],
        row["columns_sha256"],
    )
    statement, arguments = session.execute.call_args.args
    assert arguments == {"schema_name": "ci_snapshot_17", "table_name": name}
    sql = str(statement.compile(dialect=postgresql.dialect()))
    assert "ROW(a.attnum,a.attname,a.atttypid,a.atttypmod,a.attnotnull,a.attcollation," in sql
    assert "c.relkind::text AS relkind,c.relpersistence::text AS relpersistence" in sql
    assert "E'\\\\n' ORDER BY a.attnum),'UTF8'" in sql


@pytest.mark.asyncio
@pytest.mark.parametrize("name", [_ROOT, _CHILD])
async def test_empty_capabilities_still_have_valid_fixed_context_identity(name):
    document = derived_document()
    document["query"].update(filterable_fields=[], sortable_fields=[], order=[])
    parsed = CustomImportDefinition.from_mapping(document)
    row = _catalog(name)
    row["columns"] = row["columns"][: 4 if name == _CHILD else 2]
    assert await binding.projection_table_identity(_session(_result(row, mapping=True)), 17, name, parsed)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changes",
    [
        {"relkind": "v"},
        {"relpersistence": "u"},
        {"table_owner": 43},
        {"table_oid": 0},
        {"inherited": True},
        {"foreign_keys": True},
        {"triggers": True},
        {"columns_sha256": b"short"},
        {"primary_key": ["entity_binding_id"]},
        {"columns": []},
    ],
)
async def test_native_identity_rejects_custody_and_shape_changes(changes):
    with pytest.raises(binding.QueryProjectionUnavailable):
        await binding.projection_table_identity(
            _session(_result(_catalog(**changes), mapping=True)), 17, _ROOT, _DEFINITION
        )


@pytest.mark.asyncio
async def test_catalog_shape_rejects_wrong_precision_default_and_child_collation():
    for position, index, value in ((4, 2, "numeric(30,12)"), (0, 6, "nextval('sequence')"), (3, 8, "default")):
        row = _catalog(_CHILD)
        row["columns"][position][index] = value
        with pytest.raises(binding.QueryProjectionUnavailable, match="model shape"):
            await binding.projection_table_identity(_session(_result(row, mapping=True)), 17, _CHILD, _DEFINITION)


@pytest.mark.asyncio
@pytest.mark.parametrize("name", [None, [], "custom_import_winner", "custom_import_derived_root; DROP SCHEMA public"])
async def test_relation_names_cannot_select_caller_sql(name):
    session = _session()
    with pytest.raises(binding.QueryProjectionUnavailable, match="relation name"):
        await binding.projection_table_identity(session, 17, name, _DEFINITION)
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("require_complete", [True, False])
async def test_reader_and_incomplete_writer_pin_then_revalidate_without_writing(require_complete):
    frozen_at = _NOW if require_complete else None
    session = _session(
        _result(_parents(frozen_at=frozen_at)),
        _result(None),
        _result(_parents(frozen_at=frozen_at)),
        _result(_catalog(), mapping=True),
        _result(_catalog(_CHILD), mapping=True),
        *([_result((103, 42)), _result(_indexes(), mapping=True)] if require_complete else []),
    )
    manifest = _manifest(
        complete_at=frozen_at,
        phase="complete" if require_complete else "root",
        projection_sha256=b"p" * 32 if require_complete else None,
    )
    tables = await binding.lock_projection_tables(session, manifest, _DEFINITION, require_complete=require_complete)
    assert tuple(tables) == (_ROOT, _CHILD)
    statements = [str(call.args[0]) for call in session.execute.call_args_list]
    assert statements[1] == (
        "LOCK TABLE ci_snapshot_17.custom_import_derived_root,ci_snapshot_17.custom_import_derived_child "
        "IN ACCESS SHARE MODE"
    )
    assert all(
        not any(keyword in query for keyword in ("UPDATE ", "INSERT ", "GRANT ", "SET ROLE", "CREATE "))
        for query in statements
    )
    assert "custom_import_generation" in statements[0] and "custom_import_definition_revision" in statements[2]


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["indexes", "complete"])
async def test_native_index_evidence_is_required_before_and_after_completion(phase):
    session = _session(_result((103, 42)), _result(_indexes(), mapping=True))
    await binding.validate_projection_indexes(session, _manifest(phase=phase), _DEFINITION)
    custody_query = session.execute.call_args_list[0].args[0]
    assert "custom_import_snapshot_relation" in str(custody_query)
    assert custody_query.compile().params == {"family_id_1": 17, "relation_slot_1": 2}
    query, parameters = session.execute.call_args.args
    assert "i.indisvalid AS valid,i.indisready AS ready" in str(query)
    assert "i.indexprs IS NULL AS plain,i.indpred IS NULL AS complete" in str(query)
    assert parameters["schema_name"] == "ci_snapshot_17" and len(parameters["index_names"]) == 4
    assert "a.atttypid='pg_catalog.varchar'::regtype THEN 'pg_catalog.text'::regtype" in str(query)
    assert "opn.nspname<>'pg_catalog'" in str(query)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "name,value",
    [
        ("table_oid", 999),
        ("index_owner", 43),
        ("index_kind", "I"),
        ("access_method", "hash"),
        ("key_names", ["field_1"]),
        ("key_count", 1),
        ("attribute_count", 4),
        ("valid", False),
        ("ready", False),
        ("live", False),
        ("plain", False),
        ("complete", False),
        ("default_options", False),
        ("uncollated", False),
        ("collation_names", [None, "pg_catalog.C", None]),
        ("default_ops", False),
        ("unique_index", True),
        ("primary_index", True),
        ("exclusion_index", True),
    ],
)
async def test_changed_or_unusable_serving_index_fails_closed(name, value):
    index_records = _indexes()
    index_records[0][name] = value
    with pytest.raises(binding.QueryProjectionUnavailable, match="index shape"):
        await binding.validate_projection_indexes(
            _session(_result((103, 42)), _result(index_records, mapping=True)), _manifest(), _DEFINITION
        )


@pytest.mark.asyncio
async def test_complete_manifest_cannot_hide_a_missing_serving_index():
    with pytest.raises(binding.QueryProjectionUnavailable, match="index is missing"):
        await binding.validate_projection_indexes(
            _session(_result((103, 42)), _result(_indexes()[:-1], mapping=True)), _manifest(), _DEFINITION
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("custody", [None, (0, 42), (103, True), ("103", 42), (103, 2**63)])
async def test_binding_index_requires_existing_native_registry_custody(custody):
    session = _session(_result(custody))
    with pytest.raises(binding.QueryProjectionUnavailable, match="binding relation custody"):
        await binding.validate_projection_indexes(session, _manifest(), _DEFINITION)
    assert session.execute.await_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "name,value",
    [
        ("table_oid", 101),
        ("index_owner", 43),
        ("collation_names", [None, "pg_catalog.C", "pg_catalog.C", None]),
        ("collation_names", [None, "pg_catalog.default", "pg_catalog.default", None]),
        ("collation_names", [None, "pg_catalog.default", "public.C", None]),
        ("uncollated", True),
        ("default_ops", False),
    ],
)
async def test_binding_index_requires_registered_identity_and_exact_native_collations(name, value):
    indexes = _indexes()
    indexes[-1][name] = value
    with pytest.raises(binding.QueryProjectionUnavailable, match="index shape"):
        await binding.validate_projection_indexes(
            _session(_result((103, 42)), _result(indexes, mapping=True)), _manifest(), _DEFINITION
        )


@pytest.mark.asyncio
async def test_empty_capability_needs_no_binding_or_projection_indexes():
    document = derived_document()
    document["query"].update(filterable_fields=[], sortable_fields=[], order=[])
    parsed = CustomImportDefinition.from_mapping(document)
    manifest = _manifest(
        definition_sha256=bytes.fromhex(parsed.digest), capability_sha256=capability_sha256(parsed), index_count=0
    )
    session = _session()
    await binding.validate_projection_indexes(session, manifest, parsed)
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changes",
    [
        {"projection_contract": "unknown"},
        {"family_id": True},
        {"producing_token_sha256": b"short"},
        {"root_columns_sha256": None},
        {"capability_sha256": b""},
        {"after_entity_binding_id": -1},
        {"root_count": -1},
        {"root_table_oid": 102},
        {"complete_at": None},
        {"projection_sha256": None},
        {"phase": "root"},
        {"phase": "other"},
        {"definition_sha256": b"x" * 32},
        {"capability_sha256": b"x" * 32},
        {"index_count": True},
        {"index_count": -1},
        {"index_count": 34},
        {"index_count": 3},
        {"index_count": 2},
    ],
)
async def test_manifest_failures_never_reach_table_locks(changes):
    session = _session()
    with pytest.raises(binding.QueryProjectionUnavailable):
        await binding.lock_projection_tables(session, _manifest(**changes), _DEFINITION)
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changes",
    [
        {"generation_id": 99},
        {"dataset_id": 99},
        {"definition_revision_id": 99},
        {"schema_revision_id": 99},
        {"execution_id": 99},
        {"producing_fence": 99},
        {"producing_token_sha256": b"x" * 32},
        {"capture_bundle_id": 99},
        {"frozen_at": None},
    ],
)
async def test_reader_requires_the_exact_frozen_producer(changes):
    session = _session(_result(_parents(**changes)))
    with pytest.raises(binding.QueryProjectionUnavailable, match="binding differs"):
        await binding.lock_projection_tables(session, _manifest(), _DEFINITION)
    assert session.execute.await_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changes", [{"table_oid": 999}, {"columns_sha256": b"x" * 32}, {"table_owner": 43, "schema_owner": 43}]
)
async def test_custody_is_compared_against_retained_identity_after_lock(changes):
    session = _session(
        _result(_parents()),
        _result(None),
        _result(_parents()),
        _result(_catalog(**changes), mapping=True),
    )
    with pytest.raises(binding.QueryProjectionUnavailable, match="retained table identity"):
        await binding.lock_projection_tables(session, _manifest(), _DEFINITION)


@pytest.mark.asyncio
async def test_parent_is_revalidated_after_lock_and_no_transaction_is_rejected():
    session = _session(_result(_parents()), _result(None), _result(_parents(producing_fence=99)))
    with pytest.raises(binding.QueryProjectionUnavailable, match="producer binding"):
        await binding.lock_projection_tables(session, _manifest(), _DEFINITION)
    session = _session()
    session.in_transaction = lambda: False
    with pytest.raises(binding.QueryProjectionUnavailable, match="active transaction"):
        await binding.lock_projection_tables(session, _manifest(), _DEFINITION)
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_writer_prepare_callback_precedes_every_native_statement():
    session = _session(
        _result(_parents()),
        _result(None),
        _result(_parents()),
        _result(_catalog(), mapping=True),
        _result(_catalog(_CHILD), mapping=True),
        _result((103, 42)),
        _result(_indexes(), mapping=True),
    )
    events = []
    execute = session.execute

    async def record(statement, *parameters):
        events.append("execute")
        return await execute(statement, *parameters)

    async def prepare(actual):
        assert actual is session
        events.append("prepare")

    session.execute = record
    await binding.lock_projection_tables(session, _manifest(), _DEFINITION, prepare=prepare)
    assert events == ["prepare", "execute"] * 7


@pytest.mark.asyncio
async def test_expired_writer_callback_stops_before_catalog_or_lock():
    session = _session()
    prepare = AsyncMock(side_effect=TimeoutError("expired window"))
    with pytest.raises(TimeoutError, match="expired window"):
        await binding.projection_table_identity(session, 17, _ROOT, _DEFINITION, prepare=prepare)
    session.execute.assert_not_awaited()
    with pytest.raises(TimeoutError, match="expired window"):
        await binding.lock_projection_tables(session, _manifest(), _DEFINITION, prepare=prepare)
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_projection_parent_absence_prevents_catalog_or_lock_work():
    session = _session(_result(None))
    with pytest.raises(binding.QueryProjectionUnavailable, match="parent is missing"):
        await binding.lock_projection_tables(session, _manifest(), _DEFINITION)
    assert session.execute.await_count == 1


@pytest.mark.asyncio
async def test_index_manifest_count_must_match_declared_capabilities():
    session = _session()
    with pytest.raises(binding.QueryProjectionUnavailable, match="index coverage differs"):
        await binding.validate_projection_indexes(session, _manifest(index_count=3, phase="indexes"), _DEFINITION)
    session.execute.assert_not_awaited()


def test_projection_shape_refuses_unsupported_model_type():
    from sqlalchemy import JSON, Column

    with pytest.raises(binding.QueryProjectionUnavailable, match="column type is unsupported"):
        binding._column_shape(Column("unsupported", JSON), 1)
