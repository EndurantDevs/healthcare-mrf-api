# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Native publication must require model keys, not just a self-consistent index receipt."""

from contextlib import asynccontextmanager
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy import BigInteger, Column, MetaData, PrimaryKeyConstraint, Table
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateTable

from process import entity_address_native_publication as publication
from process import entity_address_snapshot_receipt as receipt


def _catalog():
    """Return catalog-only synthetic model evidence, with no payload or publication authority."""
    columns = [
        {"attnum": 1, "attname": "id", "type": "bigint", "attnotnull": True},
        {"attnum": 2, "attname": "value", "type": "bigint", "attnotnull": False},
    ]
    constraints = [{"contype": "p", "key_columns": "{1}", "convalidated": True}]
    indexes = [
        {
            "keys": "1",
            "key_attributes": [{"position": 0, "attribute_number": 1, "opclass_name": "int8_ops"}],
            "predicate": None,
            "indisunique": True,
            "indisvalid": True,
            "method": "btree",
        }
    ]
    return columns, constraints, indexes


def _model():
    table = Table(
        "synthetic_stage", MetaData(), Column("id", BigInteger), Column("value", BigInteger), PrimaryKeyConstraint("id")
    )
    return SimpleNamespace(__table__=table, __tablename__=table.name, __my_additional_indexes__=())


def _gate(monkeypatch, actual):
    """Run the real shared gate with native metadata responses, not a mocked success gate."""
    model = _model()
    monkeypatch.setattr(publication.generation, "ENTITY_ADDRESS_RESULT_MODELS", (model,))
    expected = _catalog()
    for name, wanted, observed in zip(
        ("_catalog_columns", "_catalog_constraints", "_catalog_indexes"), expected, actual, strict=True
    ):
        monkeypatch.setattr(receipt, name, AsyncMock(side_effect=[wanted, observed]))
    session = MagicMock()
    session.execute = AsyncMock()
    session.scalar = AsyncMock(side_effect=["pg_catalog", 201])
    return session, [{"table_name": "synthetic_stage", "relation_oid": 101}]


@pytest.mark.parametrize(
    "drift",
    [
        "missing_key",
        "wrong_key",
        "nullable_key",
        "unvalidated_key",
        "missing_index",
        "wrong_method",
        "wrong_opclass",
        "partial_key",
    ],
)
async def test_safe_catalog_drift_refuses_handoff(monkeypatch, drift):
    """Otherwise inert metadata cannot replace any required model key or index shape."""
    actual = _catalog()
    mutations_by_name = {
        "missing_key": lambda: actual[1].clear(),
        "wrong_key": lambda: actual[1][0].update(key_columns="{2}"),
        "nullable_key": lambda: actual[0][0].update(attnotnull=False),
        "unvalidated_key": lambda: actual[1][0].update(convalidated=False),
        "missing_index": lambda: actual[2].clear(),
        "wrong_method": lambda: actual[2][0].update(method="hash"),
        "wrong_opclass": lambda: actual[2][0]["key_attributes"][0].update(opclass_name="other_ops"),
        "partial_key": lambda: actual[2][0].update(predicate="value IS NOT NULL"),
    }
    mutations_by_name[drift]()
    session, relations = _gate(monkeypatch, actual)
    with pytest.raises(RuntimeError, match="model (constraints|indexes) differ"):
        await publication._require_model_catalog(session, "example", relations)
    statements = [str(call.args[0]) for call in session.execute.await_args_list]
    assert sum("CREATE TEMPORARY TABLE" in statement for statement in statements) == 1
    assert sum(statement.startswith("DROP TABLE pg_temp.") for statement in statements) == 1
    assert not any("ALTER TABLE" in statement or "CREATE SCHEMA" in statement for statement in statements)


async def test_reordered_columns_and_safe_auxiliary_indexes_remain_valid(monkeypatch):
    """Ordinary compaction may change attribute numbers and keep extra inference indexes."""
    actual = _catalog()
    actual[0][0]["attnum"], actual[0][1]["attnum"] = 4, 3
    actual[0].reverse()
    actual[1][0]["key_columns"] = "{4}"
    actual[2][0]["keys"] = "4"
    actual[2][0]["key_attributes"][0]["attribute_number"] = 4
    auxiliary_index = deepcopy(actual[2][0])
    auxiliary_index.update(predicate="value IS NOT NULL", indisunique=False)
    actual[2].append(auxiliary_index)
    session, relations = _gate(monkeypatch, actual)
    await publication._require_model_catalog(session, "example", relations)
    assert str(session.execute.await_args_list[-2].args[0]).startswith("DROP TABLE pg_temp.")
    assert session.execute.await_args_list[-1].args[1] == {"path": "pg_catalog"}


async def test_capture_rejects_before_recording_index_receipt(monkeypatch):
    """Real capture checks the shared model gate under locks before it records a handoff."""
    session = SimpleNamespace(execute=AsyncMock())
    handoff_by_field = {
        "schema_name": "example",
        "import_date": "synthetic",
        "run_id": "example",
        "dependency_bindings": {},
    }
    relations = [{"table_name": "synthetic", "relation_oid": 101}]
    monkeypatch.setattr(publication, "_native", lambda: object())
    monkeypatch.setattr(publication, "_locked_run", AsyncMock(return_value={"metrics": {}}))
    monkeypatch.setattr(publication, "raise_if_cancelled", AsyncMock())
    monkeypatch.setattr(publication, "_stage_names", lambda *_: {"synthetic": "synthetic_attempt"})
    monkeypatch.setattr(publication, "_dependency_bindings", AsyncMock(return_value={}))
    monkeypatch.setattr(publication, "_stage_inventory", AsyncMock(return_value=relations))
    executable_catalog = AsyncMock()
    monkeypatch.setattr(publication, "_require_stage_execution_catalog", executable_catalog)
    gate = AsyncMock(side_effect=RuntimeError("model constraints differ"))
    monkeypatch.setattr(publication, "_require_model_catalog", gate)
    index_receipt = AsyncMock()
    monkeypatch.setattr(publication, "_index_inventory", index_receipt)
    with pytest.raises(RuntimeError, match="model constraints differ"):
        await publication._capture_handoff(session, {}, handoff_by_field)
    gate.assert_awaited_once_with(session, "example", relations)
    executable_catalog.assert_awaited_once_with(session, handoff_by_field)
    assert "ACCESS EXCLUSIVE" in str(session.execute.await_args_list[0].args[0])
    index_receipt.assert_not_awaited()
    assert "handoff_sha256" not in handoff_by_field


async def test_native_catalog_codes_use_existing_encoder(monkeypatch):
    """Native one-byte catalog codes retain their existing receipt normalization."""
    actual = _catalog()
    actual[1][0]["contype"] = b"p"
    session, relations = _gate(monkeypatch, actual)
    await publication._require_model_catalog(session, "example", relations)


async def test_publisher_refuses_model_drift_before_sealing(monkeypatch):
    """The existing completion path cannot seal or cut over a model-incompatible handoff."""
    from process import entity_address_snapshot_adoption as adoption
    from process import entity_address_snapshot_preparation as protected

    session = SimpleNamespace(in_transaction=lambda: True)
    handoff_by_field = {"schema_name": "example", "dependency_bindings": {}, "stage_relations": []}

    @asynccontextmanager
    async def binding(actual):
        assert actual is session
        yield session

    monkeypatch.setattr(publication, "validate_entity_address_native_handoff", lambda value: value)
    monkeypatch.setattr(publication.projection, "validate_projection_dependency_bindings", lambda *_: {})
    monkeypatch.setattr(
        publication, "_native", lambda: SimpleNamespace(db=SimpleNamespace(bind_existing_session=binding))
    )
    monkeypatch.setattr(publication, "_require_native_attempt", AsyncMock())
    monkeypatch.setattr(protected.alias, "_lock_alias_relations", AsyncMock())
    stage = AsyncMock()
    monkeypatch.setattr(publication, "_require_handoff_stage", stage)
    gate = AsyncMock(side_effect=RuntimeError("model indexes differ"))
    monkeypatch.setattr(publication, "_require_model_catalog", gate)
    preparation = AsyncMock()
    monkeypatch.setattr(adoption, "prepare_entity_address_publisher_reimport", preparation)
    continuation = AsyncMock()
    with pytest.raises(RuntimeError, match="model indexes differ"):
        await publication.complete_entity_address_unified_handoff(
            session, handoff_by_field, dependency_bindings={}, publication_continuation=continuation
        )
    stage.assert_awaited_once_with(session, handoff_by_field)
    gate.assert_awaited_once_with(session, "example", [])
    preparation.assert_not_awaited()
    continuation.assert_not_awaited()


def test_expected_shape_preserves_all_native_model_keys_and_no_sequence():
    """Empty expected DDL keeps native keys without changing canonical serial compatibility."""
    for model in publication.generation.ENTITY_ADDRESS_RESULT_MODELS:
        original = str(CreateTable(model.__table__).compile(dialect=postgresql.dialect()))
        table = publication._model_catalog_table(model)
        statement = str(CreateTable(table).compile(dialect=postgresql.dialect()))
        assert "CREATE TEMPORARY TABLE" in statement and "ON COMMIT DROP" in statement
        assert "SERIAL" not in statement and table.autoincrement_column is None
        assert tuple(table.primary_key.columns.keys()) == tuple(model.__table__.primary_key.columns.keys())
        assert str(CreateTable(model.__table__).compile(dialect=postgresql.dialect())) == original


async def test_metadata_failure_keeps_original_error_and_restores_settings(monkeypatch):
    """A failed native metadata query must not be hidden by aborted-transaction cleanup."""
    session, relations = _gate(monkeypatch, _catalog())
    failure = RuntimeError("native metadata failure")
    monkeypatch.setattr(receipt, "_catalog_columns", AsyncMock(side_effect=failure))

    async def execute(statement, *_args):
        if str(statement).startswith("DROP TABLE"):
            raise RuntimeError("aborted transaction")

    session.execute.side_effect = execute
    with pytest.raises(RuntimeError, match="native metadata failure") as caught:
        await publication._require_model_catalog(session, "example", relations)
    assert caught.value is failure
    assert session.execute.await_args_list[-1].args[1] == {"path": "pg_catalog"}
