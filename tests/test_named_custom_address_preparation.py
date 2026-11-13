# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Named address recipes, staged data validation and administrator freeze orchestration."""

import json
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import network_custom_address_source as custom
from process import registry_composition_addresses as composition
from tests.test_registry_ptg_address_carry import _artifact, _legacy_artifact_document, _mint


@asynccontextmanager
async def _transaction():
    yield


@pytest.mark.parametrize(
    "field,value",
    [
        ("composition_id", "invalid"),
        ("approved_revision", True),
        ("control_schema", "invalid-name"),
        ("custom_pair_count", -1),
        ("custom_pair_count", True),
        ("approved_records_sha256", "bad"),
        ("generation_sha256", "f" * 64),
        ("source_aliases", 1),
        ("unknown", None),
    ],
)
def test_named_receipt_rejects_wrong_shape_scope_or_logical_digest(field, value):
    with pytest.raises(custom.NetworkCustomAddressSourceError):
        custom._from_document(json.dumps({**_artifact().as_dict(), field: value}))


def test_legacy_projection_ignores_obsolete_identity_values_and_keeps_logical_edition():
    document = _legacy_artifact_document()
    for field in custom._LEGACY_PHYSICAL_FIELDS:
        document[field] = {"retired": "saved-value"}
    receipt = custom._from_document(json.dumps(document))
    assert receipt.generation_sha256 == document["generation_sha256"]
    assert receipt.address_source.schema_name == "network_composition_" + receipt.composition_id.replace("-", "")
    assert not set(receipt.as_dict()) & custom._LEGACY_PHYSICAL_FIELDS
    assert custom._from_document(json.dumps(receipt.as_dict())) == receipt


async def test_internal_recipe_entry_stages_named_data_without_administrator_role_operations(monkeypatch):
    receipt = _artifact()
    connection = SimpleNamespace(
        is_in_transaction=lambda: True,
        fetchval=AsyncMock(return_value="repeatable read"),
        transaction=_transaction,
        execute=AsyncMock(),
    )
    monkeypatch.setattr(
        custom,
        "_approved_identity",
        AsyncMock(
            return_value={
                "pairs": receipt.custom_pair_count,
                "fingerprint": receipt.approved_records_sha256,
                "npi_pairs": 0,
            }
        ),
    )
    columns = AsyncMock()
    monkeypatch.setattr(custom, "_source_columns", columns)

    async def stage(actual, recipe, schema, control, office_inputs):
        assert actual is connection and schema == receipt.address_source.schema_name
        assert control == '"control_fixture"' and office_inputs == ((), None)
        assert not set(recipe.as_dict()) & custom._LEGACY_PHYSICAL_FIELDS
        return _mint(recipe)

    monkeypatch.setattr(custom, "_prepare_artifact", stage)
    arguments = custom.CustomAddressSourceArguments(
        receipt.composition_id, receipt.approved_revision, "configured_owner", (), receipt.control_schema
    )
    assert await custom.prepare_recipe_address_source(connection, receipt.base_source, arguments) == receipt
    columns.assert_awaited_once_with(connection, receipt.base_source, address=True)
    connection.execute.assert_not_awaited()


async def test_standalone_entry_applies_actual_freeze_sql_before_return(monkeypatch):
    receipt = _artifact()
    connection = SimpleNamespace(
        is_in_transaction=lambda: True,
        transaction=_transaction,
        fetch=AsyncMock(
            side_effect=[
                [{"relname": name, "relkind": "r"} for name in ("entity_address_unified", "provider_location_binding")],
                [],
                [],
                [],
            ]
        ),
        execute=AsyncMock(),
    )
    monkeypatch.setattr(custom, "prepare_recipe_address_source", AsyncMock(return_value=receipt))
    assert (
        await custom.prepare_custom_address_source(
            connection,
            receipt.base_source,
            composition_id=receipt.composition_id,
            approved_revision=receipt.approved_revision,
            owner_role="snapshot_owner",
            runtime_roles=("builder", "publisher", "reader"),
            control_schema=receipt.control_schema,
        )
        == receipt
    )
    statements = [call.args[0] for call in connection.execute.await_args_list]
    assert statements[0] == 'REVOKE "snapshot_owner" FROM "builder","publisher","reader" CASCADE'
    assert sum(statement.startswith("ALTER TABLE") for statement in statements) == 2
    assert sum(statement.startswith("GRANT SELECT ON TABLE") for statement in statements) == 2
    assert any(statement.startswith("ALTER SCHEMA") for statement in statements)
    assert not any("SET LOCAL ROLE" in statement for statement in statements)


async def test_internal_artifact_mints_logical_recipe_after_all_carries(monkeypatch):
    receipt = _artifact()

    async def scalar(query, *arguments):
        if query.startswith("SELECT obj_description"):
            return None
        assert query == "SELECT quote_literal($1::text)"
        return "'" + arguments[0] + "'"

    connection = SimpleNamespace(fetchval=AsyncMock(side_effect=scalar), execute=AsyncMock())
    monkeypatch.setattr(custom, "_build_heaps", AsyncMock(return_value=SimpleNamespace(sha256="a" * 64)))
    monkeypatch.setattr(custom, "_aca_address_carry", AsyncMock(return_value=SimpleNamespace(sha256="b" * 64)))
    monkeypatch.setattr(custom, "_fhir_address_carry", AsyncMock(return_value="c" * 64))
    monkeypatch.setattr(custom, "_source_alias_layout", AsyncMock(return_value=True))
    result = await custom._prepare_artifact(
        connection, receipt, receipt.address_source.schema_name, '"control_fixture"', ((), None)
    )
    assert custom._from_document(json.dumps(result.as_dict())) == result
    assert (result.ptg_carry_sha256, result.aca_carry_sha256, result.fhir_carry_sha256) == (
        "a" * 64,
        "b" * 64,
        "c" * 64,
    )
    statements = [call.args[0] for call in connection.execute.await_args_list]
    assert statements[0].startswith("CREATE INDEX") and statements[1].startswith("ANALYZE")
    assert statements[2].startswith("COMMENT ON SCHEMA") and len(statements) == 3


@pytest.mark.parametrize("damage", [None, "approval", "count", "columns", "stored_scope"])
@pytest.mark.parametrize("legacy", [False, True])
async def test_reuse_compares_logical_recipe_and_actual_data_layout(monkeypatch, damage, legacy):
    receipt = custom._from_document(json.dumps(_legacy_artifact_document())) if legacy else _artifact()
    connection = SimpleNamespace(fetchval=AsyncMock(return_value=json.dumps(receipt.as_dict())))
    monkeypatch.setattr(custom, "_require_transaction", AsyncMock())
    summary_by_field = {
        "pairs": 1 if damage == "count" else receipt.custom_pair_count,
        "fingerprint": "f" * 64 if damage == "approval" else receipt.approved_records_sha256,
    }
    monkeypatch.setattr(custom, "_approved_identity", AsyncMock(return_value=summary_by_field))
    monkeypatch.setattr(custom, "verify_approved_site_sources", AsyncMock(return_value=()))
    monkeypatch.setattr(custom, "_source_alias_layout", AsyncMock(return_value=False))
    shape = ({"name": "location_key", "type": "varchar(64)"},)
    columns = AsyncMock(side_effect=[shape, () if damage == "columns" else shape])
    monkeypatch.setattr(custom, "_source_columns", columns)
    monkeypatch.setattr(custom, "_binding_shape", AsyncMock())
    monkeypatch.setattr(custom, "_office_lookup_shape", AsyncMock())
    if damage == "stored_scope":
        connection.fetchval.return_value = json.dumps(
            _mint(replace(receipt, control_schema="another_control")).as_dict()
        )
    if damage is None:
        assert await custom.verify_custom_address_source(connection, receipt) == receipt
        assert connection.fetchval.await_args.args[1] == receipt.address_source.schema_name
    else:
        with pytest.raises(custom.NetworkCustomAddressSourceError):
            await custom.verify_custom_address_source(connection, receipt)


@pytest.mark.parametrize("differs", [False, True])
async def test_retained_copy_preserves_complete_row_multiset(differs):
    connection = SimpleNamespace(
        fetchrow=AsyncMock(return_value={"expected_rows": 7, "actual_rows": 7, "differs": differs})
    )
    if differs:
        with pytest.raises(custom.NetworkCustomAddressSourceError, match="full-row parity"):
            await custom._verify_retained_rows(connection, '"base".addresses', '"candidate".addresses', 7)
    else:
        await custom._verify_retained_rows(connection, '"base".addresses', '"candidate".addresses', 7)
    assert "EXCEPT ALL" in connection.fetchrow.await_args.args[0]


async def test_prepared_reuse_has_no_session_or_role_census_dependency(monkeypatch):
    from uuid import UUID

    from process.network_approved_membership_source import ApprovedMembershipSource

    approved = ApprovedMembershipSource(4, "b" * 64)
    source = composition.RegistryCompositionAddressSources(_artifact().base_source)
    writer_roles_by_kind = {"owner_role": "builder", "loader_roles": ("loader",), "reader_roles": ("reader",)}
    arguments = composition._address_arguments(UUID(int=3), approved, "ignored", (), source, "control_fixture")
    receipt = _mint(
        replace(
            _artifact(),
            composition_id=arguments.composition_id,
            address_source=replace(
                _artifact().address_source,
                schema_name="network_composition_" + arguments.composition_id.replace("-", ""),
            ),
        )
    )
    connection = SimpleNamespace(fetchval=AsyncMock(side_effect=AssertionError("Unexpected session query")))
    monkeypatch.setattr(composition, "verify_custom_address_source", AsyncMock(return_value=receipt))
    monkeypatch.setattr(composition, "prepare_recipe_address_source", AsyncMock(return_value=receipt))
    assert (
        await composition._prepared_address_artifact(
            connection, receipt, source, UUID(int=3), approved, writer_roles_by_kind, "control_fixture", ((), None)
        )
        == receipt
    )
    connection.fetchval.assert_not_awaited()


@pytest.mark.parametrize("failure", ["permission", "owner_overlap"])
async def test_standalone_freeze_failure_preserves_domain_error_and_transaction_rollback(monkeypatch, failure):
    import asyncpg

    receipt = _artifact()
    events = []

    @asynccontextmanager
    async def transaction():
        events.append("begin")
        try:
            yield
        except BaseException:
            events.append("rollback")
            raise
        else:
            events.append("commit")

    native_error = asyncpg.InsufficientPrivilegeError("freeze refused")
    connection = SimpleNamespace(
        is_in_transaction=lambda: True,
        transaction=transaction,
        execute=AsyncMock(side_effect=native_error),
        fetch=AsyncMock(return_value=[]),
    )
    monkeypatch.setattr(custom, "prepare_recipe_address_source", AsyncMock(return_value=receipt))
    with pytest.raises(custom.NetworkCustomAddressSourceError, match="native freeze failed") as caught:
        await custom.prepare_custom_address_source(
            connection,
            receipt.base_source,
            composition_id=receipt.composition_id,
            approved_revision=receipt.approved_revision,
            owner_role="snapshot_owner",
            runtime_roles=("reader",) if failure == "permission" else ("snapshot_owner",),
            control_schema=receipt.control_schema,
        )
    assert events == ["begin", "rollback"]
    if failure == "permission":
        assert caught.value.__cause__ is native_error
    else:
        connection.execute.assert_not_awaited()
