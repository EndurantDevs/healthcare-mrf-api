# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Full native canonical model and indexed contribution policy regressions."""

from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateTable

from db.models import AddressArchiveV2
from process import entity_address_native_publication as native
from process import mrf_address_publication as canonical
from process import npi_result_archive as npi
from process import reference_family_archive as reference


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", [None, "identity", "closed", "restarted", "schema", "flag"])
async def test_canonical_fence_requires_exact_live_transaction_and_catalog_identity(fault):
    transaction = SimpleNamespace(is_active=True)
    holder = SimpleNamespace(get_transaction=lambda: transaction)
    fence = canonical.CanonicalSourceFence(holder, transaction, 21, 22, 23, "24/25")
    session = SimpleNamespace(in_transaction=lambda: True, execute=AsyncMock(), scalar=AsyncMock(return_value=True))
    schema = "mrf"
    if fault == "identity":
        session.scalar.return_value = False
    elif fault == "closed":
        transaction.is_active = False
    elif fault == "restarted":
        fence = replace(fence, transaction=SimpleNamespace(is_active=True))
    elif fault == "schema":
        schema = "foreign"
    if fault is not None:
        with pytest.raises(RuntimeError, match="source fence"):
            await canonical.require_canonical_source_fence(session, True if fault == "flag" else fence, schema)
        if fault != "identity":
            session.execute.assert_not_awaited()
        return
    await canonical.require_canonical_source_fence(session, fence, schema)
    assert str(session.execute.await_args.args[0]).endswith("IN ACCESS SHARE MODE NOWAIT")
    statement, identity = session.scalar.await_args.args
    assert identity == {"pid": 21, "database": 22, "relation": 23, "transaction": "24/25"}
    assert all(part in str(statement) for part in ("virtualtransaction", "ShareRowExclusiveLock", "to_regclass"))


@pytest.mark.parametrize("importer", ["npi", "mrf"])
def test_contribution_is_complete_model_with_native_constraints_and_no_row_hooks(importer):
    table = canonical.canonical_contribution_model(importer).__table__
    assert tuple(table.columns.keys()) == tuple(AddressArchiveV2.__table__.columns.keys())
    definition = str(CreateTable(table).compile(dialect=postgresql.dialect()))
    assert "UNIQUE (identity_key)" in definition and "PRIMARY KEY (address_key)" in definition
    assert "unit_norm TEXT DEFAULT '' NOT NULL" in definition
    assert "precision IN ('street', 'city_zip')" in definition
    assert "strict_source_bits >= 0" in definition and "(strict_source_bits & source_bits)" in definition
    assert "FOREIGN KEY" not in definition and "FUNCTION" not in definition and "TRIGGER" not in definition
    assert table.c.geo_source.type.schema == "mrf"
    assert len(table.indexes) == 5


def test_current_and_recorded_inventory_meanings_are_distinct():
    from db.models._legacy import AddressArchiveV2 as LegacyAddressArchiveV2
    from db.models.address_archive import AddressArchiveV2 as NativeAddressArchiveV2

    assert AddressArchiveV2 is LegacyAddressArchiveV2 is NativeAddressArchiveV2
    assert len(npi.npi_archive_names(canonical=True)) == 7
    assert len(npi.npi_archive_names()) == 6
    spec = reference.reference_family_spec("mrf", canonical=True)
    assert len(spec.table_names) == len(spec.archive_names) == 14
    assert spec.table_names[-1] == canonical.STAGE_TABLE and "log" not in spec.table_names
    assert len(reference.reference_family_spec("mrf").table_names) == 13


@pytest.mark.parametrize("importer", ["npi", "mrf"])
def test_source_projection_copies_redirect_rows_not_key_coordinate_dtos(importer):
    projection = canonical.canonical_reference_filter(importer, "source", lambda schema, name: f'"{schema}"."{name}"')
    assert "WITH RECURSIVE selected_keys" in projection
    assert '"source"."address_archive_v2"' in projection and "row_value.merged_into" in projection
    assert "payload" not in projection and "to_jsonb" not in projection


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["reference", "scope", "target", "cycle", None])
async def test_indexed_closure_rejects_missing_scope_target_and_bounded_cycles(failure):
    results = [False] * 4
    if failure is not None:
        results[["reference", "scope", "target", "cycle"].index(failure)] = True
    session = SimpleNamespace(scalar=AsyncMock(side_effect=results))
    operation = canonical.validate_canonical_closure(
        session, "npi", "stage", lambda schema, name: f'"{schema}"."{name}"'
    )
    if failure is not None:
        with pytest.raises(RuntimeError):
            await operation
    else:
        await operation
        statements = [str(call.args[0]) for call in session.scalar.await_args_list]
        assert "NOT EXISTS" in statements[0] and "source_bits & 1" in statements[1]
        assert "target.address_key=source.merged_into" in statements[2]
        assert "ANY(paths.path)" in statements[3] and "depth=64" in statements[3]
        assert all("FOR UPDATE" not in statement for statement in statements)


@pytest.mark.asyncio
async def test_both_identities_are_checked_before_any_merge_independent_of_input_order(monkeypatch):
    checker = AsyncMock()
    monkeypatch.setattr(canonical, "validate_canonical_merge", checker)
    session = object()
    await canonical.validate_canonical_contributions(session, {"mrf": "mrf_stage", "npi": "npi_stage"}, "incumbent")
    assert [(call.args[1], call.args[2]) for call in checker.await_args_list] == [
        ("mrf_stage", "incumbent"),
        ("mrf_stage", "npi_stage"),
        ("npi_stage", "incumbent"),
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("importer", ["npi", "mrf"])
async def test_publication_attribution_is_direct_only_and_noop_rows_are_not_updated(monkeypatch, importer):
    monkeypatch.setattr(canonical, "require_canonical_publication_capability", AsyncMock())
    monkeypatch.setattr(canonical, "require_canonical_publication_catalog", AsyncMock())
    session = SimpleNamespace(execute=AsyncMock(), scalar=AsyncMock(side_effect=[False, False, 10, 20]))
    stage = f'"stage"."{canonical.CANONICAL_POLICIES[importer][0]}"'
    receipt = await canonical.merge_canonical_contribution(session, importer, stage, '"mrf"."address_archive_v2"')
    statements = [str(call.args[0]) for call in session.execute.await_args_list]
    assert statements[0].endswith("IN SHARE ROW EXCLUSIVE MODE NOWAIT")
    insert, update = statements[1:]
    source_bit = canonical.CANONICAL_POLICIES[importer][1]
    assert f"THEN {source_bit} ELSE 0 END" in insert and "ELSE 9 END" in insert
    assert "contribution.source_bits" not in insert and f"contribution.strict_source_bits & {source_bit}" in insert
    assert "incumbent.merged_into IS DISTINCT FROM contribution.merged_into" in str(
        session.scalar.await_args_list[0].args[0]
    )
    assert "AND ((incumbent.source_bits &" in update and "FOR UPDATE" not in update
    assert "DROP" not in "\n".join(statements)
    assert canonical.validate_canonical_publication(receipt, importer, 20) == receipt


@pytest.mark.asyncio
@pytest.mark.parametrize("safe", [None, False])
async def test_source_topology_refusal_precedes_executable_or_payload_use(monkeypatch, safe):
    checker = AsyncMock()
    monkeypatch.setattr(native, "_require_stage_execution_catalog", checker)
    session = SimpleNamespace(scalar=AsyncMock(return_value=safe))
    with pytest.raises(RuntimeError, match="heap topology"):
        await canonical.require_native_read_catalog(session, (10, 11))
    checker.assert_not_awaited()
    statement = str(session.scalar.await_args.args[0])
    assert "relrowsecurity" in statement and "pg_inherits" in statement and "relkind='r'" in statement


@pytest.mark.asyncio
async def test_source_catalog_accepts_only_fixed_native_extensions_and_checks_code_owners(monkeypatch):
    checker = AsyncMock()
    monkeypatch.setattr(native, "_require_stage_execution_catalog", checker)
    session = SimpleNamespace(scalar=AsyncMock(side_effect=[True, False, False]))
    await canonical.require_native_read_catalog(session, (10, 11))
    checker.assert_awaited_once_with(
        session,
        {"stage_relations": [{"relation_oid": 10}, {"relation_oid": 11}]},
        allowed_type_oids=(),
        allowed_extensions=("postgis", "intarray", "btree_gin", "btree_gist", "pg_trgm"),
    )
    assert [call.args[1] for call in session.scalar.await_args_list[1:]] == [{"oid": 10}, {"oid": 11}]


@pytest.mark.parametrize(
    "mutation", ["contract", "source_bit", "display_priority", "archive_oid", "contribution_oid", "extra"]
)
def test_local_publication_receipt_cannot_impersonate_foreign_provenance(mutation):
    receipt_by_field = {
        "contract": canonical.MERGE_CONTRACT,
        "source_bit": 1,
        "display_priority": 0,
        "archive_oid": 10,
        "contribution_oid": 20,
    }
    receipt_by_field[mutation] = True
    with pytest.raises(RuntimeError, match="local publication receipt"):
        canonical.validate_canonical_publication(receipt_by_field, "npi", 20)
