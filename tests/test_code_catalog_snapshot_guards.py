# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Catalog archive admission preserves access and rejects unsafe cutovers."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import code_catalog_snapshot as catalog


def _capture():
    return catalog.CodeCatalogCapture(
        catalog.CodeCatalogResultReceipt(catalog._table_name(), "a" * 64, 1, "b" * 64), 17
    )


@pytest.mark.parametrize(
    "changes",
    [
        {"table_name": "other"},
        {"row_count": True},
        {"row_count": -1},
        {"schema_sha256": "bad"},
        {"row_sha256": "bad"},
        {"extra": 1},
    ],
)
def test_catalog_receipt_requires_exact_full_table_identity(changes):
    with pytest.raises(catalog.CodeCatalogSnapshotError, match="result receipt is invalid"):
        catalog.validate_code_catalog_result_receipt({**_capture().receipt.as_dict(), **changes})


@pytest.mark.asyncio
@pytest.mark.parametrize("schema", [None, "bad;schema"])
async def test_capture_rejects_invalid_schema_before_database_access(schema):
    session = SimpleNamespace(execute=AsyncMock())
    with pytest.raises(catalog.CodeCatalogSnapshotError, match="schema is invalid"):
        await catalog.capture_code_catalog_result(session, schema_name=schema)
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_capture_requires_an_active_transaction():
    session = SimpleNamespace(in_transaction=lambda: False, execute=AsyncMock())
    with pytest.raises(catalog.CodeCatalogSnapshotError, match="caller transaction"):
        await catalog.capture_code_catalog_result(session, schema_name="mrf")
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["session", "relation", "schema", "rows"])
async def test_capture_translates_receipt_failure_without_publishing(monkeypatch, failure):
    error = catalog._receipt.EntityAddressArchiveReceiptError("synthetic invalid catalog")
    monkeypatch.setattr(
        catalog._receipt, "_normalize_receipt_session", AsyncMock(side_effect=error if failure == "session" else None)
    )
    monkeypatch.setattr(
        catalog._receipt,
        "_relation_oid",
        AsyncMock(return_value=17, side_effect=error if failure == "relation" else None),
    )
    monkeypatch.setattr(
        catalog._receipt,
        "_schema_identity",
        AsyncMock(return_value="a" * 64, side_effect=error if failure == "schema" else None),
    )
    monkeypatch.setattr(catalog._receipt, "_row_identity", AsyncMock(side_effect=error if failure == "rows" else None))
    session = SimpleNamespace(in_transaction=lambda: True, execute=AsyncMock())
    with pytest.raises(catalog.CodeCatalogSnapshotError) as observed:
        await catalog.capture_code_catalog_result(session, schema_name="mrf")
    assert observed.value.__cause__ is error
    assert all(str(call.args[0]).startswith("LOCK TABLE ") for call in session.execute.await_args_list)


@pytest.mark.parametrize(
    "changes",
    [
        {"grantee_name": 1},
        {"privilege_type": "SUPERUSER"},
        {"grantor_name": None},
        {"grantor_name": "other_owner"},
        {"column_name": "bad;column"},
        {"column_name": "code", "privilege_type": "TRUNCATE"},
    ],
)
def test_access_grants_cannot_change_grantor_or_privilege_scope(changes):
    grant_by_field = {
        "column_name": None,
        "grantee_name": "reader",
        "grantee_is_public": False,
        "privilege_type": "SELECT",
        "is_grantable": False,
        "grantor_name": "owner",
        **changes,
    }
    with pytest.raises(catalog.CodeCatalogSnapshotError):
        catalog._validated_access_grant(grant_by_field, "owner")


@pytest.mark.asyncio
@pytest.mark.parametrize("owner_name", [None, "", 17])
async def test_cutover_cannot_preserve_an_unavailable_owner(owner_name):
    session = SimpleNamespace(scalar=AsyncMock(return_value=owner_name), execute=AsyncMock())
    with pytest.raises(catalog.CodeCatalogSnapshotError, match="owner is unavailable"):
        await catalog._relation_access(session, 17)
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("has_acl", [None, True])
async def test_restored_stage_must_not_import_access_grants(has_acl):
    session = SimpleNamespace(scalar=AsyncMock(return_value=has_acl))
    with pytest.raises(catalog.CodeCatalogSnapshotError, match="unsupported access grants"):
        await catalog._require_stage_has_no_acl(session, 17)


@pytest.mark.asyncio
async def test_access_replay_cannot_assume_another_owner():
    session = SimpleNamespace(scalar=AsyncMock(return_value="other_owner"), execute=AsyncMock())
    with pytest.raises(catalog.CodeCatalogSnapshotError, match="not owned by the local cutover role"):
        await catalog._apply_relation_access(
            session, "mrf", catalog._table_name(), catalog._RelationAccess("owner", ())
        )
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("quoted_role", [None, "", 17])
async def test_access_replay_rejects_unavailable_role_quoting(quoted_role):
    session = SimpleNamespace(scalar=AsyncMock(return_value=quoted_role))
    with pytest.raises(catalog.CodeCatalogSnapshotError, match="grant role is unavailable"):
        await catalog._quoted_role(session, "reader")


@pytest.mark.asyncio
async def test_live_table_cannot_be_admitted_as_restored_stage():
    session = SimpleNamespace(execute=AsyncMock())
    with pytest.raises(catalog.CodeCatalogSnapshotError, match="must not be the live table"):
        await catalog.validate_code_catalog_restored_stage(
            session, schema_name="mrf", stage_table_name=catalog._table_name(), expected_receipt=_capture().receipt
        )
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changes",
    [
        {"incumbent_capture": None},
        {"expected_stage_capture": None},
        {"require_import_idle": None},
        {"retained_table_name": "stage"},
    ],
)
async def test_promotion_rejects_missing_authority_or_overlapping_table_names(changes):
    arguments_by_field = {
        "schema_name": "mrf",
        "stage_table_name": "stage",
        "retained_table_name": "old_catalog",
        "incumbent_capture": _capture(),
        "expected_stage_capture": _capture(),
        "require_import_idle": AsyncMock(),
        **changes,
    }
    session = SimpleNamespace(execute=AsyncMock())
    with pytest.raises(catalog.CodeCatalogSnapshotError):
        await catalog.promote_code_catalog_restored_stage(session, **arguments_by_field)
    session.execute.assert_not_awaited()
