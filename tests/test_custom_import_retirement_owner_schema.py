# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Maintenance resolves quoted schemas and preserves transaction and census bounds."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from sqlalchemy.dialects.postgresql import dialect
from sqlalchemy.sql import quoted_name

from db.models.custom_import_storage import CustomImportSnapshotFamily
from process.custom_import import retirement
from process.custom_import.retirement import SnapshotRetirementRefused, _owner_schema


def _owner_session(*, options=None, allowed=True):
    connection = SimpleNamespace(
        sync_connection=SimpleNamespace(get_execution_options=Mock(return_value=options or {})),
        dialect=dialect(),
    )
    return SimpleNamespace(
        in_transaction=Mock(return_value=True),
        new=(),
        dirty=(),
        deleted=(),
        connection=AsyncMock(return_value=connection),
        scalar=AsyncMock(return_value=allowed),
    )


async def test_owner_schema_accepts_actual_model_default_without_translation():
    schema = CustomImportSnapshotFamily.__table__.schema
    assert isinstance(schema, quoted_name)
    session = _owner_session()

    actual_schema, quoted = await _owner_schema(session)

    assert actual_schema is schema
    assert quoted == dialect().identifier_preparer.quote_schema(schema)
    session.scalar.assert_awaited_once()
    statement = str(session.scalar.await_args.args[0])
    assert all(
        name in statement
        for name in ("transaction_isolation", "transaction_read_only", "statement_timeout", "lock_timeout")
    )
    assert all(name not in statement for name in ("current_user", "session_user", "pg_roles", "rolsuper", "registry"))


async def test_owner_schema_preserves_translated_identifier_quoting():
    schema = quoted_name("owner_schema", quote=True)
    session = _owner_session(options={"schema_translate_map": {CustomImportSnapshotFamily.__table__.schema: schema}})

    actual_schema, quoted = await _owner_schema(session)

    assert actual_schema is schema
    assert quoted == '"owner_schema"'
    assert len(session.scalar.await_args.args) == 1


@pytest.mark.parametrize("schema", ("a" * 63, quoted_name("é" * 31 + "a", quote=None)))
async def test_owner_schema_accepts_identifiers_at_existing_byte_limit(schema):
    session = _owner_session(options={"schema_translate_map": {CustomImportSnapshotFamily.__table__.schema: schema}})

    assert (await _owner_schema(session))[0] is schema
    session.scalar.assert_awaited_once()


class _StringLike:
    def __str__(self):
        return "owner_schema"


@pytest.mark.parametrize("schema", (None, True, 1, _StringLike(), "", "a" * 64, "é" * 32))
async def test_owner_schema_rejects_invalid_translations_before_transaction_query(schema):
    session = _owner_session(options={"schema_translate_map": {CustomImportSnapshotFamily.__table__.schema: schema}})

    with pytest.raises(SnapshotRetirementRefused, match="schema is invalid"):
        await _owner_schema(session)

    session.scalar.assert_not_awaited()


@pytest.mark.parametrize("allowed", (False, None, 1))
async def test_actual_model_schema_requires_writable_bounded_read_committed_transaction(allowed):
    session = _owner_session(allowed=allowed)

    with pytest.raises(SnapshotRetirementRefused, match="writable bounded read-committed transaction"):
        await _owner_schema(session)

    session.scalar.assert_awaited_once()


@pytest.mark.parametrize("state", ("new", "dirty", "deleted", "no_transaction"))
async def test_owner_schema_requires_clean_caller_transaction_before_connection(state):
    session = _owner_session()
    if state == "no_transaction":
        session.in_transaction.return_value = False
    else:
        setattr(session, state, (object(),))

    with pytest.raises(SnapshotRetirementRefused, match="clean caller transaction"):
        await _owner_schema(session)

    session.connection.assert_not_awaited()
    session.scalar.assert_not_awaited()


@pytest.mark.parametrize("change", (None, "logical_family", "optional_relation"))
async def test_final_reference_census_uses_logical_family_and_qualified_relation_names(monkeypatch, change):
    family_values_by_name = {name: 1 for name in retirement._IDENTITY_NAMES}
    family_values_by_name.update(producing_token_sha256=b"t" * 32, created_at=None, frozen_at=None)
    family = SimpleNamespace(**family_values_by_name)
    observed_values_by_name = dict(family_values_by_name)
    if change == "logical_family":
        observed_values_by_name["generation_id"] = 2
    session = SimpleNamespace(
        execute=AsyncMock(
            side_effect=(None, None, SimpleNamespace(all=lambda: [tuple(observed_values_by_name.values())]))
        ),
    )
    optional = ("source_bulk_landing",)
    names = (*optional, "legacy_copy_origin") if change == "optional_relation" else optional
    monkeypatch.setattr(retirement, "_optional_relation_names", AsyncMock(return_value=names))
    for name in ("assert_closed_continuations", "_close_mutable_references", "_revision_references"):
        monkeypatch.setattr(retirement, name, AsyncMock())
    dropped = AsyncMock(return_value="retired")
    monkeypatch.setattr(retirement, "_retire_verified_family", dropped)

    operation = retirement._commit_reference_closure(
        session, '"control"', family, [family], {1: optional}, "synthetic", "superseded"
    )
    if change:
        with pytest.raises(SnapshotRetirementRefused, match="reference census changed"):
            await operation
        dropped.assert_not_awaited()
        retirement._revision_references.assert_not_awaited()
    else:
        assert await operation == "retired"
        retirement._revision_references.assert_awaited_once_with(session, family, [])
        dropped.assert_awaited_once_with(session, '"control"', family, "synthetic", "superseded")
