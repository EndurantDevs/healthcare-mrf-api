# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Owner-schema validation accepts real model metadata without weakening custody."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from sqlalchemy.dialects.postgresql import dialect
from sqlalchemy.sql import quoted_name

from db.models.custom_import_storage import CustomImportSnapshotFamily
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
    assert session.scalar.await_args.args[1] == {"registry": f"{quoted}.custom_import_snapshot_family"}


async def test_owner_schema_preserves_translated_identifier_quoting():
    schema = quoted_name("owner_schema", quote=True)
    session = _owner_session(options={"schema_translate_map": {CustomImportSnapshotFamily.__table__.schema: schema}})

    actual_schema, quoted = await _owner_schema(session)

    assert actual_schema is schema
    assert quoted == '"owner_schema"'
    assert session.scalar.await_args.args[1] == {"registry": '"owner_schema".custom_import_snapshot_family'}


@pytest.mark.parametrize("schema", ("a" * 63, quoted_name("é" * 31 + "a", quote=None)))
async def test_owner_schema_accepts_identifiers_at_existing_byte_limit(schema):
    session = _owner_session(options={"schema_translate_map": {CustomImportSnapshotFamily.__table__.schema: schema}})

    assert (await _owner_schema(session))[0] is schema
    session.scalar.assert_awaited_once()


class _StringLike:
    def __str__(self):
        return "owner_schema"


@pytest.mark.parametrize("schema", (None, True, 1, _StringLike(), "", "a" * 64, "é" * 32))
async def test_owner_schema_rejects_invalid_translations_before_authority_query(schema):
    session = _owner_session(options={"schema_translate_map": {CustomImportSnapshotFamily.__table__.schema: schema}})

    with pytest.raises(SnapshotRetirementRefused, match="schema is invalid"):
        await _owner_schema(session)

    session.scalar.assert_not_awaited()


@pytest.mark.parametrize("allowed", (False, None, 1))
async def test_actual_model_schema_still_requires_true_native_owner_authority(allowed):
    session = _owner_session(allowed=allowed)

    with pytest.raises(SnapshotRetirementRefused, match="protected native owner authority and finite bounds"):
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
