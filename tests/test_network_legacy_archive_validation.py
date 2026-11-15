# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Archive scope remains semantic across named and saved receipt editions."""

import hashlib
from copy import deepcopy
from dataclasses import replace

import pytest

from process import network_legacy_membership_source as legacy
from process import reference_family_archive as archive
from tests.test_registry_aca_source_recipe import _prepared


def _named_validation(validation):
    fields = validation.as_dict()
    for retired in ("stage_schema_oid", "relation_oids", "sealed_owner_oid", "validation_sha256"):
        fields.pop(retired)
    fields["relations"] = [{"schema_name": schema, "relation_name": name} for schema, name in validation.relations]
    return archive.validate_reference_family_validation_receipt(
        {**fields, "validation_sha256": archive._validation_digest(fields)}
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("edition", ["named", "legacy"])
async def test_native_archive_matches_declared_names_and_preserves_saved_raw(monkeypatch, edition):
    prepared, validation = await _prepared(monkeypatch)
    if edition == "named":
        validation = _named_validation(validation)
    saved = deepcopy(validation.as_dict())
    ownership = replace(
        prepared.ownership,
        schema_oid=999,
        relation_oids=tuple((name, address + 1000) for name, address in prepared.ownership.relation_oids),
    )
    actual, validated = legacy._validated_native_archive(replace(prepared, ownership=ownership), validation)
    assert actual is ownership
    assert validated.relations == tuple((ownership.schema_name, name) for name in ownership.relation_names)
    assert validated.tables == prepared.manifest.tables
    assert validated.manifest_sha256 == hashlib.sha256(archive._canonical_json(prepared.manifest.as_dict())).hexdigest()
    assert validation.as_dict() == validated.as_dict() == saved


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["schema", "relation", "importer", "manifest_digest", "table_count", "table_schema"])
async def test_native_archive_refuses_foreign_scope_or_changed_data_receipts(monkeypatch, damage):
    prepared, validation = await _prepared(monkeypatch)
    validation = _named_validation(validation)
    match damage:
        case "schema":
            prepared = replace(prepared, ownership=replace(prepared.ownership, schema_name="foreign_stage"))
        case "relation":
            ownership = prepared.ownership
            prepared = replace(
                prepared,
                ownership=replace(ownership, relation_oids=(("foreign_table", 1), *ownership.relation_oids[1:])),
            )
        case "importer":
            prepared = replace(prepared, ownership=replace(prepared.ownership, importer_id="mrf-address"))
        case _:
            fields = validation.as_dict()
            fields.pop("validation_sha256")
            match damage:
                case "manifest_digest":
                    fields["manifest_sha256"] = "f" * 64
                case "table_count":
                    fields["tables"][0]["row_count"] += 1
                case "table_schema":
                    fields["tables"][0]["schema_sha256"] = "f" * 64
            validation = archive.validate_reference_family_validation_receipt(
                {**fields, "validation_sha256": archive._validation_digest(fields)}
            )
    with pytest.raises(legacy.LegacyMembershipSourceError, match="scope or digest differs"):
        legacy._validated_native_archive(prepared, validation)


@pytest.mark.asyncio
async def test_native_archive_still_rejects_corrupt_named_validation_digest(monkeypatch):
    prepared, validation = await _prepared(monkeypatch)
    validation = replace(_named_validation(validation), validation_sha256="f" * 64)
    with pytest.raises(legacy.LegacyMembershipSourceError, match="archive receipt differs"):
        legacy._validated_native_archive(prepared, validation)


@pytest.mark.asyncio
async def test_native_archive_locks_and_reads_complete_declared_model_inventory(monkeypatch):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    prepared, validation = await _prepared(monkeypatch)
    connection = SimpleNamespace(execute=AsyncMock())
    await legacy._require_native_archive(connection, prepared, validation, object())
    # The lock and every model read go to PostgreSQL in one simple-protocol round trip.
    connection.execute.assert_awaited_once()
    (round_trip,) = connection.execute.await_args.args
    statements = round_trip.split(";")
    namespace = '"' + prepared.ownership.schema_name + '"'
    names = prepared.ownership.relation_names
    assert (
        statements[0] == "LOCK TABLE " + ",".join(f'{namespace}."{name}"' for name in names) + " IN ACCESS SHARE MODE"
    )
    models = archive.reference_family_spec("mrf", canonical=True).model_types
    assert statements[1:] == [
        "SELECT "
        + ",".join('"' + str(column.name) + '"' for column in model.__table__.columns)
        + f' FROM {namespace}."{model.__tablename__}" LIMIT 0'
        for model in models
    ]
    assert not any(word in " ".join(statements) for word in ("pg_roles", "pg_class", "has_table_privilege", "relowner"))


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_at", ["LOCK TABLE", "SELECT"])
@pytest.mark.parametrize("error_type", ["denied", "missing", "cancel"])
async def test_native_archive_propagates_postgres_read_failures_and_cancellation(monkeypatch, failure_at, error_type):
    import asyncio
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from asyncpg import InsufficientPrivilegeError, UndefinedTableError

    prepared, validation = await _prepared(monkeypatch)
    error_type_by_kind = {
        "denied": InsufficientPrivilegeError,
        "missing": UndefinedTableError,
        "cancel": asyncio.CancelledError,
    }
    error = error_type_by_kind[error_type]("native read refused")

    async def native_execute(round_trip):
        # PostgreSQL stops at the first failing statement of the single round trip.
        if any(statement.startswith(failure_at) for statement in round_trip.split(";")):
            raise error

    connection = SimpleNamespace(execute=AsyncMock(side_effect=native_execute))
    with pytest.raises(type(error)) as raised:
        await legacy._require_native_archive(connection, prepared, validation, ())
    assert raised.value is error


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["namespace", "manifest"])
async def test_native_archive_refuses_changed_scope_or_manifest_before_reading(monkeypatch, damage):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    prepared, validation = await _prepared(monkeypatch)
    if damage == "namespace":
        prepared = replace(prepared, ownership=replace(prepared.ownership, schema_name="foreign_stage"))
    else:
        fields = _named_validation(validation).as_dict()
        fields.pop("validation_sha256")
        fields["manifest_sha256"] = "f" * 64
        validation = archive.validate_reference_family_validation_receipt(
            {**fields, "validation_sha256": archive._validation_digest(fields)}
        )
    connection = SimpleNamespace(execute=AsyncMock())
    with pytest.raises(legacy.LegacyMembershipSourceError):
        await legacy._require_native_archive(connection, prepared, validation, ())
    connection.execute.assert_not_awaited()
