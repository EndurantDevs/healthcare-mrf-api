# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Configured read-only DCL closes grants transferred from a different builder."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process.provider_directory_generic_source_provision import freeze_fhir_source_snapshot


@pytest.mark.asyncio
async def test_freeze_closes_owner_table_column_sequence_and_schema_writes():
    connection = SimpleNamespace(
        is_in_transaction=lambda: True,
        execute=AsyncMock(),
        fetch=AsyncMock(
            side_effect=[
                [{"relname": "candidate", "relkind": "r"}, {"relname": "candidate_id_seq", "relkind": "S"}],
                [{"role": "builder", "column_name": None}, {"role": "builder", "column_name": "value"}],
                [{"role": "builder", "column_name": None}],
                [{"role": "builder"}],
            ]
        ),
    )
    await freeze_fhir_source_snapshot(
        connection,
        "candidate_schema",
        namespace_owner="snapshot_owner",
        reader_roles=("snapshot_owner", "publisher", "reader"),
        application_roles=(),
    )
    statements = [call.args[0] for call in connection.execute.await_args_list]
    for kind, name in (("TABLE", "candidate"), ("SEQUENCE", "candidate_id_seq")):
        relation = f'"candidate_schema"."{name}"'
        alter = f'ALTER {kind} {relation} OWNER TO "snapshot_owner"'
        revoke = f'REVOKE ALL ON {kind} {relation} FROM "snapshot_owner" CASCADE'
        grant = f'GRANT SELECT ON {kind} {relation} TO "snapshot_owner","publisher","reader"'
        assert statements.index(alter) < statements.index(revoke) < statements.index(grant)
    assert 'REVOKE ALL ("value") ON TABLE "candidate_schema"."candidate" FROM "snapshot_owner" CASCADE' in statements
    assert statements.index('REVOKE ALL ON SCHEMA "candidate_schema" FROM "snapshot_owner" CASCADE') < statements.index(
        'GRANT USAGE ON SCHEMA "candidate_schema" TO "snapshot_owner","publisher","reader"'
    )
    assert not any(
        "GRANT INSERT" in sql or "GRANT UPDATE" in sql or "GRANT CREATE" in sql or 'REVOKE "snapshot_owner" FROM' in sql
        for sql in statements
    )


@pytest.mark.asyncio
async def test_selected_auxiliary_freeze_leaves_schema_memberships_and_unselected_heaps_untouched():
    connection = SimpleNamespace(
        is_in_transaction=lambda: True,
        execute=AsyncMock(),
        fetch=AsyncMock(
            side_effect=[
                [{"relname": "finished", "relkind": b"r"}],
                [{"relname": "finished_id_seq", "relkind": "S"}],
                [{"role": "builder", "column_name": "value"}],
                [{"role": "builder", "column_name": None}],
            ]
        ),
    )
    await freeze_fhir_source_snapshot(
        connection,
        "shared_schema",
        namespace_owner="snapshot_owner",
        reader_roles=("snapshot_owner", "publisher", "reader"),
        application_roles=("reader",),
        relation_names=("finished",),
        freeze_schema=False,
    )
    statements = [call.args[0] for call in connection.execute.await_args_list]
    assert 'ALTER TABLE "shared_schema"."finished" OWNER TO "snapshot_owner"' in statements
    assert 'REVOKE ALL ON SEQUENCE "shared_schema"."finished_id_seq" FROM "snapshot_owner" CASCADE' in statements
    assert 'GRANT SELECT ON TABLE "shared_schema"."finished" TO "snapshot_owner","publisher","reader"' in statements
    assert all(
        " ON SCHEMA " not in sql and not sql.startswith("ALTER SCHEMA ") and 'REVOKE "snapshot_owner" FROM' not in sql
        for sql in statements
    )
    assert all("unselected" not in sql for sql in statements)
    assert connection.fetch.await_args_list[0].args[1:] == ("shared_schema", ["finished"])
    assert connection.fetch.await_args_list[1].args[1:] == ("shared_schema", ["finished"])


@pytest.mark.asyncio
@pytest.mark.parametrize("names", [(), ("missing",)])
async def test_selected_auxiliary_freeze_requires_complete_nonempty_declaration(names):
    connection = SimpleNamespace(is_in_transaction=lambda: True, execute=AsyncMock(), fetch=AsyncMock(return_value=[]))
    with pytest.raises(ValueError, match="selection_invalid|selection_incomplete"):
        await freeze_fhir_source_snapshot(
            connection,
            "shared_schema",
            namespace_owner="snapshot_owner",
            reader_roles=("reader",),
            relation_names=names,
            freeze_schema=False,
        )
    connection.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_selected_freeze_refuses_a_partition_parent_without_touching_its_children():
    connection = SimpleNamespace(
        is_in_transaction=lambda: True,
        execute=AsyncMock(),
        fetch=AsyncMock(return_value=[{"relname": "finished", "relkind": "p"}]),
    )
    with pytest.raises(ValueError, match="selection_incomplete"):
        await freeze_fhir_source_snapshot(
            connection,
            "shared_schema",
            namespace_owner="snapshot_owner",
            relation_names=("finished",),
            freeze_schema=False,
        )
    connection.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("freeze_schema", [False, True])
async def test_private_model_seal_transfers_schema_before_freezing_relations(monkeypatch, freeze_schema):
    from process import provider_directory_generic_source_provision as provision
    from process import reference_family_archive as archive

    ownership = SimpleNamespace(
        importer_id="nucc", schema_name="candidate_schema", relation_names=("payload",), sequence_names=()
    )
    session = SimpleNamespace(in_transaction=lambda: True)
    driver = SimpleNamespace(execute=AsyncMock())
    monkeypatch.setattr(archive, "_relation_oid", AsyncMock(return_value=7))
    monkeypatch.setattr(archive, "_schema_oid", AsyncMock(return_value=8))
    monkeypatch.setattr(archive, "_owned_sequences", AsyncMock(return_value=()))
    monkeypatch.setattr(archive, "_namespace_relations", AsyncMock(return_value=[dict(oid=7, relkind="r")]))
    monkeypatch.setattr(archive, "_native_model_copy_driver", AsyncMock(return_value=driver))
    roles = SimpleNamespace(owner_role="snapshot_owner", publisher_role="publisher", runtime_roles=("reader",))

    async def freeze(actual, schema, **kwargs):
        assert actual is driver and schema == ownership.schema_name
        driver.execute.assert_awaited_once_with('ALTER SCHEMA "candidate_schema" OWNER TO "snapshot_owner"')
        assert kwargs == dict(
            namespace_owner="snapshot_owner",
            reader_roles=("snapshot_owner", "publisher", "reader"),
            application_roles=(),
            relation_names=None if freeze_schema else ownership.relation_names,
            freeze_schema=freeze_schema,
        )

    monkeypatch.setattr(provision, "freeze_fhir_source_snapshot", freeze)
    await archive.seal_model_family_storage(session, ownership, roles=roles, freeze_schema=freeze_schema)
