# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Connection preflight uses real owner observations without a writer window."""

from contextlib import nullcontext
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.dialects.postgresql import dialect
from sqlalchemy.sql.elements import quoted_name

from db.models.custom_import_storage import CustomImportSnapshotFamily
from process.custom_import import build_source
from process.custom_import.family_shard_owner_access import require_owner_connection


def _session(*, role="synthetic_owner", procedures=True, tables=True):
    connection = SimpleNamespace(
        sync_connection=SimpleNamespace(
            get_execution_options=lambda: {
                "schema_translate_map": {
                    CustomImportSnapshotFamily.__table__.schema: quoted_name("synthetic-control", True)
                }
            }
        ),
        dialect=dialect(),
    )
    return SimpleNamespace(
        info={},
        no_autoflush=nullcontext(),
        execute=AsyncMock(),
        scalar=AsyncMock(side_effect=[role, procedures, tables]),
        connection=AsyncMock(return_value=connection),
    )


async def test_preflight_checks_exact_existing_owner_catalog_without_window():
    session = _session()
    await require_owner_connection(session, "synthetic_owner")
    assert session.info == {} and build_source._WINDOW not in session.info
    assert str(session.execute.await_args_list[0].args[0]) == "SET TRANSACTION READ ONLY"
    assert session.scalar.await_args_list[1].args[1] == {
        "authorize": '"synthetic-control".source_bulk_authorize(pg_catalog.int8,pg_catalog.int2,pg_catalog.int8,pg_catalog.bytea,pg_catalog.int4,pg_catalog.int8)',
        "finalize": '"synthetic-control".source_set_finalize(pg_catalog.uuid,pg_catalog.int4[])',
    }
    assert session.scalar.await_args_list[2].args[1] == {
        "names": [
            '"synthetic-control".custom_import_snapshot_family',
            '"synthetic-control".custom_import_family_block_layout',
            '"synthetic-control".custom_import_family_block_relation',
        ]
    }


@pytest.mark.parametrize("problem", ["role", "procedures", "tables"])
async def test_preflight_refuses_wrong_or_partial_owner_without_window(problem):
    session = _session(
        role="foreign" if problem == "role" else "synthetic_owner",
        procedures=problem != "procedures",
        tables=problem != "tables",
    )
    with pytest.raises(ValueError, match="^owner handoff is unavailable$"):
        await require_owner_connection(session, "synthetic_owner")
    assert not session.info
    assert all(
        "INSERT" not in str(call.args[0]) and "UPDATE" not in str(call.args[0])
        for call in session.execute.await_args_list
    )
