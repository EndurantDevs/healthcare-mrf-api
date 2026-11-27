# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Recognize roots already replaced by native partitioned storage."""

import json

from db.migration_provider_directory_dataset_candidates import TABLES
from process.provider_directory_terminal_root_retirement_contract import (
    TerminalRootRetirementError,
    quoted_relation,
    row_mapping,
)

_ROOTS = TABLES[:5]
_ROOT_KINDS = """SELECT r.root,c.relkind::text AS relkind
 FROM jsonb_to_recordset(CAST(:roots AS jsonb)) r(relation text,root text)
 JOIN pg_class c ON c.oid=to_regclass(r.relation)"""


def _require(condition):
    if not condition:
        raise TerminalRootRetirementError("state_invalid")


async def require_native_retirement_controls(database):
    """Return the native roots; a partial conversion or a session that skips triggers is refused."""
    roots = [{"relation": quoted_relation(root), "root": root} for root in _ROOTS]
    root_rows = [row_mapping(root_record) for root_record in await database.all(_ROOT_KINDS, roots=json.dumps(roots))]
    native_roots = {root_record["root"] for root_record in root_rows if root_record["relkind"] == "p"}
    if not native_roots:
        return set()
    _require(native_roots == set(_ROOTS))
    _require(await database.scalar("SELECT current_setting('session_replication_role')") == "origin")
    return native_roots
