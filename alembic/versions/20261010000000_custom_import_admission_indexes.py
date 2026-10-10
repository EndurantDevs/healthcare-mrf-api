# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Prepare the existing child lookup index and candidate statistics for admission."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

from alembic import op

revision = "20261010000000_custom_import_admission_indexes"
down_revision = "20261009000000_custom_import_child_presence_decode"
branch_labels = None
depends_on = None


def _previous():
    path = Path(__file__).with_name("20261009000000_custom_import_child_presence_decode.py")
    specification = importlib.util.spec_from_file_location("admission_indexes_previous", path)
    assert specification and specification.loader
    migration = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(migration)
    migration.op = op
    return migration


def _schema() -> str:
    return _previous()._schema()


def _specifications(finality, *, corrected: bool) -> str:
    """Prepare one existing index early; keep graph preparation for older workers."""
    if not corrected:
        return finality._INDEX_SPECIFICATIONS
    specifications = json.loads(finality._INDEX_SPECIFICATIONS)
    child_index = next(item for item in specifications if item["name"] == "custom_import_build_graph_child_idx")
    specifications.append({**child_index, "phase": "admission"})
    phase_ordinal_by_name = {
        phase: ordinal for ordinal, phase in enumerate(("admission", "graph", "output", "serving"))
    }
    specifications.sort(
        key=lambda item: (phase_ordinal_by_name[item["phase"]], item["name"] if item["phase"] != "serving" else "")
    )
    return json.dumps(specifications, separators=(",", ":"))


def _definition(finality, schema: str, name: str, *, corrected: bool) -> str:
    """Render the existing fenced index lifecycle; never create another entry point."""
    bulk = finality._bulk()
    storage = bulk._storage()
    body = finality._INDEX_BODY.replace(
        "__INDEX_SPECS__", storage._literal(_specifications(finality, corrected=corrected)) + "::jsonb"
    )
    if name == "prepare_custom_import_snapshot_indexes":
        statistics = finality._CANDIDATE_STATISTICS
        if corrected:
            statistics = statistics.replace("IF p_phase='serving' THEN", "IF p_phase IN ('admission','serving') THEN")
            statistics = statistics.replace("Writes are closed; sample only", "Refresh statistics only on")
        body = body.replace(
            "    PERFORM __CONTROL__.resolve_custom_import_snapshot_relations(f.family_id);",
            statistics + "    PERFORM __CONTROL__.resolve_custom_import_snapshot_relations(f.family_id);",
            1,
        )
    elif name == "verify_custom_import_snapshot_indexes":
        body = body.replace("IF p_phase='serving' THEN", "IF true THEN")
        start = body.index("            -- One native index per transaction")
        end = body.index("        END IF;", start)
        body = body[:start] + "            RAISE EXCEPTION 'custom_import_snapshot_index_missing';\n" + body[end:]
    else:
        raise ValueError("unsupported snapshot index operation")
    return (
        f"CREATE FUNCTION {storage._quote(schema)}.{name}(p_family_id bigint,p_phase text) RETURNS boolean\n"
        "LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog\n"
        f"AS $snapshot_finality$ {bulk._control_sql(storage, schema, body)} $snapshot_finality$"
    )


def _install(*, corrected: bool) -> None:
    """Change fixed schedules only, preserving function identities and all privileges."""
    previous = _previous()
    finality = previous._bulk()._previous("20261005060000_custom_import_snapshot_finality")
    schema = _schema()
    quoted = finality._bulk()._storage()._quote(schema)
    op.execute(f"LOCK TABLE {quoted}.custom_import_snapshot_family IN SHARE ROW EXCLUSIVE MODE")
    for name in ("prepare_custom_import_snapshot_indexes", "verify_custom_import_snapshot_indexes"):
        previous._refresh(
            op.get_bind(),
            f"{quoted}.{name}(bigint,text)",
            f"{quoted}.custom_import_generation",
            "boolean",
            _definition(finality, schema, name, corrected=not corrected),
            _definition(finality, schema, name, corrected=corrected),
            "$snapshot_finality$",
            preserve_execute_grants=name == "prepare_custom_import_snapshot_indexes",
        )


def upgrade() -> None:
    """Prepare the generic child index before its first source membership lookup."""
    _install(corrected=True)


def downgrade() -> None:
    """Restore scheduling without dropping candidate or serving indexes."""
    _install(corrected=False)
