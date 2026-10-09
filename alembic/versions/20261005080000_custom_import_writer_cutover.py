# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Close canonical write authority before retiring replaced record guards.

This revision does not provision roles or activate a snapshot. Existing reviewed
dispatcher ACLs survive; private provisioning grants new entry points separately.
The migration owner and roles able to assume it remain the trusted boundary.
"""

from __future__ import annotations

import hashlib
import importlib.util
import json
import re
from pathlib import Path

from sqlalchemy import text

from alembic import op

revision = "20261005080000_custom_import_writer_cutover"
down_revision = "20261005070000_custom_import_materialization_storage"
branch_labels = None
depends_on = None

_RESOURCE_DIRECTORY = Path(__file__).resolve().parents[1] / "sql/custom_import_writer_cutover"
_RESOURCES = ("quiescence.sql", "privileges.sql", "functions.sql", "guards.sql")
_VERSIONS = (
    "20261005030000_custom_import_snapshot_storage",
    "20261005040000_custom_import_bulk_snapshot_writers",
    "20261005050000_custom_import_legacy_snapshot_writers",
    "20261005060000_custom_import_snapshot_finality",
    "20261005070000_custom_import_materialization_storage",
)
_CONTROL_TABLES = (
    "custom_import_snapshot_family",
    "custom_import_snapshot_relation",
    "source_bulk_authorization",
    "source_bulk_completion",
    "custom_import_revision_home",
    "custom_import_materialization_page",
)
_OBSOLETE = (
    "commit_custom_import_build_source_page(bigint,bigint)",
    "commit_custom_import_build_copy_pack(bigint,bigint)",
    "commit_custom_import_build_family_page(bigint,bigint,bigint)",
    "commit_custom_import_build_winner_group(bigint,bigint)",
    "next_custom_import_build_child(bigint,bigint,smallint,bytea,bigint)",
    "check_custom_import_build_record_work(bigint,bigint,bigint,bigint)",
)
_RUNTIME_NAMES = frozenset(
    (
        "resolve_custom_import_generation_snapshot",
        "resolve_custom_import_build_snapshot",
        "resolve_custom_import_source_batch_snapshot",
        "resolve_custom_import_build_base_snapshot",
        "resolve_custom_import_legacy_generation_snapshot",
        "resolve_custom_import_generation_finality_snapshot",
        "freeze_custom_import_snapshot_family",
        "prepare_custom_import_snapshot_indexes",
        "verify_custom_import_build_structure",
        "check_custom_import_materialization_expected",
        "check_custom_import_materialization_authority",
        "check_custom_import_generation_materialization_authority",
        "custom_import_materialization_budget",
        "persist_custom_import_scalar_set",
        "persist_custom_import_winner_set",
        "begin_custom_import_build",
        "lock_custom_import_build",
    )
)
_WRITE_ROOT_NAMES = (
    "create_custom_import_snapshot_family",
    "bind_custom_import_snapshot_generation",
    "freeze_custom_import_snapshot_family",
    "resolve_custom_import_build_snapshot",
    "resolve_custom_import_legacy_generation_snapshot",
    "install_custom_import_snapshot_writers",
    "install_custom_import_legacy_snapshot_writers",
    "install_custom_import_materialization_writers",
    "append_custom_import_revision_home",
    "persist_custom_import_scalar_set",
    "persist_custom_import_winner_set",
    "persist_custom_import_scalar_models",
    "persist_custom_import_winner_models",
    "prepare_custom_import_snapshot_indexes",
)
_DEFINITION = re.compile(
    r"\A\s*(?:--[^\n]*\n\s*)*CREATE(?: OR REPLACE)? FUNCTION\s+(?P<qualified>\"(?:[^\"]|\"\")+\"\.[a-z_]+)"
    r"\((?P<arguments>.*?)\)\s+RETURNS.*?\bAS\s+(?P<delimiter>\$[a-z_]*\$)"
    r"(?P<body>.*)(?P=delimiter)\s*;?\s*\Z",
    re.DOTALL,
)


def _previous(name: str):
    path = Path(__file__).with_name(name + ".py")
    spec = importlib.util.spec_from_file_location("cutover_" + name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError("custom import cutover prerequisite missing")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _schema() -> str:
    return _previous(_VERSIONS[0])._schema()


class _DefinitionReceipt:
    """Render earlier fixed definitions without executing their installation."""

    def __init__(self):
        self.statements: list[str] = []

    def execute(self, statement: str) -> None:
        """Retain a rendered statement without making a database call."""
        self.statements.append(statement)


def _reviewed_functions(schema: str, *, include_obsolete: bool = False) -> list[dict]:
    """Pin exact current bodies, not merely familiar function names."""
    bulk = _previous(_VERSIONS[1])
    runtime = _RUNTIME_NAMES | frozenset(writer[0] for writer in bulk._WRITERS)
    legacy = _previous(_VERSIONS[2])
    runtime |= frozenset(writer[0] for writer in legacy._WRITERS)
    definition_by_identity = {}
    historical = ("20261002030000_custom_import_identical_children",) if include_obsolete else ()
    for name in ("20261002010000_custom_import_bounded_build", *historical, *_VERSIONS):
        migration = _previous(name)
        receipt = _DefinitionReceipt()
        migration.op = receipt
        migration._schema = lambda: schema
        if name == "20261002010000_custom_import_bounded_build":
            migration._authority_functions(schema)
            if include_obsolete:
                migration._source_functions(schema)
                migration._family_functions(schema)
                migration._output_functions(schema)
        else:
            migration.upgrade()
        for statement in receipt.statements:
            match = _DEFINITION.fullmatch(statement)
            if match is None or "SECURITY DEFINER" not in statement[: match.start("body")]:
                continue
            function_name = match["qualified"].rsplit(".", 1)[1]
            arguments = bulk._argument_types(match["arguments"]) if match["arguments"].strip() else ""
            identity = f"{match['qualified']}({arguments})"
            definition_by_identity[identity] = {
                "identity": identity,
                "body_sha256": hashlib.sha256(match["body"].encode()).hexdigest(),
                "runtime": function_name in runtime,
            }
            if include_obsolete:
                definition_by_identity[identity]["language"] = (
                    statement[: match.start("body")].split("LANGUAGE ", 1)[1].split()[0].lower()
                )
    if not definition_by_identity or not runtime <= {
        identity.split("(")[0].rsplit(".", 1)[1] for identity in definition_by_identity
    }:
        raise RuntimeError("custom import cutover definition inventory incomplete")
    return sorted(definition_by_identity.values(), key=lambda definition: definition["identity"])


def _guard_specs(hot_tables: tuple[str, ...]) -> list[dict]:
    """Inventory exact old names, functions and event bits on the fixed fifteen."""
    specs = []
    groups = (
        (hot_tables[:12], "_immutable_row_guard", "guard_custom_import_immutable_row", 27),
        (hot_tables[:2], "_dataset_first_insert_guard", "guard_custom_import_dataset_first_insert", 7),
        (hot_tables[2:12], "_sealed_append_guard", "guard_custom_import_sealed_append", 7),
        (hot_tables[2:12], "_build_phase", "guard_custom_import_build_graph", 7),
        (hot_tables[:12], "_build_frozen", "guard_custom_import_build_frozen_row", 27),
        (
            hot_tables[2:12] + (hot_tables[12], hot_tables[14]),
            "_build_link",
            "check_custom_import_build_link",
            5,
        ),
        (
            (hot_tables[3], hot_tables[8], hot_tables[11], hot_tables[14]),
            "_build_charge",
            "charge_custom_import_build_row",
            5,
        ),
    )
    for tables, suffix, function, events in groups:
        specs.extend(
            {"table": table, "trigger": table + suffix, "function": function, "events": events} for table in tables
        )
    specs.extend(
        {
            "table": table,
            "trigger": table + "_guard",
            "function": "guard_custom_import_build_owned"
            if table == hot_tables[13]
            else "guard_custom_import_build_evidence",
            "events": 31,
        }
        for table in hot_tables[12:]
    )
    return specs


def _independent_reviewed_functions(schema: str) -> list[dict]:
    """Render exact receipts for possible independent installations, not approval."""
    namespaces = op.get_bind().execute(
        text(
            "SELECT DISTINCT n.nspname FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace "
            "WHERE p.prokind='f' AND p.proname='begin_custom_import_build' AND n.nspname<>:schema "
            "ORDER BY n.nspname"
        ),
        {"schema": schema},
    )
    receipts = []
    correction = _previous("20261007000000_custom_import_rejection_anti_joins")
    child_presence = _previous("20261009000000_custom_import_child_presence_decode")
    for namespace in namespaces.scalars():
        previous = _reviewed_functions(namespace, include_obsolete=True)
        receipts.extend(previous)
        validator = next(
            receipt
            for receipt in previous
            if receipt["identity"].endswith(".verify_custom_import_snapshot_structure(bigint)")
        )
        body = " " + correction._body(correction._finality(), namespace, corrected=True) + " "
        receipts.append({**validator, "body_sha256": hashlib.sha256(body.encode()).hexdigest()})
        installer = next(
            receipt
            for receipt in previous
            if receipt["identity"].endswith(".install_custom_import_snapshot_writers(bigint)")
        )
        body = child_presence._installer(child_presence._bulk(), namespace, corrected=True).split("$bulk_snapshot$")[1]
        receipts.append({**installer, "body_sha256": hashlib.sha256(body.encode()).hexdigest()})
    return receipts


def _render(schema: str, statement: str, reviewed: list[dict], independent: list[dict]) -> str:
    bulk = _previous(_VERSIONS[1])
    storage = bulk._storage()
    hot_tables = storage._RELATION_NAMES
    replacement_by_marker = {
        "__CONTROL_LITERAL__": storage._literal(schema),
        "__HOT_TABLES__": bulk._array(storage, hot_tables),
        "__CLOSED_TABLES__": bulk._array(storage, hot_tables + _CONTROL_TABLES),
        "__OBSOLETE__": bulk._array(storage, (f"{storage._quote(schema)}.{signature}" for signature in _OBSOLETE)),
        "__REVIEWED__": storage._literal(json.dumps(reviewed, separators=(",", ": "))) + "::jsonb",
        "__INDEPENDENT_REVIEWED__": storage._literal(json.dumps(independent, separators=(",", ": "))) + "::jsonb",
        "__GUARDS__": storage._literal(json.dumps(_guard_specs(hot_tables), separators=(",", ": "))) + "::jsonb",
        "__WRITE_ROOT_NAMES__": bulk._array(
            storage,
            (
                *_WRITE_ROOT_NAMES,
                *(writer[0] for writer in bulk._WRITERS),
                *(writer[0] for writer in _previous(_VERSIONS[2])._WRITERS),
            ),
        ),
    }
    for marker, replacement in replacement_by_marker.items():
        statement = statement.replace(marker, replacement)
    return bulk._control_sql(storage, schema, statement)


def upgrade() -> None:
    """One transaction closes write roots and callers before removing row guards."""
    schema = _schema()
    reviewed = _reviewed_functions(schema)
    independent = _independent_reviewed_functions(schema)
    for name in _RESOURCES:
        op.execute(_render(schema, (_RESOURCE_DIRECTORY / name).read_text(), reviewed, independent))


def downgrade() -> None:
    """Do not reconstruct previously unsafe ACLs or permit canonical writing."""
    raise RuntimeError("custom import writer cutover requires an explicit reviewed forward migration")
