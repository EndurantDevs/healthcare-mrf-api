# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Reuse SQL plans without caching sealed-append authority or changing its checks."""

from __future__ import annotations

import hashlib
import importlib.util
import re
from pathlib import Path

from alembic import op

revision = "20261005020000_custom_import_sealed_append_plans"
down_revision = "20261001110000_profile_initial_publication"
branch_labels = None
depends_on = None

_SOURCE_SHA256 = "7f4a10278637725c14aa476dfe1813afccec648bc8d855688debd0ddf58abb49"
_STATIC_SCHEMA = "__SEALED_APPEND_SCHEMA__"
_FUNCTION_NAME = "guard_custom_import_sealed_append"
_EXECUTE = re.compile(
    r"EXECUTE format\(\s*(?P<strings>'[^']*'(?:\s*\|\|\s*'[^']*')*)\s*,\s*"
    r"(?P<schemas>TG_TABLE_SCHEMA(?:\s*,\s*TG_TABLE_SCHEMA)*)\s*\)\s*"
    r"INTO\s+(?P<into>[a-z0-9_,\s]+?)\s+USING\s+(?P<using>[^;]+);"
)


def _require_source(condition: bool) -> None:
    if not condition:
        raise RuntimeError("sealed append migration source differs")


def _legacy():
    path = Path(__file__).with_name("20260917130000_custom_import_generation_finality.py")
    _require_source(hashlib.sha256(path.read_bytes()).hexdigest() == _SOURCE_SHA256)
    spec = importlib.util.spec_from_file_location("sealed_append_finality", path)
    _require_source(spec is not None and spec.loader is not None)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _schema() -> str:
    return _legacy()._schema()


def _static_query(match: re.Match[str]) -> str:
    """Rewrite only the frozen SELECT grammar, retaining every predicate and binding."""

    query = "".join(re.findall(r"'([^']*)'", match["strings"]))
    _require_source(query.startswith("SELECT ") and ";" not in query and "'" not in query)
    _require_source(query.count("%I") == match["schemas"].count("TG_TABLE_SCHEMA"))
    _require_source("%" not in query.replace("%I", ""))
    values = [value.strip() for value in match["using"].split(",")]
    _require_source(len(values) == len(set(values)))
    _require_source(
        all(re.fullmatch(r"NEW\.[a-z_]+|authority_(?:execution_id|fence|token_sha256)", value) for value in values)
    )
    targets = [target.strip() for target in match["into"].split(",")]
    _require_source(all(re.fullmatch(r"[a-z_][a-z0-9_]*", target) for target in targets))
    _require_source(set(map(int, re.findall(r"\$(\d+)", query))) == set(range(1, len(values) + 1)))
    prepared = re.sub(r"\$(\d+)", lambda parameter: "(" + values[int(parameter[1]) - 1] + ")", query)
    prepared = prepared.replace("%I", _STATIC_SCHEMA)
    into = " INTO " + ", ".join(targets)
    if prepared.startswith("SELECT EXISTS ("):
        replacement = prepared + into + ";"
    else:
        columns, separator, relation = prepared.partition(" FROM ")
        _require_source(bool(separator) and re.fullmatch(r"SELECT [a-z0-9_., ]+", columns) is not None)
        replacement = columns + into + separator + relation + ";"
    restored = prepared.replace(_STATIC_SCHEMA, "%I")
    for index, value in enumerate(values, 1):
        restored = restored.replace("(" + value + ")", "$" + str(index))
    _require_source(restored == query)
    return replacement


def _static_body(body: str) -> str:
    """Fail closed unless every dynamic query has a reversible static equivalent."""

    matches = list(_EXECUTE.finditer(body))
    _require_source(len(matches) == body.count("EXECUTE ") == 27)
    _require_source("FOUND" not in body and "GET DIAGNOSTICS" not in body and _STATIC_SCHEMA not in body)
    replacements = [_static_query(match) for match in matches]
    replacement_iterator = iter(replacements)
    changed = _EXECUTE.sub(lambda _: next(replacement_iterator), body)
    restored = changed
    for match, replacement in zip(matches, replacements, strict=True):
        _require_source(replacement in restored)
        restored = restored.replace(replacement, match[0], 1)
    _require_source(restored == body)
    _require_source("EXECUTE " not in changed and "$1" not in changed and "TG_TABLE_SCHEMA" not in changed)
    _require_source(changed.count("FOR UPDATE") == body.count("FOR UPDATE") == 3)
    _require_source(changed.count("clock_timestamp()") == body.count("clock_timestamp()") == 1)
    return changed


def _literal(value: str) -> str:
    return "E'" + value.replace("\\", "\\\\").replace("'", "''") + "'"


def _planned_body(legacy, body: str, schema: str) -> str:
    static = _static_body(body)
    declaration, statements = body.split("    BEGIN\n", 1)
    static_declaration, static_statements = static.split("    BEGIN\n", 1)
    suffix = "    END;\n"
    _require_source(
        declaration == static_declaration and statements.endswith(suffix) and static_statements.endswith(suffix)
    )
    static_statements = static_statements.removesuffix(suffix).replace(_STATIC_SCHEMA, legacy._quote(schema))
    return (
        declaration
        + "    BEGIN\n"
        + f"        IF TG_TABLE_SCHEMA = {_literal(schema)} THEN\n"
        + static_statements
        + "        ELSE\n"
        + statements.removesuffix(suffix)
        + "        END IF;\n"
        + suffix
    )


def _delimiter(body: str) -> str:
    delimiter = "$function$"
    while delimiter in body:
        delimiter = delimiter[:-1] + "_$"
    return delimiter


def _definition(qualified: str, body: str) -> str:
    delimiter = _delimiter(body)
    return f"""
    CREATE OR REPLACE FUNCTION {qualified}()
    RETURNS trigger
    LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path = pg_catalog
    AS {delimiter}
    {body}
    {delimiter}
    """


def _existing_function_guard(qualified: str) -> str:
    body = f"""BEGIN
        IF pg_catalog.to_regprocedure({_literal(qualified + "()")}) IS NULL THEN
            RAISE EXCEPTION 'custom_import_sealed_append_function_missing' USING ERRCODE = 'P0001';
        END IF;
    END;"""
    delimiter = _delimiter(body)
    return f"DO {delimiter}\n{body}\n{delimiter}"


def _replace_guard(*, planned: bool) -> None:
    legacy = _legacy()
    schema = _schema()
    body = legacy._with_read_committed_guard(legacy._APPEND_GUARD_FUNCTION_BODY)
    if planned:
        body = _planned_body(legacy, body, schema)
    qualified = legacy._qualified(schema, _FUNCTION_NAME)
    definition = _definition(qualified, body)
    op.execute(_existing_function_guard(qualified))
    op.execute(definition)


def upgrade() -> None:
    """Replace only the existing guard; its OID, owner, ACL and triggers survive."""

    _replace_guard(planned=True)


def downgrade() -> None:
    """Restore the original dynamic guard without dropping any retained object."""

    _replace_guard(planned=False)
