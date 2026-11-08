# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bind the existing PTG JSONB helper's polymorphic native call to pg_catalog.

This installs nothing automatically. Its original Admin caller authenticates
the protected identity, qualifies all consumers and retains the outer deadline.
"""

import hashlib
from functools import lru_cache

import sqlalchemy as sa

from db.migration_cms_snapshot_allocator import _INSTALLER_ADMIN_SQL, _identity
from db.plan_release_storage import storage_identifier, storage_relation

FUNCTION = "ptg_wave_canonical_json_ascii_v1"
ORIGINAL_SHA256 = "f6170ea1c34b5db60a764d94e706bf8c2eda7be2effe10f6a61b572aa3180f1b"
_SNAPSHOT_SQL = sa.text("""SELECT p.prosrc AS body,pg_catalog.pg_get_functiondef(p.oid) AS definition,
 pg_catalog.to_jsonb(p)-'prosrc' AS identity,pg_catalog.obj_description(p.oid,'pg_proc') AS description,
 l.lanname AS language,pg_catalog.quote_ident(n.nspname) AS namespace_identifier,
 COALESCE((SELECT pg_catalog.jsonb_agg(pg_catalog.to_jsonb(t) ORDER BY t.oid)
 FROM pg_catalog.pg_trigger t WHERE t.tgfoid=p.oid),'[]'::pg_catalog.jsonb) AS hooks
 FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_language l ON l.oid=p.prolang
 JOIN pg_catalog.pg_namespace n ON n.oid=p.pronamespace
 WHERE p.oid=pg_catalog.to_regprocedure(:signature) AND p.prokind='f'""")


def _require(condition, code):
    if not condition:
        raise RuntimeError("cms_ptg_json_resolution_" + code)


@lru_cache(maxsize=8)
def ptg_json_bodies(schema):
    """Render the source-fixed predecessor and only its qualified to_jsonb call."""
    storage_identifier(schema)
    ascii_text = storage_relation(schema, "ptg_wave_json_ascii_text_v1")
    canonical_json = storage_relation(schema, FUNCTION)
    # Exact 20260810110000 source body; the 20261016050000 contract retains it.
    original = f"""
        DECLARE
            canonical_value text;
        BEGIN
            CASE jsonb_typeof(payload)
                WHEN 'object' THEN
                    SELECT '{{' || COALESCE(
                        string_agg(
                            {ascii_text}(to_jsonb(entry.key)::text)
                            || ':' || {canonical_json}(entry.value),
                            ',' ORDER BY entry.key COLLATE "C"
                        ),
                        ''
                    ) || '}}'
                      INTO canonical_value
                      FROM jsonb_each(payload) AS entry;
                    RETURN canonical_value;
                WHEN 'array' THEN
                    SELECT '[' || COALESCE(
                        string_agg(
                            {canonical_json}(entry.value),
                            ',' ORDER BY entry.ordinality
                        ),
                        ''
                    ) || ']'
                      INTO canonical_value
                      FROM jsonb_array_elements(payload) WITH ORDINALITY
                           AS entry(value, ordinality);
                    RETURN canonical_value;
                WHEN 'string' THEN
                    RETURN {ascii_text}(payload::text);
                ELSE
                    RETURN payload::text;
            END CASE;
        END;
        """
    normalized = original.replace('"' + schema + '"', '"__schema__"').replace("'" + schema + "'", "'__schema__'")
    _require(hashlib.sha256(normalized.encode()).hexdigest() == ORIGINAL_SHA256, "predecessor_changed")
    call = "to_jsonb(entry.key)"
    _require(original.count(call) == 1, "callsite_changed")
    return original, original.replace(call, "pg_catalog." + call, 1)


def _snapshot(connection, schema):
    return (
        connection.execute(_SNAPSHOT_SQL, {"signature": storage_relation(schema, FUNCTION) + "(pg_catalog.jsonb)"})
        .mappings()
        .one_or_none()
    )


def _require_definition(observed, schema):
    from process import provider_directory_source_callable_custody as source

    definition_by_field = {**observed["identity"], "prosrc": observed["body"]}
    source._require_definition(
        {"definition": definition_by_field, "language": observed["language"]},
        source._CONTRACTS[FUNCTION + "(jsonb)"],
        schema,
        int(definition_by_field["pronamespace"]),
        source._TYPE_OIDS,
        observed["namespace_identifier"],
    )


def upgrade_cms_ptg_json_resolution(connection, schema, *, expected):
    """Change one existing body in a savepoint; preserve identity and caller commit."""
    _require(connection.in_transaction(), "transaction_required")
    _require(
        connection.exec_driver_sql("SHOW search_path").scalar_one() in {"pg_catalog", "pg_catalog, pg_temp"},
        "search_path",
    )
    _require(connection.execute(_INSTALLER_ADMIN_SQL).scalar_one_or_none() is True, "original_admin_required")
    pins = dict(expected) if type(expected) is dict else expected
    original, hardened = ptg_json_bodies(schema)
    before = _snapshot(connection, schema)
    _identity(before, pins, (original, hardened))
    _require_definition(before, schema)
    if before["body"] == hardened:
        return
    _require(before["definition"].count(original) == 1, "definition_changed")
    with connection.begin_nested():
        connection.exec_driver_sql(before["definition"].replace(original, hardened, 1))
        after = _snapshot(connection, schema)
        _identity(after, pins, (hardened,))
        _require_definition(after, schema)
        _require(
            all(after[field] == before[field] for field in ("identity", "description", "hooks")),
            "identity_changed",
        )
