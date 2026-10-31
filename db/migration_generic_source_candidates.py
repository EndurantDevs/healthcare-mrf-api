# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Forward binding preparation and checked updates to existing statement guards."""

import sqlalchemy as sa

from db.plan_release_storage import storage_relation


def upgrade_generic_source_candidates(connection, schema):
    """Add only the nullable internal binding; keep live guards and ACLs intact."""
    relation = storage_relation(schema, "provider_directory_endpoint_dataset")
    connection.execute(sa.text(f"ALTER TABLE {relation} ADD COLUMN IF NOT EXISTS candidate_storage_json jsonb"))
    valid = connection.scalar(
        sa.text(
            "SELECT count(*)=1 AND bool_and(atttypid='jsonb'::regtype AND NOT attnotnull AND NOT atthasdef) "
            "FROM pg_attribute WHERE attrelid=to_regclass(:relation) AND attname='candidate_storage_json' "
            "AND attnum>0 AND NOT attisdropped"
        ),
        {"relation": relation},
    )
    if valid is not True:
        raise RuntimeError("provider_directory_generic_source_binding_column_changed")


def _canonical_copy_guard_bodies(schema, preparation_schema):
    """Extend the exact historical statement guard without changing its other rules."""
    from db.migration_provider_directory_dataset_candidates import candidate_functions, quote

    declaration = f"CREATE FUNCTION {quote(schema)}.guard_pd_dataset_candidate_parent() RETURNS trigger\n"
    functions = candidate_functions(schema)
    if functions.count(declaration) != 1:
        raise RuntimeError("provider_directory_canonical_copy_guard_template_changed")
    old = functions.split(declaration, 1)[1].split("AS $fn$", 1)[1].split("$fn$;", 1)[0]
    anchor = "    IF TG_OP<>'DELETE' AND EXISTS ("
    if old.count(anchor) != 1:
        raise RuntimeError("provider_directory_canonical_copy_guard_template_changed")
    preparation = storage_relation(preparation_schema, "preparation")
    closure = f"""    IF parent_name IN ('provider_directory_dataset_resource',
                       'provider_directory_dataset_insurance_plan',
                       'provider_directory_dataset_network_plan',
                       'provider_directory_dataset_affiliation_organization')
       AND EXISTS (SELECT FROM affected_rows changed
                   JOIN {preparation} preparation
                     ON preparation.handoff->'identity'->>'dataset_id'=changed.dataset_id
                   WHERE preparation.importer_id='provider-directory-fhir'
                     AND preparation.state='published'
                     AND preparation.publication->'native_publication'->>'contract'=
                         'generic-source-canonical-copy-v1') THEN
        RAISE EXCEPTION 'provider_directory_dataset_candidate_immutable' USING ERRCODE='55000';
    END IF;
"""
    # Both existing UPDATE statement triggers run this check: OLD closes moves
    # out of a protected dataset, and NEW closes moves into one.
    return old, old.replace(anchor, closure + anchor, 1)


def _cms_canonical_copy_guard_body(schema, preparation_schema):
    """Keep the exact v1 rules and also close a protected CMS terminal receipt."""
    generic = _canonical_copy_guard_bodies(schema, preparation_schema)[1]
    anchor = "preparation.publication->'native_publication'->>'contract'=\n                         'generic-source-canonical-copy-v1'"
    if generic.count(anchor) != 1:
        raise RuntimeError("provider_directory_canonical_copy_guard_template_changed")
    cms = """(preparation.handoff->>'contract'='generic-source-cms-detached-v1'
                          AND preparation.handoff->'identity'->'metadata'->'source_ids'='["cms-npd"]'::jsonb
                          AND preparation.publication->>'contract'='cms-npd-noncurrent-candidate-v2'
                          AND preparation.publication->>'state'='prepared'
                          AND preparation.publication->>'dataset_id'=changed.dataset_id)"""
    return generic.replace(anchor, "(" + anchor + " OR " + cms + ")", 1)


_COPY_GUARD_SNAPSHOT = sa.text("""
            SELECT p.prosrc AS body, pg_get_functiondef(p.oid) AS definition,
                   to_jsonb(p)-'prosrc' AS identity, obj_description(p.oid,'pg_proc') AS description
              FROM pg_proc p JOIN pg_language language ON language.oid=p.prolang
             WHERE p.oid=to_regprocedure(:signature) AND p.prokind='f' AND p.pronargs=0
               AND p.prorettype='trigger'::regtype AND language.lanname='plpgsql' AND p.prosecdef
               AND p.proconfig IN (ARRAY['search_path=pg_catalog']::text[],
                                   ARRAY['search_path=pg_catalog, pg_temp']::text[])
        """)


_COPY_GUARD_READER = sa.text("""
            SELECT c.relkind='r' AND NOT c.relrowsecurity AND NOT c.relforcerowsecurity
               AND NOT pg_has_role(CAST(:owner AS oid),c.relowner,'MEMBER')
               AND has_schema_privilege(:owner,c.relnamespace,'USAGE')
               AND NOT has_table_privilege(:owner,c.oid,'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER')
               AND NOT has_any_column_privilege(:owner,c.oid,'INSERT,UPDATE,REFERENCES,SELECT WITH GRANT OPTION')
               AND (SELECT count(*)=4 AND bool_and(has_column_privilege(:owner,c.oid,a.attnum,'SELECT'))
                    FROM pg_attribute a WHERE a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped
                      AND ((a.attname IN ('importer_id','state') AND a.atttypid='varchar'::regtype)
                           OR (a.attname IN ('handoff','publication') AND a.atttypid='jsonb'::regtype)))
               AND NOT EXISTS(SELECT FROM pg_attribute a WHERE a.attrelid=c.oid AND a.attnum>0
                   AND NOT a.attisdropped AND a.attname NOT IN ('importer_id','state','handoff','publication')
                   AND has_column_privilege(:owner,c.oid,a.attnum,'SELECT'))
              FROM pg_class c WHERE c.oid=to_regclass(:preparation)
        """)

_COPY_GUARD_TRIGGERS = sa.text("""
            SELECT c.relname,t.tgname,t.tgtype,t.tgenabled::text AS tgenabled,t.tgargs,t.tgoldtable,t.tgnewtable
              FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid
             WHERE c.oid=ANY(CAST(:roots AS regclass[])) AND NOT t.tgisinternal
               AND (t.tgfoid=:function OR t.tgtype & 1=1)
        """)


def _canonical_copy_guard_snapshot(connection, signature):
    return connection.execute(_COPY_GUARD_SNAPSHOT, {"signature": signature}).mappings().one_or_none()


def _canonical_copy_guard_input_parameters(schema, preparation_schema, identity):
    from db.migration_provider_directory_dataset_candidates import TABLES

    return (
        {"owner": int(identity["proowner"]), "preparation": storage_relation(preparation_schema, "preparation")},
        {"roots": [storage_relation(schema, root) for root in TABLES[:4]], "function": int(identity["oid"])},
    )


def _require_canonical_copy_guard_observations(reader_valid, triggers):
    from db.migration_provider_directory_dataset_candidates import TABLES

    if reader_valid is not True:
        raise RuntimeError("provider_directory_canonical_copy_preparation_reader_required")
    expected_triggers = {
        (
            root,
            "a_pd_candidate_" + operation + "_" + image.lower(),
            trigger_type,
            (root + "\0" + image + "\0").encode(),
            "affected_rows" if image == "OLD" else None,
            "affected_rows" if image == "NEW" else None,
        )
        for root in TABLES[:4]
        for operation, image, trigger_type in (
            ("insert", "NEW", 4),
            ("update", "NEW", 16),
            ("update", "OLD", 16),
            ("delete", "OLD", 8),
        )
    }
    if (
        len(triggers) != len(expected_triggers)
        or any(trigger["tgenabled"] not in ("O", "A") for trigger in triggers)
        or {
            tuple(trigger[key] for key in ("relname", "tgname", "tgtype", "tgargs", "tgoldtable", "tgnewtable"))
            for trigger in triggers
        }
        != expected_triggers
    ):
        raise RuntimeError("provider_directory_canonical_copy_statement_guards_required")


def _require_canonical_copy_guard_inputs(connection, schema, preparation_schema, identity):
    """Require existing statement coverage and a restricted protected-metadata reader."""
    reader, triggers = _canonical_copy_guard_input_parameters(schema, preparation_schema, identity)
    _require_canonical_copy_guard_observations(
        connection.scalar(_COPY_GUARD_READER, reader),
        connection.execute(_COPY_GUARD_TRIGGERS, triggers).mappings().all(),
    )


async def verify_generic_source_canonical_copy_guard(
    connection, schema, *, preparation_schema="hp_snapshot_retention", require_cms=False
):
    """Read the installed guard on the publisher's original transaction; never repair it."""
    from process.network_cms_registry_address_capture import NativeReceiptSession

    if not connection.is_in_transaction():
        raise RuntimeError("provider_directory_canonical_copy_guard_transaction_required")
    if await connection.fetchval("SELECT current_setting('session_replication_role')='origin'") is not True:
        raise RuntimeError("provider_directory_canonical_copy_statement_guards_required")
    session = NativeReceiptSession(connection)
    signature = storage_relation(schema, "guard_pd_dataset_candidate_parent") + "()"
    observed = (await session.execute(_COPY_GUARD_SNAPSHOT, {"signature": signature})).one_or_none()
    generic = _canonical_copy_guard_bodies(schema, preparation_schema)[1]
    cms = _cms_canonical_copy_guard_body(schema, preparation_schema)
    if observed is None or observed["body"] not in ((cms,) if require_cms else (generic, cms)):
        raise RuntimeError("provider_directory_canonical_copy_guard_changed")
    reader, triggers = _canonical_copy_guard_input_parameters(schema, preparation_schema, observed["identity"])
    _require_canonical_copy_guard_observations(
        await session.scalar(_COPY_GUARD_READER, reader), await session.execute(_COPY_GUARD_TRIGGERS, triggers)
    )


def upgrade_generic_source_canonical_copy_guard(
    connection, schema, *, preparation_schema="hp_snapshot_retention", include_cms=False
):
    """Update only the existing guard, using immutable protected publication authority.

    Enrollment must keep preparation writes outside untrusted runtime authority,
    including inherited global-write roles; the canonical header is not trusted.
    CMS requires a separately accepted consumer transition before include_cms.
    """
    if not connection.in_transaction():
        raise RuntimeError("provider_directory_canonical_copy_guard_transaction_required")
    old, new = _canonical_copy_guard_bodies(schema, preparation_schema)
    cms = _cms_canonical_copy_guard_body(schema, preparation_schema)
    signature = storage_relation(schema, "guard_pd_dataset_candidate_parent") + "()"
    before = _canonical_copy_guard_snapshot(connection, signature)
    if before is None or before["body"] not in ((new, cms) if include_cms else (old, new, cms)):
        raise RuntimeError("provider_directory_canonical_copy_guard_changed")
    _require_canonical_copy_guard_inputs(connection, schema, preparation_schema, before["identity"])
    if before["body"] == cms or (before["body"] == new and not include_cms):
        return
    old, new = (new, cms) if include_cms else (old, new)
    if before["definition"].count(old) != 1:
        raise RuntimeError("provider_directory_canonical_copy_guard_changed")
    with connection.begin_nested():
        connection.exec_driver_sql(before["definition"].replace(old, new, 1))
        after = _canonical_copy_guard_snapshot(connection, signature)
        if (
            after is None
            or after["body"] != new
            or after["identity"] != before["identity"]
            or after["description"] != before["description"]
        ):
            raise RuntimeError("provider_directory_canonical_copy_guard_identity_changed")


def downgrade_generic_source_candidates(connection, schema):
    # The binding is part of immutable publication custody and resume identity.
    """Refuse removal of durable immutable publication/resume custody."""
    raise RuntimeError("provider_directory_generic_source_forward_only")
