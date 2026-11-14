# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Render exact successor bodies for closed native rooted publication candidates."""

import hashlib

from db.plan_release_storage import storage_identifier, storage_relation
from process.provider_directory_rooted_graph_terminal import (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_AFFILIATION_FILTER_RECIPE_ID,
)

CONTRACT = "healthporta.provider-directory.rooted-candidate.v1"
SELECTOR = "healthporta.rooted_publication_candidate"
PREPARATION_HANDOFF_INDEX = "snapshot_native_preparation_handoff_hash_idx"
ORIGINAL_BEGIN = "/* rooted original validation begin */"
ORIGINAL_END = "/* rooted original validation end */"
ORIGINAL_SEMICOLON = "/* rooted original terminator */"
ROOTED = "provider_directory_rooted_graph_dataset_intrinsic_valid"
LINEAGE = "provider_directory_rooted_graph_official_lineage_current"
LEGACY = "provider_directory_uhc_flex_practitioner_dataset_valid"
GUARD = "guard_provider_directory_rooted_graph_dataset"
WORK_GUARD = "guard_provider_directory_rooted_graph_work"
SINGLE_ROOT_GUARD = "guard_provider_directory_rooted_graph_single_root_admission"
ORIGINAL_DIGESTS = {
    ROOTED: "9fed3d575d7c41c67e54ea2d28871e19e1c355917f62ba3c8e5a83312e90c770",
    LINEAGE: "5ea4b915d3d47e73f674a9151d722e743bd6d58a99474644626f7213cdaf9bdd",
    GUARD: "a271f4519c77780593f0d78bf9595b3a624fadec503f32119336c6369bfc03fc",
    LEGACY: "9bd066663a46371855b3e0e6382294a19daa498a58ab471d19b81c0d41cc8a1a",
    WORK_GUARD: "9ef517da1a928a4e28868c74cff416d5545f23616eed948dd5bad503ce026043",
    SINGLE_ROOT_GUARD: "2182b75267c57b2295928549b8f97020ddabdbe270171761b719f5c84f562801",
}
_SINGLE_ROOT_SELECT = (
    "header.acquisition_source_id, header.acquisition_endpoint_id,",
    "header.acquisition_source_id::text AS acquisition_source_id,\n"
    "                   header.acquisition_endpoint_id::text AS acquisition_endpoint_id,",
)
_MUTABLE = "'{status,is_current,validated_at,published_at,superseded_at}'::text[]"
_OWN_MUTABLE = "'{status,is_current,dataset_hash,created_at,validated_at,published_at,superseded_at}'::text[]"


def immutable_snapshot_sql(schema, candidate="candidate_dataset_id", *, own_relations=None):
    """Read fixed-size protected headers and authority rows without content scans."""

    relation = lambda name: storage_relation(schema, name)
    rooted = relation("provider_directory_rooted_graph_dataset")
    parent = relation("provider_directory_endpoint_dataset")
    legacy = relation("provider_directory_uhc_flex_practitioner_dataset")
    own_relations = own_relations or {}
    admission = own_relations.get(
        "provider_directory_rooted_graph_twin_admission", relation("provider_directory_rooted_graph_twin_admission")
    )
    attempt = own_relations.get(
        "provider_directory_rooted_graph_twin_attempt", relation("provider_directory_rooted_graph_twin_attempt")
    )
    acquisition = own_relations.get(
        "provider_directory_rooted_graph_acquisition", relation("provider_directory_rooted_graph_acquisition")
    )
    own_rooted = own_relations.get("provider_directory_rooted_graph_dataset", rooted)
    own_parent = own_relations.get("provider_directory_endpoint_dataset", parent)
    return f"""SELECT jsonb_build_object(
        'header',to_jsonb(h)-{_OWN_MUTABLE},
        'parent',(to_jsonb(p)-{_OWN_MUTABLE})-'candidate_storage_json',
        'admission',to_jsonb(a),'attempt',to_jsonb(t),'acquisition',to_jsonb(g),
        'root_header',CASE WHEN h.root_dataset_variant='uhc_flex_practitioner'
            THEN (SELECT to_jsonb(r)-{_MUTABLE} FROM {legacy} r WHERE r.dataset_id=h.root_dataset_id)
            ELSE (SELECT to_jsonb(r)-{_MUTABLE} FROM {rooted} r WHERE r.dataset_id=h.root_dataset_id) END,
        'root_parent',(SELECT (to_jsonb(r)-{_MUTABLE})-'candidate_storage_json'
            FROM {parent} r WHERE r.dataset_id=h.root_dataset_id),
        'sources',(SELECT jsonb_agg(to_jsonb(s) ORDER BY s.source_id)
            FROM {relation("provider_directory_source")} s
            WHERE s.source_id IN(h.source_id,h.root_source_id,h.practitioner_origin_source_id)),
        'endpoints',(SELECT jsonb_agg(to_jsonb(e) ORDER BY e.endpoint_id)
            FROM {relation("provider_directory_api_endpoint")} e
            WHERE e.endpoint_id IN(h.endpoint_id,h.root_endpoint_id,h.practitioner_origin_endpoint_id)))
        FROM {own_rooted} h JOIN {own_parent} p USING(dataset_id)
        JOIN {admission} a USING(admission_id)
        LEFT JOIN {attempt} t ON t.attempt_id=h.attempt_id
        JOIN {acquisition} g
          ON g.acquisition_id=h.publication_acquisition_id
        WHERE h.dataset_id={candidate}"""


def protected_handoff_sql(schema, candidate="candidate_dataset_id"):
    """Read the named dataset's ordinary logical handoff document."""
    parent = storage_relation(schema, "provider_directory_endpoint_dataset")
    return f"""SELECT candidate_storage_json AS handoff,
        CASE WHEN status='published' THEN 'published' ELSE 'validated' END AS state
        FROM {parent} WHERE dataset_id={candidate} AND candidate_storage_json->>'contract'='{CONTRACT}'"""


def native_receipt_sql(schema, candidate="candidate_dataset_id"):
    """Check the named publication's logical headers and recorded content totals."""
    rooted = storage_relation(schema, "provider_directory_rooted_graph_dataset")
    parent = storage_relation(schema, "provider_directory_endpoint_dataset")
    return f"""SELECT coalesce((SELECT count(*)=1 AND bool_and(
        b->>'contract'='{CONTRACT}' AND b->>'phase'='ready_for_freeze'
        AND b->'identity'->>'dataset_id'=h.dataset_id
        AND b->'identity'->'metadata'->>'query_filter_recipe_id'='{PROVIDER_DIRECTORY_ROOTED_GRAPH_AFFILIATION_FILTER_RECIPE_ID}'
        AND b->>'dataset_hash' ~ '^[0-9a-f]{{64}}$'
        AND h.resource_count=(SELECT sum(value::bigint) FROM jsonb_each_text(b->'counts'))
        AND h.resource_count=p.resource_count AND h.status=p.status AND h.is_current=p.is_current
        AND ((h.status='building' AND h.dataset_hash IS NULL AND p.dataset_hash IS NULL)
            OR (h.dataset_hash=b->>'dataset_hash' AND p.dataset_hash=h.dataset_hash))
        AND b->'snapshot'->'header'=to_jsonb(h)-{_OWN_MUTABLE}
        AND b->'snapshot'->'parent'=(to_jsonb(p)-{_OWN_MUTABLE})-'candidate_storage_json'
        AND {_lineage_metadata_sql(schema, candidate)}
        ) FROM {rooted} h JOIN {parent} p USING(dataset_id)
        CROSS JOIN LATERAL (SELECT p.candidate_storage_json AS b) document
        WHERE h.dataset_id={candidate}),false)"""


def original_query(body):
    """Recover only the exact retained validation query from the successor wrapper."""
    if body.count(ORIGINAL_BEGIN) != 1 or body.count(ORIGINAL_END) != 1:
        raise ValueError("rooted_publication_validator_upgrade_required")
    return body.split(ORIGINAL_BEGIN + "\n", 1)[1].split("\n" + ORIGINAL_END, 1)[0].replace(ORIGINAL_SEMICOLON, ";")


def _lineage_receipt_sql(schema):
    return (
        f"SELECT {storage_relation(schema, ROOTED)}(candidate_dataset_id)\n        AND {_lineage_metadata_sql(schema)}"
    )


def _lineage_metadata_sql(schema, candidate="candidate_dataset_id"):
    """Retain the protected lineage successor's exact attested official/cohort predicate."""
    parent = storage_relation(schema, "provider_directory_endpoint_dataset")
    cohort = storage_relation(schema, "provider_directory_uhc_flex_npi_cohort")
    return f"""EXISTS(SELECT FROM ({protected_handoff_sql(schema, candidate)}) proof JOIN {cohort} c
            ON c.cohort_id=proof.handoff->'lineage'->>'cohort_id'
            JOIN {parent} official ON official.dataset_id=c.official_dataset_id
            WHERE to_jsonb(c)=proof.handoff->'lineage'->'cohort'
              AND official.endpoint_id=c.official_endpoint_id
              AND official.acquisition_root_run_id=c.official_acquisition_root_run_id
              AND official.dataset_hash=c.official_dataset_hash
              AND official.status='published' AND official.is_current IS TRUE)"""


def _published_headers_sql(schema, candidate="candidate_dataset_id"):
    """Retain exactly the immutable ready function's published/current header predicate."""
    header = storage_relation(schema, "provider_directory_rooted_graph_dataset")
    parent = storage_relation(schema, "provider_directory_endpoint_dataset")
    return f"""EXISTS (
            SELECT 1 FROM {header} AS header
              JOIN {parent} AS endpoint_dataset
                ON endpoint_dataset.dataset_id = header.dataset_id
             WHERE header.dataset_id = {candidate}
               AND header.status = 'published'
               AND header.is_current IS TRUE
               AND endpoint_dataset.status = 'published'
               AND endpoint_dataset.is_current IS TRUE
        )"""


def _protected_guard_valid_sql(schema):
    """Evaluate the native proof once instead of planning historical SQL fallback trees."""
    return f"""WITH receipt(accepted) AS MATERIALIZED ({native_receipt_sql(schema, "NEW.dataset_id")})
        SELECT accepted
            AND CASE WHEN NEW.status IN ('validated','published')
                THEN {_lineage_metadata_sql(schema, "NEW.dataset_id")} ELSE TRUE END
            AND CASE WHEN NEW.status='published'
                THEN {_published_headers_sql(schema, "NEW.dataset_id")} ELSE TRUE END
        FROM receipt"""


def _legacy_receipt_sql(schema):
    rooted = storage_relation(schema, "provider_directory_rooted_graph_dataset")
    selector = f"nullif(current_setting('{SELECTOR}',true),'')"
    return f"""SELECT EXISTS(SELECT FROM {rooted} h WHERE h.dataset_id={selector}
        AND h.root_dataset_id=candidate_dataset_id
        AND h.root_dataset_variant='uhc_flex_practitioner'
        AND {storage_relation(schema, ROOTED)}(h.dataset_id::text))"""


def wrapped_body(name, schema, original):
    """Keep every historical validation expression in an exact fail-closed fallback."""
    parent = storage_relation(schema, "provider_directory_endpoint_dataset")
    if name == SINGLE_ROOT_GUARD:
        old, new = _SINGLE_ROOT_SELECT
        if original.count(old) != 1:
            raise ValueError("rooted_guard_predecessor_changed")
        return original.replace(old, new, 1)
    if name == WORK_GUARD:
        anchor = "        IF NEW.resource_count IS DISTINCT FROM actual_resource_count"
        if original.count(anchor) != 1:
            raise ValueError("rooted_guard_predecessor_changed")
        return original.replace(anchor, _work_guard_extension() + anchor)
    if name == GUARD:
        anchor = "        ELSIF TG_WHEN = 'AFTER' THEN"
        if original.count(anchor) != 1:
            raise ValueError("rooted_guard_predecessor_changed")
        extension = "\n" + _guard_extension(schema)
        return original.replace(anchor, anchor + extension)
    if name == LEGACY:
        condition = f"nullif(current_setting('{SELECTOR}',true),'') IS NOT NULL"
        fast = _legacy_receipt_sql(schema)
    else:
        condition = f"EXISTS(SELECT FROM {parent} WHERE dataset_id=candidate_dataset_id AND candidate_storage_json->>'contract'='{CONTRACT}')"
        fast = native_receipt_sql(schema) if name == ROOTED else _lineage_receipt_sql(schema)
    position = original.rfind(";")
    if position < 0:
        raise ValueError("rooted_validator_terminator_changed")
    query = original[:position] + ORIGINAL_SEMICOLON + original[position + 1 :]
    return f"\nSELECT CASE WHEN {condition} THEN ({fast}) ELSE (\n{ORIGINAL_BEGIN}\n{query}\n{ORIGINAL_END}\n) END;\n"


def _work_guard_extension():
    """Keep legacy terminals valid while independently authenticating filtered completion."""
    return f"""        IF NEW.kind='exact_reference_search' AND NEW.resource_type='OrganizationAffiliation'
            AND NEW.terminal_record_sha256 IS DISTINCT FROM expected_terminal THEN
            expected_terminal := pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
                'healthporta.provider-directory.rooted-graph-terminal-record.v2'||pg_catalog.chr(31)
                ||NEW.query_identity_sha256||pg_catalog.chr(31)||'completed'||pg_catalog.chr(31)
                ||expected_result||pg_catalog.chr(31)||actual_resource_count::text||pg_catalog.chr(31)
                ||actual_edge_count::text||pg_catalog.chr(31)||COALESCE(NEW.advertised_total::text,'')
                ||pg_catalog.chr(31)||NEW.terminal_page_count::text||pg_catalog.chr(31)||'true'||pg_catalog.chr(31)
                ||'{PROVIDER_DIRECTORY_ROOTED_GRAPH_AFFILIATION_FILTER_RECIPE_ID}','UTF8')),'hex');
        END IF;
"""


def _guard_extension(schema):
    parent = storage_relation(schema, "provider_directory_endpoint_dataset")
    header = storage_relation(schema, "provider_directory_rooted_graph_dataset")
    return f"""        /* rooted protected publication begin */
        IF EXISTS(SELECT FROM {parent} p WHERE p.dataset_id=NEW.dataset_id
            AND p.candidate_storage_json->>'contract'='{CONTRACT}') THEN
            IF NEW.status IN ('building','validated') AND EXISTS(SELECT FROM {header} final
                WHERE final.dataset_id=NEW.dataset_id AND final.ctid<>NEW.ctid
                  AND final.status='published' AND final.is_current IS TRUE)
                AND EXISTS(SELECT FROM ({protected_handoff_sql(schema, "NEW.dataset_id")}) proof
                    WHERE proof.state='published') THEN
                RETURN NULL;
            END IF;
            IF NEW.status='published' AND NOT EXISTS(SELECT FROM ({protected_handoff_sql(schema, "NEW.dataset_id")}) proof
                WHERE proof.state='published') THEN
                RAISE EXCEPTION 'provider_directory_rooted_graph_preparation_unpublished' USING ERRCODE='23514';
            END IF;
            IF ({_protected_guard_valid_sql(schema)}) IS DISTINCT FROM TRUE THEN
                RAISE EXCEPTION 'provider_directory_rooted_graph_dataset_invalid' USING ERRCODE='23514';
            END IF;
            RETURN NULL;
        END IF;
        /* rooted protected publication end */
"""


def upgrade_rooted_publication_freeze(connection, schema, *, application_roles=(), protected_owner_role=None):
    """Close canonical writers before retiring only isolated-input constraints and content hooks."""
    from sqlalchemy import text

    from process.provider_directory_rooted_graph_bulk import _data_module

    if not connection.in_transaction():
        raise RuntimeError("rooted_publication_transaction_required")
    rooted = _data_module("20260811020000_provider_directory_rooted_graph_acquisition.py")
    _restrict_rooted_application_writes(connection, schema, rooted, application_roles, protected_owner_role)
    _restore_rooted_validation_bodies(connection, schema)
    _retire_rooted_content_hooks(connection, schema, rooted)
    statement = _logical_current_guard_statement(schema, rooted)
    signature = storage_relation(schema, rooted._LOGICAL_CURRENT_GUARD) + "()"
    if (
        connection.execute(
            text("SELECT to_regprocedure(:signature) IS NOT NULL"), {"signature": signature}
        ).scalar_one()
        is not True
    ):
        raise RuntimeError("rooted_logical_current_guard_missing")
    connection.exec_driver_sql(statement)
    header = storage_relation(schema, rooted._ROOTED_DATASET)
    for constraint in (
        "pd_rooted_graph_dataset_admission_fkey",
        "pd_rooted_graph_dataset_attempt_fkey",
        "pd_rooted_graph_dataset_publication_fkey",
    ):
        connection.exec_driver_sql(f"ALTER TABLE {header} DROP CONSTRAINT IF EXISTS {storage_identifier(constraint)}")


def _restore_rooted_validation_bodies(connection, schema):
    """Retire the existing optional wrapper without changing its original data validator."""
    from sqlalchemy import text

    for name in (ROOTED, LINEAGE, LEGACY):
        definition_by_field = (
            connection.execute(
                text(
                    "SELECT pg_get_functiondef(to_regprocedure(:signature)) AS definition, "
                    "prosrc AS body FROM pg_proc WHERE oid=to_regprocedure(:signature)"
                ),
                {"signature": storage_relation(schema, name) + "(text)"},
            )
            .mappings()
            .one()
        )
        if ORIGINAL_BEGIN in definition_by_field["body"]:
            connection.exec_driver_sql(
                definition_by_field["definition"].replace(
                    definition_by_field["body"], original_query(definition_by_field["body"]), 1
                )
            )


def _retire_rooted_content_hooks(connection, schema, rooted):
    """Remove the existing content hooks only after the ordinary canonical writers close."""
    triggers = [
        (rooted._ROOTED_DATASET, rooted._ROOTED_DATASET + suffix) for suffix in ("_row_guard", "_truncate_guard")
    ]
    triggers.append((rooted._ROOTED_DATASET, "pd_rooted_graph_dataset_valid_guard"))
    triggers.extend(
        (
            (rooted._DATASET, "pd_uhc_flex_dataset_parent_guard"),
            (rooted._LEGACY_DATASET, "pd_uhc_flex_dataset_valid_guard"),
        )
    )
    for relation, names in rooted._DATASET_DEPENDENCY_TRIGGERS.items():
        triggers.extend((relation, name) for name in names)
    triggers.extend(
        (relation, "pd_rooted_graph_" + relation + "_dependency_guard")
        for relation in (rooted._DATASET, rooted._SOURCE, rooted._ENDPOINT)
    )
    for relation, trigger in triggers:
        # PostgreSQL truncated these historical ASCII trigger names when creating them.
        connection.exec_driver_sql(
            f"DROP TRIGGER IF EXISTS {storage_identifier(trigger[:63])} ON {storage_relation(schema, relation)}"
        )


def _logical_current_guard_statement(schema, rooted):
    """Keep the existing logical current guard independent of retired Rooted content hooks."""
    from process.provider_directory_rooted_graph_bulk import _data_module

    scoped = _data_module("20260904163000_provider_directory_exact_guard_scope.py")
    statement = scoped._logical_current_guard_sql(schema, scoped=True)
    start = statement.index(
        "           OR EXISTS (\n                SELECT 1 FROM " + storage_relation(schema, rooted._ROOTED_DATASET)
    )
    end = statement.index("           ) THEN", start)
    statement = statement[:start] + statement[end + len("           )") :]
    legacy_call = storage_relation(schema, LEGACY) + "(dataset.dataset_id)"
    statement = statement.replace(legacy_call, storage_relation(schema, LEGACY) + "(dataset.dataset_id::text)")
    return statement


_ROOTED_WRITE_FAMILY_SQL = """WITH RECURSIVE family(relation) AS (
 SELECT to_regclass(name) FROM unnest(CAST(:roots AS text[])) AS names(name)
 UNION SELECT i.inhrelid FROM pg_inherits i JOIN family f ON i.inhparent=f.relation)
 SELECT n.nspname AS schema_name,c.relname AS relation_name,
        ARRAY(SELECT a.attname FROM pg_attribute a WHERE a.attrelid=c.oid AND a.attnum>0
              AND NOT a.attisdropped ORDER BY a.attnum) AS columns
 FROM family f JOIN pg_class c ON c.oid=f.relation JOIN pg_namespace n ON n.oid=c.relnamespace
 WHERE c.relkind IN ('r','p') ORDER BY n.nspname,c.relname"""


_ROOTED_MUTATING_HELPERS = (
    "prepare_provider_directory_rooted_graph_storage",
    "attach_provider_directory_rooted_graph_storage",
    "finish_provider_directory_rooted_graph_initial_storage",
    "finish_provider_directory_rooted_graph_storage",
    "prepare_provider_directory_rooted_graph_work_stage",
    "initialize_provider_directory_rooted_graph_work",
    "admit_provider_directory_rooted_graph_work",
    "prepare_provider_directory_rooted_graph_witness_stage",
    "admit_provider_directory_rooted_graph_witnesses",
)


def _restrict_rooted_application_writes(connection, schema, rooted, application_roles, protected_owner_role):
    """Close direct parent/leaf and column writes using only the existing configured role names."""
    from sqlalchemy import text

    if type(application_roles) not in {tuple, list} or not application_roles or not protected_owner_role:
        raise RuntimeError("rooted_publication_roles_required")
    readers = ",".join(storage_identifier(role) for role in sorted(set(application_roles)))
    actors = "PUBLIC," + readers
    protected = storage_identifier(protected_owner_role)
    if protected_owner_role in application_roles:
        raise RuntimeError("rooted_publication_owner_is_application")
    roots = (
        rooted._ACQUISITION,
        rooted._WORK,
        rooted._RESOURCE,
        rooted._EDGE,
        rooted._TWIN_ATTEMPT,
        rooted._TWIN_ADMISSION,
        rooted._ROOTED_PROVENANCE,
        rooted._ROOTED_DATASET,
        *(root + "_legacy" for root in (rooted._WORK, rooted._RESOURCE, rooted._EDGE)),
    )
    root_names = [storage_relation(schema, name) for name in roots]
    connection.exec_driver_sql("SET LOCAL statement_timeout='3s'")
    connection.exec_driver_sql(
        "LOCK TABLE " + ",".join("ONLY " + name for name in root_names) + " IN ACCESS EXCLUSIVE MODE NOWAIT"
    )
    relations = list(connection.execute(text(_ROOTED_WRITE_FAMILY_SQL), {"roots": root_names}).mappings())
    if not {(schema, root) for root in roots} <= {
        (relation_by_field["schema_name"], relation_by_field["relation_name"]) for relation_by_field in relations
    }:
        raise RuntimeError("rooted_publication_roots_missing")
    names = [
        storage_relation(relation_by_field["schema_name"], relation_by_field["relation_name"])
        for relation_by_field in relations
    ]
    connection.exec_driver_sql(
        "LOCK TABLE " + ",".join("ONLY " + name for name in names) + " IN ACCESS EXCLUSIVE MODE NOWAIT"
    )
    _close_rooted_relation_writes(connection, relations, names, protected, actors, readers)
    helper_definitions = connection.execute(
        text(
            "SELECT quote_ident(n.nspname)||'.'||quote_ident(p.proname)||'('||"
            "pg_get_function_identity_arguments(p.oid)||')' AS signature FROM pg_proc p "
            "JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname=:schema AND p.proname::text=ANY(CAST(:names AS text[]))"
        ),
        {"schema": schema, "names": list(_ROOTED_MUTATING_HELPERS)},
    ).mappings()
    for definition in helper_definitions:
        connection.exec_driver_sql(f"REVOKE EXECUTE ON FUNCTION {definition['signature']} FROM {actors} CASCADE")


def _close_rooted_relation_writes(connection, relations, names, protected, actors, readers):
    """Apply owner and native table/column DML closure to the already locked named family."""
    for relation, name in zip(relations, names, strict=True):
        columns = ",".join(storage_identifier(column) for column in relation["columns"])
        connection.exec_driver_sql(f"ALTER TABLE {name} OWNER TO {protected}")
        connection.exec_driver_sql(
            f"REVOKE INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON TABLE {name} FROM {actors} CASCADE"
        )
        connection.exec_driver_sql(
            f"REVOKE INSERT({columns}),UPDATE({columns}),REFERENCES({columns}) ON TABLE {name} FROM {actors} CASCADE"
        )
        connection.exec_driver_sql(f"GRANT SELECT ON TABLE {name} TO {readers}")


def validation_body_sql(name, schema):
    """Render application-owned historical data predicates without catalog attestation."""
    import importlib.util
    import re
    from pathlib import Path

    versions = Path(__file__).resolve().parents[1] / "alembic" / "versions"
    filename, renderer = (
        ("20260909120000_fhir_request_failure_budget.py", "_intrinsic_valid_sql")
        if name == ROOTED
        else ("20260830090000_uhc_flex_retry_exhaustion.py", "_publication_valid_sql")
    )
    specification = importlib.util.spec_from_file_location(filename.removesuffix(".py"), versions / filename)
    module = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(module)
    sql = getattr(module, renderer)(schema, **({"partial": True} if name == LEGACY else {}))
    match = re.search(r"AS\s+\$(\w*)\$(.*?)\$\1\$", sql, re.S)
    if match is None:
        raise RuntimeError("rooted_validation_sql_invalid")
    body = match.group(2)
    for repair_filename in (
        "20261016040000_source_validator_argument_types.py",
        "20261016050000_acquisition_validator_argument_types.py",
    ):
        repair_specification = importlib.util.spec_from_file_location(
            repair_filename.removesuffix(".py"), versions / repair_filename
        )
        repair = importlib.util.module_from_spec(repair_specification)
        repair_specification.loader.exec_module(repair)
        for caller, old, new, count in repair._calls(schema):
            if caller == name:
                if body.count(old) != count:
                    raise RuntimeError("rooted_validation_sql_invalid")
                body = body.replace(old, new)
    return body


def body_digest(body, schema):
    """Normalize only the selected schema while retaining every original function byte."""
    canonical = body.replace(storage_identifier(schema), '"__schema__"').replace("'" + schema + "'", "'__schema__'")
    return hashlib.sha256(canonical.encode()).hexdigest()


def is_approved_successor(name, schema, body, predecessor_digest):
    """Accept only a rendered successor of the caller's exact existing function body."""
    if name not in ORIGINAL_DIGESTS or predecessor_digest != ORIGINAL_DIGESTS[name]:
        return False
    if name == SINGLE_ROOT_GUARD:
        old, new = _SINGLE_ROOT_SELECT
        if body.count(new) != 1:
            return False
        original = body.replace(new, old, 1)
        return body_digest(original, schema) == predecessor_digest and wrapped_body(name, schema, original) == body
    if name in (GUARD, WORK_GUARD):
        extension = "\n" + _guard_extension(schema) if name == GUARD else _work_guard_extension()
        if body.count(extension) != 1:
            return False
        original = body.replace(extension, "", 1)
        return body_digest(original, schema) == predecessor_digest and wrapped_body(name, schema, original) == body
    try:
        original = original_query(body)
    except ValueError:
        return False
    return body_digest(original, schema) == predecessor_digest and wrapped_body(name, schema, original) == body
