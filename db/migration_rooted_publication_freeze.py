# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Render exact successor bodies for closed native rooted publication candidates."""

import hashlib

from db.plan_release_storage import storage_identifier, storage_relation
from process.plan_release_storage_catalog import CATALOG_SQL
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
    from process.provider_directory_rooted_graph_foreign_guards import parent_foreign_guard_snapshot_sql
    from process.provider_directory_rooted_graph_header_checks import header_check_snapshot_sql

    relation = lambda name: storage_relation(schema, name)
    rooted = relation("provider_directory_rooted_graph_dataset")
    parent = relation("provider_directory_endpoint_dataset")
    legacy = relation("provider_directory_uhc_flex_practitioner_dataset")
    own_relations = own_relations or {}
    own_rooted = own_relations.get("provider_directory_rooted_graph_dataset", rooted)
    own_parent = own_relations.get("provider_directory_endpoint_dataset", parent)
    return f"""SELECT jsonb_build_object(
        'header',to_jsonb(h)-{_OWN_MUTABLE},
        'parent',(to_jsonb(p)-{_OWN_MUTABLE})-'candidate_storage_json',
        'admission',to_jsonb(a),'attempt',to_jsonb(t),'acquisition',to_jsonb(g),
        'header_check',({header_check_snapshot_sql(schema)}),
        'foreign_guards',({parent_foreign_guard_snapshot_sql(schema)}),
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
        JOIN {relation("provider_directory_rooted_graph_twin_admission")} a USING(admission_id)
        LEFT JOIN {relation("provider_directory_rooted_graph_twin_attempt")} t ON t.attempt_id=h.attempt_id
        JOIN {relation("provider_directory_rooted_graph_acquisition")} g
          ON g.acquisition_id=h.publication_acquisition_id
        WHERE h.dataset_id={candidate}"""


def catalog_rows_sql(names, owner, readers):
    """Reuse the complete existing native layout and direct-writer closure inventory."""
    return CATALOG_SQL.replace("$1", names).replace("$2", owner).replace("$3", readers)


def protected_handoff_sql(schema, candidate="candidate_dataset_id"):
    """Read only the existing protected consumer columns; ordinary JSON is a locator."""
    parent = storage_relation(schema, "provider_directory_endpoint_dataset")
    return f"""SELECT preparation.handoff,preparation.publication,preparation.state
        FROM hp_snapshot_retention.preparation preparation
        JOIN {parent} parent ON parent.dataset_id={candidate}
        WHERE preparation.importer_id='provider-directory-fhir'
          AND preparation.state IN ('validated','published')
          AND preparation.handoff->>'contract'='{CONTRACT}'
          AND preparation.handoff=parent.candidate_storage_json
          AND preparation.handoff->>'preparation_id'=parent.candidate_storage_json->>'preparation_id'
          AND preparation.handoff->'identity'->>'dataset_id'=parent.dataset_id"""


def frozen_table_custody_sql(schema):
    """Authenticate only the protected seven-table group and its exact native attachments."""
    schema_literal = "'" + schema.replace("'", "''") + "'"
    from process.provider_directory_rooted_graph_foreign_guards import inherited_foreign_guards_valid_sql

    inherited = inherited_foreign_guards_valid_sql("c.oid", "entry.key", "b->'snapshot'->'foreign_guards'")
    return f"""NOT EXISTS(SELECT FROM jsonb_each(b->'tables') entry
            LEFT JOIN pg_class c ON c.oid=(entry.value->>'oid')::oid
            LEFT JOIN pg_namespace n ON n.oid=c.relnamespace
            WHERE c.oid IS NULL OR c.relname<>entry.value->>'name'
               OR n.nspname<>b->'storage'->>'storage_schema'
               OR c.relfilenode::bigint<>(entry.value->>'relfilenode')::bigint
               OR c.relkind<>'r' OR c.relpersistence<>'p' OR c.relrowsecurity OR c.relforcerowsecurity
               OR c.relowner<>(SELECT nspowner FROM pg_namespace WHERE nspname='hp_snapshot_retention')
               OR NOT ({inherited})
               OR EXISTS(SELECT FROM pg_rewrite WHERE ev_class=c.oid)
               OR EXISTS(SELECT FROM aclexplode(coalesce(c.relacl,acldefault('r',c.relowner))) acl
                    WHERE acl.grantee<>c.relowner)
               OR EXISTS(SELECT FROM pg_attribute a,LATERAL aclexplode(a.attacl) acl
                    WHERE a.attrelid=c.oid AND acl.grantee<>c.relowner)
               OR EXISTS(SELECT FROM pg_index WHERE indrelid=c.oid AND NOT(indisvalid AND indisready AND indislive))
               OR EXISTS(SELECT FROM pg_constraint WHERE conrelid=c.oid AND (contype NOT IN('p','u','n','c') OR NOT convalidated))
               OR CASE WHEN entry.key IN('provider_directory_endpoint_dataset','provider_directory_rooted_graph_dataset')
                    THEN c.relispartition OR EXISTS(SELECT FROM pg_inherits WHERE inhrelid=c.oid)
                    ELSE (NOT c.relispartition OR
                        ARRAY(SELECT inhparent FROM pg_inherits WHERE inhrelid=c.oid ORDER BY inhparent)
                        <>ARRAY[to_regclass(format('%I.%I',{schema_literal},entry.key))::oid]) END
)"""


def native_receipt_sql(schema, candidate="candidate_dataset_id"):
    """Authenticate fixed headers and frozen physical generations from protected handoff."""
    rooted = storage_relation(schema, "provider_directory_rooted_graph_dataset")
    parent = storage_relation(schema, "provider_directory_endpoint_dataset")
    proof = protected_handoff_sql(schema, candidate)
    snapshot = immutable_snapshot_sql(schema, candidate)
    from process.provider_directory_rooted_graph_input_fence import root_input_fence_sql

    roots = "(b->'predecessor_proof'->'input_fence')->'roots'"
    legacy_fence = root_input_fence_sql(schema, "selected.root_dataset_id", roots)
    rooted_fence = root_input_fence_sql(schema, "selected.root_dataset_id", roots, variant="rooted_combined")
    frozen_catalog = catalog_rows_sql(
        "ARRAY(SELECT format('%I.%I',b->'storage'->>'storage_schema',entry.value->>'name') FROM jsonb_each(b->'tables') entry)",
        "(SELECT nspowner FROM pg_namespace WHERE nspname='hp_snapshot_retention')",
        "ARRAY[]::oid[]",
    )
    return f"""WITH proof AS MATERIALIZED ({proof}), selected AS MATERIALIZED (
        SELECT h.*,p.dataset_hash AS parent_hash,p.status AS parent_status,proof.handoff AS b,proof.publication AS publication,proof.state AS preparation_state
        FROM {rooted} h JOIN {parent} p USING(dataset_id) CROSS JOIN proof
        WHERE h.dataset_id={candidate}
    ) SELECT coalesce((SELECT count(*)=1 AND bool_and(
        ({preparation_handoff_index_sql()})
        AND b->>'contract'='{CONTRACT}' AND b->>'phase'='ready_for_freeze'
        AND b->'identity'->>'dataset_id'=dataset_id
        AND b->'identity'->'metadata'->>'query_filter_recipe_id'='{PROVIDER_DIRECTORY_ROOTED_GRAPH_AFFILIATION_FILTER_RECIPE_ID}'
        AND b->>'dataset_hash' ~ '^[0-9a-f]{{64}}$'
        AND octet_length(b::text)<=262144
        AND b->'snapshot'=({snapshot})
        AND ((status='building' AND dataset_hash IS NULL AND parent_hash IS NULL)
            OR (dataset_hash=b->>'dataset_hash' AND parent_hash=dataset_hash))
        AND status=parent_status
        AND (CASE WHEN preparation_state='published' THEN publication->'rooted_input_fence'
            ELSE b->'predecessor_proof'->'input_fence' END)=
            (CASE WHEN root_dataset_variant='rooted_combined' THEN ({rooted_fence}) ELSE ({legacy_fence}) END)
        AND (SELECT jsonb_agg(to_jsonb(actual) ORDER BY actual.oid) FROM (
            SELECT catalog.oid,catalog.nspname,catalog.relname,catalog.relkind,catalog.relpersistence,
                catalog.relrowsecurity,catalog.relforcerowsecurity,catalog.partition_key,
                catalog.columns,catalog.constraints,catalog.indexes,catalog.native_checks,
                catalog.direct_acl_closed,catalog.readers_closed
            FROM ({frozen_catalog}) catalog) actual)=
            (SELECT jsonb_agg(entry.value-ARRAY['relowner','parents','relispartition','bound','inert']::text[]
                ORDER BY (entry.value->>'oid')::bigint) FROM jsonb_array_elements(b->'catalog') entry)
        AND (SELECT count(*) FROM jsonb_each(b->'tables'))=7
        AND {frozen_table_custody_sql(schema)}
        ) FROM selected),false)"""


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


def upgrade_rooted_publication_freeze(connection, schema):
    """Replace exact existing bodies and index their existing protected handoff read."""
    from sqlalchemy import text

    from db.migration_cms_snapshot_allocator import _INSTALLER_ADMIN_SQL

    if not connection.in_transaction() or connection.execute(_INSTALLER_ADMIN_SQL).scalar_one_or_none() is not True:
        raise RuntimeError("rooted_publication_original_admin_required")
    prepared = connection.execute(text("SELECT to_regclass('hp_snapshot_retention.preparation')")).scalar_one_or_none()
    # Acquisition completes before protected publication storage is prepared.
    digests = (
        ORIGINAL_DIGESTS
        if prepared is not None
        else {name: ORIGINAL_DIGESTS[name] for name in (WORK_GUARD, SINGLE_ROOT_GUARD)}
    )
    functions_by_name = {}
    for name, digest in digests.items():
        signature = storage_relation(schema, name) + (
            "()" if name in (GUARD, WORK_GUARD, SINGLE_ROOT_GUARD) else "(text)"
        )
        function_record = _function_snapshot(connection, signature)
        if function_record is None or not _is_native_definition(function_record, name):
            raise RuntimeError("rooted_publication_function_identity_changed")
        body = function_record["body"]
        if body_digest(body, schema) != digest and not is_approved_successor(name, schema, body, digest):
            raise RuntimeError("rooted_publication_function_body_changed")
        if function_record["definition"].count(body) != 1:
            raise RuntimeError("rooted_publication_definition_changed")
        functions_by_name[name] = (signature, function_record)
    owners = {function_record["identity"]["proowner"] for _, function_record in functions_by_name.values()}
    if len(owners) != 1:
        raise RuntimeError("rooted_publication_function_owner_changed")
    if prepared is not None:
        _require_guard_implications(connection, schema, next(iter(owners)))
    auxiliary = _function_auxiliary(connection, functions_by_name)
    for name, (signature, function_record) in functions_by_name.items():
        if body_digest(function_record["body"], schema) == ORIGINAL_DIGESTS[name]:
            successor = wrapped_body(name, schema, function_record["body"])
            connection.exec_driver_sql(function_record["definition"].replace(function_record["body"], successor, 1))
        after = _function_snapshot(connection, signature)
        if (
            after["identity"] != function_record["identity"]
            or after["description"] != function_record["description"]
            or not is_approved_successor(name, schema, after["body"], ORIGINAL_DIGESTS[name])
        ):
            raise RuntimeError("rooted_publication_function_replacement_changed")
    if _function_auxiliary(connection, functions_by_name) != auxiliary:
        raise RuntimeError("rooted_publication_function_custody_changed")
    if prepared is not None:
        _ensure_preparation_handoff_index(connection)


def _require_guard_implications(connection, schema, owner):
    """Authenticate ready implies valid implies intrinsic before removing repeated evaluations."""
    from process.provider_directory_source_callable_custody import _CONTRACTS

    for name in ("provider_directory_rooted_graph_dataset_valid", "provider_directory_rooted_graph_dataset_ready"):
        signature = name + "(text)"
        function_record = _function_snapshot(connection, storage_relation(schema, name) + "(text)")
        if (
            function_record is None
            or not _is_native_definition(function_record, name)
            or function_record["identity"]["proowner"] != owner
            or body_digest(function_record["body"], schema) != _CONTRACTS[signature][-1]
        ):
            raise RuntimeError("rooted_publication_guard_implication_changed")


def _is_native_definition(row, name):
    identity = row["identity"]
    is_trigger = name in (GUARD, WORK_GUARD, SINGLE_ROOT_GUARD)
    return (
        row["language"] == ("plpgsql" if is_trigger else "sql")
        and identity["prokind"] == "f"
        and identity["prosecdef"] is True
        and identity["proconfig"] == ["search_path=pg_catalog"]
        and identity["pronargs"] == (0 if is_trigger else 1)
        and identity["proargtypes"] == ([] if is_trigger else ["25"])
        and identity["proargnames"] == (None if is_trigger else ["candidate_dataset_id"])
        and identity["proallargtypes"] is None
        and identity["proargmodes"] is None
        and identity["proargdefaults"] is None
        and str(identity["provariadic"]) == "0"
        and identity["prosqlbody"] is None
        and str(identity["prorettype"]) == ("2279" if is_trigger else "16")
        and identity["proisstrict"] is False
        and identity["proleakproof"] is False
        and identity["proretset"] is False
        and identity["proparallel"] == "u"
        and identity["provolatile"] == ("v" if is_trigger else "s")
        and identity["procost"] == 100
        and identity["prorows"] == 0
        and identity["pronargdefaults"] == 0
        and str(identity["prosupport"]) in {"0", "-"}
    )


def _function_snapshot(connection, signature):
    from sqlalchemy import text

    return (
        connection.execute(
            text("""SELECT p.prosrc AS body,pg_get_functiondef(p.oid) AS definition,
        to_jsonb(p)-'prosrc' AS identity,obj_description(p.oid,'pg_proc') AS description,language.lanname AS language
        FROM pg_proc p JOIN pg_language language ON language.oid=p.prolang
        WHERE p.oid=to_regprocedure(:signature)"""),
            {"signature": signature},
        )
        .mappings()
        .one_or_none()
    )


def _function_auxiliary(connection, before):
    from sqlalchemy import text

    oids = [int(row["identity"]["oid"]) for _, row in before.values()]
    return connection.execute(
        text("""SELECT jsonb_build_object(
        'hooks',(SELECT jsonb_agg(to_jsonb(t) ORDER BY t.oid) FROM pg_trigger t WHERE tgfoid=ANY(CAST(:oids AS oid[]))),
        'dependencies',(SELECT jsonb_agg(to_jsonb(d) ORDER BY classid,objid,objsubid,refclassid,refobjid,refobjsubid,deptype)
            FROM pg_depend d WHERE (classid='pg_proc'::regclass AND objid=ANY(CAST(:oids AS oid[])))
                OR (refclassid='pg_proc'::regclass AND refobjid=ANY(CAST(:oids AS oid[])))),
        'shared',(SELECT jsonb_agg(to_jsonb(d) ORDER BY dbid,classid,objid,objsubid,refclassid,refobjid,deptype)
            FROM pg_shdepend d WHERE classid='pg_proc'::regclass AND objid=ANY(CAST(:oids AS oid[]))))"""),
        {"oids": oids},
    ).scalar_one()


def preparation_handoff_index_sql():
    """Admit the plain native index without changing historical protected catalog checks."""
    return f"""SELECT coalesce((SELECT
        c.relkind='i' AND c.relpersistence='p' AND c.relowner=t.relowner AND c.relnamespace=t.relnamespace
        AND c.reloptions IS NULL AND c.relacl IS NULL AND am.amname='hash'
        AND i.indisvalid AND i.indisready AND i.indislive AND i.indimmediate
        AND NOT(i.indisunique OR i.indisprimary OR i.indisexclusion OR i.indnullsnotdistinct)
        AND i.indnkeyatts=1 AND i.indnatts=1 AND i.indkey[0]=a.attnum
        AND i.indcollation[0]=0 AND i.indoption[0]=0 AND i.indexprs IS NULL AND i.indpred IS NULL
        AND op.opcnamespace='pg_catalog'::regnamespace AND op.opcname='jsonb_ops'
        AND op.opcintype='pg_catalog.jsonb'::regtype
        FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid
        JOIN pg_class t ON t.oid=i.indrelid JOIN pg_am am ON am.oid=c.relam
        JOIN pg_attribute a ON a.attrelid=t.oid AND a.attname='handoff' AND NOT a.attisdropped
        JOIN pg_opclass op ON op.oid=i.indclass[0]
        WHERE i.indexrelid=to_regclass('hp_snapshot_retention.{PREPARATION_HANDOFF_INDEX}')
          AND i.indrelid=to_regclass('hp_snapshot_retention.preparation')
          AND a.atttypid='pg_catalog.jsonb'::regtype),false)"""


def _ensure_preparation_handoff_index(connection):
    from sqlalchemy import text

    before = connection.execute(text("SELECT current_user::text,current_setting('role')")).one()
    protected_owner = connection.execute(
        text("SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid='hp_snapshot_retention.preparation'::regclass")
    ).scalar_one()
    exists = connection.execute(
        text("SELECT to_regclass(:name)"), {"name": "hp_snapshot_retention." + PREPARATION_HANDOFF_INDEX}
    ).scalar_one_or_none()
    if exists is None:
        connection.exec_driver_sql("SET LOCAL ROLE " + storage_identifier(protected_owner))
        connection.exec_driver_sql(
            "CREATE INDEX "
            + storage_identifier(PREPARATION_HANDOFF_INDEX)
            + " ON hp_snapshot_retention.preparation USING hash(handoff)"
        )
        connection.exec_driver_sql(
            "RESET ROLE" if before[1] == "none" else "SET LOCAL ROLE " + storage_identifier(before[1])
        )
    if connection.execute(text("SELECT current_user::text,current_setting('role')")).one() != before:
        raise RuntimeError("rooted_publication_migration_role_changed")
    if connection.execute(text(preparation_handoff_index_sql())).scalar_one_or_none() is not True:
        raise RuntimeError("rooted_publication_handoff_index_changed")
