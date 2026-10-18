# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fixed native contract for the existing protected release controls."""

from db.migration_expression_adoption import _normalized_expression
from db.plan_release_storage import storage_relation
from process.plan_pricing_control_policy import pricing_control_schema
from process.plan_release_storage_catalog import CATALOG_SQL
from process.plan_release_storage_custody import _document

CONTROL_SHAPES = {
    "catalog_release_preparation": {
        "columns": [
            ["preparation_id", "uuid", True, None],
            ["operation_key", "character varying(64)", True, None],
            ["scope_digest", "character varying(64)", True, None],
            ["scope", "jsonb", True, None],
            ["preliminary_generation_id", "uuid", True, None],
            ["final_generation_id", "uuid", True, None],
            ["status", "character varying(16)", True, None],
            ["inventory", "jsonb", True, None],
            ["preliminary_registry", "jsonb", False, None],
            ["final_registry", "jsonb", False, None],
            ["receipt", "jsonb", False, None],
            ["receipt_digest", "character varying(64)", False, None],
            ["publication_receipt", "jsonb", False, None],
            ["publication_receipt_digest", "character varying(64)", False, None],
            ["lease_token", "uuid", True, None],
            ["lease_expires_at", "timestamp with time zone", True, None],
            ["created_at", "timestamp with time zone", True, "now()"],
        ],
        "checks": {
            "catalog_release_preparation_publication_check": "CHECK "
            "((((publication_receipt "
            "IS NULL) = "
            "(publication_receipt_digest "
            "IS NULL)) AND "
            "((publication_receipt_digest "
            "IS NULL) OR "
            "((publication_receipt_digest)::text "
            "~ "
            "'^[0-9a-f]{64}$'::text)) "
            "AND "
            "(((status)::text "
            "= "
            "'published'::text) "
            "= "
            "(publication_receipt "
            "IS NOT "
            "NULL))))",
            "catalog_release_preparation_receipt_check": "CHECK ((((receipt "
            "IS NULL) = "
            "(receipt_digest IS "
            "NULL)) AND "
            "((receipt_digest "
            "IS NULL) OR "
            "((receipt_digest)::text "
            "~ "
            "'^[0-9a-f]{64}$'::text))))",
            "catalog_release_preparation_scope_check": "CHECK (((scope_digest)::text ~ '^[0-9a-f]{64}$'::text))",
            "catalog_release_preparation_shape_check": "CHECK "
            "(((jsonb_typeof(scope) "
            "= 'object'::text) "
            "AND "
            "(jsonb_typeof(inventory) "
            "= 'object'::text)))",
            "catalog_release_preparation_status_check": "CHECK "
            "(((status)::text = "
            "ANY "
            "((ARRAY['preparing'::character "
            "varying, "
            "'admitting'::character "
            "varying, "
            "'sealed'::character "
            "varying, "
            "'published'::character "
            "varying, "
            "'abandoned'::character "
            "varying])::text[])))",
        },
        "indexes": [
            [["final_generation_id"], False, True],
            [["operation_key"], False, True],
            [["preliminary_generation_id"], False, True],
            [["preparation_id"], True, True],
            [["scope_digest"], False, True],
        ],
    },
    "catalog_release_admission_attempt": {
        "columns": [
            ["preparation_id", "uuid", True, None],
            ["publication_identity", "character varying(64)", True, None],
            ["attempt_identity", "character varying(64)", True, None],
            ["healthporta_plan_id", "character varying(64)", True, None],
            ["node_id", "character varying(64)", True, None],
            ["admission_key", "text", True, None],
            ["request_digest", "character varying(64)", True, None],
            ["request", "jsonb", True, None],
            ["status", "character varying(16)", True, None],
            ["dispatches", "jsonb", True, "'[]'::jsonb"],
            ["engine_run_id", "character varying(128)", False, None],
            ["receipt", "jsonb", False, None],
        ],
        "checks": {
            "catalog_release_admission_dispatch_check": "CHECK "
            "(((jsonb_typeof(dispatches) "
            "= "
            "'array'::text) "
            "AND "
            "(jsonb_array_length(dispatches) "
            "<= 3)))",
            "catalog_release_admission_identity_check": "CHECK "
            "((((publication_identity)::text "
            "~ "
            "'^[0-9a-f]{64}$'::text) "
            "AND "
            "((attempt_identity)::text "
            "~ "
            "'^[0-9a-f]{64}$'::text) "
            "AND "
            "((request_digest)::text "
            "~ "
            "'^[0-9a-f]{64}$'::text)))",
            "catalog_release_admission_request_check": "CHECK ((jsonb_typeof(request) = 'object'::text))",
            "catalog_release_admission_run_check": "CHECK "
            "((((status)::text "
            "= ANY "
            "((ARRAY['reserved'::character "
            "varying, "
            "'dispatching'::character "
            "varying, "
            "'failed'::character "
            "varying])::text[])) "
            "OR (engine_run_id "
            "IS NOT NULL)))",
            "catalog_release_admission_status_check": "CHECK "
            "(((status)::text "
            "= ANY "
            "((ARRAY['reserved'::character "
            "varying, "
            "'dispatching'::character "
            "varying, "
            "'accepted'::character "
            "varying, "
            "'ready'::character "
            "varying, "
            "'unavailable'::character "
            "varying, "
            "'failed'::character "
            "varying])::text[])))",
        },
        "indexes": [
            [["admission_key"], False, True],
            [["node_id", "engine_run_id"], False, False],
            [["preparation_id", "publication_identity"], True, True],
        ],
    },
}


async def require_control_catalog(connection, family):
    """Reject partial, writable, hooked or definition-substituted native controls."""
    from process.plan_pricing_admission import require

    relations = await connection.fetch(
        CATALOG_SQL,
        [storage_relation(pricing_control_schema(), name) for name in CONTROL_SHAPES],
        family["owner_oid"],
        family["reader_oids"],
    )
    require(len(relations) == len(CONTROL_SHAPES), "protected_controls_missing")
    for relation in relations:
        expected = CONTROL_SHAPES[relation["relname"]]
        require(
            relation["relowner"] == family["owner_oid"]
            and relation["relkind"] == "r"
            and relation["relpersistence"] == "p"
            and not relation["parents"]
            and not relation["relrowsecurity"]
            and not relation["relforcerowsecurity"]
            and all(
                relation[field] is True for field in ("inert", "native_checks", "direct_acl_closed", "readers_closed")
            ),
            "protected_controls_open",
        )
        require(_document(relation["columns"]) == expected["columns"], "protected_columns_changed")
        constraints = _document(relation["constraints"])
        checks_by_name = {
            catalog_entry["name"]: _normalized_expression(catalog_entry["definition"])
            for catalog_entry in constraints
            if catalog_entry["type"] == "c"
        }
        require(
            checks_by_name
            == {name: _normalized_expression(definition) for name, definition in expected["checks"].items()}
            and all(
                catalog_entry["valid"] and catalog_entry["type"] in {"p", "u", "c"} for catalog_entry in constraints
            ),
            "protected_constraints_changed",
        )
        await _require_control_indexes(connection, relation, expected)


async def _require_control_indexes(connection, relation, expected):
    from process.plan_pricing_admission import require

    indexes = _document(relation["indexes"])
    require(
        sorted(
            [[catalog_entry["keys"], catalog_entry["primary"], catalog_entry["unique"]] for catalog_entry in indexes]
        )
        == expected["indexes"]
        and all(
            catalog_entry["valid"]
            and catalog_entry["method"] == "btree"
            and catalog_entry["predicate"] is None
            and catalog_entry["expression"] is None
            for catalog_entry in indexes
        ),
        "protected_indexes_changed",
    )
    require(
        not await connection.fetchval(
            "SELECT EXISTS(SELECT 1 FROM pg_index index CROSS JOIN LATERAL unnest(index.indkey::smallint[],"
            "index.indclass::oid[],index.indcollation::oid[],index.indoption::smallint[]) key(attnum,opclass_oid,collation_oid,sort_flags) "
            "JOIN pg_opclass op ON op.oid=key.opclass_oid JOIN pg_attribute a ON a.attrelid=index.indrelid AND a.attnum=key.attnum "
            "WHERE index.indrelid=$1 AND (NOT index.indimmediate OR index.indisexclusion OR index.indnkeyatts<>index.indnatts "
            "OR op.opcnamespace<>'pg_catalog'::regnamespace OR op.opcname<>CASE a.atttypid WHEN 'uuid'::regtype THEN 'uuid_ops' ELSE 'text_ops' END "
            "OR key.collation_oid<>a.attcollation OR key.sort_flags<>0))",
            relation["oid"],
        ),
        "protected_index_operator_changed",
    )
