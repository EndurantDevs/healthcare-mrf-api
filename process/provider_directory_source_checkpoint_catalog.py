# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Qualified checkpoint relations with fixed native data and transition contracts."""

from db.migration_expression_adoption import _normalized_expression
from db.plan_release_storage import storage_identifier, storage_relation
from process.provider_directory_generic_source_storage import _json, _require

BULK_ROOT = "provider_directory_bulk_acquisition_checkpoint"
RETIRED_BULK_HOOKS = frozenset(
    "pd_subset_abandonment_bulk_" + action + "_guard" for action in ("insert", "update", "delete", "truncate")
)

CHECKPOINT_LAYOUTS = {
    "provider_directory_bulk_acquisition_checkpoint": {
        "columns": [
            ["checkpoint_id", "character varying(64)", True, None],
            ["canonical_api_base", "text", True, None],
            ["resource_type", "character varying(64)", True, None],
            ["source_scope_hash", "character varying(64)", True, None],
            ["strategy_version", "character varying(64)", True, None],
            ["acquisition_root_run_id", "character varying(64)", True, None],
            ["owner_run_id", "character varying(64)", True, None],
            ["retry_of_run_id", "character varying(64)", False, None],
            ["endpoint_id", "character varying(64)", True, None],
            ["dataset_id", "character varying(96)", True, None],
            ["start_url_hash", "character varying(64)", True, None],
            ["status_url_ciphertext", "text", False, None],
            ["status_url_hash", "character varying(64)", False, None],
            ["manifest_hash", "character varying(64)", False, None],
            ["manifest_ciphertext", "text", False, None],
            ["manifest_json", "jsonb", False, None],
            ["state", "character varying(32)", True, None],
            ["lease_expires_at", "timestamp without time zone", False, None],
            ["rows_written", "bigint", True, "0"],
            ["error", "text", False, None],
            ["created_at", "timestamp without time zone", True, "now()"],
            ["accepted_at", "timestamp without time zone", False, None],
            ["last_polled_at", "timestamp without time zone", False, None],
            ["manifest_received_at", "timestamp without time zone", False, None],
            ["completed_at", "timestamp without time zone", False, None],
            ["failed_at", "timestamp without time zone", False, None],
            ["updated_at", "timestamp without time zone", True, "now()"],
            ["next_poll_at", "timestamp without time zone", False, None],
        ],
        "constraints": [
            ["f", "FOREIGN KEY (dataset_id) REFERENCES mrf.provider_directory_endpoint_dataset(dataset_id)"],
            ["f", "FOREIGN KEY (endpoint_id) REFERENCES mrf.provider_directory_api_endpoint(endpoint_id)"],
            ["p", "PRIMARY KEY (checkpoint_id)"],
            [
                "u",
                "UNIQUE (canonical_api_base, resource_type, "
                "source_scope_hash, strategy_version, "
                "acquisition_root_run_id, dataset_id)",
            ],
        ],
        "indexes": [
            [False, False, "btree", None, None, ["acquisition_root_run_id"]],
            [False, False, "btree", None, None, ["dataset_id"]],
            [False, False, "btree", None, None, ["owner_run_id"]],
            [False, False, "btree", None, None, ["state", "updated_at"]],
            [
                False,
                True,
                "btree",
                None,
                None,
                [
                    "canonical_api_base",
                    "resource_type",
                    "source_scope_hash",
                    "strategy_version",
                    "acquisition_root_run_id",
                    "dataset_id",
                ],
            ],
            [True, True, "btree", None, None, ["checkpoint_id"]],
        ],
    },
    "provider_directory_bulk_output_checkpoint": {
        "columns": [
            ["checkpoint_id", "character varying(64)", True, None],
            ["output_id", "character varying(64)", True, None],
            ["output_index", "integer", True, None],
            ["resource_type", "character varying(64)", True, None],
            ["output_url_ciphertext", "text", False, None],
            ["output_url_hash", "character varying(64)", True, None],
            ["state", "character varying(32)", True, None],
            ["rows_written", "bigint", True, "0"],
            ["attempt_count", "integer", True, "0"],
            ["error", "text", False, None],
            ["created_at", "timestamp without time zone", True, "now()"],
            ["started_at", "timestamp without time zone", False, None],
            ["completed_at", "timestamp without time zone", False, None],
            ["updated_at", "timestamp without time zone", True, "now()"],
            ["content_length_bytes", "bigint", False, None],
            ["etag_ciphertext", "text", False, None],
            ["etag_hash", "character varying(64)", False, None],
            ["committed_bytes", "bigint", True, "0"],
            ["output_expires_at", "timestamp without time zone", False, None],
            ["validator_checked_at", "timestamp without time zone", False, None],
            ["last_error", "text", False, None],
            ["last_error_at", "timestamp without time zone", False, None],
        ],
        "constraints": [
            [
                "f",
                "FOREIGN KEY (checkpoint_id) REFERENCES "
                "mrf.provider_directory_bulk_acquisition_checkpoint(checkpoint_id) "
                "ON DELETE CASCADE",
            ],
            ["p", "PRIMARY KEY (checkpoint_id, output_id)"],
            ["u", "UNIQUE (checkpoint_id, output_index)"],
        ],
        "indexes": [
            [False, False, "btree", None, None, ["state", "updated_at"]],
            [False, True, "btree", None, None, ["checkpoint_id", "output_index"]],
            [True, True, "btree", None, None, ["checkpoint_id", "output_id"]],
        ],
    },
    "provider_directory_dataset_rehydration_checkpoint": {
        "columns": [
            ["source_id", "character varying(64)", True, None],
            ["dataset_id", "character varying(96)", True, None],
            ["acquisition_root_run_id", "character varying(64)", True, None],
            ["resource_type", "character varying(64)", True, None],
            ["endpoint_id", "character varying(64)", True, None],
            ["dataset_hash", "character varying(64)", True, None],
            ["owner_run_id", "character varying(64)", True, None],
            ["state", "character varying(32)", True, None],
            ["last_resource_id", "character varying(256)", False, None],
            ["expected_input_count", "bigint", True, "'0'::bigint"],
            ["input_count", "bigint", True, "'0'::bigint"],
            ["mapped_count", "bigint", True, "'0'::bigint"],
            ["rejected_count", "bigint", True, "'0'::bigint"],
            ["evidence_json", "jsonb", True, "'{}'::jsonb"],
            ["error", "text", False, None],
            ["created_at", "timestamp without time zone", True, None],
            ["started_at", "timestamp without time zone", True, None],
            ["updated_at", "timestamp without time zone", True, None],
            ["completed_at", "timestamp without time zone", False, None],
        ],
        "constraints": [
            ["f", "FOREIGN KEY (dataset_id) REFERENCES mrf.provider_directory_endpoint_dataset(dataset_id)"],
            ["f", "FOREIGN KEY (endpoint_id) REFERENCES mrf.provider_directory_api_endpoint(endpoint_id)"],
            ["f", "FOREIGN KEY (source_id) REFERENCES mrf.provider_directory_source(source_id)"],
            ["p", "PRIMARY KEY (source_id, dataset_id, acquisition_root_run_id, resource_type)"],
        ],
        "indexes": [
            [False, False, "btree", None, None, ["dataset_id"]],
            [False, False, "btree", None, None, ["owner_run_id"]],
            [False, False, "btree", None, None, ["state", "updated_at"]],
            [True, True, "btree", None, None, ["source_id", "dataset_id", "acquisition_root_run_id", "resource_type"]],
        ],
    },
    "provider_directory_pagination_checkpoint": {
        "columns": [
            ["canonical_api_base", "text", True, None],
            ["resource_type", "character varying(64)", True, None],
            ["source_scope_hash", "character varying(64)", True, None],
            ["dataset_id", "character varying(96)", False, None],
            ["source_ids", "jsonb", True, None],
            ["owner_run_id", "character varying(64)", True, None],
            ["retry_of_run_id", "character varying(64)", False, None],
            ["start_url_hash", "character varying(64)", True, None],
            ["next_url", "text", False, None],
            ["state", "character varying(32)", True, None],
            ["pages_processed", "bigint", True, "0"],
            ["rows_processed", "bigint", True, "0"],
            ["recent_cursor_hashes", "jsonb", True, "'[]'::jsonb"],
            ["created_at", "timestamp without time zone", True, "now()"],
            ["updated_at", "timestamp without time zone", True, "now()"],
            ["completed_at", "timestamp without time zone", False, None],
            ["acquisition_root_run_id", "character varying(64)", True, None],
            ["completeness_json", "jsonb", True, "'{}'::jsonb"],
        ],
        "constraints": [
            ["f", "FOREIGN KEY (dataset_id) REFERENCES mrf.provider_directory_endpoint_dataset(dataset_id)"],
            ["p", "PRIMARY KEY (canonical_api_base, resource_type, source_scope_hash, acquisition_root_run_id)"],
        ],
        "indexes": [
            [False, False, "btree", None, None, ["acquisition_root_run_id", "updated_at"]],
            [False, False, "btree", None, None, ["dataset_id"]],
            [False, False, "btree", None, None, ["owner_run_id"]],
            [False, False, "btree", None, None, ["state", "updated_at"]],
            [
                True,
                True,
                "btree",
                None,
                None,
                ["canonical_api_base", "resource_type", "source_scope_hash", "acquisition_root_run_id"],
            ],
        ],
    },
}

_GUARDS = {
    "provider_directory_bulk_acquisition_checkpoint": {
        "pd_subset_abandonment_bulk_delete_guard": (
            8,
            False,
            False,
            "old_rows",
            None,
            "guard_provider_directory_subset_abandonment_child",
        ),
        "pd_subset_abandonment_bulk_insert_guard": (
            4,
            False,
            False,
            None,
            "new_rows",
            "guard_provider_directory_subset_abandonment_child",
        ),
        "pd_subset_abandonment_bulk_truncate_guard": (
            34,
            False,
            False,
            None,
            None,
            "guard_provider_directory_subset_abandonment_child",
        ),
        "pd_subset_abandonment_bulk_update_guard": (
            16,
            False,
            False,
            "old_rows",
            "new_rows",
            "guard_provider_directory_subset_abandonment_child",
        ),
        "pd_trr_bulk_acq_row": (
            31,
            False,
            False,
            None,
            None,
            "guard_provider_directory_terminal_root_retirement_child",
        ),
        "pd_trr_bulk_acq_truncate": (
            34,
            False,
            False,
            None,
            None,
            "guard_provider_directory_terminal_root_retirement_child",
        ),
    },
    "provider_directory_bulk_output_checkpoint": {
        "pd_trr_bulk_output_row": (
            31,
            False,
            False,
            None,
            None,
            "guard_provider_directory_terminal_root_retirement_child",
        ),
        "pd_trr_bulk_output_truncate": (
            34,
            False,
            False,
            None,
            None,
            "guard_provider_directory_terminal_root_retirement_child",
        ),
    },
    "provider_directory_dataset_rehydration_checkpoint": {
        "pd_trr_rehydration_row": (
            31,
            False,
            False,
            None,
            None,
            "guard_provider_directory_terminal_root_retirement_child",
        ),
        "pd_trr_rehydration_truncate": (
            34,
            False,
            False,
            None,
            None,
            "guard_provider_directory_terminal_root_retirement_child",
        ),
    },
    "provider_directory_pagination_checkpoint": {
        "pd_subset_abandonment_checkpoint_guard": (
            31,
            False,
            False,
            None,
            None,
            "guard_provider_directory_subset_abandonment_checkpoint",
        ),
        "pd_subset_abandonment_checkpoint_truncate_guard": (
            34,
            False,
            False,
            None,
            None,
            "guard_provider_directory_subset_abandonment_checkpoint",
        ),
        "pd_trr_pagination_row": (
            31,
            False,
            False,
            None,
            None,
            "guard_provider_directory_terminal_root_retirement_child",
        ),
        "pd_trr_pagination_truncate": (
            34,
            False,
            False,
            None,
            None,
            "guard_provider_directory_terminal_root_retirement_child",
        ),
        "provider_directory_subset_abandonment_checkpoint_guard": (
            29,
            True,
            True,
            None,
            None,
            "guard_provider_directory_subset_abandonment_checkpoint",
        ),
        "provider_directory_subset_terminal_disposition_checkpoint_guard": (
            29,
            True,
            True,
            None,
            None,
            "guard_provider_directory_subset_abandonment_checkpoint",
        ),
    },
}

_BODIES = {
    "guard_provider_directory_subset_abandonment_child": "a23ecc5a9a36d864f60626f93d5373ebc71edebb41c27ce1236ff1201f0eb21d",
    "guard_provider_directory_terminal_root_retirement_child": "fa606094c8ba8fb2b38d44fed8fc7ab7e291a593140603684fbe08f4a652533a",
    "guard_provider_directory_subset_abandonment_checkpoint": "0923658f97c6fd101e92460c14a3310120c344de1478e97b6d5f475aef77e892",
}

_LAYOUT_SQL = """SELECT n.nspname,c.relname,c.relkind::text,c.relpersistence::text,c.relispartition,
 c.relrowsecurity,c.relforcerowsecurity,
 ARRAY(SELECT quote_ident(pn.nspname)||'.'||quote_ident(parent.relname)
  FROM pg_inherits i JOIN pg_class parent ON parent.oid=i.inhparent
  JOIN pg_namespace pn ON pn.oid=parent.relnamespace WHERE i.inhrelid=c.oid ORDER BY pn.nspname,parent.relname) AS parents,
 ARRAY(SELECT quote_ident(cn.nspname)||'.'||quote_ident(child.relname)
  FROM pg_inherits i JOIN pg_class child ON child.oid=i.inhrelid
  JOIN pg_namespace cn ON cn.oid=child.relnamespace WHERE i.inhparent=c.oid ORDER BY cn.nspname,child.relname) AS children,
 (SELECT coalesce(jsonb_agg(jsonb_build_array(a.attname,format_type(a.atttypid,a.atttypmod),
  a.attnotnull,pg_get_expr(d.adbin,d.adrelid)) ORDER BY a.attnum),'[]'::jsonb)
  FROM pg_attribute a LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum
  WHERE a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped) AS columns,
 (SELECT coalesce(jsonb_agg(jsonb_build_object('type',k.contype::text,
  'definition',pg_get_constraintdef(k.oid),
  'valid',k.convalidated AND coalesce((to_jsonb(k)->>'conenforced')::boolean,true)) ORDER BY k.conname),'[]'::jsonb)
  FROM pg_constraint k WHERE k.conrelid=c.oid AND k.contype<>'n') AS constraints,
 (SELECT coalesce(jsonb_agg(jsonb_build_object('primary',i.indisprimary,'unique',i.indisunique,
  'valid',i.indisvalid AND i.indisready AND i.indislive,'method',am.amname,
  'predicate',pg_get_expr(i.indpred,i.indrelid),'expression',pg_get_expr(i.indexprs,i.indrelid),
  'keys',ARRAY(SELECT a.attname FROM unnest(i.indkey::smallint[]) WITH ORDINALITY key(attnum,ordinal)
   LEFT JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum=key.attnum ORDER BY key.ordinal))
  ORDER BY idx.relname),'[]'::jsonb)
  FROM pg_index i JOIN pg_class idx ON idx.oid=i.indexrelid JOIN pg_am am ON am.oid=idx.relam
  WHERE i.indrelid=c.oid) AS indexes,
 NOT EXISTS(SELECT FROM pg_constraint k JOIN pg_depend d
  ON d.classid='pg_constraint'::regclass AND d.objid=k.oid
  JOIN pg_proc p ON d.refclassid='pg_proc'::regclass AND d.refobjid=p.oid
  WHERE k.conrelid=c.oid AND p.pronamespace<>'pg_catalog'::regnamespace) AS native_checks,
 NOT EXISTS(SELECT FROM pg_trigger t WHERE t.tgrelid=c.oid AND NOT t.tgisinternal) AS inert
 FROM unnest($1::text[]) requested(name) JOIN pg_class c ON c.oid=to_regclass(requested.name)
 JOIN pg_namespace n ON n.oid=c.relnamespace ORDER BY n.nspname,c.relname"""

_GUARD_SQL = """SELECT t.tgname,t.tgtype,t.tgenabled::text,t.tgnargs,t.tgqual IS NULL AS unconditional,
 t.tgoldtable,t.tgnewtable,t.tgargs,t.tgdeferrable,t.tginitdeferred,
 ARRAY(SELECT a.attname::text FROM unnest(t.tgattr::smallint[]) WITH ORDINALITY watched(attnum,ordinal)
  JOIN pg_attribute a ON a.attrelid=t.tgrelid AND a.attnum=watched.attnum
  WHERE NOT a.attisdropped ORDER BY watched.ordinal) AS update_columns,
 p.proname,n.nspname FROM pg_trigger t JOIN pg_proc p ON p.oid=t.tgfoid
 JOIN pg_namespace n ON n.oid=p.pronamespace
 WHERE t.tgrelid=to_regclass($1) AND NOT t.tgisinternal ORDER BY t.tgname"""

_NATIVE_SQL = """
SELECT NOT EXISTS(SELECT FROM pg_rewrite WHERE ev_class=$1::oid)
 AND NOT EXISTS(SELECT FROM pg_attribute a JOIN pg_type t ON t.oid=a.atttypid
 WHERE a.attrelid=$1::oid AND a.attnum>0 AND NOT a.attisdropped
 AND (a.attgenerated<>'' OR a.attidentity<>'' OR a.attcollation<>t.typcollation))
 AND NOT EXISTS(SELECT FROM pg_index i CROSS JOIN LATERAL
 unnest(i.indkey::smallint[],i.indclass::oid[],i.indcollation::oid[],i.indoption::smallint[])
 x(attnum,opclass_oid,collation_oid,flags) LEFT JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=x.attnum
 LEFT JOIN pg_opclass op ON op.oid=x.opclass_oid
 WHERE i.indrelid=$1::oid AND (NOT i.indimmediate OR i.indnkeyatts<>i.indnatts OR i.indisexclusion
 OR a.attnum IS NULL OR op.opcnamespace<>'pg_catalog'::regnamespace OR NOT op.opcdefault
 OR op.opcintype<>CASE a.atttypid WHEN 'varchar'::regtype THEN 'text'::regtype ELSE a.atttypid END
 OR x.flags<>0 OR x.collation_oid<>a.attcollation))
 AND NOT EXISTS(SELECT FROM pg_constraint k WHERE k.conrelid=$1::oid AND k.contype='f'
 AND (NOT k.convalidated OR k.condeferrable OR k.condeferred OR k.confmatchtype<>'s'
 OR k.confupdtype<>'a' OR k.confdeltype NOT IN('a','c')))
 AND NOT EXISTS(SELECT FROM pg_trigger t JOIN pg_proc p ON p.oid=t.tgfoid
 LEFT JOIN pg_constraint k ON k.oid=t.tgconstraint WHERE t.tgrelid=$1::oid AND t.tgisinternal
 AND (k.contype<>'f' OR t.tgenabled<>'O' OR p.pronamespace<>'pg_catalog'::regnamespace
 OR NOT ((t.tgtype=5 AND p.proname='RI_FKey_check_ins')
 OR (t.tgtype=17 AND p.proname IN('RI_FKey_check_upd','RI_FKey_noaction_upd'))
 OR (t.tgtype=9 AND p.proname='RI_FKey_cascade_del'))))
 AND (SELECT count(*) FROM pg_trigger WHERE tgrelid=$1::oid AND tgisinternal)
 = 2*(SELECT count(*) FROM pg_constraint WHERE contype='f' AND (conrelid=$1::oid OR confrelid=$1::oid))
"""


def _canonical_definition(value, schema):
    return _normalized_expression(
        value.replace(storage_identifier(schema) + ".", '"__schema__".').replace(schema + ".", '"__schema__".')
    )


def _require_layout(relation, schema, *, isolated=False):
    expected = CHECKPOINT_LAYOUTS[relation["relname"]]
    constraints = _json(relation["constraints"])
    actual_constraints = sorted(
        [
            (entry["type"], _canonical_definition(entry["definition"], schema))
            for entry in constraints
            if entry["type"] != "t"
        ]
    )
    expected_constraints = sorted(
        [
            (kind, _canonical_definition(definition, "mrf"))
            for kind, definition in expected["constraints"]
            if not isolated or kind != "f"
        ]
    )
    indexes = _json(relation["indexes"])
    actual_indexes = sorted(
        [
            [entry["primary"], entry["unique"], entry["method"], entry["predicate"], entry["expression"], entry["keys"]]
            for entry in indexes
        ],
        key=str,
    )
    _require(
        relation["relkind"] == "r"
        and relation["relpersistence"] == "p"
        and not relation["relispartition"]
        and not relation["parents"]
        and not relation.get("children", ())
        and not relation["relrowsecurity"]
        and not relation["relforcerowsecurity"]
        and relation["native_checks"]
        and _json(relation["columns"]) == expected["columns"]
        and all(entry["valid"] for entry in constraints)
        and actual_constraints == expected_constraints
        and all(entry["valid"] for entry in indexes)
        and actual_indexes == expected["indexes"],
        "checkpoint_layout_changed",
    )
    return {
        "columns": _json(relation["columns"]),
        "constraints": [
            {"type": kind, "definition": definition, "valid": True} for kind, definition in actual_constraints
        ],
        "indexes": [
            dict(
                zip(("primary", "unique", "method", "predicate", "expression", "keys"), entry, strict=True), valid=True
            )
            for entry in actual_indexes
        ],
    }


def _require_guards(guards, root, policy, *, legacy):
    expected = _GUARDS[root]
    names = {guard["tgname"] for guard in guards}
    retired = RETIRED_BULK_HOOKS if root == BULK_ROOT else frozenset()
    _require(
        len(guards) == len(names) and (names == set(expected) - retired or legacy and names == set(expected)),
        "checkpoint_hooks_changed",
    )
    for guard in guards:
        invocation = (
            guard["tgtype"],
            guard["tgdeferrable"],
            guard["tginitdeferred"],
            guard["tgoldtable"],
            guard["tgnewtable"],
            guard["proname"],
        )
        _require(
            invocation == expected[guard["tgname"]]
            and guard["tgenabled"] == "A"
            and guard["unconditional"]
            and not guard["update_columns"]
            and guard["tgnargs"] == 0
            and bytes(guard["tgargs"]) == b""
            and guard["nspname"] == policy.schema,
            "checkpoint_guard_changed",
        )


async def capture_checkpoint_controls(connection, policy, *, legacy=False, storage_binding=None):
    """Validate every declared checkpoint relation without database-local identity metadata."""
    if storage_binding is not None:
        from process.provider_directory_source_storage_binding import verify_isolated_source_storage_coordinates

        _require(not legacy, "checkpoint_contract_conflict")
        await verify_isolated_source_storage_coordinates(connection, policy, storage_binding)
    roots = [storage_relation(policy.schema, name) for name in CHECKPOINT_LAYOUTS]
    relations = await connection.fetch(_LAYOUT_SQL, roots)
    _require(
        len(relations) == len(roots) and {relation["relname"] for relation in relations} == set(CHECKPOINT_LAYOUTS),
        "checkpoint_missing",
    )
    controls_by_root = {}
    native_sql = _NATIVE_SQL.replace("$1::oid", "to_regclass($1)::oid")
    for relation in relations:
        name = relation["relname"]
        qualified = storage_relation(policy.schema, name)
        _require(relation["nspname"] == policy.schema, "checkpoint_namespace_changed")
        layout = _require_layout(relation, policy.schema, isolated=storage_binding is not None)
        await connection.execute("SELECT 1 FROM " + qualified + " LIMIT 0")
        _require(await connection.fetchval(native_sql, qualified) is True, "checkpoint_native_contract_changed")
        if storage_binding is None:
            guards = await connection.fetch(_GUARD_SQL, qualified)
            _require_guards(guards, name, policy, legacy=legacy)
        else:
            _require(relation["inert"], "checkpoint_hooks_changed")
            guards = []
        controls_by_root[name] = {
            "schema": policy.schema,
            "name": name,
            "layout": layout,
            "guards": [
                {
                    key: guard[key]
                    for key in (
                        "tgname",
                        "tgtype",
                        "tgdeferrable",
                        "tginitdeferred",
                        "tgoldtable",
                        "tgnewtable",
                        "proname",
                        "nspname",
                    )
                }
                for guard in guards
                if guard["tgname"] not in RETIRED_BULK_HOOKS
            ],
        }
    return controls_by_root


async def retire_bulk_abandonment_hooks(connection, policy, before):
    """Only called after fixed writers and direct/inherited privileges are closed."""
    current = await capture_checkpoint_controls(connection, policy, legacy=True)
    _require(current == before, "checkpoint_identity_changed")
    for name in sorted(RETIRED_BULK_HOOKS):
        await connection.execute(
            f"DROP TRIGGER IF EXISTS {storage_identifier(name)} ON {storage_relation(policy.schema, BULK_ROOT)}"
        )
    _require(await capture_checkpoint_controls(connection, policy) == before, "checkpoint_identity_changed")
