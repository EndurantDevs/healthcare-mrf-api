# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fixed native layouts and authentic low-volume checkpoint guard contracts."""

import hashlib
import re

from db.migration_expression_adoption import _normalized_expression
from db.plan_release_storage import storage_identifier, storage_relation
from process.plan_release_storage_catalog import CATALOG_SQL
from process.plan_release_storage_custody import storage_roles
from process.provider_directory_generic_source_storage import _json, _require
from process.provider_directory_source_coverage_control import _GUARD_SQL, _digest

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


def _require_layout(relation, schema):
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
        [(kind, _canonical_definition(definition, "mrf")) for kind, definition in expected["constraints"]]
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
        body = re.sub(
            r"\s+", " ", guard["prosrc"].replace(storage_identifier(policy.schema) + ".", '"__schema__".')
        ).strip()
        _require(
            invocation == expected[guard["tgname"]]
            and guard["tgenabled"] == "A"
            and guard["unconditional"]
            and not guard["update_columns"]
            and guard["tgnargs"] == 0
            and bytes(guard["tgargs"]) == b""
            and guard["nspname"] == policy.schema
            and guard["pronargs"] == 0
            and guard["lanname"] == "plpgsql"
            and guard["returns_trigger"]
            and guard["prosecdef"]
            and guard["provolatile"] == "v"
            and guard["proconfig"] == ["search_path=pg_catalog"]
            and guard["function_owner"] in (policy.owner_role, *policy.control_runtime_roles)
            and hashlib.sha256(body.encode()).hexdigest() == _BODIES[guard["proname"]],
            "checkpoint_guard_changed",
        )


async def capture_checkpoint_controls(connection, policy, *, legacy=False):
    """Attest every fixed root before its original-session control transaction."""
    owner, readers = await storage_roles(connection, policy.schema, **policy.role_arguments())
    roots = [storage_relation(policy.schema, name) for name in CHECKPOINT_LAYOUTS]
    relations = await connection.fetch(CATALOG_SQL, roots, owner, readers)
    _require(
        len(relations) == len(roots) and {relation["relname"] for relation in relations} == set(CHECKPOINT_LAYOUTS),
        "checkpoint_missing",
    )
    proof_by_root = {}
    for relation in relations:
        _require(relation["nspname"] == policy.schema, "checkpoint_namespace_changed")
        _require_layout(relation, policy.schema)
        if legacy:
            permitted = await connection.fetchval(
                "SELECT $1::oid=ANY(ARRAY(SELECT oid FROM pg_roles WHERE rolname=ANY($2::text[])))",
                relation["relowner"],
                [policy.owner_role, *policy.control_runtime_roles],
            )
            _require(permitted is True, "checkpoint_legacy_owner_changed")
        else:
            _require(
                relation["relowner"] == owner and relation["direct_acl_closed"] and relation["readers_closed"],
                "checkpoint_custody_changed",
            )
        _require(await connection.fetchval(_NATIVE_SQL, relation["oid"]) is True, "checkpoint_native_contract_changed")
        guards = await connection.fetch(_GUARD_SQL, relation["oid"])
        _require_guards(guards, relation["relname"], policy, legacy=legacy)
        proof_by_root[relation["relname"]] = {
            "oid": relation["oid"],
            "layout": _digest({key: _json(relation[key]) for key in ("columns", "constraints", "indexes")}),
            "guards": [dict(guard) for guard in guards if guard["tgname"] not in RETIRED_BULK_HOOKS],
        }
    return proof_by_root


async def retire_bulk_abandonment_hooks(connection, policy, before):
    """Only called after fixed writers and direct/inherited privileges are closed."""
    current = await capture_checkpoint_controls(connection, policy, legacy=True)
    _require(current == before, "checkpoint_identity_changed")
    for name in sorted(RETIRED_BULK_HOOKS):
        await connection.execute(
            f"DROP TRIGGER IF EXISTS {storage_identifier(name)} ON {storage_relation(policy.schema, BULK_ROOT)}"
        )
    _require(await capture_checkpoint_controls(connection, policy) == before, "checkpoint_identity_changed")
