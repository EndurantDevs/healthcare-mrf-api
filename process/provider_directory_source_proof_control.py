# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fixed native custody and application checks for batch proof controls."""

from db.connection import ConnectionProxy, Database
from db.plan_release_storage import storage_identifier, storage_relation
from process.plan_release_storage_catalog import CATALOG_SQL
from process.plan_release_storage_custody import storage_roles
from process.provider_directory_generic_source_policy import bound_publisher, source_copy_session
from process.provider_directory_generic_source_storage import (
    HEADER,
    _json,
    _require,
    _validate_binding,
    require_private_source_readers_closed,
)
from process.provider_directory_projection_stage import _copy_driver
from process.provider_directory_source_storage_binding import (
    require_storage_schema,
    verify_isolated_source_storage_coordinates,
)

PROOF_ROOT = "provider_directory_dataset_proof_shard"
_COLUMNS = (
    ("dataset_id", "character varying(96)"),
    ("shard_id", "character varying(64)"),
    ("endpoint_id", "character varying(64)"),
    ("acquisition_root_run_id", "character varying(64)"),
    ("source_ids_json", "jsonb"),
    ("resource_count", "bigint"),
    ("resource_counts_json", "jsonb"),
    ("first_identity_json", "jsonb"),
    ("last_identity_json", "jsonb"),
    ("input_sha256", "character varying(64)"),
    ("artifact_sha256", "character varying(64)"),
    ("artifact_byte_count", "bigint"),
    ("payload_bytes", "bytea"),
    ("created_at", "timestamp with time zone"),
)
_CHECKS = {
    PROOF_ROOT + "_identity_check": "CHECK ((((shard_id)::text ~ '^[0-9a-f]{64}$'::text) AND "
    "((input_sha256)::text ~ '^[0-9a-f]{64}$'::text) AND ((artifact_sha256)::text ~ '^[0-9a-f]{64}$'::text) "
    "AND ((endpoint_id)::text <> ''::text) AND ((acquisition_root_run_id)::text <> ''::text) "
    "AND (resource_count > 0) AND (artifact_byte_count > 0)))",
    PROOF_ROOT + "_json_check": "CHECK (((jsonb_typeof(source_ids_json) = 'array'::text) AND "
    "(jsonb_array_length(source_ids_json) > 0) AND (jsonb_typeof(resource_counts_json) = 'object'::text) "
    "AND (jsonb_typeof(first_identity_json) = 'array'::text) AND (jsonb_typeof(last_identity_json) = 'array'::text)))",
}
PROOF_HOOKS = {
    **{
        f"pd_subset_abandonment_proof_{action}_guard": (event, "guard_provider_directory_subset_abandonment_child")
        for action, event in (("insert", 4), ("update", 16), ("delete", 8), ("truncate", 34))
    },
    "pd_trr_proof_row": (31, "guard_provider_directory_terminal_root_retirement_child"),
    "pd_trr_proof_truncate": (34, "guard_provider_directory_terminal_root_retirement_child"),
}


async def require_proof_executor(executor, schema):
    """Refuse absent, inherited or substituted transactions before catalog IO."""
    publisher = bound_publisher()
    session = source_copy_session()
    if isinstance(executor, Database):
        binding = executor._transaction_binding()
        is_original = binding is not None and binding.session is session
    else:
        is_original = isinstance(executor, ConnectionProxy) and executor._connection is session
    binding = getattr(publisher, "storage_binding", None)
    expected_schema = publisher.policy.schema if binding is None else binding.catalog_schema
    _require(is_original and schema == expected_schema, "proof_transaction_changed")
    _require(await _copy_driver(session) is publisher.connection, "proof_transaction_changed")
    if binding is None:
        await verify_proof_control(publisher.connection, publisher.policy)
    else:
        require_storage_schema(publisher, schema)
        await verify_proof_control(publisher.connection, publisher.policy, storage_binding=binding)
    return publisher


async def verify_proof_control(connection, policy, *, storage_binding=None):
    """Preserve legacy custody; private controls use parent fences without foreign keys."""
    if storage_binding is not None:
        await verify_isolated_source_storage_coordinates(connection, policy, storage_binding)
    owner, readers = await storage_roles(connection, policy.schema, **policy.role_arguments())
    proof_rows = await connection.fetch(
        CATALOG_SQL, [storage_relation(policy.schema, PROOF_ROOT)], owner, readers if storage_binding is None else []
    )
    _require(len(proof_rows) == 1, "proof_control_missing")
    proof_by_field = proof_rows[0]
    _require(
        proof_by_field["relname"] == PROOF_ROOT
        and proof_by_field["nspname"] == policy.schema
        and proof_by_field["relowner"] == owner
        and proof_by_field["relkind"] == "r"
        and proof_by_field["relpersistence"] == "p"
        and not proof_by_field["parents"]
        and not proof_by_field["relrowsecurity"]
        and not proof_by_field["relforcerowsecurity"]
        and proof_by_field["native_checks"]
        and proof_by_field["direct_acl_closed"]
        and proof_by_field["readers_closed"],
        "proof_control_custody_changed",
    )
    _require_proof_layout(proof_by_field, isolated=storage_binding is not None)
    if storage_binding is None:
        await _require_batch_foreign_key(connection, policy.schema, proof_by_field["oid"])
    else:
        _require(proof_by_field["inert"], "proof_control_hooks_changed")
        await require_private_source_readers_closed(connection, [proof_by_field["oid"]], readers)
    return proof_by_field


async def retire_proof_control_hooks(connection, policy):
    """Remove only the authenticated six obsolete hooks after complete write closure."""
    hooks = await connection.fetch(
        "SELECT t.tgname,t.tgtype,t.tgenabled::text,p.proname,n.nspname FROM pg_trigger t "
        "JOIN pg_proc p ON p.oid=t.tgfoid JOIN pg_namespace n ON n.oid=p.pronamespace "
        "WHERE t.tgrelid=to_regclass($1) AND NOT t.tgisinternal",
        storage_relation(policy.schema, PROOF_ROOT),
    )
    _require(
        not hooks
        or len(hooks) == len(PROOF_HOOKS)
        and {hook["tgname"]: (hook["tgtype"], hook["proname"]) for hook in hooks} == PROOF_HOOKS
        and all(hook["nspname"] == policy.schema and hook["tgenabled"] == "A" for hook in hooks),
        "proof_control_legacy_hooks_changed",
    )
    for hook in hooks:
        await connection.execute(
            f"DROP TRIGGER {storage_identifier(hook['tgname'])} ON {storage_relation(policy.schema, PROOF_ROOT)}"
        )


def _require_proof_layout(proof_by_field, *, isolated=False):
    columns = _json(proof_by_field["columns"])
    _require(
        columns == [[name, kind, True, "now()" if name == "created_at" else None] for name, kind in _COLUMNS],
        "proof_control_columns_changed",
    )
    constraints = _json(proof_by_field["constraints"])
    expected_types_by_name = {
        PROOF_ROOT + "_pkey": "p",
        **{name: "c" for name in _CHECKS},
    }
    if not isolated:
        expected_types_by_name[PROOF_ROOT + "_dataset_fkey"] = "f"
    _require(
        {entry_by_field["name"]: entry_by_field["type"] for entry_by_field in constraints} == expected_types_by_name
        and all(
            entry_by_field["valid"]
            and (entry_by_field["type"] != "c" or entry_by_field["definition"] == _CHECKS[entry_by_field["name"]])
            for entry_by_field in constraints
        )
        and next(entry_by_field["columns"] for entry_by_field in constraints if entry_by_field["type"] == "p")
        == [1, 2],
        "proof_control_constraints_changed",
    )
    indexes = _json(proof_by_field["indexes"])
    _require(
        all(
            entry_by_field["valid"]
            and entry_by_field["native"]
            and entry_by_field["method"] == "btree"
            and entry_by_field["predicate"] is None
            and entry_by_field["expression"] is None
            for entry_by_field in indexes
        )
        and {("dataset_id", "shard_id"), ("dataset_id", "acquisition_root_run_id", "shard_id")}
        <= {tuple(entry_by_field["keys"]) for entry_by_field in indexes},
        "proof_control_indexes_changed",
    )


async def _require_batch_foreign_key(connection, schema, proof_oid):
    hooks = await connection.fetch(
        "SELECT t.tgtype,t.tgenabled::text,t.tgisinternal,p.proname,pn.nspname AS function_schema,"
        "k.conname,k.contype::text,k.convalidated,k.condeferrable,k.confupdtype::text,k.confdeltype::text,"
        "k.confmatchtype::text,k.conkey::smallint[],k.confrelid=to_regclass($2) AS exact_parent,"
        "ARRAY(SELECT a.attname::text FROM unnest(k.confkey) WITH ORDINALITY x(num,pos) JOIN pg_attribute a "
        "ON a.attrelid=k.confrelid AND a.attnum=x.num ORDER BY x.pos) AS target_columns,"
        "EXISTS(SELECT FROM pg_rewrite WHERE ev_class=t.tgrelid) AS has_rules "
        "FROM pg_trigger t LEFT JOIN pg_constraint k ON k.oid=t.tgconstraint "
        "JOIN pg_proc p ON p.oid=t.tgfoid JOIN pg_namespace pn ON pn.oid=p.pronamespace "
        "WHERE t.tgrelid=$1",
        proof_oid,
        storage_relation(schema, HEADER),
    )
    _require(
        len(hooks) == 2
        and {(hook["tgtype"], hook["proname"]) for hook in hooks}
        == {(5, "RI_FKey_check_ins"), (17, "RI_FKey_check_upd")}
        and all(
            hook["tgisinternal"]
            and hook["tgenabled"] == "O"
            and hook["function_schema"] == "pg_catalog"
            and hook["conname"] == PROOF_ROOT + "_dataset_fkey"
            and hook["contype"] == "f"
            and hook["convalidated"]
            and not hook["condeferrable"]
            and hook["confupdtype"] == "a"
            and hook["confdeltype"] == "c"
            and hook["confmatchtype"] == "s"
            and hook["conkey"] == [1]
            and hook["exact_parent"]
            and hook["target_columns"] == ["dataset_id"]
            and not hook["has_rules"]
            for hook in hooks
        ),
        "proof_control_hooks_changed",
    )


async def lock_proof_parent(publisher, dataset_id, *, acquiring):
    """One native parent lock fences the whole batch against terminal transitions."""
    _require(type(dataset_id) is str and 1 <= len(dataset_id) <= 96, "proof_dataset_invalid")
    parent_by_field = await publisher.connection.fetchrow(
        f"SELECT dataset_id,endpoint_id,acquisition_root_run_id,status,is_current,publication_metadata_json,"
        f"candidate_storage_json FROM {storage_relation(publisher.policy.schema, HEADER)} WHERE dataset_id=$1 FOR SHARE",
        dataset_id,
    )
    _require(
        parent_by_field is not None
        and parent_by_field["status"] not in {"acquisition_abandoned", "acquisition_retired"},
        "proof_parent_immutable",
    )
    if acquiring:
        _require(
            parent_by_field["status"] == "acquiring" and parent_by_field["is_current"] is False, "proof_parent_sealed"
        )
    _require_proof_identity(parent_by_field, dataset_id, acquiring=acquiring)
    return dict(parent_by_field)


def _require_proof_identity(parent_by_field, dataset_id, *, acquiring):
    binding = _json(parent_by_field["candidate_storage_json"])
    metadata = _json(parent_by_field["publication_metadata_json"])
    if binding is None:
        _require(
            not acquiring
            and parent_by_field["status"]
            in {
                "validated",
                "published",
                "superseded",
                "failed",
                "incomplete",
                "verification_baseline",
                "verification_mismatch",
            },
            "proof_binding_required",
        )
        return
    _require(
        type(binding) is dict and type(binding.get("identity")) is dict and type(metadata) is dict,
        "proof_identity_changed",
    )
    identity = binding["identity"]
    _validate_binding(
        binding, identity, phases=("loading",) if acquiring else ("loading", "prepared", "published", "disposed")
    )
    _require(
        identity["dataset_id"] == dataset_id
        and identity["endpoint_id"] == parent_by_field["endpoint_id"]
        and identity["acquisition_root_run_id"] == parent_by_field["acquisition_root_run_id"]
        and all(metadata.get(key) == expected_value for key, expected_value in identity["metadata"].items()),
        "proof_identity_changed",
    )
