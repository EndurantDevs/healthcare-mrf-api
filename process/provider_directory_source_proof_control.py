# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Named proof layout and application checks for complete source batches."""

from db.plan_release_storage import storage_identifier, storage_relation
from process.provider_directory_generic_source_policy import bound_publisher
from process.provider_directory_generic_source_storage import (
    HEADER,
    _json,
    _require,
    _validate_binding,
)
from process.provider_directory_source_storage_binding import NamedFHIRSourceStorage

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
_PROOF_LAYOUT_SQL = """SELECT
 (SELECT coalesce(jsonb_agg(jsonb_build_array(a.attname,format_type(a.atttypid,a.atttypmod),
    a.attnotnull,pg_get_expr(d.adbin,d.adrelid)) ORDER BY a.attnum),'[]'::jsonb)
  FROM pg_attribute a LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum
  WHERE a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped) AS columns,
 (SELECT coalesce(jsonb_agg(jsonb_build_object('name',conname,'type',contype::text,
    'definition',pg_get_constraintdef(oid),'valid',convalidated,
    'columns',conkey) ORDER BY conname),'[]'::jsonb)
  FROM pg_constraint WHERE conrelid=c.oid AND contype<>'n') AS constraints,
 (SELECT coalesce(jsonb_agg(jsonb_build_object(
   'valid',i.indisvalid AND i.indisready AND i.indislive,'method',am.amname,
   'native',i.indimmediate AND i.indnkeyatts=i.indnatts AND NOT i.indisexclusion AND coalesce((
      SELECT bool_and(op.opcnamespace='pg_catalog'::regnamespace
       AND op.opcname=CASE attribute.atttypid WHEN 'pg_catalog.int4'::regtype THEN 'int4_ops'
                       WHEN 'pg_catalog.varchar'::regtype THEN 'text_ops' ELSE '' END
       AND index_key.flags=0 AND index_key.collation_oid=attribute.attcollation)
      FROM unnest(i.indkey::smallint[],i.indclass::oid[],i.indcollation::oid[],i.indoption::smallint[])
        index_key(attnum,opclass_oid,collation_oid,flags)
      JOIN pg_attribute attribute ON attribute.attrelid=c.oid AND attribute.attnum=index_key.attnum
      JOIN pg_opclass op ON op.oid=index_key.opclass_oid),false),
   'predicate',pg_get_expr(i.indpred,i.indrelid),'expression',pg_get_expr(i.indexprs,i.indrelid),
   'keys',ARRAY(SELECT a.attname FROM unnest(i.indkey::smallint[]) WITH ORDINALITY k(attnum,ordinal)
     LEFT JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum=k.attnum ORDER BY k.ordinal))
   ORDER BY index_class.relname),'[]'::jsonb)
  FROM pg_index i JOIN pg_class index_class ON index_class.oid=i.indexrelid
  JOIN pg_am am ON am.oid=index_class.relam WHERE i.indrelid=c.oid) AS indexes
 FROM pg_class c WHERE c.oid=to_regclass($1)"""


async def require_proof_executor(executor, schema):
    """Select the bound writer transaction; caller objects grant no SQL authority."""
    publisher = bound_publisher()
    binding = getattr(publisher, "storage_binding", None)
    _require(binding is None or type(binding) is NamedFHIRSourceStorage, "storage_changed")
    expected_schema = publisher.policy.schema if binding is None else binding.catalog_schema
    _require(
        publisher.connection.is_in_transaction() is True and schema == expected_schema,
        "proof_transaction_changed",
    )
    if isinstance(binding, NamedFHIRSourceStorage):
        _require(publisher.policy.schema == binding.storage_schema, "storage_changed")
        return publisher
    _require(binding is None, "storage_changed")
    await verify_proof_control(publisher.connection, publisher.policy)
    return publisher


async def verify_proof_control(connection, policy, *, storage_binding=None):
    """Validate the named table's essential layout using ordinary PostgreSQL access."""
    _require(connection.is_in_transaction() is True, "proof_transaction_changed")
    if storage_binding is not None:
        _require(
            type(storage_binding) is NamedFHIRSourceStorage and storage_binding.storage_schema == policy.schema,
            "storage_changed",
        )
    relation = storage_relation(policy.schema, PROOF_ROOT)
    await connection.execute(f"SELECT dataset_id FROM {relation} LIMIT 0")
    proof_by_field = await connection.fetchrow(_PROOF_LAYOUT_SQL, relation)
    _require(proof_by_field is not None, "proof_control_missing")
    _require_proof_layout(proof_by_field, isolated=storage_binding is not None)
    if storage_binding is None:
        await _require_batch_foreign_key(connection, policy.schema, relation)
    return {
        "schema": policy.schema,
        "name": PROOF_ROOT,
        **{field: _json(proof_by_field[field]) for field in ("columns", "constraints", "indexes")},
    }


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


async def _require_batch_foreign_key(connection, schema, proof_relation):
    hooks = await connection.fetch(
        "SELECT t.tgtype,t.tgenabled::text,t.tgisinternal,p.proname,pn.nspname AS function_schema,"
        "k.conname,k.contype::text,k.convalidated,k.condeferrable,k.confupdtype::text,k.confdeltype::text,"
        "k.confmatchtype::text,k.conkey::smallint[],k.confrelid=to_regclass($2) AS exact_parent,"
        "ARRAY(SELECT a.attname::text FROM unnest(k.confkey) WITH ORDINALITY x(num,pos) JOIN pg_attribute a "
        "ON a.attrelid=k.confrelid AND a.attnum=x.num ORDER BY x.pos) AS target_columns,"
        "EXISTS(SELECT FROM pg_rewrite WHERE ev_class=t.tgrelid) AS has_rules "
        "FROM pg_trigger t LEFT JOIN pg_constraint k ON k.oid=t.tgconstraint "
        "JOIN pg_proc p ON p.oid=t.tgfoid JOIN pg_namespace pn ON pn.oid=p.pronamespace "
        "WHERE t.tgrelid=to_regclass($1)",
        proof_relation,
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
