# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Name-only creation and closed inventory for compact source intake."""

import re
from importlib import import_module
from uuid import uuid4

from db.plan_release_storage import storage_relation
from process import provider_directory_compact_payload as compact_payload

TABLE_SUFFIXES = {
    "resource_input": "ri",
    "witness_input": "wi",
    "resource_aliases": "ra",
    "resource_reference_sets": "rs",
    "resource_body_blocks": "rb",
}
SHARED_BODY_ROOT = "resource_shared_bodies"
SHARED_BODY_INDEX_ROOT = "resource_shared_body_index"
REFERENCE_TOKENS_ROOT = "resource_reference_tokens"


def require_source_identity(identity, *, payload_format=compact_payload.FORMAT):
    """Require an admitted ordinary named source and a supported layout."""
    from process import provider_directory_generic_source_storage as storage

    storage._require_named_source_identity(identity)
    directory = import_module("process.provider_directory_fhir")
    source_ids = identity["metadata"]["source_ids"]
    storage._require(
        payload_format in (compact_payload.SHARED_BODY_FORMAT, compact_payload.COMPACT_V2_FORMAT)
        or (
            payload_format == compact_payload.FORMAT
            and source_ids == [directory._stable_source_id(directory.anthem.seed_rows()[0])]
        ),
        "compact_source_unsupported",
    )
    storage._require(not storage.is_role_repeat_source(source_ids), "compact_role_merge_unsupported")


def validate_tables(binding):
    """Validate a closed logical loading inventory without physical attestations."""
    from process import provider_directory_generic_source_storage as storage

    tables = binding["tables"]
    suffixes_by_root = _input_suffixes(binding["payload_format"])
    if binding["phase"] in {"prepared", "ready_for_freeze"}:
        input_roots = set(suffixes_by_root)
        suffixes_by_root.update(resource_output_aliases="oa", **storage._SUFFIXES)
        suffixes_by_root = {
            root: suffix
            for root, suffix in suffixes_by_root.items()
            if root in {*input_roots, "resource_output_aliases", *storage.RELATIONS}
        }
    if binding["phase"] == "ready_for_freeze":
        suffixes_by_root.update({root: storage._SUFFIXES[root] for root in storage.HANDOFF_CONTROL_ROOTS})
    expected = set() if binding["phase"] == "disposed" else set(suffixes_by_root)
    storage._require(
        set(tables) == expected and binding["auxiliary"] is None and binding["relationship_sha256"] is None,
        "compact_tables_changed",
    )
    names = []
    for root, table in tables.items():
        storage._require(
            type(table) is dict
            and set(table) in ({"name"}, {"schema", "name"})
            and ("schema" not in table or table["schema"] == binding.get("storage", {}).get("storage_schema"))
            and type(table["name"]) is str
            and re.fullmatch(r"pdgs_[0-9a-f]{32}_" + suffixes_by_root[root], table["name"]),
            "compact_table_binding_changed",
        )
        names.append(table["name"])
    storage._require(
        len(set(names)) == len(names) and len({name.rsplit("_", 1)[0] for name in names}) <= 1,
        "compact_table_binding_changed",
    )


def _input_suffixes(payload_format):
    if payload_format == compact_payload.COMPACT_V2_FORMAT:
        return TABLE_SUFFIXES | {
            SHARED_BODY_ROOT: "sb",
            SHARED_BODY_INDEX_ROOT: "si",
            REFERENCE_TOKENS_ROOT: "rt",
        }
    if payload_format == compact_payload.SHARED_BODY_FORMAT:
        return TABLE_SUFFIXES | {SHARED_BODY_ROOT: "sb", REFERENCE_TOKENS_ROOT: "rt"}
    return dict(TABLE_SUFFIXES)


def payload_tables(policy, tables):
    """Resolve codec relations from the validated logical loading inventory."""
    shared_index = tables.get(SHARED_BODY_INDEX_ROOT, {}).get("name")
    return compact_payload.CompactPayloadTables(
        policy.schema,
        tables["resource_aliases"]["name"],
        tables["resource_reference_sets"]["name"],
        tables["resource_body_blocks"]["name"],
        tables.get(SHARED_BODY_ROOT, {}).get("name"),
        tables.get(REFERENCE_TOKENS_ROOT, {}).get("name"),
        shared_body_index=shared_index,
        is_v2=shared_index is not None,
    )


async def create_inputs(connection, policy, *, payload_format=compact_payload.FORMAT, defer_secondary_indexes=False):
    """Create one logical input view and its isolated compact backing relations."""
    from process import provider_directory_generic_source_storage as storage

    prefix = "pdgs_" + uuid4().hex
    tables_by_root = {
        root: {"schema": policy.schema, "name": prefix + "_" + suffix}
        for root, suffix in _input_suffixes(payload_format).items()
    }
    compact = payload_tables(policy, tables_by_root)
    ddl_statements = (
        compact_payload.compact_payload_ddl(compact, defer_secondary_indexes=True)
        if defer_secondary_indexes
        else compact_payload.compact_payload_ddl(compact)
    )
    for statement in ddl_statements:
        await connection.execute(statement)
    roots = ("aliases", "reference_sets", "body_blocks")
    if compact.reference_tokens is not None:
        roots += ("reference_tokens",)
    if compact.shared_bodies is not None:
        roots += ("shared_bodies",)
    if compact.shared_body_index is not None:
        roots += ("shared_body_index",)
    for root in roots:
        await connection.execute(f"ALTER TABLE {compact.relation(root)} SET (autovacuum_enabled=false)")
    aliases = compact.relation("aliases")
    await connection.execute(f"ALTER TABLE {aliases} ALTER COLUMN batch_ordinal ADD GENERATED BY DEFAULT AS IDENTITY")
    await connection.execute(
        f"CREATE VIEW {storage_relation(policy.schema, tables_by_root['resource_input']['name'])} AS "
        + compact_payload.compact_resource_sql(compact, observations=True)
    )
    witness = storage_relation(policy.schema, tables_by_root["witness_input"]["name"])
    await connection.execute(
        f"CREATE TABLE {witness} (LIKE {storage_relation(policy.schema, storage.WITNESS)} INCLUDING STORAGE,"
        "batch_ordinal bigint NOT NULL,row_ordinal integer NOT NULL)" + storage.PRIVATE_HEAP_OPTIONS
    )
    for table in tables_by_root.values():
        await storage._close_grants(connection, policy, table["name"])
    sequence_name = await connection.fetchval(
        "SELECT c.relname FROM pg_class c WHERE c.oid=pg_get_serial_sequence($1,'batch_ordinal')::regclass", aliases
    )
    storage._require(type(sequence_name) is str and bool(sequence_name), "compact_sequence_missing")
    await storage._close_grants(connection, policy, sequence_name, sequence=True)
    return tables_by_root


async def prepare_final_tables(connection, policy, binding):
    """Finish native bounds and restore normal maintenance on compact backing heaps."""
    from process import provider_directory_generic_source_storage as storage

    dataset_id = binding["identity"]["dataset_id"]
    literal = "'" + dataset_id.replace("'", "''") + "'"
    for root, table in binding["tables"].items():
        if root in {"resource_input", storage.RESOURCE}:
            continue
        relation = storage_relation(policy.schema, table["name"])
        await connection.execute(f"ALTER TABLE {relation} ADD CHECK (dataset_id={literal})")
        await connection.execute(f"ALTER TABLE {relation} RESET (autovacuum_enabled)")
