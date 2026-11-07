# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Closed native family-block shapes; no data, authority or database facts cached."""

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType

from sqlalchemy import (
    TIMESTAMP,
    BigInteger,
    Boolean,
    CheckConstraint,
    Column,
    Index,
    Integer,
    LargeBinary,
    MetaData,
    Numeric,
    Table,
    Text,
    text,
)
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateIndex, CreateTable

from process.custom_import.definition import CustomImportDefinition
from process.custom_import.family_block_scalar_layout import append_scalar_columns, query_scalar_fields
from process.custom_import.family_block_storage_codec import StoredFamilyManifest
from process.custom_import.query_projection_values import configured_fields
from process.custom_import.storage_layout import snapshot_schema, snapshot_tables

FAMILY_BLOCK_LAYOUT_CONTRACT = "custom-import/family-block-layout/v1"
_DIALECT = postgresql.dialect()
_COMMON = {
    "root_record": (1, "custom_import_root_record"),
    "entity_binding": (2, "custom_import_entity_binding"),
    "pack": (3, "custom_import_pack"),
    "rejection": (4, "custom_import_rejection"),
    "generation_family": (9, "custom_import_generation_family"),
    "winner": (12, "custom_import_winner"),
}
_NEW = {
    "family": (101, "custom_import_block_family"),
    "fragment": (102, "custom_import_block_fragment"),
    "revision_run": (103, "custom_import_block_revision_run"),
    "key_dictionary": (104, "custom_import_block_key"),
    "query_root": (105, "custom_import_block_query_root"),
    "query_child": (106, "custom_import_block_query_child"),
    "shard_receipt": (107, "custom_import_block_shard"),
    "query_record": (108, "custom_import_block_query_record"),
}
ROLE_SPECS = MappingProxyType(_COMMON | _NEW)
# An aggregate row must not disclose columns unavailable through its old sources.
READ_CAPABILITIES = MappingProxyType(
    {
        **{role: (name,) for role, (_, name) in _COMMON.items()},
        "family": ("custom_import_family_revision", "custom_import_root_revision", "custom_import_root_scalar"),
        "fragment": (
            "custom_import_root_revision",
            "custom_import_child_revision",
            "custom_import_root_scalar",
            "custom_import_child_scalar",
        ),
        "revision_run": ("custom_import_family_child", "custom_import_root_revision"),
        "key_dictionary": ("custom_import_child_revision",),
        "query_root": ("custom_import_root_scalar", "custom_import_winner"),
        "query_child": ("custom_import_child_scalar", "custom_import_winner"),
        "shard_receipt": (),
        "query_record": ("custom_import_family_child", "custom_import_child_revision", "custom_import_child_scalar"),
    }
)


def _big(name, *, primary=False, nullable=False):
    return Column(name, BigInteger, primary_key=primary, nullable=nullable, autoincrement=False)


def _hash(name):
    return Column(name, LargeBinary(32), nullable=False)


def _check_hashes(*names):
    return CheckConstraint(" AND ".join(f"octet_length({name}) = 32" for name in names))


def _family_role_table(metadata, definition):
    role_table = Table(
        _NEW["family"][1],
        metadata,
        _big("family_revision_id", primary=True),
        _big("root_record_id"),
        _big("root_revision_id"),
        _big("entity_binding_id"),
        _hash("family_sha256"),
        _hash("storage_sha256"),
        Column("retained", Boolean, nullable=False),
        _big("child_count"),
        _big("fragment_count"),
        _big("raw_bytes"),
        _big("stored_bytes"),
        _big("membership_count"),
        _check_hashes("family_sha256", "storage_sha256"),
        CheckConstraint(
            "family_revision_id > 0 AND root_record_id > 0 AND root_revision_id > 0 AND "
            "entity_binding_id > 0 AND child_count >= 0 AND fragment_count > 0 AND "
            "raw_bytes > 0 AND stored_bytes > 0 AND membership_count > 0"
        ),
    )
    append_scalar_columns(role_table, query_scalar_fields(definition))
    return role_table


def _fragment_role_table(metadata):
    role_table = Table(
        _NEW["fragment"][1],
        metadata,
        _big("family_revision_id", primary=True),
        _big("fragment_ordinal", primary=True),
        _big("raw_bytes"),
        Column("encoded_fragment", LargeBinary, nullable=False),
        _big("first_record_ordinal", nullable=True),
        _big("starting_record_count"),
        _big("first_record_offset", nullable=True),
        _big("continuation_bytes"),
        CheckConstraint(
            "family_revision_id > 0 AND fragment_ordinal >= 0 AND raw_bytes > 0 AND "
            "octet_length(encoded_fragment) > 16 AND starting_record_count >= 0 AND "
            "continuation_bytes BETWEEN 0 AND raw_bytes"
        ),
        CheckConstraint(
            "(starting_record_count = 0 AND first_record_ordinal IS NULL AND first_record_offset IS NULL) "
            "OR (starting_record_count > 0 AND first_record_ordinal IS NOT NULL AND first_record_ordinal >= 0 AND "
            "first_record_offset IS NOT NULL AND first_record_offset >= 0 AND first_record_offset < raw_bytes)"
        ),
    )
    return role_table


def _revision_run_table(metadata):
    role_table = Table(
        _NEW["revision_run"][1],
        metadata,
        _big("family_revision_id", primary=True),
        Column("collection_slot", Integer, primary_key=True),
        _big("first_revision_id", primary=True),
        _big("last_revision_id"),
        _big("record_count"),
        _big("first_record_ordinal", nullable=True),
        Column("encoded_membership", LargeBinary),
        CheckConstraint(
            "family_revision_id > 0 AND collection_slot >= 0 AND first_revision_id > 0 AND "
            "last_revision_id >= first_revision_id AND record_count > 0 AND "
            "((encoded_membership IS NULL AND first_record_ordinal IS NOT NULL AND first_record_ordinal >= 0 AND "
            "last_revision_id - first_revision_id = record_count - 1) OR "
            "(encoded_membership IS NOT NULL AND first_record_ordinal IS NULL AND "
            "octet_length(encoded_membership) > 0))"
        ),
    )
    return role_table


def _key_dictionary_table(metadata):
    role_table = Table(
        _NEW["key_dictionary"][1],
        metadata,
        _big("key_rank", primary=True),
        _big("key_variant", primary=True),
        _hash("child_key_sha256"),
        Column("canonical_child_key", Text(collation="C"), nullable=False),
        CheckConstraint(
            "key_rank > 0 AND key_variant > 0 AND octet_length(child_key_sha256) = 32 AND "
            "octet_length(canonical_child_key) BETWEEN 1 AND 8192"
        ),
    )
    return role_table


def _query_role_tables(metadata, definition, collection_slots):
    tables_by_role = {}
    query_field_ids = {field.field_id for field in configured_fields(definition)}
    for child, role in ((False, "query_root"), (True, "query_child")):
        cells = [
            _big("entity_binding_id", primary=True),
            _big("selected_value", primary=True),
            CheckConstraint("entity_binding_id > 0"),
        ]
        if child:
            cells += [
                _big("key_rank", primary=True),
                _big("key_variant"),
                CheckConstraint("key_rank > 0 AND key_variant > 0"),
            ]
        for slot, field in enumerate(definition.query.derived_fields, 1):
            if field.field_id in query_field_ids and (field.collection is not None) == child:
                name = f"field_{slot}"
                cells.append(Column(name, BigInteger if field.value_type == "integer" else Numeric(31, 12)))
                if field.value_type == "decimal":
                    cells.append(CheckConstraint(f"{name} <> 'NaN'::numeric"))
        tables_by_role[role] = Table(_NEW[role][1], metadata, *cells)
    query_collection = definition.query.child_collection
    tables_by_role["query_record"] = Table(
        _NEW["query_record"][1],
        metadata,
        _big("child_revision_id", primary=True),
        _big("family_revision_id"),
        Column("collection_slot", Integer, nullable=False),
        _big("key_rank"),
        _big("key_variant"),
        CheckConstraint(
            "child_revision_id > 0 AND family_revision_id > 0 AND collection_slot > 0 AND key_rank > 0 AND key_variant > 0"
        ),
        CheckConstraint(f"collection_slot = {collection_slots[query_collection]}" if query_collection else "false"),
    )
    append_scalar_columns(
        tables_by_role["query_record"], query_scalar_fields(definition, query_collection) if query_collection else ()
    )
    return tables_by_role


def _shard_receipt_table(metadata):
    output_roles = tuple(role for role in ROLE_SPECS if role != "shard_receipt")
    keys = ",".join(f"'{role}'" for role in output_roles)
    count_checks = [
        CheckConstraint(
            f"NOT (role_counts ? '{role}') OR (jsonb_typeof(role_counts->'{role}')='number' AND "
            f"(role_counts->>'{role}') ~ '^(0|[1-9][0-9]*)$' AND "
            f"(role_counts->>'{role}')::numeric BETWEEN 0 AND 9223372036854775807)"
        )
        for role in output_roles
    ]
    role_table = Table(
        _NEW["shard_receipt"][1],
        metadata,
        _big("shard_id", primary=True),
        _hash("partition_sha256"),
        _hash("result_sha256"),
        _big("source_record_count"),
        Column("role_counts", postgresql.JSONB, nullable=False),
        Column("completed_at", TIMESTAMP(timezone=True), nullable=False, server_default=text("clock_timestamp()")),
        _check_hashes("partition_sha256", "result_sha256"),
        CheckConstraint("shard_id >= 0 AND source_record_count >= 0 AND jsonb_typeof(role_counts)='object'"),
        CheckConstraint(f"(role_counts - ARRAY[{keys}]::text[])='{{}}'::jsonb"),
        *count_checks,
    )
    return role_table


def _new_tables(metadata, definition, collection_slots):
    """Construct the closed family role schemas with their exact native constraints."""
    tables_by_role = {
        "family": _family_role_table(metadata, definition),
        "fragment": _fragment_role_table(metadata),
        # Inclusive endpoints keep BIGINT_MAX safe and require observed membership.
        "revision_run": _revision_run_table(metadata),
        "key_dictionary": _key_dictionary_table(metadata),
    }
    tables_by_role.update(_query_role_tables(metadata, definition, collection_slots))
    tables_by_role["shard_receipt"] = _shard_receipt_table(metadata)
    return tables_by_role


@dataclass(frozen=True)
class FamilyBlockLayout:
    """Snapshot constants once; logical-family manifests contain only variable cells."""

    definition: CustomImportDefinition
    collection_slots: Mapping

    def __post_init__(self):
        if type(self.definition) is not CustomImportDefinition:
            raise ValueError("family layout requires a parsed definition")
        slots_by_collection = dict(self.collection_slots)
        if (
            set(slots_by_collection) != set(self.definition.collections_by_name)
            or any(type(slot) is not int or not 0 < slot < 2**31 for slot in slots_by_collection.values())
            or len(set(slots_by_collection.values())) != len(slots_by_collection)
        ):
            raise ValueError("family layout collection slots differ")
        object.__setattr__(self, "collection_slots", MappingProxyType(slots_by_collection))

    @property
    def canonical(self):
        """Return the exact versioned layout descriptor used for custody hashing."""
        return json.dumps(
            {
                "contract": FAMILY_BLOCK_LAYOUT_CONTRACT,
                "definition_sha256": self.definition.digest,
                "collection_slots": dict(self.collection_slots),
                "roles": {role: list(spec) for role, spec in ROLE_SPECS.items()},
            },
            sort_keys=True,
            separators=(",", ":"),
        )

    @property
    def digest(self):
        """Hash the canonical layout descriptor without changing its encoding."""
        return hashlib.sha256(self.canonical.encode()).digest()

    @property
    def comment(self):
        """Encode the canonical layout descriptor for native catalog custody."""
        return json.dumps(
            {"contract": FAMILY_BLOCK_LAYOUT_CONTRACT, "layout_sha256": self.digest.hex()},
            sort_keys=True,
            separators=(",", ":"),
        )

    def tables(self, family_id):
        """Build the complete role table map for one exact family identity."""
        metadata = MetaData(schema=snapshot_schema(family_id))
        common = snapshot_tables(family_id)
        tables_by_role = {role: common[name].to_metadata(metadata) for role, (_, name) in _COMMON.items()}
        tables_by_role.update(_new_tables(metadata, self.definition, self.collection_slots))
        # Common serving indexes are prepared after COPY, never built by LIKE.
        for table in tables_by_role.values():
            table.indexes.clear()
        return MappingProxyType({role: tables_by_role[role] for role in ROLE_SPECS})

    def manifest(self, row):
        """Reconstruct only after the caller has proved this closed layout binding."""
        return StoredFamilyManifest(
            self.definition.digest,
            bytes(row["family_sha256"]),
            bytes(row["storage_sha256"]),
            row["retained"],
            row["child_count"],
            row["fragment_count"],
            row["raw_bytes"],
            row["stored_bytes"],
            row["membership_count"],
        )


def load_statements(family_id, layout):
    """No schema creation or ACL mutation; the owner creates and registers atomically."""
    statements = []
    for table in layout.tables(family_id).values():
        # IDs are allocated from the existing canonical sequences by the owner.
        # Explicit COPY columns never ask local BIGSERIALs to allocate another space.
        for column in table.c:
            column.autoincrement = False
        statements.append(
            str(CreateTable(table).compile(dialect=_DIALECT)).replace("CREATE TABLE", "CREATE UNLOGGED TABLE", 1)
        )
    return tuple(statements)


def index_specifications(family_id, layout):
    """Return the closed index definitions required by each family role."""
    tables = layout.tables(family_id)
    specs = [
        ("fragment", "record_lookup", ("family_revision_id", "first_record_ordinal")),
        ("family", "root_lookup", ("root_revision_id",)),
        ("query_record", "family_key_lookup", ("family_revision_id", "collection_slot", "key_rank")),
        ("entity_binding", "canonical_order", ("dataset_id", "adapter_id", "canonical_value", "entity_binding_id")),
    ]
    for role in ("query_root", "query_child"):
        keys = ("selected_value", "key_rank") if role == "query_child" else ("selected_value",)
        specs.extend(
            (role, column.name, (*keys, column.name, "entity_binding_id"))
            for column in tables[role].c
            if column.name.startswith("field_")
        )
    return tuple((role, f"{tables[role].name}_{suffix}_idx", names) for role, suffix, names in specs)


def index_statements(family_id, layout):
    """Render deterministic index DDL for the exact family role tables."""
    tables = layout.tables(family_id)
    index_statements = []
    for role, name, names in index_specifications(family_id, layout):
        table = tables[role]
        columns = [table.c[name].collate("C") if name == "canonical_value" else table.c[name] for name in names]
        index_statements.append(str(CreateIndex(Index(name, *columns)).compile(dialect=_DIALECT)))
    return tuple(index_statements)


def copy_columns(table):
    """The whole physical row is explicit, including provenance and sparse evidence."""
    return tuple(table.c.keys())
