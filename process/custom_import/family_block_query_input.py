# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Stage only the typed query cells still needed after block encoding."""

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    Column,
    Integer,
    LargeBinary,
    MetaData,
    Table,
    Text,
    and_,
    func,
    select,
)

from process.custom_import.family_block_scalar_layout import append_scalar_columns, query_scalar_fields
from process.custom_import.storage_layout import snapshot_schema


def query_input_table(family_id, layout):
    """Define one candidate-only relation that is discarded before output closes."""
    definition = layout.definition
    collection = definition.query.child_collection
    if collection is None:
        raise ValueError("compact query input requires a child collection")
    table = Table(
        "custom_import_block_query_input",
        MetaData(schema=snapshot_schema(family_id)),
        Column("child_revision_id", BigInteger, primary_key=True, autoincrement=False),
        Column("family_revision_id", BigInteger, nullable=False),
        Column("collection_slot", Integer, nullable=False),
        Column("child_key_sha256", LargeBinary(32), nullable=False),
        Column("canonical_child_key", Text(collation="C"), nullable=False),
        CheckConstraint(
            "child_revision_id > 0 AND family_revision_id > 0 AND octet_length(child_key_sha256)=32 AND "
            f"collection_slot={layout.collection_slots[collection]} AND octet_length(canonical_child_key) BETWEEN 1 AND 8192"
        ),
        prefixes=["UNLOGGED"],
    )
    append_scalar_columns(table, query_scalar_fields(definition, collection))
    return table


def write_query_dictionary(dictionary, source):
    """Assign the existing bytewise hash ranks and canonical collision variants."""
    keys = select(source.c.child_key_sha256, source.c.canonical_child_key).distinct().subquery()
    ranks = select(
        func.dense_rank().over(order_by=keys.c.child_key_sha256),
        func.row_number().over(partition_by=keys.c.child_key_sha256, order_by=keys.c.canonical_child_key.collate("C")),
        keys.c.child_key_sha256,
        keys.c.canonical_child_key,
    )
    return dictionary.insert().from_select(tuple(dictionary.c.keys()), ranks, include_defaults=False)


def write_query_records(target, dictionary, source):
    """Write serving query records using exact original IDs and dictionary keys."""
    columns_by_name = {"key_rank": dictionary.c.key_rank, "key_variant": dictionary.c.key_variant}
    rows = select(
        *(columns_by_name[name] if name in columns_by_name else source.c[name] for name in target.c.keys())
    ).select_from(
        source.join(
            dictionary,
            and_(
                dictionary.c.child_key_sha256 == source.c.child_key_sha256,
                dictionary.c.canonical_child_key == source.c.canonical_child_key,
            ),
        )
    )
    return target.insert().from_select(tuple(target.c.keys()), rows, include_defaults=False)
