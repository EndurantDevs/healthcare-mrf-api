# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Read-only ownership injection checks against synthetic native VALUES."""

from decimal import Decimal

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import create_async_engine

from db.models.custom_import import (
    CustomImportChildRevision,
    CustomImportChildScalar,
    CustomImportFamilyChild,
    CustomImportFamilyRevision,
    CustomImportGenerationFamily,
    CustomImportRootRevision,
    CustomImportRootScalar,
    CustomImportWinner,
)
from process.custom_import.query_projection_sources import child_hash_collision_statement, child_source, root_source
from tests.custom_import_postgres_support import _database_url
from tests.test_custom_import_query_projection_sources import _context, _definition, _hash_source, _owner_rows


@pytest.fixture
async def values_connection():
    engine = create_async_engine(_database_url())
    try:
        async with engine.connect() as connection:
            yield connection
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_candidate_sources_keep_all_explicit_values_and_null_nonpositive_children(values_connection):
    context = _context()
    roots, root_values = root_source(context, [10])
    rows = (await values_connection.execute(select(roots).order_by(roots.c.family_revision_id))).all()
    assert [(row.family_revision_id, row.selected_value, row.group_value) for row in rows] == [
        (100, 2024, "segment_a"),
        (101, 2024, "segment_b"),
        (102, 2025, "segment_a"),
    ]
    children, child_values = child_source(context, roots, 1)
    statement = select(children.c.child_revision_id, child_values["quality"], child_values["quantity"]).order_by(
        children.c.child_revision_id
    )
    assert (await values_connection.execute(statement)).all() == [
        (400, None, 0),
        (401, Decimal("4"), -1),
        (402, Decimal("5"), 2),
    ]
    for entity_ids in ([20], []):
        excluded, _ = root_source(context, entity_ids)
        assert (await values_connection.execute(select(excluded))).all() == []
    assert set(root_values) == {"score", "weight", "period", "segment"}


_ROOT_OWNERS = [
    (CustomImportWinner, field)
    for field in (
        "generation_id",
        "dataset_id",
        "definition_revision_id",
        "schema_revision_id",
        "profile_slot",
        "entity_binding_id",
        "family_revision_id",
        "context_collection_slot",
        "context_child_revision_id",
    )
] + [
    (model, field)
    for model, fields in (
        (CustomImportGenerationFamily, ("generation_id", "dataset_id", "family_revision_id", "root_record_id")),
        (
            CustomImportFamilyRevision,
            (
                "family_revision_id",
                "dataset_id",
                "schema_revision_id",
                "root_record_id",
                "root_revision_id",
                "entity_binding_id",
            ),
        ),
        (CustomImportRootRevision, ("root_revision_id", "dataset_id", "schema_revision_id", "root_record_id")),
    )
    for field in fields
]


@pytest.mark.asyncio
@pytest.mark.parametrize(("model", "field"), _ROOT_OWNERS, ids=lambda value: getattr(value, "__name__", value))
async def test_root_sources_exclude_each_mismatched_winner_family_and_root_owner(model, field, values_connection):
    definition = _definition()
    rows = _owner_rows(definition)
    original = rows[model][0][field]
    rows[model][0][field] = 999 if original is None else original + 1000
    roots, _ = root_source(_context(definition, rows), [10])
    assert set((await values_connection.execute(select(roots.c.family_revision_id))).scalars()) == {101, 102}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field", ("root_revision_id", "dataset_id", "schema_revision_id", "root_record_id", "field_slot")
)
async def test_root_source_ignores_colliding_scalar_rows_from_other_owners(field, values_connection):
    definition = _definition()
    rows = _owner_rows(definition)
    slot = definition.fields_by_id["score"].field_slot
    rogue = next(dict(row) for row in rows[CustomImportRootScalar] if row["field_slot"] == slot)
    rogue.update({field: rogue[field] + 1000, "decimal_value": Decimal("999")})
    rows[CustomImportRootScalar].append(rogue)
    roots, metrics = root_source(_context(definition, rows), [10])
    statement = select(roots.c.family_revision_id, metrics["score"]).order_by(roots.c.family_revision_id)
    assert (await values_connection.execute(statement)).all() == [
        (100, Decimal("1")),
        (101, Decimal("2")),
        (102, Decimal("3")),
    ]


_CHILD_OWNERS = [
    (model, field)
    for model, fields in (
        (
            CustomImportFamilyChild,
            (
                "family_revision_id",
                "child_revision_id",
                "dataset_id",
                "schema_revision_id",
                "root_record_id",
                "collection_slot",
            ),
        ),
        (
            CustomImportChildRevision,
            ("child_revision_id", "dataset_id", "schema_revision_id", "root_record_id", "collection_slot"),
        ),
    )
    for field in fields
]


@pytest.mark.asyncio
@pytest.mark.parametrize(("model", "field"), _CHILD_OWNERS, ids=lambda value: getattr(value, "__name__", value))
async def test_child_sources_exclude_each_mismatched_membership_and_revision_owner(model, field, values_connection):
    definition = _definition()
    rows = _owner_rows(definition)
    rows[model][0][field] += 1000
    context = _context(definition, rows)
    roots, _ = root_source(context, [10])
    children, _ = child_source(context, roots, 1)
    assert set((await values_connection.execute(select(children.c.child_revision_id))).scalars()) == {401, 402}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field",
    ("child_revision_id", "dataset_id", "schema_revision_id", "root_record_id", "collection_slot", "field_slot"),
)
async def test_child_source_ignores_colliding_scalar_rows_from_other_owners(field, values_connection):
    definition = _definition()
    rows = _owner_rows(definition)
    slot = definition.fields_by_id["quantity"].field_slot
    rogue = next(dict(row) for row in rows[CustomImportChildScalar] if row["field_slot"] == slot)
    rogue.update({field: rogue[field] + 1000, "integer_value": 999})
    rows[CustomImportChildScalar].append(rogue)
    context = _context(definition, rows)
    roots, _ = root_source(context, [10])
    children, metrics = child_source(context, roots, 1)
    statement = select(children.c.child_revision_id, metrics["quantity"]).order_by(children.c.child_revision_id)
    assert (await values_connection.execute(statement)).all() == [(400, 0), (401, -1), (402, 2)]


@pytest.mark.asyncio
async def test_configured_ids_matching_envelope_names_keep_their_own_values(values_connection):
    context = _context(_definition(reserved=True))
    roots, root_values = root_source(context, [10])
    statement = select(roots.c.selected_value, root_values["selected_value"], root_values["group_value"]).order_by(
        roots.c.family_revision_id
    )
    assert (await values_connection.execute(statement)).all() == [
        (2024, Decimal("1"), 2),
        (2024, Decimal("2"), 2),
        (2025, Decimal("3"), 2),
    ]
    children, child_values = child_source(context, roots, 1)
    statement = select(
        children.c.entity_binding_id, child_values["entity_binding_id"], child_values["canonical_child_key"]
    ).order_by(children.c.child_revision_id)
    assert (await values_connection.execute(statement)).all() == [
        (10, None, 0),
        (10, Decimal("4"), -1),
        (10, Decimal("5"), 2),
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("collision", ("hash", "canonical", "none", "other_entity", "other_selection"))
async def test_child_key_hash_consistency_checks_both_directions_at_the_exact_context(collision, values_connection):
    first = (10, 2025, '["first"]', b"a" * 32)
    second = {
        "hash": (10, 2025, '["second"]', b"a" * 32),
        "canonical": (10, 2025, '["first"]', b"b" * 32),
        "none": first,
        "other_entity": (20, 2025, '["second"]', b"a" * 32),
        "other_selection": (10, 2024, '["first"]', b"b" * 32),
    }[collision]
    statement = child_hash_collision_statement(_hash_source([first, second]))
    assert (await values_connection.execute(statement)).all() == (
        [(10, 2025)] if collision in {"hash", "canonical"} else []
    )
