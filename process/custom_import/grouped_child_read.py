# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""One declared child relation inside an already selected grouped family."""

from __future__ import annotations

from sqlalchemy import and_, select

from db.models.custom_import import (
    CustomImportChildRevision,
    CustomImportChildScalar,
    CustomImportFamilyChild,
    CustomImportFamilyRevision,
    CustomImportWinner,
)
from process.custom_import import read_core as core


def verified_child_key(context, supplied):
    """Grant child semantics only for the exact pinned, eligible declaration."""

    if supplied is None:
        return None
    key = declared_child_key(context)
    expected_descriptor_map = {
        "contract": "custom-import/grouped-child-query/v1",
        "collection": key.collection,
        "key_field_id": key.field_id,
    }
    if type(supplied) is not dict or supplied != expected_descriptor_map:
        raise core.CustomImportReadRequestError("grouped child descriptor does not match the pinned definition")
    return key


def declared_child_key(context):
    """Require the complete projected child identity without selecting a winner."""

    query = context.definition.query
    collection = context.definition.collections_by_name.get(query.child_collection)
    if query.entity_selection is None or collection is None or len(collection.child_key) != 1:
        raise core.CustomImportReadRequestError("grouped child queries require one complete child key")
    field = context.definition.fields_by_id[collection.child_key[0]]
    if (
        field.nullable
        or field.projection_slot is None
        or field.field_id not in query.child_fields
        or context.collection_slots_by_name.get(field.collection) is None
    ):
        raise core.CustomImportReadRequestError("grouped child key must be a required projected query field")
    return field


def verify_child_selectors(selectors, child_key):
    """Keep child identity equality separate from child metric comparisons."""

    if len(selectors) > 1 or any(
        predicate.field.field_id != child_key.field_id
        or predicate.operator != "eq"
        or predicate.value is None
        or (predicate.field.value_type == "integer" and type(predicate.value) is not int)
        for predicate in selectors
    ):
        raise core.CustomImportReadRequestError("grouped child context requires exact complete-key equality")


def matching_child_statement(context, predicates):
    """Correlate every child comparison to one exact selected-family member."""

    winner_model = context.model(CustomImportWinner)
    family_model = context.model(CustomImportFamilyRevision)
    family_child_model = context.model(CustomImportFamilyChild)
    child_model = context.model(CustomImportChildRevision)
    child_scalar_model = context.model(CustomImportChildScalar)

    collection_slot = context.collection_slots_by_name[context.definition.query.child_collection]
    statement = (
        select(child_model.child_revision_id)
        .select_from(family_child_model)
        .join(
            child_model,
            and_(
                child_model.child_revision_id == family_child_model.child_revision_id,
                child_model.dataset_id == family_child_model.dataset_id,
                child_model.schema_revision_id == family_child_model.schema_revision_id,
                child_model.root_record_id == family_child_model.root_record_id,
                child_model.collection_slot == family_child_model.collection_slot,
            ),
        )
        .where(
            family_child_model.family_revision_id == winner_model.family_revision_id,
            family_child_model.dataset_id == context.target.dataset_id,
            family_child_model.schema_revision_id == context.target.schema_revision_id,
            family_child_model.root_record_id == family_model.root_record_id,
            family_child_model.collection_slot == collection_slot,
        )
        .correlate(winner_model, family_model)
    )
    for predicate in predicates:
        conditions = core._child_scalar_conditions(predicate.field, child_model.child_revision_id, context)
        statement = statement.where(
            core._scalar_predicate(child_scalar_model, predicate, conditions).correlate(child_model, family_model)
        )
    return statement


def child_order_expression(context, predicates, field):
    """Read one complete-key child's scalar; ambiguous identity fails closed."""

    winner_model = context.model(CustomImportWinner)
    family_model = context.model(CustomImportFamilyRevision)
    child_scalar_model = context.model(CustomImportChildScalar)

    child_revision_id = matching_child_statement(context, predicates).scalar_subquery()
    conditions = core._child_scalar_conditions(field, child_revision_id, context)
    value_column = getattr(child_scalar_model, core._SCALAR_COLUMNS[field.value_type])
    return (
        select(value_column)
        .where(*conditions, child_scalar_model.value_state == "value")
        .correlate(winner_model, family_model)
        .scalar_subquery()
    )
