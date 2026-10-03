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

    collection_slot = context.collection_slots_by_name[context.definition.query.child_collection]
    statement = (
        select(CustomImportChildRevision.child_revision_id)
        .select_from(CustomImportFamilyChild)
        .join(
            CustomImportChildRevision,
            and_(
                CustomImportChildRevision.child_revision_id == CustomImportFamilyChild.child_revision_id,
                CustomImportChildRevision.dataset_id == CustomImportFamilyChild.dataset_id,
                CustomImportChildRevision.schema_revision_id == CustomImportFamilyChild.schema_revision_id,
                CustomImportChildRevision.root_record_id == CustomImportFamilyChild.root_record_id,
                CustomImportChildRevision.collection_slot == CustomImportFamilyChild.collection_slot,
            ),
        )
        .where(
            CustomImportFamilyChild.family_revision_id == CustomImportWinner.family_revision_id,
            CustomImportFamilyChild.dataset_id == context.target.dataset_id,
            CustomImportFamilyChild.schema_revision_id == context.target.schema_revision_id,
            CustomImportFamilyChild.root_record_id == CustomImportFamilyRevision.root_record_id,
            CustomImportFamilyChild.collection_slot == collection_slot,
        )
        .correlate(CustomImportWinner, CustomImportFamilyRevision)
    )
    for predicate in predicates:
        conditions = core._child_scalar_conditions(
            predicate.field, CustomImportChildRevision.child_revision_id, context
        )
        statement = statement.where(
            core._scalar_predicate(CustomImportChildScalar, predicate, conditions).correlate(
                CustomImportChildRevision, CustomImportFamilyRevision
            )
        )
    return statement


def child_order_expression(context, predicates, field):
    """Read one complete-key child's scalar; ambiguous identity fails closed."""

    child_revision_id = matching_child_statement(context, predicates).scalar_subquery()
    conditions = core._child_scalar_conditions(field, child_revision_id, context)
    value_column = getattr(CustomImportChildScalar, core._SCALAR_COLUMNS[field.value_type])
    return (
        select(value_column)
        .where(*conditions, CustomImportChildScalar.value_state == "value")
        .correlate(CustomImportWinner, CustomImportFamilyRevision)
        .scalar_subquery()
    )
