# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Typed input relations from finalized candidate winners, before generation seal."""

import hashlib
from dataclasses import dataclass
from typing import Mapping

from sqlalchemy import and_, case, func, inspect, select, union_all

from db.models.custom_import import (
    CustomImportChildRevision,
    CustomImportChildScalar,
    CustomImportFamilyChild,
    CustomImportFamilyRevision,
    CustomImportRootScalar,
    CustomImportWinner,
)
from db.models.custom_import_query_projection import QUERY_PROJECTION_CONTRACT
from process.custom_import.definition import CustomImportDefinition, canonical_json
from process.custom_import.query_projection_values import configured_fields


@dataclass(frozen=True)
class ProjectionSource:
    """An internal, already fenced candidate binding; it grants no write authority."""

    dataset_id: int
    definition_revision_id: int
    schema_revision_id: int
    generation_id: int
    profile_slot: int
    definition: CustomImportDefinition
    storage_models: Mapping

    def model(self, canonical):
        """Resolve one finalized candidate relation from its protected binding."""

        return self.storage_models[canonical]


def capability_sha256(definition):
    """Bind the capability to the full saved definition and enabled declarations."""

    capability_by_name = {
        "definition_sha256": definition.digest,
        "fields": [field.field_id for field in configured_fields(definition)],
    }
    return hashlib.sha256(
        QUERY_PROJECTION_CONTRACT.encode("ascii") + b"\0" + canonical_json(capability_by_name).encode("utf-8")
    ).digest()


def field_slots(definition):
    """Slots are declaration positions within one exact immutable definition."""

    return {field.field_id: slot for slot, field in enumerate(definition.query.derived_fields, 1)}


def _source_fields(definition, *, child):
    requested_ids = set()
    for field in configured_fields(definition):
        if (field.collection is not None) == child:
            requested_ids.add(field.source_field_id)
            if field.weight_field_id is not None:
                requested_ids.add(field.weight_field_id)
    return tuple(field for field in definition.fields if field.field_id in requested_ids)


def _scalar_values(statement, source, fields, scalar_model, *, child):
    values_by_field = {}
    for field in fields:
        scalar = inspect(scalar_model).selectable.alias(f"projection_{'child' if child else 'root'}_{field.field_slot}")
        ownership_terms = [
            scalar.c.dataset_id == source.c.dataset_id,
            scalar.c.schema_revision_id == source.c.schema_revision_id,
            scalar.c.root_record_id == source.c.root_record_id,
            scalar.c.field_slot == field.field_slot,
        ]
        if child:
            ownership_terms.extend(
                (
                    scalar.c.child_revision_id == source.c.child_revision_id,
                    scalar.c.collection_slot == source.c.collection_slot,
                )
            )
        else:
            ownership_terms.append(scalar.c.root_revision_id == source.c.root_revision_id)
        statement = statement.outerjoin(scalar, and_(*ownership_terms))
        value_column = {"integer": "integer_value", "decimal": "decimal_value", "string": "string_value"}[
            field.value_type
        ]
        values_by_field[field.field_id] = case((scalar.c.value_state == "value", scalar.c[value_column]))
    return statement, values_by_field


def _value_relation(statement, values_by_field, name):
    relation = statement.add_columns(
        *(value.label(f"field_{index}") for index, value in enumerate(values_by_field.values()))
    ).cte(name)
    return relation, {field_id: relation.c[f"field_{index}"] for index, field_id in enumerate(values_by_field)}


def root_source(context, entity_binding_ids):
    """Retain every configured group and explicit selection value in one page."""

    from process.custom_import.read_core import _winner_identity_statement

    winner = context.model(CustomImportWinner)
    family = context.model(CustomImportFamilyRevision)
    statement = _winner_identity_statement(context).where(
        winner.generation_id == context.generation_id,
        winner.dataset_id == context.dataset_id,
        winner.definition_revision_id == context.definition_revision_id,
        winner.schema_revision_id == context.schema_revision_id,
        winner.profile_slot == context.profile_slot,
        winner.entity_binding_id.in_(entity_binding_ids),
        winner.context_collection_slot == 0,
        winner.context_child_revision_id.is_(None),
    )
    identities = statement.with_only_columns(
        winner.entity_binding_id,
        family.dataset_id,
        family.schema_revision_id,
        family.family_revision_id,
        family.root_record_id,
        family.root_revision_id,
        maintain_column_froms=True,
    ).cte("projection_winning_families")
    selection = context.definition.query.entity_selection
    fields = dict.fromkeys(
        (
            *_source_fields(context.definition, child=False),
            context.definition.fields_by_id[selection.field_id],
            context.definition.fields_by_id[selection.group_field_id],
        )
    )
    statement, values_by_field = _scalar_values(
        select(identities), identities, fields, context.model(CustomImportRootScalar), child=False
    )
    statement = statement.add_columns(
        values_by_field[selection.field_id].label("selected_value"),
        values_by_field[selection.group_field_id].label("group_value"),
    )
    statement = statement.where(
        values_by_field[selection.group_field_id].in_(selection.group_values),
        values_by_field[selection.field_id].is_not(None),
    )
    return _value_relation(statement, values_by_field, "projection_root_sources")


def child_source(context, roots, collection_slot):
    """Preserve full child identities without discarding null or invalid weights."""

    member = context.model(CustomImportFamilyChild)
    revision = context.model(CustomImportChildRevision)
    statement = (
        select(
            roots.c.entity_binding_id,
            roots.c.selected_value,
            roots.c.group_value,
            roots.c.dataset_id,
            roots.c.schema_revision_id,
            roots.c.root_record_id,
            roots.c.family_revision_id,
            revision.child_revision_id,
            revision.collection_slot,
            revision.canonical_child_key.collate("C").label("canonical_child_key"),
            revision.child_key_sha256,
        )
        .select_from(roots)
        .join(
            member,
            and_(
                member.family_revision_id == roots.c.family_revision_id,
                member.dataset_id == roots.c.dataset_id,
                member.schema_revision_id == roots.c.schema_revision_id,
                member.root_record_id == roots.c.root_record_id,
                member.collection_slot == collection_slot,
            ),
        )
        .join(
            revision,
            and_(
                revision.child_revision_id == member.child_revision_id,
                revision.dataset_id == member.dataset_id,
                revision.schema_revision_id == member.schema_revision_id,
                revision.root_record_id == member.root_record_id,
                revision.collection_slot == member.collection_slot,
            ),
        )
    )
    identities = statement.cte("projection_child_identities")
    statement, values_by_field = _scalar_values(
        select(identities),
        identities,
        _source_fields(context.definition, child=True),
        context.model(CustomImportChildScalar),
        child=True,
    )
    return _value_relation(statement, values_by_field, "projection_child_sources")


def child_hash_collision_statement(source):
    """Reject inconsistent complete keys before a hash can become a storage key."""

    identity = (source.c.entity_binding_id, source.c.selected_value)
    return union_all(
        select(*identity)
        .group_by(*identity, source.c.child_key_sha256)
        .having(func.count(func.distinct(source.c.canonical_child_key)) > 1),
        select(*identity)
        .group_by(*identity, source.c.canonical_child_key)
        .having(func.count(func.distinct(source.c.child_key_sha256)) > 1),
    )
