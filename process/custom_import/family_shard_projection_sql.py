# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Set-based projection inputs over a caller's exact compact candidate binding.

These relations grant no writer, input-verification or finality authority. The
owner still owes complete ambiguity/collision checks, original-input comparison
and closed candidate verification before accepting any projected output.
"""

from sqlalchemy import and_, case, select

from process.custom_import.family_block_binding import FamilyBlockBinding
from process.custom_import.family_block_layout import ROLE_SPECS
from process.custom_import.family_block_read_sql import _state_and_kind
from process.custom_import.family_block_records import _integer
from process.custom_import.family_block_scalar_layout import query_scalar_fields, scalar_column
from process.custom_import.materialization import GenerationIdentity
from process.custom_import.query_projection_sources import _source_fields, _value_relation
from process.custom_import.query_projection_values import configured_fields


def _projection_profile(binding, generation):
    if type(binding) is not FamilyBlockBinding or type(generation) is not GenerationIdentity:
        raise ValueError("projection requires the exact candidate and generation types")
    _integer(binding.family_id, "projection snapshot")
    for name in ("generation_id", "dataset_id", "definition_revision_id", "schema_revision_id"):
        _integer(getattr(generation, name), name)
    if set(binding.tables_by_role) != set(ROLE_SPECS):
        raise ValueError("projection candidate role census differs")
    definition = binding.layout.definition
    selection = definition.query.entity_selection
    if selection is None:
        raise ValueError("projection requires grouped entity selection")
    return next(
        slot
        for slot, profile in enumerate(definition.selection_profiles, 1)
        if profile.profile_id == selection.by_value_profile
    )


def _raw_values(table, definition, fields, collection=None):
    exposed = query_scalar_fields(definition, collection)
    values_by_field = {}
    for field in fields:
        if field not in exposed or field.projection_slot is None:
            raise ValueError("projection source field is not exposed in its original scope")
        state, override, _kind = _state_and_kind(table, exposed.index(field), field)
        values_by_field[field.field_id] = case((and_(state == 2, override == 0), table.c[scalar_column(field)]))
    return values_by_field


def _winning_families(binding, generation, profile_slot):
    family = binding.tables_by_role["family"]
    member = binding.tables_by_role["generation_family"]
    winner = binding.tables_by_role["winner"]
    owned = winner.join(
        member,
        and_(
            member.c.generation_id == winner.c.generation_id,
            member.c.dataset_id == winner.c.dataset_id,
            member.c.definition_revision_id == winner.c.definition_revision_id,
            member.c.schema_revision_id == winner.c.schema_revision_id,
            member.c.family_revision_id == winner.c.family_revision_id,
        ),
    ).join(
        family,
        and_(
            family.c.family_revision_id == winner.c.family_revision_id,
            family.c.entity_binding_id == winner.c.entity_binding_id,
            family.c.root_record_id == member.c.root_record_id,
        ),
    )
    return (
        select(
            winner.c.entity_binding_id,
            member.c.dataset_id,
            member.c.schema_revision_id,
            family.c.family_revision_id,
            family.c.root_record_id,
            family.c.root_revision_id,
        )
        .select_from(owned)
        .where(
            *(
                getattr(winner.c, name) == getattr(generation, name)
                for name in ("generation_id", "dataset_id", "definition_revision_id", "schema_revision_id")
            ),
            winner.c.profile_slot == profile_slot,
            winner.c.context_collection_slot == 0,
            winner.c.context_child_revision_id.is_(None),
        )
    )


def _root_sources(binding, generation, profile_slot):
    definition, family = binding.layout.definition, binding.tables_by_role["family"]
    selection = definition.query.entity_selection
    fields = dict.fromkeys(
        (
            *_source_fields(definition, child=False),
            definition.fields_by_id[selection.field_id],
            definition.fields_by_id[selection.group_field_id],
        )
    )
    values_by_field = _raw_values(family, definition, fields)
    statement = (
        _winning_families(binding, generation, profile_slot)
        .add_columns(
            values_by_field[selection.field_id].label("selected_value"),
            values_by_field[selection.group_field_id].label("group_value"),
        )
        .where(
            values_by_field[selection.field_id].is_not(None),
            values_by_field[selection.group_field_id].in_(selection.group_values),
        )
    )
    return _value_relation(statement, values_by_field, "block_projection_root_sources")


def _child_sources(binding, roots):
    definition = binding.layout.definition
    collection = definition.query.child_collection
    child = binding.tables_by_role["query_record"]
    dictionary = binding.tables_by_role["key_dictionary"]
    owned = roots.join(
        child,
        and_(
            child.c.family_revision_id == roots.c.family_revision_id,
            child.c.collection_slot == binding.layout.collection_slots[collection],
        ),
    ).join(
        dictionary,
        and_(
            dictionary.c.key_rank == child.c.key_rank,
            dictionary.c.key_variant == child.c.key_variant,
        ),
    )
    statement = select(
        *(
            roots.c[name]
            for name in (
                "entity_binding_id",
                "selected_value",
                "group_value",
                "dataset_id",
                "schema_revision_id",
                "root_record_id",
                "family_revision_id",
            )
        ),
        child.c.child_revision_id,
        child.c.collection_slot,
        dictionary.c.canonical_child_key.collate("C").label("canonical_child_key"),
        dictionary.c.child_key_sha256,
    ).select_from(owned)
    values_by_field = _raw_values(child, definition, _source_fields(definition, child=True), collection)
    return _value_relation(statement, values_by_field, "block_projection_child_sources")


def projection_sources(binding: FamilyBlockBinding, generation: GenerationIdentity):
    """Return original-shaped typed sources without pages, SQL writes or proof.

    Roots also carry required selection/group values when only child fields are
    enabled. Unconfigured child capabilities return ``None`` and an empty map.
    Callers must reuse the complete existing ambiguity/collision checks before
    applying the existing numeric reducers to either relation.
    """
    profile_slot = _projection_profile(binding, generation)
    roots, root_values_by_field = _root_sources(binding, generation, profile_slot)
    if not any(field.collection is not None for field in configured_fields(binding.layout.definition)):
        return roots, root_values_by_field, None, {}
    children, child_values_by_field = _child_sources(binding, roots)
    return roots, root_values_by_field, children, child_values_by_field
