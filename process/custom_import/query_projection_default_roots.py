# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Default-period root identities derived from the complete candidate winners."""

from functools import lru_cache

from sqlalchemy import BigInteger, CheckConstraint, Column, Index, MetaData, Table, Text, and_, select

from db.models.custom_import import (
    CustomImportEntityBinding,
    CustomImportFamilyRevision,
    CustomImportRootScalar,
    CustomImportWinner,
)
from process.custom_import.query_projection_sources import _root_winners, _scalar_values
from process.custom_import.storage_layout import snapshot_schema

DEFAULT_ROOTS = "custom_import_default_score_root"
DEFAULT_ROOTS_ENTITY_INDEX = f"{DEFAULT_ROOTS}_entity_idx"


@lru_cache(maxsize=32)
def default_root_table(family_id, *, schema_name=None):
    """Name the optional relation inside the selected candidate family."""

    table = Table(
        DEFAULT_ROOTS,
        MetaData(schema=snapshot_schema(family_id) if schema_name is None else schema_name),
        Column("family_revision_id", BigInteger, nullable=False),
        Column("root_record_id", BigInteger, nullable=False),
        Column("root_revision_id", BigInteger, nullable=False),
        Column("entity_binding_id", BigInteger, primary_key=True),
        Column("entity_value", Text, nullable=False),
        Column("selected_value", BigInteger, nullable=False),
        Column("group_value", Text, primary_key=True),
        CheckConstraint("entity_binding_id > 0", name=f"{DEFAULT_ROOTS}_identity"),
    )
    Index(DEFAULT_ROOTS_ENTITY_INDEX, table.c.entity_value)
    return table


def _typed_winners(context, entity_ids, profile_slot, fields, name, *, key=None):
    winner = context.model(CustomImportWinner)
    family = context.model(CustomImportFamilyRevision)
    entity = context.model(CustomImportEntityBinding)
    statement = _root_winners(context, entity_ids, profile_slot).join(
        entity,
        and_(
            entity.entity_binding_id == winner.entity_binding_id,
            entity.dataset_id == winner.dataset_id,
            entity.adapter_id == "npi",
        ),
    )
    if key is not None:
        statement = statement.where(winner.context_key_sha256 == key)
    owned = statement.with_only_columns(
        family.family_revision_id,
        family.root_record_id,
        family.root_revision_id,
        winner.entity_binding_id,
        entity.canonical_value.label("entity_value"),
        family.dataset_id,
        family.schema_revision_id,
        maintain_column_froms=True,
    ).cte(name)
    return _scalar_values(select(owned), owned, fields, context.model(CustomImportRootScalar), child=False)


def default_root_source(context, entity_ids):
    """Resolve the exact default first, then retain its complete configured groups."""

    from process.custom_import.grouped_read import _profile_context_digest

    selection = context.definition.query.entity_selection
    fields = context.definition.fields_by_id
    period, group = fields[selection.field_id], fields[selection.group_field_id]
    slots_by_profile_id = {
        profile.profile_id: slot for slot, profile in enumerate(context.definition.selection_profiles, 1)
    }
    helper, helper_values = _typed_winners(
        context,
        entity_ids,
        slots_by_profile_id[selection.default_profile],
        (period,),
        "default_period_winners",
        key=_profile_context_digest(context, selection.default_profile, {}),
    )
    defaults = helper.with_only_columns(
        helper.selected_columns.entity_binding_id,
        helper_values[period.field_id].label("selected_value"),
        maintain_column_froms=True,
    ).subquery("default_period_values")
    panel, panel_values = _typed_winners(
        context, entity_ids, context.profile_slot, (period, group), "default_group_winners"
    )
    selected = panel.selected_columns
    period_value = panel_values[period.field_id]
    group_value = panel_values[group.field_id]
    default = (
        select(defaults.c.selected_value)
        .where(defaults.c.entity_binding_id == selected.entity_binding_id)
        .correlate(*panel.get_final_froms())
        .scalar_subquery()
    )
    return panel.with_only_columns(
        selected.family_revision_id,
        selected.root_record_id,
        selected.root_revision_id,
        selected.entity_binding_id,
        selected.entity_value,
        period_value.label("selected_value"),
        group_value.label("group_value"),
        maintain_column_froms=True,
    ).where(period_value == default, group_value.in_(selection.group_values))
