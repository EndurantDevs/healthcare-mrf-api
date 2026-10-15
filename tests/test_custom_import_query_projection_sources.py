# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Synthetic finalized owners for configuration-driven projection source reads."""

from dataclasses import replace
from decimal import Decimal

import pytest
from sqlalchemy import BigInteger, LargeBinary, String, column, select, values
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import aliased

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
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.query_projection_sources import (
    ProjectionSource,
    capability_sha256,
    child_hash_collision_statement,
    child_source,
    field_slots,
    root_source,
)
from tests.test_custom_import_derived_query import derived_document


def _definition(*, reserved=False):
    document = derived_document()
    if reserved:
        field_names_by_original = {
            "score": "selected_value",
            "weight": "group_value",
            "quality": "entity_binding_id",
            "quantity": "canonical_child_key",
            "amount": "child_key_sha256",
        }

        def rename(value):
            if isinstance(value, dict):
                return {key: rename(item) for key, item in value.items()}
            if isinstance(value, list):
                return [rename(item) for item in value]
            return field_names_by_original.get(value, value) if isinstance(value, str) else value

        document = rename(document)
    return CustomImportDefinition.from_mapping(document)


def _scalar_row(definition, field_id, value, **owner):
    field = definition.fields_by_id[field_id]
    return dict(
        owner,
        field_slot=field.field_slot,
        value_state="null" if value is None else "value",
        string_value=value if field.value_type == "string" else None,
        integer_value=value if field.value_type == "integer" else None,
        decimal_value=value if field.value_type == "decimal" else None,
    )


def _append_children(rows_by_model, definition, index, root_owner_by_name, family_id):
    _, child_metric, child_preferred = definition.query.derived_fields
    child_owner_by_name = dict(root_owner_by_name, child_revision_id=400 + index, collection_slot=1)
    rows_by_model[CustomImportFamilyChild].append(dict(child_owner_by_name, family_revision_id=family_id))
    rows_by_model[CustomImportChildRevision].append(
        dict(child_owner_by_name, canonical_child_key='["chosen"]', child_key_sha256=b"a" * 32)
    )
    child_values_by_field = {
        child_metric.source_field_id: None if index == 0 else Decimal(index + 3),
        child_metric.weight_field_id: (0, -1, 2)[index],
        child_preferred.source_field_id: None if index == 0 else Decimal(index + 7),
    }
    rows_by_model[CustomImportChildScalar].extend(
        _scalar_row(definition, field_id, field_value, **child_owner_by_name)
        for field_id, field_value in child_values_by_field.items()
    )


def _owner_rows(definition):
    rows_by_model = {
        model: []
        for model in (
            CustomImportWinner,
            CustomImportGenerationFamily,
            CustomImportFamilyRevision,
            CustomImportRootRevision,
            CustomImportRootScalar,
            CustomImportFamilyChild,
            CustomImportChildRevision,
            CustomImportChildScalar,
        )
    }
    selection = definition.query.entity_selection
    root_metric = definition.query.derived_fields[0]
    for index, (period, group) in enumerate(((2024, "segment_a"), (2024, "segment_b"), (2025, "segment_a"))):
        root_owner_by_name = dict(dataset_id=1, schema_revision_id=3, root_record_id=200 + index)
        family_id, root_revision = 100 + index, 300 + index
        rows_by_model[CustomImportWinner].append(
            dict(
                generation_id=4,
                dataset_id=1,
                definition_revision_id=2,
                schema_revision_id=3,
                profile_slot=1,
                entity_binding_id=10,
                family_revision_id=family_id,
                context_collection_slot=0,
                context_child_revision_id=None,
            )
        )
        rows_by_model[CustomImportGenerationFamily].append(
            dict(generation_id=4, dataset_id=1, family_revision_id=family_id, root_record_id=200 + index)
        )
        rows_by_model[CustomImportFamilyRevision].append(
            dict(root_owner_by_name, family_revision_id=family_id, root_revision_id=root_revision, entity_binding_id=10)
        )
        rows_by_model[CustomImportRootRevision].append(dict(root_owner_by_name, root_revision_id=root_revision))
        root_values_by_field = {
            selection.field_id: period,
            selection.group_field_id: group,
            root_metric.source_field_id: Decimal(index + 1),
            root_metric.weight_field_id: 2,
        }
        rows_by_model[CustomImportRootScalar].extend(
            _scalar_row(definition, field_id, field_value, **root_owner_by_name, root_revision_id=root_revision)
            for field_id, field_value in root_values_by_field.items()
        )
        _append_children(rows_by_model, definition, index, root_owner_by_name, family_id)
    return rows_by_model


def _context(definition=None, rows=None):
    definition = definition or _definition()
    rows = _owner_rows(definition) if rows is None else rows
    models_by_type = {}
    for model, records in rows.items():
        names = tuple(records[0])
        source = (
            values(*(column(name, model.__table__.c[name].type) for name in names))
            .data([tuple(record[name] for name in names) for record in records])
            .cte("synthetic_" + model.__tablename__)
        )
        models_by_type[model] = aliased(model, select(source).subquery(), adapt_on_names=True)
    return ProjectionSource(1, 2, 3, 4, 1, definition, models_by_type)


def _hash_source(rows):
    return (
        values(
            column("entity_binding_id", BigInteger),
            column("selected_value", BigInteger),
            column("canonical_child_key", String),
            column("child_key_sha256", LargeBinary),
        )
        .data(rows)
        .cte("synthetic_complete_keys")
    )


@pytest.mark.parametrize("change", ("alias", "output", "filter", "sort"))
def test_capability_digest_binds_saved_output_and_query_configuration(change):
    document = derived_document()
    original = CustomImportDefinition.from_mapping(document)
    if change == "alias":
        document["query"]["aliases"]["another_metric"] = "combined_score"
    elif change == "output":
        document["query"]["derived_fields"][0]["output_path"] = ["combined", "metric"]
    elif change == "filter":
        document["query"]["filterable_fields"] = ["combined_score"]
    else:
        document["query"]["sortable_fields"].remove("combined_score")
    changed = CustomImportDefinition.from_mapping(document)
    assert len(capability_sha256(original)) == 32
    assert capability_sha256(original) == capability_sha256(CustomImportDefinition.from_mapping(derived_document()))
    assert capability_sha256(original) != capability_sha256(changed)


def test_field_slots_keep_declaration_positions_when_capabilities_are_disabled():
    definition = _definition()
    disabled = replace(definition, query=replace(definition.query, filterable_fields=(), sortable_fields=()))
    slots_by_field = {"combined_score": 1, "combined_quality": 2, "merged_cost": 3}
    assert field_slots(definition) == field_slots(disabled) == slots_by_field
    reordered = replace(
        definition, query=replace(definition.query, derived_fields=definition.query.derived_fields[::-1])
    )
    assert field_slots(reordered) == {"merged_cost": 1, "combined_quality": 2, "combined_score": 3}


def test_source_mapping_separates_configured_ids_from_identity_columns_before_seal():
    context = _context(_definition(reserved=True))
    roots, root_values = root_source(context, [10])
    children, child_values = child_source(context, roots, 1)
    assert root_values["selected_value"].name != roots.c.selected_value.name
    assert root_values["group_value"].name != roots.c.group_value.name
    for name in ("entity_binding_id", "canonical_child_key", "child_key_sha256"):
        assert child_values[name].name != children.c[name].name
    sql = str(select(children).compile(dialect=postgresql.dialect()))
    assert "custom_import_generation_seal" not in sql


def test_hash_consistency_checks_both_directions_within_one_selection_context():
    source = _hash_source([(10, 2025, '["first"]', b"a" * 32)])
    sql = str(child_hash_collision_statement(source).compile(dialect=postgresql.dialect()))
    assert sql.count("HAVING count(distinct(") == 2
    assert "GROUP BY synthetic_complete_keys.entity_binding_id, synthetic_complete_keys.selected_value" in sql
