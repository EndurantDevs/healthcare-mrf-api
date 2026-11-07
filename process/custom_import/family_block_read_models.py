# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Legacy read identities over one already checked family-block binding."""

from types import MappingProxyType

from sqlalchemy import and_, select
from sqlalchemy.orm import aliased, load_only

from db.models import custom_import as models
from process.custom_import.family_block_read_sql import _scope_number

_COMMON = {
    models.CustomImportRootRecord: "root_record",
    models.CustomImportEntityBinding: "entity_binding",
    models.CustomImportPack: "pack",
    models.CustomImportRejection: "rejection",
    models.CustomImportGenerationFamily: "generation_family",
    models.CustomImportWinner: "winner",
}
_IDENTITIES = {
    models.CustomImportFamilyRevision: (
        "family_revision_id",
        "dataset_id",
        "schema_revision_id",
        "root_record_id",
        "root_revision_id",
        "entity_binding_id",
        "family_sha256",
        "child_count",
    ),
    models.CustomImportRootRevision: (
        "root_revision_id",
        "dataset_id",
        "definition_revision_id",
        "schema_revision_id",
        "root_record_id",
    ),
    models.CustomImportChildRevision: (
        "child_revision_id",
        "dataset_id",
        "definition_revision_id",
        "schema_revision_id",
        "root_record_id",
        "collection_slot",
        "child_key_sha256",
        "canonical_child_key",
    ),
}


def _view(model, name, source, expressions, target):
    columns = model.__table__.c
    for field in ("dataset_id", "definition_revision_id", "schema_revision_id"):
        if field in columns:
            expressions[field] = _scope_number(getattr(target, field), columns[field].type)
    statement = select(*(value.label(name) for name, value in expressions.items())).select_from(source)
    return aliased(model, statement.subquery(name), adapt_on_names=True)


def read_models(binding, pinned_target):
    """Expose actual query identities, never a virtual unexposed child context."""
    tables = binding.tables_by_role
    models_by_type = {model: aliased(model, tables[role], adapt_on_names=True) for model, role in _COMMON.items()}
    family, child, key = tables["family"], tables["query_record"], tables["key_dictionary"]
    models_by_type[models.CustomImportFamilyRevision] = _view(
        models.CustomImportFamilyRevision,
        "block_family_identity",
        family,
        {name: family.c[name] for name in _IDENTITIES[models.CustomImportFamilyRevision] if name in family.c},
        pinned_target,
    )
    models_by_type[models.CustomImportRootRevision] = _view(
        models.CustomImportRootRevision,
        "block_root_identity",
        family,
        {name: family.c[name] for name in ("root_revision_id", "root_record_id")},
        pinned_target,
    )
    owned = child.join(family, family.c.family_revision_id == child.c.family_revision_id)
    child_source = owned.join(key, and_(key.c.key_rank == child.c.key_rank, key.c.key_variant == child.c.key_variant))
    models_by_type[models.CustomImportChildRevision] = _view(
        models.CustomImportChildRevision,
        "block_child_identity",
        child_source,
        dict(
            child_revision_id=child.c.child_revision_id,
            root_record_id=family.c.root_record_id,
            collection_slot=child.c.collection_slot,
            child_key_sha256=key.c.child_key_sha256,
            canonical_child_key=key.c.canonical_child_key,
        ),
        pinned_target,
    )
    models_by_type[models.CustomImportFamilyChild] = _view(
        models.CustomImportFamilyChild,
        "block_query_membership",
        owned,
        dict(
            family_revision_id=family.c.family_revision_id,
            root_record_id=family.c.root_record_id,
            child_revision_id=child.c.child_revision_id,
            collection_slot=child.c.collection_slot,
        ),
        pinned_target,
    )
    return MappingProxyType(models_by_type)


def identity_options(context, *, child=False):
    """Prevent unadapted legacy payload columns or deferred lazy reads."""
    included = tuple(_IDENTITIES) if child else tuple(_IDENTITIES)[:2]
    return tuple(
        load_only(*(getattr(context.model(model), name) for name in _IDENTITIES[model]), raiseload=True)
        for model in included
    )
