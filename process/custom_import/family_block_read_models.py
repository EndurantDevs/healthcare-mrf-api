# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Legacy read identities over one already checked family-block binding."""

from collections import OrderedDict
from types import MappingProxyType

from sqlalchemy import and_, select
from sqlalchemy.orm import aliased, load_only

from db.models import custom_import as models
from process.custom_import.family_block_binding import binding_cache_key
from process.custom_import.family_block_read_sql import _checked_scope_int, _scope_number

_MODEL_CACHE_LIMIT = 64
_READ_MODELS_CACHE: OrderedDict[tuple, tuple] = OrderedDict()

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


def _build_read_models(binding, pinned_target):
    tables_by_role = binding.tables_by_role
    models_by_type = {
        model: aliased(model, tables_by_role[role], adapt_on_names=True) for model, role in _COMMON.items()
    }
    family, child, key_table = (
        tables_by_role["family"],
        tables_by_role["query_record"],
        tables_by_role["key_dictionary"],
    )
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
    child_relation = owned.join(
        key_table, and_(key_table.c.key_rank == child.c.key_rank, key_table.c.key_variant == child.c.key_variant)
    )
    models_by_type[models.CustomImportChildRevision] = _view(
        models.CustomImportChildRevision,
        "block_child_identity",
        child_relation,
        dict(
            child_revision_id=child.c.child_revision_id,
            root_record_id=family.c.root_record_id,
            collection_slot=child.c.collection_slot,
            child_key_sha256=key_table.c.child_key_sha256,
            canonical_child_key=key_table.c.canonical_child_key,
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


def read_models(binding, pinned_target):
    """Expose actual query identities, never a virtual unexposed child context."""
    cache_key = (
        binding_cache_key(binding),
        _checked_scope_int(pinned_target.dataset_id),
        _checked_scope_int(pinned_target.definition_revision_id),
        _checked_scope_int(pinned_target.schema_revision_id),
    )
    cached_entry = _READ_MODELS_CACHE.get(cache_key)
    if cached_entry is not None and cached_entry[0] is binding:
        _READ_MODELS_CACHE.move_to_end(cache_key)
        return cached_entry[1]
    frozen_models = _build_read_models(binding, pinned_target)
    _READ_MODELS_CACHE[cache_key] = (binding, frozen_models)
    if len(_READ_MODELS_CACHE) > _MODEL_CACHE_LIMIT:
        _READ_MODELS_CACHE.popitem(last=False)
    return frozen_models


def identity_options(context, *, child=False):
    """Prevent unadapted legacy payload columns or deferred lazy reads."""
    included = tuple(_IDENTITIES) if child else tuple(_IDENTITIES)[:2]
    return tuple(
        load_only(*(getattr(context.model(model), name) for name in _IDENTITIES[model]), raiseload=True)
        for model in included
    )
