# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Protected native set writes for fresh and retained legacy family graphs."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, replace

from db.models.custom_import import (
    CustomImportChildRevision,
    CustomImportEntityBinding,
    CustomImportFamilyRevision,
    CustomImportRootRecord,
    CustomImportRootRevision,
)
from process.custom_import.execution import lease_token_sha256
from process.custom_import.family import RootFamily
from process.custom_import.materialization_store import (
    _authority,
    _authority_arguments,
    _call,
    _flush_pending,
    verify_materialization_authority,
)
from process.custom_import.runner_codec import (
    child_key_document,
    child_key_hash,
    digest_text,
    fields_by_collection,
    new_family_hash,
    record_payload,
    root_key_contract_hash,
    root_key_document,
    root_key_hash,
)
from process.custom_import.runner_types import (
    CandidateRunnerError,
    PublishedCandidateChild,
    PublishedCandidateFamily,
    StoredCandidateFamily,
)

_ROOT_PAGE_ROWS = 64
_CHILD_PAGE_ROWS = 85
_PAGE_BYTES = 8 * 1024 * 1024  # Multirow target; singleton values retain native TEXT limits.


@dataclass
class _RootWrite:
    record: CustomImportRootRecord
    binding: CustomImportEntityBinding
    revision: CustomImportRootRevision
    family: CustomImportFamilyRevision
    source: RootFamily | StoredCandidateFamily
    base_family_id: int | None
    byte_cost: int


@dataclass
class _ChildWrite:
    revision: CustomImportChildRevision
    family: PublishedCandidateFamily
    collection: str
    values: object
    base_family_id: int | None
    base_child_id: int | None
    family_index: int
    byte_cost: int


def _pages(rows, *, row_limit):
    page, size = [], 1024
    for row in rows:
        if page and (len(page) == row_limit or size + row.byte_cost > _PAGE_BYTES):
            yield page
            page, size = [], 1024
        page.append(row)
        size += row.byte_cost
    if page:
        yield page


def _root_identity_models(request, selected_family, key_contract):
    retained = isinstance(selected_family, StoredCandidateFamily)
    root_values = selected_family.root_values_by_field if retained else selected_family.root
    entity_value = root_values.get(request.definition.entity_field)
    if not isinstance(entity_value, str):
        raise CandidateRunnerError("accepted family has no string entity value")
    root_record = CustomImportRootRecord(
        root_record_id=selected_family.root_record.root_record_id if retained else None,
        dataset_id=request.dataset_id,
        key_contract_sha256=key_contract,
        canonical_logical_key=root_key_document(request.definition, root_values),
        logical_key_sha256=root_key_hash(request.definition, root_values),
    )
    binding = CustomImportEntityBinding(
        entity_binding_id=selected_family.entity_binding.entity_binding_id if retained else None,
        dataset_id=request.dataset_id,
        adapter_id="npi",
        canonical_value=entity_value,
        value_sha256=digest_text("entity:npi", entity_value),
    )
    return root_record, binding


def _family_revision_model(request, grant, selected_family, token):
    retained = isinstance(selected_family, StoredCandidateFamily)
    return CustomImportFamilyRevision(
        dataset_id=request.dataset_id,
        schema_revision_id=request.schema_revision_id,
        family_sha256=(
            bytes(selected_family.family.family_sha256)
            if retained
            else new_family_hash(request.definition, selected_family)
        ),
        child_count=(
            len(selected_family.children)
            if retained
            else sum(len(collection_rows) for collection_rows in selected_family.children.values())
        ),
        producing_execution_id=request.execution_id,
        producing_fence=grant.fence,
        producing_token_sha256=token,
    )


def _root_rows(request, grant, pack, selected):
    """Build unchanged canonical models before their bounded protected writes."""
    key_contract = root_key_contract_hash(request.definition)
    token = lease_token_sha256(request.lease_token)
    for ordinal, selected_family in enumerate(selected):
        retained = isinstance(selected_family, StoredCandidateFamily)
        root_values = selected_family.root_values_by_field if retained else selected_family.root
        root_record, binding = _root_identity_models(request, selected_family, key_contract)
        canonical_payload = (
            selected_family.root_revision.canonical_payload
            if retained
            else record_payload(request.definition.root_fields, root_values)
        )
        payload_hash = (
            bytes(selected_family.root_revision.payload_sha256)
            if retained
            else digest_text("root-payload", canonical_payload)
        )
        revision = CustomImportRootRevision(
            dataset_id=request.dataset_id,
            definition_revision_id=request.definition_revision_id,
            schema_revision_id=request.schema_revision_id,
            pack_id=pack.pack_id,
            source_ordinal=ordinal,
            canonical_payload=canonical_payload,
            payload_sha256=payload_hash,
        )
        family = _family_revision_model(request, grant, selected_family, token)
        # Identity and payload are separate set calls, so each keeps the native
        # TEXT domain without combining two individually valid giant values.
        size = max(
            1024 + 2 * (len(root_record.canonical_logical_key.encode()) + len(binding.canonical_value.encode())),
            640 + len(canonical_payload.encode()),
        )
        yield _RootWrite(
            root_record,
            binding,
            revision,
            family,
            selected_family,
            selected_family.family.family_revision_id if retained else None,
            size,
        )


def _ids(value, count):
    if (
        not isinstance(value, (list, tuple))
        or len(value) != count
        or any(type(item) is not int or not 0 < item < 2**63 for item in value)
    ):
        raise CandidateRunnerError("legacy graph returned identity differs")
    return value


async def _persist_root_page(session, window, page):
    is_singleton = len(page) == 1
    arguments = _authority_arguments(window) + (
        ("bytea[]", tuple(root_write.record.key_contract_sha256 for root_write in page)),
        ("text[]", (None,) if is_singleton else tuple(root_write.record.canonical_logical_key for root_write in page)),
        ("bytea[]", tuple(root_write.record.logical_key_sha256 for root_write in page)),
        ("text[]", tuple(root_write.binding.canonical_value for root_write in page)),
        ("bytea[]", tuple(root_write.binding.value_sha256 for root_write in page)),
        ("bigint[]", tuple(root_write.record.root_record_id for root_write in page)),
        ("bigint[]", tuple(root_write.binding.entity_binding_id for root_write in page)),
        ("text", page[0].record.canonical_logical_key if is_singleton else None),
    )
    identities = _ids(await _call(session, "persist_custom_import_legacy_identity_set", arguments), 2 * len(page))
    if len(set(identities[::2])) != len(page):
        raise CandidateRunnerError("legacy root identities repeat")
    for index, root_write in enumerate(page):
        root_id, binding_id = identities[2 * index : 2 * index + 2]
        if (
            root_write.record.root_record_id is not None
            and root_write.record.root_record_id != root_id
            or root_write.binding.entity_binding_id is not None
            and root_write.binding.entity_binding_id != binding_id
        ):
            raise CandidateRunnerError("legacy retained identity differs")
        root_write.record.root_record_id = root_write.revision.root_record_id = root_write.family.root_record_id = (
            root_id
        )
        root_write.binding.entity_binding_id = root_write.family.entity_binding_id = binding_id
    arguments = _authority_arguments(window) + (
        ("bigint[]", tuple(root_write.revision.root_revision_id for root_write in page)),
        ("bigint[]", tuple(root_write.family.family_revision_id for root_write in page)),
        ("bigint[]", tuple(root_write.record.root_record_id for root_write in page)),
        ("bigint[]", tuple(root_write.binding.entity_binding_id for root_write in page)),
        ("bigint[]", tuple(root_write.revision.pack_id for root_write in page)),
        ("bigint[]", tuple(root_write.revision.source_ordinal for root_write in page)),
        ("text[]", (None,) if is_singleton else tuple(root_write.revision.canonical_payload for root_write in page)),
        ("bytea[]", tuple(root_write.revision.payload_sha256 for root_write in page)),
        ("bytea[]", tuple(root_write.family.family_sha256 for root_write in page)),
        ("bigint[]", tuple(root_write.family.child_count for root_write in page)),
        ("bigint[]", tuple(root_write.base_family_id for root_write in page)),
        ("text", page[0].revision.canonical_payload if is_singleton else None),
    )
    revisions = _ids(await _call(session, "persist_custom_import_legacy_family_root_set", arguments), 2 * len(page))
    if len(set(revisions[::2])) != len(page) or len(set(revisions[1::2])) != len(page):
        raise CandidateRunnerError("legacy root revision identities repeat")
    for index, root_write in enumerate(page):
        revision_id, family_id = revisions[2 * index : 2 * index + 2]
        if (
            root_write.revision.root_revision_id is not None
            and root_write.revision.root_revision_id != revision_id
            or root_write.family.family_revision_id is not None
            and root_write.family.family_revision_id != family_id
        ):
            raise CandidateRunnerError("legacy root revision returned identity differs")
        root_write.revision.root_revision_id = root_write.family.root_revision_id = revision_id
        root_write.family.family_revision_id = family_id


def _family_child_inputs(definition, selected_family):
    """Keep retained order or the existing declared-collection/key order."""
    if isinstance(selected_family, StoredCandidateFamily):
        return (
            ((child.collection, child.values_by_field, child.child) for child in selected_family.children),
            selected_family.family.family_revision_id,
            selected_family.root_record.canonical_logical_key,
            bytes(selected_family.root_record.logical_key_sha256),
        )
    child_inputs = (
        (collection.name, child_values, None)
        for collection in definition.child_collections
        for child_values in sorted(
            selected_family.children[collection.name],
            key=lambda child_values: child_key_hash(definition, collection.name, child_values),
        )
    )
    return (
        child_inputs,
        None,
        root_key_document(definition, selected_family.root),
        root_key_hash(definition, selected_family.root),
    )


def _child_rows(request, registry, packs, selected, published):
    """Prepare child revisions with collection-global source ordinals."""
    ordinals = defaultdict(int)
    fields = fields_by_collection(request.definition)
    for family_index, (selected_family, family) in enumerate(zip(selected, published, strict=True)):
        child_inputs, base_family, parent_key, parent_hash = _family_child_inputs(request.definition, selected_family)
        for collection, child_values, stored_child in child_inputs:
            key = (
                stored_child.canonical_child_key
                if stored_child is not None
                else child_key_document(request.definition, collection, child_values)
            )
            canonical_payload = (
                stored_child.canonical_payload
                if stored_child is not None
                else record_payload(fields[collection], child_values)
            )
            key_hash = (
                bytes(stored_child.child_key_sha256)
                if stored_child is not None
                else child_key_hash(request.definition, collection, child_values)
            )
            payload_hash = (
                bytes(stored_child.payload_sha256)
                if stored_child is not None
                else digest_text("child-payload", canonical_payload)
            )
            revision = CustomImportChildRevision(
                dataset_id=request.dataset_id,
                definition_revision_id=request.definition_revision_id,
                schema_revision_id=request.schema_revision_id,
                root_record_id=family.root_record_id,
                collection_slot=registry.child_collection_slots[collection],
                pack_id=packs[collection].pack_id,
                source_ordinal=ordinals[collection],
                canonical_parent_key=parent_key,
                parent_key_sha256=parent_hash,
                canonical_child_key=key,
                child_key_sha256=key_hash,
                canonical_payload=canonical_payload,
                payload_sha256=payload_hash,
            )
            ordinals[collection] += 1
            size = 640 + len(parent_key.encode()) + len(key.encode()) + len(canonical_payload.encode())
            yield _ChildWrite(
                revision,
                family,
                collection,
                child_values,
                base_family,
                stored_child.child_revision_id if stored_child is not None else None,
                family_index,
                size,
            )


async def _persist_child_page(session, window, page):
    is_singleton = len(page) == 1
    arguments = _authority_arguments(window) + (
        ("bigint[]", tuple(row.revision.child_revision_id for row in page)),
        ("bigint[]", tuple(row.family.family_revision_id for row in page)),
        ("bigint[]", tuple(row.revision.root_record_id for row in page)),
        ("smallint[]", tuple(row.revision.collection_slot for row in page)),
        ("bigint[]", tuple(row.revision.pack_id for row in page)),
        ("bigint[]", tuple(row.revision.source_ordinal for row in page)),
        ("text[]", (None,) if is_singleton else tuple(row.revision.canonical_parent_key for row in page)),
        ("bytea[]", tuple(row.revision.parent_key_sha256 for row in page)),
        ("text[]", (None,) if is_singleton else tuple(row.revision.canonical_child_key for row in page)),
        ("bytea[]", tuple(row.revision.child_key_sha256 for row in page)),
        ("text[]", (None,) if is_singleton else tuple(row.revision.canonical_payload for row in page)),
        ("bytea[]", tuple(row.revision.payload_sha256 for row in page)),
        ("bigint[]", tuple(row.base_family_id for row in page)),
        ("bigint[]", tuple(row.base_child_id for row in page)),
        ("text", page[0].revision.canonical_parent_key if is_singleton else None),
        ("text", page[0].revision.canonical_child_key if is_singleton else None),
        ("text", page[0].revision.canonical_payload if is_singleton else None),
    )
    ids = _ids(await _call(session, "persist_custom_import_legacy_child_set", arguments), len(page))
    if len(set(ids)) != len(ids) or any(
        row.revision.child_revision_id is not None and row.revision.child_revision_id != value
        for row, value in zip(page, ids, strict=True)
    ):
        raise CandidateRunnerError("legacy child returned identity differs")
    for row, child_id in zip(page, ids, strict=True):
        row.revision.child_revision_id = child_id


async def persist_families(session, request, grant, registry, packs, selected_families):
    """Persist bounded root and cross-family child pages in the caller transaction."""

    window = await _authority(session)
    expected = (request.dataset_id, request.definition_revision_id, request.schema_revision_id, request.execution_id)
    if (
        expected != window.authority[:4]
        or grant.execution_id != request.execution_id
        or (grant.fence, lease_token_sha256(request.lease_token)) != window.authority[5:]
    ):
        raise CandidateRunnerError("legacy family authority differs")
    await _flush_pending(session)
    published_families = []
    for page in _pages(_root_rows(request, grant, packs[None], selected_families), row_limit=_ROOT_PAGE_ROWS):
        await _persist_root_page(session, window, page)
        for graph_write in page:
            field_values = (
                graph_write.source.root_values_by_field
                if isinstance(graph_write.source, StoredCandidateFamily)
                else dict(graph_write.source.root)
            )
            published_families.append(
                PublishedCandidateFamily(
                    root_record_id=graph_write.record.root_record_id,
                    root_revision_id=graph_write.revision.root_revision_id,
                    family_revision_id=graph_write.family.family_revision_id,
                    entity_binding_id=graph_write.binding.entity_binding_id,
                    family_sha256=bytes(graph_write.family.family_sha256),
                    root_values_by_field=field_values,
                    children=(),
                )
            )
    child_groups = [[] for _ in published_families]
    for page in _pages(
        _child_rows(request, registry, packs, selected_families, published_families), row_limit=_CHILD_PAGE_ROWS
    ):
        await _persist_child_page(session, window, page)
        for graph_write in page:
            field_values = graph_write.values if graph_write.base_child_id is not None else dict(graph_write.values)
            child_groups[graph_write.family_index].append(
                PublishedCandidateChild(
                    collection=graph_write.collection,
                    child_revision_id=graph_write.revision.child_revision_id,
                    child_key_sha256=bytes(graph_write.revision.child_key_sha256),
                    values_by_field=field_values,
                )
            )
    await verify_materialization_authority(session)
    return tuple(
        replace(family, children=tuple(child_rows))
        for family, child_rows in zip(published_families, child_groups, strict=True)
    )
