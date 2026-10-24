# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Persist closed source-binding variants with the existing revision ownership."""

from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy.ext.asyncio import AsyncSession

from db.models.custom_import import (
    CustomImportDefinitionRevision,
    CustomImportSchemaRevision,
    CustomImportSourceBindingRevision,
)
from process.custom_import import snowflake_source_binding as snowflake_store
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.definition_store import RegisteredDefinition, register_definition
from process.custom_import.object_source_binding import (
    OBJECT_SOURCE_BINDING_CONTRACT,
    OBJECT_SOURCE_CONNECTORS,
    ObjectSourceBinding,
    ObjectSourceBindingError,
)
from process.custom_import.read_identity import verified_definition


class ObjectSourceBindingUnavailableError(ObjectSourceBindingError):
    """Persisted object binding evidence is incomplete or inconsistent."""


@dataclass(frozen=True)
class LoadedObjectSourceBinding:
    """Exact owner identity, immutable definition and reusable object binding."""

    dataset_id: int
    definition_revision_id: int
    schema_revision_id: int
    source_binding_revision_id: int
    source_binding_sha256: bytes
    definition: CustomImportDefinition
    binding: ObjectSourceBinding


@dataclass(frozen=True)
class ObjectSourceBindingReceipt:
    """Stable identity for a newly persisted or exactly replayed object binding."""

    dataset_id: int
    definition_revision_id: int
    schema_revision_id: int
    source_binding_revision_id: int
    revision_number: int
    source_binding_sha256: bytes
    created: bool


def _loaded_object_source_binding(
    binding_row: CustomImportSourceBindingRevision,
    definition_row: CustomImportDefinitionRevision,
    schema_row: CustomImportSchemaRevision,
) -> LoadedObjectSourceBinding:
    try:
        dataset_id = snowflake_store._positive_id(binding_row.dataset_id)
        definition_revision_id = snowflake_store._positive_id(binding_row.definition_revision_id)
        schema_revision_id = snowflake_store._positive_id(binding_row.schema_revision_id)
        source_binding_revision_id = snowflake_store._positive_id(binding_row.source_binding_revision_id)
        snowflake_store._positive_id(binding_row.revision_number)
        if (
            binding_row.binding_contract != OBJECT_SOURCE_BINDING_CONTRACT
            or binding_row.connector_kind not in OBJECT_SOURCE_CONNECTORS
            or definition_row.dataset_id != dataset_id
            or definition_row.definition_revision_id != definition_revision_id
            or definition_row.schema_revision_id != schema_revision_id
            or schema_row.dataset_id != dataset_id
            or schema_row.schema_revision_id != schema_revision_id
        ):
            raise ValueError
        definition = verified_definition(definition_row, schema_row)
        binding = ObjectSourceBinding.from_json(binding_row.canonical_binding)
        binding_sha256 = bytes.fromhex(binding.digest)
        if (
            binding_row.binding_contract != binding.contract
            or binding_row.connector_kind != binding.connector
            or binding_row.canonical_binding != binding.canonical
            or not snowflake_store._has_matching_digest(binding_row.binding_sha256, binding_sha256)
            or not snowflake_store._has_matching_digest(
                binding_row.definition_sha256, bytes.fromhex(binding.definition_sha256)
            )
            or not snowflake_store._has_matching_digest(binding_row.schema_sha256, bytes.fromhex(binding.schema_sha256))
            or not snowflake_store._has_matching_digest(
                binding_row.source_object_fingerprint_sha256,
                bytes.fromhex(binding.source_object["fingerprint_sha256"]),
            )
            or binding_row.source_object_version != binding.source_object["version"]
        ):
            raise ValueError
        binding.validate_definition(definition)
    except AttributeError, TypeError, ValueError:
        raise ObjectSourceBindingUnavailableError("object source binding persisted identity is invalid") from None
    return LoadedObjectSourceBinding(
        dataset_id=dataset_id,
        definition_revision_id=definition_revision_id,
        schema_revision_id=schema_revision_id,
        source_binding_revision_id=source_binding_revision_id,
        source_binding_sha256=binding_sha256,
        definition=definition,
        binding=binding,
    )


async def load_source_binding(
    session: AsyncSession,
    *,
    definition_revision_id: int,
    source_binding_revision_id: int,
) -> LoadedObjectSourceBinding | snowflake_store.LoadedSnowflakeSourceBinding:
    """Select an explicit retained contract after the shared owner-joined lookup."""

    rows = await snowflake_store._source_binding_rows(
        session,
        definition_revision_id=definition_revision_id,
        source_binding_revision_id=source_binding_revision_id,
    )
    if rows[0].binding_contract in snowflake_store.SOURCE_BINDING_CONTRACTS:
        return snowflake_store._loaded_snowflake_source_binding(*rows)
    if rows[0].binding_contract == OBJECT_SOURCE_BINDING_CONTRACT:
        return _loaded_object_source_binding(*rows)
    raise ObjectSourceBindingUnavailableError("source binding contract is unavailable")


def _validated_object_registration(
    definition: CustomImportDefinition, binding: ObjectSourceBinding
) -> ObjectSourceBinding:
    try:
        if not isinstance(definition, CustomImportDefinition) or not isinstance(binding, ObjectSourceBinding):
            raise ValueError
        canonical_binding = ObjectSourceBinding.from_json(binding.canonical)
        if canonical_binding != binding:
            raise ValueError
        canonical_binding.validate_definition(definition)
        return canonical_binding
    except AttributeError, TypeError, ValueError:
        raise ObjectSourceBindingError("object source binding registration inputs are invalid") from None


async def _object_readback_receipt(
    session: AsyncSession,
    *,
    definition: CustomImportDefinition,
    binding: ObjectSourceBinding,
    definition_registration: RegisteredDefinition,
    binding_revision: CustomImportSourceBindingRevision,
    created: bool,
) -> ObjectSourceBindingReceipt:
    source_binding_revision_id = snowflake_store._positive_id(binding_revision.source_binding_revision_id)
    loaded = await load_source_binding(
        session,
        definition_revision_id=definition_registration.definition_revision_id,
        source_binding_revision_id=source_binding_revision_id,
    )
    binding_sha256 = bytes.fromhex(binding.digest)
    if (
        loaded.dataset_id != definition_registration.dataset_id
        or loaded.definition_revision_id != definition_registration.definition_revision_id
        or loaded.schema_revision_id != definition_registration.schema_revision_id
        or loaded.source_binding_revision_id != source_binding_revision_id
        or not snowflake_store._has_matching_digest(loaded.source_binding_sha256, binding_sha256)
        or loaded.definition != definition
        or loaded.binding != binding
    ):
        raise ObjectSourceBindingUnavailableError("object source binding readback does not match registration")
    return ObjectSourceBindingReceipt(
        dataset_id=loaded.dataset_id,
        definition_revision_id=loaded.definition_revision_id,
        schema_revision_id=loaded.schema_revision_id,
        source_binding_revision_id=source_binding_revision_id,
        revision_number=binding_revision.revision_number,
        source_binding_sha256=binding_sha256,
        created=created,
    )


async def register_source_binding(
    session: AsyncSession,
    *,
    dataset_key: str,
    definition: CustomImportDefinition,
    binding: ObjectSourceBinding | snowflake_store.SnowflakeSourceBinding,
) -> ObjectSourceBindingReceipt | snowflake_store.SnowflakeSourceBindingReceipt:
    """Register in the caller transaction, serialized by the existing dataset lock."""

    if isinstance(binding, snowflake_store.SnowflakeSourceBinding):
        return await snowflake_store.register_snowflake_source_binding(
            session, dataset_key=dataset_key, definition=definition, binding=binding
        )
    canonical_binding = _validated_object_registration(definition, binding)
    definition_registration = await register_definition(session, dataset_key, definition)
    try:
        binding_revisions = await snowflake_store._locked_source_binding_revisions(session, definition_registration)
        binding_revision = snowflake_store._matching_binding_revision(binding_revisions, canonical_binding)
        is_created = binding_revision is None
        if binding_revision is None:
            binding_revision = CustomImportSourceBindingRevision(
                dataset_id=definition_registration.dataset_id,
                definition_revision_id=definition_registration.definition_revision_id,
                schema_revision_id=definition_registration.schema_revision_id,
                revision_number=snowflake_store._next_binding_revision(binding_revisions),
                binding_contract=canonical_binding.contract,
                connector_kind=canonical_binding.connector,
                definition_sha256=bytes.fromhex(canonical_binding.definition_sha256),
                schema_sha256=bytes.fromhex(canonical_binding.schema_sha256),
                source_object_fingerprint_sha256=bytes.fromhex(canonical_binding.source_object["fingerprint_sha256"]),
                source_object_version=canonical_binding.source_object["version"],
                canonical_binding=canonical_binding.canonical,
                binding_sha256=bytes.fromhex(canonical_binding.digest),
            )
            session.add(binding_revision)
            await session.flush()
        return await _object_readback_receipt(
            session,
            definition=definition,
            binding=canonical_binding,
            definition_registration=definition_registration,
            binding_revision=binding_revision,
            created=is_created,
        )
    except snowflake_store.SnowflakeSourceBindingUnavailableError:
        raise ObjectSourceBindingUnavailableError("object source binding revision state is invalid") from None
