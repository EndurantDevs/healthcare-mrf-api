# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Persist immutable Snowflake source bindings and re-export their pure contract."""

from __future__ import annotations

import hmac
from dataclasses import dataclass

from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncSession

from db.models.custom_import import (
    CustomImportDefinitionRevision,
    CustomImportSchemaRevision,
    CustomImportSourceBindingRevision,
)
from process.custom_import.definition import MAX_REVISION_NUMBER, CustomImportDefinition
from process.custom_import.definition_store import RegisteredDefinition, register_definition
from process.custom_import.read_identity import verified_definition
from process.custom_import.snowflake import SnowflakeApprovedRelation, SnowflakeConnectorError
from process.custom_import.snowflake_binding import (
    SNOWFLAKE_SOURCE_BINDING_CONNECTOR,
    SOURCE_BINDING_CONTRACT,
    SnowflakeSourceBinding,
    SnowflakeSourceBindingError,
)
from process.custom_import.snowflake_bundle import SnowflakeBundleBinding, SnowflakeBundleError

__all__ = (
    "LoadedSnowflakeSourceBinding",
    "SNOWFLAKE_SOURCE_BINDING_CONNECTOR",
    "SOURCE_BINDING_CONTRACT",
    "SnowflakeSourceBinding",
    "SnowflakeSourceBindingError",
    "SnowflakeSourceBindingReceipt",
    "SnowflakeSourceBindingUnavailableError",
    "load_snowflake_source_binding",
    "register_snowflake_source_binding",
)


class SnowflakeSourceBindingUnavailableError(SnowflakeSourceBindingError):
    """Persisted source-binding evidence cannot be used for an operator run."""


@dataclass(frozen=True)
class SnowflakeSourceBindingReceipt:
    """Stable identity for one newly persisted or exactly replayed binding."""

    dataset_id: int
    definition_revision_id: int
    schema_revision_id: int
    source_binding_revision_id: int
    revision_number: int
    source_binding_sha256: bytes
    created: bool


def _positive_id(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 0 < value < 2**63:
        raise SnowflakeSourceBindingUnavailableError("source binding identifiers are invalid")
    return value


@dataclass(frozen=True)
class LoadedSnowflakeSourceBinding:
    """Exact persisted binding and derived allowlist ready for operator composition."""

    dataset_id: int
    definition_revision_id: int
    schema_revision_id: int
    source_binding_revision_id: int
    source_binding_sha256: bytes
    definition: CustomImportDefinition
    binding: SnowflakeSourceBinding
    approved_relations: tuple[SnowflakeApprovedRelation, ...]
    bundle_bindings: tuple[SnowflakeBundleBinding, ...]


def _has_matching_digest(value: object, expected: bytes) -> bool:
    try:
        return hmac.compare_digest(bytes(value), expected)
    except TypeError, ValueError:
        return False


def _loaded_snowflake_source_binding(
    binding_row: CustomImportSourceBindingRevision,
    definition_row: CustomImportDefinitionRevision,
    schema_row: CustomImportSchemaRevision,
) -> LoadedSnowflakeSourceBinding:
    """Fail closed unless canonical persisted rows form one exact source capability."""

    try:
        dataset_id = _positive_id(binding_row.dataset_id)
        definition_revision_id = _positive_id(binding_row.definition_revision_id)
        schema_revision_id = _positive_id(binding_row.schema_revision_id)
        source_binding_revision_id = _positive_id(binding_row.source_binding_revision_id)
        _positive_id(binding_row.revision_number)
        if (
            binding_row.binding_contract != SOURCE_BINDING_CONTRACT
            or binding_row.connector_kind != SNOWFLAKE_SOURCE_BINDING_CONNECTOR
            or definition_row.dataset_id != dataset_id
            or definition_row.definition_revision_id != definition_revision_id
            or definition_row.schema_revision_id != schema_revision_id
            or schema_row.dataset_id != dataset_id
            or schema_row.schema_revision_id != schema_revision_id
        ):
            raise SnowflakeSourceBindingUnavailableError("source binding row identity is invalid")
        definition = verified_definition(definition_row, schema_row)
        binding = SnowflakeSourceBinding.from_json(binding_row.canonical_binding)
        binding_sha256 = bytes.fromhex(binding.digest)
        if (
            binding_row.canonical_binding != binding.canonical
            or not _has_matching_digest(binding_row.binding_sha256, binding_sha256)
            or not _has_matching_digest(binding_row.definition_sha256, bytes.fromhex(binding.definition_sha256))
            or not _has_matching_digest(binding_row.schema_sha256, bytes.fromhex(binding.schema_sha256))
            or not _has_matching_digest(
                binding_row.source_object_fingerprint_sha256,
                bytes.fromhex(binding.source_object.fingerprint_sha256),
            )
            or binding_row.source_object_version != binding.source_object.version
            or binding.definition_sha256 != definition.digest
            or binding.schema_sha256 != definition.schema_digest
        ):
            raise SnowflakeSourceBindingUnavailableError("source binding persisted identity is invalid")
        approved_relations, bundle_bindings = binding.bundle_components(definition)
    except (AttributeError, TypeError, ValueError, SnowflakeConnectorError, SnowflakeBundleError) as exc:
        if isinstance(exc, SnowflakeSourceBindingUnavailableError):
            raise
        raise SnowflakeSourceBindingUnavailableError("source binding persisted identity is invalid") from exc
    return LoadedSnowflakeSourceBinding(
        dataset_id=dataset_id,
        definition_revision_id=definition_revision_id,
        schema_revision_id=schema_revision_id,
        source_binding_revision_id=source_binding_revision_id,
        source_binding_sha256=binding_sha256,
        definition=definition,
        binding=binding,
        approved_relations=approved_relations,
        bundle_bindings=bundle_bindings,
    )


async def load_snowflake_source_binding(
    session: AsyncSession,
    *,
    definition_revision_id: int,
    source_binding_revision_id: int,
) -> LoadedSnowflakeSourceBinding:
    """Load one immutable binding without accepting a source-controlled selector."""

    definition_revision_id = _positive_id(definition_revision_id)
    source_binding_revision_id = _positive_id(source_binding_revision_id)
    statement = (
        select(
            CustomImportSourceBindingRevision,
            CustomImportDefinitionRevision,
            CustomImportSchemaRevision,
        )
        .join(
            CustomImportDefinitionRevision,
            and_(
                CustomImportDefinitionRevision.definition_revision_id
                == CustomImportSourceBindingRevision.definition_revision_id,
                CustomImportDefinitionRevision.dataset_id == CustomImportSourceBindingRevision.dataset_id,
                CustomImportDefinitionRevision.schema_revision_id
                == CustomImportSourceBindingRevision.schema_revision_id,
            ),
        )
        .join(
            CustomImportSchemaRevision,
            and_(
                CustomImportSchemaRevision.schema_revision_id == CustomImportSourceBindingRevision.schema_revision_id,
                CustomImportSchemaRevision.dataset_id == CustomImportSourceBindingRevision.dataset_id,
            ),
        )
        .where(CustomImportSourceBindingRevision.definition_revision_id == definition_revision_id)
        .where(CustomImportSourceBindingRevision.source_binding_revision_id == source_binding_revision_id)
    )
    query_result = await session.execute(statement)
    binding_rows = tuple(query_result.all())
    if len(binding_rows) != 1:
        raise SnowflakeSourceBindingUnavailableError("source binding is unavailable")
    binding_row, definition_row, schema_row = binding_rows[0]
    return _loaded_snowflake_source_binding(binding_row, definition_row, schema_row)


def _validated_registration_binding(
    definition: object,
    binding: object,
) -> tuple[CustomImportDefinition, SnowflakeSourceBinding]:
    """Canonicalize the declarative inputs before definition persistence begins."""

    if not isinstance(definition, CustomImportDefinition) or not isinstance(binding, SnowflakeSourceBinding):
        raise SnowflakeSourceBindingError("source binding registration inputs are invalid")
    try:
        canonical_binding = SnowflakeSourceBinding.from_json(binding.canonical)
    except (AttributeError, TypeError, ValueError) as exc:
        raise SnowflakeSourceBindingError("source binding registration inputs are invalid") from exc
    if canonical_binding != binding:
        raise SnowflakeSourceBindingError("source binding registration inputs are not canonical")
    try:
        canonical_binding.bundle_components(definition)
    except SnowflakeSourceBindingError:
        raise
    except (AttributeError, TypeError, ValueError) as exc:
        raise SnowflakeSourceBindingError("source binding registration inputs are invalid") from exc
    return definition, canonical_binding


async def _locked_source_binding_revisions(
    session: AsyncSession,
    definition_registration: RegisteredDefinition,
) -> tuple[CustomImportSourceBindingRevision, ...]:
    """Lock every binding revision after definition registration holds the dataset."""

    result = await session.execute(
        select(CustomImportSourceBindingRevision)
        .where(
            CustomImportSourceBindingRevision.definition_revision_id == definition_registration.definition_revision_id
        )
        .order_by(CustomImportSourceBindingRevision.revision_number)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    binding_revisions = tuple(result.scalars().all())
    for binding_revision in binding_revisions:
        if (
            binding_revision.dataset_id != definition_registration.dataset_id
            or binding_revision.definition_revision_id != definition_registration.definition_revision_id
            or binding_revision.schema_revision_id != definition_registration.schema_revision_id
            or isinstance(binding_revision.revision_number, bool)
            or not isinstance(binding_revision.revision_number, int)
            or not 0 < binding_revision.revision_number <= MAX_REVISION_NUMBER
        ):
            raise SnowflakeSourceBindingUnavailableError("source binding revision state is invalid")
    return binding_revisions


def _matching_binding_revision(
    binding_revisions: tuple[CustomImportSourceBindingRevision, ...],
    binding: SnowflakeSourceBinding,
) -> CustomImportSourceBindingRevision | None:
    """Return the one stored digest match, leaving exact validation to readback."""

    binding_sha256 = bytes.fromhex(binding.digest)
    matching_revisions = tuple(
        binding_revision
        for binding_revision in binding_revisions
        if _has_matching_digest(binding_revision.binding_sha256, binding_sha256)
    )
    if len(matching_revisions) > 1:
        raise SnowflakeSourceBindingUnavailableError("source binding identity is ambiguous")
    if not matching_revisions:
        if any(binding_revision.canonical_binding == binding.canonical for binding_revision in binding_revisions):
            raise SnowflakeSourceBindingUnavailableError("source binding canonical state is invalid")
        return None
    return matching_revisions[0]


def _next_binding_revision(binding_revisions: tuple[CustomImportSourceBindingRevision, ...]) -> int:
    """Allocate the next per-definition revision from already locked rows."""

    revision_number = max((binding_revision.revision_number for binding_revision in binding_revisions), default=0) + 1
    if revision_number > MAX_REVISION_NUMBER:
        raise SnowflakeSourceBindingUnavailableError("source binding revision limit is exhausted")
    return revision_number


async def _readback_receipt(
    session: AsyncSession,
    *,
    definition: CustomImportDefinition,
    binding: SnowflakeSourceBinding,
    definition_registration: RegisteredDefinition,
    binding_revision: CustomImportSourceBindingRevision,
    created: bool,
) -> SnowflakeSourceBindingReceipt:
    """Read the stored row through the normal fail-closed loader before receipt."""

    source_binding_revision_id = _positive_id(binding_revision.source_binding_revision_id)
    loaded = await load_snowflake_source_binding(
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
        or not _has_matching_digest(loaded.source_binding_sha256, binding_sha256)
        or loaded.definition != definition
        or loaded.binding != binding
    ):
        raise SnowflakeSourceBindingUnavailableError("source binding readback does not match registration")
    return SnowflakeSourceBindingReceipt(
        dataset_id=loaded.dataset_id,
        definition_revision_id=loaded.definition_revision_id,
        schema_revision_id=loaded.schema_revision_id,
        source_binding_revision_id=source_binding_revision_id,
        revision_number=binding_revision.revision_number,
        source_binding_sha256=binding_sha256,
        created=created,
    )


async def register_snowflake_source_binding(
    session: AsyncSession,
    *,
    dataset_key: str,
    definition: CustomImportDefinition,
    binding: SnowflakeSourceBinding,
) -> SnowflakeSourceBindingReceipt:
    """Persist one bounded binding revision or return its exact immutable replay.

    The caller owns the active transaction.  Definition registration takes the
    dataset lock before this function locks existing binding revisions, so a
    concurrent binding append cannot choose the same revision number.
    """

    canonical_definition, canonical_binding = _validated_registration_binding(definition, binding)
    definition_registration = await register_definition(session, dataset_key, canonical_definition)
    binding_revisions = await _locked_source_binding_revisions(session, definition_registration)
    binding_revision = _matching_binding_revision(binding_revisions, canonical_binding)
    is_created = binding_revision is None
    if binding_revision is None:
        binding_revision = CustomImportSourceBindingRevision(
            dataset_id=definition_registration.dataset_id,
            definition_revision_id=definition_registration.definition_revision_id,
            schema_revision_id=definition_registration.schema_revision_id,
            revision_number=_next_binding_revision(binding_revisions),
            binding_contract=SOURCE_BINDING_CONTRACT,
            connector_kind=SNOWFLAKE_SOURCE_BINDING_CONNECTOR,
            definition_sha256=bytes.fromhex(canonical_binding.definition_sha256),
            schema_sha256=bytes.fromhex(canonical_binding.schema_sha256),
            source_object_fingerprint_sha256=bytes.fromhex(canonical_binding.source_object.fingerprint_sha256),
            source_object_version=canonical_binding.source_object.version,
            canonical_binding=canonical_binding.canonical,
            binding_sha256=bytes.fromhex(canonical_binding.digest),
        )
        session.add(binding_revision)
        await session.flush()
    return await _readback_receipt(
        session,
        definition=canonical_definition,
        binding=canonical_binding,
        definition_registration=definition_registration,
        binding_revision=binding_revision,
        created=is_created,
    )
