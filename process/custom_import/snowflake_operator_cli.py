# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Redacted standalone operator for one retained Snowflake source binding."""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import json
import re
import secrets
import sys
from collections.abc import Mapping
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Sequence

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from db.connection import db
from db.models.custom_import import (
    CustomImportDataset,
    CustomImportExecution,
    CustomImportLease,
    CustomImportSourceBindingRevision,
)
from process.custom_import.cli import _MISSING, _read_stdin, _receipt_only_database_output, _set_engine_echo
from process.custom_import.definition import CustomImportDefinition, canonical_json, load_json_definition
from process.custom_import.definition_store import DefinitionRegistrationError, _normalized_dataset_key
from process.custom_import.execution import IdempotencyConflict, lookup_execution_request
from process.custom_import.family import RootFamily
from process.custom_import.runner import CandidateRunResult
from process.custom_import.runner_registry import database_now
from process.custom_import.snowflake import FixedLocalKeyPairCredentialProvider
from process.custom_import.snowflake_bundle import SnowflakeBundleAcquisitionConnector
from process.custom_import.snowflake_candidate import (
    SnowflakeBundleCandidateRequest,
    bundle_request_identity_sha256,
    run_snowflake_bundle_candidate,
)
from process.custom_import.snowflake_preflight import (
    DEFAULT_MAX_CHILD_ROWS,
    DEFAULT_MAX_ELAPSED_SECONDS,
    DEFAULT_MAX_ROOT_KEYS,
    DEFAULT_MAX_TOTAL_BYTES,
    SnowflakePreflightError,
    SnowflakePreflightLimits,
    SnowflakePreflightRejectionDiagnostic,
    SnowflakePreflightResult,
    SnowflakePreflightSample,
    SnowflakePreflightStreamObservation,
    preflight_snowflake_bundle,
)
from process.custom_import.snowflake_python import SnowflakePythonConnectorAdapter, SnowflakePythonPreflightAdapter
from process.custom_import.snowflake_source_binding import (
    SnowflakeSourceBinding,
    SnowflakeSourceBindingError,
    SnowflakeSourceBindingReceipt,
    SnowflakeSourceBindingUnavailableError,
    load_snowflake_source_binding,
    register_snowflake_source_binding,
)

FIXED_CREDENTIAL_DIRECTORY = Path("/run/custom-import-operator")
_IDEMPOTENCY_KEY = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$", flags=re.ASCII)
_SAFE_ERROR_CODES = frozenset(
    {"canceled", "failed", "invalid_arguments", "invalid_registration", "source_binding_unavailable"}
)
_SAFE_STATUSES = frozenset(
    {
        "activated",
        "candidate_rejected",
        "canceled",
        "lease_lost",
        "no_change",
        "not_claimed",
        "sealed_unpublished",
    }
)
_SAFE_PREFLIGHT_REASONS = frozenset(
    {
        "byte_limit",
        "child_limit_reached",
        "definition_invalid",
        "duplicate_root_key",
        "family_invalid",
        "limits_invalid",
        "mapping_invalid",
        "query_timeout",
        "query_unavailable",
        "result_invalid",
        "result_schema_invalid",
        "root_data_incomplete",
        "root_key_missing",
        "runtime_integer_unsupported",
        "runtime_type_unsupported",
        "snapshot_invalid",
    }
)
_GENERIC_REJECTION_CODE = re.compile(r"^[a-z][a-z0-9_]{0,63}$", flags=re.ASCII)


class _ResumeUnavailableError(RuntimeError):
    """A retained execution cannot safely be resumed."""


class _RedactedArgumentParser(argparse.ArgumentParser):
    def error(self, _message: str) -> None:
        """Exit without reflecting invalid command-line input."""

        self.exit(2, _error_json("invalid_arguments") + "\n")


def _error_json(code: str) -> str:
    return json.dumps(
        {"code": code if code in _SAFE_ERROR_CODES else "failed", "status": "error"},
        separators=(",", ":"),
        sort_keys=True,
    )


def _positive_identifier(value: str) -> int:
    if not isinstance(value, str) or not re.fullmatch(r"[1-9][0-9]{0,18}", value, flags=re.ASCII):
        raise argparse.ArgumentTypeError("invalid")
    parsed = int(value)
    if parsed >= 2**63:
        raise argparse.ArgumentTypeError("invalid")
    return parsed


def _idempotency_key(value: str) -> str:
    if not isinstance(value, str) or _IDEMPOTENCY_KEY.fullmatch(value) is None:
        raise argparse.ArgumentTypeError("invalid")
    return value


def _parser() -> argparse.ArgumentParser:
    parser = _RedactedArgumentParser(allow_abbrev=False)
    commands = parser.add_subparsers(dest="command", required=True, parser_class=_RedactedArgumentParser)
    commands.add_parser("register", allow_abbrev=False, help="read one canonical registration envelope from stdin")
    for command in ("execute", "resume"):
        operation = commands.add_parser(command, allow_abbrev=False)
        operation.add_argument("--definition-revision-id", required=True, type=_positive_identifier)
        operation.add_argument("--source-binding-revision-id", required=True, type=_positive_identifier)
        operation.add_argument("--idempotency-key", required=True, type=_idempotency_key)
    preflight = commands.add_parser("preflight", allow_abbrev=False)
    preflight.add_argument("--definition-revision-id", required=True, type=_positive_identifier)
    preflight.add_argument("--source-binding-revision-id", required=True, type=_positive_identifier)
    preflight.add_argument("--maximum-root-keys", default=DEFAULT_MAX_ROOT_KEYS, type=_positive_identifier)
    preflight.add_argument("--maximum-child-rows", default=DEFAULT_MAX_CHILD_ROWS, type=_positive_identifier)
    preflight.add_argument("--maximum-total-bytes", default=DEFAULT_MAX_TOTAL_BYTES, type=_positive_identifier)
    preflight.add_argument("--maximum-elapsed-seconds", default=DEFAULT_MAX_ELAPSED_SECONDS, type=_positive_identifier)
    preflight.add_argument("--include-sample", action="store_true")
    return parser


def _registration_from_stdin(stream: Any | None = None) -> tuple[str, CustomImportDefinition, SnowflakeSourceBinding]:
    """Read only dataset_key, definition, and source_binding within the stdin budget."""

    try:
        document = load_json_definition(_read_stdin(stream))
        if not isinstance(document, dict) or set(document) != {"dataset_key", "definition", "source_binding"}:
            raise SnowflakeSourceBindingError("registration envelope is invalid")
        dataset_key = _normalized_dataset_key(document["dataset_key"])
        definition = CustomImportDefinition.from_mapping(document["definition"])
        binding = SnowflakeSourceBinding.from_mapping(document["source_binding"])
        if (
            canonical_json(document["definition"]) != definition.canonical
            or canonical_json(document["source_binding"]) != binding.canonical
        ):
            raise SnowflakeSourceBindingError("registration documents are not canonical")
        binding.bundle_components(definition)
        return dataset_key, definition, binding
    except OSError, TypeError, ValueError:
        raise SnowflakeSourceBindingError("registration input is invalid") from None


def _registration_receipt(
    registration: SnowflakeSourceBindingReceipt,
    definition: CustomImportDefinition,
    binding: SnowflakeSourceBinding,
) -> str:
    """Render only validated numeric identifiers and canonical digests."""

    if not isinstance(registration, SnowflakeSourceBindingReceipt):
        raise ValueError("operator registration result is invalid")
    identifier_by_field = {
        "dataset_id": registration.dataset_id,
        "definition_revision_id": registration.definition_revision_id,
        "schema_revision_id": registration.schema_revision_id,
        "source_binding_revision_id": registration.source_binding_revision_id,
        "source_binding_revision": registration.revision_number,
    }
    if (
        any(type(identifier) is not int or not 0 < identifier < 2**63 for identifier in identifier_by_field.values())
        or type(registration.created) is not bool
        or not isinstance(registration.source_binding_sha256, bytes)
        or registration.source_binding_sha256 != bytes.fromhex(binding.digest)
    ):
        raise ValueError("operator registration result is invalid")
    return json.dumps(
        {
            **identifier_by_field,
            "definition_sha256": definition.digest,
            "schema_sha256": definition.schema_digest,
            "source_binding_sha256": binding.digest,
            "status": "registered" if registration.created else "replayed",
        },
        separators=(",", ":"),
        sort_keys=True,
    )


async def _committed_registration_receipt(
    session: AsyncSession,
    *,
    dataset_key: str,
    registration: SnowflakeSourceBindingReceipt,
    definition: CustomImportDefinition,
    binding: SnowflakeSourceBinding,
) -> str:
    """Verify one committed registration in a fresh session before reporting it."""

    loaded = await load_snowflake_source_binding(
        session,
        definition_revision_id=registration.definition_revision_id,
        source_binding_revision_id=registration.source_binding_revision_id,
    )
    identity_rows = (
        await session.execute(
            select(CustomImportDataset.dataset_key, CustomImportSourceBindingRevision.revision_number)
            .select_from(CustomImportSourceBindingRevision)
            .join(CustomImportDataset, CustomImportDataset.dataset_id == CustomImportSourceBindingRevision.dataset_id)
            .where(
                CustomImportSourceBindingRevision.source_binding_revision_id == registration.source_binding_revision_id
            )
            .where(CustomImportSourceBindingRevision.dataset_id == registration.dataset_id)
            .where(CustomImportSourceBindingRevision.definition_revision_id == registration.definition_revision_id)
            .where(CustomImportSourceBindingRevision.schema_revision_id == registration.schema_revision_id)
        )
    ).all()
    if (
        len(identity_rows) != 1
        or identity_rows[0] != (dataset_key, registration.revision_number)
        or loaded.dataset_id != registration.dataset_id
        or loaded.definition_revision_id != registration.definition_revision_id
        or loaded.schema_revision_id != registration.schema_revision_id
        or loaded.source_binding_revision_id != registration.source_binding_revision_id
        or loaded.definition != definition
        or loaded.definition.digest != definition.digest
        or loaded.definition.schema_digest != definition.schema_digest
        or loaded.binding != binding
        or loaded.source_binding_sha256 != registration.source_binding_sha256
        or loaded.source_binding_sha256 != bytes.fromhex(binding.digest)
    ):
        raise SnowflakeSourceBindingUnavailableError("committed registration does not match")
    return _registration_receipt(registration, loaded.definition, loaded.binding)


async def _register_snowflake_binding(*, stream: Any | None = None, database=db) -> str:
    """Commit one canonical registration before returning its redacted receipt."""

    dataset_key, definition, binding = _registration_from_stdin(stream)
    with _receipt_only_database_output(database):
        engine = None
        previous_echo = _MISSING
        has_primary_failure = False
        try:
            await database.connect()
            engine = getattr(database, "engine", None)
            previous_echo = getattr(engine, "echo", _MISSING)
            _set_engine_echo(database, False)
            async with database.session() as session, session.begin():
                registration = await register_snowflake_source_binding(
                    session,
                    dataset_key=dataset_key,
                    definition=definition,
                    binding=binding,
                )
            async with database.session() as session:
                return await _committed_registration_receipt(
                    session,
                    dataset_key=dataset_key,
                    registration=registration,
                    definition=definition,
                    binding=binding,
                )
        except BaseException:
            has_primary_failure = True
            raise
        finally:
            if previous_echo is not _MISSING:
                engine.echo = previous_echo
            try:
                await database.disconnect()
            except Exception:
                if not has_primary_failure:
                    raise


def _receipt(result: CandidateRunResult) -> str:
    if (
        not isinstance(result, CandidateRunResult)
        or result.status not in _SAFE_STATUSES
        or isinstance(result.execution_id, bool)
        or not isinstance(result.execution_id, int)
        or result.execution_id <= 0
        or (
            result.generation_id is not None
            and (
                isinstance(result.generation_id, bool)
                or not isinstance(result.generation_id, int)
                or result.generation_id <= 0
            )
        )
    ):
        raise ValueError("operator result is invalid")
    receipt_by_field: dict[str, object] = {"execution_id": result.execution_id, "status": result.status}
    if result.generation_id is not None:
        receipt_by_field["generation_id"] = result.generation_id
    return json.dumps(receipt_by_field, separators=(",", ":"), sort_keys=True)


def _preflight_limits(parsed: argparse.Namespace) -> SnowflakePreflightLimits:
    """Build the core's bounded limit contract from fixed CLI fields."""

    return SnowflakePreflightLimits(
        maximum_root_keys=parsed.maximum_root_keys,
        maximum_child_rows=parsed.maximum_child_rows,
        maximum_total_bytes=parsed.maximum_total_bytes,
        maximum_elapsed_seconds=parsed.maximum_elapsed_seconds,
    )


def _preflight_receipt(
    preflight_result: SnowflakePreflightResult,
    *,
    definition: CustomImportDefinition,
    source_binding_sha256: bytes,
    limits: SnowflakePreflightLimits,
    include_sample: bool,
) -> str:
    """Render one validated result without exposing source execution metadata."""

    _validate_preflight_result(preflight_result, definition, source_binding_sha256, limits)
    complete_families = _complete_preflight_families(preflight_result, limits)
    receipt_by_field: dict[str, object] = {
        "definition_sha256": preflight_result.definition_sha256,
        "schema_sha256": preflight_result.schema_sha256,
        "source_binding_sha256": preflight_result.source_binding_sha256,
        "flags": _preflight_flags(preflight_result),
        "observations": _scoped_stream_payloads(preflight_result, definition, limits),
        "observed_bytes": preflight_result.observed_bytes,
        "status": preflight_result.status,
        "reason": _preflight_reason(preflight_result),
        "family_count": len(complete_families),
    }
    if type(include_sample) is not bool:
        raise ValueError("preflight sample flag is invalid")
    if include_sample:
        receipt_by_field.update(_preflight_sample_payload(preflight_result, definition, limits, complete_families))
    return json.dumps(receipt_by_field, allow_nan=False, separators=(",", ":"), sort_keys=True)


def _validate_preflight_result(
    preflight_result: object,
    definition: CustomImportDefinition,
    source_binding_sha256: bytes,
    limits: SnowflakePreflightLimits,
) -> None:
    if (
        not isinstance(preflight_result, SnowflakePreflightResult)
        or not isinstance(definition, CustomImportDefinition)
        or not isinstance(source_binding_sha256, bytes)
        or len(source_binding_sha256) != 32
        or not isinstance(limits, SnowflakePreflightLimits)
        or preflight_result.definition_sha256 != definition.digest
        or preflight_result.schema_sha256 != definition.schema_digest
        or preflight_result.source_binding_sha256 != source_binding_sha256.hex()
        or type(preflight_result.observed_bytes) is not int
        or not 0 <= preflight_result.observed_bytes <= limits.maximum_total_bytes
    ):
        raise ValueError("preflight result is invalid")


def _preflight_flags(preflight_result: SnowflakePreflightResult) -> dict[str, bool]:
    validation = preflight_result.validation
    flag_by_name = {
        "definition_valid": getattr(validation, "definition_valid", None),
        "mapping_valid": getattr(validation, "mapping_valid", None),
        "runtime_supported": getattr(validation, "runtime_supported", None),
    }
    if any(type(value) is not bool for value in flag_by_name.values()):
        raise ValueError("preflight flags are invalid")
    return flag_by_name


def _scoped_stream_payloads(
    preflight_result: SnowflakePreflightResult,
    definition: CustomImportDefinition,
    limits: SnowflakePreflightLimits,
) -> list[dict[str, object]]:
    observations = preflight_result.observations
    expected_stream_ids = tuple(stream.stream_id for stream in definition.source_streams)
    validation = preflight_result.validation
    if not isinstance(observations, tuple):
        raise ValueError("preflight observations are invalid")
    if (
        observations == ()
        and preflight_result.status == "unavailable"
        and preflight_result.observed_bytes == 0
        and preflight_result.rejection_diagnostics == ()
        and getattr(validation, "mapping_valid", None) is False
        and getattr(validation, "runtime_supported", None) is False
        and (
            (
                preflight_result.unavailable_reason == "definition_invalid"
                and getattr(validation, "definition_valid", None) is False
            )
            or (
                preflight_result.unavailable_reason in {"limits_invalid", "mapping_invalid"}
                and getattr(validation, "definition_valid", None) is True
            )
        )
    ):
        return []
    if len(observations) != len(expected_stream_ids):
        raise ValueError("preflight observations are invalid")
    return [
        _preflight_observation(observation, stream_id, limits)
        for observation, stream_id in zip(observations, expected_stream_ids, strict=True)
    ]


def _preflight_observation(
    observation: object,
    expected_stream_id: str,
    limits: SnowflakePreflightLimits,
) -> dict[str, object]:
    if (
        not isinstance(observation, SnowflakePreflightStreamObservation)
        or observation.stream_id != expected_stream_id
        or type(observation.observed_rows) is not int
        or observation.observed_rows < 0
        or type(observation.observed_bytes) is not int
        or not 0 <= observation.observed_bytes <= limits.maximum_total_bytes
        or observation.precision not in {"exact", "lower_bound", "unknown"}
    ):
        raise ValueError("preflight observation is invalid")
    return {
        "stream_id": observation.stream_id,
        "observed_rows": observation.observed_rows,
        "observed_bytes": observation.observed_bytes,
        "precision": observation.precision,
    }


def _complete_preflight_families(
    preflight_result: SnowflakePreflightResult,
    limits: SnowflakePreflightLimits,
) -> tuple[RootFamily, ...]:
    if preflight_result.status == "unavailable":
        if preflight_result.sample is not None:
            raise ValueError("unavailable preflight sample is invalid")
        return ()
    sample = preflight_result.sample
    if (
        preflight_result.status != "complete"
        or not isinstance(sample, SnowflakePreflightSample)
        or not isinstance(sample.families, tuple)
        or len(sample.families) > limits.maximum_root_keys
        or not all(isinstance(family, RootFamily) for family in sample.families)
    ):
        raise ValueError("complete preflight sample is invalid")
    return sample.families


def _preflight_reason(preflight_result: SnowflakePreflightResult) -> str | None:
    if preflight_result.status == "complete":
        if preflight_result.unavailable_reason is not None:
            raise ValueError("complete preflight reason is invalid")
        return None
    if preflight_result.status != "unavailable":
        raise ValueError("preflight status is invalid")
    return (
        preflight_result.unavailable_reason
        if preflight_result.unavailable_reason in _SAFE_PREFLIGHT_REASONS
        else "unavailable"
    )


def _preflight_sample_payload(
    preflight_result: SnowflakePreflightResult,
    definition: CustomImportDefinition,
    limits: SnowflakePreflightLimits,
    complete_families: tuple[RootFamily, ...],
) -> dict[str, object]:
    if preflight_result.status == "complete":
        return {
            "sample": {
                "families": [_preflight_family_payload(family, definition, limits) for family in complete_families]
            }
        }
    return {"rejection_diagnostics": _rejection_diagnostic_payloads(preflight_result, limits)}


def _preflight_family_payload(
    family: RootFamily,
    definition: CustomImportDefinition,
    limits: SnowflakePreflightLimits,
) -> dict[str, object]:
    child_field_ids_by_name = {
        collection.name: tuple(field.field_id for field in definition.fields if field.collection == collection.name)
        for collection in definition.child_collections
    }
    if not isinstance(family.children, Mapping) or set(family.children) != set(child_field_ids_by_name):
        raise ValueError("preflight family children are invalid")
    return {
        "root": _preflight_record(family.root, tuple(field.field_id for field in definition.root_fields)),
        "children": {
            collection_name: _preflight_child_records(
                family.children[collection_name],
                field_ids,
                limits,
            )
            for collection_name, field_ids in child_field_ids_by_name.items()
        },
    }


def _preflight_child_records(
    records: object,
    field_ids: tuple[str, ...],
    limits: SnowflakePreflightLimits,
) -> list[dict[str, object]]:
    if not isinstance(records, tuple) or len(records) > limits.maximum_child_rows:
        raise ValueError("preflight child records are invalid")
    return [_preflight_record(record, field_ids) for record in records]


def _preflight_record(record: object, field_ids: tuple[str, ...]) -> dict[str, object]:
    if not isinstance(record, Mapping) or set(record) != set(field_ids):
        raise ValueError("preflight record is invalid")
    return {field_id: _preflight_scalar(record[field_id]) for field_id in field_ids}


def _preflight_scalar(value: object) -> str | int | bool | None:
    if value is None or isinstance(value, (str, bool)) or type(value) is int:
        return value
    if isinstance(value, Decimal) and value.is_finite():
        return format(value, "f")
    raise ValueError("preflight scalar is invalid")


def _rejection_diagnostic_payloads(
    preflight_result: SnowflakePreflightResult,
    limits: SnowflakePreflightLimits,
) -> list[dict[str, object]]:
    diagnostics = preflight_result.rejection_diagnostics
    if not isinstance(diagnostics, tuple) or len(diagnostics) > limits.maximum_root_keys:
        raise ValueError("preflight diagnostics are invalid")
    return [_preflight_diagnostic(diagnostic) for diagnostic in diagnostics]


def _preflight_diagnostic(diagnostic: object) -> dict[str, object]:
    if (
        not isinstance(diagnostic, SnowflakePreflightRejectionDiagnostic)
        or not isinstance(diagnostic.root_key, tuple)
        or not diagnostic.root_key
        or _GENERIC_REJECTION_CODE.fullmatch(diagnostic.code) is None
    ):
        raise ValueError("preflight diagnostic is invalid")
    return {
        "root_key": [_preflight_scalar(value) for value in diagnostic.root_key],
        "code": diagnostic.code,
    }


async def _disconnect_preflight_database(database: Any, *, has_primary_failure: bool) -> None:
    """Finish one disconnect without masking a body failure or first SIGINT."""

    disconnect_task = asyncio.create_task(database.disconnect())
    try:
        await asyncio.shield(disconnect_task)
    except asyncio.CancelledError as cancellation:
        if disconnect_task.cancelled():
            if not has_primary_failure:
                raise
            return
        try:
            await asyncio.shield(disconnect_task)
        except Exception:
            if has_primary_failure:
                return
        if not has_primary_failure:
            raise cancellation
    except Exception:
        if not has_primary_failure:
            raise


async def _preflight_retained_snowflake_binding(
    *,
    definition_revision_id: int,
    source_binding_revision_id: int,
    limits: SnowflakePreflightLimits,
    include_sample: bool,
    database=db,
) -> str:
    """Run one bounded retained-binding preflight without lifecycle writes."""

    with _receipt_only_database_output(database):
        engine = None
        previous_echo = _MISSING
        has_primary_failure = False
        try:
            await database.connect()
            engine = getattr(database, "engine", None)
            previous_echo = getattr(engine, "echo", _MISSING)
            _set_engine_echo(database, False)
            async with database.session() as session:
                loaded_binding = await load_snowflake_source_binding(
                    session,
                    definition_revision_id=definition_revision_id,
                    source_binding_revision_id=source_binding_revision_id,
                )
            preflight_result = _run_snowflake_preflight(loaded_binding, limits)
            # Let a pending first SIGINT become primary before cleanup awaits.
            await asyncio.sleep(0)
            rendered = _preflight_receipt(
                preflight_result,
                definition=loaded_binding.definition,
                source_binding_sha256=loaded_binding.source_binding_sha256,
                limits=limits,
                include_sample=include_sample,
            )
            # Receipt rendering is also synchronous before the cleanup boundary.
            await asyncio.sleep(0)
            return rendered
        except BaseException:
            has_primary_failure = True
            raise
        finally:
            if previous_echo is not _MISSING:
                engine.echo = previous_echo
            await _disconnect_preflight_database(database, has_primary_failure=has_primary_failure)


def _run_snowflake_preflight(loaded_binding: Any, limits: SnowflakePreflightLimits) -> SnowflakePreflightResult:
    """Compose the fixed local credential and generated preflight adapter."""

    source_adapter = SnowflakePythonConnectorAdapter(
        role=loaded_binding.binding.role,
        warehouse=loaded_binding.binding.warehouse,
    )
    with FixedLocalKeyPairCredentialProvider(FIXED_CREDENTIAL_DIRECTORY) as credential_provider:
        bundle_connector = SnowflakeBundleAcquisitionConnector(
            approved_relations=loaded_binding.approved_relations,
            credential_provider=credential_provider,
            adapter=source_adapter,
        )
        preflight_adapter = SnowflakePythonPreflightAdapter(
            connector=source_adapter,
            credential_provider=credential_provider,
        )
        return preflight_snowflake_bundle(
            loaded_binding.definition,
            loaded_binding.binding,
            bundle_connector,
            preflight_adapter,
            limits=limits,
        )


def _run_preflight_command(parsed: argparse.Namespace, limits: SnowflakePreflightLimits | None) -> str:
    """Run one bounded preflight after fixed parser validation."""

    if limits is None:
        raise ValueError("preflight limits are unavailable")
    return asyncio.run(
        _preflight_retained_snowflake_binding(
            definition_revision_id=parsed.definition_revision_id,
            source_binding_revision_id=parsed.source_binding_revision_id,
            limits=limits,
            include_sample=parsed.include_sample,
        )
    )


async def _run_retained_snowflake_binding(
    *,
    definition_revision_id: int,
    source_binding_revision_id: int,
    idempotency_key: str,
    database=db,
) -> CandidateRunResult:
    with _receipt_only_database_output(database):
        engine = None
        previous_echo = _MISSING
        has_primary_failure = False
        try:
            await database.connect()
            engine = getattr(database, "engine", None)
            previous_echo = getattr(engine, "echo", _MISSING)
            _set_engine_echo(database, False)
            async with database.session() as session:
                loaded = await load_snowflake_source_binding(
                    session,
                    definition_revision_id=definition_revision_id,
                    source_binding_revision_id=source_binding_revision_id,
                )
            adapter = SnowflakePythonConnectorAdapter(role=loaded.binding.role, warehouse=loaded.binding.warehouse)
            with FixedLocalKeyPairCredentialProvider(FIXED_CREDENTIAL_DIRECTORY) as credential_provider:
                connector = SnowflakeBundleAcquisitionConnector(
                    approved_relations=loaded.approved_relations,
                    credential_provider=credential_provider,
                    adapter=adapter,
                )
                bundle_request = connector.prepare_request(loaded.definition, bindings=loaded.bundle_bindings)
                request = SnowflakeBundleCandidateRequest(
                    dataset_id=loaded.dataset_id,
                    definition_revision_id=loaded.definition_revision_id,
                    schema_revision_id=loaded.schema_revision_id,
                    definition=loaded.definition,
                    bundle_request=bundle_request,
                    idempotency_key=idempotency_key,
                    lease_token=secrets.token_urlsafe(32),
                    source_binding_revision_id=loaded.source_binding_revision_id,
                    source_binding_sha256=loaded.source_binding_sha256,
                )
                return await run_snowflake_bundle_candidate(database.session, connector, request)
        except BaseException:
            has_primary_failure = True
            raise
        finally:
            if previous_echo is not _MISSING:
                engine.echo = previous_echo
            try:
                await database.disconnect()
            except Exception:
                if not has_primary_failure:
                    raise


def _resume_source_access_forbidden(*_args: object, **_kwargs: object) -> None:
    raise AssertionError("resume must not acquire a source")


def _resume_connector(loaded: Any) -> SnowflakeBundleAcquisitionConnector:
    """Build statements from the retained allowlist without credential access."""

    return SnowflakeBundleAcquisitionConnector(
        approved_relations=loaded.approved_relations,
        credential_provider=SimpleNamespace(load_key_pair=_resume_source_access_forbidden),
        adapter=SimpleNamespace(fetch_bundle=_resume_source_access_forbidden),
    )


def _is_resumable_execution(
    execution: Any,
    lease: Any,
    submission: Any,
    loaded: Any,
    idempotency_key: str,
    request_identity_sha256: bytes,
    now: dt.datetime,
) -> bool:
    """Require one exact, capture-bound execution whose database lease expired."""

    return (
        submission is not None
        and getattr(submission, "state", None) in {"running", "canceling"}
        and type(getattr(submission, "capture_bundle_id", None)) is int
        and submission.capture_bundle_id > 0
        and execution is not None
        and execution.execution_id == submission.execution_id
        and execution.dataset_id == loaded.dataset_id
        and execution.definition_revision_id == loaded.definition_revision_id
        and execution.schema_revision_id == loaded.schema_revision_id
        and execution.source_binding_revision_id == loaded.source_binding_revision_id
        and execution.idempotency_key == idempotency_key
        and execution.mechanism == "local"
        and execution.state == submission.state
        and execution.capture_bundle_id == submission.capture_bundle_id
        and isinstance(execution.request_identity_sha256, (bytes, bytearray, memoryview))
        and bytes(execution.request_identity_sha256) == request_identity_sha256
        and lease is not None
        and lease.execution_id == submission.execution_id
        and type(lease.fence) is int
        and lease.fence > 0
        and isinstance(lease.expires_at, dt.datetime)
        and lease.expires_at.tzinfo is not None
        and lease.expires_at <= now
    )


async def _resumable_snowflake_candidate(
    session: AsyncSession,
    *,
    definition_revision_id: int,
    source_binding_revision_id: int,
    idempotency_key: str,
) -> tuple[SnowflakeBundleAcquisitionConnector, SnowflakeBundleCandidateRequest]:
    """Load an exact retained request whose capture can be replayed."""

    loaded_binding = await load_snowflake_source_binding(
        session,
        definition_revision_id=definition_revision_id,
        source_binding_revision_id=source_binding_revision_id,
    )
    bundle_connector = _resume_connector(loaded_binding)
    bundle_request = bundle_connector.prepare_request(
        loaded_binding.definition, bindings=loaded_binding.bundle_bindings
    )
    request_identity_sha256 = bundle_request_identity_sha256(
        bundle_request,
        bundle_connector.build_statement(bundle_request),
        source_binding_sha256=loaded_binding.source_binding_sha256,
    )
    await _require_exact_resumable_execution(
        session,
        loaded_binding=loaded_binding,
        idempotency_key=idempotency_key,
        request_identity_sha256=request_identity_sha256,
    )
    return bundle_connector, SnowflakeBundleCandidateRequest(
        dataset_id=loaded_binding.dataset_id,
        definition_revision_id=loaded_binding.definition_revision_id,
        schema_revision_id=loaded_binding.schema_revision_id,
        definition=loaded_binding.definition,
        bundle_request=bundle_request,
        idempotency_key=idempotency_key,
        lease_token=secrets.token_urlsafe(32),
        source_binding_revision_id=loaded_binding.source_binding_revision_id,
        source_binding_sha256=loaded_binding.source_binding_sha256,
    )


async def _require_exact_resumable_execution(
    session: AsyncSession,
    *,
    loaded_binding: Any,
    idempotency_key: str,
    request_identity_sha256: bytes,
) -> None:
    """Require an exact execution identity with a retained expired capture."""

    try:
        execution_submission = await lookup_execution_request(
            session,
            dataset_id=loaded_binding.dataset_id,
            definition_revision_id=loaded_binding.definition_revision_id,
            schema_revision_id=loaded_binding.schema_revision_id,
            idempotency_key=idempotency_key,
            mechanism="local",
            request_identity_sha256=request_identity_sha256,
            source_binding_revision_id=loaded_binding.source_binding_revision_id,
        )
    except IdempotencyConflict as exc:
        raise _ResumeUnavailableError("execution identity is unavailable") from exc
    if execution_submission is None:
        raise _ResumeUnavailableError("execution is unavailable")
    execution_row = await session.get(CustomImportExecution, execution_submission.execution_id)
    lease_row = await session.get(CustomImportLease, execution_submission.execution_id)
    database_time = await database_now(session)
    if not _is_resumable_execution(
        execution_row,
        lease_row,
        execution_submission,
        loaded_binding,
        idempotency_key,
        request_identity_sha256,
        database_time,
    ):
        raise _ResumeUnavailableError("execution is unavailable")


async def _run_resumed_snowflake_binding(
    *,
    definition_revision_id: int,
    source_binding_revision_id: int,
    idempotency_key: str,
    database=db,
) -> CandidateRunResult:
    """Replay only an exact expired source capture; never acquire a new source."""

    with _receipt_only_database_output(database):
        engine = None
        previous_echo = _MISSING
        has_primary_failure = False
        try:
            await database.connect()
            engine = getattr(database, "engine", None)
            previous_echo = getattr(engine, "echo", _MISSING)
            _set_engine_echo(database, False)
            async with database.session() as session, session.begin():
                bundle_connector, candidate_request = await _resumable_snowflake_candidate(
                    session,
                    definition_revision_id=definition_revision_id,
                    source_binding_revision_id=source_binding_revision_id,
                    idempotency_key=idempotency_key,
                )
            candidate_result = await run_snowflake_bundle_candidate(
                database.session,
                bundle_connector,
                candidate_request,
            )
            if candidate_result.status == "not_claimed":
                raise _ResumeUnavailableError("execution lease is unavailable")
            return candidate_result
        except BaseException:
            has_primary_failure = True
            raise
        finally:
            if previous_echo is not _MISSING:
                engine.echo = previous_echo
            try:
                await database.disconnect()
            except Exception:
                if not has_primary_failure:
                    raise


def run_command(arguments: Sequence[str] | None = None, *, stream: Any | None = None) -> int:
    """Register or run a retained binding while emitting only compact safe receipts."""

    parser = _parser()
    parsed = parser.parse_args(arguments)
    try:
        preflight_limits = _preflight_limits(parsed) if parsed.command == "preflight" else None
    except SnowflakePreflightError:
        parser.error("invalid")
    try:
        if parsed.command == "register":
            rendered = asyncio.run(_register_snowflake_binding(stream=stream))
        elif parsed.command == "execute":
            candidate_result = asyncio.run(
                _run_retained_snowflake_binding(
                    definition_revision_id=parsed.definition_revision_id,
                    source_binding_revision_id=parsed.source_binding_revision_id,
                    idempotency_key=parsed.idempotency_key,
                )
            )
            rendered = _receipt(candidate_result)
        elif parsed.command == "resume":
            candidate_result = asyncio.run(
                _run_resumed_snowflake_binding(
                    definition_revision_id=parsed.definition_revision_id,
                    source_binding_revision_id=parsed.source_binding_revision_id,
                    idempotency_key=parsed.idempotency_key,
                )
            )
            rendered = _receipt(candidate_result)
        else:
            rendered = _run_preflight_command(parsed, preflight_limits)
        print(rendered)
        return 0
    except KeyboardInterrupt:
        print(_error_json("canceled"), file=sys.stderr)
        return 130
    except DefinitionRegistrationError, SnowflakeSourceBindingError:
        code = "invalid_registration" if parsed.command == "register" else "source_binding_unavailable"
        print(_error_json(code), file=sys.stderr)
        return 1
    except _ResumeUnavailableError:
        print(_error_json("failed"), file=sys.stderr)
        return 1
    except Exception:
        print(_error_json("failed"), file=sys.stderr)
        return 1
