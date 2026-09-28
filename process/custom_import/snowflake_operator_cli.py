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
from process.custom_import.runner import CandidateRunResult
from process.custom_import.runner_registry import database_now
from process.custom_import.snowflake import FixedLocalKeyPairCredentialProvider
from process.custom_import.snowflake_bundle import SnowflakeBundleAcquisitionConnector
from process.custom_import.snowflake_candidate import (
    SnowflakeBundleCandidateRequest,
    bundle_request_identity_sha256,
    run_snowflake_bundle_candidate,
)
from process.custom_import.snowflake_python import SnowflakePythonConnectorAdapter
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
    try:
        await database.connect()
        async with database.session() as session, session.begin():
            result = await register_snowflake_source_binding(
                session,
                dataset_key=dataset_key,
                definition=definition,
                binding=binding,
            )
        async with database.session() as session:
            return await _committed_registration_receipt(
                session,
                dataset_key=dataset_key,
                registration=result,
                definition=definition,
                binding=binding,
            )
    finally:
        await database.disconnect()


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


async def _run_retained_snowflake_binding(
    *,
    definition_revision_id: int,
    source_binding_revision_id: int,
    idempotency_key: str,
    database=db,
) -> CandidateRunResult:
    await database.connect()
    try:
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
    finally:
        await database.disconnect()


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

    parsed = _parser().parse_args(arguments)
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
        else:
            candidate_result = asyncio.run(
                _run_resumed_snowflake_binding(
                    definition_revision_id=parsed.definition_revision_id,
                    source_binding_revision_id=parsed.source_binding_revision_id,
                    idempotency_key=parsed.idempotency_key,
                )
            )
            rendered = _receipt(candidate_result)
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
