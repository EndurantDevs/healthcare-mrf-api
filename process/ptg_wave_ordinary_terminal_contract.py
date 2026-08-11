"""Closed request and digest contract for ordinary terminal receipts."""

from __future__ import annotations

import datetime as dt
import re
from collections.abc import Mapping
from typing import Any

from process.ptg_wave_receipt_authority import (
    PTGWaveReceiptAuthorityError,
    canonical_receipt_timestamp,
    require_receipt_key_id,
)
from process.ptg_wave_state import canonical_json, sha256_digest


ORDINARY_TERMINAL_REQUEST_SCHEMA = (
    "healthporta.ptg-wave-ordinary-terminal-receipt-request.v1"
)
COORDINATE_DIGEST_DOMAIN = (
    "healthporta.ptg-wave-ordinary-terminal-coordinate.v1"
)
SCOPE_DIGEST_DOMAIN = "healthporta.ptg-wave-ordinary-terminal-scope.v1"
TERMINAL_RESULT_DIGEST_DOMAIN = (
    "healthporta.ptg-wave-ordinary-terminal-result.v1"
)
RUN_PARAMS_DIGEST_DOMAIN = (
    "healthporta.ptg-wave-ordinary-terminal-run-params.v1"
)
RUN_METRICS_DIGEST_DOMAIN = (
    "healthporta.ptg-wave-ordinary-terminal-run-metrics.v1"
)
ENGINE_OPTIONS_DIGEST_DOMAIN = (
    "healthporta.ptg-wave-ordinary-terminal-engine-options.v1"
)
ENGINE_REPORT_DIGEST_DOMAIN = (
    "healthporta.ptg-wave-ordinary-terminal-engine-report.v1"
)
SNAPSHOT_MANIFEST_DIGEST_DOMAIN = (
    "healthporta.ptg-wave-ordinary-terminal-snapshot-manifest.v1"
)

REQUEST_FIELDS = frozenset(
    {
        "schema",
        "key_id",
        "operation_id",
        "member_ordinal",
        "source_file_import_id",
        "run_id",
    }
)
COORDINATE_FIELDS = frozenset(
    {
        "source_file_id",
        "content_version",
        "import_month",
        "historical_source_file_import_id",
        "direct_input_digest",
    }
)
SCOPE_FIELDS = frozenset(
    {
        "plan_ids",
        "plan_market_types",
        "admission_plan_ids",
        "admission_plan_market_types",
        "authorization_digest",
        "membership_digest",
        "subscription_coverage_digest",
        "entitlement_coverage_digest",
        "entitlement_coverage_count",
    }
)
TERMINAL_RESULT_FIELDS = frozenset(
    {
        "engine",
        "importer",
        "status",
        "engine_result_status",
        "source_file_import_id",
        "run_id",
        "node_id",
        "source_key",
        "snapshot_id",
        "engine_import_run_id",
        "import_month",
        "finished_at",
        "run_params_digest",
        "run_metrics_digest",
        "engine_options_digest",
        "engine_report_digest",
        "snapshot_manifest_digest",
    }
)
ORDINARY_TERMINAL_PAYLOAD_FIELDS = frozenset(
    {
        "operation_id",
        "cutover_id",
        "wave_id",
        "wave_digest",
        "member_ordinal",
        "source_file_import_id",
        "run_id",
        "node_id",
        "source_key",
        "snapshot_id",
        "coordinate",
        "coordinate_digest",
        "scope",
        "scope_digest",
        "terminal_result",
        "terminal_result_digest",
        "abandonment_receipt_payload_digest",
        "recovery_evidence_sha256",
    }
)

_HEX_64 = re.compile(r"[0-9a-f]{64}\Z")
_MONTH = re.compile(r"[0-9]{4}-[0-9]{2}\Z")


class PTGWaveOrdinaryTerminalConflict(ValueError):
    """The requested ordinary run is not one exact V12 member result."""


class PTGWaveOrdinaryTerminalRetryable(RuntimeError):
    """A bounded database wait prevented this member-local receipt attempt."""

    retryable = True


def validate_ordinary_terminal_request(
    request: object,
    *,
    operation_id: object | None = None,
) -> dict[str, Any]:
    """Validate the closed request without trusting result assertions."""

    if not isinstance(request, Mapping) or set(request) != REQUEST_FIELDS:
        raise PTGWaveOrdinaryTerminalConflict(
            "ordinary terminal receipt request fields are invalid"
        )
    if request.get("schema") != ORDINARY_TERMINAL_REQUEST_SCHEMA:
        raise PTGWaveOrdinaryTerminalConflict(
            "ordinary terminal receipt request schema is unsupported"
        )
    validated_request_by_field = dict(request)
    validated_request_by_field["key_id"] = _key_id(
        validated_request_by_field.get("key_id")
    )
    validated_request_by_field["operation_id"] = _digest(
        validated_request_by_field.get("operation_id"), "operation ID"
    )
    validated_request_by_field["member_ordinal"] = _ordinal(
        validated_request_by_field.get("member_ordinal")
    )
    validated_request_by_field["source_file_import_id"] = _text(
        validated_request_by_field.get("source_file_import_id"),
        "source-file import ID",
        64,
    )
    validated_request_by_field["run_id"] = _text(
        validated_request_by_field.get("run_id"), "run ID", 64
    )
    if (
        operation_id is not None
        and validated_request_by_field["operation_id"]
        != _digest(operation_id, "operation ID")
    ):
        raise PTGWaveOrdinaryTerminalConflict(
            "ordinary terminal request identifies another operation"
        )
    return validated_request_by_field


def _canonical_digest(domain: str, value: Mapping[str, Any]) -> str:
    return sha256_digest(domain.encode("ascii") + b"\0" + canonical_json(value))


def _object_digest(domain: str, value: object, name: str) -> str:
    return _canonical_digest(domain, _object(value, name))


def _object(value: object, name: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise PTGWaveOrdinaryTerminalConflict(f"{name} must be an object")
    return dict(value)


def _digest(value: object, name: str) -> str:
    if not isinstance(value, str) or _HEX_64.fullmatch(value) is None:
        raise PTGWaveOrdinaryTerminalConflict(f"{name} is invalid")
    return value


def _key_id(value: object) -> str:
    try:
        return require_receipt_key_id(value, "receipt key ID")
    except PTGWaveReceiptAuthorityError as exc:
        raise PTGWaveOrdinaryTerminalConflict(str(exc)) from exc


def _ordinal(value: object) -> int:
    if type(value) is not int or value < 0:
        raise PTGWaveOrdinaryTerminalConflict("member ordinal is invalid")
    return value


def _count(value: object, name: str) -> int:
    if type(value) is not int or value < 0:
        raise PTGWaveOrdinaryTerminalConflict(f"{name} is invalid")
    return value


def _text(value: object, name: str, max_bytes: int) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or len(value.encode("utf-8")) > max_bytes
    ):
        raise PTGWaveOrdinaryTerminalConflict(f"{name} is invalid")
    return value


def _month(value: object) -> str:
    if isinstance(value, (dt.date, dt.datetime)):
        value = value.strftime("%Y-%m")
    if isinstance(value, str) and re.fullmatch(r"[0-9]{4}-[0-9]{2}-01", value):
        value = value[:7]
    if not isinstance(value, str) or _MONTH.fullmatch(value) is None:
        raise PTGWaveOrdinaryTerminalConflict("import month is invalid")
    try:
        parsed = dt.datetime.strptime(value, "%Y-%m")
    except ValueError as exc:
        raise PTGWaveOrdinaryTerminalConflict("import month is invalid") from exc
    if parsed.strftime("%Y-%m") != value:
        raise PTGWaveOrdinaryTerminalConflict("import month is invalid")
    return value


def _string_list(value: object, name: str) -> list[str]:
    if (
        not isinstance(value, list)
        or not value
        or value != sorted(set(value))
        or any(
            not isinstance(entry, str)
            or not entry
            or entry != entry.strip()
            or len(entry.encode("utf-8")) > 128
            for entry in value
        )
    ):
        raise PTGWaveOrdinaryTerminalConflict(f"{name} are invalid")
    return list(value)


def _market_types(value: object) -> list[str]:
    market_types = _string_list(value, "plan market types")
    if market_types != ["group"]:
        raise PTGWaveOrdinaryTerminalConflict("plan market types are invalid")
    return market_types


def _receipt_datetime(value: str) -> dt.datetime:
    return dt.datetime.strptime(
        canonical_receipt_timestamp(value),
        "%Y-%m-%dT%H:%M:%S.%fZ",
    ).replace(tzinfo=dt.UTC)
