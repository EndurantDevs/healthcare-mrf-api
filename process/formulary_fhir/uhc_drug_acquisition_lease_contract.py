# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Pure and database-setting contracts for durable source acquisition."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any, TypeVar

from process.formulary_fhir.repository_shared import row_mapping
from process.formulary_fhir.repository_shared import strict_hash
from process.formulary_fhir.repository_shared import strict_text


DEFAULT_LEASE_SECONDS = 300
DEFAULT_HEARTBEAT_SECONDS = 30.0
DEFAULT_HEARTBEAT_TIMEOUT_SECONDS = 15.0
FAILURE_DRAIN_WINDOW_SECONDS = 60.0
MIN_LEASE_SECONDS = 1
MAX_LEASE_SECONDS = 3_600

ACTION_SETTING = "healthporta.formulary_source_acquisition_action"
SOURCE_SETTING = "healthporta.formulary_source_acquisition_source"
GENERATION_SETTING = "healthporta.formulary_source_acquisition_generation"
TOKEN_SETTING = "healthporta.formulary_source_acquisition_token"
LEASE_ERROR_CODES = frozenset({"busy", "lease_lost", "state"})

ResultT = TypeVar("ResultT")


class UHCDrugSourceAcquisitionLeaseError(RuntimeError):
    """Carry one bounded source-lease failure without source or token values."""

    def __init__(self, code: str) -> None:
        self.code = code if code in LEASE_ERROR_CODES else "state"
        super().__init__("UHC drug source acquisition lease failed")


@dataclass(frozen=True, slots=True, repr=False)
class UHCDrugSourceAcquisitionClaim:
    """Identify exactly one live generation of a reusable source lease."""

    source_id: str
    lease_generation: int
    lease_token: str = field(repr=False)

    def __post_init__(self) -> None:
        strict_text(self.source_id, "source id", 64)
        strict_hash(self.lease_token, "source acquisition lease token")
        if type(self.lease_generation) is not int or self.lease_generation < 1:
            raise ValueError("FHIR formulary source acquisition claim is invalid")

    def __repr__(self) -> str:
        return (
            "UHCDrugSourceAcquisitionClaim("
            f"source_id={self.source_id!r}, "
            f"lease_generation={self.lease_generation})"
        )


LeaseOperation = Callable[
    [UHCDrugSourceAcquisitionClaim],
    Awaitable[ResultT],
]


def _lease_seconds(value: object) -> int:
    if type(value) is not int or not MIN_LEASE_SECONDS <= value <= MAX_LEASE_SECONDS:
        raise ValueError("FHIR formulary source acquisition lease is invalid")
    return value


def _positive_seconds(value: object, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"FHIR formulary source acquisition {label} is invalid")
    seconds = float(value)
    if not 0.0 < seconds <= float(MAX_LEASE_SECONDS):
        raise ValueError(f"FHIR formulary source acquisition {label} is invalid")
    return seconds


def _validate_supervision_window(
    lease_seconds: int,
    heartbeat_seconds: float,
    heartbeat_timeout_seconds: float,
    failure_drain_seconds: float,
) -> None:
    if lease_seconds <= (
        heartbeat_seconds + heartbeat_timeout_seconds + failure_drain_seconds
    ):
        raise ValueError(
            "FHIR formulary source acquisition supervision window is invalid"
        )


async def _set_action(
    database: Any,
    action: str,
    *,
    source_id: str,
    lease_generation: int | None,
    lease_token: str,
) -> None:
    await database.scalar(
        "SELECT pg_catalog.set_config(:action_key, :action, true) || "
        "pg_catalog.set_config(:source_key, :source_id, true) || "
        "pg_catalog.set_config(:generation_key, :lease_generation, true) || "
        "pg_catalog.set_config(:token_key, :lease_token, true);",
        action_key=ACTION_SETTING,
        action=action,
        source_key=SOURCE_SETTING,
        source_id=source_id,
        generation_key=GENERATION_SETTING,
        lease_generation=("" if lease_generation is None else str(lease_generation)),
        token_key=TOKEN_SETTING,
        lease_token=lease_token,
    )


def _claim_from_row(database_row: Any) -> UHCDrugSourceAcquisitionClaim:
    fields = row_mapping(database_row)
    if not fields:
        raise UHCDrugSourceAcquisitionLeaseError("busy")
    try:
        return UHCDrugSourceAcquisitionClaim(
            source_id=fields.get("source_id"),
            lease_generation=fields.get("lease_generation"),
            lease_token=fields.get("lease_token"),
        )
    except (TypeError, ValueError):
        raise UHCDrugSourceAcquisitionLeaseError("state") from None


__all__ = (
    "DEFAULT_HEARTBEAT_SECONDS",
    "DEFAULT_HEARTBEAT_TIMEOUT_SECONDS",
    "DEFAULT_LEASE_SECONDS",
    "FAILURE_DRAIN_WINDOW_SECONDS",
    "LeaseOperation",
    "ResultT",
    "UHCDrugSourceAcquisitionClaim",
    "UHCDrugSourceAcquisitionLeaseError",
)
