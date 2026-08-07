# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Resolve one exact billing code inside a canonical release binding."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from api import ptg2_serving
from api.plan_release_serving import (
    PLAN_RELEASE_IN_NETWORK_ROLE,
    PlanReleaseSnapshotBinding,
)
from api.plan_release_readiness import is_release_binding_serving_scope_exact
from api.ptg2_capacity_evidence import (
    CapacityEvidenceError,
    normalize_capacity_code,
    normalize_capacity_code_system,
)
from api.ptg2_types import PTG2ServingTables
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError

MAX_EXACT_BILLING_CODE_WITNESSES = 256
_MAX_METADATA_TEXT_CHARACTERS = 4096


@dataclass(frozen=True, slots=True, repr=False)
class BillingCodeWitness:
    """One exact code dictionary row scoped to a release binding."""

    code_key: int
    code_system: str
    code: str
    negotiation_arrangement: str | None
    billing_code_type_version: str | None
    source_name: str | None
    source_description: str | None

    @property
    def stable_sort_key(self) -> tuple[str, str, str, int]:
        """Return the complete deterministic dictionary coordinate."""

        return (
            self.code_system,
            self.code,
            self.negotiation_arrangement or "",
            self.code_key,
        )

    def __repr__(self) -> str:
        return (
            "<billing-code-witness "
            f"code_system={self.code_system} code={self.code} "
            "code_key=<internal>>"
        )


def _exact_code(code_system: object, code: object) -> tuple[str, str]:
    if type(code_system) is not str or type(code) is not str:
        raise PTG2ManifestArtifactError("PTG2 exact billing code is invalid")
    try:
        normalized_system = normalize_capacity_code_system(code_system)
        normalized_code = normalize_capacity_code(normalized_system, code)
    except CapacityEvidenceError as exc:
        raise PTG2ManifestArtifactError("PTG2 exact billing code is invalid") from exc
    if normalized_system != code_system or normalized_code != code:
        raise PTG2ManifestArtifactError("PTG2 exact billing code is not canonical")
    return normalized_system, normalized_code


def _optional_text(value: object, *, category: str) -> str | None:
    if value is None:
        return None
    if (
        type(value) is not str
        or len(value) > _MAX_METADATA_TEXT_CHARACTERS
        or not value.isprintable()
    ):
        raise PTG2ManifestArtifactError(
            f"PTG2 exact billing code {category} is malformed"
        )
    return value


def _code_key(value: object) -> int:
    if type(value) is not int or not 0 <= value <= 2**31 - 1:
        raise PTG2ManifestArtifactError(
            "PTG2 exact billing code dictionary key is malformed"
        )
    return value


def _billing_code_witness(
    raw_row: Mapping[str, Any],
    *,
    binding: PlanReleaseSnapshotBinding,
    code_system: str,
    code: str,
) -> BillingCodeWitness:
    if type(raw_row) is not dict:
        raise PTG2ManifestArtifactError(
            "PTG2 exact billing code dictionary row is malformed"
        )
    row_plan_id = raw_row.get("plan_id")
    row_market_type = raw_row.get("plan_market_type")
    row_system, row_code = _exact_code(
        raw_row.get("reported_code_system"),
        raw_row.get("reported_code"),
    )
    if (
        type(row_plan_id) is not str
        or row_plan_id != binding.plan_id
        or type(row_market_type) is not str
        or row_market_type != binding.plan_market_type
        or row_system != code_system
        or row_code != code
    ):
        raise PTG2ManifestArtifactError(
            "PTG2 exact billing code crossed its release binding"
        )
    return BillingCodeWitness(
        code_key=_code_key(raw_row.get("code_key")),
        code_system=row_system,
        code=row_code,
        negotiation_arrangement=_optional_text(
            raw_row.get("negotiation_arrangement"),
            category="negotiation arrangement",
        ),
        billing_code_type_version=_optional_text(
            raw_row.get("billing_code_type_version"),
            category="type version",
        ),
        source_name=_optional_text(
            raw_row.get("source_name"),
            category="source name",
        ),
        source_description=_optional_text(
            raw_row.get("source_description"),
            category="source description",
        ),
    )


def _validated_code_witnesses(
    rows: Iterable[Mapping[str, Any]],
    *,
    binding: PlanReleaseSnapshotBinding,
    code_system: str,
    code: str,
) -> tuple[BillingCodeWitness, ...]:
    witnesses: list[BillingCodeWitness] = []
    for raw_row in rows:
        if len(witnesses) >= MAX_EXACT_BILLING_CODE_WITNESSES:
            raise PTG2ManifestArtifactError(
                "PTG2 exact billing code scope exceeds its witness limit"
            )
        witnesses.append(
            _billing_code_witness(
                raw_row,
                binding=binding,
                code_system=code_system,
                code=code,
            )
        )
    if len({witness.code_key for witness in witnesses}) != len(witnesses):
        raise PTG2ManifestArtifactError(
            "PTG2 exact billing code scope contains duplicate keys"
        )
    return tuple(sorted(witnesses, key=lambda witness: witness.stable_sort_key))


def _validate_binding(
    serving_tables: PTG2ServingTables,
    binding: PlanReleaseSnapshotBinding,
) -> None:
    if (
        type(serving_tables) is not PTG2ServingTables
        or not serving_tables.uses_v4_graph
        or type(binding) is not PlanReleaseSnapshotBinding
        or binding.role != PLAN_RELEASE_IN_NETWORK_ROLE
        or serving_tables.snapshot_id != binding.snapshot_id
        or not is_release_binding_serving_scope_exact(serving_tables, binding)
        or not binding.plan_id
        or not binding.plan_market_type
    ):
        raise PTG2ManifestArtifactError(
            "PTG2 exact billing code requires one sealed network binding"
        )


async def load_exact_billing_code_witnesses(
    session,
    serving_tables: PTG2ServingTables,
    binding: PlanReleaseSnapshotBinding,
    *,
    code_system: object,
    code: object,
) -> tuple[BillingCodeWitness, ...]:
    """Load bounded code keys for one exact release/snapshot/plan coordinate."""

    _validate_binding(serving_tables, binding)
    normalized_system, normalized_code = _exact_code(code_system, code)
    code_metadata_rows = await ptg2_serving._manifest_reverse_code_rows(
        session,
        serving_tables,
        requested_plan=binding.plan_id,
        plan_market_type=binding.plan_market_type,
        code_value=normalized_code,
        code_system=normalized_system,
        q_text="",
        code_context=None,
        limit_rows=MAX_EXACT_BILLING_CODE_WITNESSES + 1,
        offset_rows=0,
    )
    if code_metadata_rows is None:
        raise PTG2ManifestArtifactError(
            "PTG2 exact billing code dictionary is unavailable"
        )
    return _validated_code_witnesses(
        code_metadata_rows,
        binding=binding,
        code_system=normalized_system,
        code=normalized_code,
    )


__all__ = [
    "MAX_EXACT_BILLING_CODE_WITNESSES",
    "BillingCodeWitness",
    "load_exact_billing_code_witnesses",
]
