# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Set-wise UHC NPI evidence reduction after bounded native COPY landing."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import heapq
import json
import re
from typing import Any, AsyncIterator, Iterable


_IDENTIFIER_RE = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")
UHC_EVIDENCE_ROW_KIND = 2
UHC_EVIDENCE_CONFLICT_FIELDS = (
    "accepting",
    "address_sets",
    "dates",
    "facility_names",
    "facility_types",
    "genders",
    "names",
    "provider_types",
    "specialties",
)
_SIGNATURE_OFFSETS = {
    "accepting": 1,
    "address_sets": 33,
    "dates": 65,
    "facility_names": 97,
    "facility_types": 129,
    "genders": 161,
    "names": 193,
    "provider_types": 225,
    "specialties": 257,
}


def _quoted_stage_ref(stage_ref: str) -> str:
    parts = stage_ref.split(".")
    if len(parts) != 2 or any(_IDENTIFIER_RE.fullmatch(part) is None for part in parts):
        raise ValueError("UHC semantic stage reference must be safe schema.table")
    return ".".join(f'"{part}"' for part in parts)


def uhc_evidence_summary_sql(stage_ref: str) -> str:
    """Build one external-sort/hash-friendly aggregate over staged evidence."""

    table = _quoted_stage_ref(stage_ref)
    distinct_counts = ",\n".join(
        "               count(DISTINCT substring("
        f"conflict_signature_pack FROM {offset} FOR 32"
        f"))::bigint AS {field}_values"
        for field, offset in _SIGNATURE_OFFSETS.items()
    )
    conflict_counts = ",\n".join(
        f"           count(*) FILTER (WHERE {field}_values > 1)::bigint "
        f"AS conflict_{field}"
        for field in UHC_EVIDENCE_CONFLICT_FIELDS
    )
    conflict_predicate = " OR ".join(
        f"{field}_values > 1" for field in UHC_EVIDENCE_CONFLICT_FIELDS
    )
    return f"""
        WITH per_npi AS (
            SELECT npi,
                   count(*)::bigint AS occurrence_count,
{distinct_counts}
              FROM {table}
             WHERE row_kind = {UHC_EVIDENCE_ROW_KIND}
             GROUP BY npi
        ),
        evidence_total AS (
            SELECT count(*)::bigint AS evidence_count
              FROM {table}
             WHERE row_kind = {UHC_EVIDENCE_ROW_KIND}
        )
        SELECT evidence_total.evidence_count,
               count(per_npi.npi)::bigint AS distinct_npis,
               count(*) FILTER (
                   WHERE occurrence_count > 1
               )::bigint AS duplicate_npi_groups,
               count(*) FILTER (
                   WHERE occurrence_count > 1
                     AND ({conflict_predicate})
               )::bigint AS conflicting_npi_groups,
{conflict_counts}
          FROM evidence_total
          LEFT JOIN per_npi ON true
         GROUP BY evidence_total.evidence_count
    """


@dataclass(frozen=True)
class UhcNpiEvidenceSummary:
    evidence_count: int
    distinct_npis: int
    duplicate_npi_groups: int
    conflicting_npi_groups: int
    conflict_counts: dict[str, int]
    proof_sha256: str = ""


def _nonnegative_int(row: Any, field: str) -> int:
    value = row[field]
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise RuntimeError(f"UHC semantic evidence {field} is not nonnegative")
    return value


def validate_uhc_evidence_summary(
    evidence_summary_row: Any,
    *,
    expected_evidence_count: int,
) -> UhcNpiEvidenceSummary:
    """Validate setwise NPI evidence aggregates against the sealed count."""

    if isinstance(expected_evidence_count, bool) or expected_evidence_count < 0:
        raise ValueError("expected UHC evidence count must be nonnegative")
    evidence_count = _nonnegative_int(evidence_summary_row, "evidence_count")
    distinct_npis = _nonnegative_int(evidence_summary_row, "distinct_npis")
    duplicate_npi_groups = _nonnegative_int(
        evidence_summary_row,
        "duplicate_npi_groups",
    )
    conflicting_npi_groups = _nonnegative_int(
        evidence_summary_row,
        "conflicting_npi_groups",
    )
    conflict_count_by_field = {
        field: _nonnegative_int(
            evidence_summary_row,
            f"conflict_{field}",
        )
        for field in UHC_EVIDENCE_CONFLICT_FIELDS
    }
    if evidence_count != expected_evidence_count:
        raise RuntimeError(
            "UHC semantic evidence rows do not match the sealed provider fact count"
        )
    if (
        distinct_npis > evidence_count
        or evidence_count < distinct_npis + duplicate_npi_groups
        or conflicting_npi_groups > duplicate_npi_groups
        or any(
            conflict_count > conflicting_npi_groups
            for conflict_count in conflict_count_by_field.values()
        )
    ):
        raise RuntimeError("UHC semantic evidence aggregate invariants do not hold")
    return UhcNpiEvidenceSummary(
        evidence_count=evidence_count,
        distinct_npis=distinct_npis,
        duplicate_npi_groups=duplicate_npi_groups,
        conflicting_npi_groups=conflicting_npi_groups,
        conflict_counts=conflict_count_by_field,
    )


async def summarize_uhc_npi_evidence(
    connection: Any,
    stage_ref: str,
    *,
    expected_evidence_count: int,
) -> UhcNpiEvidenceSummary:
    """Stream one indexed SEALED stage without a global GROUP BY/sort."""

    return await summarize_uhc_npi_evidence_stages(
        connection,
        (stage_ref,),
        expected_evidence_count=expected_evidence_count,
    )


def _evidence_query(stage_ref: str) -> str:
    return f"""
        SELECT npi, conflict_signature_pack
          FROM {_quoted_stage_ref(stage_ref)}
         WHERE row_kind = {UHC_EVIDENCE_ROW_KIND}
         ORDER BY npi
    """


async def _stage_evidence_rows(
    connection: Any,
    stage_ref: str,
) -> AsyncIterator[tuple[str, bytes]]:
    async for row in connection.cursor(_evidence_query(stage_ref), prefetch=512):
        npi = row["npi"]
        signature_pack = bytes(row["conflict_signature_pack"])
        if (
            not isinstance(npi, str)
            or re.fullmatch(r"[0-9]{10}", npi) is None
            or len(signature_pack) != 32 * len(UHC_EVIDENCE_CONFLICT_FIELDS)
        ):
            raise RuntimeError("UHC semantic evidence row is malformed")
        yield npi, signature_pack


async def _next_evidence(
    rows: AsyncIterator[tuple[str, bytes]],
) -> tuple[str, bytes] | None:
    try:
        return await anext(rows)
    except StopAsyncIteration:
        return None


async def _merged_evidence_rows(
    connection: Any,
    stage_refs: Iterable[str],
) -> AsyncIterator[tuple[str, bytes]]:
    iterators = [
        _stage_evidence_rows(connection, stage_ref).__aiter__()
        for stage_ref in sorted(stage_refs)
    ]
    pending_evidence_entries: list[tuple[str, int, bytes]] = []
    for index, rows in enumerate(iterators):
        evidence = await _next_evidence(rows)
        if evidence is not None:
            heapq.heappush(
                pending_evidence_entries,
                (evidence[0], index, evidence[1]),
            )
    while pending_evidence_entries:
        npi, index, signature_pack = heapq.heappop(
            pending_evidence_entries
        )
        yield npi, signature_pack
        evidence = await _next_evidence(iterators[index])
        if evidence is not None:
            heapq.heappush(
                pending_evidence_entries,
                (evidence[0], index, evidence[1]),
            )


class _EvidenceSummaryAccumulator:
    def __init__(self) -> None:
        self.evidence_count = 0
        self.distinct_npis = 0
        self.duplicate_npi_groups = 0
        self.conflicting_npi_groups = 0
        self.conflict_counts = dict.fromkeys(UHC_EVIDENCE_CONFLICT_FIELDS, 0)
        self.proof = hashlib.sha256()
        self.current_npi: str | None = None
        self.current_count = 0
        self.current_signatures = [set() for _ in UHC_EVIDENCE_CONFLICT_FIELDS]

    def observe(self, npi: str, signature_pack: bytes) -> None:
        """Merge one ordered NPI evidence row into bounded state."""

        if self.current_npi is not None and npi != self.current_npi:
            if npi < self.current_npi:
                raise RuntimeError("UHC semantic evidence merge order changed")
            self._finish_group()
        if self.current_npi is None:
            self.current_npi = npi
        self.current_count += 1
        self.evidence_count += 1
        for index, signatures in enumerate(self.current_signatures):
            start = index * 32
            signatures.add(signature_pack[start : start + 32])

    def _finish_group(self) -> None:
        if self.current_npi is None:
            return
        conflicts = [len(signatures) > 1 for signatures in self.current_signatures]
        self.distinct_npis += 1
        if self.current_count > 1:
            self.duplicate_npi_groups += 1
            if any(conflicts):
                self.conflicting_npi_groups += 1
        for field_name, has_conflict in zip(
            UHC_EVIDENCE_CONFLICT_FIELDS,
            conflicts,
            strict=True,
        ):
            if has_conflict:
                self.conflict_counts[field_name] += 1
        identity = json.dumps(
            [
                self.current_npi,
                self.current_count,
                [
                    sorted(signature.hex() for signature in signatures)
                    for signatures in self.current_signatures
                ],
            ],
            separators=(",", ":"),
        ).encode()
        if self.distinct_npis > 1:
            self.proof.update(b"\n")
        self.proof.update(identity)
        self.current_npi = None
        self.current_count = 0
        self.current_signatures = [set() for _ in UHC_EVIDENCE_CONFLICT_FIELDS]

    def complete(self, expected_evidence_count: int) -> UhcNpiEvidenceSummary:
        """Seal exact NPI counters after validating the evidence total."""

        self._finish_group()
        summary = validate_uhc_evidence_summary(
            {
                "evidence_count": self.evidence_count,
                "distinct_npis": self.distinct_npis,
                "duplicate_npi_groups": self.duplicate_npi_groups,
                "conflicting_npi_groups": self.conflicting_npi_groups,
                **{
                    f"conflict_{field_name}": count
                    for field_name, count in self.conflict_counts.items()
                },
            },
            expected_evidence_count=expected_evidence_count,
        )
        return UhcNpiEvidenceSummary(
            evidence_count=summary.evidence_count,
            distinct_npis=summary.distinct_npis,
            duplicate_npi_groups=summary.duplicate_npi_groups,
            conflicting_npi_groups=summary.conflicting_npi_groups,
            conflict_counts=summary.conflict_counts,
            proof_sha256=self.proof.hexdigest(),
        )


async def summarize_uhc_npi_evidence_stages(
    connection: Any,
    stage_refs: Iterable[str],
    *,
    expected_evidence_count: int,
) -> UhcNpiEvidenceSummary:
    """K-way merge native sorted evidence stages into bounded NPI proof."""

    selected_stage_refs = tuple(sorted(set(stage_refs)))
    if not selected_stage_refs and expected_evidence_count:
        raise RuntimeError("UHC semantic evidence stages are missing")
    accumulator = _EvidenceSummaryAccumulator()
    async with connection.transaction():
        async for npi, signature_pack in _merged_evidence_rows(
            connection,
            selected_stage_refs,
        ):
            accumulator.observe(npi, signature_pack)
    return accumulator.complete(expected_evidence_count)
