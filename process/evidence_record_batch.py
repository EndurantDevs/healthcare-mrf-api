# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded, integrity-only batches of phase-one public evidence records."""

from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import hmac
import json
from typing import Literal, Mapping

from process.evidence_record_values import (
    PUBLIC_EVIDENCE_RECORD_CONTRACT,
    PublicEvidenceRecordError,
    _fail,
    _strict_prefixed_digest,
    _validated_release,
)
from process.evidence_source_release_contract import (
    PublicEvidenceSourceReleaseDescriptor,
)

PUBLIC_EVIDENCE_BATCH_MAX_RECORDS = 1_024
_BATCH_DOMAIN = b"HEALTHPORTA_PUBLIC_EVIDENCE_BATCH_V1\x00"


def _canonical_digest(domain: bytes, payload: Mapping[str, object], prefix: str) -> str:
    encoded = json.dumps(
        payload, ensure_ascii=True, separators=(",", ":"), sort_keys=True
    ).encode("ascii")
    digest = hashlib.sha256(
        domain + len(encoded).to_bytes(8, "big") + encoded
    ).hexdigest()
    return prefix + digest


def _batch_digest(
    release: PublicEvidenceSourceReleaseDescriptor, records: tuple[object, ...]
) -> str:
    return _canonical_digest(
        _BATCH_DOMAIN,
        {
            "contract": PUBLIC_EVIDENCE_RECORD_CONTRACT,
            "release_contract_sha256": release.contract_sha256,
            "record_count": len(records),
            "evidence_ids": [record.evidence_id for record in records],
            "positive_evidence_only": True,
        },
        "evb1_",
    )


def _validated_batch_records(
    release: PublicEvidenceSourceReleaseDescriptor, values: object
) -> tuple[object, ...]:
    if type(values) is not tuple or len(values) > PUBLIC_EVIDENCE_BATCH_MAX_RECORDS:
        raise _fail()
    from process.evidence_record_contract import validate_public_evidence_record

    records = tuple(validate_public_evidence_record(value) for value in values)
    if any(
        not hmac.compare_digest(record.release.contract_sha256, release.contract_sha256)
        for record in records
    ):
        raise _fail()
    evidence_ids = tuple(record.evidence_id for record in records)
    if evidence_ids != tuple(sorted(evidence_ids)) or len(set(evidence_ids)) != len(
        records
    ):
        raise _fail()
    return records


@dataclass(frozen=True, slots=True, repr=False)
class PublicEvidenceBatch:
    release: PublicEvidenceSourceReleaseDescriptor
    records: tuple[object, ...]
    record_count: int
    batch_id: str
    contract: str = field(default=PUBLIC_EVIDENCE_RECORD_CONTRACT, init=False)
    positive_evidence_only: Literal[True] = field(default=True, init=False)
    serving_authority: Literal["none"] = field(default="none", init=False)
    deletion_enabled: Literal[False] = field(default=False, init=False)
    replacement_enabled: Literal[False] = field(default=False, init=False)
    publication_enabled: Literal[False] = field(default=False, init=False)

    def __post_init__(self) -> None:
        if (
            type(self.records) is not tuple
            or len(self.records) > PUBLIC_EVIDENCE_BATCH_MAX_RECORDS
        ):
            raise _fail()
        release = _validated_release(self.release)
        records = _validated_batch_records(release, self.records)
        if type(self.record_count) is not int or self.record_count != len(records):
            raise _fail()
        supplied_id = _strict_prefixed_digest(self.batch_id, "evb1_")
        if not hmac.compare_digest(supplied_id, _batch_digest(release, records)):
            raise _fail()
        object.__setattr__(self, "release", release)
        object.__setattr__(self, "records", records)


def build_public_evidence_batch(
    release: PublicEvidenceSourceReleaseDescriptor,
    evidence_records: tuple[object, ...],
) -> PublicEvidenceBatch:
    """Validate, sort, and freeze at most 1,024 records for one release."""
    try:
        if (
            type(evidence_records) is not tuple
            or len(evidence_records) > PUBLIC_EVIDENCE_BATCH_MAX_RECORDS
        ):
            raise _fail()
        fixed_release = _validated_release(release)
        from process.evidence_record_contract import validate_public_evidence_record

        ordered_records = tuple(
            sorted(
                map(validate_public_evidence_record, evidence_records),
                key=lambda evidence_record: evidence_record.evidence_id,
            )
        )
        evidence_ids = {
            evidence_record.evidence_id for evidence_record in ordered_records
        }
        if len(evidence_ids) != len(ordered_records) or any(
            not hmac.compare_digest(
                evidence_record.release.contract_sha256,
                fixed_release.contract_sha256,
            )
            for evidence_record in ordered_records
        ):
            raise _fail()
        return PublicEvidenceBatch(
            fixed_release,
            ordered_records,
            len(ordered_records),
            _batch_digest(fixed_release, ordered_records),
        )
    except PublicEvidenceRecordError:
        raise
    except Exception:
        raise _fail() from None


def validate_public_evidence_batch(value: object) -> PublicEvidenceBatch:
    """Integrity-only validation; it grants no serving/publication authority."""
    if type(value) is not PublicEvidenceBatch:
        raise _fail()
    try:
        if (
            type(value.contract) is not str
            or value.contract != PUBLIC_EVIDENCE_RECORD_CONTRACT
            or value.positive_evidence_only is not True
            or type(value.serving_authority) is not str
            or value.serving_authority != "none"
            or value.deletion_enabled is not False
            or value.replacement_enabled is not False
            or value.publication_enabled is not False
        ):
            raise _fail()
        return PublicEvidenceBatch(
            value.release, value.records, value.record_count, value.batch_id
        )
    except PublicEvidenceRecordError:
        raise
    except Exception:
        raise _fail() from None


__all__ = [
    "PUBLIC_EVIDENCE_BATCH_MAX_RECORDS",
    "PublicEvidenceBatch",
    "build_public_evidence_batch",
    "validate_public_evidence_batch",
]
