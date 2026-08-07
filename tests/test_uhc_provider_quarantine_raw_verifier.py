# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
import hashlib
import json
from pathlib import Path

import pytest

from process.uhc_provider_quarantine_contract import (
    UHC_PROVIDER_QUARANTINE_REASON_INVALID_NPI_CHECKSUM,
    UHC_PROVIDER_QUARANTINE_REASON_INVALID_NPI_STRUCTURE,
    UhcProviderQuarantine,
)
from process.uhc_provider_quarantine_raw_verifier import (
    UhcProviderQuarantineRawError,
    UhcProviderQuarantineRawSource,
    verify_provider_quarantine_source_records,
)
from process.uhc_provider_quarantine_record import (
    UhcProviderQuarantineRecordError,
    validate_checksum_invalid_provider_record,
    validate_structurally_invalid_provider_record,
)
from process.uhc_retained_range_manifest import (
    RANGE_CANONICALIZATION_ID,
    RANGE_CONTRACT_ID,
    RANGE_CONTRACT_VERSION,
    range_set_digest,
)
from process.uhc_retained_types import RawRangeProof


def _provider_record(
    npi: str,
    plan_years: tuple[int, ...] = (2026,),
) -> dict[str, object]:
    return {
        "type": "INDIVIDUAL",
        "npi": npi,
        "name": {"first": "Ada", "middle": None, "last": "Lovelace"},
        "facility_name": None,
        "facility_type": None,
        "gender": "F",
        "accepting": "accepting",
        "addresses": [
            {
                "address": "1 Main St",
                "city": "Chicago",
                "state": "IL",
                "zip": "60601",
                "phone": "3125551212",
            }
        ],
        "plans": [
            {
                "plan_id_type": "HIOS-PLAN-ID",
                "plan_id": "12345IL0010001",
                "years": list(plan_years),
                "network_tier": "PREFERRED",
            }
        ],
        "specialty": ["Family Medicine"],
        "last_updated_on": "2026-07-01",
    }


def _fixture_record_bytes(plan_years: tuple[int, ...]) -> list[bytes]:
    return [
        json.dumps(
            _provider_record(
                npi,
                plan_years if npi == "1003821381" else (2026,),
            ),
            separators=(",", ":"),
        ).encode()
        for npi in (
            ("1003821380", "1003821381", "1234567893", "1588616783")
        )
    ]


def _fixture_range_proofs(
    raw_path: Path,
    artifact_sha256: str,
    record_bytes: list[bytes],
) -> tuple[list[RawRangeProof], list[dict[str, object]]]:
    raw_range_proofs = []
    range_proof_maps = []
    offset = 1
    for ordinal, encoded_record in enumerate(record_bytes):
        raw_start = offset
        raw_end = raw_start + len(encoded_record)
        canonical = encoded_record.replace(b"\r", b"").replace(b"\n", b"") + b"\n"
        raw_range = RawRangeProof(
            artifact_sha256=artifact_sha256,
            contract_version=RANGE_CONTRACT_VERSION,
            range_count=4,
            range_ordinal=ordinal,
            raw_byte_start=raw_start,
            raw_byte_end=raw_end,
            raw_sha256=hashlib.sha256(encoded_record).hexdigest(),
            raw_byte_count=len(encoded_record),
            record_start=ordinal,
            record_end=ordinal + 1,
            record_count=1,
            canonical_sha256=hashlib.sha256(canonical).hexdigest(),
            canonical_byte_count=len(canonical),
            path=str(raw_path),
        )
        raw_range_proofs.append(raw_range)
        range_proof_maps.append(
            {
                "range_ordinal": ordinal,
                "raw_byte_start": raw_start,
                "raw_byte_end": raw_end,
                "raw_byte_count": len(encoded_record),
                "raw_sha256": raw_range.raw_sha256,
                "record_start": ordinal,
                "record_end": ordinal + 1,
                "record_count": 1,
                "canonical_sha256": raw_range.canonical_sha256,
                "canonical_byte_count": len(canonical),
            }
        )
        offset = raw_end + 1
    return raw_range_proofs, range_proof_maps


def _write_fixture_manifest(
    tmp_path: Path,
    raw_path: Path,
    artifact_sha256: str,
    raw_bytes: bytes,
    raw_range_proofs: list[RawRangeProof],
    range_proof_maps: list[dict[str, object]],
) -> tuple[Path, bytes, str]:
    range_set_sha256 = range_set_digest(
        artifact_sha256,
        len(raw_bytes),
        4,
        tuple(raw_range_proofs),
    )
    manifest_by_field = {
        "contract_id": RANGE_CONTRACT_ID,
        "contract_version": RANGE_CONTRACT_VERSION,
        "canonicalization_id": RANGE_CANONICALIZATION_ID,
        "producer_build_id": "test-build",
        "raw_artifact": {
            "file_name": raw_path.name,
            "sha256": artifact_sha256,
            "byte_count": len(raw_bytes),
            "record_count": 4,
        },
        "range_count": 4,
        "ranges": range_proof_maps,
        "range_set_sha256": range_set_sha256,
    }
    manifest_bytes = json.dumps(
        manifest_by_field,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    manifest_path = tmp_path / (
        f"raw-{artifact_sha256}-ranges-4-v2.manifest.json"
    )
    manifest_path.write_bytes(manifest_bytes)
    manifest_path.chmod(0o600)
    return manifest_path, manifest_bytes, range_set_sha256


def _fixture(
    tmp_path,
    plan_years: tuple[int, ...] = (2026,),
    *,
    rejected_npi: str = "1003821381",
    reason: str = UHC_PROVIDER_QUARANTINE_REASON_INVALID_NPI_CHECKSUM,
):
    """Create exact admitted raw/manifest fixtures for sparse verification."""

    tmp_path.mkdir(parents=True, exist_ok=True)
    record_bytes = _fixture_record_bytes(plan_years)
    if rejected_npi != "1003821381":
        record_bytes[1] = record_bytes[1].replace(
            b"1003821381",
            rejected_npi.encode(),
        )
    raw_bytes = b"[" + b",".join(record_bytes) + b"]"
    artifact_sha256 = hashlib.sha256(raw_bytes).hexdigest()
    raw_path = tmp_path / f"raw-{artifact_sha256}.json"
    raw_path.write_bytes(raw_bytes)
    raw_path.chmod(0o600)
    raw_range_proofs, range_proof_maps = _fixture_range_proofs(
        raw_path,
        artifact_sha256,
        record_bytes,
    )
    manifest_path, manifest_bytes, range_set_sha256 = _write_fixture_manifest(
        tmp_path,
        raw_path,
        artifact_sha256,
        raw_bytes,
        raw_range_proofs,
        range_proof_maps,
    )
    source_file_id = hashlib.sha256(b"source-file").hexdigest()
    quarantine = UhcProviderQuarantine(
        source_file_id=source_file_id,
        range_ordinal=1,
        occurrence_ordinal=1,
        record_sha256=hashlib.sha256(record_bytes[1]).hexdigest(),
        reason=reason,
    )
    argument_by_field = {
        "source": UhcProviderQuarantineRawSource(
            raw_path=raw_path,
            manifest_path=manifest_path,
            artifact_sha256=artifact_sha256,
            artifact_byte_count=len(raw_bytes),
            raw_contract_version=RANGE_CONTRACT_VERSION,
            manifest_sha256=hashlib.sha256(manifest_bytes).hexdigest(),
            range_set_sha256=range_set_sha256,
            record_count=4,
            range_count=4,
            raw_producer_build_id="test-build",
            source_file_id=source_file_id,
        ),
        "quarantines": (quarantine,),
        "max_record_bytes": max(len(encoded) for encoded in record_bytes),
    }
    return argument_by_field, record_bytes


def test_sparse_raw_verifier_binds_exact_checksum_invalid_record(tmp_path):
    arguments, _records = _fixture(tmp_path)

    census = verify_provider_quarantine_source_records(**arguments)

    assert census.counter_map == {
        "invalid_npi_individual_records": 1,
        "invalid_npi_facility_records": 0,
        "invalid_npi_address_rows": 1,
        "invalid_npi_provider_plan_rows": 1,
        "invalid_npi_structure_count": 0,
        "invalid_npi_structure_individual_records": 0,
        "invalid_npi_structure_facility_records": 0,
        "invalid_npi_structure_address_rows": 0,
        "invalid_npi_structure_provider_plan_rows": 0,
    }


@pytest.mark.parametrize("rejected_npi", ["3000000000", "10/00/0491"])
def test_sparse_raw_verifier_binds_structural_reason_and_census(
    tmp_path,
    rejected_npi,
):
    arguments, _records = _fixture(
        tmp_path,
        rejected_npi=rejected_npi,
        reason=UHC_PROVIDER_QUARANTINE_REASON_INVALID_NPI_STRUCTURE,
    )

    census = verify_provider_quarantine_source_records(**arguments)

    assert census.structural_count == 1
    assert census.structural_individual_records == 1
    assert census.structural_facility_records == 0
    assert census.structural_address_rows == 1
    assert census.structural_provider_plan_rows == 1


def test_sparse_raw_verifier_rejects_overridden_source_contract(tmp_path):
    arguments, _records = _fixture(tmp_path)

    class _UntrustedRawSource(UhcProviderQuarantineRawSource):
        """Model an injected source subtype that could override behavior."""

    arguments["source"] = _UntrustedRawSource(**vars(arguments["source"]))
    with pytest.raises(UhcProviderQuarantineRawError, match="source contract"):
        verify_provider_quarantine_source_records(**arguments)


@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        ("raw_contract_version", RANGE_CONTRACT_VERSION + 1),
        ("raw_producer_build_id", "different-build"),
    ],
)
def test_sparse_raw_verifier_rejects_manifest_identity_drift(
    tmp_path,
    field_name,
    value,
):
    arguments, _records = _fixture(tmp_path)
    arguments["source"] = replace(
        arguments["source"],
        **{field_name: value},
    )

    with pytest.raises(UhcProviderQuarantineRawError, match="manifest|layout"):
        verify_provider_quarantine_source_records(**arguments)


def test_sparse_raw_verifier_counts_multi_year_relationship_rows(tmp_path):
    arguments, _records = _fixture(tmp_path, (2025, 2026))

    census = verify_provider_quarantine_source_records(**arguments)

    assert census.provider_plan_rows == 2


@pytest.mark.parametrize(
    "mutation",
    [
        lambda record_by_field: record_by_field.update(extra="forbidden"),
        lambda record_by_field: record_by_field.update(type="unsupported"),
        lambda record_by_field: record_by_field.update(addresses=[]),
        lambda record_by_field: record_by_field.update(plans=[]),
        lambda record_by_field: record_by_field.update(accepting="sometimes"),
        lambda record_by_field: record_by_field["addresses"][0].update(
            extra="forbidden"
        ),
        lambda record_by_field: record_by_field["plans"][0].update(years=[]),
        lambda record_by_field: record_by_field["plans"][0].update(
            plan_id=" "
        ),
        lambda record_by_field: record_by_field["plans"].append("bad"),
        lambda record_by_field: record_by_field.update(facility_name=7),
        lambda record_by_field: record_by_field.update(facility_type="bad"),
        lambda record_by_field: record_by_field.update(name="bad"),
    ],
)
def test_checksum_invalid_record_rejects_every_other_semantic_defect(mutation):
    record_by_field = deepcopy(_provider_record("1003821381"))
    mutation(record_by_field)

    with pytest.raises(UhcProviderQuarantineRecordError):
        validate_checksum_invalid_provider_record(record_by_field)


def test_checksum_invalid_record_matches_native_permissive_plan_text() -> None:
    record_by_field = _provider_record("1003821381")
    record_by_field["plans"][0].update(
        years=[2026, 2026],
        plan_id_type="BAD TYPE!",
        plan_id="x" * 257,
        network_tier="tier\tvalue",
    )

    census = validate_checksum_invalid_provider_record(record_by_field)

    assert census.provider_plan_rows == 2


def test_structural_record_requires_a_string_shape_failure_only() -> None:
    for npi in ("3000000000", "10/00/0491"):
        census = validate_structurally_invalid_provider_record(
            _provider_record(npi)
        )

        assert census.structural_count == 1
        assert census.structural_individual_records == 1

    for npi in (
        None,
        3_000_000_000,
        "123",
        "abcdefghij",
        "10000004-1",
        "0000000000",
        "1003821380",
        "1003821381",
    ):
        with pytest.raises(UhcProviderQuarantineRecordError):
            validate_structurally_invalid_provider_record(
                _provider_record(npi)
            )

    malformed = _provider_record("3000000000")
    malformed["plans"] = []
    with pytest.raises(UhcProviderQuarantineRecordError, match="plans"):
        validate_structurally_invalid_provider_record(malformed)


@pytest.mark.parametrize("npi", [None, "123", "abcdefghij", "9999999999", "1003821380"])
def test_quarantine_record_rejects_non_checksum_only_npi_classes(npi):
    with pytest.raises(UhcProviderQuarantineRecordError):
        validate_checksum_invalid_provider_record(_provider_record(npi))


def test_sparse_raw_verifier_rejects_hash_and_checksum_class_drift(tmp_path):
    arguments, records = _fixture(tmp_path)
    quarantine = arguments["quarantines"][0]
    arguments["quarantines"] = (
        UhcProviderQuarantine(
            source_file_id=quarantine.source_file_id,
            range_ordinal=quarantine.range_ordinal,
            occurrence_ordinal=quarantine.occurrence_ordinal,
            record_sha256="0" * 64,
        ),
    )
    with pytest.raises(UhcProviderQuarantineRawError, match="hash"):
        verify_provider_quarantine_source_records(**arguments)

    arguments, _records = _fixture(tmp_path / "valid")
    valid_quarantine = arguments["quarantines"][0]
    arguments["quarantines"] = (
        UhcProviderQuarantine(
            source_file_id=valid_quarantine.source_file_id,
            range_ordinal=0,
            occurrence_ordinal=0,
            record_sha256=hashlib.sha256(records[0]).hexdigest(),
        ),
    )
    with pytest.raises(UhcProviderQuarantineRawError, match="reason does not match"):
        verify_provider_quarantine_source_records(**arguments)


def test_sparse_raw_verifier_rejects_unsafe_or_changed_files(tmp_path):
    arguments, _records = _fixture(tmp_path)
    arguments["source"].raw_path.chmod(0o622)
    with pytest.raises(UhcProviderQuarantineRawError, match="unsafe"):
        verify_provider_quarantine_source_records(**arguments)

    arguments, _records = _fixture(tmp_path / "changed")
    raw_path = arguments["source"].raw_path
    changed = bytearray(raw_path.read_bytes())
    changed[changed.index(b"1003821381")] = ord("2")
    raw_path.write_bytes(changed)
    raw_path.chmod(0o600)
    with pytest.raises(UhcProviderQuarantineRawError, match="hash|range proof"):
        verify_provider_quarantine_source_records(**arguments)


def test_sparse_raw_verifier_enforces_native_record_byte_bound(tmp_path):
    arguments, records = _fixture(tmp_path)
    arguments["max_record_bytes"] = len(records[1]) - 1

    with pytest.raises(UhcProviderQuarantineRawError, match="byte bound"):
        verify_provider_quarantine_source_records(**arguments)
