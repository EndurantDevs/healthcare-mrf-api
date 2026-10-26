# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Portable MRF completion and canonical-model receipts for reference archives."""

import hashlib
import re
from collections.abc import Mapping
from typing import Any

from process.ext.address_canon import archive_table_name
from process.mrf_address_publication import (
    ACCOUNTED_MODEL_CLOSURE_CONTRACT,
    ADDRESS_COVERAGE_CONTRACT,
    MODEL_CLOSURE_CONTRACT,
    MODEL_CLOSURE_CONTRACTS,
    STAGE_TABLE,
)

_AUX_SCHEMA = "mrf-canonical-address.payload-jsonb.v1"
_AUX_NATIVE_SET_CONTRACT = "mrf-canonical-address.indexed-set.v2"


def mrf_publication_digest(publication):
    """Bind completion identity and the exact versioned coverage proof without rewriting legacy evidence."""
    from process import reference_family_archive as archive

    return hashlib.sha256(
        archive._canonical_json(
            {
                "attempt_id": str(publication["attempt_id"]),
                "generation": publication["generation"],
                "address_content": publication["native_address_coverage"]
                if "native_address_coverage" in publication
                else publication["address_content"],
            }
        )
    ).hexdigest()


def _canonical_model_receipt(publication=None):
    """Record the typed model meaning without inventing mutable payload authority."""
    from process import reference_family_archive as archive

    auxiliary_map = {
        "contract": MODEL_CLOSURE_CONTRACT,
        "table_name": STAGE_TABLE,
        "archive_name": archive_table_name(),
        "source_bit": 16,
        "display_priority": 5,
    }
    if publication is not None:
        coverage = publication["native_address_coverage"]
        if coverage.get("contract") != ADDRESS_COVERAGE_CONTRACT:
            raise archive.ReferenceFamilyArchiveError("MRF canonical address accounting contract differs")
        from process.mrf_address_publication import require_address_coverage

        require_address_coverage(coverage)
        auxiliary_map.update(
            contract=ACCOUNTED_MODEL_CLOSURE_CONTRACT,
            address_coverage=coverage,
            publication_sha256=mrf_publication_digest(publication),
        )
    return auxiliary_map


def _validate_mrf_auxiliary_receipt(auxiliary: object) -> Mapping[str, Any]:
    """Validate the required portable canonical-address receipt."""
    from process import reference_family_archive as archive

    if isinstance(auxiliary, Mapping) and auxiliary.get("contract") in MODEL_CLOSURE_CONTRACTS:
        expected_map = {
            "contract": MODEL_CLOSURE_CONTRACT,
            "table_name": STAGE_TABLE,
            "archive_name": "address_archive_v2",
            "source_bit": 16,
            "display_priority": 5,
        }
        if auxiliary["contract"] == ACCOUNTED_MODEL_CLOSURE_CONTRACT:
            from process.mrf_address_publication import require_address_coverage

            coverage = auxiliary.get("address_coverage")
            if not isinstance(coverage, dict) or coverage.get("contract") != ADDRESS_COVERAGE_CONTRACT:
                raise archive.ReferenceFamilyArchiveError("MRF canonical address accounting contract differs")
            require_address_coverage(coverage)
            if re.fullmatch(r"[0-9a-f]{64}", str(auxiliary.get("publication_sha256"))) is None:
                raise archive.ReferenceFamilyArchiveError("MRF canonical publication digest is invalid")
            expected_map.update(
                contract=ACCOUNTED_MODEL_CLOSURE_CONTRACT,
                address_coverage=coverage,
                publication_sha256=auxiliary["publication_sha256"],
            )
        if auxiliary != expected_map:
            raise archive.ReferenceFamilyArchiveError("MRF canonical model receipt is invalid")
        return dict(auxiliary)
    expected_fields = {
        "table_name",
        "archive_name",
        "schema_sha256",
        "row_count",
        "content_sha256",
        "publication_sha256",
    }
    is_native_set = isinstance(auxiliary, Mapping) and auxiliary.get("contract") == _AUX_NATIVE_SET_CONTRACT
    if is_native_set:
        expected_fields.remove("content_sha256")
        expected_fields.add("contract")
    if (
        not isinstance(auxiliary, Mapping)
        or set(auxiliary) != expected_fields
        or auxiliary["table_name"] != STAGE_TABLE
        or archive._IDENTIFIER.fullmatch(str(auxiliary["archive_name"])) is None
        or auxiliary["schema_sha256"] != hashlib.sha256(_AUX_SCHEMA.encode()).hexdigest()
        or type(auxiliary["row_count"]) is not int
        or auxiliary["row_count"] < 0
        or any(
            re.fullmatch(r"[0-9a-f]{64}", str(auxiliary[key])) is None
            for key in (("publication_sha256",) if is_native_set else ("content_sha256", "publication_sha256"))
        )
    ):
        raise archive.ReferenceFamilyArchiveError("MRF canonical auxiliary receipt is invalid")
    return auxiliary


def _manifest_auxiliary(manifest_value, spec, receipts):
    """Keep the recorded JSON receipt distinct from the new native model contract."""
    from process import reference_family_archive as archive

    if spec.importer_id != "mrf":
        return None
    auxiliary = _validate_mrf_auxiliary_receipt(manifest_value.get("auxiliary"))
    if (manifest_value["contract"] == archive.TYPED_MRF_CONTRACT) != (
        auxiliary.get("contract") in MODEL_CLOSURE_CONTRACTS
    ):
        raise archive.ReferenceFamilyArchiveError("MRF canonical model contract differs")
    if auxiliary.get("contract") == ACCOUNTED_MODEL_CLOSURE_CONTRACT:
        rows_by_table = {table.table_name: table.row_count for table in receipts}
        if any(
            counts["rows"] != rows_by_table[name] for name, counts in auxiliary["address_coverage"]["tables"].items()
        ):
            raise archive.ReferenceFamilyArchiveError("MRF canonical raw row accounting differs")
    return auxiliary
