# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Boundary failures for candidate-audit public tuple projection."""

import pytest

from api import ptg2_candidate_audit_projection as projection
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError
from scripts.validation import ptg2_v3_source_api_audit as source_audit


def _price_payload() -> dict:
    return {
        "negotiated_type": "negotiated",
        "negotiated_rate": "123.45",
        "expiration_date": "2026-12-31",
        "service_code": ["11"],
        "billing_class": "professional",
        "setting": "office",
        "billing_code_modifier": ["25"],
        "additional_information": "test",
    }


def test_candidate_projection_wraps_public_schema_errors(monkeypatch):
    def invalid_public_tuple(*_args):
        raise source_audit.ApiSchemaError("invalid")

    monkeypatch.setattr(
        projection.source_audit,
        "canonical_api_price_tuple",
        invalid_public_tuple,
    )

    with pytest.raises(PTG2ManifestArtifactError, match="public API contract"):
        projection._build_canonical_candidate_tuple(
            source_audit.QueryKey("CPT", "99213", 1234567890),
            {},
            (),
            _price_payload(),
        )
