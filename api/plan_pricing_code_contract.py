# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed release/procedure identity, separate from whole-release projections."""

from dataclasses import asdict
from uuid import UUID

from api.plan_pricing_projection_contract import HEX_DIGEST, canonical_json, projection_code_identity
from api.plan_release_serving import PlanReleaseServingSelection
from api.ptg_membership_contract import digest
from db.models.plan_pricing_code_projection import (
    PlanPricingCodeAggregatePack,
    PlanPricingCodeManifest,
    PlanPricingCodeMembership,
    PlanPricingCodeOccurrence,
    PlanPricingCodeProviderCell,
    PlanPricingCodeProviderState,
    PlanPricingCodeRateProfile,
)
from process.reference_family_archive import ReferenceFamilySpec

CONTRACT = "plan_pricing_code_v1"
IMPORTER = "plan-pricing-code-projection"
BUILD_REVISION = "complete-occurrences-memberships-provider-witnesses-v1"
SPEC = ReferenceFamilySpec(
    IMPORTER,
    (
        PlanPricingCodeManifest,
        PlanPricingCodeMembership,
        PlanPricingCodeProviderCell,
        PlanPricingCodeProviderState,
        PlanPricingCodeOccurrence,
        PlanPricingCodeAggregatePack,
        PlanPricingCodeRateProfile,
    ),
)


def validate_params(params, *, include_identity=True):
    """Accept immutable release coordinates and one canonical procedure only."""
    names = {"plan_release_id", "serving_revision_id", "code_system", "code"}
    if include_identity:
        names.add("expected_identity_digest")
    if (
        type(params) is not dict
        or set(params) != names
        or any(type(v) is not str or not v or v != v.strip() for v in params.values())
    ):
        raise ValueError("code projection requires exact release, revision, and procedure selectors")
    identity = projection_code_identity(params["code_system"], params["code"])
    if identity != (params["code_system"], params["code"]):
        raise ValueError("code projection requires a canonical procedure identity")
    if include_identity and HEX_DIGEST.fullmatch(params["expected_identity_digest"]) is None:
        raise ValueError("code projection requires an exact expected identity digest")


def capability_identity(selection, code_identity, signature, sources):
    """Bind complete logical attribution and authenticated physical code coverage."""
    if type(selection) is not PlanReleaseServingSelection or not HEX_DIGEST.fullmatch(signature or ""):
        raise ValueError("code projection release or provider identity is invalid")
    if projection_code_identity(*code_identity) != tuple(code_identity):
        raise ValueError("code projection requires a canonical procedure identity")
    expected = tuple(binding.binding_ordinal for binding in selection.bindings if binding.role == "in_network")
    if not expected or tuple(source["binding_ordinal"] for source in sources) != expected:
        raise ValueError("code projection source coverage is incomplete")
    if any(
        set(source)
        != {
            "binding_ordinal",
            "materialized_binding_ordinal",
            "read_identity_digest",
            "source_identity",
            "code_rows",
            "membership",
        }
        for source in sources
    ):
        raise ValueError("code projection source identity is incomplete")
    prior = {}
    for source in sources:
        candidates = prior | {source["binding_ordinal"]: source}
        representative = source["materialized_binding_ordinal"]
        if type(representative) is not int or representative not in candidates:
            raise ValueError("code projection read representative is invalid")
        target = candidates[representative]
        if (
            target["materialized_binding_ordinal"] != representative
            or source["read_identity_digest"] != target["read_identity_digest"]
            or source["code_rows"] != target["code_rows"]
        ):
            raise ValueError("code projection aliased physical read differs")
        prior[source["binding_ordinal"]] = source
    identity = {
        "contract": CONTRACT,
        "build_revision": BUILD_REVISION,
        "plan_release_id": selection.plan_release_id,
        "serving_revision_id": selection.serving_revision_id,
        "binding_set_digest": selection.binding_set_digest,
        "bindings": [asdict(binding) for binding in selection.bindings],
        "provider_signature": signature,
        "code_system": code_identity[0],
        "code": code_identity[1],
        "sources": sources,
    }
    if len(canonical_json(identity).encode("ascii")) > 512 * 1024:
        raise ValueError("code projection identity exceeds its metadata bound")
    return identity


def identity_digest(identity):
    return digest(identity)


def source_coverage(
    binding_ordinal, source_identity, code_rows, membership, *, read_identity_digest, materialized_binding_ordinal=None
):
    """Retain all physical variants and the exact sealed membership generation."""
    keys = [int(row["code_key"]) for row in code_rows]
    covered = {row["code_key"]: row for row in membership["code_coverage"]}
    if len(keys) != len(set(keys)) or any(key not in covered for key in keys):
        raise ValueError("code projection physical code coverage differs")
    if any(int(row["rate_count"]) != covered[int(row["code_key"])]["occurrence_count"] for row in code_rows):
        raise ValueError("code projection physical occurrence count differs")
    return {
        "binding_ordinal": binding_ordinal,
        "materialized_binding_ordinal": binding_ordinal
        if materialized_binding_ordinal is None
        else materialized_binding_ordinal,
        "read_identity_digest": read_identity_digest,
        "source_identity": source_identity,
        "code_rows": sorted(
            ({"code_key": int(row["code_key"]), "occurrence_count": int(row["rate_count"])} for row in code_rows),
            key=lambda row: row["code_key"],
        ),
        "membership": {
            "projection_id": membership["projection_id"],
            "content_digest": membership["content_digest"],
            "active_attachment": membership["active_attachment"],
            "storage": membership["storage"],
            "coverage_digest": digest(membership["code_coverage"]),
        },
    }


def sealed_read_digest(read_identity):
    """Hash only the established authenticated tuple, never caller layout hints."""

    def normalize(value):
        if isinstance(value, UUID):
            return str(value)
        if type(value) is tuple:
            return [normalize(item) for item in value]
        if value is None or type(value) in {int, str, bool}:
            return value
        raise ValueError("code projection sealed read identity is invalid")

    return digest(normalize(read_identity))
