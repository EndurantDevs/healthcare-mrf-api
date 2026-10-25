# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed release/procedure identity, separate from whole-release projections."""

import re
from dataclasses import asdict
from decimal import Decimal, InvalidOperation
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
ZIP_CONTRACT = "plan_pricing_code_zip_v1"
ZIP_BUILD_REVISION = "complete-centroid-zip-aggregates-v1"
CALIBRATION_OPERATION = "calibrate"
ZIP_SCOPE_FIELDS = frozenset({"scope_kind", "zip5", "zip_radius_miles"})
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
    if type(params) is dict and ZIP_SCOPE_FIELDS.intersection(params):
        names.update(ZIP_SCOPE_FIELDS)
    if type(params) is dict and "operation" in params:
        if (
            not include_identity
            or params["operation"] != CALIBRATION_OPERATION
            or not ZIP_SCOPE_FIELDS <= params.keys()
        ):
            raise ValueError("code calibration requires an exact scoped operator admission")
        names.add("operation")
    if (
        type(params) is not dict
        or set(params) != names
        or any(
            type(selector_value) is not str or not selector_value or selector_value != selector_value.strip()
            for selector_value in params.values()
        )
    ):
        raise ValueError("code projection requires exact release, revision, and procedure selectors")
    identity = projection_code_identity(params["code_system"], params["code"])
    if identity != (params["code_system"], params["code"]):
        raise ValueError("code projection requires a canonical procedure identity")
    if include_identity and HEX_DIGEST.fullmatch(params["expected_identity_digest"]) is None:
        raise ValueError("code projection requires an exact expected identity digest")
    scope_selectors(params)


def canonical_radius(value):
    """Use one finite decimal representation in control and reader identities."""
    if isinstance(value, bool) or len(str(value)) > 32:
        raise ValueError("code pricing ZIP radius is invalid")
    try:
        number = Decimal(str(value))
        if not number.is_finite() or number < 0 or number.adjusted() > 31 or number.as_tuple().exponent < -32:
            raise ValueError
        result = format(number, "f")
        if "." in result:
            result = result.rstrip("0").rstrip(".")
        if number.is_zero():
            result = "0"
        if len(result) > 32:
            raise ValueError
        return result
    except InvalidOperation, ValueError, OverflowError:
        raise ValueError("code pricing ZIP radius is invalid") from None


def scope_selectors(params):
    """The additive ZIP contract accepts all three selectors or none."""
    present = ZIP_SCOPE_FIELDS.intersection(params)
    if not present:
        return None
    if (
        present != ZIP_SCOPE_FIELDS
        or params["scope_kind"] != "zip_radius"
        or type(params["zip5"]) is not str
        or re.fullmatch(r"[0-9]{5}", params["zip5"], flags=re.ASCII) is None
        or type(params["zip_radius_miles"]) is not str
        or canonical_radius(params["zip_radius_miles"]) != params["zip_radius_miles"]
    ):
        raise ValueError("code pricing requires exact canonical ZIP scope selectors")
    return {"kind": "zip_radius", "zip5": params["zip5"], "zip_radius_miles": params["zip_radius_miles"]}


def validate_scope(scope):
    """Require a complete, ordered centroid ZIP set in the immutable identity."""
    from api.plan_pricing_projection_contract import MAX_GEO_CELLS

    if type(scope) is not dict or set(scope) != {"kind", "zip5", "zip_radius_miles", "geo_cells"}:
        raise ValueError("code pricing ZIP coverage is invalid")
    scope_selectors({"scope_kind": scope["kind"], "zip5": scope["zip5"], "zip_radius_miles": scope["zip_radius_miles"]})
    cells = scope["geo_cells"]
    if (
        type(cells) is not list
        or len(cells) > MAX_GEO_CELLS
        or any(type(cell) is not str or re.fullmatch(r"[0-9]{5}", cell, flags=re.ASCII) is None for cell in cells)
        or cells != sorted(set(cells))
    ):
        raise ValueError("code pricing ZIP coverage is invalid")


def _require_source_coverage(selection, source_coverages):
    expected_ordinals = tuple(binding.binding_ordinal for binding in selection.bindings if binding.role == "in_network")
    if (
        not expected_ordinals
        or tuple(coverage["binding_ordinal"] for coverage in source_coverages) != expected_ordinals
    ):
        raise ValueError("code projection source coverage is incomplete")
    if any(
        set(coverage)
        != {
            "binding_ordinal",
            "materialized_binding_ordinal",
            "read_identity_digest",
            "source_identity",
            "code_rows",
            "membership",
        }
        for coverage in source_coverages
    ):
        raise ValueError("code projection source identity is incomplete")
    coverage_by_ordinal = {}
    for coverage in source_coverages:
        candidates_by_ordinal = coverage_by_ordinal | {coverage["binding_ordinal"]: coverage}
        representative = coverage["materialized_binding_ordinal"]
        if type(representative) is not int or representative not in candidates_by_ordinal:
            raise ValueError("code projection read representative is invalid")
        representative_coverage = candidates_by_ordinal[representative]
        if (
            representative_coverage["materialized_binding_ordinal"] != representative
            or coverage["read_identity_digest"] != representative_coverage["read_identity_digest"]
            or coverage["code_rows"] != representative_coverage["code_rows"]
        ):
            raise ValueError("code projection aliased physical read differs")
        coverage_by_ordinal[coverage["binding_ordinal"]] = coverage


def capability_identity(selection, code_identity, signature, sources, *, scope=None):
    """Bind complete logical attribution and authenticated physical code coverage."""
    if type(selection) is not PlanReleaseServingSelection or not HEX_DIGEST.fullmatch(signature or ""):
        raise ValueError("code projection release or provider identity is invalid")
    if projection_code_identity(*code_identity) != tuple(code_identity):
        raise ValueError("code projection requires a canonical procedure identity")
    _require_source_coverage(selection, sources)
    identity_by_field = {
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
    if scope is not None:
        validate_scope(scope)
        identity_by_field.update(contract=ZIP_CONTRACT, build_revision=ZIP_BUILD_REVISION, scope=scope)
    if len(canonical_json(identity_by_field).encode("ascii")) > 512 * 1024:
        raise ValueError("code projection identity exceeds its metadata bound")
    return identity_by_field


def identity_digest(identity):
    """Hash the closed immutable capability identity using its canonical codec."""
    return digest(identity)


def source_coverage(
    binding_ordinal, source_identity, code_rows, membership, *, read_identity_digest, materialized_binding_ordinal=None
):
    """Retain all physical variants and the exact sealed membership generation."""
    keys = [int(code_row["code_key"]) for code_row in code_rows]
    coverage_by_code_key = {code_row["code_key"]: code_row for code_row in membership["code_coverage"]}
    if len(keys) != len(set(keys)) or any(key not in coverage_by_code_key for key in keys):
        raise ValueError("code projection physical code coverage differs")
    if any(
        int(code_row["rate_count"]) != coverage_by_code_key[int(code_row["code_key"])]["occurrence_count"]
        for code_row in code_rows
    ):
        raise ValueError("code projection physical occurrence count differs")
    return {
        "binding_ordinal": binding_ordinal,
        "materialized_binding_ordinal": binding_ordinal
        if materialized_binding_ordinal is None
        else materialized_binding_ordinal,
        "read_identity_digest": read_identity_digest,
        "source_identity": source_identity,
        "code_rows": sorted(
            (
                {"code_key": int(code_row["code_key"]), "occurrence_count": int(code_row["rate_count"])}
                for code_row in code_rows
            ),
            key=lambda code_row: code_row["code_key"],
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
        """Normalize only authenticated canonical scalars and tuple coordinates."""
        if isinstance(value, UUID):
            return str(value)
        if type(value) is tuple:
            return [normalize(item) for item in value]
        if value is None or type(value) in {int, str, bool}:
            return value
        raise ValueError("code projection sealed read identity is invalid")

    return digest(normalize(read_identity))
