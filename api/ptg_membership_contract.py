# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed physical-source/code identity for the derived membership family."""

import hashlib
import json
from dataclasses import asdict

from api.code_systems import canonical_catalog_code, normalize_code_system
from db.models.ptg_membership_projection import (
    PTGMembershipCode,
    PTGMembershipCodeCoverage,
    PTGMembershipFirst,
    PTGMembershipManifest,
    PTGMembershipSecond,
)
from process.reference_family_archive import ReferenceFamilySpec

CONTRACT = "ptg_provider_membership_v1"
IMPORTER = "ptg-provider-membership-projection"
SPEC = ReferenceFamilySpec(
    IMPORTER,
    (PTGMembershipManifest, PTGMembershipFirst, PTGMembershipSecond, PTGMembershipCode, PTGMembershipCodeCoverage),
)
MAX_BUILD_EDGES = 32_000_000
MAX_BUILD_BYTES = 512 * 1024 * 1024
MAX_CODE_OCCURRENCES = 200_000
MAX_BUILD_OWNERS = 8_000_000
MAX_MAP_BYTES = 512 * 1024 * 1024
MAX_FACTOR_BATCH_MEMBERS = 1_000_000


def canonical_code(code_system, code):
    system = normalize_code_system(code_system)
    normalized = canonical_catalog_code(system, code)
    if not system or not normalized or system != code_system or normalized != code:
        raise ValueError("membership index requires a canonical procedure code")
    return system, normalized


def validate_params(params):
    names = {"source_key", "snapshot_id", "code_system", "code"}
    if (
        type(params) is not dict
        or set(params) != names
        or any(type(value) is not str or not value or value != value.strip() for value in params.values())
    ):
        raise ValueError("membership projection params must be exactly source_key, snapshot_id, code_system, code")
    canonical_code(params["code_system"], params["code"])


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)


def digest(value):
    return hashlib.sha256(encoded(value).encode()).hexdigest()


def physical_identity(tables):
    binding = getattr(tables, "physical_binding", None)
    if binding is None:
        return None
    document = asdict(binding)
    document["dataset_id"] = str(binding.dataset_id)
    return json.loads(encoded(document))


def identity(tables, root, finalizer, code_system, code):
    system, normalized = canonical_code(code_system, code)
    return {
        "contract": CONTRACT,
        "source_key": tables.source_key,
        "snapshot_id": tables.snapshot_id,
        "physical_binding": physical_identity(tables),
        "graph_root": asdict(root) | {"map_digest": root.map_digest.hex()},
        "finalizer": finalizer,
        "code_system": system,
        "code": normalized,
    }
