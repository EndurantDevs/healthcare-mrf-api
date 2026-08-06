# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Dormant typed inventory and source-record inclusion contracts."""

from __future__ import annotations

import hmac
from typing import Any, Mapping

from public_evidence.evidence_record_primitives import (
    EvidenceSourceRecordReference,
    PublicEvidenceRecordError,
    validate_evidence_source_record_reference,
)
from public_evidence.source_record_inclusion_primitives import (
    PUBLIC_EVIDENCE_SOURCE_RECORD_INCLUSION_CONTRACT,
    PUBLIC_EVIDENCE_SOURCE_RECORD_INVENTORY_CONTRACT,
    REQUIRED_AUTHENTICATED_REPLAY_ORDERING_CONTRACT,
    SOURCE_RECORD_INCLUSION_REF_PREFIX,
    SOURCE_RECORD_INVENTORY_ORDERING_CONTRACT,
    SOURCE_RECORD_INVENTORY_REF_PREFIX,
    SOURCE_RECORD_INVENTORY_TREE_CONTRACT,
    SOURCE_RECORD_KINDS_BY_SOURCE,
    PublicEvidenceSourceRecordInclusionError,
    PublicEvidenceSourceRecordInclusionWitness,
    PublicEvidenceSourceRecordInventoryDescriptor,
    SourceRecordInventoryAuthorityState,
    _bounded_audit_path,
    _canonical_sha256,
    _derived_ref,
    _exact_dict,
    _fail,
    _leaf_sha256,
    _source_binding_fingerprint,
    _strict_kind,
    _strict_ordinal,
    _strict_positive_count,
    _strict_protocol,
    _strict_sha256,
    _validate_derived_ref,
    _verify_audit_path,
)
from public_evidence.source_release_contract import (
    PUBLIC_EVIDENCE_FOUNDATION_SCOPE,
    PublicEvidenceSourceReleaseDescriptor,
    PublicEvidenceSourceReleaseError,
    validate_public_evidence_source_release,
)

_INVENTORY_NAMESPACE_FIELDS = frozenset(
    "record_kind record_identity_contract_id "
    "payload_canonicalization_contract_id member_count".split()
)
_INVENTORY_INPUT_FIELDS = _INVENTORY_NAMESPACE_FIELDS | {"member_root_sha256"}


def _validated_release(
    value: object,
) -> PublicEvidenceSourceReleaseDescriptor:
    try:
        return validate_public_evidence_source_release(value)
    except PublicEvidenceSourceReleaseError:
        raise _fail() from None


def _fixed_inventory_authority() -> SourceRecordInventoryAuthorityState:
    return SourceRecordInventoryAuthorityState(
        lifecycle_state="declared_inventory_descriptor_only",
        authenticated_replay_state="required_not_executed",
        source_bytes_authenticated=False,
        complete_inventory_scan_verified=False,
        member_ordering_verified=False,
        duplicate_rejection_verified=False,
        source_authenticity_claimed=False,
        whole_source_complete=False,
        adapter_execution_authority="none",
        database_io_enabled=False,
        serving_authority="none",
        current_pointer_authority="none",
        publication_enabled=False,
        replacement_enabled=False,
        deletion_enabled=False,
        retirement_enabled=False,
        supersession_enabled=False,
    )


def _inventory_components(
    release: object,
    raw: object,
) -> dict[str, Any]:
    fixed_release = _validated_release(release)
    namespace_fields = _exact_dict(raw, _INVENTORY_NAMESPACE_FIELDS)
    source_kind = fixed_release.source_kind
    record_kind = _strict_kind(namespace_fields["record_kind"])
    allowed_kinds = SOURCE_RECORD_KINDS_BY_SOURCE.get(source_kind)
    if allowed_kinds is None or record_kind not in allowed_kinds:
        raise _fail()
    inventory_component_map = {
        "release": fixed_release,
        "source_kind": source_kind,
        "record_kind": record_kind,
        "record_identity_contract_id": _strict_protocol(
            namespace_fields["record_identity_contract_id"]
        ),
        "payload_canonicalization_contract_id": _strict_protocol(
            namespace_fields["payload_canonicalization_contract_id"]
        ),
        "member_count": _strict_positive_count(namespace_fields["member_count"]),
        "source_binding_fingerprint_sha256": _source_binding_fingerprint(fixed_release),
    }
    inventory_component_map["inventory_policy_descriptor_sha256"] = _canonical_sha256(
        "source_record_inventory_policy",
        {
            "tree_contract_id": SOURCE_RECORD_INVENTORY_TREE_CONTRACT,
            "ordering_contract_id": SOURCE_RECORD_INVENTORY_ORDERING_CONTRACT,
            "member_ordering_state": "declared_not_verified",
            "duplicate_rejection_state": "required_not_executed",
            "required_authenticated_replay_ordering_contract_id": (
                REQUIRED_AUTHENTICATED_REPLAY_ORDERING_CONTRACT
            ),
            "odd_node_policy": "rfc6962_shape_largest_power_of_two_split",
            "source_kind": inventory_component_map["source_kind"],
            "record_kind": inventory_component_map["record_kind"],
            "record_identity_contract_id": inventory_component_map[
                "record_identity_contract_id"
            ],
            "payload_canonicalization_contract_id": inventory_component_map[
                "payload_canonicalization_contract_id"
            ],
            "authenticated_replay_state": "required_not_executed",
        },
    )
    return inventory_component_map


def _namespace_payload(component_fields: Mapping[str, Any]) -> dict[str, object]:
    release = component_fields["release"]
    artifact = release.artifact_identity
    return {
        "contract": PUBLIC_EVIDENCE_SOURCE_RECORD_INVENTORY_CONTRACT,
        "foundation_scope": PUBLIC_EVIDENCE_FOUNDATION_SCOPE,
        "tree_contract_id": SOURCE_RECORD_INVENTORY_TREE_CONTRACT,
        "ordering_contract_id": SOURCE_RECORD_INVENTORY_ORDERING_CONTRACT,
        "source_kind": component_fields["source_kind"],
        "source_release_ref": release.source_release_ref,
        "source_release_contract_sha256": release.contract_sha256,
        "artifact_identity_kind": artifact.identity_kind,
        "artifact_content_identity_kind": artifact.content_identity_kind,
        "artifact_identity_ref": artifact.identity_ref,
        "artifact_content_sha256": artifact.content_sha256,
        "source_binding_fingerprint_sha256": component_fields[
            "source_binding_fingerprint_sha256"
        ],
        "record_kind": component_fields["record_kind"],
        "record_identity_contract_id": component_fields["record_identity_contract_id"],
        "payload_canonicalization_contract_id": component_fields[
            "payload_canonicalization_contract_id"
        ],
        "member_count": component_fields["member_count"],
        "inventory_policy_descriptor_sha256": component_fields[
            "inventory_policy_descriptor_sha256"
        ],
    }


def _source_record_payload(
    source_record: EvidenceSourceRecordReference,
) -> dict[str, str]:
    return {
        "source_release_ref": source_record.source_release_ref,
        "record_kind": source_record.record_kind,
        "identity_contract_id": source_record.identity_contract_id,
        "record_hmac_sha256": source_record.record_hmac_sha256,
        "payload_sha256": source_record.payload_sha256,
        "source_record_ref": source_record.source_record_ref,
    }


def derive_inventory_leaf_sha256(
    release: PublicEvidenceSourceReleaseDescriptor,
    raw_inventory_namespace: Mapping[str, object],
    source_record: EvidenceSourceRecordReference,
    member_ordinal: int,
) -> str:
    """Derive one typed leaf before an inventory root has been finalized."""
    try:
        component_fields = _inventory_components(release, raw_inventory_namespace)
        ordinal = _strict_ordinal(member_ordinal, component_fields["member_count"])
        fixed_record = validate_evidence_source_record_reference(
            component_fields["release"], source_record
        )
        if (
            fixed_record.record_kind != component_fields["record_kind"]
            or fixed_record.identity_contract_id
            != component_fields["record_identity_contract_id"]
        ):
            raise _fail()
        return _leaf_sha256(
            {
                "inventory_namespace": _namespace_payload(component_fields),
                "member_ordinal": ordinal,
                "source_record": _source_record_payload(fixed_record),
            }
        )
    except (PublicEvidenceRecordError, PublicEvidenceSourceRecordInclusionError):
        raise _fail() from None
    except Exception:
        raise _fail() from None


def _inventory_payload(
    component_fields: Mapping[str, Any],
    member_root_sha256: str,
    authority: SourceRecordInventoryAuthorityState,
) -> dict[str, object]:
    return {
        **_namespace_payload(component_fields),
        "member_root_sha256": member_root_sha256,
        "authority_state": authority._asdict(),
    }


def build_source_record_inventory_descriptor(
    release: PublicEvidenceSourceReleaseDescriptor,
    raw: Mapping[str, object],
) -> PublicEvidenceSourceRecordInventoryDescriptor:
    """Freeze one declared typed inventory; no authenticated scan is claimed."""
    try:
        inventory_descriptor_fields = _exact_dict(raw, _INVENTORY_INPUT_FIELDS)
        component_fields = _inventory_components(
            release,
            {
                field: inventory_descriptor_fields[field]
                for field in _INVENTORY_NAMESPACE_FIELDS
            },
        )
        member_root = _strict_sha256(inventory_descriptor_fields["member_root_sha256"])
        authority = _fixed_inventory_authority()
        inventory_contract_payload = _inventory_payload(
            component_fields, member_root, authority
        )
        return PublicEvidenceSourceRecordInventoryDescriptor(
            contract=PUBLIC_EVIDENCE_SOURCE_RECORD_INVENTORY_CONTRACT,
            foundation_scope=PUBLIC_EVIDENCE_FOUNDATION_SCOPE,
            tree_contract_id=SOURCE_RECORD_INVENTORY_TREE_CONTRACT,
            ordering_contract_id=SOURCE_RECORD_INVENTORY_ORDERING_CONTRACT,
            release=component_fields["release"],
            source_kind=component_fields["source_kind"],
            record_kind=component_fields["record_kind"],
            record_identity_contract_id=component_fields["record_identity_contract_id"],
            payload_canonicalization_contract_id=component_fields[
                "payload_canonicalization_contract_id"
            ],
            member_count=component_fields["member_count"],
            member_root_sha256=member_root,
            source_binding_fingerprint_sha256=component_fields[
                "source_binding_fingerprint_sha256"
            ],
            inventory_policy_descriptor_sha256=component_fields[
                "inventory_policy_descriptor_sha256"
            ],
            inventory_ref=_derived_ref(
                SOURCE_RECORD_INVENTORY_REF_PREFIX,
                "source_record_inventory",
                inventory_contract_payload,
            ),
            contract_sha256=_canonical_sha256(
                "source_record_inventory_contract", inventory_contract_payload
            ),
            authority_state=authority,
        )
    except PublicEvidenceSourceRecordInclusionError:
        raise
    except Exception:
        raise _fail() from None


def validate_source_record_inventory_descriptor(
    candidate: object,
) -> PublicEvidenceSourceRecordInventoryDescriptor:
    """Rebuild an exact descriptor without promoting its trust state."""
    if type(candidate) is not PublicEvidenceSourceRecordInventoryDescriptor:
        raise _fail()
    try:
        rebuilt = build_source_record_inventory_descriptor(
            candidate.release,
            {
                "record_kind": candidate.record_kind,
                "record_identity_contract_id": candidate.record_identity_contract_id,
                "payload_canonicalization_contract_id": (
                    candidate.payload_canonicalization_contract_id
                ),
                "member_count": candidate.member_count,
                "member_root_sha256": candidate.member_root_sha256,
            },
        )
        fixed_pairs = (
            (candidate.contract, rebuilt.contract),
            (candidate.foundation_scope, rebuilt.foundation_scope),
            (candidate.tree_contract_id, rebuilt.tree_contract_id),
            (candidate.ordering_contract_id, rebuilt.ordering_contract_id),
            (candidate.source_kind, rebuilt.source_kind),
            (
                candidate.source_binding_fingerprint_sha256,
                rebuilt.source_binding_fingerprint_sha256,
            ),
            (
                candidate.inventory_policy_descriptor_sha256,
                rebuilt.inventory_policy_descriptor_sha256,
            ),
            (candidate.authority_state, rebuilt.authority_state),
        )
        if any(
            type(left) is not type(right) or left != right
            for left, right in fixed_pairs
        ):
            raise _fail()
        _validate_derived_ref(
            candidate.inventory_ref,
            SOURCE_RECORD_INVENTORY_REF_PREFIX,
            rebuilt.inventory_ref,
        )
        if not hmac.compare_digest(
            _strict_sha256(candidate.contract_sha256), rebuilt.contract_sha256
        ):
            raise _fail()
        return rebuilt
    except PublicEvidenceSourceRecordInclusionError:
        raise
    except Exception:
        raise _fail() from None


def _inclusion_payload(
    inventory: PublicEvidenceSourceRecordInventoryDescriptor,
    source_record: EvidenceSourceRecordReference,
    ordinal: int,
    leaf_sha256: str,
    audit_path: tuple[str, ...],
) -> dict[str, object]:
    return {
        "contract": PUBLIC_EVIDENCE_SOURCE_RECORD_INCLUSION_CONTRACT,
        "tree_contract_id": SOURCE_RECORD_INVENTORY_TREE_CONTRACT,
        "inventory_ref": inventory.inventory_ref,
        "inventory_contract_sha256": inventory.contract_sha256,
        "source_record": _source_record_payload(source_record),
        "member_ordinal": ordinal,
        "leaf_sha256": leaf_sha256,
        "audit_path_sha256s": list(audit_path),
        "membership_state": "verified_against_declared_inventory",
        "authenticated_replay_state": "required_not_executed",
        "source_bytes_authenticated": False,
        "complete_inventory_scan_verified": False,
        "payload_derivation_verified": False,
        "source_authenticity_claimed": False,
    }


def _verified_inclusion_components(
    inventory: PublicEvidenceSourceRecordInventoryDescriptor,
    source_record: EvidenceSourceRecordReference,
    member_ordinal: int,
    audit_path_sha256s: tuple[str, ...],
) -> tuple[
    PublicEvidenceSourceRecordInventoryDescriptor,
    EvidenceSourceRecordReference,
    int,
    tuple[str, ...],
    str,
]:
    fixed_inventory = validate_source_record_inventory_descriptor(inventory)
    fixed_record = validate_evidence_source_record_reference(
        fixed_inventory.release, source_record
    )
    ordinal = _strict_ordinal(member_ordinal, fixed_inventory.member_count)
    audit_path = _bounded_audit_path(audit_path_sha256s)
    inventory_namespace_map = {
        "record_kind": fixed_inventory.record_kind,
        "record_identity_contract_id": fixed_inventory.record_identity_contract_id,
        "payload_canonicalization_contract_id": (
            fixed_inventory.payload_canonicalization_contract_id
        ),
        "member_count": fixed_inventory.member_count,
    }
    leaf = derive_inventory_leaf_sha256(
        fixed_inventory.release,
        inventory_namespace_map,
        fixed_record,
        ordinal,
    )
    _verify_audit_path(
        leaf,
        ordinal,
        fixed_inventory.member_count,
        audit_path,
        fixed_inventory.member_root_sha256,
    )
    return fixed_inventory, fixed_record, ordinal, audit_path, leaf


def build_source_record_inclusion_witness(
    inventory: PublicEvidenceSourceRecordInventoryDescriptor,
    source_record: EvidenceSourceRecordReference,
    member_ordinal: int,
    audit_path_sha256s: tuple[str, ...],
) -> PublicEvidenceSourceRecordInclusionWitness:
    """Verify one exact audit path against a declared typed inventory root."""
    try:
        fixed_inventory, fixed_record, ordinal, audit_path, leaf = (
            _verified_inclusion_components(
                inventory, source_record, member_ordinal, audit_path_sha256s
            )
        )
        inclusion_contract_payload = _inclusion_payload(
            fixed_inventory, fixed_record, ordinal, leaf, audit_path
        )
        return PublicEvidenceSourceRecordInclusionWitness(
            contract=PUBLIC_EVIDENCE_SOURCE_RECORD_INCLUSION_CONTRACT,
            tree_contract_id=SOURCE_RECORD_INVENTORY_TREE_CONTRACT,
            inventory=fixed_inventory,
            source_record=fixed_record,
            member_ordinal=ordinal,
            leaf_sha256=leaf,
            audit_path_sha256s=audit_path,
            inclusion_ref=_derived_ref(
                SOURCE_RECORD_INCLUSION_REF_PREFIX,
                "source_record_inclusion",
                inclusion_contract_payload,
            ),
            contract_sha256=_canonical_sha256(
                "source_record_inclusion_contract", inclusion_contract_payload
            ),
            membership_state="verified_against_declared_inventory",
            authenticated_replay_state="required_not_executed",
            source_bytes_authenticated=False,
            complete_inventory_scan_verified=False,
            payload_derivation_verified=False,
            source_authenticity_claimed=False,
        )
    except (PublicEvidenceRecordError, PublicEvidenceSourceRecordInclusionError):
        raise _fail() from None
    except Exception:
        raise _fail() from None


def validate_source_record_inclusion_witness(
    candidate: object,
) -> PublicEvidenceSourceRecordInclusionWitness:
    """Recompute the exact leaf, path result, reference, and fixed trust state."""
    if type(candidate) is not PublicEvidenceSourceRecordInclusionWitness:
        raise _fail()
    try:
        rebuilt = build_source_record_inclusion_witness(
            candidate.inventory,
            candidate.source_record,
            candidate.member_ordinal,
            candidate.audit_path_sha256s,
        )
        fixed_pairs = (
            (candidate.contract, rebuilt.contract),
            (candidate.tree_contract_id, rebuilt.tree_contract_id),
            (candidate.leaf_sha256, rebuilt.leaf_sha256),
            (candidate.membership_state, rebuilt.membership_state),
            (
                candidate.authenticated_replay_state,
                rebuilt.authenticated_replay_state,
            ),
            (candidate.source_bytes_authenticated, False),
            (candidate.complete_inventory_scan_verified, False),
            (candidate.payload_derivation_verified, False),
            (candidate.source_authenticity_claimed, False),
        )
        if any(
            type(left) is not type(right) or left != right
            for left, right in fixed_pairs
        ):
            raise _fail()
        _validate_derived_ref(
            candidate.inclusion_ref,
            SOURCE_RECORD_INCLUSION_REF_PREFIX,
            rebuilt.inclusion_ref,
        )
        if not hmac.compare_digest(
            _strict_sha256(candidate.contract_sha256), rebuilt.contract_sha256
        ):
            raise _fail()
        return rebuilt
    except PublicEvidenceSourceRecordInclusionError:
        raise
    except Exception:
        raise _fail() from None
