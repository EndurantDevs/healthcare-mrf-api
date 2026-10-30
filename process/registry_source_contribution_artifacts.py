# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Direct references to original independently sealed source-fact owners.

This leaf validates references only. Its DTOs grant no phase authority for a new
candidate's own facts, and it never changes source receipts or copies fact rows.
"""

import json
import re
from dataclasses import asdict, dataclass
from uuid import UUID

from process.network_address_projection import _identifier
from process.network_candidate_custody import MAX_RECEIPT_BYTES as CUSTODY_MAX_RECEIPT_BYTES
from process.network_candidate_custody import verify_network_candidate_custody
from process.network_membership_copy import MembershipCopyTarget
from process.network_read_identity import NetworkReadIdentityError, verify_consumed_network_relations
from process.registry_source_contribution_store import (
    CONTRIBUTION_TABLE,
    MAX_RECEIPT_BYTES,
    SUMMARY_KEY,
    RegistrySourceContributionError,
    contribution_fact_sha256,
    validate_registry_source_contribution_receipt,
    validate_source_contribution_descriptor,
    verify_registry_source_contributions,
)

_ANCHOR_FIELDS = frozenset(("fact_candidate_id", "fact_table_oid", "fact_seal_sha256"))
_FACT_IDENTITY_FIELDS = ("recipe_sha256", "capture_sha256", "transform_version", "fact_sha256", "fact_rows")


@dataclass(frozen=True)
class SourceFactArtifactReference:
    """A direct physical owner and expected independent seal; never an alias head."""

    fact_candidate_id: str
    fact_table_oid: int
    fact_seal_sha256: str

    def __post_init__(self):
        try:
            identity = UUID(self.fact_candidate_id) if type(self.fact_candidate_id) is str else None
            if (
                identity is None
                or not identity.int
                or str(identity) != self.fact_candidate_id
                or type(self.fact_table_oid) is not int
                or not 0 < self.fact_table_oid < 2**32
                or type(self.fact_seal_sha256) is not str
                or re.fullmatch(r"[0-9a-f]{64}", self.fact_seal_sha256) is None
            ):
                raise ValueError
        except ValueError, TypeError, AttributeError:
            raise RegistrySourceContributionError("registry_source_artifact_reference_invalid") from None


@dataclass(frozen=True)
class ResolvedSourceFactArtifact:
    """An authenticated direct fact owner; API payload reads need their separate identity guard."""

    reference: SourceFactArtifactReference
    schema_name: str
    descriptor: dict

    @property
    def relation(self):
        """Return the exact native heap name established by independent custody."""
        return f"{_identifier(self.schema_name)}.{CONTRIBUTION_TABLE}"


def _validated_descriptor(descriptor):
    if type(descriptor) is not dict:
        raise RegistrySourceContributionError("registry_source_artifact_reference_invalid")
    if "descriptor_version" in descriptor:
        document = validate_source_contribution_descriptor(descriptor)
        return {key: value for key, value in document.items() if key not in _ANCHOR_FIELDS}
    anchor_fields = set(descriptor) & _ANCHOR_FIELDS
    if anchor_fields and anchor_fields != _ANCHOR_FIELDS:
        raise RegistrySourceContributionError("registry_source_artifact_reference_invalid")
    descriptor_by_field = {key: value for key, value in descriptor.items() if key not in _ANCHOR_FIELDS}
    validate_registry_source_contribution_receipt(
        {
            "component": SUMMARY_KEY,
            "revision": 1,
            "table_name": CONTRIBUTION_TABLE,
            "sources": [descriptor_by_field],
        }
    )
    return descriptor_by_field


def source_fact_artifact_reference(descriptor):
    """Validate all-or-none anchor fields; absent fields describe locally owned facts."""
    _validated_descriptor(descriptor)
    anchor_by_field = {key: descriptor.get(key) for key in _ANCHOR_FIELDS}
    if all(value is None for value in anchor_by_field.values()):
        return None
    if any(value is None for value in anchor_by_field.values()):
        raise RegistrySourceContributionError("registry_source_artifact_reference_invalid")
    return SourceFactArtifactReference(**anchor_by_field)


def anchored_source_fact_descriptor(descriptor, artifact):
    """Preserve current mapping accounting while pointing directly to the original owner."""
    native = _validated_descriptor(descriptor)
    if type(artifact) is not ResolvedSourceFactArtifact or any(
        native[field] != artifact.descriptor[field] for field in _FACT_IDENTITY_FIELDS
    ):
        raise RegistrySourceContributionError("registry_source_artifact_identity_changed")
    return {**native, **asdict(artifact.reference)}


def _owning_descriptor(candidate, requested, *, sources=None):
    if sources is None:
        receipt = verify_registry_source_contributions(candidate)
        if receipt is None:
            raise RegistrySourceContributionError("registry_source_artifact_owner_missing")
        sources = receipt["sources"]
    matches = [source for source in sources if source["recipe_sha256"] == requested["recipe_sha256"]]
    if len(matches) != 1 or source_fact_artifact_reference(matches[0]) is not None:
        raise RegistrySourceContributionError("registry_source_artifact_chain_forbidden")
    if any(matches[0][field] != requested[field] for field in _FACT_IDENTITY_FIELDS):
        raise RegistrySourceContributionError("registry_source_artifact_identity_changed")
    return matches[0]


async def _published_artifact_candidates(connection, control_schema, candidate_ids, *, api_read=False):
    namespace = _identifier(control_schema)
    if api_read:
        from process.network_candidate_custody import read_network_control_relation_pins

        relations = await read_network_control_relation_pins(
            connection, ("network_membership_candidate", "network_serving_manifest"), control_schema=control_schema
        )
        pid = connection.get_server_pid()
    candidates = await connection.fetch(
        f"SELECT c.* FROM {namespace}.network_membership_candidate c JOIN {namespace}.network_serving_manifest m "
        "USING(candidate_id) WHERE c.candidate_id=ANY($1::uuid[]) AND c.state='published' "
        "AND octet_length(c.validation_json::text)+octet_length(c.source_recipes_json::text) "
        "+octet_length(c.source_generations::text)<=$2 ORDER BY c.candidate_id",
        [UUID(identity) for identity in sorted(candidate_ids)],
        CUSTODY_MAX_RECEIPT_BYTES,
    )
    if api_read:
        try:
            await verify_consumed_network_relations(connection, relations, expected_backend_pid=pid)
        except NetworkReadIdentityError as error:
            raise RegistrySourceContributionError("registry_source_artifact_owner_unavailable") from error
    by_identity = {str(candidate["candidate_id"]): dict(candidate) for candidate in candidates}
    if set(by_identity) != candidate_ids:
        raise RegistrySourceContributionError("registry_source_artifact_owner_unavailable")
    return by_identity


def _artifact_copy_target(candidate):
    return MembershipCopyTarget(
        *(
            str(candidate[field])
            for field in (
                "dataset_id",
                "schema_id",
                "producer_id",
                "candidate_id",
                "schema_name",
            )
        )
    )


async def _verify_artifact_owner(connection, candidate, control_schema, *, api_read=False):
    if api_read:
        from process.network_candidate_custody import verify_network_candidate_read_metadata

        return await verify_network_candidate_read_metadata(
            connection, _artifact_copy_target(candidate), control_schema=control_schema, required=True
        )
    return await verify_network_candidate_custody(
        connection, _artifact_copy_target(candidate), control_schema=control_schema, required=True
    )


async def _owner_sources(connection, candidate, custody, control_schema, *, api_read=False):
    receipt = verify_registry_source_contributions(candidate)
    if receipt is None:
        raise RegistrySourceContributionError("registry_source_artifact_owner_missing")
    if "sources" in receipt:
        return tuple(receipt["sources"])
    from db.registry_source_inventory_schema import INVENTORY_TABLE
    from process.registry_source_inventory_reference import verify_source_recipe_metadata
    from process.registry_source_inventory_store import _load_locked_source_inventory
    from process.registry_source_inventory_summaries import (
        contribution_summary_v3,
        contribution_summary_v4,
        contribution_summary_v5,
    )

    try:
        expected = verify_source_recipe_metadata(candidate).inventory
        inventory_oid = custody["receipt"]["closure"]["relation_oids"][INVENTORY_TABLE]
        if expected is None or type(inventory_oid) is not int or not 0 < inventory_oid < 2**32:
            raise ValueError
        if api_read:
            from process.registry_source_inventory_store import load_source_inventory_for_api

            reference, entries = await load_source_inventory_for_api(
                connection, _artifact_copy_target(candidate), candidate, control_schema=control_schema
            )
        else:
            reference, entries = await _load_locked_source_inventory(
                connection,
                _artifact_copy_target(candidate),
                candidate,
                expected_table_oid=inventory_oid,
                control_schema=control_schema,
            )
        source_descriptors = tuple(
            json.loads(entry.contribution_json) for entry in entries if entry.contribution_json is not None
        )
        summarize = {
            3: contribution_summary_v3,
            4: contribution_summary_v4,
            5: contribution_summary_v5,
        }[receipt["revision"]]
        if (
            reference != expected
            or summarize(reference.root, sorted(source_descriptors, key=lambda source: source["recipe_sha256"]))
            != receipt
        ):
            raise ValueError
        return source_descriptors
    except ValueError, TypeError, KeyError, UnicodeError, RecursionError:
        raise RegistrySourceContributionError("registry_source_artifact_inventory_changed") from None


def _selected_by_owner(descriptors):
    try:
        if type(descriptors) not in {tuple, list} or len(descriptors) > 100:
            raise ValueError
        if len(json.dumps(descriptors, allow_nan=False).encode()) > MAX_RECEIPT_BYTES:
            raise ValueError
    except ValueError, TypeError, RecursionError:
        raise RegistrySourceContributionError("registry_source_artifact_reference_invalid") from None
    recipe_ids, selected_by_owner = set(), {}
    for descriptor in descriptors:
        reference = source_fact_artifact_reference(descriptor)
        if descriptor["recipe_sha256"] in recipe_ids:
            raise RegistrySourceContributionError("registry_source_artifact_reference_invalid")
        recipe_ids.add(descriptor["recipe_sha256"])
        if reference is not None:
            selected_by_owner.setdefault(reference.fact_candidate_id, []).append((descriptor, reference))
    return selected_by_owner


def _authenticate_owned_source(owner, custody, descriptor, reference, *, sources=None):
    if (
        custody["seal_sha256"] != reference.fact_seal_sha256
        or custody["receipt"]["closure"]["relation_oids"].get(CONTRIBUTION_TABLE) != reference.fact_table_oid
        or owner["schema_name"] != "network_candidate_" + UUID(reference.fact_candidate_id).hex
    ):
        raise RegistrySourceContributionError("registry_source_artifact_physical_identity_changed")
    owning = _owning_descriptor(owner, descriptor, sources=sources)
    return ResolvedSourceFactArtifact(reference, owner["schema_name"], dict(owning))


async def _resolve_owner_page(connection, owners_by_identity, selected_by_owner, control_schema, *, api_read=False):
    resolved_by_recipe = {}
    for identity, candidate in owners_by_identity.items():
        custody = await _verify_artifact_owner(
            connection, candidate, control_schema, **({"api_read": True} if api_read else {})
        )
        sources = await _owner_sources(
            connection, candidate, custody, control_schema, **({"api_read": True} if api_read else {})
        )
        for descriptor, reference in selected_by_owner[identity]:
            resolved_by_recipe[descriptor["recipe_sha256"]] = _authenticate_owned_source(
                candidate, custody, descriptor, reference, sources=sources
            )
    return resolved_by_recipe


async def authenticate_anchored_source_fact_metadata(connection, descriptors, *, control_schema, api_read=False):
    """Resolve bounded direct owners once each, including retained ineligible manifests.

    Own-source descriptors are omitted: their physical identity requires the
    caller's checked creation/closure phase. No predecessor chains are followed.
    This authenticates metadata and native closure, without reading fact payloads.
    Staging additionally calls the distinct bounded content verifier below.
    """
    if (
        type(api_read) is not bool
        or not connection.is_in_transaction()
        or await connection.fetchval("SHOW transaction_isolation")
        not in {
            "repeatable read",
            "serializable",
        }
    ):
        raise RegistrySourceContributionError("registry_source_artifact_scope_invalid")
    if api_read and await connection.fetchval("SHOW transaction_read_only") != "on":
        raise RegistrySourceContributionError("registry_source_artifact_scope_invalid")
    selected_by_owner = _selected_by_owner(descriptors)
    owner_ids = sorted(selected_by_owner)
    resolved_by_recipe = {}
    for offset in range(0, len(owner_ids), 4):
        requested_ids = set(owner_ids[offset : offset + 4])
        owners_by_identity = await _published_artifact_candidates(
            connection, control_schema, requested_ids, **({"api_read": True} if api_read else {})
        )
        resolved_by_recipe.update(
            await _resolve_owner_page(
                connection,
                owners_by_identity,
                selected_by_owner,
                control_schema,
                **({"api_read": True} if api_read else {}),
            )
        )
    return resolved_by_recipe


async def verify_anchored_source_fact_contents(connection, descriptors, *, control_schema):
    """Authenticate direct owners and verify bounded native content before stage reuse."""
    try:
        if type(descriptors) not in {tuple, list} or len(descriptors) > 1000:
            raise ValueError
        recipe_ids = [descriptor["recipe_sha256"] for descriptor in descriptors]
        if len(set(recipe_ids)) != len(recipe_ids):
            raise ValueError
    except ValueError, TypeError, KeyError, RecursionError:
        raise RegistrySourceContributionError("registry_source_artifact_reference_invalid") from None
    resolved_by_recipe = {}
    for offset in range(0, max(1, len(descriptors)), 100):
        resolved_by_recipe.update(
            await authenticate_anchored_source_fact_metadata(
                connection, descriptors[offset : offset + 100], control_schema=control_schema
            )
        )
    for recipe_sha256, artifact in resolved_by_recipe.items():
        if await contribution_fact_sha256(connection, artifact.relation, recipe_sha256) != (
            artifact.descriptor["fact_sha256"],
            artifact.descriptor["fact_rows"],
        ):
            raise RegistrySourceContributionError("registry_source_artifact_content_changed")
    return resolved_by_recipe


async def retained_source_fact_descriptors(connection, candidate_id, *, control_schema):
    """Bind each published source to its original own heap or its existing direct anchor."""
    candidates = await _published_artifact_candidates(connection, control_schema, {candidate_id})
    candidate = candidates[candidate_id]
    custody = await _verify_artifact_owner(connection, candidate, control_schema)
    sources = await _owner_sources(connection, candidate, custody, control_schema)
    reference = SourceFactArtifactReference(
        candidate_id, custody["receipt"]["closure"]["relation_oids"][CONTRIBUTION_TABLE], custody["seal_sha256"]
    )
    return tuple(
        {**source, **asdict(reference)} if source_fact_artifact_reference(source) is None else dict(source)
        for source in sources
    )


resolve_anchored_source_facts = verify_anchored_source_fact_contents
