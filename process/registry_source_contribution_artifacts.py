# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Resolve named published source facts and verify their complete bounded content."""

import json
import re
from dataclasses import asdict, dataclass
from uuid import UUID

from process.network_address_projection import _identifier
from process.network_membership_copy import MembershipCopyTarget
from process.registry_source_contribution_store import (
    ALL_FACT_ANCHOR_FIELDS,
    CONTRIBUTION_TABLE,
    FACT_ANCHOR_FIELDS,
    LEGACY_FACT_ANCHOR_FIELDS,
    MAX_RECEIPT_BYTES,
    SUMMARY_KEY,
    RegistrySourceContributionError,
    contribution_fact_sha256,
    validate_registry_source_contribution_receipt,
    validate_source_contribution_descriptor,
    verify_registry_source_contributions,
)

_ANCHOR_FIELDS = FACT_ANCHOR_FIELDS
_MAX_ARTIFACT_METADATA_BYTES = 2 * 1024 * 1024
_FACT_IDENTITY_FIELDS = ("recipe_sha256", "capture_sha256", "transform_version", "fact_sha256", "fact_rows")


@dataclass(frozen=True)
class SourceFactArtifactReference:
    """A named fact snapshot and its logical published generation."""

    fact_candidate_id: str
    fact_schema_name: str | None = None
    fact_table_name: str | None = None
    fact_generation_id: str | None = None

    def __post_init__(self):
        try:
            identity = UUID(self.fact_candidate_id) if type(self.fact_candidate_id) is str else None
            if identity is None or not identity.int or str(identity) != self.fact_candidate_id:
                raise ValueError
            names = self.fact_schema_name, self.fact_table_name, self.fact_generation_id
            if names == (None, None, None):
                return
            _identifier(self.fact_schema_name)
            if (
                self.fact_table_name != CONTRIBUTION_TABLE
                or type(self.fact_generation_id) is not str
                or re.fullmatch(r"[0-9a-f]{64}", self.fact_generation_id) is None
            ):
                raise ValueError
        except ValueError, TypeError, AttributeError:
            raise RegistrySourceContributionError("registry_source_artifact_reference_invalid") from None


@dataclass(frozen=True)
class ResolvedSourceFactArtifact:
    """Resolved published fact data and its immutable logical descriptor."""

    reference: SourceFactArtifactReference
    schema_name: str
    descriptor: dict

    def __post_init__(self):
        _identifier(self.schema_name)
        if self.reference.fact_schema_name is not None and self.schema_name != self.reference.fact_schema_name:
            raise RegistrySourceContributionError("registry_source_artifact_reference_invalid")

    @property
    def relation(self):
        """Return the schema-qualified published fact relation."""
        return f"{_identifier(self.schema_name)}.{CONTRIBUTION_TABLE}"


def _validated_descriptor(descriptor):
    if type(descriptor) is not dict:
        raise RegistrySourceContributionError("registry_source_artifact_reference_invalid")
    if descriptor.get("descriptor_version") == 4:
        raise RegistrySourceContributionError("registry_source_artifact_reference_invalid")
    if "descriptor_version" in descriptor:
        document = validate_source_contribution_descriptor(descriptor)
        return {key: value for key, value in document.items() if key not in ALL_FACT_ANCHOR_FIELDS}
    anchor_fields = set(descriptor) & ALL_FACT_ANCHOR_FIELDS
    if anchor_fields and anchor_fields not in (FACT_ANCHOR_FIELDS, LEGACY_FACT_ANCHOR_FIELDS):
        raise RegistrySourceContributionError("registry_source_artifact_reference_invalid")
    descriptor_by_field = {key: value for key, value in descriptor.items() if key not in ALL_FACT_ANCHOR_FIELDS}
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
    """Decode names or project a known retained reference to its logical candidate locator."""
    _validated_descriptor(descriptor)
    fields = set(descriptor) & ALL_FACT_ANCHOR_FIELDS
    if not fields or all(descriptor[field] is None for field in fields):
        return None
    if fields == LEGACY_FACT_ANCHOR_FIELDS:
        return SourceFactArtifactReference(descriptor["fact_candidate_id"])
    return SourceFactArtifactReference(**{field: descriptor[field] for field in FACT_ANCHOR_FIELDS})


def anchored_source_fact_descriptor(descriptor, artifact):
    """Preserve current mapping accounting while pointing directly to the original owner."""
    native = _validated_descriptor(descriptor)
    if type(artifact) is not ResolvedSourceFactArtifact or any(
        native[field] != artifact.descriptor[field] for field in _FACT_IDENTITY_FIELDS
    ):
        raise RegistrySourceContributionError("registry_source_artifact_identity_changed")
    if artifact.reference.fact_schema_name is None:
        raise RegistrySourceContributionError("registry_source_artifact_reference_invalid")
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
    candidates = await connection.fetch(
        f"SELECT c.* FROM {namespace}.network_membership_candidate c JOIN {namespace}.network_serving_manifest m "
        "USING(candidate_id) WHERE c.candidate_id=ANY($1::uuid[]) AND c.state='published' "
        "AND octet_length(c.validation_json::text)+octet_length(c.source_recipes_json::text) "
        "+octet_length(c.source_generations::text)<=$2 ORDER BY c.candidate_id",
        [UUID(identity) for identity in sorted(candidate_ids)],
        _MAX_ARTIFACT_METADATA_BYTES,
    )
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


async def load_published_artifact_validation(connection, candidate, control_schema):
    """Read the published logical generation and its completed data-validation report."""
    if not connection.is_in_transaction():
        raise RegistrySourceContributionError("registry_source_artifact_snapshot_required")
    copy_target = _artifact_copy_target(candidate)
    manifest = await connection.fetchrow(
        f"SELECT c.dataset_id,c.schema_id,c.producer_id,c.candidate_id,c.schema_name,c.source_generations,c.validation_json,m.manifest_sha256 "
        f"FROM {_identifier(control_schema)}.network_membership_candidate c "
        f"JOIN {_identifier(control_schema)}.network_serving_manifest m USING(candidate_id) "
        "WHERE c.candidate_id=$1 AND c.state='published'",
        UUID(copy_target.candidate_id),
    )
    if manifest is None or any(
        str(manifest[field]) != getattr(copy_target, field)
        for field in ("dataset_id", "schema_id", "producer_id", "candidate_id", "schema_name")
    ):
        raise RegistrySourceContributionError("registry_source_artifact_owner_unavailable")
    generation = manifest["manifest_sha256"]
    if type(generation) is not str or re.fullmatch(r"[0-9a-f]{64}", generation) is None:
        raise RegistrySourceContributionError("registry_source_artifact_generation_invalid")
    report = (
        json.loads(manifest["validation_json"])
        if type(manifest["validation_json"]) is str
        else manifest["validation_json"]
    )
    if type(report) is not dict:
        raise RegistrySourceContributionError("registry_source_artifact_validation_invalid")
    validation = report.get("final_validation")
    if validation is not None and (
        type(validation) is not dict
        or validation.get("candidate_id") != copy_target.candidate_id
        or validation.get("schema_name") != copy_target.schema_name
        or validation.get("complete") is not True
        or "source_generations" in validation
        and validation["source_generations"]
        != (
            json.loads(manifest["source_generations"])
            if type(manifest["source_generations"]) is str
            else manifest["source_generations"]
        )
    ):
        raise RegistrySourceContributionError("registry_source_artifact_validation_invalid")
    return {
        "candidate_id": copy_target.candidate_id,
        "generation_id": generation,
        "validation": validation,
        "semantic_validation": validation.get("semantic_validation") if validation is not None else None,
    }


async def _owner_sources(connection, candidate, control_schema, *, api_read=False, inventory_data=None):
    receipt = verify_registry_source_contributions(candidate)
    if receipt is None:
        raise RegistrySourceContributionError("registry_source_artifact_owner_missing")
    if "sources" in receipt:
        return tuple(receipt["sources"])
    from process.registry_source_inventory_reference import verify_source_recipe_metadata
    from process.registry_source_inventory_store import load_source_inventory
    from process.registry_source_inventory_summaries import (
        contribution_summary_v3,
        contribution_summary_v4,
        contribution_summary_v5,
    )

    try:
        expected = verify_source_recipe_metadata(candidate).inventory
        if expected is None:
            raise ValueError
        if inventory_data is None:
            reference, entries = await load_source_inventory(
                connection, _artifact_copy_target(candidate), candidate, control_schema=control_schema
            )
        else:
            reference, entries = inventory_data
        source_descriptors = tuple(
            json.loads(entry.contribution_json) for entry in entries if entry.contribution_json is not None
        )
        from process.registry_typed_source_metadata import typed_contribution_summary

        summarize = {
            6: typed_contribution_summary,
            3: contribution_summary_v3,
            4: contribution_summary_v4,
            5: contribution_summary_v5,
        }[receipt["revision"]]
        if (
            reference != expected
            or summarize(reference.root, sorted(source_descriptors, key=lambda source_pin: source_pin["recipe_sha256"]))
            != receipt
        ):
            raise ValueError
        return tuple(source_pin for source_pin in source_descriptors if source_pin.get("descriptor_version") != 4)
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


def _resolved_owned_source(candidate, generation, descriptor, reference, *, sources):
    if str(candidate["candidate_id"]) != reference.fact_candidate_id:
        raise RegistrySourceContributionError("registry_source_artifact_generation_changed")
    coordinates = candidate["schema_name"], CONTRIBUTION_TABLE, generation
    if reference.fact_schema_name is not None and coordinates != (
        reference.fact_schema_name,
        reference.fact_table_name,
        reference.fact_generation_id,
    ):
        raise RegistrySourceContributionError("registry_source_artifact_generation_changed")
    owning = _owning_descriptor(candidate, descriptor, sources=sources)
    named = SourceFactArtifactReference(reference.fact_candidate_id, *coordinates)
    return ResolvedSourceFactArtifact(named, candidate["schema_name"], dict(owning))


_UNFILTERED_FACT_HEAP_SQL = """SELECT c.relkind='r' AND c.relpersistence='p'
 AND c.relam=(SELECT oid FROM pg_catalog.pg_am WHERE amname='heap')
 AND NOT (c.relrowsecurity OR c.relforcerowsecurity OR c.relispartition)
 AND NOT EXISTS(SELECT FROM pg_catalog.pg_inherits WHERE inhrelid=c.oid OR inhparent=c.oid)
 AND NOT EXISTS(SELECT FROM pg_catalog.pg_rewrite WHERE ev_class=c.oid)
 FROM pg_catalog.pg_class c WHERE c.oid=pg_catalog.to_regclass($1)"""


async def _require_unfiltered_fact_heap(connection, schema_name):
    """API reads consume anchored facts directly; row filtering would return a silent partial page."""
    relation = f"{_identifier(schema_name)}.{CONTRIBUTION_TABLE}"
    if await connection.fetchval(_UNFILTERED_FACT_HEAP_SQL, relation) is not True:
        raise RegistrySourceContributionError("registry_source_artifact_physical_identity_changed")


async def _resolve_owner_page(connection, owners_by_identity, selected_by_owner, control_schema, *, api_read=False):
    resolved_by_recipe = {}
    for identity, candidate in owners_by_identity.items():
        evidence = await load_published_artifact_validation(connection, candidate, control_schema)
        sources = await _owner_sources(connection, candidate, control_schema, api_read=api_read)
        for descriptor, reference in selected_by_owner[identity]:
            resolved_by_recipe[descriptor["recipe_sha256"]] = _resolved_owned_source(
                candidate, evidence["generation_id"], descriptor, reference, sources=sources
            )
    return resolved_by_recipe


async def authenticate_anchored_source_fact_metadata(connection, descriptors, *, control_schema, api_read=False):
    """Resolve complete source scope and original named generations in bounded pages."""
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
    """Resolve original named generations and compare complete fact content before reuse."""
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
    """Bind retained source facts to recorded names and logical published generations."""
    candidates = await _published_artifact_candidates(connection, control_schema, {candidate_id})
    candidate = candidates[candidate_id]
    evidence = await load_published_artifact_validation(connection, candidate, control_schema)
    sources = await _owner_sources(connection, candidate, control_schema)
    reference = SourceFactArtifactReference(
        candidate_id, candidate["schema_name"], CONTRIBUTION_TABLE, evidence["generation_id"]
    )
    legacy_sources = [
        source
        for source in sources
        if source_fact_artifact_reference(source) is not None
        and source_fact_artifact_reference(source).fact_schema_name is None
    ]
    resolved = (
        await authenticate_anchored_source_fact_metadata(connection, legacy_sources, control_schema=control_schema)
        if legacy_sources
        else {}
    )
    named_sources = []
    for source in sources:
        selected = source_fact_artifact_reference(source)
        if selected is None:
            selected = reference
        elif selected.fact_schema_name is None:
            selected = resolved[source["recipe_sha256"]].reference
        named_sources.append({**_validated_descriptor(source), **asdict(selected)})
    return tuple(named_sources)


resolve_anchored_source_facts = verify_anchored_source_fact_contents
