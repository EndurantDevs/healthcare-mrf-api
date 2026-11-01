# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact site memberships from a pinned retained FHIR projection."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass, replace
from datetime import date

from process.network_address_projection import _identifier
from process.network_approved_membership_source import ApprovedMembershipSource
from process.network_approved_source_bindings import (
    APPROVED_NETWORK_BINDINGS_SQL,
    ApprovedNetworkSourceBindingError,
    RegistryNetworkSourceCoordinates,
    require_approved_network_source_bindings,
)
from process.network_cms_provider_identity import cms_provider_sql_fields
from process.network_fhir_retained_capture_contract import (
    GENERIC_UNVERIFIED_NETWORK,
    SEMANTIC_PROFILE,
    RetainedFHIRCapture,
)
from process.network_fhir_retained_capture_contract import (
    PRODUCER as RETAINED_FHIR_PRODUCER,
)
from process.network_fhir_retained_capture_contract import (
    validate_capture as validate_retained_fhir_capture,
)
from process.network_fhir_source_epoch import (
    FHIRSourceEpochError,
    RetainedCMSFHIRSourceEpoch,
    require_retained_cms_fhir_source_epoch,
    validate_retained_cms_fhir_source_epoch,
)
from process.network_fhir_source_witness_contract import (
    PRODUCER as SOURCE_WITNESS_PRODUCER,
)
from process.network_fhir_source_witness_contract import (
    SourceWitnessCapture,
    SourceWitnessCaptureAnchor,
    SourceWitnessContractError,
    require_capture_anchor,
)
from process.network_membership_candidate_lifecycle import _control_namespace, _locked_candidate
from process.network_membership_copy import MAX_INPUT_BYTES, MAX_ROWS, copy_network_membership_batch


class FHIRMembershipSourceError(ValueError):
    """Pinned source evidence is unavailable, unresolved or exceeds batch bounds."""


class FHIRMembershipBatchBoundsError(FHIRMembershipSourceError):
    """A source page must be split before native membership COPY."""


@dataclass(frozen=True)
class PinnedFHIRMembershipSource:
    """Trusted source admission coordinates; no current-head lookup is performed."""

    schema_name: str
    source_id: str
    endpoint_id: str
    dataset_id: str
    dataset_sha256: str
    release_id: str
    resource_table_oid: int
    alias_scope: str
    as_of: str
    custody_owner_role: str | None = None
    custody_runtime_roles: tuple[str, ...] = ()
    custody_proof_sha256: str | None = None
    custody_catalog_sha256: str | None = None
    retained_epoch: RetainedCMSFHIRSourceEpoch | None = None
    retained_successor: PinnedFHIRMembershipSource | None = None
    retained_capture: RetainedFHIRCapture | None = None
    source_witness_capture: SourceWitnessCapture | None = None
    source_witness_anchor: SourceWitnessCaptureAnchor | None = None

    def __post_init__(self):
        _identifier(self.schema_name)
        for field_name, maximum in (
            ("source_id", 64),
            ("endpoint_id", 64),
            ("dataset_id", 96),
            ("release_id", 256),
            ("alias_scope", 512),
        ):
            field_text = getattr(self, field_name)
            if (
                type(field_text) is not str
                or not 1 <= len(field_text.encode()) <= maximum
                or field_text.strip() != field_text
                or not field_text.isprintable()
            ):
                raise FHIRMembershipSourceError("FHIR source coordinates are invalid")
        if type(self.dataset_sha256) is not str or re.fullmatch(r"[0-9a-f]{64}", self.dataset_sha256) is None:
            raise FHIRMembershipSourceError("FHIR dataset digest is invalid")
        if type(self.resource_table_oid) is not int or not 0 < self.resource_table_oid <= 4294967295:
            raise FHIRMembershipSourceError("FHIR source relation identity is invalid")
        if type(self.as_of) is not str or date.fromisoformat(self.as_of).isoformat() != self.as_of:
            raise FHIRMembershipSourceError("FHIR membership date is invalid")
        _validate_custody_coordinates(self)
        _validate_epoch_coordinates(self)
        _validate_successor_coordinates(self)
        _validate_retained_capture(self)
        _validate_source_witness(self)

    @property
    def coordinates(self):
        """Preserve the original nine-coordinate wire for sources without custody."""
        coordinates_by_name = asdict(self)
        if self.custody_owner_role is None:
            for name in tuple(coordinates_by_name):
                if name.startswith("custody_"):
                    coordinates_by_name.pop(name)
        if self.retained_epoch is None:
            coordinates_by_name.pop("retained_epoch")
        else:
            coordinates_by_name["retained_epoch"] = self.retained_epoch.as_dict()
        if self.retained_successor is None:
            coordinates_by_name.pop("retained_successor")
        else:
            coordinates_by_name["retained_successor"] = self.retained_successor.coordinates
        if self.retained_capture is None:
            coordinates_by_name.pop("retained_capture")
        else:
            coordinates_by_name["retained_capture"] = self.retained_capture.as_dict()
        if self.source_witness_capture is None:
            coordinates_by_name.pop("source_witness_capture")
            coordinates_by_name.pop("source_witness_anchor")
        else:
            coordinates_by_name["source_witness_capture"] = self.source_witness_capture.as_dict()
        return coordinates_by_name

    @property
    def read_schema_name(self):
        """Read closed retained tables while keeping original source provenance."""
        physical = fhir_physical_source(self)
        capture = fhir_normalized_capture(physical)
        if capture is not None:
            return capture.schema_name
        return physical.retained_epoch.schema_name if physical.retained_epoch is not None else physical.schema_name

    @property
    def read_resource_table_oid(self):
        """Bind the exact physical resource heap selected by retained custody."""
        physical = fhir_physical_source(self)
        capture = fhir_normalized_capture(physical)
        if capture is not None:
            return dict(capture.relation_oids)["provider_directory_dataset_resource"]
        if physical.retained_epoch is None:
            return physical.resource_table_oid
        return dict(physical.retained_epoch.relation_oids)["provider_directory_dataset_resource"]

    @property
    def generation_id(self):
        """Bind every pinned coordinate for the candidate's source generation map."""
        return hashlib.sha256(json.dumps(self.coordinates, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _validate_custody_coordinates(source):
    """Accept either no custody or one complete CMS-only retained receipt."""
    if (
        source.custody_owner_role,
        source.custody_runtime_roles,
        source.custody_proof_sha256,
        source.custody_catalog_sha256,
    ) == (None, (), None, None):
        return
    if (
        source.source_id != "cms-npd"
        or type(source.custody_owner_role) is not str
        or type(source.custody_runtime_roles) is not tuple
        or not 1 <= len(source.custody_runtime_roles) <= 32
        or any(type(role) is not str for role in source.custody_runtime_roles)
        or tuple(sorted(set(source.custody_runtime_roles))) != source.custody_runtime_roles
        or source.custody_owner_role in source.custody_runtime_roles
        or any(
            type(digest) is not str or re.fullmatch(r"[0-9a-f]{64}", digest) is None
            for digest in (source.custody_proof_sha256, source.custody_catalog_sha256)
        )
    ):
        raise FHIRMembershipSourceError("FHIR custody coordinates are invalid")
    for role in (source.custody_owner_role, *source.custody_runtime_roles):
        _identifier(role)


def _validate_successor_coordinates(source):
    successor = source.retained_successor
    if successor is None:
        return
    if (
        type(successor) is not PinnedFHIRMembershipSource
        or successor.retained_successor is not None
        or any(
            getattr(source, name) != getattr(successor, name)
            for name in ("schema_name", "source_id", "endpoint_id", "alias_scope")
        )
    ):
        raise FHIRMembershipSourceError("FHIR successor coordinates are invalid")
    if source.source_id == "cms-npd":
        if successor.retained_epoch is None or (source.retained_epoch is None and source.custody_owner_role is None):
            raise FHIRMembershipSourceError("FHIR successor coordinates are invalid")
    elif (
        type(source.retained_capture) is not RetainedFHIRCapture
        or type(successor.retained_capture) is not RetainedFHIRCapture
        or source.retained_capture.origin.admission_profile != SEMANTIC_PROFILE
        or successor.retained_capture.origin.admission_profile != SEMANTIC_PROFILE
        or successor.as_of != successor.retained_capture.origin.semantic_projection_as_of
    ):
        raise FHIRMembershipSourceError("FHIR successor coordinates are invalid")


def fhir_physical_source(source):
    """Select one independently retained physical capture, preserving the logical anchor."""
    if type(source) is not PinnedFHIRMembershipSource:
        raise FHIRMembershipSourceError("Pinned FHIR source is invalid")
    return source.retained_successor or source


def fhir_normalized_capture(source):
    """Select the distinct normalized capture without converting its provenance."""
    return source.retained_capture or source.source_witness_capture


def with_fhir_successor(source, successor):
    """Replace a flat accepted capture without extending its original enrollment anchor."""
    if type(successor) is not PinnedFHIRMembershipSource or successor.retained_successor is not None:
        raise FHIRMembershipSourceError("FHIR successor coordinates are invalid")
    physical = fhir_physical_source(source)
    if physical == successor:
        return source
    original = replace(source, retained_successor=None)
    return original if original == successor else replace(original, retained_successor=successor)


def _validate_epoch_coordinates(source):
    if source.retained_epoch is None:
        return
    if source.source_id != "cms-npd" or source.custody_owner_role is not None:
        raise FHIRMembershipSourceError("FHIR epoch coordinates are invalid")
    try:
        epoch = validate_retained_cms_fhir_source_epoch(source.retained_epoch)
    except FHIRSourceEpochError:
        raise FHIRMembershipSourceError("FHIR epoch coordinates are invalid") from None
    origin = (
        source.schema_name,
        source.source_id,
        source.endpoint_id,
        source.dataset_id,
        source.dataset_sha256,
        source.release_id,
        source.resource_table_oid,
        source.alias_scope,
        source.as_of,
    )
    if type(source.retained_epoch) is not RetainedCMSFHIRSourceEpoch or epoch.origin_coordinates != origin:
        raise FHIRMembershipSourceError("FHIR epoch coordinates are invalid")


def _validate_retained_capture(source_pin):
    """Keep new normalized capture evidence distinct from CMS and historical aliases."""
    if source_pin.retained_capture is None:
        return
    if type(source_pin.retained_capture) is not RetainedFHIRCapture:
        raise FHIRMembershipSourceError("FHIR retained capture coordinates are invalid")
    capture = validate_retained_fhir_capture(source_pin.retained_capture)
    origin = capture.origin
    if (
        source_pin.source_id == "cms-npd"
        or source_pin.retained_epoch is not None
        or source_pin.custody_owner_role is not None
        or (
            source_pin.schema_name,
            source_pin.source_id,
            source_pin.endpoint_id,
            source_pin.dataset_id,
            source_pin.dataset_sha256,
            source_pin.resource_table_oid,
        )
        != (
            origin.schema_name,
            origin.source_id,
            origin.endpoint_id,
            origin.dataset_id,
            origin.dataset_sha256,
            origin.resource_parent_oid,
        )
        or source_pin.release_id != capture.release_id
    ):
        raise FHIRMembershipSourceError("FHIR retained capture coordinates are invalid")


def _validate_source_witness(source_pin):
    """Bind Source provenance and independently issued Core coordinates as one closed pin."""
    capture, anchor = source_pin.source_witness_capture, source_pin.source_witness_anchor
    if capture is None and anchor is None:
        return
    if (
        type(capture) is not SourceWitnessCapture
        or type(anchor) is not SourceWitnessCaptureAnchor
        or source_pin.retained_capture is not None
        or source_pin.retained_epoch is not None
        or source_pin.custody_owner_role is not None
        or source_pin.retained_successor is not None
    ):
        raise FHIRMembershipSourceError("FHIR Source witness coordinates are invalid")
    try:
        require_capture_anchor(capture, anchor)
    except SourceWitnessContractError:
        raise FHIRMembershipSourceError("FHIR Source witness coordinates are invalid") from None
    origin = capture.origin
    if (
        source_pin.schema_name,
        source_pin.source_id,
        source_pin.endpoint_id,
        source_pin.dataset_id,
        source_pin.dataset_sha256,
        source_pin.resource_table_oid,
        source_pin.release_id,
        source_pin.as_of,
    ) != (
        origin.schema_name,
        origin.source_id,
        origin.endpoint_id,
        origin.dataset_id,
        origin.dataset_sha256,
        origin.resource_parent_oid,
        capture.release_id,
        origin.semantic_projection_as_of,
    ):
        raise FHIRMembershipSourceError("FHIR Source witness coordinates are invalid")


@dataclass(frozen=True)
class FHIRResourceDisposition:
    """One retained resource position, independently of its membership fanout."""

    generation_id: str
    resource_type: str
    resource_id: str
    payload_sha256: str
    outcome: str
    reason: str | None


@dataclass(frozen=True)
class FHIRMembershipBatch:
    """Bounded exact rows and the immutable source recipe used to extract them."""

    source: PinnedFHIRMembershipSource
    source_rows: int
    membership_rows: int
    unresolved_rows: int
    next_resource: tuple[str, str] | None
    input_bytes: bytes
    input_sha256: str
    approved_source: ApprovedMembershipSource | None = None
    binding_coordinates: RegistryNetworkSourceCoordinates | None = None
    approved_only: bool = False
    omitted_rows: int = 0
    accepted_source_rows: int = 0
    unresolved_source_rows: int = 0
    omitted_source_rows: int = 0
    source_outcomes: tuple[FHIRResourceDisposition, ...] = ()

    complete_raw_facts: bool = True
    source_fact_bytes: bytes = b"[]"
    provider_only_rows: int = 0

    @property
    def generation_id(self):
        """Bind reviewed mappings without changing the legacy source generation."""
        return _generation_id(self.source, self.approved_source, self.binding_coordinates, self.approved_only)


def _generation_id(source, approved_source, coordinates, approved_only):
    """Preserve the existing source/approved-map generation for positions and retries."""
    _reviewed_descriptor(source, approved_source, coordinates, approved_only)
    if approved_source is None and coordinates is None:
        return source.generation_id
    recipe_dict = {
        "source": source.coordinates,
        "approved_source": asdict(approved_source),
        "binding_coordinates": asdict(coordinates),
    }
    if approved_only:
        recipe_dict["selection_policy"] = "approved-bindings-only"
    return hashlib.sha256(json.dumps(recipe_dict, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _reviewed_descriptor(source, approved_source, coordinates, approved_only=False):
    """Require paired pins and their exact admitted FHIR coordinates."""
    if type(source) is not PinnedFHIRMembershipSource or type(approved_only) is not bool:
        raise FHIRMembershipSourceError("Pinned FHIR source is invalid")
    if approved_source is None and coordinates is None:
        if approved_only or fhir_normalized_capture(source) is not None:
            raise FHIRMembershipSourceError("Reviewed FHIR source coordinates are invalid")
        return None
    if (
        type(approved_source) is not ApprovedMembershipSource
        or type(coordinates) is not RegistryNetworkSourceCoordinates
        or coordinates.source_system != "fhir"
        or coordinates.source_id != source.source_id
        or coordinates.dataset_schema != source.schema_name
        or coordinates.dataset_id != source.dataset_id
    ):
        raise FHIRMembershipSourceError("Reviewed FHIR source coordinates are invalid")
    if source.retained_capture is not None and (
        coordinates.producer_id != RETAINED_FHIR_PRODUCER
        or coordinates.edition_id != source.retained_capture.capture_id
    ):
        raise FHIRMembershipSourceError("Reviewed FHIR source coordinates are invalid")
    if source.source_witness_capture is not None and (
        coordinates.producer_id != SOURCE_WITNESS_PRODUCER
        or coordinates.edition_id != source.source_witness_capture.capture_id
    ):
        raise FHIRMembershipSourceError("Reviewed FHIR source coordinates are invalid")
    return {**asdict(coordinates), "alias_scope": source.alias_scope, "source_key_kind": "organization_resource_id"}


async def _require_pinned_source(connection, source_pin, descriptor=None, retained_read=None, *, api_read=False):
    if type(api_read) is not bool:
        raise FHIRMembershipSourceError("Pinned FHIR source is unavailable")
    if api_read:
        return await _require_flat_pinned_source(connection, source_pin, descriptor, retained_read, api_read=True)
    if source_pin.retained_successor is None:
        return await _require_flat_pinned_source(connection, source_pin, descriptor, retained_read)
    if descriptor is None:
        raise FHIRMembershipSourceError("Reviewed FHIR source coordinates are invalid")
    original = replace(source_pin, retained_successor=None)
    physical = fhir_physical_source(source_pin)
    if original.retained_capture is not None:
        reads = retained_read
        if reads is None:
            from process.network_fhir_retained_capture import pin_retained_fhir_read

            reads = RetainedFHIRSuccessorReads(
                await pin_retained_fhir_read(connection, original.retained_capture),
                await pin_retained_fhir_read(connection, physical.retained_capture),
            )
        if type(reads) is not RetainedFHIRSuccessorReads:
            raise FHIRMembershipSourceError("Pinned FHIR source is unavailable")
        await _require_flat_pinned_source(connection, original, descriptor, reads.original)
        await _require_flat_pinned_source(connection, physical, None, reads.physical)
        return
    await _require_flat_pinned_source(connection, original, descriptor)
    declaration = await _successor_descriptor(connection, physical, descriptor)
    await _require_flat_pinned_source(connection, physical, declaration)


@dataclass(frozen=True)
class RetainedFHIRSuccessorReads:
    """Two genuine transaction-local capabilities; serialized pins confer neither."""

    original: object
    physical: object


async def _successor_descriptor(connection, physical, original):
    """Read the closed declaration from the authenticated retained capture itself."""
    namespace = _identifier(physical.read_schema_name)
    header = await connection.fetchrow(
        f"""SELECT CASE WHEN octet_length((publication_metadata_summary_json->'network_bindings')::text)<=4096
          THEN (publication_metadata_summary_json->'network_bindings')::text END AS declaration,
          publication_metadata_summary_json->>'semantic_projection_as_of' AS as_of
          FROM {namespace}.provider_directory_endpoint_dataset WHERE dataset_id=$1""",
        physical.dataset_id,
    )
    try:
        declaration = json.loads(header["declaration"])
        coordinates = RegistryNetworkSourceCoordinates(
            **{
                key: coordinate_value
                for key, coordinate_value in declaration.items()
                if key not in {"alias_scope", "source_key_kind"}
            }
        )
        expected_by_field = {
            **asdict(coordinates),
            "alias_scope": physical.alias_scope,
            "source_key_kind": "organization_resource_id",
        }
        is_matching = (
            declaration == expected_by_field
            and header["as_of"] == physical.as_of
            and coordinates.dataset_id == physical.dataset_id
            and all(
                declaration[key] == original[key]
                for key in (
                    "source_system",
                    "source_id",
                    "dataset_schema",
                    "producer_id",
                    "alias_scope",
                    "source_key_kind",
                )
            )
        )
    except AttributeError, KeyError, TypeError, ValueError:
        is_matching = False
    if not is_matching:
        raise FHIRMembershipSourceError("Pinned FHIR successor is unavailable")
    return declaration


async def _require_api_epoch_pinned_source(connection, source_pin, descriptor, retained_read):
    """Check CMS epoch metadata; returned pins still need guards on actual payload reads."""
    from process.network_compact_layout import CompactAddressRelation
    from process.network_fhir_source_epoch import _require_api_metadata_read
    from process.network_read_identity import verify_consumed_network_relations

    await _require_api_metadata_read(connection, True)
    if (
        source_pin.retained_epoch is None
        or source_pin.retained_capture is not None
        or source_pin.retained_successor is not None
        or retained_read is not None
    ):
        raise FHIRMembershipSourceError("Pinned FHIR source is unavailable")
    try:
        epoch = await require_retained_cms_fhir_source_epoch(
            connection, source_pin, source_pin.retained_epoch, verify_content=False, api_read=True
        )
    except FHIRSourceEpochError as error:
        raise FHIRMembershipSourceError("Pinned FHIR source is unavailable") from error
    namespace = _identifier(source_pin.read_schema_name)
    backend_pid = await connection.fetchval("SELECT pg_backend_pid()")
    matches = await connection.fetchval(
        f"""SELECT EXISTS (SELECT 1 FROM {namespace}.provider_directory_endpoint_dataset dataset
          JOIN pg_namespace ns ON ns.nspname=$1
          JOIN pg_class relation ON relation.relnamespace=ns.oid
            AND relation.relname='provider_directory_dataset_resource' AND relation.relkind='r'
          WHERE dataset.dataset_id=$2 AND dataset.endpoint_id=$3 AND dataset.dataset_hash=$4
            AND dataset.status IN ('validated','published','superseded')
            AND dataset.publication_metadata_summary_json->'source_ids'=jsonb_build_array($5::text)
            AND ($7::jsonb IS NULL OR dataset.publication_metadata_summary_json->'network_bindings'=$7::jsonb)
            AND relation.oid=$6::oid)""",
        source_pin.read_schema_name,
        source_pin.dataset_id,
        source_pin.endpoint_id,
        source_pin.dataset_sha256,
        source_pin.source_id,
        source_pin.read_resource_table_oid,
        json.dumps(descriptor) if descriptor is not None else None,
    )
    await verify_consumed_network_relations(
        connection,
        (
            CompactAddressRelation(
                epoch.schema_name,
                "provider_directory_endpoint_dataset",
                dict(epoch.relation_oids)["provider_directory_endpoint_dataset"],
            ),
        ),
        expected_backend_pid=backend_pid,
    )
    if not matches:
        raise FHIRMembershipSourceError("Pinned FHIR source is unavailable")
    return epoch


async def _require_flat_pinned_source(connection, source_pin, descriptor, retained_read=None, *, api_read=False):
    if type(api_read) is not bool:
        raise FHIRMembershipSourceError("Pinned FHIR source is unavailable")
    if api_read:
        return await _require_api_epoch_pinned_source(connection, source_pin, descriptor, retained_read)
    if source_pin.source_witness_capture is not None:
        from process.network_fhir_source_witness_capture import SourceWitnessRead

        if type(retained_read) is not SourceWitnessRead or descriptor is None:
            raise FHIRMembershipSourceError("Pinned FHIR Source witness read is unavailable")
        require_capture_anchor(source_pin.source_witness_capture, source_pin.source_witness_anchor)
        await retained_read.verify(connection, source_pin.source_witness_capture)
        return
    if source_pin.retained_capture is not None:
        from process.network_fhir_retained_capture import RetainedFHIRRead, pin_retained_fhir_read

        if retained_read is None:
            retained_read = await pin_retained_fhir_read(connection, source_pin.retained_capture)
        if type(retained_read) is not RetainedFHIRRead:
            raise FHIRMembershipSourceError("Pinned FHIR source is unavailable")
        await retained_read.verify(connection, source_pin.retained_capture)
        return
    if source_pin.retained_epoch is not None:
        try:
            await require_retained_cms_fhir_source_epoch(
                connection, source_pin, source_pin.retained_epoch, verify_content=False
            )
        except FHIRSourceEpochError as error:
            raise FHIRMembershipSourceError("Pinned FHIR source is unavailable") from error
    namespace = _identifier(source_pin.read_schema_name)
    admission = (
        "dataset.status IN ('validated','published','superseded')"
        if source_pin.retained_epoch is not None
        else "dataset.status IN ('published','superseded') AND dataset.published_at IS NOT NULL"
    )
    matches = await connection.fetchval(
        f"""SELECT EXISTS (SELECT 1 FROM {namespace}.provider_directory_endpoint_dataset dataset
          JOIN pg_namespace ns ON ns.nspname=$1
          JOIN pg_class relation ON relation.relnamespace=ns.oid
            AND relation.relname='provider_directory_dataset_resource' AND relation.relkind='r'
          WHERE dataset.dataset_id=$2 AND dataset.endpoint_id=$3 AND dataset.dataset_hash=$4
            AND {admission}
            AND dataset.publication_metadata_summary_json->'source_ids'=jsonb_build_array($5::text)
            AND ($7::jsonb IS NULL OR dataset.publication_metadata_summary_json->'network_bindings'=$7::jsonb)
            AND relation.oid=$6::oid
            AND NOT EXISTS (SELECT 1 FROM {namespace}.provider_directory_dataset_resource WHERE false))""",
        source_pin.read_schema_name,
        source_pin.dataset_id,
        source_pin.endpoint_id,
        source_pin.dataset_sha256,
        source_pin.source_id,
        source_pin.read_resource_table_oid,
        json.dumps(descriptor) if descriptor is not None else None,
    )
    if not matches:
        raise FHIRMembershipSourceError("Pinned FHIR source is unavailable")


async def _require_source_recipe(
    connection, source_pin, descriptor, approved_source, coordinates, registry_schema, retained_read=None
):
    """Recheck admission and the complete approved pin before extracting rows."""
    await _require_pinned_source(connection, source_pin, descriptor, retained_read)
    if descriptor is None:
        return ()
    try:
        await require_approved_network_source_bindings(
            connection, approved_source, coordinates, control_schema=registry_schema
        )
    except ApprovedNetworkSourceBindingError as error:
        raise FHIRMembershipSourceError("Reviewed FHIR bindings are unavailable") from error
    return (approved_source.approved_revision, *coordinates.sql_parameters)


_EFFECTIVE_FHIR_NETWORK_BINDINGS_SQL = """fhir_product_plans AS MATERIALIZED (
  SELECT binding.record_key,binding.network_id,binding.record_json,
    binding.record_json->'source_scope_json'->>'insurance_plan_id' AS insurance_plan_id,
    plan.payload_hash AS plan_payload_sha256,plan.payload_json::jsonb AS plan_payload_json,
    plan.resource_id IS NULL OR plan.payload_hash !~ '^[0-9a-f]{{64}}$'
      OR jsonb_typeof(plan.payload_json::jsonb) IS DISTINCT FROM 'object'
      OR jsonb_typeof(plan.payload_json::jsonb->'network_refs') IS DISTINCT FROM 'array'
      OR plan.payload_json::jsonb->>'resource_id' IS DISTINCT FROM plan.resource_id
      OR binding.record_json->>'source_key' IS DISTINCT FROM
        'InsurancePlan/'||(binding.record_json->'source_scope_json'->>'insurance_plan_id')
      OR binding.record_json->'source_scope_json'->>'capture_sha256' IS DISTINCT FROM {capture}::text
      OR binding.record_json->'source_scope_json'->>'product_network_id' IS DISTINCT FROM binding.network_id::text
      AS invalid
  FROM approved_network_bindings binding
  LEFT JOIN {source}.provider_directory_dataset_resource plan
    ON plan.dataset_id={dataset} AND plan.resource_type='InsurancePlan'
      AND plan.resource_id=binding.record_json->'source_scope_json'->>'insurance_plan_id'
  WHERE binding.record_json->'source_scope_json' ? 'insurance_plan_id'
), fhir_product_references AS MATERIALIZED (
  SELECT DISTINCT plans.*,refs.value AS network_reference,
    CASE WHEN jsonb_typeof(refs.value)='string' AND refs.value #>> '{{}}' ~ '^Organization/[A-Za-z0-9.-]{{1,64}}$'
      THEN split_part(refs.value #>> '{{}}','/',2) END AS network_resource_id
  FROM fhir_product_plans plans LEFT JOIN LATERAL jsonb_array_elements(
    CASE WHEN jsonb_typeof(plan_payload_json->'network_refs')='array'
      THEN plan_payload_json->'network_refs' ELSE '[]'::jsonb END) refs(value) ON true
), fhir_product_selection_guard AS MATERIALIZED (
  SELECT EXISTS(SELECT FROM fhir_product_references refs
    LEFT JOIN {source}.provider_directory_insurance_network_plan_evidence evidence
      ON evidence.source_id={source_id} AND evidence.release_id={release}
        AND evidence.insurance_plan_resource_id=refs.insurance_plan_id
        AND evidence.network_resource_type='Organization' AND evidence.network_resource_id=refs.network_resource_id
        AND evidence.plan_payload_sha256=refs.plan_payload_sha256
        AND evidence.plan_payload_json=refs.plan_payload_json
        AND evidence.network_refs=refs.plan_payload_json->'network_refs'
    LEFT JOIN {source}.provider_directory_dataset_resource network
      ON network.dataset_id={dataset} AND network.resource_type='Organization'
        AND network.resource_id=refs.network_resource_id
    LEFT JOIN {source}.provider_directory_entity_release_evidence witness
      ON witness.source_id={source_id} AND witness.release_id={release} AND witness.resource_type='Organization'
        AND witness.resource_id=network.resource_id AND witness.payload_sha256=network.payload_hash
        AND witness.payload_json=network.payload_json::jsonb
    WHERE refs.invalid OR (refs.network_reference IS NOT NULL AND (refs.network_resource_id IS NULL
      OR evidence.insurance_plan_resource_id IS NULL OR network.resource_id IS NULL OR witness.resource_id IS NULL)))
    OR EXISTS(SELECT FROM fhir_product_plans plans
      JOIN {source}.provider_directory_insurance_network_plan_evidence evidence
        ON evidence.source_id={source_id} AND evidence.release_id={release}
          AND evidence.insurance_plan_resource_id=plans.insurance_plan_id
      WHERE evidence.network_resource_type<>'Organization'
        OR evidence.plan_payload_sha256 IS DISTINCT FROM plans.plan_payload_sha256
        OR evidence.plan_payload_json IS DISTINCT FROM plans.plan_payload_json
        OR evidence.network_refs IS DISTINCT FROM plans.plan_payload_json->'network_refs'
        OR NOT EXISTS(SELECT FROM fhir_product_references refs WHERE refs.record_key=plans.record_key
          AND refs.network_resource_id=evidence.network_resource_id)) AS invalid
), effective_fhir_network_bindings AS MATERIALIZED (
  SELECT record_key,network_id,record_json,record_json->>'source_key' AS network_resource_id,
    NULL::text AS insurance_plan_id,NULL::text AS plan_payload_sha256,NULL::jsonb AS plan_payload_json
  FROM approved_network_bindings
  WHERE record_json->'source_scope_json'=jsonb_build_object('organization_id',record_json->>'source_key',
    'capture_sha256',{capture}::text)
  UNION
  SELECT record_key,network_id,record_json,network_resource_id,insurance_plan_id,plan_payload_sha256,plan_payload_json
  FROM fhir_product_references WHERE network_resource_id IS NOT NULL
)"""


def effective_fhir_network_bindings_sql(
    source_namespace,
    *,
    dataset_parameter="$8",
    source_parameter="$12",
    release_parameter="$13",
    capture_parameter="$17",
):
    """Expand exact retained plan selectors before resolving witnessed Organization facts."""
    parameters = (dataset_parameter, source_parameter, release_parameter, capture_parameter)
    if any(type(parameter) is not str or re.fullmatch(r"\$[1-9][0-9]*", parameter) is None for parameter in parameters):
        raise FHIRMembershipSourceError("FHIR mapping SQL parameters are invalid")
    return _EFFECTIVE_FHIR_NETWORK_BINDINGS_SQL.format(
        source=_identifier(source_namespace),
        dataset=dataset_parameter,
        source_id=source_parameter,
        release=release_parameter,
        capture=capture_parameter,
    )


_BATCH_SQL = """WITH {approved_cte}{effective_bindings_cte}page AS MATERIALIZED (
        SELECT dataset_id,resource_type,resource_id,payload_hash,acquired_resource_sha256,payload_json::jsonb AS payload
        FROM {source_namespace}.provider_directory_dataset_resource
        WHERE dataset_id=$1 AND resource_type IN ('PractitionerRole','OrganizationAffiliation')
          AND (resource_type,resource_id)>($2::text,$3::text)
        ORDER BY resource_type,resource_id LIMIT $4
      ), plan_networks AS MATERIALIZED (
        SELECT refs.reference AS insurance_plan_reference,evidence.network_resource_id
        FROM (
          SELECT DISTINCT ON (reference COLLATE "C") reference FROM page CROSS JOIN LATERAL jsonb_array_elements_text(
            CASE WHEN jsonb_typeof(payload->'insurance_plan_refs')='array'
              THEN payload->'insurance_plan_refs' ELSE '[]'::jsonb END) refs(reference)
        ) refs
          LEFT JOIN {source_namespace}.provider_directory_dataset_resource plan
            ON plan.dataset_id=$1 AND plan.resource_type='InsurancePlan'
              AND reference ~ '^InsurancePlan/[A-Za-z0-9.-]{{1,64}}$'
              AND plan.resource_id=split_part(reference,'/',2)
          {plan_witness_join}
          LEFT JOIN LATERAL jsonb_array_elements_text(
            CASE WHEN jsonb_typeof(plan.payload_json::jsonb->'network_refs')='array'
              THEN plan.payload_json::jsonb->'network_refs' ELSE '[]'::jsonb END
          ) plan_network(reference) ON true
          LEFT JOIN {source_namespace}.provider_directory_insurance_network_plan_evidence evidence
            ON evidence.source_id=$5 AND evidence.release_id=$6
              AND evidence.insurance_plan_resource_id=plan.resource_id
              AND plan_network.reference ~ '^Organization/[A-Za-z0-9.-]{{1,64}}$'
              AND evidence.network_resource_id=split_part(plan_network.reference,'/',2)
              AND evidence.plan_payload_sha256={plan_payload_sha256}
      ), providers AS (
        SELECT page.*, {provider_system} AS provider_system,{provider_id} AS provider_id,
          provider.resource_type AS provider_resource_type,provider.resource_id AS provider_resource_id,
          {provider_payload_sha256} AS provider_payload_sha256,
          CASE WHEN payload->'active'='false'::jsonb THEN 'inactive'
            WHEN pg_input_is_valid(left(payload->>'period_end',10),'date')
              AND left(payload->>'period_end',10)<$8 THEN 'expired'
            WHEN pg_input_is_valid(left(payload->>'period_start',10),'date')
              AND left(payload->>'period_start',10)>$8 THEN 'future' END AS omission_reason,
          page.resource_id !~ '^[A-Za-z0-9.-]{{1,64}}$' OR page.payload_hash !~ '^[0-9a-f]{{64}}$'
          OR (payload->'active' IS NOT NULL AND payload->'active'<>'null'::jsonb
            AND jsonb_typeof(payload->'active')<>'boolean')
          OR coalesce((pg_input_is_valid(left(payload->>'period_start',10),'date')
            AND pg_input_is_valid(left(payload->>'period_end',10),'date')
            AND left(payload->>'period_start',10)>left(payload->>'period_end',10)),false)
          OR EXISTS(SELECT 1 FROM jsonb_array_elements(CASE
            WHEN jsonb_typeof(payload->'location_refs')='array' THEN payload->'location_refs' ELSE '[]'::jsonb END) item
            WHERE jsonb_typeof(item)<>'string')
          OR EXISTS(SELECT 1 FROM jsonb_array_elements(CASE
            WHEN jsonb_typeof(payload->'network_refs')='array' THEN payload->'network_refs' ELSE '[]'::jsonb END) item
            WHERE jsonb_typeof(item)<>'string')
          OR EXISTS(SELECT 1 FROM jsonb_array_elements(CASE
            WHEN jsonb_typeof(payload->'insurance_plan_refs')='array' THEN payload->'insurance_plan_refs' ELSE '[]'::jsonb END) item
            WHERE jsonb_typeof(item)<>'string')
          OR page.resource_id IS DISTINCT FROM page.payload->>'resource_id'
          OR (page.payload->>'source_id' IS NOT NULL AND page.payload->>'source_id'<>$5)
          OR (page.payload->>'npi' IS NOT NULL AND provider.payload_json::jsonb->>'npi' IS NOT NULL
            AND page.payload->>'npi'<>provider.payload_json::jsonb->>'npi')
          OR (page.payload->>'period_start' IS NOT NULL AND NOT pg_input_is_valid(left(page.payload->>'period_start',10),'date'))
          OR (page.payload->>'period_end' IS NOT NULL AND NOT pg_input_is_valid(left(page.payload->>'period_end',10),'date'))
          OR jsonb_typeof(coalesce(page.payload->'location_refs','[]'::jsonb))<>'array'
          OR jsonb_typeof(coalesce(page.payload->'network_refs','[]'::jsonb))<>'array'
          OR jsonb_typeof(coalesce(page.payload->'insurance_plan_refs','[]'::jsonb))<>'array' AS malformed
        FROM page LEFT JOIN {source_namespace}.provider_directory_dataset_resource provider
          ON provider.dataset_id=$1
          AND provider.resource_type=CASE page.resource_type WHEN 'PractitionerRole' THEN 'Practitioner' ELSE 'Organization' END
          AND CASE WHEN page.resource_type='PractitionerRole'
            AND page.payload->>'practitioner_ref' ~ '^Practitioner/[A-Za-z0-9.-]{{1,64}}$'
            THEN split_part(page.payload->>'practitioner_ref','/',2)
            WHEN page.resource_type='OrganizationAffiliation'
            AND page.payload->>'participating_organization_ref' ~ '^Organization/[A-Za-z0-9.-]{{1,64}}$'
            THEN split_part(page.payload->>'participating_organization_ref','/',2) END=provider.resource_id
        {page_witness_join}
        {provider_witness_join}
      ), expanded AS MATERIALIZED (
        SELECT providers.*,locations.reference AS location_reference,networks.network_resource_id
        FROM providers LEFT JOIN LATERAL (
          SELECT reference FROM jsonb_array_elements_text(CASE WHEN jsonb_typeof(payload->'location_refs')='array'
            THEN payload->'location_refs' ELSE '[]'::jsonb END) refs(reference)
        ) locations ON true
        LEFT JOIN LATERAL (
          SELECT CASE WHEN reference ~ '^Organization/[A-Za-z0-9.-]{{1,64}}$'
            THEN split_part(reference,'/',2) END AS network_resource_id
          FROM jsonb_array_elements_text(CASE WHEN jsonb_typeof(payload->'network_refs')='array'
            THEN payload->'network_refs' ELSE '[]'::jsonb END) refs(reference)
          UNION
          SELECT plan_networks.network_resource_id FROM jsonb_array_elements_text(
            CASE WHEN jsonb_typeof(payload->'insurance_plan_refs')='array'
              THEN payload->'insurance_plan_refs' ELSE '[]'::jsonb END) refs(reference)
          LEFT JOIN plan_networks ON plan_networks.insurance_plan_reference COLLATE "C"=refs.reference COLLATE "C"
        ) networks ON true
        WHERE omission_reason IS NULL OR malformed
        LIMIT 5001
      ), selected_sites AS (
        SELECT DISTINCT binding.site_id FROM expanded
        JOIN {source_namespace}.provider_directory_entity_source_binding binding
          ON binding.source_id=$5 AND binding.resource_type='Location'
            AND binding.resource_id=CASE WHEN expanded.location_reference ~ '^Location/[A-Za-z0-9.-]{{1,64}}$'
              THEN split_part(expanded.location_reference,'/',2) END
      ), ambiguous_sites AS (
        SELECT binding.site_id FROM selected_sites
        JOIN {source_namespace}.provider_directory_entity_source_binding binding USING (site_id)
        JOIN {source_namespace}.provider_directory_dataset_resource location ON location.dataset_id=$1
          AND location.resource_type='Location' AND location.resource_id=binding.resource_id
        WHERE binding.source_id=$5 AND binding.resource_type='Location'
        GROUP BY binding.site_id HAVING count(*)>1
      ), resolved AS MATERIALIZED (
        SELECT expanded.*,binding.site_id,{network_id} AS network_id,
          {provider_only} AS provider_only,network.resource_id AS fact_source_key,
          {fact_source_scope} AS fact_source_scope,{evidence_basis} AS fact_evidence_basis,
          {permanent_network_omission} AS permanent_network_omission,
          {omission_columns}
          malformed OR provider_id IS NULL OR (provider_system='npi' AND provider_id !~ '^[12][0-9]{{9}}$')
            OR (binding.site_id IS NULL AND NOT ({provider_only}))
            OR {network_unresolved} OR ambiguous_sites.site_id IS NOT NULL AS unresolved,
          CASE WHEN malformed THEN 'malformed_resource'
            WHEN provider_id IS NULL OR (provider_system='npi' AND provider_id !~ '^[12][0-9]{{9}}$')
              THEN 'provider_unresolved'
            WHEN (binding.site_id IS NULL AND NOT ({provider_only}))
              OR ambiguous_sites.site_id IS NOT NULL THEN 'location_unresolved'
            WHEN {network_unresolved} THEN 'network_unresolved' END AS unresolved_reason
        FROM expanded
        LEFT JOIN {source_namespace}.provider_directory_dataset_resource location
          ON location.dataset_id=$1 AND location.resource_type='Location'
            AND location.resource_id=CASE WHEN expanded.location_reference ~ '^Location/[A-Za-z0-9.-]{{1,64}}$'
              THEN split_part(expanded.location_reference,'/',2) END
        {location_witness_join}
        LEFT JOIN {source_namespace}.provider_directory_entity_release_evidence location_evidence
          ON location_evidence.source_id=$5 AND location_evidence.release_id=$6
            AND location_evidence.resource_type='Location' AND location_evidence.resource_id=location.resource_id
            AND location_evidence.payload_sha256={location_payload_sha256}
        LEFT JOIN {source_namespace}.provider_directory_entity_source_binding binding
          ON binding.source_id=$5 AND binding.resource_type='Location' AND binding.resource_id=location_evidence.resource_id
        LEFT JOIN ambiguous_sites ON ambiguous_sites.site_id=binding.site_id
        LEFT JOIN {source_namespace}.provider_directory_dataset_resource network
          ON network.dataset_id=$1 AND network.resource_type='Organization' AND network.resource_id=expanded.network_resource_id
        {network_witness_join}
        LEFT JOIN {source_namespace}.provider_directory_entity_release_evidence network_evidence
          ON network_evidence.source_id=$5 AND network_evidence.release_id=$6
            AND network_evidence.resource_type='Organization' AND network_evidence.resource_id=network.resource_id
            AND network_evidence.payload_sha256={network_payload_sha256}
        {legacy_joins}
        {reviewed_join}
        {resolved_limit}
      ), resource_edges AS (
        SELECT resource_type,resource_id,count(*) AS edge_count,bool_or(unresolved) AS unresolved,
          bool_or(NOT unresolved AND NOT omitted) AS accepted,
          min(unresolved_reason COLLATE "C") AS unresolved_reason
        FROM resolved GROUP BY resource_type,resource_id
      ), resource_status AS (
        SELECT providers.resource_type,providers.resource_id,providers.payload_hash,
          CASE WHEN malformed OR coalesce(edges.unresolved,false)
              OR (omission_reason IS NULL AND edge_count IS NULL) THEN 'unresolved'
            WHEN omission_reason IS NOT NULL THEN 'omitted'
            WHEN edges.accepted THEN 'accepted' ELSE 'omitted' END AS outcome,
          CASE WHEN malformed THEN 'malformed_resource'
            WHEN edges.unresolved THEN edges.unresolved_reason
            WHEN omission_reason IS NOT NULL THEN omission_reason
            WHEN edge_count IS NULL THEN 'location_unresolved'
            WHEN NOT edges.accepted THEN 'unmapped_network' END AS reason
        FROM providers LEFT JOIN resource_edges edges USING(resource_type,resource_id)
      ), resource_documents AS (
        SELECT outcome,jsonb_build_object('generation_id',$9::text,'resource_type',resource_type,
          'resource_id',resource_id,'payload_sha256',payload_hash,'outcome',outcome,'reason',reason) AS document
        FROM resource_status
      ), resource_accounting AS (
        SELECT count(*) FILTER(WHERE outcome='accepted') AS accepted_source_rows,
          count(*) FILTER(WHERE outcome='unresolved') AS unresolved_source_rows,
          count(*) FILTER(WHERE outcome='omitted') AS omitted_source_rows,
          coalesce(jsonb_agg(document ORDER BY document->>'resource_type' COLLATE "C",
            document->>'resource_id' COLLATE "C"),'[]'::jsonb)::text AS source_outcomes
        FROM resource_documents
      ), encoded AS (
        SELECT {encoded_distinct}jsonb_build_object('network_id',network_id,'provider_system',provider_system,'provider_id',provider_id,
          'location_id',site_id,'evidence_id',encode(sha256(convert_to(
            ({provider_evidence})::text,'UTF8')),'hex')) AS membership
        FROM resolved WHERE NOT unresolved AND NOT provider_only AND omission_reason IS NULL {encoded_filter}
      ), source_facts AS (
        SELECT jsonb_build_object('resource_type',resource_type,'resource_id',resource_id,
          'payload_sha256',payload_hash,'omission_reason',NULL,'source_key',fact_source_key,
          'source_scope_json',fact_source_scope,'provider_system',provider_system,'provider_id',provider_id,
          'location_id',site_id,'evidence_basis',fact_evidence_basis) AS fact
        FROM resolved WHERE NOT unresolved AND omission_reason IS NULL AND NOT permanent_network_omission
        {permanent_omission_facts}
        UNION ALL SELECT jsonb_build_object('resource_type',resource_type,'resource_id',resource_id,
          'payload_sha256',payload_hash,'omission_reason',omission_reason,'source_key',NULL,
          'source_scope_json',NULL,'provider_system',NULL,'provider_id',NULL,'location_id',NULL,'evidence_basis',NULL)
        FROM providers WHERE omission_reason IS NOT NULL AND NOT malformed
      ), fact_transfer AS (
        SELECT CASE WHEN {capture_facts} THEN
          coalesce(jsonb_agg(fact ORDER BY fact->>'resource_type',fact->>'resource_id',fact::text),'[]'::jsonb)::text
          ELSE '[]' END AS facts
        FROM source_facts
      ), transfer AS (
        SELECT (SELECT count(*) FROM resolved)<=5000
          AND (NOT {capture_facts} OR (SELECT count(*) FROM source_facts)<=5000)
          AND (SELECT coalesce(sum(octet_length(membership::text)),0)+2*count(*)+2 FROM encoded)
            +octet_length(source_outcomes)+(SELECT octet_length(facts) FROM fact_transfer)<=8388608 AS bounded FROM resource_accounting
      )
      SELECT {product_selection_guard}CASE WHEN transfer.bounded THEN (SELECT facts FROM fact_transfer) END AS source_facts,
        (SELECT count(DISTINCT (resource_type,resource_id)) FROM resolved WHERE NOT unresolved AND NOT omitted AND provider_only
          AND omission_reason IS NULL) AS provider_only_rows,
        resource_accounting.accepted_source_rows,resource_accounting.unresolved_source_rows,
        resource_accounting.omitted_source_rows,CASE WHEN transfer.bounded THEN source_outcomes END AS source_outcomes,
        (SELECT count(*) FROM page) AS source_rows,
        (SELECT resource_type FROM page ORDER BY resource_type DESC,resource_id DESC LIMIT 1) AS last_type,
        (SELECT resource_id FROM page ORDER BY resource_type DESC,resource_id DESC LIMIT 1) AS last_id,
        (SELECT count(*) FROM resolved) AS expanded_rows,
        (SELECT count(*) FROM resolved WHERE unresolved {unresolved_filter}) AS unresolved_rows,
        {omitted_summary}
        (SELECT count(*) FROM encoded) AS membership_rows,
        CASE WHEN transfer.bounded
          THEN (SELECT coalesce(jsonb_agg(membership ORDER BY membership::text),'[]'::jsonb)::text FROM encoded)
          END AS input_json FROM resource_accounting CROSS JOIN transfer"""


def _reviewed_network_join(is_retained, reviewed, approved_only):
    """Keep approved eligibility separate from optional Organization witnesses."""
    if is_retained:
        network_key = "expanded.network_resource_id" if approved_only else "network.resource_id"
        return (
            "LEFT JOIN effective_fhir_network_bindings approved "
            f"ON approved.network_resource_id={network_key} AND $14::text IS NOT NULL"
        )
    if reviewed:
        return (
            "LEFT JOIN approved_network_bindings approved ON approved.record_json->>'source_key'=network.resource_id "
            "AND approved.record_json->'source_scope_json'=jsonb_build_object('organization_id',network.resource_id,"
            "'legacy_uuid',legacy.network_id::text,'alias_scope',$14::text)"
        )
    return ""


def _fhir_fact_fields(is_retained, reviewed, approved_only, capture_facts):
    """Keep permanent generic omissions separate from remappable witnessed facts."""
    return dict(
        capture_facts="true" if capture_facts else "false",
        permanent_network_omission=(
            "approved.record_key IS NULL AND (network.resource_id IS NULL OR network_evidence.resource_id IS NULL)"
        )
        if is_retained and approved_only
        else "false",
        permanent_omission_facts=(
            "UNION ALL SELECT DISTINCT jsonb_build_object('resource_type',resource_type,'resource_id',resource_id,"
            "'payload_sha256',payload_hash,'omission_reason','" + GENERIC_UNVERIFIED_NETWORK + "','source_key',NULL,"
            "'source_scope_json',NULL,'provider_system',NULL,'provider_id',NULL,'location_id',NULL,'evidence_basis',NULL) "
            "FROM resolved WHERE NOT unresolved AND omission_reason IS NULL AND permanent_network_omission"
        )
        if is_retained
        else "",
        fact_source_scope="jsonb_build_object('organization_id',network.resource_id,'capture_sha256',$17::text)"
        if is_retained
        else f"jsonb_build_object('organization_id',network.resource_id,'legacy_uuid',legacy.network_id::text,'alias_scope',${(14 if reviewed else 7)}::text)",
    )


def _fhir_legacy_joins(source_pin, registry_namespace, reviewed, is_retained):
    source_parameter, alias_parameter = (12, 14) if reviewed else (5, 7)
    return (
        ""
        if is_retained
        else (
            f"LEFT JOIN {_identifier(source_pin.read_schema_name)}.provider_directory_insurance_network_source_binding legacy "
            f"ON legacy.source_id=${source_parameter} AND legacy.resource_id=network_evidence.resource_id "
            f"LEFT JOIN {registry_namespace}.network_registry_alias alias "
            f"ON alias.source_system='fhir' AND alias.source_id=${source_parameter} AND alias.alias_type='legacy_fhir_uuid' "
            f"AND alias.alias_value=legacy.network_id::text AND alias.scope_key=${alias_parameter} "
            f"LEFT JOIN {registry_namespace}.network_registry_identity identity ON identity.network_id=alias.network_id"
        )
    )


def _extraction_sql(source_pin, registry_namespace, reviewed, approved_only=False, capture_facts=False):
    """Embed the approved CTE while shifting only extraction bind parameters."""
    sql = re.sub(r"\$(\d+)", lambda match: "$" + str(int(match[1]) + 7), _BATCH_SQL) if reviewed else _BATCH_SQL
    approved_cte = APPROVED_NETWORK_BINDINGS_SQL.format(namespace=registry_namespace) + "," if reviewed else ""
    complete_key = (
        "network.resource_id IS NOT NULL AND network_evidence.resource_id IS NOT NULL "
        "AND legacy.network_id IS NOT NULL AND legacy.resource_type='Organization'"
    )
    is_retained = fhir_normalized_capture(fhir_physical_source(source_pin)) is not None
    if is_retained:
        complete_key = "network.resource_id IS NOT NULL AND network_evidence.resource_id IS NOT NULL"
    witness_fields = _source_witness_fields(source_pin, reviewed)
    provider_fields = _provider_identity_fields(source_pin, reviewed)
    return sql.format(
        **witness_fields,
        **provider_fields,
        provider_only=(
            "expanded.location_reference IS NULL AND "
            "jsonb_array_length(coalesce(expanded.payload->'location_refs','[]'::jsonb))=0"
        )
        if approved_only
        else "false",
        evidence_basis=re.sub(
            r"\b(resource_type|resource_id|payload_hash|provider_system|provider_id|provider_resource_type|provider_resource_id|provider_payload_sha256)\b",
            r"expanded.\1",
            provider_fields["provider_evidence"].replace("network_id", "NULL::integer"),
        ).replace("site_id", "binding.site_id"),
        **_fhir_fact_fields(is_retained, reviewed, approved_only, capture_facts),
        approved_cte=approved_cte,
        effective_bindings_cte=effective_fhir_network_bindings_sql(source_pin.read_schema_name) + ","
        if is_retained
        else "",
        product_selection_guard="(SELECT invalid FROM fhir_product_selection_guard) AS product_selection_invalid,"
        if is_retained
        else "",
        source_namespace=_identifier(source_pin.read_schema_name),
        registry_namespace=registry_namespace,
        network_id="approved.network_id" if reviewed else "identity.network_id",
        network_unresolved=_unresolved_network_sql(is_retained, approved_only, reviewed, complete_key),
        omission_columns="expanded.network_resource_id IS NOT NULL AND approved.record_key IS NULL AS omitted,"
        if is_retained and approved_only
        else f"approved.record_key IS NULL AND ({complete_key}) AS omitted,"
        if approved_only
        else "false AS omitted,",
        encoded_filter="AND NOT omitted" if approved_only else "",
        encoded_distinct="DISTINCT " if is_retained else "",
        resolved_limit="LIMIT 5001 /* reviewed mappings */" if is_retained else "",
        unresolved_filter="",
        omitted_summary=(
            "((SELECT count(*) FROM resolved WHERE omitted AND NOT unresolved AND omission_reason IS NULL "
            "AND NOT permanent_network_omission)+(SELECT count(DISTINCT(resource_type,resource_id,payload_hash)) "
            "FROM resolved WHERE permanent_network_omission AND NOT unresolved AND omission_reason IS NULL)) AS omitted_rows,"
        )
        if is_retained and capture_facts
        else "(SELECT count(*) FROM resolved WHERE omitted AND NOT unresolved AND omission_reason IS NULL) AS omitted_rows,"
        if approved_only
        else "",
        reviewed_join=_reviewed_network_join(is_retained, reviewed, approved_only),
        legacy_joins=_fhir_legacy_joins(source_pin, registry_namespace, reviewed, is_retained),
    )


def _unresolved_network_sql(is_retained, approved_only, reviewed, complete_key):
    return (
        f"expanded.network_resource_id IS NULL OR (approved.record_key IS NOT NULL AND NOT ({complete_key}))"
        if is_retained and approved_only
        else f"NOT ({complete_key})"
        if approved_only
        else "approved.record_key IS NULL"
        if is_retained
        else "approved.record_key IS NULL OR (alias.network_id IS NOT NULL AND alias.network_id<>approved.network_id)"
        if reviewed
        else "identity.network_id IS NULL"
    )


def _provider_identity_fields(source_pin, reviewed):
    """Preserve legacy NPI bytes; only admitted CMS witnesses qualify non-NPI IDs."""
    if source_pin.source_id == "cms-npd":
        fields = cms_provider_sql_fields(12 if reviewed else 5)
        if reviewed:
            fields["provider_evidence"] = re.sub(
                r"\$(\d+)", lambda match: "$" + str(int(match[1]) + 7), fields["provider_evidence"]
            )
        return fields
    dataset, source, release = (8, 12, 13) if reviewed else (1, 5, 6)
    return {
        "provider_system": "'npi'",
        "provider_id": "coalesce(page.payload->>'npi',provider.payload_json::jsonb->>'npi')",
        "provider_evidence": (
            f"jsonb_build_array(${dataset},${source},${release},resource_type,resource_id,payload_hash,"
            "network_id,provider_id,site_id)"
        ),
    }


def _source_witness_fields(source_pin, reviewed):
    """CMS raw hashes come from its release-bound witness, not the normalized projection."""
    witness_by_field = {}
    source_parameter, release_parameter = (12, 13) if reviewed else (5, 6)
    for resource in ("plan", "location", "network", "page", "provider"):
        witness_by_field[resource + "_witness_join"] = ""
        witness_by_field[resource + "_payload_sha256"] = resource + ".acquired_resource_sha256"
        if fhir_normalized_capture(fhir_physical_source(source_pin)) is not None:
            witness_by_field[resource + "_payload_sha256"] = resource + ".payload_hash"
        if source_pin.source_id == "cms-npd":
            witness_by_field[resource + "_witness_join"] = (
                f"LEFT JOIN {_identifier(source_pin.read_schema_name)}.provider_directory_cms_npd_resource_witness "
                f"{resource}_witness ON {resource}_witness.dataset_id={resource}.dataset_id "
                f"AND {resource}_witness.resource_type={resource}.resource_type "
                f"AND {resource}_witness.resource_id={resource}.resource_id "
                f"AND {resource}_witness.source_id=${source_parameter} "
                f"AND {resource}_witness.release_id=${release_parameter} "
                f"AND {resource}_witness.normalized_payload_hash={resource}.payload_hash "
                f"AND ({resource}.acquired_resource_sha256 IS NULL "
                f"OR {resource}.acquired_resource_sha256={resource}_witness.raw_payload_sha256)"
            )
            witness_by_field[resource + "_payload_sha256"] = resource + "_witness.raw_payload_sha256"
    return witness_by_field


def _validate_source_page(limit, after_resource):
    """Validate bounded keyset input without querying any source relation."""
    if type(limit) is not int or not 1 <= limit <= MAX_ROWS:
        raise FHIRMembershipSourceError("FHIR source page limit is invalid")
    if after_resource is not None and (
        type(after_resource) is not tuple
        or len(after_resource) != 2
        or type(after_resource[0]) is not str
        or after_resource[0] not in {"PractitionerRole", "OrganizationAffiliation"}
        or type(after_resource[1]) is not str
        or re.fullmatch(r"[A-Za-z0-9.-]{1,64}", after_resource[1]) is None
    ):
        raise FHIRMembershipSourceError("FHIR source cursor is invalid")


async def read_fhir_membership_batch(
    connection,
    source_pin,
    *,
    registry_schema,
    after_resource=None,
    limit=1000,
    approved_source=None,
    binding_coordinates=None,
    approved_only=False,
):
    """Read a bounded retained membership page and complete raw-resource accounting.

    Unresolved combinations must reject complete admission. Approved-only selection
    counts unmapped combinations separately; absent office references remain provider
    facts and never become office memberships. An empty page ends keyset traversal.
    """
    return await _read_fhir_membership_page(
        connection,
        source_pin,
        registry_schema,
        after_resource,
        limit,
        approved_source,
        binding_coordinates,
        (approved_only, False, None),
    )


async def read_retained_fhir_membership_batch(
    connection,
    source_pin,
    retained_read,
    *,
    registry_schema,
    after_resource=None,
    limit=1000,
    approved_source=None,
    binding_coordinates=None,
):
    """Replay only an approved normalized capture using its transaction-local admission."""
    if type(source_pin) is not PinnedFHIRMembershipSource or fhir_normalized_capture(source_pin) is None:
        raise FHIRMembershipSourceError("Pinned FHIR source is unavailable")
    return await _read_fhir_membership_page(
        connection,
        source_pin,
        registry_schema,
        after_resource,
        limit,
        approved_source,
        binding_coordinates,
        (True, False, retained_read),
    )


async def read_fhir_membership_fact_batch(
    connection,
    source_pin,
    *,
    registry_schema,
    after_resource=None,
    limit=1000,
    approved_source=None,
    binding_coordinates=None,
    retained_read=None,
):
    """Capture bounded unmapped facts alongside approved office membership accounting."""
    return await _read_fhir_membership_page(
        connection,
        source_pin,
        registry_schema,
        after_resource,
        limit,
        approved_source,
        binding_coordinates,
        (True, True, retained_read),
    )


async def _read_fhir_membership_page(
    connection, source_pin, registry_schema, after_resource, limit, approved_source, binding_coordinates, selection
):
    approved_only, capture_facts, retained_read = selection
    if type(source_pin) is not PinnedFHIRMembershipSource or not connection.is_in_transaction():
        raise FHIRMembershipSourceError("Pinned FHIR reads require a caller transaction")
    descriptor = _reviewed_descriptor(source_pin, approved_source, binding_coordinates, approved_only)
    if type(capture_facts) is not bool or (capture_facts and not approved_only):
        raise FHIRMembershipSourceError("FHIR fact capture requires approved-only selection")
    _validate_source_page(limit, after_resource)
    registry_namespace = _identifier(registry_schema)
    approved_parameters = await _require_source_recipe(
        connection, source_pin, descriptor, approved_source, binding_coordinates, registry_schema, retained_read
    )
    after_type, after_id = after_resource or ("", "")
    physical = fhir_physical_source(source_pin)
    batch = await connection.fetchrow(
        _extraction_sql(source_pin, registry_namespace, descriptor is not None, approved_only, capture_facts),
        *approved_parameters,
        physical.dataset_id,
        after_type,
        after_id,
        limit,
        physical.source_id,
        physical.release_id,
        physical.alias_scope,
        physical.as_of,
        _generation_id(source_pin, approved_source, binding_coordinates, approved_only),
        *(
            (fhir_normalized_capture(source_pin).capture_sha256,)
            if fhir_normalized_capture(physical) is not None
            else ()
        ),
    )
    return _fhir_batch(source_pin, batch, approved_source, binding_coordinates, approved_only)


def _fhir_batch(source_pin, batch, approved_source, binding_coordinates, approved_only):
    """Bound and validate one complete SQL page before exposing its dispositions."""
    if batch.get("product_selection_invalid", False):
        raise FHIRMembershipSourceError("Selected FHIR product plan is unavailable")
    if batch["input_json"] is None:
        raise FHIRMembershipBatchBoundsError("FHIR membership batch exceeds bounds")
    input_bytes = batch["input_json"].encode()
    if len(input_bytes) > MAX_INPUT_BYTES:
        raise FHIRMembershipBatchBoundsError("FHIR membership batch exceeds bounds")
    next_resource = (batch["last_type"], batch["last_id"]) if batch["source_rows"] else None
    outcomes = _resource_outcomes(
        batch, _generation_id(source_pin, approved_source, binding_coordinates, approved_only)
    )
    return FHIRMembershipBatch(
        source_pin,
        batch["source_rows"],
        batch["membership_rows"],
        batch["unresolved_rows"],
        next_resource,
        input_bytes,
        hashlib.sha256(input_bytes).hexdigest(),
        approved_source,
        binding_coordinates,
        approved_only,
        batch["omitted_rows"] if approved_only else 0,
        batch["accepted_source_rows"],
        batch["unresolved_source_rows"],
        batch["omitted_source_rows"],
        outcomes,
        source_fact_bytes=batch.get("source_facts", "[]").encode(),
        provider_only_rows=batch.get("provider_only_rows", 0),
    )


def _resource_outcomes(batch, generation_id):
    """Validate the closed resource census before a caller may admit any edge."""
    outcomes = tuple(FHIRResourceDisposition(**document) for document in json.loads(batch["source_outcomes"]))
    reasons_by_outcome = {
        "accepted": {None},
        "unresolved": {"malformed_resource", "provider_unresolved", "location_unresolved", "network_unresolved"},
        "omitted": {"inactive", "expired", "future", "unmapped_network"},
    }
    if (
        batch["source_rows"]
        != sum(batch[name] for name in ("accepted_source_rows", "unresolved_source_rows", "omitted_source_rows"))
        or len(outcomes) != batch["source_rows"]
        or len({(outcome.resource_type, outcome.resource_id) for outcome in outcomes}) != len(outcomes)
        or any(
            outcome.generation_id != generation_id
            or outcome.reason not in reasons_by_outcome.get(outcome.outcome, set())
            for outcome in outcomes
        )
    ):
        raise FHIRMembershipSourceError("FHIR resource accounting is incomplete")
    return outcomes


async def _require_reviewed_candidate(connection, batch, copy_target, control_schema):
    """Check the complete retained extraction identity under the candidate lock."""
    if batch.approved_source is None:
        return
    candidate = await _locked_candidate(connection, copy_target, _control_namespace(control_schema))
    generations = json.loads(candidate["source_generations"])
    if (
        candidate["approved_custom_revision"] != batch.approved_source.approved_revision
        or generations.get("custom_membership") != batch.approved_source.generation_id
        or batch.generation_id not in generations.values()
    ):
        raise FHIRMembershipSourceError("Reviewed FHIR candidate pin does not match")


async def copy_fhir_membership_batch(
    connection,
    batch,
    copy_target,
    *,
    require_candidate_authority,
    control_schema=None,
):
    """COPY resolved rows through the native encoder and trusted authority.

    The authority must bind ``batch.generation_id`` to the candidate's pinned
    source map and, for reviewed batches, its exact approved revision. Batch
    registration and whole-source accounting belong to the candidate lifecycle.
    """
    if type(batch) is not FHIRMembershipBatch or batch.unresolved_rows or batch.unresolved_source_rows:
        raise FHIRMembershipSourceError("FHIR source memberships are unresolved")
    _reviewed_descriptor(batch.source, batch.approved_source, batch.binding_coordinates, batch.approved_only)
    if not connection.is_in_transaction():
        raise FHIRMembershipSourceError("FHIR membership COPY requires a caller transaction")
    async with connection.transaction():
        await _require_reviewed_candidate(connection, batch, copy_target, control_schema)
        receipt = await copy_network_membership_batch(
            connection,
            copy_target=copy_target,
            input_bytes=batch.input_bytes,
            expected_input_sha256=batch.input_sha256,
            require_candidate_authority=require_candidate_authority,
        )
        if receipt.row_count != batch.membership_rows:
            raise FHIRMembershipSourceError("FHIR membership COPY accounting mismatch")
        await _require_reviewed_candidate(connection, batch, copy_target, control_schema)
        return receipt


# Explicit v2 witnesses never change the legacy fact transform or its SQL bytes.
FHIR_SOURCE_WITNESS_TRANSFORM_V2 = "fhir-source-witnesses-v2"


_WITNESS_SQL_V2 = """WITH page AS MATERIALIZED (
        SELECT dataset_id,resource_type,resource_id,payload_hash,acquired_resource_sha256,payload_json::jsonb AS payload,$7::text AS alias_scope
        FROM {source_namespace}.provider_directory_dataset_resource
        WHERE dataset_id=$1 AND resource_type IN ('PractitionerRole','OrganizationAffiliation')
          AND (resource_type COLLATE "C",resource_id COLLATE "C")>($2::text COLLATE "C",$3::text COLLATE "C")
        ORDER BY resource_type COLLATE "C",resource_id COLLATE "C" LIMIT $4
      ), plan_networks AS MATERIALIZED (
        SELECT refs.reference AS insurance_plan_reference,evidence.network_resource_id
        FROM (
          SELECT DISTINCT ON (reference COLLATE "C") reference FROM page CROSS JOIN LATERAL jsonb_array_elements_text(
            CASE WHEN jsonb_typeof(payload->'insurance_plan_refs')='array'
              THEN payload->'insurance_plan_refs' ELSE '[]'::jsonb END) refs(reference)
        ) refs
          LEFT JOIN {source_namespace}.provider_directory_dataset_resource plan
            ON plan.dataset_id=$1 AND plan.resource_type='InsurancePlan'
              AND reference ~ '^InsurancePlan/[A-Za-z0-9.-]{{1,64}}$'
              AND plan.resource_id=split_part(reference,'/',2)
          {plan_witness_join}
          LEFT JOIN LATERAL jsonb_array_elements_text(
            CASE WHEN jsonb_typeof(plan.payload_json::jsonb->'network_refs')='array'
              THEN plan.payload_json::jsonb->'network_refs' ELSE '[]'::jsonb END
          ) plan_network(reference) ON true
          LEFT JOIN {source_namespace}.provider_directory_insurance_network_plan_evidence evidence
            ON evidence.source_id=$5 AND evidence.release_id=$6
              AND evidence.insurance_plan_resource_id=plan.resource_id
              AND plan_network.reference ~ '^Organization/[A-Za-z0-9.-]{{1,64}}$'
              AND evidence.network_resource_id=split_part(plan_network.reference,'/',2)
              AND evidence.plan_payload_sha256={plan_payload_sha256}
      ), providers AS (
        SELECT page.*, {provider_system} AS provider_system,{provider_id} AS provider_id,
          provider.resource_type AS provider_resource_type,provider.resource_id AS provider_resource_id,
          {provider_payload_sha256} AS provider_payload_sha256,
          CASE WHEN payload->'active'='false'::jsonb THEN 'inactive'
            WHEN pg_input_is_valid(left(payload->>'period_end',10),'date')
              AND left(payload->>'period_end',10)<$8 THEN 'expired'
            WHEN pg_input_is_valid(left(payload->>'period_start',10),'date')
              AND left(payload->>'period_start',10)>$8 THEN 'future' END AS omission_reason,
          page.resource_id !~ '^[A-Za-z0-9.-]{{1,64}}$' OR page.payload_hash !~ '^[0-9a-f]{{64}}$'
          OR (payload->'active' IS NOT NULL AND payload->'active'<>'null'::jsonb
            AND jsonb_typeof(payload->'active')<>'boolean')
          OR coalesce((pg_input_is_valid(left(payload->>'period_start',10),'date')
            AND pg_input_is_valid(left(payload->>'period_end',10),'date')
            AND left(payload->>'period_start',10)>left(payload->>'period_end',10)),false)
          OR EXISTS(SELECT 1 FROM jsonb_array_elements(CASE
            WHEN jsonb_typeof(payload->'location_refs')='array' THEN payload->'location_refs' ELSE '[]'::jsonb END) item
            WHERE jsonb_typeof(item)<>'string')
          OR EXISTS(SELECT 1 FROM jsonb_array_elements(CASE
            WHEN jsonb_typeof(payload->'network_refs')='array' THEN payload->'network_refs' ELSE '[]'::jsonb END) item
            WHERE jsonb_typeof(item)<>'string')
          OR EXISTS(SELECT 1 FROM jsonb_array_elements(CASE
            WHEN jsonb_typeof(payload->'insurance_plan_refs')='array' THEN payload->'insurance_plan_refs' ELSE '[]'::jsonb END) item
            WHERE jsonb_typeof(item)<>'string')
          OR page.resource_id IS DISTINCT FROM page.payload->>'resource_id'
          OR (page.payload->>'source_id' IS NOT NULL AND page.payload->>'source_id'<>$5)
          OR (page.payload->>'npi' IS NOT NULL AND provider.payload_json::jsonb->>'npi' IS NOT NULL
            AND page.payload->>'npi'<>provider.payload_json::jsonb->>'npi')
          OR (page.payload->>'period_start' IS NOT NULL AND NOT pg_input_is_valid(left(page.payload->>'period_start',10),'date'))
          OR (page.payload->>'period_end' IS NOT NULL AND NOT pg_input_is_valid(left(page.payload->>'period_end',10),'date'))
          OR jsonb_typeof(coalesce(page.payload->'location_refs','[]'::jsonb))<>'array'
          OR jsonb_typeof(coalesce(page.payload->'network_refs','[]'::jsonb))<>'array'
          OR jsonb_typeof(coalesce(page.payload->'insurance_plan_refs','[]'::jsonb))<>'array' AS malformed
        FROM page LEFT JOIN {source_namespace}.provider_directory_dataset_resource provider
          ON provider.dataset_id=$1
          AND provider.resource_type=CASE page.resource_type WHEN 'PractitionerRole' THEN 'Practitioner' ELSE 'Organization' END
          AND CASE WHEN page.resource_type='PractitionerRole'
            AND page.payload->>'practitioner_ref' ~ '^Practitioner/[A-Za-z0-9.-]{{1,64}}$'
            THEN split_part(page.payload->>'practitioner_ref','/',2)
            WHEN page.resource_type='OrganizationAffiliation'
            AND page.payload->>'participating_organization_ref' ~ '^Organization/[A-Za-z0-9.-]{{1,64}}$'
            THEN split_part(page.payload->>'participating_organization_ref','/',2) END=provider.resource_id
        {page_witness_join}
        {provider_witness_join}
      ), expanded AS MATERIALIZED (
        SELECT providers.*,locations.reference AS location_reference,networks.network_resource_id
        FROM providers LEFT JOIN LATERAL (
          SELECT reference FROM jsonb_array_elements_text(CASE WHEN jsonb_typeof(payload->'location_refs')='array'
            THEN payload->'location_refs' ELSE '[]'::jsonb END) refs(reference)
        ) locations ON true
        LEFT JOIN LATERAL (
          SELECT CASE WHEN reference ~ '^Organization/[A-Za-z0-9.-]{{1,64}}$'
            THEN split_part(reference,'/',2) END AS network_resource_id
          FROM jsonb_array_elements_text(CASE WHEN jsonb_typeof(payload->'network_refs')='array'
            THEN payload->'network_refs' ELSE '[]'::jsonb END) refs(reference)
          UNION
          SELECT plan_networks.network_resource_id FROM jsonb_array_elements_text(
            CASE WHEN jsonb_typeof(payload->'insurance_plan_refs')='array'
              THEN payload->'insurance_plan_refs' ELSE '[]'::jsonb END) refs(reference)
          LEFT JOIN plan_networks ON plan_networks.insurance_plan_reference COLLATE "C"=refs.reference COLLATE "C"
        ) networks ON true
        WHERE omission_reason IS NULL OR malformed
        LIMIT 5001
      ), selected_sites AS (
        SELECT DISTINCT binding.site_id FROM expanded
        JOIN {source_namespace}.provider_directory_entity_source_binding binding
          ON binding.source_id=$5 AND binding.resource_type='Location'
            AND binding.resource_id=CASE WHEN expanded.location_reference ~ '^Location/[A-Za-z0-9.-]{{1,64}}$'
              THEN split_part(expanded.location_reference,'/',2) END
      ), ambiguous_sites AS (
        SELECT binding.site_id FROM selected_sites
        JOIN {source_namespace}.provider_directory_entity_source_binding binding USING (site_id)
        JOIN {source_namespace}.provider_directory_dataset_resource location ON location.dataset_id=$1
          AND location.resource_type='Location' AND location.resource_id=binding.resource_id
        WHERE binding.source_id=$5 AND binding.resource_type='Location'
        GROUP BY binding.site_id HAVING count(*)>1
      ), resolved AS MATERIALIZED (
        SELECT expanded.*,binding.site_id,
          {provider_only} AS provider_only,
          provider_id IS NULL OR (provider_system='npi' AND provider_id !~ '^[12][0-9]{{9}}$') AS provider_unresolved,
          (binding.site_id IS NULL AND NOT ({provider_only})) OR ambiguous_sites.site_id IS NOT NULL AS location_unresolved,
          expanded.network_resource_id AS source_key,{source_scope} AS source_scope_json,
          {complete_key} AS network_witness_complete,{evidence_basis} AS evidence_basis
        FROM expanded
        LEFT JOIN {source_namespace}.provider_directory_dataset_resource location
          ON location.dataset_id=$1 AND location.resource_type='Location'
            AND location.resource_id=CASE WHEN expanded.location_reference ~ '^Location/[A-Za-z0-9.-]{{1,64}}$'
              THEN split_part(expanded.location_reference,'/',2) END
        {location_witness_join}
        LEFT JOIN {source_namespace}.provider_directory_entity_release_evidence location_evidence
          ON location_evidence.source_id=$5 AND location_evidence.release_id=$6
            AND location_evidence.resource_type='Location' AND location_evidence.resource_id=location.resource_id
            AND location_evidence.payload_sha256={location_payload_sha256}
        LEFT JOIN {source_namespace}.provider_directory_entity_source_binding binding
          ON binding.source_id=$5 AND binding.resource_type='Location' AND binding.resource_id=location_evidence.resource_id
        LEFT JOIN ambiguous_sites ON ambiguous_sites.site_id=binding.site_id
        LEFT JOIN {source_namespace}.provider_directory_dataset_resource network
          ON network.dataset_id=$1 AND network.resource_type='Organization' AND network.resource_id=expanded.network_resource_id
        {network_witness_join}
        LEFT JOIN {source_namespace}.provider_directory_entity_release_evidence network_evidence
          ON network_evidence.source_id=$5 AND network_evidence.release_id=$6
            AND network_evidence.resource_type='Organization' AND network_evidence.resource_id=network.resource_id
            AND network_evidence.payload_sha256={network_payload_sha256}
        {native_legacy_join}


      ), documents AS (
        SELECT resource_type,resource_id,jsonb_build_object('fact_version',2,'kind','resource',
          'source_binding',$9::jsonb,'resource_type',resource_type,'resource_id',resource_id,
          'payload_sha256',payload_hash,'malformed',malformed,'omission_reason',omission_reason) AS fact
        FROM providers
        UNION ALL SELECT resource_type,resource_id,jsonb_build_object('fact_version',2,'kind','edge',
          'source_binding',$9::jsonb,'resource_type',resource_type,'resource_id',resource_id,
          'payload_sha256',payload_hash,'source_key',source_key,'source_scope_json',source_scope_json,
          'provider_system',provider_system,'provider_id',CASE WHEN provider_unresolved THEN NULL ELSE provider_id END,
          'location_id',site_id,'provider_only',provider_only,'provider_unresolved',provider_unresolved,
          'location_unresolved',location_unresolved,'network_witness_complete',network_witness_complete,
          'evidence_basis',CASE WHEN provider_unresolved THEN jsonb_set(evidence_basis,
            CASE WHEN provider_system='npi' THEN '{{7}}'::text[] ELSE '{{8}}'::text[] END,'null'::jsonb)
            ELSE evidence_basis END) FROM resolved
      ), transfer AS (
        SELECT CASE WHEN (SELECT count(*) FROM resolved)<=5000 AND count(*)<=10000
          AND coalesce(sum(octet_length(fact::text)),0)+2*count(*)+2<=8388608
          THEN coalesce(jsonb_agg(fact ORDER BY resource_type COLLATE "C",resource_id COLLATE "C",
            fact->>'kind' DESC,fact::text),'[]'::jsonb)::text END AS source_facts FROM documents
      ) SELECT source_facts,(SELECT count(*) FROM page) AS source_rows,
        (SELECT resource_type FROM page ORDER BY resource_type COLLATE "C" DESC,resource_id COLLATE "C" DESC LIMIT 1) AS last_type,
        (SELECT resource_id FROM page ORDER BY resource_type COLLATE "C" DESC,resource_id COLLATE "C" DESC LIMIT 1) AS last_id
      FROM transfer"""


@dataclass(frozen=True)
class FHIRSourceWitnessPageV2:
    """Source-native census only; independent re-extraction and custody remain mandatory."""

    source: PinnedFHIRMembershipSource
    source_rows: int
    next_resource: tuple[str, str] | None
    source_fact_bytes: bytes


def fhir_source_witness_binding_v2(source_pin):
    """Keep original admission, selected successor and logical recipe independently bound."""
    return {
        "logical_generation_id": source_pin.generation_id,
        "original_generation_id": replace(source_pin, retained_successor=None).generation_id,
        "physical_generation_id": fhir_physical_source(source_pin).generation_id,
    }


def _source_witness_sql_v2(source_pin):
    """Render native witnesses without consulting approved mappings or mutable aliases."""
    is_retained = fhir_normalized_capture(fhir_physical_source(source_pin)) is not None
    provider_fields = _provider_identity_fields(source_pin, False)
    complete_key = "network.resource_id IS NOT NULL AND network_evidence.resource_id IS NOT NULL"
    if not is_retained:
        complete_key += " AND legacy.network_id IS NOT NULL AND legacy.resource_type='Organization'"
    return _WITNESS_SQL_V2.format(
        **_source_witness_fields(source_pin, False),
        **provider_fields,
        source_namespace=_identifier(source_pin.read_schema_name),
        provider_only=(
            "expanded.location_reference IS NULL AND CASE WHEN "
            "jsonb_typeof(expanded.payload->'location_refs')='array' "
            "THEN jsonb_array_length(expanded.payload->'location_refs')=0 "
            "ELSE expanded.payload->'location_refs' IS NULL END"
        ),
        complete_key=complete_key,
        source_scope=(
            "jsonb_build_object('organization_id',expanded.network_resource_id,'capture_sha256',$10::text)"
            if is_retained
            else "jsonb_build_object('organization_id',expanded.network_resource_id,"
            "'legacy_uuid',legacy.network_id::text,'alias_scope',$7::text)"
        ),
        native_legacy_join=(
            ""
            if is_retained
            else f"LEFT JOIN {_identifier(source_pin.read_schema_name)}.provider_directory_insurance_network_source_binding "
            "legacy ON legacy.source_id=$5 AND legacy.resource_id=network_evidence.resource_id"
        ),
        evidence_basis=re.sub(
            r"\b(resource_type|resource_id|payload_hash|provider_system|provider_id|provider_resource_type|provider_resource_id|provider_payload_sha256)\b",
            r"expanded.\1",
            provider_fields["provider_evidence"].replace("network_id", "NULL::integer"),
        ).replace("site_id", "binding.site_id"),
    )


def _validate_witness_cursor(after_resource):
    if after_resource is not None and (
        type(after_resource) is not tuple
        or len(after_resource) != 2
        or type(after_resource[0]) is not str
        or after_resource[0] not in {"PractitionerRole", "OrganizationAffiliation"}
        or type(after_resource[1]) is not str
        or not 1 <= len(after_resource[1]) <= 256
        or "\x00" in after_resource[1]
    ):
        raise FHIRMembershipSourceError("FHIR source cursor is invalid")


def _witness_query_arguments(source_pin, after_resource, limit):
    physical = fhir_physical_source(source_pin)
    arguments = (
        physical.dataset_id,
        *(after_resource or ("", "")),
        limit,
        physical.source_id,
        physical.release_id,
        physical.alias_scope,
        physical.as_of,
        json.dumps(
            fhir_source_witness_binding_v2(source_pin),
            sort_keys=True,
            separators=(",", ":"),
        ),
    )
    if fhir_normalized_capture(physical) is not None:
        arguments += (fhir_normalized_capture(source_pin).capture_sha256,)
    return arguments


async def read_fhir_witness_page_v2(
    connection,
    source_pin,
    *,
    registry_schema,
    after_resource=None,
    limit=1000,
    approved_source=None,
    binding_coordinates=None,
    retained_read=None,
):
    """Extract explicit v2 native facts; selected policy and office admission are separate.

    Fresh independent custody must replay this exact extraction. Neither a returned
    page nor its source-native flags replace admission or an independently sealed anchor.
    """
    if type(source_pin) is not PinnedFHIRMembershipSource or not connection.is_in_transaction():
        raise FHIRMembershipSourceError("Pinned FHIR reads require a caller transaction")
    _validate_source_page(limit, None)
    _validate_witness_cursor(after_resource)
    descriptor = _reviewed_descriptor(source_pin, approved_source, binding_coordinates, True)
    _identifier(registry_schema)
    await _require_source_recipe(
        connection,
        source_pin,
        descriptor,
        approved_source,
        binding_coordinates,
        registry_schema,
        retained_read,
    )
    arguments = _witness_query_arguments(source_pin, after_resource, limit)
    witness_page = await connection.fetchrow(_source_witness_sql_v2(source_pin), *arguments)
    await _require_source_recipe(
        connection,
        source_pin,
        descriptor,
        approved_source,
        binding_coordinates,
        registry_schema,
        retained_read,
    )
    if witness_page["source_facts"] is None or len(witness_page["source_facts"].encode()) > MAX_INPUT_BYTES:
        raise FHIRMembershipBatchBoundsError("FHIR source witness page exceeds bounds")
    return FHIRSourceWitnessPageV2(
        source_pin,
        witness_page["source_rows"],
        (witness_page["last_type"], witness_page["last_id"]) if witness_page["source_rows"] else None,
        witness_page["source_facts"].encode(),
    )


read_fhir_source_witness_page_v2 = read_fhir_witness_page_v2
