# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact site memberships from a pinned retained FHIR projection."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass
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
from process.network_fhir_source_epoch import (
    FHIRSourceEpochError,
    RetainedCMSFHIRSourceEpoch,
    require_retained_cms_fhir_source_epoch,
    validate_retained_cms_fhir_source_epoch,
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
        return coordinates_by_name

    @property
    def read_schema_name(self):
        """Read closed retained tables while keeping original source provenance."""
        return self.retained_epoch.schema_name if self.retained_epoch is not None else self.schema_name

    @property
    def read_resource_table_oid(self):
        """Bind the exact physical resource heap selected by retained custody."""
        if self.retained_epoch is None:
            return self.resource_table_oid
        return dict(self.retained_epoch.relation_oids)["provider_directory_dataset_resource"]

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

    @property
    def generation_id(self):
        """Bind reviewed mappings without changing the legacy source generation."""
        _reviewed_descriptor(self.source, self.approved_source, self.binding_coordinates, self.approved_only)
        if self.approved_source is None and self.binding_coordinates is None:
            return self.source.generation_id
        recipe_dict = {
            "source": self.source.coordinates,
            "approved_source": asdict(self.approved_source),
            "binding_coordinates": asdict(self.binding_coordinates),
        }
        if self.approved_only:
            recipe_dict["selection_policy"] = "approved-bindings-only"
        return hashlib.sha256(json.dumps(recipe_dict, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _reviewed_descriptor(source, approved_source, coordinates, approved_only=False):
    """Require paired pins and their exact admitted FHIR coordinates."""
    if type(source) is not PinnedFHIRMembershipSource or type(approved_only) is not bool:
        raise FHIRMembershipSourceError("Pinned FHIR source is invalid")
    if approved_source is None and coordinates is None:
        if approved_only:
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
    return {**asdict(coordinates), "alias_scope": source.alias_scope, "source_key_kind": "organization_resource_id"}


async def _require_pinned_source(connection, source_pin, descriptor=None):
    if source_pin.retained_epoch is not None:
        try:
            await require_retained_cms_fhir_source_epoch(
                connection, source_pin, source_pin.retained_epoch, verify_content=False
            )
        except FHIRSourceEpochError:
            raise FHIRMembershipSourceError("Pinned FHIR source is unavailable") from None
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


async def _require_source_recipe(connection, source_pin, descriptor, approved_source, coordinates, registry_schema):
    """Recheck admission and the complete approved pin before extracting rows."""
    await _require_pinned_source(connection, source_pin, descriptor)
    if descriptor is None:
        return ()
    try:
        await require_approved_network_source_bindings(
            connection, approved_source, coordinates, control_schema=registry_schema
        )
    except ApprovedNetworkSourceBindingError:
        raise FHIRMembershipSourceError("Reviewed FHIR bindings are unavailable") from None
    return (approved_source.approved_revision, *coordinates.sql_parameters)


_BATCH_SQL = """WITH {approved_cte}page AS MATERIALIZED (
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
          page.resource_id IS DISTINCT FROM page.payload->>'resource_id'
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
        WHERE payload->'active' IS DISTINCT FROM 'false'::jsonb
          AND (payload->>'period_start' IS NULL OR NOT pg_input_is_valid(left(payload->>'period_start',10),'date')
            OR left(payload->>'period_start',10)<=$8)
          AND (payload->>'period_end' IS NULL OR NOT pg_input_is_valid(left(payload->>'period_end',10),'date')
            OR left(payload->>'period_end',10)>=$8)
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
          {omission_columns}
          malformed OR provider_id IS NULL OR (provider_system='npi' AND provider_id !~ '^[12][0-9]{{9}}$')
            OR binding.site_id IS NULL OR {network_unresolved} OR ambiguous_sites.site_id IS NOT NULL AS unresolved
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
        LEFT JOIN {source_namespace}.provider_directory_insurance_network_source_binding legacy
          ON legacy.source_id=$5 AND legacy.resource_id=network_evidence.resource_id
        LEFT JOIN {registry_namespace}.network_registry_alias alias
          ON alias.source_system='fhir' AND alias.source_id=$5 AND alias.alias_type='legacy_fhir_uuid'
            AND alias.alias_value=legacy.network_id::text AND alias.scope_key=$7
        LEFT JOIN {registry_namespace}.network_registry_identity identity ON identity.network_id=alias.network_id
        {reviewed_join}
      ), encoded AS (
        SELECT jsonb_build_object('network_id',network_id,'provider_system',provider_system,'provider_id',provider_id,
          'location_id',site_id,'evidence_id',encode(sha256(convert_to(
            ({provider_evidence})::text,'UTF8')),'hex')) AS membership
        FROM resolved WHERE NOT unresolved {encoded_filter}
      )
      SELECT (SELECT count(*) FROM page) AS source_rows,
        (SELECT resource_type FROM page ORDER BY resource_type DESC,resource_id DESC LIMIT 1) AS last_type,
        (SELECT resource_id FROM page ORDER BY resource_type DESC,resource_id DESC LIMIT 1) AS last_id,
        (SELECT count(*) FROM resolved) AS expanded_rows,
        (SELECT count(*) FROM resolved WHERE unresolved {unresolved_filter}) AS unresolved_rows,
        {omitted_summary}
        (SELECT count(*) FROM encoded) AS membership_rows,
        CASE WHEN (SELECT count(*) FROM resolved)<=5000
          AND (SELECT coalesce(sum(octet_length(membership::text)),0)+2*count(*)+2 FROM encoded)<=8388608
          THEN (SELECT coalesce(jsonb_agg(membership ORDER BY membership::text),'[]'::jsonb)::text FROM encoded)
          END AS input_json"""


def _extraction_sql(source_pin, registry_namespace, reviewed, approved_only=False):
    """Embed the approved CTE while shifting only extraction bind parameters."""
    sql = re.sub(r"\$(\d+)", lambda match: "$" + str(int(match[1]) + 7), _BATCH_SQL) if reviewed else _BATCH_SQL
    approved_cte = APPROVED_NETWORK_BINDINGS_SQL.format(namespace=registry_namespace) + "," if reviewed else ""
    complete_key = (
        "network.resource_id IS NOT NULL AND network_evidence.resource_id IS NOT NULL "
        "AND legacy.network_id IS NOT NULL AND legacy.resource_type='Organization'"
    )
    witness_fields = _source_witness_fields(source_pin, reviewed)
    provider_fields = _provider_identity_fields(source_pin, reviewed)
    return sql.format(
        **witness_fields,
        **provider_fields,
        approved_cte=approved_cte,
        source_namespace=_identifier(source_pin.read_schema_name),
        registry_namespace=registry_namespace,
        network_id="approved.network_id" if reviewed else "identity.network_id",
        network_unresolved=(
            f"NOT ({complete_key})"
            if approved_only
            else "approved.record_key IS NULL OR (alias.network_id IS NOT NULL AND alias.network_id<>approved.network_id)"
            if reviewed
            else "identity.network_id IS NULL"
        ),
        omission_columns=f"approved.record_key IS NULL AND ({complete_key}) AS omitted," if approved_only else "",
        encoded_filter="AND NOT omitted" if approved_only else "",
        unresolved_filter="AND NOT omitted" if approved_only else "",
        omitted_summary="(SELECT count(*) FROM resolved WHERE omitted) AS omitted_rows," if approved_only else "",
        reviewed_join=(
            "LEFT JOIN approved_network_bindings approved ON approved.record_json->>'source_key'=network.resource_id "
            "AND approved.record_json->'source_scope_json'=jsonb_build_object('organization_id',network.resource_id,"
            "'legacy_uuid',legacy.network_id::text,'alias_scope',$14::text)"
            if reviewed
            else ""
        ),
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
    """Read one bounded retained page in the caller transaction, without source writes.

    Unresolved combinations are counted and omitted. Callers must reject them
    before admitting a complete candidate; the COPY convenience function does.
    Approved-only selection separately counts complete unmapped combinations in
    ``omitted_rows``; their retained aliases do not override reviewed bindings.
    ``next_resource`` is an exclusive keyset cursor; an empty page ends traversal.
    """
    if type(source_pin) is not PinnedFHIRMembershipSource or not connection.is_in_transaction():
        raise FHIRMembershipSourceError("Pinned FHIR reads require a caller transaction")
    descriptor = _reviewed_descriptor(source_pin, approved_source, binding_coordinates, approved_only)
    _validate_source_page(limit, after_resource)
    registry_namespace = _identifier(registry_schema)
    approved_parameters = await _require_source_recipe(
        connection, source_pin, descriptor, approved_source, binding_coordinates, registry_schema
    )
    after_type, after_id = after_resource or ("", "")
    batch = await connection.fetchrow(
        _extraction_sql(source_pin, registry_namespace, descriptor is not None, approved_only),
        *approved_parameters,
        source_pin.dataset_id,
        after_type,
        after_id,
        limit,
        source_pin.source_id,
        source_pin.release_id,
        source_pin.alias_scope,
        source_pin.as_of,
    )
    if batch["input_json"] is None:
        raise FHIRMembershipBatchBoundsError("FHIR membership batch exceeds bounds")
    input_bytes = batch["input_json"].encode()
    if len(input_bytes) > MAX_INPUT_BYTES:
        raise FHIRMembershipBatchBoundsError("FHIR membership batch exceeds bounds")
    next_resource = (batch["last_type"], batch["last_id"]) if batch["source_rows"] else None
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
    )


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
    if type(batch) is not FHIRMembershipBatch or batch.unresolved_rows:
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
