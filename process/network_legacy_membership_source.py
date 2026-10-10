# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact ACA sites from admitted frozen reference archives.

PTG snapshots retain source hashes but their provider-location dictionaries have
no canonical site UUID or reviewed per-site network assignment. Those inputs
cannot establish a membership and are deliberately not adapted here.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass

from process.network_address_projection import _identifier
from process.network_approved_membership_source import ApprovedMembershipSource
from process.network_approved_source_bindings import (
    APPROVED_NETWORK_BINDINGS_SQL,
    ApprovedNetworkSourceBindingError,
    RegistryNetworkSourceCoordinates,
    require_approved_network_source_bindings,
)
from process.network_membership_candidate_lifecycle import _control_namespace, _locked_candidate
from process.network_membership_copy import MAX_INPUT_BYTES, MAX_ROWS, copy_network_membership_batch
from process.network_membership_writer_closure import _NATIVE_WRITER_PRIVILEGES_SQL, _protected_owner
from process.reference_family_archive import (
    ReferenceFamilyArchiveError,
    ReferenceFamilyPreparedSource,
    ReferenceFamilyValidationReceipt,
    _canonical_json,
    reference_family_stage_schema,
    validate_reference_family_manifest,
    validate_reference_family_validation_receipt,
)


class LegacyMembershipSourceError(ValueError):
    """Retained source coordinates, reviewed bindings or batch bounds differ."""


class LegacyMembershipBatchBoundsError(LegacyMembershipSourceError):
    """A source page must be split before native membership COPY."""


def aca_checksum_scope(issuer_id, year, alias_scope):
    """Include the issuer and reporting year in a reviewed checksum namespace."""
    if type(issuer_id) is not int or not 0 < issuer_id <= 2147483647:
        raise LegacyMembershipSourceError("ACA issuer identity is invalid")
    if type(year) is not int or not 1900 <= year <= 9999:
        raise LegacyMembershipSourceError("ACA reporting year is invalid")
    if (
        type(alias_scope) is not str
        or not 1 <= len(alias_scope.encode()) <= 400
        or alias_scope.strip() != alias_scope
        or not alias_scope.isprintable()
    ):
        raise LegacyMembershipSourceError("ACA alias namespace is invalid")
    return _canonical_json([issuer_id, year, alias_scope]).decode()


@dataclass(frozen=True)
class PinnedACAMembershipSource:
    """Trusted archive admission, never a live/current or inferred source lookup.

    Admission stores the explicit coordinates under manifest source metadata's
    ``network_membership`` key. Package and validation receipts are trusted
    controller inputs; digest validity alone does not confer admission authority.
    ``runtime_roles`` must include every configured source loader and reader;
    their effective native write and owner privilege paths are rechecked.
    """

    prepared: ReferenceFamilyPreparedSource
    validation: ReferenceFamilyValidationReceipt
    source_id: str
    release_id: str
    import_id: str
    issuer_id: int
    year: int
    source_url: str
    alias_scope: str
    reviewed_alias_sha256: str
    runtime_roles: tuple[str, ...]

    def __post_init__(self):
        _validated_archive(self)
        if (
            type(self.runtime_roles) is not tuple
            or not 1 <= len(self.runtime_roles) <= 32
            or any(type(role_name) is not str for role_name in self.runtime_roles)
            or tuple(sorted(set(self.runtime_roles))) != self.runtime_roles
        ):
            raise LegacyMembershipSourceError("ACA runtime roles are invalid")
        for role_name in self.runtime_roles:
            try:
                _identifier(role_name)
            except ValueError:
                raise LegacyMembershipSourceError("ACA runtime roles are invalid") from None
        if (
            type(self.reviewed_alias_sha256) is not str
            or re.fullmatch(r"[0-9a-f]{64}", self.reviewed_alias_sha256) is None
        ):
            raise LegacyMembershipSourceError("ACA reviewed alias digest is invalid")
        for field_name, maximum in (
            ("source_id", 128),
            ("release_id", 256),
            ("import_id", 256),
            ("source_url", 2048),
            ("alias_scope", 400),
        ):
            coordinate = getattr(self, field_name)
            if (
                type(coordinate) is not str
                or not 1 <= len(coordinate.encode()) <= maximum
                or coordinate.strip() != coordinate
                or not coordinate.isprintable()
            ):
                raise LegacyMembershipSourceError("ACA source coordinates are invalid")
        aca_checksum_scope(self.issuer_id, self.year, self.alias_scope)
        if self.prepared.manifest.source_metadata.get("network_membership") != self.coordinates:
            raise LegacyMembershipSourceError("ACA source admission coordinates differ")

    @property
    def coordinates(self):
        """Return the closed source namespace recorded at archive admission."""
        return {
            field: getattr(self, field)
            for field in (
                "source_id",
                "release_id",
                "import_id",
                "issuer_id",
                "year",
                "source_url",
                "alias_scope",
                "reviewed_alias_sha256",
            )
        } | {"runtime_roles": list(self.runtime_roles)}

    @property
    def generation_id(self):
        """Bind full archive identity, release and reviewed alias namespace."""
        return hashlib.sha256(
            _canonical_json(
                {
                    "coordinates": self.coordinates,
                    "manifest": self.prepared.manifest.as_dict(),
                    "validation": self.validation.as_dict(),
                    "dataset_id": str(self.prepared.ownership.dataset_id),
                    "auxiliary_oid": self.prepared.ownership.auxiliary_oid,
                    "sequence_oids": self.prepared.ownership.sequence_oids,
                }
            )
        ).hexdigest()


def _validated_archive(source_pin):
    if (
        type(source_pin.prepared) is not ReferenceFamilyPreparedSource
        or type(source_pin.validation) is not ReferenceFamilyValidationReceipt
    ):
        raise LegacyMembershipSourceError("Admitted ACA archive is required")
    try:
        manifest = validate_reference_family_manifest(source_pin.prepared.manifest)
        validation = validate_reference_family_validation_receipt(source_pin.validation)
    except ReferenceFamilyArchiveError, ValueError:
        raise LegacyMembershipSourceError("ACA archive receipt differs") from None
    ownership = source_pin.prepared.ownership
    if (
        manifest.importer_id not in {"mrf", "mrf-address"}
        or ownership.importer_id != manifest.importer_id
        or ownership.schema_name != reference_family_stage_schema(ownership.dataset_id)
        or validation.importer_id != manifest.importer_id
        or validation.stage_schema != ownership.schema_name
        or validation.stage_schema_oid != ownership.schema_oid
        or validation.relation_oids != ownership.relation_oids
        or validation.tables != manifest.tables
        or validation.manifest_sha256 != hashlib.sha256(_canonical_json(manifest.as_dict())).hexdigest()
    ):
        raise LegacyMembershipSourceError("ACA archive scope or digest differs")
    return ownership, validation


_CATALOG_SQL = """WITH privileges AS (WITH closure_scope AS (
 SELECT $2::oid AS schema_oid,$3::oid AS owner_oid,NULL::text[] AS relation_names,
 ARRAY(SELECT DISTINCT role.rolname::text FROM (
   SELECT a.grantee FROM pg_namespace n CROSS JOIN LATERAL aclexplode(n.nspacl) a WHERE n.oid=$2::oid
   UNION SELECT a.grantee FROM pg_class c CROSS JOIN LATERAL aclexplode(c.relacl) a WHERE c.relnamespace=$2::oid
   UNION SELECT a.grantee FROM pg_class c JOIN pg_attribute att ON att.attrelid=c.oid
     CROSS JOIN LATERAL aclexplode(att.attacl) a WHERE c.relnamespace=$2::oid
   UNION SELECT a.grantee FROM pg_default_acl d CROSS JOIN LATERAL aclexplode(d.defaclacl) a
     WHERE d.defaclnamespace=$2::oid
 ) grants JOIN pg_roles role ON role.oid=grants.grantee WHERE role.oid<>$3::oid
 ORDER BY role.rolname::text LIMIT 65) || $4::text[] AS role_names
) {privilege_sql}),inventory AS MATERIALIZED (
 SELECT * FROM pg_class WHERE relnamespace=$2::oid ORDER BY relname LIMIT 257)
SELECT owner.*,n.oid::bigint AS schema_oid,n.nspowner=$3::oid AS owner_matches,
 (SELECT jsonb_object_agg(c.relname,c.oid::bigint) FROM inventory c
   WHERE c.relnamespace=n.oid AND c.relkind='r')::text AS relation_oids,
 (SELECT coalesce(jsonb_object_agg(c.relname,c.oid::bigint),'{{}}'::jsonb) FROM inventory c
   WHERE c.relnamespace=n.oid AND c.relkind='S')::text AS sequence_oids,
 NOT EXISTS(SELECT 1 FROM pg_class c WHERE c.relnamespace=n.oid AND (c.relowner<>$3::oid OR c.relpersistence<>'p' OR c.relkind NOT IN ('r','i','S')))
   AND NOT EXISTS(SELECT 1 FROM pg_proc WHERE pronamespace=n.oid) AS relations_owned,
 privileges.closed FROM pg_namespace n JOIN pg_roles owner ON owner.oid=$3::oid CROSS JOIN privileges
 WHERE n.nspname=$1 AND NOT EXISTS(SELECT 1 FROM {namespace}.mrf_address_evidence WHERE false)"""


async def _require_archive(connection, source_pin):
    ownership, validation = _validated_archive(source_pin)
    if source_pin.prepared.manifest.source_metadata.get("network_membership") != source_pin.coordinates:
        raise LegacyMembershipSourceError("ACA source admission coordinates differ")
    catalog = await connection.fetchrow(
        _CATALOG_SQL.format(namespace=_identifier(ownership.schema_name), privilege_sql=_NATIVE_WRITER_PRIVILEGES_SQL),
        ownership.schema_name,
        ownership.schema_oid,
        validation.sealed_owner_oid,
        list(source_pin.runtime_roles),
    )
    if (
        catalog is None
        or not _protected_owner(catalog)
        or not catalog["owner_matches"]
        or catalog["schema_oid"] != ownership.schema_oid
        or not catalog["relations_owned"]
        or not catalog["closed"]
        or json.loads(catalog["sequence_oids"]) != {name: oid for name, oid, _, _ in ownership.sequence_oids}
        or json.loads(catalog["relation_oids"])
        != (
            dict(ownership.relation_oids)
            | ({"mrf_canonical_address": ownership.auxiliary_oid} if ownership.auxiliary_oid is not None else {})
        )
    ):
        raise LegacyMembershipSourceError("Pinned ACA archive is unavailable or writable")


_ALIAS_PARTITION_SQL = """SELECT alias.source_system,alias.source_id,alias.alias_type,alias.alias_value,alias.scope_key,
 alias.network_id,alias.evidence_id,alias.created_at AT TIME ZONE 'UTC' AS created_at,
 identity.network_id IS NULL OR CASE WHEN pg_input_is_valid(alias.alias_value,'bigint')
   THEN alias.alias_value::bigint::text<>alias.alias_value ELSE true END AS unresolved
 FROM {registry_namespace}.network_registry_alias alias
 LEFT JOIN {registry_namespace}.network_registry_identity identity USING(network_id)
 WHERE source_system='aca' AND source_id=$7 AND alias_type='legacy_checksum' AND scope_key=$8
 ORDER BY alias_value LIMIT 5001"""

_ALIAS_DIGEST_SQL = """SELECT CASE WHEN count(*)<=5000 AND NOT coalesce(bool_or(unresolved),false)
 AND coalesce(sum(octet_length(to_jsonb(selected)::text)),0)<=8388608
 THEN encode(sha256(convert_to(coalesce(jsonb_agg(to_jsonb(selected) ORDER BY to_jsonb(selected)::text),
 '[]'::jsonb)::text,'UTF8')),'hex') END FROM selected"""

_ALIASES_SQL = (
    "WITH selected AS MATERIALIZED ("
    + _ALIAS_PARTITION_SQL.replace("$7", "$1").replace("$8", "$2")
    + ") "
    + _ALIAS_DIGEST_SQL
)


async def capture_aca_reviewed_alias_digest(connection, *, source_id, issuer_id, year, alias_scope, registry_schema):
    """Pin an already reviewed bounded alias partition; this does not approve it."""
    if not connection.is_in_transaction():
        raise LegacyMembershipSourceError("ACA alias capture requires a caller transaction")
    digest = await connection.fetchval(
        _ALIASES_SQL.format(registry_namespace=_identifier(registry_schema)),
        source_id,
        aca_checksum_scope(issuer_id, year, alias_scope),
    )
    if digest is None:
        raise LegacyMembershipSourceError("ACA reviewed aliases are unresolved or exceed bounds")
    return digest


_BATCH_SQL = """WITH selected AS MATERIALIZED ({alias_partition}),page AS MATERIALIZED (
 SELECT * FROM {source_namespace}.mrf_address_evidence
 WHERE import_id=$1 AND issuer_id=$2 AND year=$3 AND source_url=$4
   AND ($5::bigint IS NULL OR evidence_checksum>$5) ORDER BY evidence_checksum LIMIT $6
), resolved AS MATERIALIZED (
 SELECT page.*,identity.network_id,alias.evidence_id AS reviewed_evidence,
   address_key IS NULL OR address_key='00000000-0000-0000-0000-000000000000'::uuid
   OR npi::text !~ '^[12][0-9]{{9}}$' OR checksum_network IS NULL OR identity.network_id IS NULL
   OR source_record_id='' OR address_source<>'marketplace_provider' OR source_table<>'plan_npi_raw' AS unresolved
 FROM page LEFT JOIN selected alias ON alias.alias_value=page.checksum_network::text
 LEFT JOIN {registry_namespace}.network_registry_identity identity ON identity.network_id=alias.network_id
), encoded AS (
 SELECT jsonb_build_object('network_id',network_id,'provider_system','npi','provider_id',npi::text,
   'location_id',address_key,'evidence_id',encode(sha256(convert_to(
     jsonb_build_array($9::text,to_jsonb(resolved))::text,'UTF8')),'hex')) AS membership
 FROM resolved WHERE NOT unresolved
)
SELECT ({alias_digest}) AS alias_sha256,(SELECT count(*) FROM page) AS source_rows,(SELECT max(evidence_checksum) FROM page) AS last_checksum,
 (SELECT count(*) FROM resolved WHERE unresolved) AS unresolved_rows,
 (SELECT count(*) FROM encoded) AS membership_rows,
 CASE WHEN (SELECT coalesce(sum(octet_length(membership::text)),0)+2*count(*)+2 FROM encoded)<=8388608
 THEN (SELECT coalesce(jsonb_agg(membership ORDER BY membership::text),'[]'::jsonb)::text FROM encoded) END AS input_json"""


_REVIEWED_SQL = """WITH {bindings},page AS MATERIALIZED (
 SELECT * FROM {source_namespace}.mrf_address_evidence
 WHERE import_id=$8 AND issuer_id=$9 AND year=$10 AND source_url=$11
   AND ($12::bigint IS NULL OR evidence_checksum>$12) ORDER BY evidence_checksum LIMIT $13
), expanded AS MATERIALIZED (
 SELECT page.*,provider.npi AS retained_npi,tier.plan_id AS retained_plan_id,
   plan.plan_id,plan.state AS plan_state,plan.issuer_id AS plan_issuer,plan.year AS plan_year,
   approved.network_id,approved.record_key AS binding_record_key
 FROM page LEFT JOIN {source_namespace}.plan_npi_raw provider
   ON provider.npi=page.npi AND provider.checksum_network=page.checksum_network
  AND provider.issuer_id=page.issuer_id AND provider.year=page.year AND provider.network_tier=page.network_tier
 LEFT JOIN {source_namespace}.plan_networktier tier
   ON tier.checksum_network=provider.checksum_network AND tier.issuer_id=provider.issuer_id
  AND tier.year=provider.year AND tier.network_tier=provider.network_tier
 LEFT JOIN {source_namespace}.plan plan
   ON plan.plan_id=tier.plan_id AND plan.year=tier.year AND plan.issuer_id=tier.issuer_id
 LEFT JOIN approved_network_bindings approved ON approved.record_json->>'source_key'=plan.plan_id
  AND approved.record_json->'source_scope_json'=jsonb_build_object(
    'issuer_id',lpad(plan.issuer_id::text,5,'0'),'state',plan.state,'plan_year',plan.year,
    'plan_id',plan.plan_id,'checksum_network',page.checksum_network)
 LIMIT 5001
), resolved AS MATERIALIZED (
 SELECT expanded.*,{omission_columns}COALESCE(
   address_key IS NULL OR address_key='00000000-0000-0000-0000-000000000000'::uuid
   OR npi::text !~ '^[12][0-9]{{9}}$' OR checksum_network NOT BETWEEN -2147483648 AND 2147483647
   OR checksum_network IS NULL OR retained_npi IS NULL OR retained_plan_id IS NULL OR plan_id IS NULL
   OR plan_issuer NOT BETWEEN 1 AND 99999 OR plan_year NOT BETWEEN 2010 AND 2100
   OR network_id IS NULL OR network_tier IS NULL OR network_tier=''
   OR source_record_id='' OR address_source<>'marketplace_provider' OR source_table<>'plan_npi_raw',true) AS unresolved
 FROM expanded
), encoded AS (
 SELECT jsonb_build_object('network_id',network_id,'provider_system','npi','provider_id',npi::text,
   'location_id',address_key,'evidence_id',encode(sha256(convert_to(
     jsonb_build_array($14::text,to_jsonb(resolved))::text,'UTF8')),'hex')) AS membership
 FROM resolved WHERE NOT unresolved {encoded_filter}
)
SELECT (SELECT count(*) FROM page) AS source_rows,(SELECT max(evidence_checksum) FROM page) AS last_checksum,
 (SELECT count(*) FROM resolved WHERE unresolved {unresolved_filter}) AS unresolved_rows,
 {omitted_summary}
 (SELECT count(*) FROM encoded) AS membership_rows,
 CASE WHEN (SELECT count(*) FROM expanded)<=5000
 AND (SELECT coalesce(sum(octet_length(membership::text)),0)+2*count(*)+2 FROM encoded)<=8388608
 THEN (SELECT coalesce(jsonb_agg(membership ORDER BY membership::text),'[]'::jsonb)::text FROM encoded) END AS input_json"""


def _reviewed_descriptor(source, approved_source, coordinates, approved_only=False):
    """The sealed full archive supplies producer/edition authority, never a guess."""
    if type(approved_only) is not bool:
        raise LegacyMembershipSourceError("Reviewed ACA source coordinates are invalid")
    if approved_source is None and coordinates is None:
        if approved_only:
            raise LegacyMembershipSourceError("Reviewed ACA source coordinates are invalid")
        return None
    if (
        type(approved_source) is not ApprovedMembershipSource
        or type(coordinates) is not RegistryNetworkSourceCoordinates
        or source.prepared.manifest.importer_id != "mrf"
        or coordinates.source_system != "aca"
        or coordinates.source_id != source.source_id
        or coordinates.dataset_schema != source.prepared.ownership.schema_name
        or coordinates.dataset_id != str(source.prepared.ownership.dataset_id)
    ):
        raise LegacyMembershipSourceError("Reviewed ACA source coordinates are invalid")
    descriptor_dict = {**asdict(coordinates), "source_key_kind": "hios_plan_id"}
    # ReferenceFamilyStageOwnership has no producer field; the sealed descriptor is its authority.
    if source.prepared.manifest.source_metadata.get("network_bindings") != descriptor_dict:
        raise LegacyMembershipSourceError("Reviewed ACA source admission coordinates differ")
    return descriptor_dict


def _reviewed_generation(source, approved_source, coordinates, approved_only=False):
    recipe_dict = {
        "source_generation": source.generation_id,
        "approved_source": asdict(approved_source),
        "binding_coordinates": asdict(coordinates),
    }
    if approved_only:
        recipe_dict["selection_policy"] = "approved-bindings-only"
    return hashlib.sha256(_canonical_json(recipe_dict)).hexdigest()


@dataclass(frozen=True)
class ACAMembershipBatch:
    source: PinnedACAMembershipSource
    source_rows: int
    membership_rows: int
    unresolved_rows: int
    next_evidence_checksum: int | None
    input_bytes: bytes
    input_sha256: str
    approved_source: ApprovedMembershipSource | None = None
    binding_coordinates: RegistryNetworkSourceCoordinates | None = None
    approved_only: bool = False
    omitted_rows: int = 0

    @property
    def generation_id(self):
        """Bind admitted source lineage and the exact approved mapping recipe."""
        _reviewed_descriptor(self.source, self.approved_source, self.binding_coordinates, self.approved_only)
        if self.approved_source is None and self.binding_coordinates is None:
            return self.source.generation_id
        return _reviewed_generation(self.source, self.approved_source, self.binding_coordinates, self.approved_only)


def _reviewed_sql(namespace, registry_schema, approved_only):
    complete_key = """COALESCE(retained_npi IS NOT NULL AND retained_plan_id IS NOT NULL AND plan_id IS NOT NULL
      AND checksum_network BETWEEN -2147483648 AND 2147483647
      AND plan_issuer BETWEEN 1 AND 99999 AND plan_year BETWEEN 2010 AND 2100
      AND plan_state=ANY(ARRAY['AL','AK','AZ','AR','CA','CO','CT','DE','DC','FL','GA','HI','ID','IL',
        'IN','IA','KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ','NM','NY','NC',
        'ND','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VT','VA','WA','WV','WI','WY','AS','GU','MP','PR','VI'])
      AND plan_id ~ ('^'||lpad(plan_issuer::text,5,'0')||plan_state||'[0-9]{7}(-[0-9]{2})?$')
      AND network_tier IS NOT NULL AND network_tier<>'' AND source_record_id<>''
      AND address_source='marketplace_provider' AND source_table='plan_npi_raw',false)"""
    return _REVIEWED_SQL.format(
        source_namespace=_identifier(namespace),
        bindings=APPROVED_NETWORK_BINDINGS_SQL.format(namespace=_identifier(registry_schema)),
        omission_columns=f"network_id IS NULL AND ({complete_key}) AS omitted," if approved_only else "",
        encoded_filter="AND NOT omitted" if approved_only else "",
        unresolved_filter="AND NOT omitted" if approved_only else "",
        omitted_summary="(SELECT count(*) FROM resolved WHERE omitted) AS omitted_rows," if approved_only else "",
    )


async def _read_reviewed(
    connection, source, approved_source, coordinates, page_options, registry_schema, approved_only
):
    try:
        await require_approved_network_source_bindings(
            connection, approved_source, coordinates, control_schema=registry_schema
        )
    except ApprovedNetworkSourceBindingError:
        raise LegacyMembershipSourceError("Reviewed ACA bindings are unavailable") from None
    generation = _reviewed_generation(source, approved_source, coordinates, approved_only)
    return await connection.fetchrow(
        _reviewed_sql(source.prepared.ownership.schema_name, registry_schema, approved_only),
        approved_source.approved_revision,
        *coordinates.sql_parameters,
        source.import_id,
        source.issuer_id,
        source.year,
        source.source_url,
        *page_options,
        generation,
    )


async def read_aca_membership_batch(
    connection,
    source_pin,
    *,
    registry_schema,
    after_evidence_checksum=None,
    limit=1000,
    approved_source=None,
    binding_coordinates=None,
    approved_only=False,
):
    """Read a bounded keyset page; unresolved rows cannot be copied.

    Paired approved pins require the full sealed plan family and exact admitted
    coordinates. Reviewed extraction never falls back to the legacy alias map.
    Approved-only selection counts complete unmapped plan combinations in
    ``omitted_rows`` while incomplete source lineage remains unresolved.
    """
    if type(source_pin) is not PinnedACAMembershipSource or not connection.is_in_transaction():
        raise LegacyMembershipSourceError("Pinned ACA reads require a caller transaction")
    if type(limit) is not int or not 1 <= limit <= MAX_ROWS:
        raise LegacyMembershipSourceError("ACA source page limit is invalid")
    if after_evidence_checksum is not None and (
        type(after_evidence_checksum) is not int or not -(2**63) <= after_evidence_checksum < 2**63
    ):
        raise LegacyMembershipSourceError("ACA source cursor is invalid")
    descriptor = _reviewed_descriptor(source_pin, approved_source, binding_coordinates, approved_only)
    registry_namespace = _identifier(registry_schema)
    await _require_archive(connection, source_pin)
    if descriptor is not None:
        batch = await _read_reviewed(
            connection,
            source_pin,
            approved_source,
            binding_coordinates,
            (after_evidence_checksum, limit),
            registry_schema,
            approved_only,
        )
    else:
        batch = await _read_legacy(connection, source_pin, registry_namespace, after_evidence_checksum, limit)
    if batch["input_json"] is None:
        raise LegacyMembershipBatchBoundsError("ACA membership batch exceeds bounds")
    input_bytes = batch["input_json"].encode()
    if len(input_bytes) > MAX_INPUT_BYTES:
        raise LegacyMembershipBatchBoundsError("ACA membership batch exceeds bounds")
    return ACAMembershipBatch(
        source_pin,
        batch["source_rows"],
        batch["membership_rows"],
        batch["unresolved_rows"],
        batch["last_checksum"],
        input_bytes,
        hashlib.sha256(input_bytes).hexdigest(),
        approved_source,
        binding_coordinates,
        approved_only,
        batch["omitted_rows"] if approved_only else 0,
    )


async def _read_legacy(connection, source_pin, registry_namespace, after_evidence_checksum, limit):
    batch = await connection.fetchrow(
        _BATCH_SQL.format(
            source_namespace=_identifier(source_pin.prepared.ownership.schema_name),
            registry_namespace=registry_namespace,
            alias_partition=_ALIAS_PARTITION_SQL.format(registry_namespace=registry_namespace),
            alias_digest=_ALIAS_DIGEST_SQL,
        ),
        source_pin.import_id,
        source_pin.issuer_id,
        source_pin.year,
        source_pin.source_url,
        after_evidence_checksum,
        limit,
        source_pin.source_id,
        aca_checksum_scope(source_pin.issuer_id, source_pin.year, source_pin.alias_scope),
        source_pin.generation_id,
    )
    if batch["alias_sha256"] != source_pin.reviewed_alias_sha256:
        raise LegacyMembershipSourceError("Pinned ACA reviewed aliases changed or exceed bounds")
    return batch


async def _require_reviewed_candidate(connection, batch, copy_target, control_schema):
    if batch.approved_source is None:
        return
    candidate = await _locked_candidate(connection, copy_target, _control_namespace(control_schema))
    generations = json.loads(candidate["source_generations"])
    if (
        candidate["approved_custom_revision"] != batch.approved_source.approved_revision
        or generations.get("custom_membership") != batch.approved_source.generation_id
        or batch.generation_id not in generations.values()
    ):
        raise LegacyMembershipSourceError("Reviewed ACA candidate pin does not match")


async def copy_aca_membership_batch(
    connection,
    batch,
    copy_target,
    *,
    require_candidate_authority,
    control_schema=None,
):
    """Use native COPY; authority must bind ``batch.generation_id`` to the candidate.

    Durable batch registration and whole-source accounting remain with the
    lifecycle caller. Reviewed candidate revision and custom fingerprint are
    checked before and after COPY. Successful rows stay in the caller transaction.
    """
    if type(batch) is not ACAMembershipBatch or batch.unresolved_rows:
        raise LegacyMembershipSourceError("ACA source memberships are unresolved")
    _reviewed_descriptor(batch.source, batch.approved_source, batch.binding_coordinates, batch.approved_only)
    if not connection.is_in_transaction():
        raise LegacyMembershipSourceError("ACA membership COPY requires a caller transaction")
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
            raise LegacyMembershipSourceError("ACA membership COPY accounting mismatch")
        await _require_reviewed_candidate(connection, batch, copy_target, control_schema)
        return receipt
