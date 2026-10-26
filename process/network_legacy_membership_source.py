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
from dataclasses import asdict, dataclass, field, replace
from functools import lru_cache
from typing import TYPE_CHECKING

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

if TYPE_CHECKING:
    from process.registry_aca_global_epoch import PinnedACAGlobalEpoch


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
    retained_successor: PinnedACAMembershipSource | None = None
    retained_epoch: PinnedACAGlobalEpoch | None = None

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
        if aca_source_descriptor(self).get("network_membership") != self.coordinates:
            raise LegacyMembershipSourceError("ACA source admission coordinates differ")
        _validate_successor(self)

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
        generation_by_name = {
            "coordinates": self.coordinates,
            "manifest": self.prepared.manifest.as_dict(),
            "validation": self.validation.as_dict(),
            "dataset_id": str(self.prepared.ownership.dataset_id),
            "auxiliary_oid": self.prepared.ownership.auxiliary_oid,
            "sequence_oids": self.prepared.ownership.sequence_oids,
        }
        if self.retained_successor is not None:
            generation_by_name["retained_successor"] = self.retained_successor.generation_id
        if self.retained_epoch is not None:
            generation_by_name["retained_epoch"] = self.retained_epoch.generation_id
        return hashlib.sha256(_canonical_json(generation_by_name)).hexdigest()


def _validate_successor(source):
    if source.retained_epoch is not None:
        _validate_retained_epoch(source)
        return
    successor = source.retained_successor
    if successor is None:
        return
    if (
        type(successor) is not PinnedACAMembershipSource
        or successor.retained_successor is not None
        or successor.retained_epoch is not None
    ):
        raise LegacyMembershipSourceError("ACA successor must be one flat admitted archive")
    original = _aca_binding_coordinates(source)
    current = _aca_binding_coordinates(successor)
    if (source.source_id, source.issuer_id, source.year) != (
        successor.source_id,
        successor.issuer_id,
        successor.year,
    ) or any(getattr(original, key) != getattr(current, key) for key in ("source_system", "source_id", "producer_id")):
        raise LegacyMembershipSourceError("ACA successor logical source differs")


@dataclass(frozen=True)
class ACAGlobalEpochReadScope:
    """A physical epoch view; issuer/year are original scope, not rewritten provenance."""

    epoch: PinnedACAGlobalEpoch
    issuer_id: int
    year: int
    import_id: None = None
    source_url: None = None

    def __post_init__(self):
        from process.registry_aca_global_epoch import PinnedACAGlobalEpoch

        if (
            type(self.epoch) is not PinnedACAGlobalEpoch
            or type(self.issuer_id) is not int
            or not 0 < self.issuer_id <= 2147483647
            or type(self.year) is not int
            or not 1900 <= self.year <= 9999
            or self.import_id is not None
            or self.source_url is not None
        ):
            raise LegacyMembershipSourceError("ACA global read scope differs")

    @property
    def prepared(self):
        """Return the genuine retained epoch prepared archive."""
        return self.epoch.prepared

    @property
    def validation(self):
        """Return the complete typed epoch validation receipt."""
        return self.epoch.validation

    @property
    def runtime_roles(self):
        """Return the frozen explicit native reader census."""
        return self.epoch.runtime_roles

    @property
    def coordinates(self):
        """Return a physical diagnostic identity without invented source coordinates."""
        return {"global_epoch": self.epoch.generation_id, "issuer_id": self.issuer_id, "year": self.year}


def _validate_retained_epoch(source):
    from process.registry_aca_global_epoch import PinnedACAGlobalEpoch

    epoch = source.retained_epoch
    if source.retained_successor is not None or type(epoch) is not PinnedACAGlobalEpoch:
        raise LegacyMembershipSourceError("ACA physical authorities are mutually exclusive")
    _aca_binding_coordinates(source)
    original = source.prepared.manifest.source_serving_generation
    current = epoch.prepared.manifest.source_serving_generation
    if original is None or current is None or original.origin_lineage_id != current.origin_lineage_id:
        raise LegacyMembershipSourceError("ACA global epoch origin lineage differs")


def with_aca_global_epoch(anchor, epoch):
    """Attach one global physical epoch without inventing per-scope source metadata."""
    return replace(anchor, retained_successor=None, retained_epoch=epoch)


def _aca_binding_coordinates(source):
    descriptor = aca_source_descriptor(source).get("network_bindings")
    keys = {*RegistryNetworkSourceCoordinates.__dataclass_fields__, "source_key_kind"}
    if type(descriptor) is not dict or set(descriptor) != keys or descriptor["source_key_kind"] != "hios_plan_id":
        raise LegacyMembershipSourceError("ACA successor binding descriptor differs")
    coordinates = RegistryNetworkSourceCoordinates(
        **{key: value for key, value in descriptor.items() if key != "source_key_kind"}
    )
    if coordinates.sql_parameters[:4] != (
        "aca",
        source.source_id,
        source.prepared.ownership.schema_name,
        str(source.prepared.ownership.dataset_id),
    ):
        raise LegacyMembershipSourceError("ACA successor binding descriptor differs")
    return coordinates


@lru_cache(maxsize=64)
def _cached_batch_descriptors(encoded_metadata, runtime_roles):
    """Reuse bounded structural validation only; native authority is checked separately."""
    from process.registry_aca_source_recipe import _batch_descriptors

    return tuple(_canonical_json(scope) for scope in _batch_descriptors(json.loads(encoded_metadata), runtime_roles))


def aca_source_descriptor(source):
    """Select one exact sealed scope without changing the physical archive identity."""
    metadata = source.prepared.manifest.source_metadata
    if "registry_aca_initial_batch" not in metadata:
        return metadata
    matches = [
        descriptor
        for descriptor in map(
            json.loads, _cached_batch_descriptors(_canonical_json(dict(metadata)), source.runtime_roles)
        )
        if descriptor["network_membership"] == source.coordinates
    ]
    if len(matches) != 1:
        raise LegacyMembershipSourceError("ACA source admission coordinates differ")
    return matches[0]


def aca_physical_source(source):
    """Return one flat physical pin; this structural accessor confers no SQL authority."""
    if type(source) is not PinnedACAMembershipSource:
        raise LegacyMembershipSourceError("Pinned ACA source is required")
    _validate_successor(source)
    if source.retained_epoch is not None:
        return ACAGlobalEpochReadScope(source.retained_epoch, source.issuer_id, source.year)
    return source if source.retained_successor is None else source.retained_successor


def with_aca_successor(anchor, successor):
    """Keep the original approved anchor and replace only its single physical attachment."""
    if (
        type(successor) is not PinnedACAMembershipSource
        or successor.retained_successor is not None
        or successor.retained_epoch is not None
    ):
        raise LegacyMembershipSourceError("ACA successor must be one flat admitted archive")
    if aca_physical_source(anchor) == successor:
        return anchor
    return replace(anchor, retained_successor=successor, retained_epoch=None)


def _validated_archive(source_pin):
    return _validated_native_archive(source_pin.prepared, source_pin.validation)


def _validated_native_archive(prepared, validation):
    if type(prepared) is not ReferenceFamilyPreparedSource or type(validation) is not ReferenceFamilyValidationReceipt:
        raise LegacyMembershipSourceError("Admitted ACA archive is required")
    try:
        manifest = validate_reference_family_manifest(prepared.manifest)
        validation = validate_reference_family_validation_receipt(validation)
    except ReferenceFamilyArchiveError, ValueError:
        raise LegacyMembershipSourceError("ACA archive receipt differs") from None
    ownership = prepared.ownership
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
    """Prove both immutable archives when one original anchor carries a successor."""
    physical = aca_physical_source(source_pin)
    original = (
        source_pin if physical is source_pin else replace(source_pin, retained_successor=None, retained_epoch=None)
    )
    await _require_flat_archive(connection, original)
    if type(physical) is ACAGlobalEpochReadScope:
        from process.network_cms_registry_address_capture import NativeReceiptSession
        from process.registry_aca_global_epoch import require_registry_aca_global_epoch

        epoch = await require_registry_aca_global_epoch(
            NativeReceiptSession(connection), physical.prepared, runtime_roles=physical.runtime_roles
        )
        if epoch != physical.epoch:
            raise LegacyMembershipSourceError("ACA protected global epoch differs")
    elif physical is not source_pin:
        from process.network_cms_registry_address_capture import NativeReceiptSession
        from process.registry_aca_source_recipe import require_registry_aca_source_recipe

        recipe = await require_registry_aca_source_recipe(
            NativeReceiptSession(connection),
            physical.prepared,
            runtime_roles=physical.runtime_roles,
            **({"binding_coordinates": _aca_binding_coordinates(physical)} if _is_initial_batch(physical) else {}),
        )
        if recipe.source_pin != physical or recipe.binding_coordinates != _aca_binding_coordinates(physical):
            raise LegacyMembershipSourceError("ACA successor protected recipe differs")


async def _require_flat_archive(connection, source_pin):
    if aca_source_descriptor(source_pin).get("network_membership") != source_pin.coordinates:
        raise LegacyMembershipSourceError("ACA source admission coordinates differ")
    if _is_initial_batch(source_pin):
        from process.network_cms_registry_address_capture import NativeReceiptSession
        from process.registry_aca_source_recipe import require_registry_aca_source_recipe

        recipe = await require_registry_aca_source_recipe(
            NativeReceiptSession(connection),
            source_pin.prepared,
            runtime_roles=source_pin.runtime_roles,
            binding_coordinates=_aca_binding_coordinates(source_pin),
        )
        if recipe.source_pin != source_pin:
            raise LegacyMembershipSourceError("ACA protected batch scope differs")
        return
    await _require_native_archive(connection, source_pin.prepared, source_pin.validation, source_pin.runtime_roles)


def _is_initial_batch(source):
    return "registry_aca_initial_batch" in source.prepared.manifest.source_metadata


async def _require_native_archive(connection, prepared, validation, runtime_roles):
    """Prove a full native archive without assigning any logical membership identity."""
    ownership, validation = _validated_native_archive(prepared, validation)
    if (
        type(runtime_roles) is not tuple
        or not 1 <= len(runtime_roles) <= 32
        or any(type(role) is not str for role in runtime_roles)
        or tuple(sorted(set(runtime_roles))) != runtime_roles
    ):
        raise LegacyMembershipSourceError("ACA runtime roles are invalid")
    for role in runtime_roles:
        _identifier(role)
    catalog = await connection.fetchrow(
        _CATALOG_SQL.format(namespace=_identifier(ownership.schema_name), privilege_sql=_NATIVE_WRITER_PRIVILEGES_SQL),
        ownership.schema_name,
        ownership.schema_oid,
        validation.sealed_owner_oid,
        list(runtime_roles),
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
 SELECT evidence_checksum,jsonb_build_object('network_id',network_id,'provider_system','npi','provider_id',npi::text,
   'location_id',address_key,'evidence_id',encode(sha256(convert_to(
     jsonb_build_array($9::text,to_jsonb(resolved))::text,'UTF8')),'hex')) AS membership
 FROM resolved WHERE NOT unresolved
)
SELECT ({alias_digest}) AS alias_sha256,(SELECT count(*) FROM page) AS source_rows,(SELECT max(evidence_checksum) FROM page) AS last_checksum,
 (SELECT count(*) FROM resolved WHERE unresolved) AS unresolved_rows,
 (SELECT count(*) FROM encoded) AS membership_rows,
 (SELECT coalesce(jsonb_agg(DISTINCT evidence_checksum ORDER BY evidence_checksum),'[]'::jsonb)::text FROM encoded) AS accepted_evidence_checksums,
 CASE WHEN (SELECT coalesce(sum(octet_length(membership::text)),0)+2*count(*)+2 FROM encoded)<=8388608
 THEN (SELECT coalesce(jsonb_agg(membership ORDER BY membership::text),'[]'::jsonb)::text FROM encoded) END AS input_json"""


_ACA_TRANSPORT_SQL = """, anchor_maps AS MATERIALIZED (
 SELECT binding.* FROM approved_network_bindings binding WHERE EXISTS(
   SELECT 1 FROM {anchor}.plan plan JOIN {anchor}.plan_networktier tier
     ON tier.plan_id=plan.plan_id AND tier.issuer_id=plan.issuer_id AND tier.year=plan.year
   WHERE binding.record_json->>'source_key'=plan.plan_id
     AND binding.record_json->'source_scope_json'=jsonb_build_object(
       'issuer_id',lpad(plan.issuer_id::text,5,'0'),'state',plan.state,'plan_year',plan.year,
       'plan_id',plan.plan_id,'checksum_network',tier.checksum_network))
), successor_guard AS (
 SELECT (SELECT count(*) FROM anchor_maps)<>(SELECT count(*) FROM approved_network_bindings)
   OR EXISTS(SELECT 1 FROM anchor_maps GROUP BY record_json->>'source_key',
       record_json->'source_scope_json'->>'issuer_id',record_json->'source_scope_json'->>'state',
       record_json->'source_scope_json'->>'plan_year' HAVING count(DISTINCT network_id)>1) AS invalid
), transported_network_bindings AS MATERIALIZED (
 SELECT encode(sha256(convert_to(array_agg(record_key ORDER BY record_key)::text,'UTF8')),'hex') AS record_key,
   network_id,jsonb_build_object('source_key',record_json->>'source_key',
     'source_scope_json',(record_json->'source_scope_json')-'checksum_network') AS record_json
 FROM anchor_maps GROUP BY network_id,record_json->>'source_key',(record_json->'source_scope_json')-'checksum_network'
)"""


@dataclass(frozen=True)
class ACAReviewedBindingProjection:
    cte_sql: str
    relation_name: str
    scope_without_checksum: bool
    invalid_sql: str


def _binding_projection(binding_cte, anchor_namespace):
    if anchor_namespace is None:
        return ACAReviewedBindingProjection(binding_cte, "approved_network_bindings", False, "false")
    return ACAReviewedBindingProjection(
        binding_cte + _ACA_TRANSPORT_SQL.format(anchor=_identifier(anchor_namespace)),
        "transported_network_bindings",
        True,
        "(SELECT invalid FROM successor_guard)",
    )


def reviewed_aca_binding_projection(source_pin, *, registry_schema, binding_cte=None):
    """Share exact native anchor projection and evidence identity with trusted SQL callers."""
    aca_physical_source(source_pin)
    if binding_cte is None:
        binding_cte = APPROVED_NETWORK_BINDINGS_SQL.format(namespace=_identifier(registry_schema))
    if type(binding_cte) is not str or not binding_cte.startswith("approved_network_bindings AS MATERIALIZED ("):
        raise LegacyMembershipSourceError("ACA approved binding SQL differs")
    anchor = (
        source_pin.prepared.ownership.schema_name
        if source_pin.retained_successor is not None or source_pin.retained_epoch is not None
        else None
    )
    return _binding_projection(binding_cte, anchor)


_REVIEWED_SQL = """WITH {bindings},{raw_page}page AS MATERIALIZED ({page_sql}), expanded AS MATERIALIZED (
 SELECT page.*,provider.npi AS retained_npi,tier.plan_id AS retained_plan_id,
   plan.plan_id,plan.state AS plan_state,plan.issuer_id AS plan_issuer,plan.year AS plan_year,
   approved.network_id,approved.record_key AS binding_record_key
 FROM page LEFT JOIN {source_namespace}.plan_npi_raw provider
   ON {provider_join}
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
), {fact_status}encoded AS (
 SELECT evidence_checksum,jsonb_build_object('network_id',network_id,'provider_system','npi','provider_id',npi::text,
   'location_id',address_key,'evidence_id',encode(sha256(convert_to(
     jsonb_build_array($14::text,to_jsonb(resolved))::text,'UTF8')),'hex')) AS membership
 FROM resolved WHERE NOT unresolved {encoded_filter}
)
SELECT {successor_check}{source_rows} AS source_rows,(SELECT max(evidence_checksum) FROM page) AS last_checksum,
 (SELECT count(*) FROM resolved WHERE unresolved {unresolved_filter}) AS unresolved_rows,
 {omitted_summary}{raw_summary}
 (SELECT count(*) FROM encoded) AS membership_rows,
 (SELECT coalesce(jsonb_agg(DISTINCT evidence_checksum ORDER BY evidence_checksum),'[]'::jsonb)::text FROM encoded) AS accepted_evidence_checksums,
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
        if source.retained_successor is not None or source.retained_epoch is not None:
            raise LegacyMembershipSourceError("ACA successor requires approved original bindings")
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
    if aca_source_descriptor(source).get("network_bindings") != descriptor_dict:
        raise LegacyMembershipSourceError("Reviewed ACA source admission coordinates differ")
    return descriptor_dict


def _reviewed_generation(source, approved_source, coordinates, approved_only=False, complete_raw_facts=False):
    recipe_dict = {
        "source_generation": source.generation_id,
        "approved_source": asdict(approved_source),
        "binding_coordinates": asdict(coordinates),
    }
    if approved_only:
        recipe_dict["selection_policy"] = "approved-bindings-only"
    if complete_raw_facts:
        physical = aca_physical_source(source)
        has_batch_scope = type(physical) is PinnedACAMembershipSource and _is_initial_batch(physical)
        recipe_dict["fact_policy"] = "retained-url-raw-pk-v1" if has_batch_scope else "retained-raw-pk-v1"
    return hashlib.sha256(_canonical_json(recipe_dict)).hexdigest()


@dataclass(frozen=True)
class ACARawFactPosition:
    generation_id: str
    npi: int
    checksum_network: int


@dataclass(frozen=True)
class ACARawFactOutcome:
    npi: int
    checksum_network: int
    outcome: str
    reason: str | None


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
    accepted_evidence_checksums: tuple[int, ...] = ()
    complete_raw_facts: bool = False
    after_source_position: ACARawFactPosition | None = None
    next_source_position: ACARawFactPosition | None = None
    accepted_source_rows: int = 0
    unresolved_source_rows: int = 0
    omitted_source_rows: int = 0
    source_outcomes: tuple[ACARawFactOutcome, ...] = ()
    verified_source: object = field(default=None, compare=False, repr=False)

    @property
    def generation_id(self):
        """Bind admitted source lineage and the exact approved mapping recipe."""
        _reviewed_descriptor(self.source, self.approved_source, self.binding_coordinates, self.approved_only)
        if self.approved_source is None and self.binding_coordinates is None:
            return self.source.generation_id
        return _reviewed_generation(
            self.source, self.approved_source, self.binding_coordinates, self.approved_only, self.complete_raw_facts
        )


def _raw_fact_selection_sql(batch_scope=False):
    # Raw provenance is the admitted immutable relation and issuer/year, not an inferred URL.
    raw_page = """raw_page AS MATERIALIZED (
      SELECT npi,checksum_network,network_tier,issuer_id,year FROM {source}.plan_npi_raw
      WHERE issuer_id=$9 AND year=$10 AND $12::bigint IS NULL
      AND ($15::bigint IS NULL OR (npi,checksum_network)>($15,$16::bigint))
      ORDER BY npi,checksum_network LIMIT $13),"""
    if batch_scope:
        raw_page = raw_page.replace("FROM {source}.plan_npi_raw", "FROM {source}.plan_npi_raw raw")
        raw_page = raw_page.replace(
            "AND ($15::bigint IS NULL",
            "AND EXISTS(SELECT 1 FROM {source}.mrf_address_evidence selected_evidence "
            "WHERE selected_evidence.npi=raw.npi AND selected_evidence.checksum_network=raw.checksum_network "
            "AND selected_evidence.issuer_id=raw.issuer_id AND selected_evidence.year=raw.year "
            "AND selected_evidence.network_tier=raw.network_tier "
            "AND selected_evidence.import_id=$8 AND selected_evidence.source_url=$11) "
            "AND ($15::bigint IS NULL",
        )
    page_sql = """SELECT evidence.*,raw.npi AS fact_npi,raw.checksum_network AS fact_checksum
      FROM raw_page raw LEFT JOIN {source}.mrf_address_evidence evidence
      ON evidence.npi=raw.npi AND evidence.checksum_network=raw.checksum_network
      AND evidence.issuer_id=raw.issuer_id AND evidence.year=raw.year
      AND evidence.network_tier=raw.network_tier
      AND evidence.import_id=$8 AND evidence.source_url=$11 LIMIT 5001"""
    provider_join = """provider.npi=page.fact_npi AND provider.checksum_network=page.fact_checksum
      AND provider.issuer_id=$9 AND provider.year=$10"""
    fact_status = """fact_status AS MATERIALIZED (
      SELECT fact_npi,fact_checksum,
        CASE WHEN bool_or(unresolved AND NOT omitted) THEN 'unresolved'
          WHEN bool_or(NOT unresolved AND NOT omitted) THEN 'accepted' ELSE 'omitted' END AS outcome,
        CASE WHEN bool_or(unresolved AND NOT omitted) THEN
          CASE WHEN bool_or(address_key IS NULL OR address_key='00000000-0000-0000-0000-000000000000'::uuid
            OR address_source IS DISTINCT FROM 'marketplace_provider' OR source_table IS DISTINCT FROM 'plan_npi_raw')
          THEN 'missing_location' ELSE 'invalid_lineage' END END AS reason
      FROM resolved GROUP BY fact_npi,fact_checksum),"""
    raw_summary = """
      (SELECT npi FROM raw_page ORDER BY npi DESC,checksum_network DESC LIMIT 1) AS last_npi,
      (SELECT checksum_network FROM raw_page ORDER BY npi DESC,checksum_network DESC LIMIT 1) AS last_network,
      (SELECT count(*) FROM fact_status WHERE outcome='accepted') AS accepted_source_rows,
      (SELECT count(*) FROM fact_status WHERE outcome='unresolved') AS unresolved_source_rows,
      (SELECT count(*) FROM fact_status WHERE outcome='omitted') AS omitted_source_rows,
      (SELECT coalesce(jsonb_agg(jsonb_build_object('npi',fact_npi,'checksum_network',fact_checksum,
        'outcome',outcome,'reason',reason) ORDER BY fact_npi,fact_checksum),'[]'::jsonb)::text FROM fact_status) AS source_outcomes,
    """
    return raw_page, page_sql, provider_join, fact_status, raw_summary


def _reviewed_sql(
    namespace,
    registry_schema,
    approved_only,
    complete_raw_facts=False,
    *,
    anchor_namespace=None,
    binding_projection=None,
    global_epoch=False,
    batch_scope=False,
):
    """Build bounded raw/evidence membership SQL from a verified physical source view."""
    complete_key = """COALESCE(retained_npi IS NOT NULL AND retained_plan_id IS NOT NULL AND plan_id IS NOT NULL
      AND checksum_network BETWEEN -2147483648 AND 2147483647
      AND plan_issuer BETWEEN 1 AND 99999 AND plan_year BETWEEN 2010 AND 2100
      AND plan_state=ANY(ARRAY['AL','AK','AZ','AR','CA','CO','CT','DE','DC','FL','GA','HI','ID','IL',
        'IN','IA','KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ','NM','NY','NC',
        'ND','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VT','VA','WA','WV','WI','WY','AS','GU','MP','PR','VI'])
      AND plan_id ~ ('^'||lpad(plan_issuer::text,5,'0')||plan_state||'[0-9]{7}(-[0-9]{2})?$')
      AND network_tier IS NOT NULL AND network_tier<>'' AND source_record_id<>''
      AND address_source='marketplace_provider' AND source_table='plan_npi_raw',false)"""
    if complete_raw_facts:
        complete_key += " AND address_key IS NOT NULL AND address_key<>'00000000-0000-0000-0000-000000000000'::uuid"
    raw_page = ""
    page_sql = """SELECT * FROM {source}.mrf_address_evidence
      WHERE import_id=$8 AND issuer_id=$9 AND year=$10 AND source_url=$11
      AND ($12::bigint IS NULL OR evidence_checksum>$12) ORDER BY evidence_checksum LIMIT $13"""
    provider_join = """provider.npi=page.npi AND provider.checksum_network=page.checksum_network
      AND provider.issuer_id=page.issuer_id AND provider.year=page.year AND provider.network_tier=page.network_tier"""
    fact_status = raw_summary = ""
    if complete_raw_facts:
        raw_page, page_sql, provider_join, fact_status, raw_summary = _raw_fact_selection_sql(batch_scope)
    namespace_sql = _identifier(namespace)
    if binding_projection is None:
        binding_projection = _binding_projection(
            APPROVED_NETWORK_BINDINGS_SQL.format(namespace=_identifier(registry_schema)), anchor_namespace
        )
    if type(binding_projection) is not ACAReviewedBindingProjection or type(global_epoch) is not bool:
        raise LegacyMembershipSourceError("ACA binding projection differs")
    sql = _REVIEWED_SQL.format(
        successor_check=binding_projection.invalid_sql + " AS successor_invalid,"
        if binding_projection.scope_without_checksum
        else "",
        raw_page=raw_page.format(source=namespace_sql),
        page_sql=page_sql.format(source=namespace_sql),
        provider_join=provider_join,
        source_rows="(SELECT count(*) FROM raw_page)" if complete_raw_facts else "(SELECT count(*) FROM page)",
        fact_status=fact_status,
        raw_summary=raw_summary,
        source_namespace=_identifier(namespace),
        bindings=binding_projection.cte_sql,
        omission_columns=f"network_id IS NULL AND ({complete_key}) AS omitted,"
        if approved_only
        else ("false AS omitted," if complete_raw_facts else ""),
        encoded_filter="AND NOT omitted" if approved_only else "",
        unresolved_filter="AND NOT omitted" if approved_only else "",
        omitted_summary="(SELECT count(*) FROM resolved WHERE omitted) AS omitted_rows," if approved_only else "",
    )

    return _project_reviewed_sql(sql, binding_projection, global_epoch, complete_raw_facts)


def _project_reviewed_sql(sql, binding_projection, global_epoch, complete_raw_facts):
    """Apply trusted binding projection and the explicit global physical predicates."""
    if binding_projection.scope_without_checksum:
        sql = sql.replace(
            "LEFT JOIN approved_network_bindings approved",
            "LEFT JOIN " + binding_projection.relation_name + " approved",
            1,
        )
        sql = sql.replace(
            "'plan_id',plan.plan_id,'checksum_network',page.checksum_network)", "'plan_id',plan.plan_id)", 1
        )
    if global_epoch:
        prefix = "evidence." if complete_raw_facts else ""
        sql = sql.replace(prefix + "import_id=$8", "($8::text IS NULL OR " + prefix + "import_id=$8)")
        sql = sql.replace(prefix + "source_url=$11", "($11::text IS NULL OR " + prefix + "source_url=$11)")
    return sql


async def _read_reviewed(
    connection,
    source_pin,
    approved_source,
    coordinates,
    page_options,
    registry_schema,
    approved_only,
):
    after_evidence_checksum, limit, after_source_position, complete_raw_facts = page_options
    try:
        await require_approved_network_source_bindings(
            connection, approved_source, coordinates, control_schema=registry_schema
        )
    except ApprovedNetworkSourceBindingError as error:
        raise LegacyMembershipSourceError("Reviewed ACA bindings are unavailable") from error
    generation = _reviewed_generation(source_pin, approved_source, coordinates, approved_only, complete_raw_facts)
    physical = aca_physical_source(source_pin)
    page_by_name = await connection.fetchrow(
        _reviewed_sql(
            physical.prepared.ownership.schema_name,
            registry_schema,
            approved_only,
            complete_raw_facts,
            binding_projection=reviewed_aca_binding_projection(source_pin, registry_schema=registry_schema),
            global_epoch=type(physical) is ACAGlobalEpochReadScope,
            batch_scope=type(physical) is PinnedACAMembershipSource and _is_initial_batch(physical),
        ),
        approved_source.approved_revision,
        *coordinates.sql_parameters,
        physical.import_id,
        physical.issuer_id,
        physical.year,
        physical.source_url,
        after_evidence_checksum,
        limit,
        generation,
        *(
            (
                None if after_source_position is None else after_source_position.npi,
                None if after_source_position is None else after_source_position.checksum_network,
            )
            if complete_raw_facts
            else ()
        ),
    )

    if (source_pin.retained_successor is not None or source_pin.retained_epoch is not None) and page_by_name[
        "successor_invalid"
    ]:
        raise LegacyMembershipSourceError("ACA successor approved anchor is unavailable or ambiguous")
    return page_by_name


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
    """Read the compatible evidence-keyset page; unresolved rows cannot be copied."""
    return await _read_aca_batch(
        connection,
        source_pin,
        registry_schema,
        approved_source,
        binding_coordinates,
        approved_only,
        (after_evidence_checksum, limit, None, False),
    )


async def read_aca_raw_fact_batch(
    connection,
    source_pin,
    *,
    registry_schema,
    after_source_position=None,
    limit=1000,
    approved_source=None,
    binding_coordinates=None,
    approved_only=False,
):
    """Account retained raw PKs using paired full-archive pins and exact evidence.

    Raw authority is the sealed relation and issuer/year, not a per-row source URL.
    Original JSON duplicates and address ordinals are unavailable in this model.
    """
    return await _read_aca_batch(
        connection,
        source_pin,
        registry_schema,
        approved_source,
        binding_coordinates,
        approved_only,
        (None, limit, after_source_position, True),
    )


def _validate_aca_page(connection, source_pin, approved_source, binding_coordinates, approved_only, page_options):
    after_evidence_checksum, limit, after_source_position, complete_raw_facts = page_options
    if type(source_pin) is not PinnedACAMembershipSource or not connection.is_in_transaction():
        raise LegacyMembershipSourceError("Pinned ACA reads require a caller transaction")
    if type(limit) is not int or not 1 <= limit <= MAX_ROWS:
        raise LegacyMembershipSourceError("ACA source page limit is invalid")
    if after_evidence_checksum is not None and (
        type(after_evidence_checksum) is not int or not -(2**63) <= after_evidence_checksum < 2**63
    ):
        raise LegacyMembershipSourceError("ACA source cursor is invalid")
    descriptor = _reviewed_descriptor(source_pin, approved_source, binding_coordinates, approved_only)
    if type(complete_raw_facts) is not bool or (complete_raw_facts and descriptor is None):
        raise LegacyMembershipSourceError("Complete ACA facts require paired full archive pins")
    if after_source_position is not None:
        if not complete_raw_facts:
            raise LegacyMembershipSourceError("ACA source position is invalid")
        generation = _reviewed_generation(source_pin, approved_source, binding_coordinates, approved_only, True)
        if (
            type(after_source_position) is not ACARawFactPosition
            or after_source_position.generation_id != generation
            or type(after_source_position.npi) is not int
            or not -(2**63) <= after_source_position.npi < 2**63
            or type(after_source_position.checksum_network) is not int
            or not -(2**63) <= after_source_position.checksum_network < 2**63
        ):
            raise LegacyMembershipSourceError("ACA source position is invalid")
    if complete_raw_facts and after_evidence_checksum is not None:
        raise LegacyMembershipSourceError("ACA source cursors cannot be mixed")
    return descriptor


async def _read_aca_batch(
    connection,
    source_pin,
    registry_schema,
    approved_source,
    binding_coordinates,
    approved_only,
    page_options,
    verified_source=None,
):
    after_evidence_checksum, limit, _, _ = page_options
    descriptor = _validate_aca_page(
        connection, source_pin, approved_source, binding_coordinates, approved_only, page_options
    )
    registry_namespace = _identifier(registry_schema)
    if verified_source is None:
        await _require_archive(connection, source_pin)
    else:
        from process.registry_aca_source_recipe import require_registry_aca_batch_read

        await require_registry_aca_batch_read(connection, source_pin, verified_source)
    if descriptor is not None:
        batch = await _read_reviewed(
            connection,
            source_pin,
            approved_source,
            binding_coordinates,
            page_options,
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
    return replace(
        _aca_batch_from_row(
            source_pin, batch, approved_source, binding_coordinates, approved_only, page_options, input_bytes
        ),
        verified_source=verified_source,
    )


def _aca_batch_from_row(
    source_pin, batch, approved_source, binding_coordinates, approved_only, page_options, input_bytes
):
    _, _, after_source_position, complete_raw_facts = page_options
    outcomes = (
        tuple(ACARawFactOutcome(**outcome_dict) for outcome_dict in json.loads(batch["source_outcomes"]))
        if complete_raw_facts
        else ()
    )
    if complete_raw_facts and (
        batch["source_rows"]
        != sum(batch[name] for name in ("accepted_source_rows", "unresolved_source_rows", "omitted_source_rows"))
        or len(outcomes) != batch["source_rows"]
    ):
        raise LegacyMembershipSourceError("ACA raw fact accounting is incomplete")
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
        accepted_evidence_checksums=tuple(json.loads(batch["accepted_evidence_checksums"])),
        complete_raw_facts=complete_raw_facts,
        after_source_position=after_source_position,
        next_source_position=ACARawFactPosition(
            _reviewed_generation(source_pin, approved_source, binding_coordinates, approved_only, True),
            batch["last_npi"],
            batch["last_network"],
        )
        if complete_raw_facts and batch["source_rows"]
        else None,
        accepted_source_rows=batch["accepted_source_rows"] if complete_raw_facts else 0,
        unresolved_source_rows=batch["unresolved_source_rows"] if complete_raw_facts else 0,
        omitted_source_rows=batch["omitted_source_rows"] if complete_raw_facts else 0,
        source_outcomes=outcomes,
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
