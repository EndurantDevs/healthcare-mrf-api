# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Immutable CMS edition copies with original source coordinates and native ACLs.

Admission verifies the existing semantic projection and metadata proof through
bounded COPY spools. Expected admission digests must come from a trusted publisher
receipt, independently of the source rows being captured. Origin metadata is never
rewritten to claim that the physical retained namespace was the source namespace.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import struct
import tempfile
from dataclasses import asdict, dataclass
from importlib import import_module
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

from process.network_address_projection import _identifier
from process.network_bootstrap_sources import _copy_native_batch, _CopyBatchTooLarge
from process.network_custom_address_source import _require_transaction
from process.network_membership_writer_closure import _protected_owner
from process.uhc_flex_practitioner_async_safety import drain_operation

_TABLES = (
    "provider_directory_endpoint_dataset",
    "provider_directory_dataset_resource",
    "provider_directory_entity_source_binding",
    "provider_directory_entity_release_evidence",
    "provider_directory_insurance_network_source_binding",
    "provider_directory_insurance_network_plan_evidence",
    "provider_directory_cms_npd_resource_witness",
    "provider_directory_cms_npd_relationship",
    "provider_directory_cms_npd_relationship_receipt",
    "provider_directory_cms_candidate_coverage",
    "provider_directory_cms_serving_coverage",
)
_PRIMARY_KEYS = (
    ("provider_directory_endpoint_dataset", ("dataset_id",)),
    ("provider_directory_dataset_resource", ("dataset_id", "resource_type", "resource_id")),
    ("provider_directory_entity_source_binding", ("source_id", "resource_type", "resource_id")),
    ("provider_directory_entity_release_evidence", ("source_id", "resource_type", "resource_id", "release_id")),
    ("provider_directory_insurance_network_source_binding", ("source_id", "resource_id")),
    (
        "provider_directory_insurance_network_plan_evidence",
        ("source_id", "release_id", "network_resource_id", "insurance_plan_resource_id"),
    ),
    ("provider_directory_cms_npd_resource_witness", ("dataset_id", "resource_type", "resource_id")),
    (
        "provider_directory_cms_npd_relationship",
        ("dataset_id", "resource_type", "resource_id", "reference_field", "parent_ordinal", "reference_ordinal"),
    ),
    ("provider_directory_cms_npd_relationship_receipt", ("dataset_id",)),
    ("provider_directory_cms_candidate_coverage", ("dataset_id", "release_id", "proof_version")),
    ("provider_directory_cms_serving_coverage", ("dataset_id", "release_id")),
)
# Source coverage keys include proof version; retained index identities stay unchanged.
_COPY_KEYS = dict(_PRIMARY_KEYS)
_COPY_KEYS["provider_directory_cms_serving_coverage"] = ("dataset_id", "release_id", "proof_version")
_LOOKUP_INDEXES = (
    ("cms_epoch_entity_site", "provider_directory_entity_source_binding", ("site_id",)),
    ("cms_epoch_entity_organization", "provider_directory_entity_source_binding", ("organization_id",)),
    (
        "cms_epoch_plan_release",
        "provider_directory_insurance_network_plan_evidence",
        ("source_id", "release_id", "insurance_plan_resource_id"),
    ),
    ("cms_epoch_network_identity", "provider_directory_insurance_network_source_binding", ("network_id",)),
    (
        "cms_epoch_relationship_target",
        "provider_directory_cms_npd_relationship",
        ("dataset_id", "target_type", "target_resource_id"),
    ),
)
_ORIGIN_FIELDS = (
    "schema_name",
    "source_id",
    "endpoint_id",
    "dataset_id",
    "dataset_sha256",
    "release_id",
    "resource_table_oid",
    "alias_scope",
    "as_of",
)
_COPY_HEADER = b"PGCOPY\n\xff\r\n\0" + struct.pack("!ii", 0, 0)
MAX_EPOCH_BYTES = 4 * 1024**3
MAX_ROW_BYTES = 16 * 1024**2
MAX_RECEIPT_BYTES = 65536
MAX_COPY_BATCH_ROWS = 4096
MAX_COPY_BATCH_BYTES = 64 * 1024**2


class FHIRSourceEpochError(ValueError):
    """A retained CMS epoch is unavailable; messages never include source values."""


def _fail():
    raise FHIRSourceEpochError("fhir_source_epoch_unavailable")


def _hash(value):
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False, default=str).encode()
    ).hexdigest()


def _sha(value):
    return type(value) is str and len(value) == 64 and all(character in "0123456789abcdef" for character in value)


@dataclass(frozen=True)
class RetainedCMSFHIRSourceEpoch:
    """Closed physical custody paired with the unchanged nine origin coordinates."""

    origin_coordinates: tuple
    epoch_id: UUID
    schema_name: str
    schema_oid: int
    relation_oids: tuple[tuple[str, int], ...]
    owner_role: str
    runtime_roles: tuple[str, ...]
    admission_sha256: str
    metadata_sha256: str
    content_sha256: str
    catalog_sha256: str

    def as_dict(self):
        """Return a bounded closed wire object without a recursive source pin."""
        document_by_field = asdict(self)
        document_by_field["epoch_id"] = str(self.epoch_id)
        document_by_field["origin_coordinates"] = list(self.origin_coordinates)
        document_by_field["relation_oids"] = [list(pair) for pair in self.relation_oids]
        document_by_field["runtime_roles"] = list(self.runtime_roles)
        return document_by_field


def _origin(source_pin):
    from process.network_fhir_membership_source import PinnedFHIRMembershipSource

    if (
        type(source_pin) is not PinnedFHIRMembershipSource
        or source_pin.source_id != "cms-npd"
        or source_pin.custody_owner_role is not None
    ):
        _fail()
    return tuple(getattr(source_pin, name) for name in _ORIGIN_FIELDS)


def _decode_epoch(epoch_input):
    """Decode closed canonical wire arrays without coercing scalar strings."""
    value = epoch_input
    if type(value) is dict:
        if set(value) != set(RetainedCMSFHIRSourceEpoch.__dataclass_fields__):
            _fail()
        if (
            type(value["epoch_id"]) is not str
            or type(value["origin_coordinates"]) is not list
            or type(value["relation_oids"]) is not list
            or type(value["runtime_roles"]) is not list
            or any(type(pair) is not list or len(pair) != 2 for pair in value["relation_oids"])
        ):
            _fail()
        try:
            if str(UUID(value["epoch_id"])) != value["epoch_id"]:
                _fail()
            value = RetainedCMSFHIRSourceEpoch(
                **{
                    **value,
                    "epoch_id": UUID(value["epoch_id"]),
                    "origin_coordinates": tuple(value["origin_coordinates"]),
                    "relation_oids": tuple(tuple(pair) for pair in value["relation_oids"]),
                    "runtime_roles": tuple(value["runtime_roles"]),
                }
            )
        except TypeError, ValueError, AttributeError:
            _fail()
    return value


def validate_retained_cms_fhir_source_epoch(epoch_input):
    """Validate exact scalar/tuple fields and canonical namespace identity."""
    from process.network_fhir_membership_source import PinnedFHIRMembershipSource

    epoch = _decode_epoch(epoch_input)
    if (
        type(epoch) is not RetainedCMSFHIRSourceEpoch
        or type(epoch.origin_coordinates) is not tuple
        or len(epoch.origin_coordinates) != 9
        or type(epoch.runtime_roles) is not tuple
        or type(epoch.relation_oids) is not tuple
        or any(type(pair) is not tuple or len(pair) != 2 for pair in epoch.relation_oids)
    ):
        _fail()
    try:
        _origin(PinnedFHIRMembershipSource(*epoch.origin_coordinates))
        _identifier(epoch.owner_role)
        for role in epoch.runtime_roles:
            _identifier(role)
    except TypeError, ValueError:
        _fail()
    if (
        type(epoch.epoch_id) is not UUID
        or not epoch.epoch_id.int
        or epoch.schema_name != "registry_cms_epoch_" + epoch.epoch_id.hex
        or type(epoch.schema_oid) is not int
        or not 0 < epoch.schema_oid <= 4294967295
        or type(epoch.runtime_roles) is not tuple
        or not 1 <= len(epoch.runtime_roles) <= 64
        or tuple(sorted(set(epoch.runtime_roles))) != epoch.runtime_roles
        or epoch.owner_role in epoch.runtime_roles
        or type(epoch.relation_oids) is not tuple
        or tuple(name for name, _ in epoch.relation_oids) != tuple(sorted(_TABLES))
        or any(type(oid) is not int or not 0 < oid <= 4294967295 for _, oid in epoch.relation_oids)
        or any(
            not _sha(getattr(epoch, field))
            for field in ("admission_sha256", "metadata_sha256", "content_sha256", "catalog_sha256")
        )
    ):
        _fail()
    if len(json.dumps(epoch.as_dict(), sort_keys=True, separators=(",", ":")).encode()) > MAX_RECEIPT_BYTES:
        _fail()
    return epoch


async def _copy_json_rows(connection, query, parameters, path):
    """Spool one native COPY stream with a hard byte bound and exact cleanup ownership."""
    with path.open("wb") as output:

        async def write(chunk):
            """Append only while the owned spool remains bounded."""
            if output.tell() + len(chunk) > MAX_EPOCH_BYTES:
                _fail()
            output.write(chunk)

        await connection.copy_from_query(query, *parameters, output=write, format="binary")


def _json_copy_rows(path):
    """Read bounded one-field binary COPY rows without retaining the full source."""
    with path.open("rb") as source:
        if source.read(19) != _COPY_HEADER:
            _fail()
        while True:
            count = source.read(2)
            if count == b"\xff\xff":
                if source.read(1):
                    _fail()
                return
            if count != b"\0\1":
                _fail()
            length_bytes = source.read(4)
            if len(length_bytes) != 4:
                _fail()
            length = struct.unpack("!i", length_bytes)[0]
            if not 0 <= length <= MAX_ROW_BYTES:
                _fail()
            payload = source.read(length)
            if len(payload) != length:
                _fail()
            yield json.loads(payload)


def _verify_resource_copy(path, source_pin, metadata, evidence_run_id):
    """Reuse the importer's semantic and raw projection validators once at admission."""
    cms = import_module("process.provider_directory_cms_npd")
    fhir = import_module("process.provider_directory_fhir")

    contract = metadata["resource_hash_contract"]
    candidate = SimpleNamespace(
        semantic_projection_as_of=metadata["semantic_projection_as_of"], acquisition_root_run_id=evidence_run_id
    )
    digest, count, previous = hashlib.sha256(), 0, None
    for row_by_field in _json_copy_rows(path):
        identity = (row_by_field["resource_type"], row_by_field["resource_id"], row_by_field["payload_hash"])
        if previous == identity[:2]:
            _fail()
        fhir._assert_endpoint_dataset_resource_payload_hash(row_by_field, resource_hash_contract=contract)
        raw = row_by_field["raw_payload_json"]
        if raw.get("resourceType") != identity[0] or raw.get("id") != identity[1]:
            _fail()
        raw_hash = hashlib.sha256(
            json.dumps(raw, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()
        ).hexdigest()
        if raw_hash != row_by_field["raw_payload_sha256"]:
            _fail()
        model, parsed = cms._parse_batch_row(fhir, raw, candidate)
        retained = fhir._endpoint_dataset_resource_rows(
            model, [parsed], dataset_id=source_pin.dataset_id, resource_hash_contract=contract
        )
        if len(retained) != 1 or retained[0]["payload_hash"] != identity[2]:
            _fail()
        if count:
            digest.update(b"\n")
        digest.update(fhir._stable_identity_json(identity).encode())
        count += 1
        previous = identity[:2]
    if digest.hexdigest() != source_pin.dataset_sha256:
        _fail()
    return count


async def _require_admitted_candidate(connection, source_pin):
    """Read immutable admission scalars and validate complete source-scoped witnesses as sets."""
    namespace = _identifier(source_pin.schema_name)
    proof_record = await connection.fetchrow(
        f"""SELECT d.content_proof_admission_sha256 AS admission_sha256,
      d.publication_metadata_sha256 AS metadata_sha256,d.resource_count,c.relationship_count,
      d.publication_metadata_summary_json->'network_bindings' AS network_bindings
      FROM {namespace}.provider_directory_endpoint_dataset d
      JOIN {namespace}.provider_directory_cms_candidate_coverage c ON c.dataset_id=d.dataset_id
        AND c.endpoint_id=d.endpoint_id AND c.release_id=$4 AND c.dataset_hash=d.dataset_hash
        AND c.proof_version=$5 AND c.admission_sha256=d.content_proof_admission_sha256
        AND c.metadata_sha256=d.publication_metadata_sha256
      JOIN {namespace}.provider_directory_cms_npd_relationship_receipt r ON r.dataset_id=d.dataset_id
        AND r.release_id=$4 AND r.projection_contract='cms-npd-reference-ledger-v1'
        AND r.relationship_count=c.relationship_count
      WHERE d.dataset_id=$1 AND d.endpoint_id=$2 AND d.dataset_hash=$3
        AND d.status IN ('validated','published','superseded')
        AND d.publication_metadata_summary_json->'source_ids'='["cms-npd"]'::jsonb
        AND d.publication_metadata_summary_json->'source_release'->>'vector_sha256'=$4
        AND d.content_proof_admission_version=1 AND d.content_proof_admission_kind='generic'
        AND d.content_proof_resource_types=ARRAY['Endpoint','HealthcareService','InsurancePlan','Location',
          'Organization','OrganizationAffiliation','Practitioner','PractitionerRole']::varchar[]""",
        source_pin.dataset_id,
        source_pin.endpoint_id,
        source_pin.dataset_sha256,
        source_pin.release_id,
        2,
    )
    if proof_record is None:
        _fail()
    valid = await connection.fetchval(
        f"""SELECT
      (SELECT count(*) FROM {namespace}.provider_directory_dataset_resource WHERE dataset_id=$1)=$3
      AND (SELECT count(*) FROM {namespace}.provider_directory_cms_npd_relationship WHERE dataset_id=$1)=$4
      AND NOT EXISTS(SELECT 1 FROM {namespace}.provider_directory_dataset_resource r
        LEFT JOIN {namespace}.provider_directory_cms_npd_resource_witness w
          USING(dataset_id,resource_type,resource_id)
        WHERE r.dataset_id=$1 AND (w.source_id IS DISTINCT FROM 'cms-npd' OR w.release_id IS DISTINCT FROM $2
          OR w.normalized_payload_hash IS DISTINCT FROM r.payload_hash
          OR (r.acquired_resource_sha256 IS NOT NULL AND w.raw_payload_sha256 IS DISTINCT FROM r.acquired_resource_sha256)))
      AND NOT EXISTS(SELECT 1 FROM {namespace}.provider_directory_dataset_resource r
        JOIN {namespace}.provider_directory_cms_npd_resource_witness w USING(dataset_id,resource_type,resource_id)
        LEFT JOIN {namespace}.provider_directory_entity_release_evidence e ON e.source_id='cms-npd'
          AND e.resource_type=r.resource_type AND e.resource_id=r.resource_id AND e.release_id=$2
        LEFT JOIN {namespace}.provider_directory_entity_source_binding b ON b.source_id='cms-npd'
          AND b.resource_type=r.resource_type AND b.resource_id=r.resource_id
        WHERE r.dataset_id=$1 AND r.resource_type IN ('Organization','Location')
          AND (e.payload_sha256 IS DISTINCT FROM w.raw_payload_sha256 OR b.resource_id IS NULL))""",
        source_pin.dataset_id,
        source_pin.release_id,
        proof_record["resource_count"],
        proof_record["relationship_count"],
    )
    if valid is not True:
        _fail()
    return proof_record


async def _admit_origin(connection, source_pin, admission_sha256, metadata_sha256):
    """Reconstruct metadata and complete normalized/raw correspondence before any copy."""
    from process.provider_directory_admission_backfill import _locked_dataset_row, _validated_row_seal

    namespace = _identifier(source_pin.schema_name)
    dataset_ref = namespace + '."provider_directory_endpoint_dataset"'
    dataset_record = await _locked_dataset_row(connection, dataset_ref, source_pin.dataset_id)
    if (
        dataset_record is None
        or dataset_record["dataset_hash"] != source_pin.dataset_sha256
        or dataset_record["endpoint_id"] != source_pin.endpoint_id
    ):
        _fail()
    if (
        await connection.fetchval(
            "SELECT to_regclass($1)::oid::bigint", namespace + '."provider_directory_dataset_resource"'
        )
        != source_pin.resource_table_oid
    ):
        _fail()
    seal = await _validated_row_seal(connection, dataset_ref, dataset_record)
    if (seal.proof_sha256, seal.metadata_sha256) != (admission_sha256, metadata_sha256):
        _fail()
    await _require_admitted_candidate(connection, source_pin)
    query = f"""SELECT jsonb_build_object('resource_type',r.resource_type,'resource_id',r.resource_id,
      'payload_hash',r.payload_hash,'payload_json',r.payload_json::jsonb,'acquired_resource_sha256',r.acquired_resource_sha256,
      'raw_payload_json',w.raw_payload_json,'raw_payload_sha256',w.raw_payload_sha256)::text
      FROM {namespace}.provider_directory_dataset_resource r LEFT JOIN {namespace}.provider_directory_cms_npd_resource_witness w
      USING(dataset_id,resource_type,resource_id) WHERE r.dataset_id=$1 ORDER BY r.resource_type COLLATE "C",r.resource_id COLLATE "C"
    """
    with tempfile.TemporaryDirectory(prefix="cms-epoch-admission-") as temporary:
        path = Path(temporary) / "resources.copy"
        await _copy_json_rows(connection, query, (source_pin.dataset_id,), path)
        count = await drain_operation(
            asyncio.to_thread(
                _verify_resource_copy, path, source_pin, seal.metadata_summary, dataset_record["evidence_run_id"]
            ),
            preserve_cancellation=True,
        )
    if count != dataset_record["resource_count"]:
        _fail()
    return seal


def _scope(table, namespace):
    """Copy only the exact dataset, release and resource-bound stable identity closure."""
    if table in _TABLES[:2] or table in _TABLES[6:]:
        return "dataset_id=$1"
    if table == "provider_directory_entity_source_binding":
        return f"source_id='cms-npd' AND EXISTS(SELECT 1 FROM {namespace}.provider_directory_dataset_resource r WHERE r.dataset_id=$1 AND (r.resource_type,r.resource_id)=(head.resource_type,head.resource_id))"
    if table == "provider_directory_entity_release_evidence":
        return f"source_id='cms-npd' AND release_id=$2 AND EXISTS(SELECT 1 FROM {namespace}.provider_directory_dataset_resource r WHERE r.dataset_id=$1 AND (r.resource_type,r.resource_id)=(head.resource_type,head.resource_id))"
    if table == "provider_directory_insurance_network_source_binding":
        return f"source_id='cms-npd' AND EXISTS(SELECT 1 FROM {namespace}.provider_directory_dataset_resource r WHERE r.dataset_id=$1 AND r.resource_type='Organization' AND r.resource_id=head.resource_id)"
    return f"source_id='cms-npd' AND release_id=$2 AND EXISTS(SELECT 1 FROM {namespace}.provider_directory_dataset_resource r WHERE r.dataset_id=$1 AND r.resource_type='InsurancePlan' AND r.resource_id=head.insurance_plan_resource_id)"


async def _content(connection, schema):
    """Fold native bounded row-hash chunks for the fixed eleven-relation epoch."""
    receipts = []
    namespace = _identifier(schema)
    for table in sorted(_TABLES):
        chunks = await connection.fetch(f"""WITH hashed AS (
          SELECT encode(sha256(convert_to(to_jsonb(row_value)::text,'UTF8')),'hex') AS sha FROM {namespace}.{_identifier(table)} row_value
        ), ordered AS (SELECT sha,row_number() OVER(ORDER BY sha COLLATE "C")-1 AS ordinal FROM hashed)
        SELECT ordinal/8192 AS ordinal,count(*) AS count,encode(sha256(convert_to(string_agg(sha,'' ORDER BY ordinal),'UTF8')),'hex') AS sha
        FROM ordered GROUP BY ordinal/8192 ORDER BY ordinal/8192""")
        digest, count = hashlib.sha256(b"cms-epoch-row-chunks-v1\0"), 0
        for index, chunk in enumerate(chunks):
            if chunk["ordinal"] != index or not 0 < chunk["count"] <= 8192:
                _fail()
            digest.update(struct.pack("!I", chunk["count"]))
            digest.update(bytes.fromhex(chunk["sha"]))
            count += chunk["count"]
        receipts.append((table, count, digest.hexdigest()))
    return _hash(receipts)


async def _catalog_relations(connection, schema):
    """Read native layout, keys and exact index identities as one bounded catalog set."""
    original_search_path = await connection.fetchval("SELECT current_setting('search_path')")
    # Deparsed expressions and operator classes must not depend on the reader's path.
    async with connection.transaction():
        await connection.execute("SET LOCAL search_path TO pg_catalog")
        relations = await connection.fetch(
            """SELECT c.oid::bigint,c.relname,c.relkind,c.relpersistence,c.relowner::bigint,c.relhastriggers,
      c.relrowsecurity,c.relforcerowsecurity,pg_relation_filenode(c.oid)::bigint AS filenode,n.oid::bigint AS schema_oid,n.nspowner::bigint,
      EXISTS(SELECT 1 FROM pg_inherits WHERE inhrelid=c.oid OR inhparent=c.oid) AS inherited,
      (SELECT jsonb_agg(jsonb_build_array(a.attname,a.atttypid,a.atttypmod,a.attnotnull,a.attgenerated,a.attidentity) ORDER BY a.attnum)
       FROM pg_attribute a WHERE a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped)::text AS columns,
      EXISTS(SELECT 1 FROM pg_attrdef d WHERE d.adrelid=c.oid) AS defaults,
      (SELECT jsonb_agg(jsonb_build_object('oid',i.indexrelid::bigint,'name',ix.relname,'method',am.amname,
        'owner_oid',ix.relowner::bigint,'filenode',pg_relation_filenode(ix.oid)::bigint,'unique',i.indisunique,'primary',i.indisprimary,
        'valid',i.indisvalid,'ready',i.indisready,'live',i.indislive,'key_count',i.indnkeyatts,'attribute_count',i.indnatts,
        'expressions',i.indexprs IS NOT NULL,'predicate',i.indpred IS NOT NULL,'options',i.indoption::text,
        'columns',(SELECT jsonb_agg(a.attname ORDER BY key.ordinal) FROM unnest(i.indkey::smallint[]) WITH ORDINALITY key(attnum,ordinal)
          LEFT JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=key.attnum),
        'definition',pg_get_indexdef(i.indexrelid)) ORDER BY ix.relname)
        FROM pg_index i JOIN pg_class ix ON ix.oid=i.indexrelid JOIN pg_am am ON am.oid=ix.relam
        WHERE i.indrelid=c.oid)::text AS indexes,
      (SELECT jsonb_agg(jsonb_build_array(k.oid,k.contype,k.conindid,pg_get_constraintdef(k.oid)) ORDER BY k.oid)
        FROM pg_constraint k WHERE k.conrelid=c.oid AND k.contype<>'n')::text AS constraints
      FROM pg_namespace n JOIN pg_class c ON c.relnamespace=n.oid WHERE n.nspname=$1 AND c.relkind IN ('r','p','v','m','f','S') ORDER BY c.relname""",
            schema,
        )
        await connection.execute("SELECT pg_catalog.set_config('search_path',$1,true)", original_search_path)
        return relations


async def _catalog(connection, schema, owner_role, runtime_roles):
    """Require trigger-free heaps, exact local OIDs and SELECT-only native capabilities."""
    owner = await connection.fetchrow("SELECT * FROM pg_roles WHERE rolname=$1", owner_role)
    if not _protected_owner(owner):
        _fail()
    namespace = _identifier(schema)
    await connection.execute(
        "LOCK TABLE "
        + ",".join(namespace + "." + _identifier(name) for name in _TABLES)
        + " IN ACCESS SHARE MODE NOWAIT"
    )
    relations = await _catalog_relations(connection, schema)
    if tuple(relation_record["relname"] for relation_record in relations) != tuple(sorted(_TABLES)) or any(
        relation_record["relkind"] != b"r"
        or relation_record["relpersistence"] != b"p"
        or relation_record["relowner"] != owner["oid"]
        or relation_record["nspowner"] != owner["oid"]
        or any(
            relation_record[field]
            for field in ("relhastriggers", "relrowsecurity", "relforcerowsecurity", "inherited", "defaults")
        )
        for relation_record in relations
    ):
        _fail()
    _require_index_profiles(relations, owner["oid"])
    oids = tuple((relation_record["relname"], relation_record["oid"]) for relation_record in relations)
    unsafe = await connection.fetchval(
        """WITH configured AS (SELECT r.* FROM unnest($1::text[]) name LEFT JOIN pg_roles r ON r.rolname=name), acl AS (
      SELECT a.* FROM pg_namespace n CROSS JOIN LATERAL aclexplode(coalesce(n.nspacl,acldefault('n',n.nspowner))) a WHERE n.oid=$2::oid
      UNION ALL SELECT a.* FROM pg_class c CROSS JOIN LATERAL aclexplode(coalesce(c.relacl,acldefault('r',c.relowner))) a WHERE c.oid=ANY($4::oid[])
      UNION ALL SELECT a.* FROM pg_attribute att CROSS JOIN LATERAL aclexplode(att.attacl) a WHERE att.attrelid=ANY($4::oid[])
    ) SELECT EXISTS(SELECT 1 FROM configured r WHERE r.oid IS NULL OR r.rolsuper OR r.rolcreaterole OR r.rolcreatedb OR r.rolreplication OR r.rolbypassrls
      OR pg_has_role(r.oid,$3::oid,'MEMBER') OR pg_has_role(r.oid,$3::oid,'SET') OR has_parameter_privilege(r.oid,'session_replication_role','SET')
      OR EXISTS(SELECT 1 FROM pg_roles elevated WHERE (elevated.rolsuper OR elevated.rolcreaterole)
        AND pg_has_role(r.oid,elevated.oid,'MEMBER')))
      OR EXISTS(SELECT 1 FROM acl WHERE grantee<>$3::oid AND (is_grantable OR grantee NOT IN (SELECT oid FROM configured) OR privilege_type NOT IN ('USAGE','SELECT')))
      OR EXISTS(SELECT 1 FROM configured r WHERE NOT has_schema_privilege(r.oid,$2::oid,'USAGE'))
      OR EXISTS(SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname=$5)
      OR EXISTS(SELECT 1 FROM pg_proc p WHERE p.prosecdef AND p.prorettype NOT IN ('trigger'::regtype,'event_trigger'::regtype)
        AND EXISTS(SELECT 1 FROM pg_roles definer WHERE definer.oid=p.proowner
          AND (definer.rolsuper OR definer.rolcreaterole OR pg_has_role(definer.oid,$3::oid,'MEMBER') OR pg_has_role(definer.oid,$3::oid,'SET')))
        AND EXISTS(SELECT 1 FROM configured r WHERE has_schema_privilege(r.oid,p.pronamespace,'USAGE') AND has_function_privilege(r.oid,p.oid,'EXECUTE')))""",
        list(runtime_roles),
        relations[0]["schema_oid"],
        owner["oid"],
        [oid for _, oid in oids],
        schema,
    )
    if unsafe:
        _fail()
    return relations[0]["schema_oid"], oids, _hash([dict(relation_record) for relation_record in relations])


def _require_index_profiles(relations, owner_oid):
    """Recognize fixed native keys before binding their exact catalog identities."""
    profiles_by_table = {
        table: [("cms_epoch_pk_" + str(index), keys, True)] for index, (table, keys) in enumerate(_PRIMARY_KEYS)
    }
    for name, table, keys in _LOOKUP_INDEXES:
        profiles_by_table[table].append((name, keys, False))
    for relation_record in relations:
        indexes = json.loads(relation_record["indexes"] or "[]")
        expected_profiles = sorted(profiles_by_table[relation_record["relname"]])
        if len(indexes) != len(expected_profiles):
            _fail()
        for index_record, (name, keys, primary) in zip(indexes, expected_profiles, strict=True):
            if (
                index_record["name"] != name
                or tuple(index_record["columns"]) != keys
                or index_record["method"] != "btree"
                or index_record["owner_oid"] != owner_oid
                or index_record["unique"] is not primary
                or index_record["primary"] is not primary
                or any(index_record[field] is not True for field in ("valid", "ready", "live"))
                or index_record["key_count"] != len(keys)
                or index_record["attribute_count"] != len(keys)
                or index_record["expressions"]
                or index_record["predicate"]
                or index_record["options"] != " ".join("0" for _ in keys)
            ):
                _fail()
        constraints = json.loads(relation_record["constraints"] or "[]")
        if len(constraints) != 1 or constraints[0][1] != "p":
            _fail()


async def _admit_epoch_copy(copy_admission, phase, schema, table, oid):
    """Recheck one batch-owned relation without adding any per-row authority work."""
    if copy_admission is None:
        return
    try:
        await copy_admission(phase, schema, table, oid)
    except Exception:
        raise FHIRSourceEpochError("fhir_source_epoch_copy_admission_refused") from None


async def _epoch_index(connection, schema, table, statement, copy_admission, oid_by_table):
    """Bracket each native index's growth with the original admitted heap OID."""
    oid = oid_by_table.get(table) if oid_by_table is not None else None
    await _admit_epoch_copy(copy_admission, "before_index", schema, table, oid)
    await connection.execute(statement)
    await _admit_epoch_copy(copy_admission, "after_index", schema, table, oid)


async def _create_epoch_indexes(connection, schema, copy_admission=None, oid_by_table=None):
    """Build essential native constraints and exact lookup indexes after bulk loading."""
    namespace = _identifier(schema)
    for index, (table, keys) in enumerate(_PRIMARY_KEYS):
        columns = ",".join(_identifier(column) for column in keys)
        await _epoch_index(
            connection,
            schema,
            table,
            f"ALTER TABLE {namespace}.{_identifier(table)} ADD CONSTRAINT cms_epoch_pk_{index} PRIMARY KEY ({columns})",
            copy_admission,
            oid_by_table,
        )
    for name, table, keys in _LOOKUP_INDEXES:
        columns = ",".join(_identifier(column) for column in keys)
        await _epoch_index(
            connection,
            schema,
            table,
            f"CREATE INDEX {_identifier(name)} ON {namespace}.{_identifier(table)} ({columns})",
            copy_admission,
            oid_by_table,
        )
    for table in _TABLES:
        await connection.execute(f"ANALYZE {namespace}.{_identifier(table)}")


def _epoch_copy_limits(copy_admission):
    """Use positive transport caps independently of signed physical reservations."""
    try:
        rows = getattr(copy_admission, "copy_batch_rows", MAX_COPY_BATCH_ROWS)
        byte_limit = getattr(copy_admission, "copy_batch_bytes", MAX_COPY_BATCH_BYTES)
    except Exception:
        _fail()
    if (
        type(rows) is not int
        or not 0 < rows <= MAX_COPY_BATCH_ROWS
        or type(byte_limit) is not int
        or not 0 < byte_limit <= MAX_COPY_BATCH_BYTES
    ):
        _fail()
    return rows, byte_limit


async def _require_epoch_copy_key(connection, source_pin, table):
    """Require exact existing native primary keys; never index or rewrite parents."""
    keys = _COPY_KEYS[table]
    relation = _identifier(source_pin.schema_name) + "." + _identifier(table)
    has_native_key = await connection.fetchval(
        """SELECT EXISTS(SELECT 1 FROM pg_index i
        JOIN pg_class c ON c.oid=i.indexrelid JOIN pg_am am ON am.oid=c.relam
        WHERE i.indrelid=to_regclass($1) AND i.indisprimary AND i.indisunique
        AND i.indisvalid AND i.indisready AND i.indislive AND i.indimmediate
        AND am.amname='btree' AND i.indnkeyatts=i.indnatts
        AND i.indexprs IS NULL AND i.indpred IS NULL
        AND ARRAY(SELECT a.attname::text FROM unnest(i.indkey::smallint[]) WITH ORDINALITY k(attnum,position)
          JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=k.attnum ORDER BY k.position)=$2::text[]
        AND NOT EXISTS(SELECT 1 FROM unnest(i.indkey::smallint[]) WITH ORDINALITY k(attnum,position)
          JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=k.attnum
          JOIN pg_opclass op ON op.oid=i.indclass[k.position::int-1]
          WHERE NOT a.attnotnull OR a.attisdropped OR i.indoption[k.position::int-1]<>0
            OR i.indcollation[k.position::int-1]<>a.attcollation OR NOT op.opcdefault
            OR op.opcnamespace<>'pg_catalog'::regnamespace OR op.opcmethod<>c.relam))""",
        relation,
        list(keys),
    )
    if has_native_key is not True:
        _fail()


def _epoch_copy_range(source_pin, table, last_keys, upper_keys=None):
    """Keep the complete edition closure in every native keyset query."""
    namespace = _identifier(source_pin.schema_name)
    keys = _COPY_KEYS[table]
    key_columns = ",".join("head." + _identifier(key) for key in keys)
    parameters = (source_pin.dataset_id,)
    if table not in _TABLES[:2] + _TABLES[6:] and table not in {_TABLES[2], _TABLES[4]}:
        parameters += (source_pin.release_id,)
    predicate = "(" + _scope(table, namespace) + ")"
    for boundary, comparison in ((last_keys, ">"), (upper_keys, "<=")):
        if boundary is not None:
            placeholders = ",".join("$" + str(len(parameters) + index + 1) for index in range(len(keys)))
            predicate += f" AND ROW({key_columns}){comparison}ROW({placeholders})"
            parameters += tuple(boundary)
    return predicate, parameters


async def _epoch_batch_bounds(connection, source_pin, table, last_keys, row_limit):
    """Read only bounded aggregate and key metadata, never individual source rows."""
    namespace = _identifier(source_pin.schema_name)
    keys = _COPY_KEYS[table]
    columns = ",".join(_identifier(key) for key in keys)
    last_columns = ",".join(_identifier(key) + " AS copy_key_" + str(index) for index, key in enumerate(keys))
    descending = ",".join(_identifier(key) + " DESC" for key in keys)
    predicate, parameters = _epoch_copy_range(source_pin, table, last_keys)
    return await connection.fetchrow(
        f"""WITH batch AS MATERIALIZED (SELECT head.* FROM {namespace}.{_identifier(table)} head
        WHERE {predicate} ORDER BY {columns} LIMIT {row_limit}),
        totals AS (SELECT count(*)::bigint AS row_count,
          COALESCE(sum(octet_length(record_send(batch))::bigint),0)::bigint AS native_bytes FROM batch),
        last_row AS (SELECT {last_columns} FROM batch ORDER BY {descending} LIMIT 1)
        SELECT totals.*,last_row.* FROM totals LEFT JOIN last_row ON TRUE""",
        *parameters,
    )


async def _copy_epoch_batch(connection, source_pin, schema, table, boundaries, copy_admission, oid):
    """Recheck admission after export and bracket exactly one native binary import."""
    last_keys, upper_keys, count, byte_limit = boundaries
    predicate, parameters = _epoch_copy_range(source_pin, table, last_keys, upper_keys)
    namespace = _identifier(source_pin.schema_name)
    columns = ",".join(_identifier(key) for key in _COPY_KEYS[table])

    async def admit_import(phase):
        """Retain original relation identity at both target COPY boundaries."""
        phase = {"before_import": "before_copy_batch", "after_import": "after_copy_batch"}[phase]
        await _admit_epoch_copy(copy_admission, phase, schema, table, oid)

    await _copy_native_batch(
        connection,
        f"SELECT head.* FROM {namespace}.{_identifier(table)} head WHERE {predicate} ORDER BY {columns}",
        parameters,
        schema,
        table,
        count,
        byte_limit=byte_limit,
        import_admission=admit_import,
    )


async def _copy_epoch_rows(connection, source_pin, schema, table, oid, copy_admission):
    """Copy complete compound-key ranges with bounded binary input and no row callbacks."""
    row_limit, byte_limit = _epoch_copy_limits(copy_admission)
    last_keys = None
    while True:
        bounds = await _epoch_batch_bounds(connection, source_pin, table, last_keys, row_limit)
        count = bounds["row_count"]
        if type(count) is not int or not 0 <= count <= row_limit:
            _fail()
        if count == 0:
            return
        if type(bounds["native_bytes"]) is not int or bounds["native_bytes"] < 0:
            _fail()
        upper_keys = tuple(bounds["copy_key_" + str(index)] for index in range(len(_COPY_KEYS[table])))
        if any(key is None for key in upper_keys) or upper_keys == last_keys:
            _fail()
        if bounds["native_bytes"] + 21 > byte_limit:
            if count == 1:
                _fail()
            row_limit = max(1, count // 2)
            continue
        try:
            await _copy_epoch_batch(
                connection, source_pin, schema, table, (last_keys, upper_keys, count, byte_limit), copy_admission, oid
            )
        except _CopyBatchTooLarge:
            if count == 1:
                _fail()
            row_limit = max(1, count // 2)
            continue
        last_keys = upper_keys


async def _copy_epoch(connection, source_pin, schema, owner_role, runtime_roles, copy_admission=None):
    """Create only task-owned native tables, without original functions or row hooks."""
    namespace = _identifier(source_pin.schema_name)
    await connection.execute(f"CREATE SCHEMA {_identifier(schema)}")
    oid_by_table = {}
    for table in _TABLES:
        await _require_epoch_copy_key(connection, source_pin, table)
        table_ref = _identifier(schema) + "." + _identifier(table)
        await connection.execute(f"CREATE TABLE {table_ref} (LIKE {namespace}.{_identifier(table)} INCLUDING STORAGE)")
        oid = None
        if copy_admission is not None:
            oid = await connection.fetchval(
                "SELECT oid::bigint FROM pg_class WHERE oid=to_regclass($1) AND relkind='r' AND relpersistence='p'",
                table_ref,
            )
            if oid is None:
                _fail()
            oid_by_table[table] = oid
        await _admit_epoch_copy(copy_admission, "created", schema, table, oid)
        await _admit_epoch_copy(copy_admission, "before_insert", schema, table, oid)
        await _copy_epoch_rows(connection, source_pin, schema, table, oid, copy_admission)
        await _admit_epoch_copy(copy_admission, "after_insert", schema, table, oid)
    await _create_epoch_indexes(connection, schema, copy_admission, oid_by_table)
    await connection.execute(f"ALTER SCHEMA {_identifier(schema)} OWNER TO {_identifier(owner_role)}")
    for table in _TABLES:
        table_ref = _identifier(schema) + "." + _identifier(table)
        await connection.execute(f"ALTER TABLE {table_ref} OWNER TO {_identifier(owner_role)}")
    for role in runtime_roles:
        await connection.execute(f"GRANT USAGE ON SCHEMA {_identifier(schema)} TO {_identifier(role)}")
        await connection.execute(f"GRANT SELECT ON ALL TABLES IN SCHEMA {_identifier(schema)} TO {_identifier(role)}")


def _validate_capture_options(
    epoch_id, owner_role, runtime_roles, expected_admission_sha256, expected_metadata_sha256, copy_admission
):
    """Preserve closed independent admission and custody inputs before creating heaps."""
    _epoch_copy_limits(copy_admission)
    if (
        type(epoch_id) is not UUID
        or not epoch_id.int
        or type(runtime_roles) is not tuple
        or not _sha(expected_admission_sha256)
        or not _sha(expected_metadata_sha256)
        or (copy_admission is not None and not callable(copy_admission))
    ):
        _fail()
    _identifier(owner_role)
    for role in runtime_roles:
        _identifier(role)


async def capture_retained_cms_fhir_source_epoch(
    connection,
    source_pin,
    *,
    epoch_id,
    owner_role,
    runtime_roles,
    expected_admission_sha256,
    expected_metadata_sha256,
    copy_admission=None,
):
    """Admit one exact edition and atomically freeze its task-owned native copy."""
    origin = _origin(source_pin)
    _validate_capture_options(
        epoch_id, owner_role, runtime_roles, expected_admission_sha256, expected_metadata_sha256, copy_admission
    )
    schema = "registry_cms_epoch_" + epoch_id.hex
    await _require_transaction(connection)
    try:
        async with connection.transaction():
            if await connection.fetchval("SELECT to_regnamespace($1)", schema) is not None:
                _fail()
            namespace = _identifier(source_pin.schema_name)
            await connection.execute(
                "LOCK TABLE "
                + ",".join(namespace + "." + _identifier(name) for name in _TABLES)
                + " IN ACCESS SHARE MODE NOWAIT"
            )
            await _admit_origin(connection, source_pin, expected_admission_sha256, expected_metadata_sha256)
            await _copy_epoch(connection, source_pin, schema, owner_role, runtime_roles, copy_admission)
            schema_oid, oids, catalog = await _catalog(connection, schema, owner_role, runtime_roles)
            content = await _content(connection, schema)
            epoch = validate_retained_cms_fhir_source_epoch(
                RetainedCMSFHIRSourceEpoch(
                    origin,
                    epoch_id,
                    schema,
                    schema_oid,
                    oids,
                    owner_role,
                    runtime_roles,
                    expected_admission_sha256,
                    expected_metadata_sha256,
                    content,
                    catalog,
                )
            )
            comment = _receipt_comment(epoch).replace("'", "''")
            await connection.execute(f"COMMENT ON SCHEMA {_identifier(schema)} IS '{comment}'")
            return epoch
    except FHIRSourceEpochError:
        raise
    except Exception:
        raise FHIRSourceEpochError("fhir_source_epoch_unavailable") from None


def _receipt_comment(epoch):
    """Bind the complete independently admitted receipt in protected native metadata."""
    return "cms-fhir-epoch-v1:" + json.dumps(epoch.as_dict(), sort_keys=True, separators=(",", ":"), allow_nan=False)


async def require_retained_cms_fhir_source_epoch(connection, source_pin, epoch, *, verify_content=True):
    """Revalidate immutable physical custody without consulting a later origin edition."""
    if type(verify_content) is not bool:
        _fail()
    epoch = validate_retained_cms_fhir_source_epoch(epoch)
    if epoch.origin_coordinates != _origin(source_pin):
        _fail()
    await _require_transaction(connection)
    try:
        observed = await _catalog(connection, epoch.schema_name, epoch.owner_role, epoch.runtime_roles)
        if (
            observed != (epoch.schema_oid, epoch.relation_oids, epoch.catalog_sha256)
            or await connection.fetchval("SELECT obj_description($1::oid,'pg_namespace')", epoch.schema_oid)
            != _receipt_comment(epoch)
            or (verify_content and await _content(connection, epoch.schema_name) != epoch.content_sha256)
        ):
            _fail()
        return epoch
    except FHIRSourceEpochError:
        raise
    except Exception:
        raise FHIRSourceEpochError("fhir_source_epoch_unavailable") from None
