# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Application-owned generic SOURCE heaps bound to one existing dataset header.

This module never obtains credentials or substitutes an ambient connection. The
trusted caller supplies the publisher capability and the original transaction.
Only low-volume header controls are locked; payload intake is append-only COPY.
"""

import asyncio
import hashlib
import json
import re
import urllib.parse
from contextlib import asynccontextmanager, nullcontext
from dataclasses import dataclass
from importlib import import_module
from uuid import uuid4

from db.plan_release_storage import storage_identifier, storage_relation
from process import provider_directory_compact_payload as compact_payload
from process import provider_directory_compact_source_storage as compact_storage
from process import provider_directory_timestamp_recovery as timestamp_recovery
from process.native_copy_cleanup import cleanup_interrupted_copy
from process.plan_release_storage_provision import _GRANTEES_SQL
from process.provider_directory_generic_candidate import (
    RESOURCE_COLUMNS,
    WITNESS_COLUMNS,
    GenericCandidateError,
    copy_record_batches,
)
from process.provider_directory_generic_source_policy import source_copy_session, source_owner_transaction
from process.provider_directory_role_merge import is_role_repeat_source, role_order_index_sql, staged_role_rows
from process.provider_directory_source_storage_binding import (
    NamedFHIRSourceStorage,
    named_source_document,
    require_storage_schema,
    storage_document,
)
from process.vsp_directory_contract import (
    OFFICE_CENSUS_UNRESOLVED,
    OFFICE_DOCTORS_INVALID,
    PROFILE_MEMBERSHIP_UNRESOLVED,
    SEARCH_RESPONSE_INVALID,
)
from process.vsp_directory_contract import RESOURCE_TYPES as VSP_RESOURCE_TYPES
from process.vsp_directory_contract import SOURCE_ID as VSP_SOURCE_ID
from process.vsp_directory_store import is_artifact_root_failure

_RETAINED_VSP_CAPTURE_ERRORS = (
    OFFICE_DOCTORS_INVALID,
    SEARCH_RESPONSE_INVALID,
    PROFILE_MEMBERSHIP_UNRESOLVED,
    OFFICE_CENSUS_UNRESOLVED,
)
_POSTAL_START_ERROR = "provider_directory_generic_source_isolated_transaction_required"
_POSTAL_REDUCTION_CONFLICTS = frozenset(
    {
        "provider_directory_resource_payload_conflict",
        "provider_directory_organization_identity_payload_conflict",
        "provider_directory_practitioner_identity_payload_conflict",
    }
)

HEADER = "provider_directory_endpoint_dataset"
RESOURCE = "provider_directory_dataset_resource"
WITNESS = "provider_directory_cms_npd_resource_witness"
RELATIONSHIP = "provider_directory_cms_npd_relationship"
RELATIONS = (
    RESOURCE,
    "provider_directory_dataset_insurance_plan",
    "provider_directory_dataset_network_plan",
    "provider_directory_dataset_affiliation_organization",
    WITNESS,
    RELATIONSHIP,
)
ISOLATED_STORAGE_ROOTS = (HEADER, *RELATIONS)
HANDOFF_CONTROL_ROOTS = (HEADER, "provider_directory_dataset_proof_shard")
CMS_IDENTITY_ROOTS = (
    "provider_directory_entity_source_binding",
    "provider_directory_entity_release_evidence",
    "provider_directory_insurance_network_source_binding",
    "provider_directory_insurance_network_plan_evidence",
)
CMS_RESOURCE_IDENTITY_ROOT = "provider_directory_resource_identity"
AUXILIARY_ROOTS = (*CMS_IDENTITY_ROOTS, CMS_RESOURCE_IDENTITY_ROOT)
CONTRACT = "generic-source-candidate-v2"
ISOLATED_CONTRACT = "generic-source-detached-v1"
NAMED_ISOLATED_CONTRACT = "generic-source-detached-by-name-v1"
NAMED_COMPACT_CONTRACT = "generic-source-detached-by-name-compact-v1"
NAMED_CONTRACTS = (NAMED_ISOLATED_CONTRACT, NAMED_COMPACT_CONTRACT)
CMS_ISOLATED_CONTRACT = "generic-source-cms-detached-v1"
ISOLATED_CONTRACTS = (ISOLATED_CONTRACT, NAMED_ISOLATED_CONTRACT, NAMED_COMPACT_CONTRACT, CMS_ISOLATED_CONTRACT)
_COMPACT_SUFFIXES = compact_storage.TABLE_SUFFIXES
# Autovacuum on a private heap competes with the bulk load and reduce for I/O; statistics come from
# explicit ANALYZE and attached heaps get the default back right before they join their root.
PRIVATE_HEAP_OPTIONS = " WITH (autovacuum_enabled = false)"
_NAME = re.compile(r"pdgs_[0-9a-f]{32}_(?:ri|wi|r|ip|np|ao|w|rl|eb|ee|nb|ne|re|h|p)")
_SUFFIXES = dict(zip((*RELATIONS, *AUXILIARY_ROOTS), ("r", "ip", "np", "ao", "w", "rl", "eb", "ee", "nb", "ne", "re")))
_SUFFIXES.update(zip(HANDOFF_CONTROL_ROOTS, ("h", "p")))
_BINDING_KEYS = {"contract", "identity", "phase", "ordinal", "resets", "tables", "auxiliary", "relationship_sha256"}
HANDOFF_HEADER_FIELDS = (
    "dataset_hash",
    "resource_count",
    "previous_dataset_id",
    "completion_proof_required_version",
    "completion_proof_sha256",
    "content_proof_admission_sha256",
)


@dataclass(frozen=True)
class _StoredCandidate:
    """A native protected header binding, never an ambient caller's role claim."""

    identity: dict
    resource_hash_contract: str
    semantic_projection_as_of: str | None

    @property
    def dataset_id(self):
        """Return the exact dataset in the authenticated protected binding."""
        return self.identity["dataset_id"]

    @property
    def selected_resources(self):
        """Return the immutable declared resource-family scope."""
        return tuple(self.identity["metadata"]["selected_resources"])

    @property
    def source_release(self):
        """Return the immutable source vector without discovering another release."""
        return self.identity["metadata"].get("source_release")


def _require(condition, code):
    if not condition:
        raise GenericCandidateError("provider_directory_generic_source_" + code)


def _json(value):
    decoded = json.loads(value) if isinstance(value, str) else value
    return named_source_document(decoded)


_LEGACY_SHARED_TABLE_FIELDS = {"name", "oid", "owner", "layout_sha256"}


def _binding_json(value, schema):
    """Decode a stored binding; a pre-upgrade shared-schema record names its tables in the configured schema."""
    return _shared_schema_binding(_json(value), schema)


def _shared_schema_binding(binding_by_field, schema):
    """Convert validated legacy table records to qualified names; their catalog tokens are never resolved."""
    if type(binding_by_field) is not dict or binding_by_field.get("contract") != CONTRACT:
        return binding_by_field
    auxiliary = binding_by_field.get("auxiliary")
    groups = [binding_by_field.get("tables"), auxiliary.get("tables") if type(auxiliary) is dict else None]
    groups = [group for group in groups if type(group) is dict]
    table_records = [table for group in groups for table in group.values()]
    if not any(type(table) is dict and set(table) == _LEGACY_SHARED_TABLE_FIELDS for table in table_records):
        return binding_by_field
    storage_identifier(schema)
    for group in groups:
        for root, table in group.items():
            suffix = {"resource_input": "ri", "witness_input": "wi"}.get(root, _SUFFIXES.get(root))
            _require(
                type(table) is dict
                and set(table) == _LEGACY_SHARED_TABLE_FIELDS
                and type(table["name"]) is str
                and _NAME.fullmatch(table["name"]) is not None
                and suffix is not None
                and table["name"].endswith("_" + suffix)
                and all(type(table[token]) is int and table[token] > 0 for token in ("oid", "owner"))
                and type(table["layout_sha256"]) is str
                and re.fullmatch(r"[0-9a-f]{64}", table["layout_sha256"]) is not None,
                "table_binding_changed",
            )
    converted_by_field = {
        **binding_by_field,
        "tables": {
            root: {"schema": schema, "name": table["name"]} for root, table in binding_by_field["tables"].items()
        },
    }
    if type(auxiliary) is dict and type(auxiliary.get("tables")) is dict:
        converted_by_field["auxiliary"] = {
            **auxiliary,
            "tables": {root: {"schema": schema, "name": table["name"]} for root, table in auxiliary["tables"].items()},
        }
    return converted_by_field


def _encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def candidate_identity(directory, candidate):
    """Bind all existing immutable acquisition/hash/source/producer declarations."""
    if type(candidate) is _StoredCandidate:
        return candidate.identity
    _require(type(candidate.dataset_id) is str and 1 <= len(candidate.dataset_id) <= 96, "dataset_invalid")
    return dict(
        dataset_id=candidate.dataset_id,
        endpoint_id=candidate.endpoint_id,
        acquisition_root_run_id=candidate.acquisition_root_run_id,
        metadata=directory._endpoint_dataset_candidate_metadata(candidate),
    )


def _validate_binding(binding_by_field, identity, *, phases):
    """Authenticate the complete closed candidate document before native lookup."""
    _validate_binding_shape(binding_by_field, identity, phases=phases)
    _validate_binding_storage(binding_by_field, is_isolated=binding_by_field["contract"] in ISOLATED_CONTRACTS)
    _validate_binding_tables(binding_by_field, identity)


def _validate_binding_shape(binding_by_field, identity, *, phases):
    """Share the document checks across loading and protected snapshot readers."""
    is_isolated = type(binding_by_field) is dict and binding_by_field.get("contract") in ISOLATED_CONTRACTS
    keys = _BINDING_KEYS | {"storage", "admission"} if is_isolated else _BINDING_KEYS
    if type(binding_by_field) is dict and "payload_conversion" in binding_by_field:
        keys |= {"payload_conversion"}
    if type(binding_by_field) is dict and binding_by_field.get("contract") == NAMED_COMPACT_CONTRACT:
        keys |= {"payload_format"}
        _require(
            binding_by_field.get("payload_format")
            in (compact_payload.FORMAT, compact_payload.SHARED_BODY_FORMAT, compact_payload.COMPACT_V2_FORMAT)
            and binding_by_field.get("phase") in {"loading", "prepared", "ready_for_freeze", "disposed"},
            "compact_phase_or_format_changed",
        )
    _require(
        type(binding_by_field) is dict
        and set(binding_by_field) == keys
        and (
            binding_by_field["contract"] in ISOLATED_CONTRACTS
            if is_isolated
            else binding_by_field["contract"] == CONTRACT
        )
        and type(binding_by_field["identity"]) is dict
        and set(binding_by_field["identity"]) == {"dataset_id", "endpoint_id", "acquisition_root_run_id", "metadata"}
        and type(binding_by_field["identity"]["metadata"]) is dict
        and binding_by_field["identity"] == identity
        and binding_by_field["phase"] in phases
        and (
            binding_by_field["relationship_sha256"] is None
            or type(binding_by_field["relationship_sha256"]) is str
            and re.fullmatch(r"[0-9a-f]{64}", binding_by_field["relationship_sha256"])
        )
        and type(binding_by_field["ordinal"]) is int
        and 0 <= binding_by_field["ordinal"] < 2**63 - 1
        and type(binding_by_field["resets"]) is dict
        and all(
            type(key) is str
            and 1 <= len(key) <= 64
            and type(cutoff) is int
            and 0 <= cutoff <= binding_by_field["ordinal"]
            for key, cutoff in binding_by_field["resets"].items()
        ),
        "binding_changed",
    )
    if "payload_conversion" in binding_by_field:
        from process.provider_directory_compact_source_migration import validate_conversion

        validate_conversion(binding_by_field)


def _validate_binding_storage(binding_by_field, *, is_isolated):
    """Keep detached evidence inseparable from the originally issued namespace."""
    if is_isolated:
        from process.provider_directory_generic_source_policy import bound_publisher

        _validate_isolated_binding_storage(binding_by_field, storage_document(bound_publisher()))
    else:
        _require(binding_by_field["phase"] != "ready_for_freeze", "binding_changed")


def _validate_isolated_binding_storage(binding, expected_storage):
    metadata = binding["identity"]["metadata"]
    _validate_named_storage(expected_storage)
    if binding["contract"] == NAMED_ISOLATED_CONTRACT:
        _require_named_source_identity(binding["identity"])
    if binding["contract"] == NAMED_COMPACT_CONTRACT:
        compact_storage.require_source_identity(binding["identity"], payload_format=binding["payload_format"])
    _require(binding["storage"] == expected_storage, "storage_binding_changed")
    _require(binding["phase"] != "published" and binding["auxiliary"] is None, "isolated_kind_unsupported")
    if binding["contract"] == CMS_ISOLATED_CONTRACT:
        from process.provider_directory_cms_source_handoff import require_cms_handoff_identity

        require_cms_handoff_identity(binding["identity"], expected_storage["catalog_schema"])
        _require(binding["relationship_sha256"] is None, "isolated_cms_relationship_unexpected")
    else:
        _require(
            metadata.get("source_release") is None
            and type(metadata.get("source_ids")) is list
            and "cms-npd" not in metadata["source_ids"],
            "isolated_kind_unsupported",
        )
    if binding["phase"] == "ready_for_freeze":
        validate_handoff_header(binding["admission"])
    else:
        _require(binding["admission"] is None, "admission_changed")


def _validate_named_storage(document):
    _require(
        type(document) is dict and set(document) == {"catalog_schema", "storage_schema"}, "storage_binding_changed"
    )
    NamedFHIRSourceStorage(**document)


def named_source_resources(source_ids):
    """Return registered resource contracts used by retained source transitions."""
    if type(source_ids) not in (list, tuple):
        return None
    if tuple(source_ids) == (VSP_SOURCE_ID,):
        return VSP_RESOURCE_TYPES
    directory = import_module("process.provider_directory_fhir")
    registered_source = directory._stable_source_id(directory.anthem.seed_rows()[0])
    if tuple(source_ids) == (registered_source,):
        return directory.anthem.RESOURCES
    return None


def _require_named_source_identity(identity):
    """Keep ordinary singleton scope and complete registered retained resources."""
    metadata = identity["metadata"]
    _require_ordinary_source_metadata(metadata)
    _require(len(metadata["source_ids"]) == 1, "isolated_kind_unsupported")
    if metadata["source_ids"] == ["delta-dental-national-finder"]:
        from process.delta_dental_directory_producer import (
            METADATA_KEY,
            RESOURCE_TYPES,
            validate_delta_positive_receipt,
        )

        validate_delta_positive_receipt(metadata.get(METADATA_KEY))
        _require(
            metadata["selected_resources"] == list(RESOURCE_TYPES)
            and metadata["expected_resources"] == list(RESOURCE_TYPES),
            "isolated_kind_unsupported",
        )
        return
    resources = named_source_resources(metadata.get("source_ids"))
    if resources is not None and metadata["source_ids"] != [VSP_SOURCE_ID]:
        _require(
            all(
                type(metadata.get(field)) is list
                and len(metadata[field]) == len(resources)
                and all(type(resource) is str for resource in metadata[field])
                and set(metadata[field]) == set(resources)
                for field in ("selected_resources", "expected_resources")
            ),
            "isolated_kind_unsupported",
        )


def validate_handoff_document(document, expected_storage):
    """Check data against issued coordinates; this grants no read or publication rights."""
    _require(type(document) is dict and document.get("contract") in ISOLATED_CONTRACTS, "isolated_storage_required")
    _validate_binding_shape(document, document.get("identity"), phases=("ready_for_freeze",))
    _validate_isolated_binding_storage(document, expected_storage)
    _validate_binding_tables(document, document["identity"])


def _validate_binding_tables(binding_by_field, identity):
    """Validate the complete logical table group and its schema-qualified names."""
    _require(type(binding_by_field["tables"]) is dict, "tables_changed")
    if binding_by_field["contract"] == NAMED_COMPACT_CONTRACT:
        compact_storage.validate_tables(binding_by_field)
        return
    phase = binding_by_field["phase"]
    expected = set() if phase in {"published", "disposed", "ready_for_freeze"} else {"resource_input", "witness_input"}
    if phase in {"prepared", "published", "ready_for_freeze"}:
        expected.update(RELATIONS)
    if phase == "ready_for_freeze":
        expected.update(HANDOFF_CONTROL_ROOTS)
    _require(set(binding_by_field["tables"]) == expected, "tables_changed")
    references = list(binding_by_field["tables"].values()) + list(
        _auxiliary_tables(binding_by_field, identity).values()
    )
    names = []
    for table in references:
        _require(type(table) is dict and set(table) == {"schema", "name"}, "table_binding_changed")
        storage_identifier(table["schema"])
        storage_identifier(table["name"])
        if "storage" in binding_by_field:
            _require(table["schema"] == binding_by_field["storage"]["storage_schema"], "table_binding_changed")
        names.append((table["schema"], table["name"]))
    _require(len(set(names)) == len(names), "table_binding_changed")


HANDOFF_METADATA_PROJECTION = (
    "CASE WHEN (d.publication_metadata_json->'import_completion') IS NOT NULL "
    "THEN jsonb_build_object('import_completion',d.publication_metadata_json->'import_completion') "
    "END AS publication_metadata_json"
)


def handoff_header(header):
    """Bind optional partial acceptance into the same logical handoff as its data."""
    admission_by_field = {field: header[field] for field in HANDOFF_HEADER_FIELDS}
    metadata = _json(header.get("publication_metadata_json"))
    if isinstance(metadata, dict) and "import_completion" in metadata:
        admission_by_field["import_completion"] = metadata["import_completion"]
    return admission_by_field


def validate_handoff_header(seal):
    """Keep complete content/completion evidence for revalidation after writer closure."""
    _require(
        type(seal) is dict and set(seal) in (set(HANDOFF_HEADER_FIELDS), {*HANDOFF_HEADER_FIELDS, "import_completion"}),
        "admission_changed",
    )
    if "import_completion" in seal:
        from process.import_completion_policy import validate_completion_receipt

        receipt = seal["import_completion"]
        _require(type(receipt) is dict and isinstance(receipt.get("request"), dict), "admission_changed")
        validate_completion_receipt(
            receipt,
            retained_counts=receipt["request"].get("retained_counts"),
            content_hash=seal["dataset_hash"],
        )
    from process.provider_directory_fhir_subset_canonical import SERVER_ISSUED_SUBSET_REQUIRED_VERSION

    completion_version, completion_hash = seal["completion_proof_required_version"], seal["completion_proof_sha256"]
    completion_valid = (
        completion_version is None
        and completion_hash is None
        or (
            type(completion_version) is int
            and completion_version == SERVER_ISSUED_SUBSET_REQUIRED_VERSION
            and type(completion_hash) is str
            and re.fullmatch(r"[0-9a-f]{64}", completion_hash)
        )
    )
    _require(
        all(
            type(seal[key]) is str and re.fullmatch(r"[0-9a-f]{64}", seal[key])
            for key in ("dataset_hash", "content_proof_admission_sha256")
        )
        and type(seal["resource_count"]) is int
        and 0 <= seal["resource_count"] < 2**63
        and completion_valid
        and (
            seal["previous_dataset_id"] is None
            or type(seal["previous_dataset_id"]) is str
            and 1 <= len(seal["previous_dataset_id"]) <= 96
        ),
        "admission_changed",
    )


def validate_handoff_completion(header):
    """Apply the two legacy callable CHECKs without installing them on private heaps."""
    from process.provider_directory_fhir_subset_canonical import validate_subset_completion_proof_pair

    validate_handoff_header(handoff_header(header))
    from process.provider_directory_import_completion import validate_metadata

    validate_metadata(header)
    _validate_handoff_recovery_completion(header)
    metadata = _json(header.get("publication_metadata_json"))
    if type(metadata) is dict and metadata.get("source_ids") == ["delta-dental-national-finder"]:
        from process.delta_dental_directory_producer import METADATA_KEY, validate_delta_positive_receipt

        validate_delta_positive_receipt(metadata.get(METADATA_KEY), resource_count=header["resource_count"])
    proof = _json(header["completion_proof_json"])
    if header["completion_proof_required_version"] is None:
        _require(proof is None, "completion_proof_unexpected")
        return
    try:
        proof, _ = validate_subset_completion_proof_pair(proof, header["completion_proof_sha256"])
    except ValueError as error:
        raise GenericCandidateError("provider_directory_generic_source_completion_proof_invalid") from error
    _require(
        proof["dataset"]["hash"] == header["dataset_hash"] and proof["dataset"]["count"] == header["resource_count"],
        "completion_dataset_changed",
    )


def _validate_handoff_recovery_completion(header):
    """Recheck compact recovery receipts against the detached header's logical scope."""
    metadata = _json(header.get("publication_metadata_json"))
    from process.provider_directory_import_completion import validate_metadata

    accepted_resources = validate_metadata(header)
    diagnostics = metadata.get("resource_diagnostics", {}) if isinstance(metadata, dict) else {}
    if not isinstance(diagnostics, dict):
        return
    for resource_type, diagnostic in diagnostics.items():
        if not isinstance(diagnostic, dict):
            continue
        if resource_type in accepted_resources:
            _require(diagnostic.get("complete") is False, "completion_partial_changed")
            continue
        mode = diagnostic.get("fetch_mode")
        if not (
            timestamp_recovery.marked(diagnostic)
            or isinstance(mode, str)
            and mode.startswith(timestamp_recovery.FETCH_MODE)
        ):
            continue
        source_ids = metadata.get("source_ids")
        selected_resources = metadata.get("selected_resources")
        _require(
            type(source_ids) is list
            and len(source_ids) == 1
            and type(selected_resources) is list
            and resource_type in selected_resources,
            "completion_recovery_invalid",
        )
        _require(
            timestamp_recovery.diagnostic_is_complete(
                diagnostic,
                expected={
                    "dataset_id": header.get("dataset_id"),
                    "acquisition_root_run_id": header.get("acquisition_root_run_id"),
                    "source_id": source_ids[0],
                    "resource_type": resource_type,
                },
            ),
            "completion_recovery_invalid",
        )


def _require_ordinary_source_metadata(metadata):
    """Validate the ordinary FHIR source and resource scope for isolated storage."""
    _require(
        metadata.get("source_release") is None
        and type(metadata.get("source_ids")) is list
        and bool(metadata["source_ids"])
        and all(type(source_id) is str and 1 <= len(source_id) <= 64 for source_id in metadata["source_ids"])
        and type(metadata.get("selected_resources")) is list
        and bool(metadata["selected_resources"])
        and all(type(resource) is str for resource in metadata["selected_resources"])
        and set(metadata["selected_resources"])
        <= {
            "Organization",
            "OrganizationAffiliation",
            "Practitioner",
            "PractitionerRole",
            "Location",
            "HealthcareService",
            "InsurancePlan",
            "Endpoint",
        },
        "isolated_kind_unsupported",
    )


def _require_candidate_storage(publisher, directory, identity):
    binding = require_storage_schema(publisher, directory._schema())
    if binding is not None:
        metadata = identity["metadata"]
        if metadata.get("source_ids") == ["cms-npd"]:
            from process.provider_directory_cms_source_handoff import require_cms_handoff_identity

            require_cms_handoff_identity(identity, binding.catalog_schema)
            return binding
        _require_ordinary_source_metadata(metadata)
    return binding


def _auxiliary_tables(binding_by_field, identity):
    """Require the complete producer-qualified auxiliary phase before table lookup."""
    auxiliary = binding_by_field["auxiliary"]
    if auxiliary is not None:
        _require(
            is_cms_identity(identity)
            and binding_by_field["phase"] in {"prepared", "published", "disposed"}
            and type(auxiliary) is dict
            and set(auxiliary) == {"phase", "tables", "sealed_sha256"}
            and auxiliary["phase"] in {"loading", "sealed", "published", "disposed"}
            and type(auxiliary["tables"]) is dict
            and set(auxiliary["tables"])
            == (set(AUXILIARY_ROOTS) if auxiliary["phase"] in {"loading", "sealed"} else set())
            and (
                auxiliary["sealed_sha256"] is None
                if auxiliary["phase"] == "loading"
                else type(auxiliary["sealed_sha256"]) is str
                and re.fullmatch(r"[0-9a-f]{64}", auxiliary["sealed_sha256"])
            )
            and (
                auxiliary["phase"] == binding_by_field["phase"]
                if binding_by_field["phase"] in {"published", "disposed"}
                else auxiliary["phase"] in {"loading", "sealed"}
            ),
            "auxiliary_binding_changed",
        )
        return auxiliary["tables"]
    else:
        _require(
            not is_cms_identity(identity)
            or binding_by_field["phase"] in {"loading", "disposed"}
            or binding_by_field["contract"] == CMS_ISOLATED_CONTRACT,
            "auxiliary_required",
        )
    return {}


def is_cms_identity(identity):
    """Select only the exact CMS producer/vector from immutable acquisition facts."""
    metadata = identity["metadata"]
    release = metadata.get("source_release")
    return (
        metadata.get("source_ids") == ["cms-npd"]
        and type(release) is dict
        and type(release.get("vector_sha256")) is str
        and re.fullmatch(r"[0-9a-f]{64}", release["vector_sha256"]) is not None
    )


async def _locked(connection, policy, identity, *, phases=("loading",), lock="UPDATE", allow_conversion=False):
    _require(lock in {"UPDATE", "KEY SHARE", "NO KEY UPDATE"}, "header_lock_invalid")
    header = await connection.fetchrow(
        f"SELECT endpoint_id,acquisition_root_run_id,status,is_current,publication_metadata_json,"
        f"candidate_storage_json FROM {storage_relation(policy.schema, HEADER)} WHERE dataset_id=$1 FOR {lock}",
        identity["dataset_id"],
    )
    _require(header is not None, "header_missing")
    metadata = _json(header["publication_metadata_json"])
    _require(
        header["endpoint_id"] == identity["endpoint_id"]
        and header["acquisition_root_run_id"] == identity["acquisition_root_run_id"]
        and type(metadata) is dict
        and all(metadata.get(key) == field_value for key, field_value in identity["metadata"].items()),
        "acquisition_changed",
    )
    if "published" not in phases and "ready_for_freeze" not in phases:
        _require(header["status"] == "acquiring" and header["is_current"] is False, "header_sealed")
    binding_by_field = _binding_json(header["candidate_storage_json"], policy.schema)
    if binding_by_field is not None:
        _require(
            binding_by_field.get("contract") != NAMED_COMPACT_CONTRACT
            or bool(set(phases) & {"loading", "prepared", "ready_for_freeze", "disposed"}),
            "compact_completion_unavailable",
        )
        _validate_binding(binding_by_field, identity, phases=phases)
        _require(
            allow_conversion or binding_by_field.get("payload_conversion", {}).get("state") != "copying",
            "payload_conversion_incomplete",
        )
        from process.provider_directory_generic_source_policy import current_source_storage_binding

        _require(
            (current_source_storage_binding() is not None) == (binding_by_field["contract"] in ISOLATED_CONTRACTS),
            "storage_contract_changed",
        )
    return binding_by_field


async def _store(connection, policy, identity, previous, binding_by_field, *, header_name=HEADER):
    raw_previous = None
    if previous is not None:
        raw_previous = await connection.fetchval(
            f"SELECT candidate_storage_json FROM {storage_relation(policy.schema, header_name)} "
            "WHERE dataset_id=$1 FOR NO KEY UPDATE",
            identity["dataset_id"],
        )
        _require(_binding_json(raw_previous, policy.schema) == named_source_document(previous), "header_compare_failed")
        if isinstance(raw_previous, str):
            raw_previous = json.loads(raw_previous)
    status = await connection.execute(
        f"UPDATE {storage_relation(policy.schema, header_name)} SET candidate_storage_json=$2::jsonb "
        "WHERE dataset_id=$1 AND candidate_storage_json IS NOT DISTINCT FROM $3::jsonb",
        identity["dataset_id"],
        _encoded(binding_by_field),
        None if raw_previous is None else _encoded(raw_previous),
    )
    _require(status == "UPDATE 1", "header_compare_failed")


async def _close_grants(connection, policy, name, *, readable=False, sequence=False):
    """Remove default/column grants as well as PUBLIC before any COPY/read access."""
    relation = storage_relation(policy.schema, name)
    oid = await connection.fetchval("SELECT to_regclass($1)::oid::bigint", relation)
    for grant in await connection.fetch(_GRANTEES_SQL, oid):
        principal = "PUBLIC" if grant["grantee"] == 0 else storage_identifier(grant["rolname"])
        column = grant["column_name"]
        privileges = f"ALL PRIVILEGES ({storage_identifier(column)})" if column else "ALL PRIVILEGES"
        kind = "SEQUENCE" if sequence else "TABLE"
        await connection.execute(f"REVOKE {privileges} ON {kind} {relation} FROM {principal} CASCADE")
    if readable:
        readers = ",".join(storage_identifier(role) for role in policy.runtime_roles)
        await connection.execute(f"GRANT SELECT ON TABLE {relation} TO {readers}")


async def _capture(connection, policy, names, *, private=True):
    """Resolve table names with ordinary SQL permissions and record portable coordinates."""
    references_by_name = {}
    for name in names:
        await connection.execute("SELECT 1 FROM " + storage_relation(policy.schema, name) + " LIMIT 0")
        references_by_name[name] = {"schema": policy.schema, "name": name}
    return references_by_name


async def _verify_tables(connection, policy, binding_by_field):
    """Check the logical group before resolving its SQL relations."""
    _validate_binding_tables(binding_by_field, binding_by_field["identity"])
    for table in binding_by_field["tables"].values():
        await connection.execute(
            "SELECT 1 FROM " + storage_relation(table.get("schema", policy.schema), table["name"]) + " LIMIT 0"
        )


async def _verify_auxiliary_tables(connection, policy, auxiliary):
    if auxiliary is not None and auxiliary["tables"]:
        receipts = auxiliary["tables"]
        actual = await _capture(connection, policy, [proof["name"] for proof in receipts.values()])
        _require(
            all(actual[proof["name"]] == proof for proof in receipts.values()), "auxiliary_storage_binding_changed"
        )


async def refresh_statistics(connection, schema, names):
    """Refresh planner statistics on relations this transaction just filled.

    Set-based statements that follow a bulk load otherwise plan against stale or
    absent statistics. PostgreSQL only warns and skips a root the current
    role may not analyze, so no privilege probe is needed.
    """
    for name in names:
        await connection.execute(f"ANALYZE {storage_relation(schema, name)}")


async def _verify_tables_recently(connection, policy, binding_by_field):
    """Validate the locked logical binding before another intake batch."""
    _validate_binding_tables(binding_by_field, binding_by_field["identity"])


async def _create_source_input_heaps(connection, policy, identity, *, by_name):
    """Create unindexed intake heaps and return the configured table records."""
    prefix = "pdgs_" + uuid4().hex
    names_by_kind = dict(resource_input=prefix + "_ri", witness_input=prefix + "_wi")
    for key, root in (("resource_input", RESOURCE), ("witness_input", WITNESS)):
        relation = storage_relation(policy.schema, names_by_kind[key])
        identity_clause = " GENERATED BY DEFAULT AS IDENTITY" if key == "resource_input" else ""
        await connection.execute(
            f"CREATE TABLE {relation} (LIKE {storage_relation(policy.schema, root)} "
            f"INCLUDING STORAGE, batch_ordinal bigint{identity_clause} NOT NULL, row_ordinal integer NOT NULL)"
            + PRIVATE_HEAP_OPTIONS
        )
        await _close_grants(connection, policy, names_by_kind[key])
        if key == "resource_input":
            sequence_name = await connection.fetchval(
                "SELECT c.relname FROM pg_class c WHERE c.oid=pg_get_serial_sequence($1,'batch_ordinal')::regclass",
                relation,
            )
            await _close_grants(connection, policy, sequence_name, sequence=True)
            if is_role_repeat_source(identity["metadata"].get("source_ids")):
                await connection.execute(role_order_index_sql(policy.schema, names_by_kind[key]))
    captured = (
        {name: {"schema": policy.schema, "name": name} for name in names_by_kind.values()}
        if by_name
        else await _capture(connection, policy, list(names_by_kind.values()))
    )
    return {key: captured[name] for key, name in names_by_kind.items()}


def _require_shared_payload_format(publisher, payload_format):
    if payload_format in (compact_payload.SHARED_BODY_FORMAT, compact_payload.COMPACT_V2_FORMAT):
        _require(getattr(publisher.storage_binding, "payload_format", None) == payload_format, "payload_format_changed")


async def _prepare_saved_source_storage(connection, policy, publisher, binding, payload_format):
    saved_format = binding.get("payload_conversion", {}).get("format", binding.get("payload_format"))
    _require_shared_payload_format(publisher, saved_format)
    _require(payload_format is None or binding.get("payload_format") == payload_format, "payload_format_changed")
    await _verify_tables(connection, policy, binding)
    return binding


async def prepare_generic_source_candidate(database, directory, candidate, *, payload_format=None):
    """Initialize two unindexed private input heaps, or authenticate exact resume."""
    identity = candidate_identity(directory, candidate)
    _require(
        payload_format
        in (None, compact_payload.FORMAT, compact_payload.SHARED_BODY_FORMAT, compact_payload.COMPACT_V2_FORMAT),
        "payload_format_changed",
    )
    async with source_owner_transaction(database) as publisher:
        connection, policy = publisher.connection, publisher.policy
        isolated = _require_candidate_storage(publisher, directory, identity)
        _require_shared_payload_format(publisher, payload_format)
        if payload_format is not None:
            _require(isinstance(isolated, NamedFHIRSourceStorage), "compact_named_source_required")
            compact_storage.require_source_identity(identity, payload_format=payload_format)
        binding_by_field = await _locked(connection, policy, identity, allow_conversion=True)
        if binding_by_field is not None:
            return await _prepare_saved_source_storage(connection, policy, publisher, binding_by_field, payload_format)
        old_payload = await connection.fetchval(
            f"SELECT EXISTS(SELECT FROM {storage_relation(policy.schema, RESOURCE)} WHERE dataset_id=$1)",
            candidate.dataset_id,
        )
        _require(old_payload is False and not candidate.reused_from_checkpoint, "legacy_acquisition_rerun_required")
        if payload_format is None and getattr(isolated, "payload_format", None) is not None:
            payload_format = isolated.payload_format
            compact_storage.require_source_identity(identity, payload_format=payload_format)
        by_name = isinstance(isolated, NamedFHIRSourceStorage)
        tables_by_kind = (
            await compact_storage.create_inputs(connection, policy, payload_format=payload_format)
            if payload_format is not None
            else await _create_source_input_heaps(connection, policy, identity, by_name=by_name)
        )
        binding_by_field = dict(
            contract=CONTRACT,
            identity=identity,
            phase="loading",
            ordinal=0,
            resets={},
            tables=tables_by_kind,
            auxiliary=None,
            relationship_sha256=None,
        )
        if isolated is not None:
            binding_by_field.update(
                contract=(
                    CMS_ISOLATED_CONTRACT
                    if is_cms_identity(identity)
                    else NAMED_ISOLATED_CONTRACT
                    if by_name
                    else ISOLATED_CONTRACT
                ),
                storage=storage_document(publisher),
                admission=None,
            )
        if by_name:
            if payload_format is not None:
                binding_by_field.update(contract=NAMED_COMPACT_CONTRACT, payload_format=payload_format)
            _validate_binding(binding_by_field, identity, phases=("loading",))
        await _store(connection, policy, identity, None, binding_by_field)
        return binding_by_field


async def resolve_generic_source_candidate(
    database, directory, dataset_id, *, resource_hash_contract, semantic_projection_as_of
):
    """Resolve an already prepared native binding with the caller's exact contract."""
    candidate = await resolve_existing_source_candidate(database, directory, dataset_id)
    _require(
        candidate.resource_hash_contract == resource_hash_contract
        and candidate.semantic_projection_as_of == semantic_projection_as_of,
        "hash_contract_changed",
    )
    return candidate


async def resolve_existing_source_candidate(database, directory, dataset_id):
    """Read only the explicit existing protected binding for reset/witness controls."""
    _require(type(dataset_id) is str and 1 <= len(dataset_id) <= 96, "dataset_invalid")
    async with source_owner_transaction(database) as publisher:
        require_storage_schema(publisher, directory._schema())
        row = await publisher.connection.fetchrow(
            f"SELECT candidate_storage_json FROM {storage_relation(publisher.policy.schema, HEADER)} "
            "WHERE dataset_id=$1 FOR KEY SHARE",
            dataset_id,
        )
        binding_by_field = (
            _binding_json(row["candidate_storage_json"], publisher.policy.schema) if row is not None else None
        )
        _require(
            type(binding_by_field) is dict and type(binding_by_field.get("identity")) is dict, "preparation_required"
        )
        identity = binding_by_field["identity"]
        _require(identity.get("dataset_id") == dataset_id, "dataset_changed")
        await _locked(
            publisher.connection, publisher.policy, identity, phases=("loading", "prepared"), lock="KEY SHARE"
        )
        metadata = identity["metadata"]
        contract = metadata.get(directory.RESOURCE_HASH_CONTRACT_METADATA_KEY)
        _require(contract in directory.RESOURCE_HASH_CONTRACTS, "hash_contract_changed")
        return _StoredCandidate(identity, contract, metadata.get(directory.SEMANTIC_PROJECTION_AS_OF_METADATA_KEY))


def _validated_transition_identity(identity_by_field, expected_resource_counts):
    """Require one exact unpublished dataset and complete resource census."""
    _require(
        type(identity_by_field) is dict
        and set(identity_by_field) == {"dataset_id", "endpoint_id", "acquisition_root_run_id", "failed_run_id"}
        and type(identity_by_field["dataset_id"]) is str
        and 1 <= len(identity_by_field["dataset_id"]) <= 96
        and type(identity_by_field["endpoint_id"]) is str
        and bool(identity_by_field["endpoint_id"].strip())
        and all(
            type(identity_by_field[name]) is str and re.fullmatch(r"run_[0-9a-f]{32}", identity_by_field[name])
            for name in ("acquisition_root_run_id", "failed_run_id")
        )
        and type(expected_resource_counts) is dict
        and 1 <= len(expected_resource_counts) <= 32
        and all(
            type(name) is str
            and re.fullmatch(r"(?:[A-Za-z][A-Za-z0-9]{0,63}|LU:[A-Za-z][A-Za-z0-9]{0,63}:pass:[12])", name)
            and type(count) is int
            and count >= 0
            for name, count in expected_resource_counts.items()
        ),
        "transition_identity_invalid",
    )


def _is_empty_vsp_checkpoint_failure(
    header, candidate_by_field, identity_by_field, expected_counts, new_storage, *, manifest_replay=False
):
    """Recognize exact VSP failures before any normalized input was committed."""
    recorded_storage = candidate_by_field.get("storage")
    is_same_producer = recorded_storage == new_storage
    if type(recorded_storage) is dict and "producer" in recorded_storage:
        is_same_producer = {
            key: recorded_storage.get(key) for key in ("catalog_schema", "storage_schema", "producer")
        } == new_storage
    elif manifest_replay and type(new_storage) is dict:
        is_same_producer = recorded_storage == {
            key: new_storage.get(key) for key in ("catalog_schema", "storage_schema")
        }
    failure_metadata = _json(header["publication_metadata_json"])
    identity_metadata = candidate_by_field["identity"]["metadata"]
    root_run_id = identity_by_field["acquisition_root_run_id"]
    return (
        header.get("resource_count") == 0
        and type(failure_metadata) is dict
        and (
            (
                header["import_run_id"] == root_run_id
                and failure_metadata.get("error") == "vsp_prepared_candidate_required"
                and (
                    (
                        identity_by_field["failed_run_id"] == root_run_id
                        and (header["status"] == "failed" or is_same_producer)
                    )
                    or (identity_by_field["failed_run_id"] != root_run_id and header["status"] == "acquiring")
                )
            )
            or (
                header["import_run_id"] == identity_by_field["failed_run_id"] != root_run_id
                and is_artifact_root_failure(failure_metadata.get("error"))
                and (header["status"] == "failed" or is_same_producer)
                and candidate_by_field["relationship_sha256"] is None
            )
            or (
                header["import_run_id"] == identity_by_field["failed_run_id"]
                and (
                    header["import_run_id"] == root_run_id or candidate_by_field["contract"] == NAMED_ISOLATED_CONTRACT
                )
                and header["status"] == "failed"
                and (
                    failure_metadata.get("error") == OFFICE_DOCTORS_INVALID
                    or candidate_by_field["contract"] == NAMED_ISOLATED_CONTRACT
                    and failure_metadata.get("error") in _RETAINED_VSP_CAPTURE_ERRORS
                )
                and candidate_by_field["relationship_sha256"] is None
            )
        )
        and identity_metadata.get("source_ids") == [VSP_SOURCE_ID]
        and all(failure_metadata.get(name) == expected_value for name, expected_value in identity_metadata.items())
        and expected_counts == dict.fromkeys(VSP_RESOURCE_TYPES, 0)
        and candidate_by_field["ordinal"] == 0
        and candidate_by_field["resets"] in ({}, {"*": 0})
    )


async def _locked_transition_header(connection, policy, identity_by_field):
    """Fence the endpoint and stopped worker before inspecting the exact header."""
    endpoint_id = identity_by_field["endpoint_id"]
    failed_run_id = identity_by_field["failed_run_id"]
    endpoint_lock_acquired = await connection.fetchval(
        "SELECT pg_try_advisory_xact_lock(hashtextextended($1,0))", endpoint_id
    )
    _require(endpoint_lock_acquired is True, "transition_endpoint_busy")
    worker_lock_acquired = await connection.fetchval(
        "SELECT pg_try_advisory_xact_lock(hashtextextended($1,0))",
        "control-run-worker-action:v1:" + failed_run_id,
    )
    _require(worker_lock_acquired is True, "transition_worker_action_busy")
    header = await connection.fetchrow(
        "SELECT dataset_id,endpoint_id,acquisition_root_run_id,import_run_id,status,is_current,published_at,resource_count,"
        "publication_metadata_json,candidate_storage_json FROM "
        + storage_relation(policy.schema, HEADER)
        + " WHERE dataset_id=$1 FOR UPDATE NOWAIT",
        identity_by_field["dataset_id"],
    )
    _require(
        header is not None
        and all(
            header[field] == identity_by_field[identity_field]
            for field, identity_field in (
                ("dataset_id", "dataset_id"),
                ("endpoint_id", "endpoint_id"),
                ("acquisition_root_run_id", "acquisition_root_run_id"),
            )
        )
        and header["status"] in {"acquiring", "failed"}
        and header["is_current"] is False
        and header["published_at"] is None,
        "transition_candidate_changed",
    )
    return header


def _transition_metadata_matches(metadata, expected, status):
    """A failed candidate can add an error without changing its identity."""
    return metadata == expected or (
        status == "failed"
        and type(metadata) is dict
        and all(field in metadata and metadata[field] == value for field, value in expected.items())
    )


def _transition_candidate_document(stored_document, old_storage, new_storage):
    """Project only known saved candidate encodings onto qualified logical names."""
    raw = json.loads(stored_document) if isinstance(stored_document, str) else stored_document
    identity = raw.get("identity") if type(raw) is dict else None
    _validate_binding_shape(raw, identity, phases=("loading",))
    _require(raw["contract"] in (ISOLATED_CONTRACT, *NAMED_CONTRACTS), "transition_candidate_changed")
    recorded = raw["storage"]
    names = {"catalog_schema", "storage_schema"}
    legacy_fields = {
        "database_oid",
        "catalog_schema_oid",
        "storage_schema_oid",
        "owner_oid",
        "namespace_owner_oid",
        "enrollment_sha256",
    }
    _require(
        type(recorded) is dict and set(recorded) in (names, names | {"producer"}, names | legacy_fields | {"producer"}),
        "transition_storage_changed",
    )
    if "producer" in recorded:
        _require(
            recorded["producer"] in (old_storage["producer"], new_storage["producer"]), "transition_storage_changed"
        )
    storage_by_field = {field: recorded[field] for field in names}
    _validate_named_storage(storage_by_field)
    tables_by_kind = {}
    _require(type(raw["tables"]) is dict, "table_binding_changed")
    for kind, table in raw["tables"].items():
        _require(
            type(table) is dict
            and set(table) in ({"name"}, {"schema", "name"}, {"name", "oid", "owner", "layout_sha256"}),
            "table_binding_changed",
        )
        tables_by_kind[kind] = {
            "schema": table.get("schema", storage_by_field["storage_schema"]),
            "name": table["name"],
        }
    candidate_by_field = {**raw, "storage": storage_by_field, "tables": tables_by_kind}
    _validate_binding_tables(candidate_by_field, identity)
    return raw, candidate_by_field


def _is_unstarted_named_binding(binding):
    """Recognize a named loading candidate before any input batch or conversion."""
    return (
        binding["contract"] == NAMED_ISOLATED_CONTRACT
        and binding["ordinal"] == 0
        and binding["resets"] in ({}, {"*": 0})
        and binding["relationship_sha256"] is None
        and "payload_conversion" not in binding
    )


def _is_unstarted_named_root(header, binding, identity, expected_counts, manifest_replay):
    """Identify only the original failed named root or its issued replay."""
    return (
        (header["status"] == "failed" or manifest_replay and header["status"] == "acquiring")
        and header["import_run_id"] == identity["failed_run_id"] == identity["acquisition_root_run_id"]
        and _is_unstarted_named_binding(binding)
        and not any(expected_counts.values())
        and header.get("resource_count") == 0
        and named_source_resources(binding["identity"]["metadata"].get("source_ids")) is None
    )


async def _locked_transition_candidate(
    connection, policy, identity_by_field, old_storage, new_storage, expected_counts, *, manifest_replay=False
):
    """Verify the stopped acquisition and its declared storage."""
    header = await _locked_transition_header(connection, policy, identity_by_field)
    raw_candidate, candidate_by_field = _transition_candidate_document(
        header["candidate_storage_json"], old_storage, new_storage
    )
    stored_identity = candidate_by_field.get("identity") if type(candidate_by_field) is dict else None
    _validate_binding_shape(candidate_by_field, stored_identity, phases=("loading",))
    _validate_binding_tables(candidate_by_field, stored_identity)
    is_empty_vsp_failure = _is_empty_vsp_checkpoint_failure(
        header, raw_candidate, identity_by_field, expected_counts, new_storage, manifest_replay=manifest_replay
    )
    is_unstarted_named_root = not is_empty_vsp_failure and _is_unstarted_named_root(
        header, candidate_by_field, identity_by_field, expected_counts, manifest_replay
    )
    metadata = _json(header["publication_metadata_json"])
    metadata_status = "failed" if is_unstarted_named_root else header["status"]
    has_identity_metadata = _transition_metadata_matches(metadata, stored_identity["metadata"], metadata_status)
    has_changed_header_owner = header["import_run_id"] != identity_by_field["failed_run_id"]
    candidate_storage_by_field = candidate_by_field.get("storage")
    source_ids = stored_identity["metadata"].get("source_ids")
    if is_unstarted_named_root or (source_ids != [VSP_SOURCE_ID] and named_source_resources(source_ids) is not None):
        _require_named_source_identity(stored_identity)
    expected_storage_by_field = {key: old_storage[key] for key in ("catalog_schema", "storage_schema")}
    _require(
        candidate_by_field["contract"] in (ISOLATED_CONTRACT, *NAMED_CONTRACTS)
        and (not has_changed_header_owner or is_empty_vsp_failure)
        and candidate_by_field["admission"] is None
        and candidate_by_field["auxiliary"] is None
        and all(
            stored_identity[field] == identity_by_field[field]
            for field in ("dataset_id", "endpoint_id", "acquisition_root_run_id")
        )
        and (is_empty_vsp_failure or has_identity_metadata)
        and set(stored_identity["metadata"].get("selected_resources", ()))
        == set(stored_identity["metadata"].get("expected_resources", ()))
        == {name for name in expected_counts if not name.startswith("LU:")}
        == {_resource_family(name) for name in expected_counts}
        and candidate_storage_by_field == expected_storage_by_field,
        "transition_candidate_changed",
    )
    await _verify_tables(connection, policy, candidate_by_field)
    return (
        candidate_by_field,
        header["status"],
        is_empty_vsp_failure,
        has_changed_header_owner,
        metadata.get("error"),
        raw_candidate,
        is_unstarted_named_root,
    )


async def _verify_transition_run(
    connection,
    catalog_schema,
    identity_by_field,
    source_ids,
    *,
    has_changed_header_owner,
    is_empty_vsp_failure=False,
    expected_failure_error=None,
):
    """Require the exact latest stopped run and no active same-root run."""
    root_run_id = identity_by_field["acquisition_root_run_id"]
    failed_run_id = identity_by_field["failed_run_id"]
    is_vsp_artifact_failure = is_empty_vsp_failure and not has_changed_header_owner and failed_run_id != root_run_id
    is_retained_vsp_failure = is_empty_vsp_failure and expected_failure_error in _RETAINED_VSP_CAPTURE_ERRORS
    directory = import_module("process.provider_directory_fhir")
    is_highmark_companion_source = directory.highmark.is_companion_source(directory, source_ids)
    run_rows = await _read_transition_runs(connection, catalog_schema, root_run_id)
    run_ids = {run_record["run_id"] for run_record in run_rows}
    _verify_transition_run_rows(run_rows, run_ids, root_run_id, failed_run_id, source_ids)
    is_postal_start_failure = _is_postal_start_failure(run_rows[0], expected_failure_error)
    ancestor_run_ids = _transition_run_ancestors(run_rows, root_run_id, failed_run_id)
    if has_changed_header_owner:
        _verify_rearmed_vsp_child(run_rows, root_run_id)
    elif is_retained_vsp_failure:
        _require(
            len(run_rows) == len(ancestor_run_ids)
            and run_rows[0].get("error_message") == expected_failure_error
            and all(
                run_record["status"] == "failed" and run_record["finished_at"] is not None for run_record in run_rows
            ),
            "transition_run_changed",
        )
    elif is_vsp_artifact_failure:
        _verify_vsp_pre_capture_lineage(run_rows, root_run_id, ancestor_run_ids, expected_failure_error)
    return {
        "lineage_run_ids": sorted(run_ids),
        "ancestor_run_ids": ancestor_run_ids,
        "failed_ancestor_run_ids": sorted(
            run_record["run_id"]
            for run_record in run_rows
            if run_record["run_id"] in ancestor_run_ids
            and run_record["status"] == "failed"
            and run_record["finished_at"] is not None
        ),
        "status": run_rows[0]["status"],
        "is_empty_vsp_failure": is_empty_vsp_failure,
        "is_postal_start_failure": is_postal_start_failure,
        "is_stopped_postal_scan": _is_stopped_postal_scan(directory, run_rows, ancestor_run_ids),
        "pending_highmark_resources": _transition_pending_resources(run_rows[0])
        if is_highmark_companion_source
        else (),
    }


def _transition_pending_resources(run):
    """Require the stopped result and the same durable audit to name the families."""
    pending = _json(run.get("pending_resources"))
    if (
        run["status"] == "failed"
        and type(pending) is list
        and all(isinstance(entry, str) for entry in pending)
        and run.get("error_message") == "provider_directory_pagination_resume_required:" + ",".join(pending)
    ):
        return pending
    return ()


async def _read_transition_runs(connection, catalog_schema, root_run_id):
    """Read one ordered lineage with the failure details required by its transition."""
    return await connection.fetch(
        "SELECT run_id,status,finished_at,retry_of_run_id,created_at,"
        "params::jsonb->>'provider_directory_pagination_root_run_id' AS root_run_id,"
        "params::jsonb->'source_ids' AS source_ids,"
        "params::jsonb->>'provider_directory_endpoint_scope' AS postal_endpoint_scope,"
        "params::jsonb->'allow_retained_pagination_reset' IS NOT DISTINCT FROM 'false'::jsonb AS postal_reset_disabled,"
        "error::jsonb->>'code' AS postal_error_code,error::jsonb->>'message' AS error_message,"
        "metrics::jsonb#>'{audit,pagination_resume_required}' AS pending_resources FROM "
        + storage_relation(catalog_schema, "import_run")
        + " WHERE importer='provider-directory-fhir' AND "
        "(run_id=$1 OR params::jsonb->>'provider_directory_pagination_root_run_id'=$1) "
        "ORDER BY created_at DESC,run_id DESC LIMIT 65",
        root_run_id,
    )


def _is_postal_start_failure(run, expected_failure_error):
    if expected_failure_error != _POSTAL_START_ERROR:
        return False
    directory = import_module("process.provider_directory_fhir")
    if run["postal_endpoint_scope"] != directory.UHC_PROVIDER_DIRECTORY_BASE:
        return False
    _require(run["error_message"] == expected_failure_error, "transition_run_changed")
    return True


def _is_stopped_postal_scan(directory, runs, ancestor_ids):
    """Recognize a stopped postal intake that can retain its unmodified rows."""
    leaf = runs[0]
    endpoint = directory.UHC_PROVIDER_DIRECTORY_BASE
    if leaf.get("postal_endpoint_scope") != endpoint:
        return False
    if not _has_postal_stop_result(directory, leaf):
        return False
    _require(
        leaf["status"] == "failed"
        and len(runs) == len(ancestor_ids)
        and all(
            run["status"] == "failed"
            and run["finished_at"] is not None
            and run.get("postal_endpoint_scope") == endpoint
            and run.get("postal_reset_disabled") is True
            for run in runs
        ),
        "transition_run_changed",
    )
    return True


def _has_postal_stop_result(directory, leaf):
    """Permit an exact merge failure for transfer while still verifying all inputs."""
    if leaf.get("postal_error_code") == "import_interrupted":
        return True
    source_ids = _json(leaf["source_ids"])
    if leaf.get("postal_error_code") != "import_failed" or len(source_ids) != 1:
        return False
    if leaf.get("error_message") in _POSTAL_REDUCTION_CONFLICTS:
        return True
    pending_resources = _transition_pending_resources(leaf)
    allowed_resources = {f"{source_ids[0]}:{resource}" for resource in directory.UHC_SUPPORTED_RESOURCES}
    distinct_pending_resources = set(pending_resources)
    return (
        bool(distinct_pending_resources)
        and distinct_pending_resources <= allowed_resources
        and len(pending_resources) == len(distinct_pending_resources)
    )


def _verify_vsp_pre_capture_lineage(run_rows, root_run_id, ancestor_run_ids, expected_failure_error):
    """Allow capture creation only when every stopped ancestor failed before capture."""
    _require(
        len(run_rows) == len(ancestor_run_ids)
        and run_rows[0].get("error_message") == expected_failure_error
        and is_artifact_root_failure(expected_failure_error)
        and all(
            run_record["status"] == "failed"
            and run_record["finished_at"] is not None
            and (
                run_record.get("error_message") == "vsp_prepared_candidate_required"
                if run_record["run_id"] == root_run_id
                else is_artifact_root_failure(run_record.get("error_message"))
                or (
                    run_record["retry_of_run_id"] == root_run_id
                    and run_record.get("error_message")
                    == "provider_directory_generic_source_isolated_relation_override_changed"
                )
            )
            for run_record in run_rows
        ),
        "transition_run_changed",
    )


def _verify_transition_run_rows(run_rows, run_ids, root_run_id, failed_run_id, source_ids):
    """Check the latest stop and reject active or out-of-scope runs."""
    _require(
        1 <= len(run_rows) <= 64
        and run_rows[0]["run_id"] == failed_run_id
        and run_rows[0]["status"] in {"failed", "canceled"}
        and run_rows[0]["finished_at"] is not None
        and (
            run_rows[0]["root_run_id"] == root_run_id
            or (run_rows[0]["run_id"] == root_run_id and run_rows[0]["root_run_id"] is None)
        )
        and _json(run_rows[0]["source_ids"]) == source_ids
        and all(
            run_record["retry_of_run_id"] is None
            if run_record["run_id"] == root_run_id
            else run_record["retry_of_run_id"] in run_ids
            for run_record in run_rows
        )
        and all(
            _json(run_record["source_ids"]) == source_ids
            and (run_record["run_id"] == root_run_id or run_record["root_run_id"] == root_run_id)
            for run_record in run_rows
        )
        and all(
            run_record["status"] not in {"queued", "starting", "running", "finalizing", "canceling"}
            for run_record in run_rows
        ),
        "transition_run_changed",
    )


def _verify_rearmed_vsp_child(run_rows, root_run_id):
    """Accept only the stopped direct child that never attached to the candidate."""
    _require(
        len(run_rows) == 2
        and run_rows[0]["retry_of_run_id"] == root_run_id
        and run_rows[0]["status"] == "failed"
        and run_rows[1]["status"] == "failed"
        and run_rows[1]["finished_at"] is not None
        and run_rows[0].get("error_message") == "provider_directory_generic_source_isolated_relation_override_changed",
        "transition_run_changed",
    )


def _transition_run_ancestors(run_rows, root_run_id, failed_run_id):
    """Follow the exact failed lineage to its root without cycles."""
    parents_by_id = {run_record["run_id"]: run_record["retry_of_run_id"] for run_record in run_rows}
    lineage_cursor = failed_run_id
    seen_run_ids = set()
    while lineage_cursor != root_run_id:
        _require(lineage_cursor in parents_by_id and lineage_cursor not in seen_run_ids, "transition_run_changed")
        seen_run_ids.add(lineage_cursor)
        lineage_cursor = parents_by_id[lineage_cursor]
    return sorted(seen_run_ids | {root_run_id})


def _is_resumable_transition_checkpoint(checkpoint):
    """Keep completed scans or an unchanged, same-origin retained continuation."""
    completeness = _json(checkpoint.get("completeness_json"))
    if timestamp_recovery.marked(completeness) and not _valid_transition_recovery_checkpoint(checkpoint, completeness):
        return False
    if checkpoint["state"] == "complete":
        return checkpoint["completed_at"] is not None
    cursor = checkpoint["next_url"]
    if (
        checkpoint["state"] != "active"
        or checkpoint["completed_at"] is not None
        or type(cursor) is not str
        or not cursor
        or any(character.isspace() for character in cursor)
        or hashlib.sha256(cursor.encode()).hexdigest() == checkpoint["start_url_hash"]
    ):
        return False
    directory = import_module("process.provider_directory_fhir")
    base = checkpoint["canonical_api_base"]
    try:
        return directory._is_trusted_fhir_pagination_url({"canonical_api_base": base}, base, cursor)
    except ValueError:
        return False


def _valid_transition_recovery_checkpoint(checkpoint, completeness):
    """Bind retained recovery progress and terminal request evidence to its checkpoint."""
    try:
        source_ids = _json(checkpoint["source_ids"])
        if type(source_ids) is not list or len(source_ids) != 1:
            return False
        identity_by_field = {
            key: checkpoint[key]
            for key in (
                "canonical_api_base",
                "dataset_id",
                "acquisition_root_run_id",
                "resource_type",
                "start_url_hash",
                "source_scope_hash",
            )
        }
        identity_by_field["source_id"] = source_ids[0]
        is_complete = checkpoint["state"] == "complete"
        state = timestamp_recovery.validate_checkpoint(
            completeness,
            identity=identity_by_field,
            next_url=checkpoint["next_url"],
            pages_processed=checkpoint["pages_processed"],
            rows_processed=checkpoint["rows_processed"],
            complete=is_complete,
        )
        if not is_complete:
            return True
        recent = _json(checkpoint["recent_cursor_hashes"])
        directory = import_module("process.provider_directory_fhir")
        return bool(
            type(recent) is list
            and recent
            and recent[-1] == directory._pagination_url_hash(state["terminal_request_url"])
        )
    except KeyError, TypeError, ValueError:
        return False


def _is_bulk_transition_request(directory, checkpoint, audit):
    """Keep canonical commitments and the two historical submitted query orderings."""
    if "normalized_request_hash" not in audit and audit["request_hash"] == checkpoint["start_url_hash"]:
        return True
    for should_omit_format in (False, True):
        request = directory._bulk_export_start_url(
            {
                "api_base": checkpoint["canonical_api_base"],
                "metadata_json": {"provider_directory_bulk_export_omit_output_format": should_omit_format},
            },
            checkpoint["resource_type"],
        )
        if hashlib.sha256(request.encode()).hexdigest() != checkpoint["start_url_hash"]:
            continue
        if "normalized_request_hash" in audit:
            return (
                audit["normalized_request_hash"]
                == hashlib.sha256(directory._normalized_bulk_request_identity(request).encode()).hexdigest()
            )
        parsed = urllib.parse.urlsplit(request)
        reversed_query = urllib.parse.urlencode(
            list(reversed(urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)))
        )
        reversed_request = urllib.parse.urlunsplit(parsed._replace(query=reversed_query))
        return audit["request_hash"] in {
            checkpoint["start_url_hash"],
            hashlib.sha256(reversed_request.encode()).hexdigest(),
        }
    return False


def _validate_bulk_transition_checkpoint(checkpoint, identity_by_field, lineage_run_ids):
    """Retain the exact export identity and its URL-free manifest evidence."""
    directory = import_module("process.provider_directory_fhir")
    checkpoint_identity_by_field = {
        field: checkpoint[field]
        for field in (
            "canonical_api_base",
            "resource_type",
            "source_scope_hash",
            "strategy_version",
            "acquisition_root_run_id",
            "endpoint_id",
            "dataset_id",
            "start_url_hash",
        )
    }
    audit = _json(checkpoint["manifest_json"])
    _require(
        all(
            checkpoint[field] == identity_by_field[field]
            for field in ("acquisition_root_run_id", "endpoint_id", "dataset_id")
        )
        and (
            checkpoint["owner_run_id"] == identity_by_field["failed_run_id"]
            or (checkpoint["state"] == "complete" and checkpoint["owner_run_id"] in lineage_run_ids)
        )
        and checkpoint["checkpoint_id"] == directory._identity_hash(checkpoint_identity_by_field)
        and checkpoint["strategy_version"] == directory.BULK_EXPORT_CHECKPOINT_STRATEGY_VERSION
        and directory._is_safe_bulk_https_url(checkpoint["canonical_api_base"])
        and directory._canonical_base(checkpoint["canonical_api_base"]) == checkpoint["canonical_api_base"]
        and all(
            type(checkpoint[field]) is str and re.fullmatch(r"[0-9a-f]{64}", checkpoint[field])
            for field in ("source_scope_hash", "start_url_hash", "manifest_hash")
        )
        and checkpoint["state"] in {"manifest_ready", "streaming", "retryable", "complete"}
        and (checkpoint["state"] == "complete") == (checkpoint["completed_at"] is not None)
        and checkpoint["failed_at"] is None
        and (checkpoint["error"] is None or type(checkpoint["error"]) is str)
        and not directory._is_bulk_export_error_terminal(checkpoint["error"])
        and type(audit) is dict
        and type(audit.get("requiresAccessToken")) is bool
        and type(audit.get("request_hash")) is str
        and re.fullmatch(r"[0-9a-f]{64}", audit["request_hash"])
        and _is_bulk_transition_request(directory, checkpoint, audit)
        and type(audit.get("output")) is list
        and bool(audit["output"]),
        "transition_bulk_checkpoint_changed",
    )
    try:
        directory._validated_bulk_transaction_time(audit.get("transactionTime"))
    except ValueError:
        _require(False, "transition_bulk_checkpoint_changed")
    return audit["output"]


def _validate_bulk_transition_outputs(checkpoint, outputs, manifest_outputs, expected_count):
    """Keep every committed byte and require the original immutable file proofs."""
    directory = import_module("process.provider_directory_fhir")
    manifest_outputs = [
        entry for entry in manifest_outputs if type(entry) is dict and entry.get("type") == checkpoint["resource_type"]
    ]
    _require(
        len(outputs) == len(manifest_outputs)
        and [output["output_index"] for output in outputs] == list(range(len(outputs)))
        and len({output["output_id"] for output in outputs}) == len(outputs)
        and type(checkpoint["rows_written"]) is int
        and checkpoint["rows_written"] == expected_count,
        "transition_bulk_output_changed",
    )
    for output, manifest_output in zip(outputs, manifest_outputs, strict=True):
        _require(
            type(manifest_output) is dict
            and set(manifest_output) == {"type", "url_hash"}
            and manifest_output["type"] == output["resource_type"] == checkpoint["resource_type"]
            and manifest_output["url_hash"] == output["output_url_hash"]
            and all(
                type(output[field]) is str and re.fullmatch(r"[0-9a-f]{64}", output[field])
                for field in ("output_id", "output_url_hash")
            )
            and output["checkpoint_id"] == checkpoint["checkpoint_id"]
            and output["state"] in {"pending", "streaming", "complete"}
            and (output["state"] == "complete") == (output["completed_at"] is not None)
            and (checkpoint["state"] != "complete" or output["state"] == "complete")
            and (output["state"] == "complete" or (output["has_output_url"] and output["has_etag"]))
            and output["error"] is None
            and type(output["rows_written"]) is int
            and output["rows_written"] >= 0
            and type(output["committed_bytes"]) is int
            and output["rows_written"] <= output["committed_bytes"],
            "transition_bulk_output_changed",
        )
        try:
            directory._bulk_output_validator_proof(output)
        except ValueError:
            _require(False, "transition_bulk_output_changed")
    _require(sum(output["rows_written"] for output in outputs) == expected_count, "transition_bulk_output_changed")


async def _verify_bulk_transition_checkpoints(
    connection, policy, checkpoints, identity_by_field, expected_counts, lineage_run_ids
):
    """Verify one bulk-only candidate, including selected resources not yet started."""
    resources = {checkpoint["resource_type"] for checkpoint in checkpoints}
    _require(
        len(resources) == len(checkpoints)
        and resources <= set(expected_counts)
        and all(count == 0 or resource in resources for resource, count in expected_counts.items())
        and len({(checkpoint["canonical_api_base"], checkpoint["source_scope_hash"]) for checkpoint in checkpoints})
        == 1,
        "transition_bulk_checkpoint_changed",
    )
    outputs = await connection.fetch(
        "SELECT checkpoint_id,output_id,output_index,resource_type,output_url_hash,state,rows_written,"
        "content_length_bytes,etag_hash,committed_bytes,validator_checked_at,completed_at,error,"
        "output_url_ciphertext IS NOT NULL AS has_output_url,etag_ciphertext IS NOT NULL AS has_etag FROM "
        + storage_relation(policy.schema, "provider_directory_bulk_output_checkpoint")
        + " WHERE checkpoint_id=ANY($1::varchar[]) ORDER BY checkpoint_id,output_index FOR SHARE NOWAIT",
        [checkpoint["checkpoint_id"] for checkpoint in checkpoints],
    )
    _require(
        {output["checkpoint_id"] for output in outputs} == {checkpoint["checkpoint_id"] for checkpoint in checkpoints},
        "transition_bulk_output_changed",
    )
    for checkpoint in checkpoints:
        manifest_outputs = _validate_bulk_transition_checkpoint(checkpoint, identity_by_field, lineage_run_ids)
        _validate_bulk_transition_outputs(
            checkpoint,
            [output for output in outputs if output["checkpoint_id"] == checkpoint["checkpoint_id"]],
            manifest_outputs,
            expected_counts[checkpoint["resource_type"]],
        )


def _require_registered_transition_scope(checkpoints, bulk_checkpoints, source_ids):
    """Keep the registered Anthem endpoint and one complete checkpoint scope."""
    if source_ids == [VSP_SOURCE_ID] or named_source_resources(source_ids) is None:
        return
    directory = import_module("process.provider_directory_fhir")
    _require(
        not bulk_checkpoints
        and all(
            checkpoint["canonical_api_base"] == directory.anthem.API_BASE
            and _json(checkpoint["source_ids"]) == source_ids
            and type(checkpoint["source_scope_hash"]) is str
            and re.fullmatch(r"[0-9a-f]{64}", checkpoint["source_scope_hash"])
            for checkpoint in checkpoints
        )
        and len({checkpoint["source_scope_hash"] for checkpoint in checkpoints}) == 1,
        "transition_checkpoint_changed",
    )


def _record_unstarted_checkpoint_proof(run_evidence, has_checkpoints):
    """Use the ordinary verification path whenever there is a saved checkpoint."""
    if run_evidence.get("is_unstarted_named_root"):
        run_evidence["is_unstarted_named_root"] = not has_checkpoints
        if not has_checkpoints:
            _require(run_evidence["status"] == "failed", "transition_checkpoint_changed")
            run_evidence["require_empty_checkpoints"] = True


async def _verify_transition_checkpoints(
    connection, policy, identity_by_field, expected_counts, run_evidence, candidate
):
    """Reject mixed checkpoint families before comparing their retained input census."""
    run_status = run_evidence["status"]
    source_ids = candidate["identity"]["metadata"]["source_ids"]
    root_run_id = identity_by_field["acquisition_root_run_id"]
    dataset_id = identity_by_field["dataset_id"]
    checkpoint_rows = await connection.fetch(
        "SELECT resource_type,state,owner_run_id,dataset_id,rows_processed,pages_processed,completed_at,"
        "canonical_api_base,start_url_hash,next_url,acquisition_root_run_id,source_ids,source_scope_hash,"
        "completeness_json,recent_cursor_hashes FROM "
        + storage_relation(policy.schema, "provider_directory_pagination_checkpoint")
        + " WHERE acquisition_root_run_id=$1 OR dataset_id=$2 FOR SHARE NOWAIT",
        root_run_id,
        dataset_id,
    )
    bulk_checkpoints = await connection.fetch(
        "SELECT checkpoint_id,canonical_api_base,resource_type,source_scope_hash,strategy_version,"
        "acquisition_root_run_id,owner_run_id,endpoint_id,dataset_id,start_url_hash,manifest_hash,manifest_json,"
        "state,rows_written,completed_at,failed_at,error FROM "
        + storage_relation(policy.schema, "provider_directory_bulk_acquisition_checkpoint")
        + " WHERE acquisition_root_run_id=$1 OR dataset_id=$2 FOR SHARE NOWAIT",
        root_run_id,
        dataset_id,
    )
    _require_registered_transition_scope(checkpoint_rows, bulk_checkpoints, source_ids)
    _record_unstarted_checkpoint_proof(run_evidence, bool(checkpoint_rows or bulk_checkpoints))
    if run_evidence.get("is_postal_start_failure"):
        _require(not bulk_checkpoints, "transition_checkpoint_changed")
        _require_postal_start_checkpoint(checkpoint_rows, identity_by_field, expected_counts, run_status, source_ids)
        return [], []
    if run_evidence.get("is_stopped_postal_scan"):
        _require(not bulk_checkpoints, "transition_checkpoint_changed")
        await _verify_stopped_postal_checkpoints(
            connection, policy, checkpoint_rows, identity_by_field, expected_counts, run_evidence, candidate
        )
        return [], []
    if run_evidence.get("require_empty_checkpoints"):
        _require(not (checkpoint_rows or bulk_checkpoints), "transition_checkpoint_changed")
        return [], []
    _require(not (checkpoint_rows and bulk_checkpoints), "transition_checkpoint_changed")
    if bulk_checkpoints:
        _require(not any(name.startswith("LU:") for name in expected_counts), "transition_checkpoint_changed")
        await _verify_bulk_transition_checkpoints(
            connection, policy, bulk_checkpoints, identity_by_field, expected_counts, run_evidence["ancestor_run_ids"]
        )
        return [], []
    from process.provider_directory_partition_successor import partition_checkpoint_renewals

    transitions = await partition_checkpoint_renewals(
        checkpoint_rows, identity_by_field, expected_counts, source_ids, run_evidence
    )
    if transitions is None:
        from process.provider_directory_completion_successor import require_rest

        return await require_rest(
            connection, candidate, identity_by_field, checkpoint_rows, expected_counts, run_evidence
        )
    return transitions


def _require_postal_start_checkpoint(checkpoints, identity_by_field, expected_counts, run_status, source_ids):
    """Preserve completed plans when the postal resources never reached their first request."""
    directory = import_module("process.provider_directory_fhir")
    plans = expected_counts.get("InsurancePlan", 0)
    _require(
        len(checkpoints) == len(source_ids) == 1
        and set(expected_counts) == set(directory.UHC_SUPPORTED_RESOURCES)
        and plans > 0
        and plans == sum(expected_counts.values())
        and checkpoints[0]["canonical_api_base"] == directory.UHC_PROVIDER_DIRECTORY_BASE
        and _json(checkpoints[0]["source_ids"]) == source_ids
        and checkpoints[0]["state"] == "complete"
        and checkpoints[0]["next_url"] is None
        and type(checkpoints[0]["source_scope_hash"]) is str
        and re.fullmatch(r"[0-9a-f]{64}", checkpoints[0]["source_scope_hash"]) is not None,
        "transition_checkpoint_changed",
    )
    _require_rest_transition_checkpoints(checkpoints, identity_by_field, {"InsurancePlan": plans}, run_status)


def _valid_postal_transition_leaf(directory, source_record, resource_type, identity):
    """Check the exact identity encoding of one completed source partition."""
    if not isinstance(identity, str) or not identity or len(identity) > 128:
        return False
    if resource_type == "PractitionerRole":
        prefix = identity
    else:
        query = urllib.parse.parse_qsl(identity, keep_blank_values=True)
        if (
            len(query) != 1
            or directory._uhc_partition_identity(
                source_record, resource_type, f"{directory.UHC_PROVIDER_DIRECTORY_BASE}/{resource_type}?{identity}"
            )
            != identity
        ):
            return False
        prefix = query[0][1]
    if resource_type in directory.uhc_scan.POSTAL_SEARCH_PARAMS:
        return re.fullmatch(r"[A-Z]|[0-9]{1,5}|[0-9]{5}-[0-9]{1,4}", prefix) is not None
    return (
        bool(prefix)
        and prefix[0] in directory.UHC_NAME_ROOT_ALPHABET
        and all(character in directory.UHC_TEXT_SEARCH_ALPHABET for character in prefix[1:])
    )


def _conversion_checkpoint_owner(candidate, checkpoints, identity, run_evidence):
    """Preserve unanimous ancestor checkpoint ownership when compact conversion interrupted before intake."""
    conversion = candidate.get("payload_conversion")
    if type(conversion) is dict and conversion.get("state") == "copying" and checkpoints:
        owners = {checkpoint["owner_run_id"] for checkpoint in checkpoints}
        if len(owners) == 1:
            owner = next(iter(owners))
            if owner in run_evidence.get("failed_ancestor_run_ids", ()):
                return owner
    return identity["failed_run_id"]


def _verify_postal_cursors(directory, checkpoints, identity, expected_counts, run_evidence, candidate):
    """Keep the completed plan cursor and validate any other ordinary markers."""
    endpoint = directory.UHC_PROVIDER_DIRECTORY_BASE
    source_ids = candidate["identity"]["metadata"]["source_ids"]
    ordinary_types = {checkpoint["resource_type"] for checkpoint in checkpoints}
    _require(
        len(source_ids) == 1
        and set(expected_counts) == set(directory.UHC_SUPPORTED_RESOURCES)
        and "InsurancePlan" in ordinary_types
        and expected_counts["InsurancePlan"] > 0
        and all(
            checkpoint["canonical_api_base"] == endpoint
            and _json(checkpoint["source_ids"]) == source_ids
            and type(checkpoint["source_scope_hash"]) is str
            and re.fullmatch(r"[0-9a-f]{64}", checkpoint["source_scope_hash"])
            and (
                checkpoint["resource_type"] != "InsurancePlan"
                or (checkpoint["state"] == "complete" and checkpoint["next_url"] is None)
            )
            for checkpoint in checkpoints
        ),
        "transition_checkpoint_changed",
    )
    owner_run_id = _conversion_checkpoint_owner(candidate, checkpoints, identity, run_evidence)
    _require_rest_transition_checkpoints(
        checkpoints,
        identity,
        {resource: count for resource, count in expected_counts.items() if resource in ordinary_types},
        run_evidence["status"],
        owner_run_id=owner_run_id,
    )
    return ordinary_types


def _covered_postal_counts(directory, leaves, identity, source_record, expected_counts):
    """Check the exact source, resource and partition identity for every finished leaf."""
    resource_by_kind = {
        kind: resource
        for resource in expected_counts
        if (kind := directory._uhc_partition_checkpoint_type(source_record, resource))
    }
    counts_by_resource = {}
    for leaf in leaves:
        resource = resource_by_kind.get(leaf["checkpoint_type"])
        _require(
            leaf["dataset_id"] == identity["dataset_id"]
            and leaf["acquisition_root_run_id"] == identity["acquisition_root_run_id"]
            and resource is not None
            and _valid_postal_transition_leaf(directory, source_record, resource, leaf["partition_identity"])
            and leaf["pages_fetched"] > 0
            and leaf["rows_fetched"] >= 0,
            "transition_checkpoint_changed",
        )
        counts_by_resource[resource] = counts_by_resource.get(resource, 0) + leaf["rows_fetched"]
    return counts_by_resource


async def _verify_stopped_postal_checkpoints(
    connection, policy, checkpoints, identity, expected_counts, run_evidence, candidate
):
    """Preserve finished leaves while allowing partitioned raw intake ahead of its first leaf."""
    from process.provider_directory_postal_checkpoint import ROOT as POSTAL_CHECKPOINT

    directory = import_module("process.provider_directory_fhir")
    source_ids = candidate["identity"]["metadata"]["source_ids"]
    ordinary_types = _verify_postal_cursors(directory, checkpoints, identity, expected_counts, run_evidence, candidate)
    source_by_field = {"canonical_api_base": directory.UHC_PROVIDER_DIRECTORY_BASE, "source_id": source_ids[0]}
    leaves = await connection.fetch(
        "SELECT dataset_id,acquisition_root_run_id,checkpoint_type,partition_identity,pages_fetched,rows_fetched "
        "FROM "
        + storage_relation(policy.schema, POSTAL_CHECKPOINT)
        + " WHERE dataset_id=$1 OR acquisition_root_run_id=$2 FOR SHARE NOWAIT",
        identity["dataset_id"],
        identity["acquisition_root_run_id"],
    )
    counts_by_resource = _covered_postal_counts(directory, leaves, identity, source_by_field, expected_counts)
    _require(
        bool(leaves)
        and all(counts_by_resource.get(resource, 0) <= count for resource, count in expected_counts.items())
        and all(
            count == 0
            or resource in ordinary_types
            or directory._uhc_partition_checkpoint_type(source_by_field, resource) is not None
            for resource, count in expected_counts.items()
        ),
        "transition_checkpoint_changed",
    )


def _require_rest_transition_checkpoints(
    checkpoint_rows, identity_by_field, expected_counts, run_status, *, owner_run_id=None
):
    """Retain each ordinary scan's existing cursor, owner and exact row census."""
    _require(run_status == "failed", "transition_run_changed")
    owner_run_id = identity_by_field["failed_run_id"] if owner_run_id is None else owner_run_id
    _require(
        len(checkpoint_rows) == len(expected_counts)
        and {checkpoint_record["resource_type"] for checkpoint_record in checkpoint_rows} == set(expected_counts)
        and all(
            _is_resumable_transition_checkpoint(checkpoint_record)
            and checkpoint_record["owner_run_id"] == owner_run_id
            and checkpoint_record["acquisition_root_run_id"] == identity_by_field["acquisition_root_run_id"]
            and checkpoint_record["dataset_id"] == identity_by_field["dataset_id"]
            and checkpoint_record["rows_processed"] == expected_counts[checkpoint_record["resource_type"]]
            and checkpoint_record["pages_processed"] > 0
            for checkpoint_record in checkpoint_rows
        ),
        "transition_checkpoint_changed",
    )


async def _verify_transition_inputs(
    connection,
    policy,
    candidate_by_field,
    identity_by_field,
    expected_counts,
    run_evidence,
):
    """Compare completed or resumable checkpoints with every active detached input row."""
    if candidate_by_field["contract"] == NAMED_COMPACT_CONTRACT:
        from process.provider_directory_compact_source_completion import validate_storage

        await validate_storage(connection, policy, candidate_by_field)
    renewals, companion_starts = await _verify_transition_checkpoints(
        connection,
        policy,
        identity_by_field,
        expected_counts,
        run_evidence,
        candidate_by_field,
    )
    dataset_id = identity_by_field["dataset_id"]
    resource_relation = storage_relation(policy.schema, candidate_by_field["tables"]["resource_input"]["name"])
    has_retired_observations = (
        candidate_by_field["contract"] == NAMED_COMPACT_CONTRACT or "payload_conversion" in candidate_by_field
    )
    input_rows = await connection.fetch(
        "SELECT dataset_id,resource_type,count(*) AS total,"
        "count(*) FILTER (WHERE batch_ordinal<=0 OR batch_ordinal>$3::bigint "
        "OR row_ordinal<0 OR row_ordinal>=4096) AS invalid,"
        "count(*) FILTER (WHERE batch_ordinal > greatest($1::bigint,"
        "coalesce(($2::jsonb->>resource_type)::bigint,0)) AND batch_ordinal <= $3::bigint) AS active "
        "FROM " + resource_relation + " GROUP BY dataset_id,resource_type",
        candidate_by_field["resets"].get("*", 0),
        _encoded(candidate_by_field["resets"]),
        candidate_by_field["ordinal"],
    )
    _require_transition_census(input_rows, dataset_id, expected_counts, has_retired_observations)
    for resource, _checkpoint, _payload, _observed_count in renewals:
        _require(
            not any(
                input_row["resource_type"] == resource or input_row["resource_type"].startswith(f"LU:{resource}:")
                for input_row in input_rows
            ),
            "transition_partition_input_changed",
        )
    await _verify_transition_witness(
        connection, policy, candidate_by_field, dataset_id, run_evidence.get("require_empty_checkpoints", False)
    )
    return renewals, companion_starts


def _require_transition_census(input_rows, dataset_id, expected_counts, has_retired_observations):
    """Match active checkpoint counts while retaining exactly reset-excluded observations."""
    input_counts_by_type = {input_record["resource_type"]: input_record["total"] for input_record in input_rows}
    _require(
        len(input_counts_by_type) == len(input_rows)
        and all(
            input_record["dataset_id"] == dataset_id
            and input_record["resource_type"] in expected_counts
            and input_record["active"] == expected_counts[input_record["resource_type"]]
            and (
                input_record["invalid"] == 0 and input_record["total"] >= input_record["active"]
                if has_retired_observations
                else input_record["total"] == input_record["active"]
            )
            for input_record in input_rows
        )
        and all(
            any(
                observation["resource_type"] == resource_type and observation["active"] == count
                for observation in input_rows
            )
            or count == 0
            and resource_type not in input_counts_by_type
            for resource_type, count in expected_counts.items()
        ),
        "transition_input_changed",
    )


async def has_empty_isolated_vsp_storage(directory, candidate, header):
    """Prove the locked VSP candidate has never committed normalized input."""
    identity = candidate_identity(directory, candidate)
    metadata = identity["metadata"]
    retained_metadata = _json(header["publication_metadata_json"])
    _require(
        metadata["source_ids"] == [VSP_SOURCE_ID]
        and tuple(metadata["selected_resources"]) == tuple(metadata["expected_resources"]) == VSP_RESOURCE_TYPES
        and type(retained_metadata) is dict
        and all(retained_metadata.get(field_name) == expected_value for field_name, expected_value in metadata.items()),
        "vsp_retry_candidate_changed",
    )
    binding = _json(header["candidate_storage_json"])
    async with source_owner_transaction(directory.db) as publisher:
        connection, policy = publisher.connection, publisher.policy
        isolated = _require_candidate_storage(publisher, directory, identity)
        _validate_binding(binding, identity, phases=("loading",))
        _require(
            (isolated is not None) == (binding["contract"] in ISOLATED_CONTRACTS)
            and (header["status"] == "failed" or (header["status"] == "acquiring" and isolated is not None))
            and binding["ordinal"] == 0
            and binding["resets"] in ({}, {"*": 0})
            and binding["relationship_sha256"] is None
            and binding["auxiliary"] is None,
            "vsp_retry_candidate_changed",
        )
        await _verify_tables(connection, policy, binding)
        await _verify_transition_inputs(
            connection,
            policy,
            binding,
            identity,
            dict.fromkeys(VSP_RESOURCE_TYPES, 0),
            {"status": header["status"], "require_empty_checkpoints": True},
        )
        return isolated is not None


async def has_empty_named_root_storage(directory, candidate, header):
    """Prove named loading storage is empty under the worker's admission lock."""
    binding = _json(header["candidate_storage_json"])
    if type(binding) is not dict or binding.get("contract") != NAMED_ISOLATED_CONTRACT:
        return False
    identity = candidate_identity(directory, candidate)
    if named_source_resources(identity["metadata"]["source_ids"]) is not None:
        return False
    async with source_owner_transaction(directory.db) as publisher:
        connection, policy = publisher.connection, publisher.policy
        _require_candidate_storage(publisher, directory, identity)
        _validate_binding(binding, identity, phases=("loading",))
        _require(header["resource_count"] == 0 and _is_unstarted_named_binding(binding), "transition_candidate_changed")
        await _verify_tables(connection, policy, binding)
        await _verify_transition_inputs(
            connection,
            policy,
            binding,
            identity,
            dict.fromkeys(candidate.selected_resources, 0),
            {"status": header["status"], "require_empty_checkpoints": True},
        )
    return True


async def _verify_transition_witness(connection, policy, candidate_by_field, dataset_id, require_empty):
    witness_relation = storage_relation(policy.schema, candidate_by_field["tables"]["witness_input"]["name"])
    check = f"SELECT NOT EXISTS(SELECT FROM {witness_relation} WHERE dataset_id<>$1)"
    if require_empty:
        proof_relation = storage_relation(policy.schema, "provider_directory_dataset_proof_shard")
        check = (
            f"SELECT NOT EXISTS(SELECT FROM {witness_relation}) "
            f"AND NOT EXISTS(SELECT FROM {proof_relation} WHERE dataset_id=$1)"
        )
    _require(await connection.fetchval(check, dataset_id) is True, "transition_witness_changed")


def _transition_catalog_schema(policy, old_storage, new_storage):
    """Resolve the logical catalog for the selected transition contract."""
    for packet in (old_storage, new_storage):
        _require(
            type(packet) is dict and set(packet) == {"catalog_schema", "storage_schema", "producer"},
            "transition_storage_invalid",
        )
        _validate_named_storage({key: packet[key] for key in ("catalog_schema", "storage_schema")})
        producer = packet["producer"]
        _require(
            type(producer) is list
            and len(producer) == 2
            and type(producer[0]) is str
            and re.fullmatch(r"[0-9a-f]{40}", producer[0])
            and producer[0] != "0" * 40
            and type(producer[1]) is str
            and re.fullmatch(r"sha256:[0-9a-f]{64}", producer[1])
            and producer[1] != "sha256:" + "0" * 64,
            "transition_storage_changed",
        )
    _require(
        old_storage["storage_schema"] == new_storage["storage_schema"] == policy.schema
        and old_storage["catalog_schema"] == new_storage["catalog_schema"]
        and old_storage["producer"] != new_storage["producer"],
        "transition_storage_changed",
    )
    return old_storage["catalog_schema"]


async def _validate_transition_payloads(connection, policy, candidate_by_field):
    """Validate each retained observation in bounded reads without reducing or rewriting it."""
    from process import provider_directory_generic_source_completion as completion

    directory = import_module("process.provider_directory_fhir")
    identity = candidate_by_field["identity"]
    metadata = identity["metadata"]
    contract = metadata.get(directory.RESOURCE_HASH_CONTRACT_METADATA_KEY)
    _require(contract in directory.RESOURCE_HASH_CONTRACTS, "hash_contract_changed")
    candidate = _StoredCandidate(identity, contract, metadata.get(directory.SEMANTIC_PROJECTION_AS_OF_METADATA_KEY))
    prepare, _merge = completion._resource_callbacks(directory, candidate)
    for key, columns, validate in (
        ("resource_input", RESOURCE_COLUMNS, prepare),
        ("witness_input", WITNESS_COLUMNS, completion._witness),
    ):
        async for row in completion._active_rows(connection, policy, candidate_by_field, key, columns, ordered=False):
            _require(row["dataset_id"] == identity["dataset_id"], "transition_input_changed")
            validate(row)


async def _verify_stopped_candidate(
    connection, policy, catalog_schema, identity_by_field, locked_candidate, expected_resource_counts
):
    """Check the failed acquisition lineage and candidate contents before changing storage."""
    candidate_by_field, _, is_empty_vsp_failure, has_changed_header_owner, failure_error, _, is_unstarted_named_root = (
        locked_candidate
    )
    run_evidence_by_field = await _verify_transition_run(
        connection,
        catalog_schema,
        identity_by_field,
        candidate_by_field["identity"]["metadata"]["source_ids"],
        has_changed_header_owner=has_changed_header_owner,
        is_empty_vsp_failure=is_empty_vsp_failure,
        expected_failure_error=failure_error,
    )
    run_evidence_by_field["is_unstarted_named_root"] = is_unstarted_named_root
    run_evidence_by_field["require_empty_checkpoints"] = is_empty_vsp_failure
    _require(not is_empty_vsp_failure or run_evidence_by_field["status"] == "failed", "transition_checkpoint_changed")
    transitions = await _verify_transition_inputs(
        connection,
        policy,
        candidate_by_field,
        identity_by_field,
        expected_resource_counts,
        run_evidence_by_field,
    )
    if any(expected_resource_counts.values()):
        await _validate_transition_payloads(connection, policy, candidate_by_field)
    return run_evidence_by_field, transitions


async def transition_isolated_loading_candidate_storage(
    connection, *, policy, identity_by_field, old_storage, new_storage, expected_resource_counts, manifest_replay=False
):
    """Move one verified stopped private intake to an admin-issued successor producer."""
    from process.provider_directory_generic_source_policy import GenericSourcePolicy
    from process.provider_directory_partition_successor import apply_partition_transitions

    _require(type(manifest_replay) is bool, "transition_replay_invalid")
    _require(type(policy) is GenericSourcePolicy and connection.is_in_transaction(), "transition_transaction_required")
    _require(await connection.fetchval("SHOW transaction_isolation") == "serializable", "transition_isolation_required")
    _validated_transition_identity(identity_by_field, expected_resource_counts)
    catalog_schema = _transition_catalog_schema(policy, old_storage, new_storage)
    locked_candidate = await _locked_transition_candidate(
        connection,
        policy,
        identity_by_field,
        old_storage,
        new_storage,
        expected_resource_counts,
        manifest_replay=manifest_replay,
    )
    candidate_by_field, previous_status, is_empty_vsp_failure, _, failure_error, raw_candidate, _ = locked_candidate
    run_evidence_by_field, transitions = await _verify_stopped_candidate(
        connection, policy, catalog_schema, identity_by_field, locked_candidate, expected_resource_counts
    )
    if run_evidence_by_field["is_postal_start_failure"]:
        from process.provider_directory_postal_checkpoint import provision

        await provision(connection, policy)
    successor_by_field, receipt_by_field = _transition_result(
        candidate_by_field,
        old_storage,
        new_storage,
        expected_resource_counts,
        run_evidence_by_field,
    )
    is_unstarted_named_root = run_evidence_by_field["is_unstarted_named_root"]
    should_rearm = is_unstarted_named_root or is_empty_vsp_failure and failure_error not in _RETAINED_VSP_CAPTURE_ERRORS
    is_already_applied = (
        candidate_by_field == successor_by_field
        and raw_candidate == candidate_by_field
        and not (is_unstarted_named_root and previous_status == "failed")
    )
    await _store_transition_successor(
        connection,
        policy,
        identity_by_field,
        successor_by_field,
        raw_candidate,
        previous_status,
        should_rearm,
    )
    await apply_partition_transitions(connection, policy.schema, transitions)
    return {**receipt_by_field, "already_applied": is_already_applied}


def _transition_result(candidate_by_field, old_storage, new_storage, expected_resource_counts, run_evidence_by_field):
    """Build the successor and report the stopped candidate lineage."""
    storage_by_field = {key: old_storage[key] for key in ("catalog_schema", "storage_schema")}
    contract = (
        NAMED_COMPACT_CONTRACT if candidate_by_field["contract"] == NAMED_COMPACT_CONTRACT else NAMED_ISOLATED_CONTRACT
    )
    successor_by_field = {**candidate_by_field, "contract": contract, "storage": storage_by_field}
    receipt_by_field = _transition_receipt(
        candidate_by_field, storage_by_field, successor_by_field, expected_resource_counts, run_evidence_by_field
    )
    return successor_by_field, receipt_by_field


def _transition_receipt(candidate, old_storage, successor, expected_counts, run_evidence):
    """Bind the verified input counts and both exact storage states."""
    old_candidate_by_field = {**candidate, "storage": old_storage}
    return {
        "dataset_id": candidate["identity"]["dataset_id"],
        "rows": sum(expected_counts.values()),
        "lineage_run_ids": run_evidence["lineage_run_ids"],
        "old_binding_sha256": hashlib.sha256(_encoded(old_candidate_by_field).encode()).hexdigest(),
        "new_binding_sha256": hashlib.sha256(_encoded(successor).encode()).hexdigest(),
    }


async def _store_transition_successor(
    connection, policy, identity_by_field, successor_by_field, candidate_by_field, previous_status, should_rearm
):
    """Commit only the unchanged unpublished header preimage."""
    updated = await connection.execute(
        "UPDATE "
        + storage_relation(policy.schema, HEADER)
        + " SET candidate_storage_json=$2::jsonb,status=$4 WHERE dataset_id=$1 "
        "AND candidate_storage_json IS NOT DISTINCT FROM $3::jsonb "
        "AND status=$5 AND NOT is_current AND published_at IS NULL",
        identity_by_field["dataset_id"],
        _encoded(successor_by_field),
        _encoded(candidate_by_field),
        "acquiring" if should_rearm else previous_status,
        previous_status,
    )
    _require(updated == "UPDATE 1", "transition_header_compare_failed")


def _records(rows, columns, ordinal):
    for index, row in enumerate(rows):
        _require(
            set(row) <= set(columns) and all(name in row for name in columns if name != "acquired_resource_sha256"),
            "copy_columns_changed",
        )
        yield tuple(_json_text(row.get(name)) if name.endswith("_json") else row.get(name) for name in columns) + (
            ordinal,
            index,
        )


def _json_text(value):
    """Staging workers hand over canonical JSON text already; encode only Python values."""
    return value if isinstance(value, str) else _encoded(value)


async def _copy(connection, policy, table, columns, records):
    count = 0
    for batch in copy_record_batches(records):
        try:
            status = await connection.copy_records_to_table(
                table, schema_name=policy.schema, columns=columns, records=batch
            )
        except (asyncio.CancelledError, TimeoutError) as interruption:
            await cleanup_interrupted_copy(source_copy_session(), connection, interruption)
            raise
        _require(status == f"COPY {len(batch)}", "copy_incomplete")
        count += len(batch)
    return count


async def _lock_resource_family(connection, policy, dataset_id, resource_type):
    """Serialize one family's observations and resets while sibling COPY stays independent."""
    await connection.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
        "provider-directory-source-family:" + _encoded([policy.schema, dataset_id, resource_type]),
    )


def _resource_family(resource_type):
    return resource_type.split(":", 2)[1] if resource_type.startswith("LU:") else resource_type


async def append_generic_source_batch(database, directory, candidate, resources, witnesses=()):
    """Commit payload observations and CMS raw facts in the same original savepoint."""
    identity = candidate_identity(directory, candidate)
    _require(len(resources) <= 4096 and len(witnesses) <= 4096 and bool(resources or witnesses), "batch_size_invalid")
    for batch_rows in (resources, witnesses):
        _require(
            all(resource_row.get("dataset_id") == candidate.dataset_id for resource_row in batch_rows),
            "dataset_changed",
        )
    async with source_owner_transaction(database) as publisher:
        connection, policy = publisher.connection, publisher.policy
        _require_candidate_storage(publisher, directory, identity)
        previous = await _locked(connection, policy, identity, lock="KEY SHARE")
        _require(previous is not None, "preparation_required")
        families = {_resource_family(resource_row["resource_type"]) for resource_row in (*resources, *witnesses)}
        for resource_type in sorted(families):
            await _lock_resource_family(connection, policy, candidate.dataset_id, resource_type)
        previous = await _locked(connection, policy, identity, lock="KEY SHARE")
        _require(previous is not None, "preparation_required")
        await _verify_tables_recently(connection, policy, previous)
        is_compact = previous["contract"] == NAMED_COMPACT_CONTRACT
        if is_compact:
            _require_shared_payload_format(publisher, previous["payload_format"])
            _require(not witnesses, "compact_witness_unsupported")
            previous = await _locked(connection, policy, identity, lock="NO KEY UPDATE")
        sequence_root = "resource_aliases" if is_compact else "resource_input"
        ordinal = await connection.fetchval(
            "SELECT nextval(pg_get_serial_sequence($1,'batch_ordinal'))",
            storage_relation(policy.schema, previous["tables"][sequence_root]["name"]),
        )
        _require(type(ordinal) is int and previous["ordinal"] < ordinal < 2**63 - 1, "ordinal_exhausted")
        if is_compact:
            await _append_compact_resources(connection, policy, previous, resources, ordinal)
        else:
            resources = await staged_role_rows(
                connection, policy.schema, previous["tables"]["resource_input"]["name"], previous, resources
            )
            for key, columns, batch_rows in (
                ("resource_input", RESOURCE_COLUMNS, resources),
                ("witness_input", WITNESS_COLUMNS, witnesses),
            ):
                if batch_rows:
                    await _copy(
                        connection,
                        policy,
                        previous["tables"][key]["name"],
                        (*columns, "batch_ordinal", "row_ordinal"),
                        _records(batch_rows, columns, ordinal),
                    )
        current = await _locked(connection, policy, identity, lock="NO KEY UPDATE")
        _require(current is not None and current["tables"] == previous["tables"], "storage_binding_changed")
        binding_by_field = {**current, "ordinal": max(current["ordinal"], ordinal)}
        await _store(connection, policy, identity, current, binding_by_field)
    return len(resources)


async def _append_compact_resources(connection, policy, binding, resources, ordinal):
    """Reuse the shared COPY cancellation cleanup and original owner savepoint."""
    try:
        await compact_payload.append_compact_payload_batch(
            connection,
            compact_storage.payload_tables(policy, binding["tables"]),
            resources,
            dataset_id=binding["identity"]["dataset_id"],
            batch_ordinal=ordinal,
            format_id=binding["payload_format"],
        )
    except (asyncio.CancelledError, TimeoutError) as interruption:
        await cleanup_interrupted_copy(source_copy_session(), connection, interruption)
        raise


async def reset_generic_source_family(database, directory, candidate, resource_type=None):
    """Retire old observations by a low-volume cutoff; never delete payload rows."""
    identity = candidate_identity(directory, candidate)
    _require(
        resource_type is None
        or resource_type in candidate.selected_resources
        or any(
            resource_type == directory._last_updated_partition_checkpoint_resource_type(selected)
            or resource_type
            in tuple(directory._last_updated_partition_fingerprint_resource_type(selected, number) for number in (1, 2))
            for selected in candidate.selected_resources
        ),
        "reset_scope_changed",
    )
    async with source_owner_transaction(database) as publisher:
        connection, policy = publisher.connection, publisher.policy
        _require_candidate_storage(publisher, directory, identity)
        if resource_type is not None:
            await _locked(connection, policy, identity, lock="KEY SHARE")
            await _lock_resource_family(connection, policy, candidate.dataset_id, _resource_family(resource_type))
        previous = await _locked(
            connection, policy, identity, lock="UPDATE" if resource_type is None else "NO KEY UPDATE"
        )
        _require(previous is not None, "preparation_required")
        await _verify_tables(connection, policy, previous)
        cutoff_by_family = dict(previous["resets"])
        cutoff_by_family[resource_type or "*"] = previous["ordinal"]
        binding_by_field = {**previous, "resets": cutoff_by_family}
        await _store(connection, policy, identity, previous, binding_by_field)


async def discard_generic_source_inputs(database, directory, candidate, release):
    """Reclaim only exact failed, unpublished, unattached heaps; never row DELETE."""
    async with source_owner_transaction(database) as publisher:
        connection, policy = publisher.connection, publisher.policy
        require_storage_schema(publisher, directory._schema())
        header_by_field = await connection.fetchrow(
            f"SELECT endpoint_id,acquisition_root_run_id,status,is_current,published_at,publication_metadata_json,"
            f"candidate_storage_json FROM {storage_relation(policy.schema, HEADER)} WHERE dataset_id=$1 FOR UPDATE",
            candidate.dataset_id,
        )
        _require(
            header_by_field is not None
            and header_by_field["status"] == "failed"
            and header_by_field["is_current"] is False
            and header_by_field["published_at"] is None
            and header_by_field["endpoint_id"] == candidate.endpoint_id
            and header_by_field["acquisition_root_run_id"] == candidate.acquisition_root_run_id
            and _json(header_by_field["publication_metadata_json"]).get("source_release") == release,
            "disposition_changed",
        )
        binding_by_field = _binding_json(header_by_field["candidate_storage_json"], policy.schema)
        _require(
            type(binding_by_field) is dict and type(binding_by_field.get("identity")) is dict,
            "legacy_retirement_required",
        )
        _validate_binding(binding_by_field, binding_by_field["identity"], phases=("loading", "prepared", "disposed"))
        _require(
            binding_by_field["identity"]["dataset_id"] == candidate.dataset_id
            and binding_by_field["identity"]["endpoint_id"] == candidate.endpoint_id
            and binding_by_field["identity"]["acquisition_root_run_id"] == candidate.acquisition_root_run_id
            and binding_by_field["identity"]["metadata"].get("source_release") == release,
            "disposition_changed",
        )
        if binding_by_field["phase"] == "disposed":
            return
        await _verify_tables(connection, policy, binding_by_field)
        auxiliary = binding_by_field["auxiliary"]
        receipts = list(binding_by_field["tables"].values())
        receipts = await _discard_payload_views(connection, policy, binding_by_field, receipts)
        if auxiliary is not None:
            receipts.extend(auxiliary["tables"].values())
        for table in receipts:
            await connection.execute(f"DROP TABLE {storage_relation(policy.schema, table['name'])}")
        disposed_auxiliary = (
            None
            if auxiliary is None
            else {
                "phase": "disposed",
                "tables": {},
                "sealed_sha256": auxiliary["sealed_sha256"] or hashlib.sha256(_encoded(auxiliary).encode()).hexdigest(),
            }
        )
        disposed_by_field = {**binding_by_field, "phase": "disposed", "tables": {}, "auxiliary": disposed_auxiliary}
        disposed_by_field.pop("payload_conversion", None)
        await _store(connection, policy, binding_by_field["identity"], binding_by_field, disposed_by_field)


async def _discard_payload_views(connection, policy, binding_by_field, receipts):
    """Dispose only the candidate's views and its retained alternate conversion inventory."""
    if binding_by_field["contract"] == NAMED_COMPACT_CONTRACT:
        for root in ("resource_input", RESOURCE):
            view = binding_by_field["tables"].get(root)
            if view is not None:
                await connection.execute(f"DROP VIEW {storage_relation(policy.schema, view['name'])}")
                receipts = [table for table in receipts if table != view]
    if "payload_conversion" in binding_by_field:
        from process.provider_directory_compact_source_migration import discard_conversion_tables

        await discard_conversion_tables(connection, policy, binding_by_field)
    return receipts


@asynccontextmanager
async def generic_source_relation_scope(database, directory, candidate):
    """Read/write only the exact closed candidate group in the original owner tx."""
    identity = candidate_identity(directory, candidate)
    async with source_owner_transaction(database) as publisher:
        isolated = _require_candidate_storage(publisher, directory, identity)
        binding_by_field = await _locked(publisher.connection, publisher.policy, identity, phases=("prepared",))
        _require(binding_by_field is not None, "completion_required")
        await _verify_tables(publisher.connection, publisher.policy, binding_by_field)
        relations_by_root = {name: binding_by_field["tables"][name]["name"] for name in RELATIONS}
        auxiliary = binding_by_field["auxiliary"]
        if auxiliary is not None:
            relations_by_root.update({root: proof["name"] for root, proof in auxiliary["tables"].items()})
        from process.provider_directory_generic_source_auxiliary import identity_candidate_scope
        from process.provider_directory_source_storage_binding import isolated_source_relation_scope

        async with identity_candidate_scope(publisher, binding_by_field):
            isolated_scope = isolated_source_relation_scope(publisher, relations_by_root) if isolated else nullcontext()
            with isolated_scope, directory._provider_directory_artifact_relation_scope(relations_by_root):
                yield publisher, binding_by_field


def _table_proof(row, *, private):
    columns, constraints, indexes = (_json(row[key]) for key in ("columns", "constraints", "indexes"))
    _require(
        row["relkind"] == "r"
        and row["relpersistence"] == "p"
        and not row["relrowsecurity"]
        and not row["relforcerowsecurity"]
        and row["inert"]
        and row["native_checks"]
        and row["direct_acl_closed"]
        and (private or row["readers_closed"])
        and (
            not row["parents"] and not row["relispartition"]
            if private
            else len(row["parents"]) == 1 and row["relispartition"]
        )
        and all(constraint["type"] in {"c", "p", "u"} and constraint["valid"] for constraint in constraints)
        and all(index["valid"] for index in indexes),
        "storage_custody_changed",
    )
