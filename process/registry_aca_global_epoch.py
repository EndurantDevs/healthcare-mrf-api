# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Protected full MRF epochs, independent of any reviewed logical network scope."""

import hashlib
import json
from dataclasses import asdict, dataclass
from uuid import UUID

from sqlalchemy import text

from process.mrf_address_publication import ADDRESS_COVERAGE_CONTRACT, REFERENCES, require_address_coverage
from process.mrf_publication_receipt import SUMMARY_INPUTS, require_completed_publication
from process.network_address_projection import _identifier
from process.network_cms_registry_address_capture import native_driver
from process.network_legacy_membership_source import _validated_archive
from process.reference_family_archive import (
    TYPED_MRF_CONTRACT,
    ReferenceFamilyPreparedSource,
    ReferenceFamilyValidationReceipt,
    _canonical_json,
    _source_metadata,
    mrf_publication_digest,
    prepare_reference_family_activation,
    protected_publisher_owner,
    reference_family_profile_contract,
    validate_reference_family_manifest,
    validate_reference_family_validation_receipt,
)
from process.reference_family_result_generation import (
    validate_reference_family_result_generation_authority,
    validate_reference_family_serving_generation,
)
from process.registry_source_recipe_store import _decode_ownership, _unique_document

CONTRACT = "registry-aca-global-epoch/v1"
PUBLICATION_CONTRACT = "reference-family-serving-generation.v1"
MAX_BYTES = 524288
_PREFIX = CONTRACT + ":"


def _invalid():
    raise ValueError("registry_aca_global_epoch_invalid")


def _roles(runtime_roles):
    if (
        type(runtime_roles) is not tuple
        or not 1 <= len(runtime_roles) <= 32
        or any(type(role) is not str for role in runtime_roles)
        or tuple(sorted(set(runtime_roles))) != runtime_roles
    ):
        _invalid()
    for role in runtime_roles:
        _identifier(role)


def _publication(publication):
    if (
        type(publication) is not dict
        or set(publication) != {"contract", "importer_id", "serving_generation"}
        or publication["contract"] != PUBLICATION_CONTRACT
        or publication["importer_id"] != "mrf"
    ):
        _invalid()
    serving = validate_reference_family_serving_generation(publication["serving_generation"])
    if publication["serving_generation"] != serving.as_dict():
        _invalid()
    return serving


def _oid(oid):
    if type(oid) is not int or not 0 < oid <= 4294967295:
        _invalid()


def _completion(completion, publication):
    if type(completion) is not dict or set(completion) != {
        "attempt_id",
        "import_id",
        "generation",
        "summary_oid",
        "summary_inputs",
        "native_address_coverage",
    }:
        _invalid()
    attempt = completion["attempt_id"]
    if type(attempt) is not str or str(UUID(attempt)) != attempt or not UUID(attempt).int:
        _invalid()
    import_id = completion["import_id"]
    if type(import_id) is not str or not 1 <= len(import_id.encode()) <= 256 or not import_id.isprintable():
        _invalid()
    _oid(completion["summary_oid"])
    inputs = completion["summary_inputs"]
    if type(inputs) is not dict or set(inputs) != set(SUMMARY_INPUTS):
        _invalid()
    for oid in inputs.values():
        _oid(oid)
    _generation(completion["generation"], publication)
    _coverage(completion["native_address_coverage"])


def _generation(generation, publication):
    if type(generation) is not dict or set(generation) != {
        "importer_id",
        "local_lineage_id",
        "local_generation",
        "serving_generation",
        "relation_oids",
    }:
        _invalid()
    serving = _publication(publication)
    if type(generation["serving_generation"]) is not dict:
        _invalid()
    authority = validate_reference_family_result_generation_authority(
        {
            **{
                key: generation[key] for key in ("importer_id", "local_lineage_id", "local_generation", "relation_oids")
            },
            **generation["serving_generation"],
        }
    )
    if generation != authority.as_dict() or authority.importer_id != "mrf" or authority.serving_generation != serving:
        _invalid()


def _coverage(coverage):
    if type(coverage) is not dict or set(coverage) != {"contract", "archive_name", "archive_oid", "tables"}:
        _invalid()
    if coverage["contract"] == ADDRESS_COVERAGE_CONTRACT:
        require_address_coverage(coverage)
        return
    if (
        coverage["contract"] != "mrf-address-coverage.indexed-set.v2"
        or coverage["archive_name"] != "address_archive_v2"
    ):
        _invalid()
    _oid(coverage["archive_oid"])
    tables = coverage["tables"]
    if type(tables) is not dict or set(tables) != set(REFERENCES):
        _invalid()
    for counts in tables.values():
        if type(counts) is not dict or set(counts) != {"rows", "uncovered"}:
            _invalid()
        if any(type(count) is not int or not 0 <= count <= 9223372036854775807 for count in counts.values()):
            _invalid()
    require_address_coverage(coverage)


def _metadata(prepared, runtime_roles):
    _roles(runtime_roles)
    metadata = prepared.manifest.source_metadata
    if type(metadata) is not dict or set(metadata) != {"registry_aca_global_epoch"}:
        _invalid()
    epoch_map = metadata["registry_aca_global_epoch"]
    if (
        type(epoch_map) is not dict
        or set(epoch_map) != {"contract", "runtime_roles", "publication", "completed_publication"}
        or epoch_map["contract"] != CONTRACT
        or epoch_map["runtime_roles"] != list(runtime_roles)
    ):
        _invalid()
    serving = _publication(epoch_map["publication"])
    _completion(epoch_map["completed_publication"], epoch_map["publication"])
    completion = epoch_map["completed_publication"]
    coverage = completion["native_address_coverage"]
    auxiliary = prepared.manifest.auxiliary
    if coverage["contract"] == ADDRESS_COVERAGE_CONTRACT or "address_coverage" in auxiliary:
        if auxiliary.get("address_coverage") != coverage or auxiliary.get(
            "publication_sha256"
        ) != mrf_publication_digest(completion):
            _invalid()
    if prepared.manifest.source_serving_generation != serving:
        _invalid()


@dataclass(frozen=True)
class PinnedACAGlobalEpoch:
    """A complete protected physical epoch; no logical mapping or import-ID filter."""

    prepared: ReferenceFamilyPreparedSource
    validation: ReferenceFamilyValidationReceipt
    runtime_roles: tuple[str, ...]

    def __post_init__(self):
        _validated_archive(self)
        _metadata(self.prepared, self.runtime_roles)
        if (
            self.prepared.manifest.importer_id != "mrf"
            or self.prepared.manifest.publication_authority != "tracked-generation"
            or self.validation.profile_contract != TYPED_MRF_CONTRACT
            or reference_family_profile_contract(self.prepared.manifest) != TYPED_MRF_CONTRACT
        ):
            _invalid()
        if len(_canonical_json(self.as_dict())) > MAX_BYTES:
            _invalid()

    def as_dict(self):
        """Return the complete closed receipt without conferring native authority."""
        ownership = asdict(self.prepared.ownership)
        ownership["dataset_id"] = str(self.prepared.ownership.dataset_id)
        ownership["relation_oids"] = [list(pair) for pair in self.prepared.ownership.relation_oids]
        ownership["sequence_oids"] = [list(entry) for entry in self.prepared.ownership.sequence_oids]
        return {
            "contract": CONTRACT,
            "prepared": {"manifest": self.prepared.manifest.as_dict(), "ownership": ownership},
            "validation": self.validation.as_dict(),
            "runtime_roles": list(self.runtime_roles),
        }

    @property
    def generation_id(self):
        """Bind the full original capture, immutable clone and native validation."""
        return hashlib.sha256(_canonical_json(self.as_dict())).hexdigest()


def decode_pinned_aca_global_epoch(document):
    """Decode bounded closed evidence; callers must still perform native admission."""
    if type(document) is str:
        if len(document.encode()) > MAX_BYTES:
            _invalid()
        document = json.loads(document, object_pairs_hook=_unique_document)
    if (
        type(document) is not dict
        or set(document) != {"contract", "prepared", "validation", "runtime_roles"}
        or document["contract"] != CONTRACT
        or type(document["runtime_roles"]) is not list
        or type(document["prepared"]) is not dict
        or set(document["prepared"]) != {"manifest", "ownership"}
    ):
        _invalid()
    if len(_canonical_json(document)) > MAX_BYTES:
        _invalid()
    prepared = ReferenceFamilyPreparedSource(
        validate_reference_family_manifest(document["prepared"]["manifest"]),
        _decode_ownership(document["prepared"]["ownership"]),
    )
    return PinnedACAGlobalEpoch(
        prepared, validate_reference_family_validation_receipt(document["validation"]), tuple(document["runtime_roles"])
    )


async def capture_registry_aca_global_metadata(session, publication, *, schema, runtime_roles):
    """Bind the original automatic publication and completed proof under the same locks."""
    _roles(runtime_roles)
    _publication(publication)
    completed = await require_completed_publication(session, schema, native_set=True)
    completion_map = {
        name: completed[name]
        for name in ("import_id", "generation", "summary_oid", "summary_inputs", "native_address_coverage")
    }
    completion_map["attempt_id"] = str(completed["attempt_id"])
    _completion(completion_map, publication)
    return _source_metadata(
        {
            "registry_aca_global_epoch": {
                "contract": CONTRACT,
                "runtime_roles": list(runtime_roles),
                "publication": publication,
                "completed_publication": completion_map,
            }
        }
    )[0]


async def _native_epoch(session, epoch):
    # Shared native custody remains independent of this metadata/stamp format.
    from process.network_legacy_membership_source import _require_native_archive

    await _require_native_archive(await native_driver(session), epoch.prepared, epoch.validation, epoch.runtime_roles)


async def admit_registry_aca_global_epoch(session, prepared, *, runtime_roles):
    """Validate and stamp inside the original protected clone transaction."""
    if not session.in_transaction():
        _invalid()
    _metadata(prepared, runtime_roles)
    owner_oid = await protected_publisher_owner(session)
    validation = await prepare_reference_family_activation(
        session,
        ownership=prepared.ownership,
        manifest=prepared.manifest,
        package_id=hashlib.sha256(_canonical_json(prepared.manifest.as_dict())).hexdigest(),
        profile_contract=reference_family_profile_contract(prepared.manifest),
        sealed_owner_oid=owner_oid,
    )
    epoch = PinnedACAGlobalEpoch(prepared, validation, runtime_roles)
    await _native_epoch(session, epoch)
    encoded = _PREFIX + _canonical_json(epoch.as_dict()).decode()
    current = await session.scalar(
        text("SELECT obj_description(CAST(:oid AS oid),'pg_namespace')"), {"oid": prepared.ownership.schema_oid}
    )
    if current not in (None, encoded):
        _invalid()
    await (await native_driver(session)).execute(
        f'COMMENT ON SCHEMA "{prepared.ownership.schema_name}" IS ' + "'" + encoded.replace("'", "''") + "'"
    )
    return epoch


async def require_registry_aca_global_epoch(session, prepared, *, runtime_roles):
    """Reprove the protected schema stamp and exact native closure after commit."""
    if not session.in_transaction():
        _invalid()
    _roles(runtime_roles)
    encoded = await session.scalar(
        text("SELECT obj_description(CAST(:oid AS oid),'pg_namespace')"), {"oid": prepared.ownership.schema_oid}
    )
    if type(encoded) is not str or not encoded.startswith(_PREFIX) or len(encoded.encode()) > MAX_BYTES + len(_PREFIX):
        _invalid()
    epoch = decode_pinned_aca_global_epoch(encoded[len(_PREFIX) :])
    if (
        epoch.prepared != prepared
        or epoch.runtime_roles != runtime_roles
        or encoded != _PREFIX + _canonical_json(epoch.as_dict()).decode()
    ):
        _invalid()
    await _native_epoch(session, epoch)
    return epoch
