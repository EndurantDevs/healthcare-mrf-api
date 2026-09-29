# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Require a sealed, complete CMS publication through the existing source selector."""

import re
from datetime import date

from api.provider_directory_entities_contract import DirectoryReadError, opaque_directory_key
from api.provider_directory_source_dataset_selection import _source_local_current_published_dataset_statement
from db.models import ProviderDirectoryEndpointDataset
from process.provider_directory_admission_seal import ADMISSION_GENERIC_PROOF_SUMMARY_KEY

SOURCE = "cms-npd"
RESOURCE_TYPES = (
    "Organization",
    "Location",
    "Endpoint",
    "HealthcareService",
    "InsurancePlan",
    "Practitioner",
    "PractitionerRole",
    "OrganizationAffiliation",
)
RESOURCE_FILES = {f"{index:02}-{kind}.ndjson": kind for index, kind in enumerate(RESOURCE_TYPES, 1)}
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


def _release_counts(release):
    """Validate all eight retained members without accepting a subset or unknown file."""
    if not isinstance(release, dict) or release.get("source_id") != SOURCE:
        raise ValueError
    for field in ("manifest_sha256", "vector_sha256"):
        if not isinstance(release.get(field), str) or not _SHA256.fullmatch(release[field]):
            raise ValueError
    if date.fromisoformat(release["generated_at"]).isoformat() != release["generated_at"]:
        raise ValueError
    files = release["files"]
    if not isinstance(files, dict) or set(files) != set(RESOURCE_FILES):
        raise ValueError
    counts_by_type = {}
    for name, kind in RESOURCE_FILES.items():
        member = files[name]
        if not isinstance(member["sha256"], str) or not _SHA256.fullmatch(member["sha256"]):
            raise ValueError
        for field in ("compressed_bytes", "original_bytes", "row_count", "distinct_count"):
            if type(member[field]) is not int or not 0 <= member[field] < 2**63:
                raise ValueError
        if member["compressed_bytes"] == 0 or member["distinct_count"] > member["row_count"]:
            raise ValueError
        counts_by_type[kind] = member["distinct_count"]
    return counts_by_type


def accepted_release(dataset):
    """Bind the source release to the admission summary's exact content counts."""
    try:
        metadata = dataset["publication_metadata"]
        release = metadata["source_release"]
        counts = _release_counts(release)
        proof = metadata[ADMISSION_GENERIC_PROOF_SUMMARY_KEY]
        if (
            metadata["source_ids"] != [SOURCE]
            or proof["resource_counts"] != counts
            or set(proof["resource_hashes"]) != set(RESOURCE_TYPES)
            or proof["dataset_hash"] != dataset["dataset_hash"]
            or proof["resource_count"] != sum(counts.values())
            or dataset["resource_count"] != sum(counts.values())
        ):
            raise ValueError
        return release["vector_sha256"]
    except KeyError, TypeError, ValueError, AttributeError:
        raise DirectoryReadError(503) from None


async def accepted_cms_generation(session, key):
    """Exclude legacy fallback, candidates and non-CMS publication scopes."""
    model = ProviderDirectoryEndpointDataset
    statement = (
        _source_local_current_published_dataset_statement(((SOURCE,),))
        .where(
            model.content_proof_admission_version == 1,
            model.content_proof_admission_kind == "generic",
            model.content_proof_resource_types == sorted(RESOURCE_TYPES),
        )
        .limit(2)
    )
    publications = (await session.execute(statement)).mappings().all()
    if len(publications) != 1:
        raise DirectoryReadError(503)
    dataset = publications[0]
    release_id = accepted_release(dataset)
    generation_id = (
        None
        if key is None
        else opaque_directory_key(
            key,
            "gen_",
            SOURCE,
            dataset["endpoint_id"],
            dataset["dataset_id"],
            dataset["acquisition_root_run_id"],
            dataset["dataset_hash"],
            release_id,
            dataset["published_at"].isoformat(),
        )
    )
    return {
        "dataset_id": dataset["dataset_id"],
        "dataset_hash": dataset["dataset_hash"],
        "release_id": release_id,
        "generation_id": generation_id,
        "observed_at": dataset["published_at"],
    }
