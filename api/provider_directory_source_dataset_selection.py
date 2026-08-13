# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded dataset selection for Provider Directory source scopes."""

from sqlalchemy import and_, case, cast, func, or_, select, true, union_all
from sqlalchemy.dialects.postgresql import JSONB

from db.models import ProviderDirectoryEndpointDataset, ProviderDirectorySource, db
from process.provider_directory_admission_seal import (
    ADMISSION_KIND_GENERIC,
    ADMISSION_KIND_UHC_CANONICAL,
    ADMISSION_SEAL_VERSION,
)
from process.provider_directory_validated_publication_contract import (
    ProviderDirectoryDatasetIdentity,
)

_ADMISSION_SCHEMA = ProviderDirectoryEndpointDataset.__table__.schema


def _catalog_publication_metadata(dataset_model):
    """Return compact admitted metadata without opening the raw proof JSON."""

    digest = getattr(
        getattr(func, _ADMISSION_SCHEMA),
        "provider_directory_endpoint_dataset_admission_metadata_sha256",
    )
    sealed = and_(
        dataset_model.content_proof_admission_version == ADMISSION_SEAL_VERSION,
        dataset_model.content_proof_admission_kind.in_(
            (ADMISSION_KIND_GENERIC, ADMISSION_KIND_UHC_CANONICAL)
        ),
        dataset_model.content_proof_admission_sha256.op("~")(r"^[0-9a-f]{64}$"),
        func.jsonb_typeof(dataset_model.publication_metadata_summary_json)
        == "object",
        dataset_model.publication_metadata_sha256
        == digest(
            dataset_model.publication_metadata_summary_json,
            dataset_model.content_proof_admission_version,
            dataset_model.content_proof_admission_kind,
            dataset_model.content_proof_admission_sha256,
            dataset_model.content_proof_resource_types,
        ),
    )
    return case(
        (sealed, dataset_model.publication_metadata_summary_json),
        else_=cast(None, JSONB),
    )


def _exact_source_scope_predicate(publication_metadata, source_ids):
    metadata_source_ids = publication_metadata.op("->")("source_ids")
    requested_source_ids = cast(list(source_ids), JSONB)
    return and_(
        case(
            (func.jsonb_typeof(metadata_source_ids) == "array",
             func.jsonb_array_length(metadata_source_ids)),
            else_=-1,
        ) == len(source_ids),
        metadata_source_ids.op("@>")(requested_source_ids),
        requested_source_ids.op("@>")(metadata_source_ids),
    )


def _sealed_dataset_predicate(dataset_model):
    """Return the accepted states for the latest source-local outcome."""

    return or_(
        and_(
            dataset_model.status == "validated",
            dataset_model.is_current.is_(False),
            dataset_model.validated_at.is_not(None),
            dataset_model.published_at.is_(None),
            dataset_model.superseded_at.is_(None),
        ),
        and_(
            dataset_model.status.in_(("published", "superseded")),
            dataset_model.is_current
            == (dataset_model.status == "published"),
            dataset_model.published_at.is_not(None),
            dataset_model.superseded_at.is_not(None)
            == (dataset_model.status == "superseded"),
        ),
    )


def _current_published_dataset_predicate(dataset_model):
    """Match the authoritative dataset state used by Profile selection."""

    return and_(
        dataset_model.status == "published",
        dataset_model.is_current.is_(True),
        dataset_model.published_at.is_not(None),
        dataset_model.superseded_at.is_(None),
    )


def _source_scope_dataset_statement(
    source_ids,
    current_source_ids,
    *,
    current_published_only=False,
    other_profile_source_ids=(),
):
    dataset_model = ProviderDirectoryEndpointDataset
    source_model = ProviderDirectorySource
    admitted_metadata = _catalog_publication_metadata(dataset_model)
    # Legacy current parents predate admission seals; the locked Profile
    # attestation independently revalidates their raw source scope.
    publication_metadata = (
        case(
            (func.jsonb_typeof(admitted_metadata) == "object", admitted_metadata),
            (
                and_(
                    dataset_model.publication_metadata_summary_json.is_(None),
                    dataset_model.publication_metadata_sha256.is_(None),
                    dataset_model.content_proof_admission_version.is_(None),
                    dataset_model.content_proof_admission_kind.is_(None),
                    dataset_model.content_proof_admission_sha256.is_(None),
                    dataset_model.content_proof_resource_types.is_(None),
                    ~select(source_model.source_id).where(
                        source_model.endpoint_id == dataset_model.endpoint_id,
                        source_model.source_id.in_(other_profile_source_ids),
                    ).correlate(dataset_model).exists(),
                ),
                func.jsonb_build_object("source_ids", cast(list(source_ids), JSONB)),
            ),
            else_=cast(None, JSONB),
        )
        if current_published_only
        else admitted_metadata
    )
    catalog_metadata = select(
        publication_metadata.label("publication_metadata")
    ).correlate(dataset_model).lateral("catalog_metadata")
    publication_metadata = catalog_metadata.c.publication_metadata
    bound_endpoint_ids = (
        select(source_model.endpoint_id)
        .where(source_model.source_id.in_(source_ids),
               source_model.endpoint_id.is_not(None))
        .group_by(source_model.endpoint_id)
        .having(func.count(source_model.source_id) == len(source_ids))
    )
    return (
        select(
            dataset_model.endpoint_id.label("endpoint_id"),
            dataset_model.dataset_id.label("dataset_id"),
            dataset_model.acquisition_root_run_id.label("acquisition_root_run_id"),
            dataset_model.previous_dataset_id.label("previous_dataset_id"),
            dataset_model.dataset_hash.label("dataset_hash"),
            dataset_model.status.label("status"),
            dataset_model.is_current.label("is_current"),
            dataset_model.validated_at.label("validated_at"),
            dataset_model.published_at.label("published_at"),
            dataset_model.superseded_at.label("superseded_at"),
            dataset_model.resource_count.label("resource_count"),
            publication_metadata.label("publication_metadata"),
            current_source_ids.label("current_source_ids"),
        )
        .join(catalog_metadata, true())
        .where(
            dataset_model.endpoint_id.in_(bound_endpoint_ids),
            _exact_source_scope_predicate(publication_metadata, source_ids),
            dataset_model.resource_count >= 0,
            _current_published_dataset_predicate(dataset_model)
            if current_published_only
            else _sealed_dataset_predicate(dataset_model),
        )
        .order_by(
            func.coalesce(
                dataset_model.published_at,
                dataset_model.validated_at,
            ).desc(),
            dataset_model.dataset_id.desc(),
            dataset_model.endpoint_id.desc(),
        )
        .limit(1)
    )


def _source_local_dataset_statement(
    source_id_groups,
    *,
    current_published_only=False,
):
    dataset_model = ProviderDirectoryEndpointDataset
    source_model = ProviderDirectorySource
    current_source_ids = (
        select(func.array_agg(source_model.source_id))
        .where(source_model.endpoint_id == dataset_model.endpoint_id)
        .correlate(dataset_model)
        .scalar_subquery()
    )
    profile_source_ids = {
        source_id
        for source_ids in source_id_groups
        for source_id in source_ids
    }
    scope_statements = [
        _source_scope_dataset_statement(
            source_ids,
            current_source_ids,
            current_published_only=current_published_only,
            other_profile_source_ids=tuple(
                sorted(profile_source_ids.difference(source_ids))
            ),
        )
        for source_ids in sorted(source_id_groups)
    ]
    if len(scope_statements) == 1:
        return scope_statements[0]
    return union_all(*scope_statements)


def _source_local_current_published_dataset_statement(source_id_groups):
    """Select one bounded current published dataset per exact source scope."""

    return _source_local_dataset_statement(
        source_id_groups,
        current_published_only=True,
    )


def _current_dataset_identity_statement(dataset_ids):
    """Select only the scalar CAS identity of exact current datasets."""

    dataset_model = ProviderDirectoryEndpointDataset
    return select(
        dataset_model.endpoint_id,
        dataset_model.dataset_id,
        dataset_model.acquisition_root_run_id,
        dataset_model.dataset_hash,
        dataset_model.status,
        dataset_model.is_current,
        dataset_model.published_at,
        dataset_model.superseded_at,
    ).where(
        dataset_model.dataset_id.in_(sorted(dataset_ids)),
        _current_published_dataset_predicate(dataset_model),
    )


async def _current_dataset_identities_by_id(dataset_ids):
    """Read exact current identities and recheck their serving state."""

    if not dataset_ids:
        return {}
    query_result = await db.execute(
        _current_dataset_identity_statement(dataset_ids)
    )
    identities_by_id = {}
    for dataset_record in query_result.mappings().all():
        try:
            identity = ProviderDirectoryDatasetIdentity.from_payload(
                {
                    "endpoint_id": dataset_record.get("endpoint_id"),
                    "dataset_id": dataset_record.get("dataset_id"),
                    "dataset_hash": dataset_record.get("dataset_hash"),
                    "acquisition_root_run_id": dataset_record.get(
                        "acquisition_root_run_id"
                    ),
                }
            )
        except ValueError:
            continue
        if (
            identity is not None
            and dataset_record.get("status") == "published"
            and dataset_record.get("is_current") is True
            and dataset_record.get("published_at") is not None
            and dataset_record.get("superseded_at") is None
        ):
            identities_by_id[identity.dataset_id] = identity
    return identities_by_id


def _dataset_identity(dataset):
    """Parse one selected dataset's scalar CAS identity."""

    if dataset is None:
        return None
    try:
        return ProviderDirectoryDatasetIdentity.from_payload(
            {
                "endpoint_id": dataset.endpoint_id,
                "dataset_id": dataset.dataset_id,
                "dataset_hash": dataset.dataset_hash,
                "acquisition_root_run_id": dataset.acquisition_root_run_id,
            }
        )
    except ValueError:
        return None
