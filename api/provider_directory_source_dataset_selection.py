# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded dataset selection for Provider Directory source scopes."""

from sqlalchemy import and_, case, cast, func, or_, select, union_all
from sqlalchemy.dialects.postgresql import JSONB

from db.models import ProviderDirectoryEndpointDataset, ProviderDirectorySource


def _exact_source_scope_predicate(dataset_model, source_ids):
    metadata_source_ids = cast(
        dataset_model.publication_metadata_json,
        JSONB,
    )["source_ids"]
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


def _source_scope_dataset_statement(source_ids, current_source_ids):
    dataset_model = ProviderDirectoryEndpointDataset
    source_model = ProviderDirectorySource
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
            dataset_model.dataset_hash.label("dataset_hash"),
            dataset_model.status.label("status"),
            dataset_model.is_current.label("is_current"),
            dataset_model.validated_at.label("validated_at"),
            dataset_model.published_at.label("published_at"),
            dataset_model.superseded_at.label("superseded_at"),
            dataset_model.resource_count.label("resource_count"),
            dataset_model.publication_metadata_json.label("publication_metadata"),
            current_source_ids.label("current_source_ids"),
        )
        .where(
            dataset_model.endpoint_id.in_(bound_endpoint_ids),
            _exact_source_scope_predicate(dataset_model, source_ids),
            dataset_model.resource_count >= 0,
            or_(
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
            ),
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


def _source_local_dataset_statement(source_id_groups):
    dataset_model = ProviderDirectoryEndpointDataset
    source_model = ProviderDirectorySource
    current_source_ids = (
        select(func.array_agg(source_model.source_id))
        .where(source_model.endpoint_id == dataset_model.endpoint_id)
        .correlate(dataset_model)
        .scalar_subquery()
    )
    scope_statements = [
        _source_scope_dataset_statement(source_ids, current_source_ids)
        for source_ids in sorted(source_id_groups)
    ]
    if len(scope_statements) == 1:
        return scope_statements[0]
    return union_all(*scope_statements)
