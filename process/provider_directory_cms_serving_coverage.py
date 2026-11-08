# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""One-time completeness proof for an exact published CMS directory dataset."""

from contextlib import asynccontextmanager

from sqlalchemy import text

from api.provider_directory_cms_generation import accepted_cms_generation
from api.provider_directory_cms_queries import KINDS, require_cms_bindings
from api.provider_directory_entities_contract import DirectoryReadError
from api.provider_directory_medical_groups import _schema_name

_TABLE = "provider_directory_cms_serving_coverage"
_PROOF_VERSION = 2
_DATASET_PROOF_TABLES = (
    "provider_directory_dataset_resource",
    "provider_directory_cms_npd_relationship",
)
_PROOF_TABLES = (
    *_DATASET_PROOF_TABLES,
    "provider_directory_entity_source_binding",
    "provider_directory_entity_release_evidence",
    "provider_directory_insurance_network_source_binding",
    "provider_directory_insurance_network_plan_evidence",
    "provider_directory_resource_identity",
)


async def _is_cms_covered(session, schema, generation):
    return await session.scalar(
        text(f"""SELECT EXISTS (SELECT 1 FROM {schema}.{_TABLE}
            WHERE dataset_id=:dataset_id AND release_id=:release_id
              AND dataset_hash=:dataset_hash AND published_at=:observed_at
              AND proof_version=:proof_version)"""),
        {**generation, "proof_version": _PROOF_VERSION},
    )


async def require_cms_coverage(session, schema, generation):
    """Use only a proof of this accepted dataset and release, never another generation."""
    if not await _is_cms_covered(session, schema, generation):
        raise DirectoryReadError(503)


@asynccontextmanager
async def _coverage_query_scope(session):
    """Keep proof plans scoped and preserve stricter caller limits and outer locks."""
    async with session.begin_nested():
        settings_by_name = dict(
            (
                await session.execute(
                    text(
                        "SELECT current_setting('statement_timeout') AS statement_timeout,"
                        "current_setting('lock_timeout') AS lock_timeout,"
                        "current_setting('plan_cache_mode') AS plan_cache_mode"
                    )
                )
            )
            .mappings()
            .one()
        )
        for name, bound in (("lock_timeout", "250ms"), ("statement_timeout", "15min")):
            await session.execute(
                text(
                    "SELECT set_config(:name,CASE WHEN current_setting(:name)::interval=interval '0' "
                    "OR current_setting(:name)::interval>CAST(CAST(:bound AS text) AS interval) "
                    "THEN CAST(:bound AS text) ELSE current_setting(:name) END,true)"
                ),
                {"name": name, "bound": bound},
            )
        await session.execute(text("SET LOCAL plan_cache_mode='force_custom_plan'"))
        yield
        for name, setting in settings_by_name.items():
            await session.execute(text("SELECT set_config(:name,:setting,true)"), {"name": name, "setting": setting})


async def _lock_cms_proof_inputs(session, schema, dataset_id):
    # Validated dataset rows and sealed source facts are immutable; the release
    # advisory lock serializes the only mutable witness. Keep TRUNCATE/DDL out
    # without blocking writers for other sources during the full scan.
    tables = ", ".join(f"ONLY {schema}.{name}" for name in sorted(_PROOF_TABLES))
    await session.execute(text(f"LOCK TABLE {tables} IN ACCESS SHARE MODE NOWAIT"))
    for root in _DATASET_PROOF_TABLES:
        # The scope forces custom plans so bound dataset values prune siblings.
        await session.execute(
            text(f"SELECT dataset_id FROM {schema}.{root} WHERE dataset_id=:dataset_id LIMIT 0"),
            {"dataset_id": dataset_id},
        )


async def validate_cms_candidate_coverage(session, dataset_id, release_id):
    """Keep checked evidence frozen through the source-local cutover."""

    if not session.in_transaction():
        raise ValueError("cms_serving_coverage_requires_transaction")
    schema = _schema_name()
    generation_by_field = {"dataset_id": dataset_id, "release_id": release_id}
    async with _coverage_query_scope(session):
        await _lock_cms_proof_inputs(session, schema, dataset_id)
        locked = await session.scalar(
            text("SELECT pg_try_advisory_xact_lock(hashtext('cms-npd'), hashtext(:release_id))"),
            {"release_id": release_id},
        )
        if not locked:
            raise DirectoryReadError(503)
        for kind in KINDS:
            await require_cms_bindings(session, schema, generation_by_field, kind)


async def seal_cms_candidate_coverage(fhir, candidate, release_id, dataset_hash):
    """Commit the checked generation's receipt with the source-local pointer."""

    async with _source_publication_body(fhir):
        await _insert_cms_candidate_coverage(fhir, candidate, release_id, dataset_hash)


@asynccontextmanager
async def _source_publication_body(fhir, session=None):
    """Borrow only the already authenticated original SOURCE owner; keep legacy callers."""
    from process.provider_directory_cms_serving_receipt import require_registry_receipt_owner
    from process.provider_directory_generic_source_policy import (
        bound_publisher,
        is_source_storage_required,
        source_copy_session,
        source_transaction,
    )
    from process.provider_directory_source_cms_epoch_control import capture_source_cms_epoch_controls

    if not is_source_storage_required():
        yield
        return
    publisher = bound_publisher()
    original = source_copy_session()
    if publisher.policy.schema != fhir._schema() or (session is not None and session is not original):
        raise RuntimeError("cms_serving_coverage_original_session_required")
    await require_registry_receipt_owner(original, fhir._schema())
    async with source_transaction(fhir.db) as borrowed:
        if borrowed is not original:
            raise RuntimeError("cms_serving_coverage_original_session_required")
        await capture_source_cms_epoch_controls(publisher.connection, publisher.policy)
        yield


async def _insert_cms_candidate_coverage(fhir, candidate, release_id, dataset_hash):
    """Use the caller-owned publication body; never open another physical transaction."""

    table = fhir._qt(fhir._schema(), _TABLE)
    dataset = fhir._qt(fhir._schema(), "provider_directory_endpoint_dataset")
    sealed = await fhir.db.first(
        f"INSERT INTO {table} (dataset_id, release_id, dataset_hash, published_at, created_at, proof_version) "
        "SELECT dataset_id, :release_id, dataset_hash, published_at, now(), :proof_version "
        f"FROM {dataset} WHERE dataset_id=:dataset_id AND endpoint_id=:endpoint_id "
        "AND status=:published AND is_current=true AND dataset_hash=:dataset_hash "
        "AND published_at IS NOT NULL "
        "AND publication_metadata_summary_json->'source_ids' = '[\"cms-npd\"]'::jsonb "
        "RETURNING dataset_id",
        dataset_id=candidate.dataset_id,
        endpoint_id=candidate.endpoint_id,
        published=fhir.ENDPOINT_DATASET_PUBLISHED,
        release_id=release_id,
        dataset_hash=dataset_hash,
        proof_version=_PROOF_VERSION,
    )
    if sealed is None:
        raise RuntimeError("cms_npd_coverage_cutover_changed")


async def build_cms_coverage(session):
    """Run expensive checks once after publication, outside the bounded cutover."""
    from process.provider_directory_generic_source_policy import is_source_storage_required

    if is_source_storage_required():
        raise RuntimeError("cms_source_candidate_coverage_preparation_required")
    if session.in_transaction():
        raise ValueError("cms_serving_coverage_requires_own_transaction")
    schema = _schema_name()
    async with session.begin(), _coverage_query_scope(session):
        generation = await accepted_cms_generation(session, None)
        if await _is_cms_covered(session, schema, generation):
            return generation["dataset_id"]
        await _lock_cms_proof_inputs(session, schema, generation["dataset_id"])
        locked = await session.scalar(
            text("SELECT pg_try_advisory_xact_lock(hashtext('cms-npd'), hashtext(:release_id))"),
            {"release_id": generation["release_id"]},
        )
        if not locked:
            raise DirectoryReadError(503)
        for kind in KINDS:
            await require_cms_bindings(session, schema, generation, kind)
        await session.execute(
            text(f"""INSERT INTO {schema}.{_TABLE}
                (dataset_id, release_id, dataset_hash, published_at, created_at, proof_version)
                VALUES (:dataset_id, :release_id, :dataset_hash, :observed_at, now(), :proof_version)
                ON CONFLICT (dataset_id, release_id, proof_version) DO NOTHING"""),
            {**generation, "proof_version": _PROOF_VERSION},
        )
        await require_cms_coverage(session, schema, generation)
    return generation["dataset_id"]


_CANDIDATE_TABLE = "provider_directory_cms_candidate_coverage"


def _candidate_dataset_sql(schema):
    """Read bounded admission scalars and the immutable relationship receipt."""
    return f"""SELECT d.content_proof_admission_sha256 AS admission_sha256,
        d.publication_metadata_sha256 AS metadata_sha256, r.relationship_count
        FROM {schema}.provider_directory_endpoint_dataset d
        JOIN {schema}.provider_directory_cms_npd_relationship_receipt r USING (dataset_id)
        WHERE d.dataset_id=:dataset_id AND d.endpoint_id=:endpoint_id AND d.dataset_hash=:dataset_hash
          AND ((d.status='validated' AND NOT d.is_current AND d.published_at IS NULL)
            OR (d.status='published' AND d.is_current AND d.published_at IS NOT NULL)
            OR (d.status='published' AND NOT d.is_current AND d.published_at IS NOT NULL
                AND d.validated_at IS NOT NULL AND d.superseded_at IS NULL))
          AND d.publication_metadata_summary_json->'source_ids'='["cms-npd"]'::jsonb
          AND d.content_proof_admission_version=1
          AND d.content_proof_admission_kind='generic'
          AND d.content_proof_resource_types=ARRAY['Endpoint','HealthcareService','InsurancePlan','Location',
                  'Organization','OrganizationAffiliation','Practitioner','PractitionerRole']::varchar[]
          AND d.content_proof_admission_sha256 IS NOT NULL AND d.publication_metadata_sha256 IS NOT NULL
          AND r.release_id=:release_id AND r.projection_contract='cms-npd-reference-ledger-v1'
        FOR SHARE OF d, r"""


async def assert_sealed_cms_candidate_coverage(session, schema, proof):
    """Fence one exact sealed candidate using indexed scalar reads in the caller transaction."""
    if not session.in_transaction():
        raise ValueError("cms_serving_coverage_requires_transaction")
    if proof.get("proof_version") != _PROOF_VERSION:
        raise RuntimeError("cms_npd_candidate_coverage_changed")
    sealed = (
        (
            await session.execute(
                text(f"""SELECT admission_sha256, metadata_sha256, relationship_count
            FROM {schema}.{_CANDIDATE_TABLE}
            WHERE dataset_id=:dataset_id AND endpoint_id=:endpoint_id AND release_id=:release_id
              AND dataset_hash=:dataset_hash AND proof_version=:proof_version FOR SHARE"""),
                proof,
            )
        )
        .mappings()
        .one_or_none()
    )
    current = (await session.execute(text(_candidate_dataset_sql(schema)), proof)).mappings().one_or_none()
    if sealed is None or current is None or dict(sealed) != dict(current):
        raise RuntimeError("cms_npd_candidate_coverage_changed")


async def prepare_cms_candidate_coverage(fhir, candidate, release_id, dataset_hash):
    """Validate once and commit an immutable proof before the bounded composite cutover."""
    from process.provider_directory_generic_source_policy import bound_publisher, source_transaction
    from process.provider_directory_source_coverage_control import capture_source_coverage_control

    async with source_transaction(fhir.db, isolation="repeatable read") as session:
        publisher = bound_publisher()
        await capture_source_coverage_control(publisher.connection, publisher.policy)
        return await prepare_cms_candidate_coverage_in_session(session, candidate, release_id, dataset_hash)


async def prepare_cms_candidate_coverage_in_session(session, candidate, release_id, dataset_hash):
    """Seal exact candidate coverage in the caller's existing transaction.

    This reuses the native checks without opening a session or binding SOURCE.
    The caller must independently qualify CMS custody, authority and its fence.
    No pointer is changed and returned data is not publication authority.
    """
    if not session.in_transaction():
        raise ValueError("cms_serving_coverage_requires_transaction")
    async with _coverage_query_scope(session):
        schema = _schema_name()
        proof_by_field = {
            "dataset_id": candidate.dataset_id,
            "endpoint_id": candidate.endpoint_id,
            "release_id": release_id,
            "dataset_hash": dataset_hash,
            "proof_version": _PROOF_VERSION,
        }
        current = (await session.execute(text(_candidate_dataset_sql(schema)), proof_by_field)).mappings().one_or_none()
        if current is None:
            raise RuntimeError("cms_npd_candidate_coverage_invalid")
        existing = await session.scalar(
            text(
                f"SELECT EXISTS (SELECT 1 FROM {schema}.{_CANDIDATE_TABLE} "
                "WHERE dataset_id=:dataset_id AND release_id=:release_id "
                "AND proof_version=:proof_version)"
            ),
            proof_by_field,
        )
        if not existing:
            await validate_cms_candidate_coverage(session, candidate.dataset_id, release_id)
            await session.execute(
                text(f"""INSERT INTO {schema}.{_CANDIDATE_TABLE}
                (dataset_id, endpoint_id, release_id, dataset_hash, proof_version,
                 admission_sha256, metadata_sha256, relationship_count, created_at)
                VALUES (:dataset_id, :endpoint_id, :release_id, :dataset_hash, :proof_version,
                        :admission_sha256, :metadata_sha256, :relationship_count, now())"""),
                {**proof_by_field, **current},
            )
        await assert_sealed_cms_candidate_coverage(session, schema, proof_by_field)
        return proof_by_field
