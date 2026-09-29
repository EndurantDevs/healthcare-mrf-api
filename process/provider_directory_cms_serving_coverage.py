# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""One-time completeness proof for an exact published CMS directory dataset."""

from sqlalchemy import text

from api.provider_directory_cms_generation import accepted_cms_generation
from api.provider_directory_cms_queries import KINDS, require_cms_bindings
from api.provider_directory_entities_contract import DirectoryReadError
from api.provider_directory_medical_groups import _schema_name

_TABLE = "provider_directory_cms_serving_coverage"
_PROOF_TABLES = (
    "provider_directory_dataset_resource",
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
              AND dataset_hash=:dataset_hash AND published_at=:observed_at)"""),
        generation,
    )


async def require_cms_coverage(session, schema, generation):
    """Use only a proof of this accepted dataset and release, never another generation."""
    if not await _is_cms_covered(session, schema, generation):
        raise DirectoryReadError(503)


async def _lock_cms_proof_inputs(session, schema):
    # ponytail: shared table locks block unrelated writes during validation; use source-scoped write guards if needed.
    tables = ", ".join(f"{schema}.{name}" for name in _PROOF_TABLES)
    await session.execute(text(f"LOCK TABLE {tables} IN SHARE MODE"))


async def validate_cms_candidate_coverage(session, dataset_id, release_id):
    """Keep checked evidence frozen through the source-local cutover."""

    if not session.in_transaction():
        raise ValueError("cms_serving_coverage_requires_transaction")
    schema = _schema_name()
    generation_by_field = {"dataset_id": dataset_id, "release_id": release_id}
    await session.execute(text("SET LOCAL statement_timeout = '15min'"))
    await session.execute(text("SET LOCAL lock_timeout = '5s'"))
    await _lock_cms_proof_inputs(session, schema)
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

    table = fhir._qt(fhir._schema(), _TABLE)
    dataset = fhir._qt(fhir._schema(), "provider_directory_endpoint_dataset")
    sealed = await fhir.db.first(
        f"INSERT INTO {table} (dataset_id, release_id, dataset_hash, published_at, created_at) "
        "SELECT dataset_id, :release_id, dataset_hash, published_at, now() "
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
    )
    if sealed is None:
        raise RuntimeError("cms_npd_coverage_cutover_changed")


async def build_cms_coverage(session):
    """Run expensive checks once after publication, outside the bounded cutover."""
    if session.in_transaction():
        raise ValueError("cms_serving_coverage_requires_own_transaction")
    schema = _schema_name()
    async with session.begin():
        await session.execute(text("SET LOCAL statement_timeout = '15min'"))
        await session.execute(text("SET LOCAL lock_timeout = '5s'"))
        generation = await accepted_cms_generation(session, None)
        if await _is_cms_covered(session, schema, generation):
            return generation["dataset_id"]
        await _lock_cms_proof_inputs(session, schema)
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
                (dataset_id, release_id, dataset_hash, published_at, created_at)
                VALUES (:dataset_id, :release_id, :dataset_hash, :observed_at, now())
                ON CONFLICT (dataset_id, release_id) DO NOTHING"""),
            generation,
        )
        await require_cms_coverage(session, schema, generation)
    return generation["dataset_id"]
