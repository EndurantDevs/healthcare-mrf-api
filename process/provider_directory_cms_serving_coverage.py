# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""One-time completeness proof for an exact published CMS directory dataset."""

from sqlalchemy import text

from api.provider_directory_cms_generation import accepted_cms_generation
from api.provider_directory_cms_queries import KINDS, require_cms_bindings
from api.provider_directory_entities_contract import DirectoryReadError
from api.provider_directory_medical_groups import _schema_name

_TABLE = "provider_directory_cms_serving_coverage"
_PROOF_VERSION = 2
_PROOF_TABLES = (
    "provider_directory_dataset_resource",
    "provider_directory_entity_source_binding",
    "provider_directory_entity_release_evidence",
    "provider_directory_insurance_network_source_binding",
    "provider_directory_insurance_network_plan_evidence",
    "provider_directory_resource_identity",
    "provider_directory_cms_npd_relationship",
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


async def _lock_cms_proof_inputs(session, schema):
    # Validated dataset rows and sealed source facts are immutable; the release
    # advisory lock serializes the only mutable witness. Keep TRUNCATE/DDL out
    # without blocking writers for other sources during the full scan.
    tables = ", ".join(f"{schema}.{name}" for name in _PROOF_TABLES)
    await session.execute(text(f"LOCK TABLE {tables} IN ACCESS SHARE MODE"))


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
            OR (d.status='published' AND d.is_current AND d.published_at IS NOT NULL))
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
    schema = _schema_name()
    proof_by_field = {
        "dataset_id": candidate.dataset_id,
        "endpoint_id": candidate.endpoint_id,
        "release_id": release_id,
        "dataset_hash": dataset_hash,
        "proof_version": _PROOF_VERSION,
    }
    async with fhir.db.session() as session:
        await session.execute(text("SET LOCAL lock_timeout = '5s'"))
        await session.execute(text("SET LOCAL statement_timeout = '15min'"))
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
