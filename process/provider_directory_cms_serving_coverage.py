# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""One-time completeness proof for an exact published CMS directory dataset."""

from sqlalchemy import text

from api.provider_directory_cms_generation import accepted_cms_generation
from api.provider_directory_cms_queries import KINDS, require_cms_bindings
from api.provider_directory_entities_contract import DirectoryReadError
from api.provider_directory_medical_groups import _schema_name

_TABLE = "provider_directory_cms_serving_coverage"


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
