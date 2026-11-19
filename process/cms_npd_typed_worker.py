# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Worker entry points of the CMS National Provider Directory typed importer."""

from arq import create_pool

from db.models import db
from process.cms_npd_typed_import import run
from process.cms_npd_typed_schema import QUEUE_NAME
from process.ext.utils import ensure_database, my_init_db
from process.redis_config import build_redis_settings
from process.serialization import deserialize_job, serialize_job


async def startup(ctx):
    """Connect the worker to the database before it takes a job."""
    await my_init_db(db)
    ctx["context"] = {}
    await ensure_database(False)


async def shutdown(ctx):
    """Nothing to finish: a release is published inside the job or not at all."""


async def process_data(ctx, task=None):
    """Load and publish the current upstream release; unchanged releases are a no-op."""
    task_by_field = task if isinstance(task, dict) else {}
    return await run(ctx, task_by_field, str(task_by_field.get("run_id") or ""))


async def main():
    """Queue one import."""
    redis = await create_pool(
        build_redis_settings(),
        job_serializer=serialize_job,
        job_deserializer=deserialize_job,
    )
    await redis.enqueue_job("process_data", {}, _queue_name=QUEUE_NAME)
