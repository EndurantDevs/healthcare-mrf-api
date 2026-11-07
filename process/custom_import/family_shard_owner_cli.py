# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fixed trusted owner entry: original protected inputs, existing owner factory."""

import argparse
import asyncio
import base64
import hashlib
import json
import os
import shutil
import time
from pathlib import Path

from process.custom_import import family_shard_handoff as handoff

PROFILE_ENV = "HLTHPRT_CUSTOM_IMPORT_OWNER_PROFILE_JSON"
NODE_ENV = "HLTHPRT_CUSTOM_IMPORT_OWNER_NODE_NAME"
PROFILE_FIELDS = frozenset(
    {
        "contract",
        "image",
        "namespace",
        "node_name",
        "pvc",
        "service_account",
        "image_pull_secret",
        "database_configmap",
        "database_secret",
        "owner_role",
        "verifying_key",
        "cpu_millis",
        "memory_bytes",
        "disk_bytes",
        "sql_memory_bytes",
        "reserve_memory_bytes",
        "work_mem_mb",
        "maintenance_work_mem_mb",
        "source_limits",
    }
)


def source_limits(document):
    """Build the existing typed phase budgets from a closed deployment profile."""
    from process.custom_import.family_block_codec import FamilyBlockLimits
    from process.custom_import.family_shard_allocate import SourceAllocationLimits
    from process.custom_import.family_shard_capture import SourcePreparationLimits
    from process.custom_import.family_shard_facts import SourceFactLimits
    from process.custom_import.family_shard_input import ShardInputLimits
    from process.custom_import.family_shard_source_stage import SourceStageLimits
    from process.custom_import.family_shard_worker import SourceWorkerLimits

    if type(document) is not dict or document.keys() != {
        "input",
        "plan",
        "allocation",
        "facts",
        "worker",
        "maximum_workers",
        "checkpoint",
    }:
        handoff.unavailable()
    worker = document["worker"]
    if (
        worker.keys() != {"facts", "output", "audit", "maximum_metadata_rows", "maximum_metadata_bytes"}
        or document["allocation"].keys() != {"spool", "maximum_batch_records", "maximum_batch_bytes"}
        or worker["output"].get("spool_directory") is not None
    ):
        handoff.unavailable()
    return SourceStageLimits(
        SourcePreparationLimits(ShardInputLimits(**document["input"]), SourceFactLimits(**document["plan"])),
        SourceAllocationLimits(
            SourceFactLimits(**document["allocation"]["spool"]),
            document["allocation"]["maximum_batch_records"],
            document["allocation"]["maximum_batch_bytes"],
        ),
        SourceFactLimits(**document["facts"]),
        SourceWorkerLimits(
            SourceFactLimits(**worker["facts"]),
            FamilyBlockLimits(**worker["output"]),
            SourceFactLimits(**worker["audit"]),
            worker["maximum_metadata_rows"],
            worker["maximum_metadata_bytes"],
        ),
        document["maximum_workers"],
        SourceFactLimits(**document["checkpoint"]),
    )


def profile(environment=os.environ):
    """Validate the fixed deployment identity and aggregate resource admission."""
    raw = environment.get(PROFILE_ENV)
    if type(raw) is not str:
        handoff.unavailable()
    profile_document = handoff.document(raw.encode("ascii"))
    if (
        type(profile_document) is not dict
        or profile_document.keys() not in (PROFILE_FIELDS, PROFILE_FIELDS | {"reader_activation"})
        or profile_document["contract"] != "custom-import/owner-profile/v1"
    ):
        handoff.unavailable()
    for name in (
        "cpu_millis",
        "memory_bytes",
        "disk_bytes",
        "sql_memory_bytes",
        "reserve_memory_bytes",
        "work_mem_mb",
        "maintenance_work_mem_mb",
    ):
        if type(profile_document[name]) is not int or profile_document[name] <= 0:
            handoff.unavailable()
    import re

    if (
        type(profile_document["node_name"]) is not str
        or re.fullmatch(r"[a-z0-9](?:[-.a-z0-9]{0,251}[a-z0-9])?", profile_document["node_name"]) is None
        or environment.get(NODE_ENV) != profile_document["node_name"]
    ):
        handoff.unavailable()
    for name in ("namespace", "pvc", "service_account", "image_pull_secret", "database_configmap", "database_secret"):
        if (
            type(profile_document[name]) is not str
            or re.fullmatch(r"[a-z0-9](?:[-a-z0-9]{0,61}[a-z0-9])?", profile_document[name]) is None
        ):
            handoff.unavailable()
    if (
        type(profile_document["owner_role"]) is not str
        or re.fullmatch(r"[a-z_][a-z0-9_]{0,62}", profile_document["owner_role"]) is None
    ):
        handoff.unavailable()
    if (
        type(profile_document["image"]) is not str
        or re.fullmatch(r"[a-z0-9][a-z0-9./_-]+@sha256:[0-9a-f]{64}", profile_document["image"]) is None
    ):
        handoff.unavailable()
    try:
        key = base64.b64decode(profile_document["verifying_key"] + "=", altchars=b"-_", validate=True)
        if len(key) != 32 or base64.urlsafe_b64encode(key).decode().rstrip("=") != profile_document["verifying_key"]:
            handoff.unavailable()
    except TypeError, ValueError:
        handoff.unavailable()
    limits = source_limits(profile_document["source_limits"])
    from process.custom_import.family_block_reader import activation_profile

    activation_profile(profile_document)
    _require_aggregate_budget(profile_document, limits)
    return profile_document, key, limits


def require_profile(signed, profile_document):
    """Require the descriptor to match the exact deployment profile and image."""
    descriptor = signed["descriptor"]
    if (
        descriptor["owner_profile_sha256"] != hashlib.sha256(handoff.canonical(profile_document)).hexdigest()
        or descriptor["owner_image"] != profile_document["image"]
    ):
        handoff.unavailable()


def require_capacity(profile_document, *, parent=handoff.OWNER_PARENT):
    # Admission uses current node availability, while the Job gets hard CPU/RSS
    # limits. Persistent PVC accounting includes retained/provisional duplicates.
    """Admit current node memory and persistent disk before owner execution."""
    available = None
    with open("/proc/meminfo", encoding="ascii") as stream:
        for line in stream:
            if line.startswith("MemAvailable:"):
                available = int(line.split()[1]) * 1024
                break
    if (
        available is None
        or available
        < profile_document["memory_bytes"]
        + profile_document["sql_memory_bytes"]
        + profile_document["reserve_memory_bytes"]
        or shutil.disk_usage(parent).free < profile_document["disk_bytes"]
    ):
        handoff.unavailable()


def _owner_inputs(handoff_id, attempt_sha256, key):
    """Reopen exact signed custody before any database connection or write."""
    signed, bearer = handoff.reopen_job(handoff_id)
    child = None
    if attempt_sha256 is None:
        handoff.verify(signed, verifying_key=key, bearer=bearer, now=time.time())
    else:
        from process.custom_import import family_shard_owner_attempt as attempts
        from process.custom_import import family_shard_owner_attempt_files as attempt_files

        child, bearer = attempt_files.reopen(signed, attempt_sha256, root=handoff.OWNER_INPUT)
        attempts.verify(child, signed, verifying_key=key, now=time.time(), token=bearer)
    return signed, bearer, child


async def run_owner(handoff_id, database, *, attempt_sha256=None):
    """Execute the original protected request through the existing owner factory."""
    from process.custom_import.family_block_reader import ReaderActivation, activation_profile
    from process.custom_import.family_shard_owner_access import require_owner_connection
    from process.custom_import.family_shard_owner_job import OwnerShardJob, advance_owner_shard_job
    from process.custom_import.family_shard_plan import PlanWork
    from process.custom_import.family_shard_production import original_request, require_native, run_original_owner
    from process.custom_import.family_shard_sessions import import_session_factory
    from process.custom_import.family_shard_terminal_cleanup import bind as bind_terminal_custody

    profile_document, key, limits = profile()
    signed, bearer, child = _owner_inputs(handoff_id, attempt_sha256, key)
    require_profile(signed, profile_document)
    require_capacity(profile_document)
    await database.connect()
    try:
        factory = import_session_factory(
            database.session_factory,
            work_mem_mb=profile_document["work_mem_mb"],
            maintenance_work_mem_mb=profile_document["maintenance_work_mem_mb"],
        )
        async with factory() as session, session.begin():
            await require_owner_connection(session, profile_document["owner_role"])
        if child is not None and child["descriptor"]["authorization"] is not None:
            from process.custom_import.family_shard_production import owner_continuation_request

            request = await owner_continuation_request(
                factory,
                signed["descriptor"],
                child["descriptor"]["authorization"],
                bearer,
                start=child["descriptor"]["kind"] == "continue",
            )
            original = None
        else:
            original = await original_request(factory, signed["descriptor"], bearer)
            require_native(signed["descriptor"], original.native)
            request = original.build_request
        registered_paths = []
        reader_profile = activation_profile(profile_document)
        reader_activation = None if reader_profile is None else ReaderActivation(reader_profile, database)
        job = OwnerShardJob(
            request,
            work=PlanWork(handoff.OWNER_PARENT / "owner", limits.facts, registered_paths.append),
            source_limits=limits,
            projection_work_mem_mb=profile_document["work_mem_mb"],
            reader_activation=reader_activation,
        )
        bind_terminal_custody(job, signed, profile_document, factory)
        if original is None:
            from process.custom_import.family_shard_production import run_continued_owner

            return await run_continued_owner(factory, request, job)
        return await run_original_owner(factory, original, job, advance=advance_owner_shard_job)
    finally:
        await database.disconnect()


def main(argv=None):
    """Run one fixed owner identity and emit only a bounded safe receipt."""
    parser = argparse.ArgumentParser(description="Run one custody-bound trusted owner")
    parser.add_argument("handoff_id")
    parser.add_argument("--attempt-sha256")
    arguments = parser.parse_args(argv)
    from db.connection import db
    from process.custom_import.snowflake_operator_cli import _receipt

    try:
        options = {} if arguments.attempt_sha256 is None else {"attempt_sha256": arguments.attempt_sha256}
        job_receipt = asyncio.run(run_owner(arguments.handoff_id, db, **options))
        print(_receipt(job_receipt))
        return 0
    except Exception:
        # No exception, request body, descriptor or credential enters stdout/stderr.
        print('{"status":"owner_unavailable"}')
        return 1


def _disk_allowance(limits):
    """Reserve declared overlapping work, including the complete retained cohort."""
    from process.custom_import.family_shard_source_checkpoint import _ROLES
    from process.custom_import.family_shard_source_journal import _OWNER_SLOTS, _SOURCE_SLOTS

    # Original input, plan and temporary part metadata coexist during preparation.
    # Allocation/metadata/events/checkpoints share the journal; common COPY is separate.
    spools = (
        limits.preparation.input.max_spool_bytes,
        limits.preparation.plan.max_spool_bytes,
        limits.preparation.plan.max_spool_bytes,
        limits.allocation.spool.max_spool_bytes,
        limits.allocation.spool.max_spool_bytes,
        limits.checkpoint.max_spool_bytes,
    )
    # Each recovery slot includes its prior attempts through retained_work_limits.
    # READY can fill every copied role before its final aggregate check. Its
    # descriptor, metadata scratch, original route and worker checkpoint are separate.
    fact_roots = len(_OWNER_SLOTS) + len(_SOURCE_SLOTS) + len(_ROLES) + 4
    retained_workers = limits.preparation.input.shard_count * (
        limits.worker.output.max_spool_bytes + limits.worker.audit.max_spool_bytes
    )
    transient_workers = limits.maximum_workers * (
        limits.worker.facts.max_spool_bytes + limits.worker.maximum_metadata_bytes
    )
    # Preserve the existing duplication margin after counting independent owners.
    # This admission allowance is not a filesystem quota or measured physical peak.
    return 2 * (sum(spools) + fact_roots * limits.facts.max_spool_bytes + retained_workers + transient_workers)


def _require_aggregate_budget(profile_document, limits):
    """Account simultaneous workers, retained duplicates, journals and SQL."""
    worker_memory = (
        sum(
            bound.memory_bytes + bound.max_record_bytes
            for bound in (limits.worker.facts, limits.worker.output, limits.worker.audit)
        )
        + limits.worker.maximum_metadata_bytes
    )
    memory = (
        limits.maximum_workers * worker_memory
        + sum(
            bound.memory_bytes + bound.max_record_bytes
            for bound in (limits.preparation.plan, limits.allocation.spool, limits.facts, limits.checkpoint)
        )
        + limits.preparation.input.chunk_bytes
    )
    if (
        limits.maximum_workers * 1000 > profile_document["cpu_millis"]
        or _disk_allowance(limits) > profile_document["disk_bytes"]
        or memory + profile_document["reserve_memory_bytes"] > profile_document["memory_bytes"]
        or profile_document["cpu_millis"] > 20_000
        or profile_document["memory_bytes"] % 1024**2
    ):
        handoff.unavailable()
    if (
        profile_document["sql_memory_bytes"]
        < (profile_document["work_mem_mb"] + profile_document["maintenance_work_mem_mb"]) * 1024**2
    ):
        handoff.unavailable()


if __name__ == "__main__":
    raise SystemExit(main())
