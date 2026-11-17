# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Turn an admitted ordinary import run into one retained network publication."""

import asyncio
import hashlib
import os
import re
from contextlib import AsyncExitStack
from uuid import UUID

from process.registry_membership_refresh_contract import (
    SERVICE_ACTOR,
    accepted_membership_source_sha256,
    frozen_membership_refresh_envelope,
)


def network_import_attempt_key(run_id):
    """A worker replay keeps its attempt; a new explicit run gets a new attempt."""
    if type(run_id) is not str or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:@-]{0,127}", run_id) is None:
        raise ValueError("registry_membership_refresh_worker_params_invalid")
    return "network-import:" + hashlib.sha256(run_id.encode()).hexdigest()


def latest_network_refresh_envelope(observation, run_id):
    """Freeze only server-observed data; run parameters supply no source or policy."""
    sources = observation["accepted_sources"]
    return frozen_membership_refresh_envelope(
        {
            "actor": dict(SERVICE_ACTOR),
            "command": {
                "operation": "refresh_memberships",
                "expected_approved_revision": observation["approved_revision"],
                "expected_serving_generation": observation["expected_serving_generation"],
                "expected_serving_manifest_sha256": observation["expected_serving_manifest_sha256"],
                "address_artifact_sha256": observation["address_artifact_sha256"],
                "accepted_source_vector_sha256": accepted_membership_source_sha256(sources),
                "accepted_sources": sources,
                "enrolled_scopes": [],
                "reason": "Rebuild networks from current retained imports.",
                "idempotency_key": network_import_attempt_key(run_id),
            },
        }
    )


def _network_request_replay(existing):
    """Reuse only the validated durable outcome of this same admitted run."""
    from process.registry_membership_refresh_queue import validated_membership_refresh_request_status

    validated_membership_refresh_request_status(existing)
    if existing["state"] == "completed":
        return None, {"request_id": str(existing["request_id"]), "state": "completed"}
    if existing["state"] == "rejected":
        raise ValueError("registry_imported_publication_rejected")
    return UUID(str(existing["request_id"])), None


async def _existing_network_import_request(worker_config, run_id, *, settings=None):
    """Read this exact run's protected queue result before any Source preparation."""
    from db.models import db
    from process.registry_membership_refresh_runtime import CONFIG_ENV, _actor_key, _settings
    from process.registry_source_observation_store import _namespace

    settings = _settings(os.environ.get(CONFIG_ENV)) if settings is None else settings
    if settings.app_origin is not None or settings.control_schema != worker_config.consumer.control_schema:
        raise ValueError("registry_native_network_import_required")
    namespace = _namespace(settings.control_schema)
    async with db.acquire_driver() as connection, connection.transaction(isolation="repeatable_read"):
        existing = await connection.fetchrow(
            f"SELECT * FROM {namespace}.registry_publication_request WHERE actor_key=$1 AND idempotency_key=$2",
            _actor_key(),
            network_import_attempt_key(run_id),
        )
        return None if existing is None else _network_request_replay(existing)


async def admit_latest_network_import(worker_config, cleanup, run_id):
    """Use the ordinary application queue writer after genuine Import Control admission."""
    from db.models import db
    from process.registry_imported_membership_refresh import RegistryImportedMembershipAdmission
    from process.registry_membership_refresh_queue import enqueue_registry_membership_refresh
    from process.registry_membership_refresh_runtime import (
        CONFIG_ENV,
        _build_native_bundle,
        _semantic_digest,
        _settings,
    )

    settings = _settings(os.environ.get(CONFIG_ENV))
    existing = await _existing_network_import_request(worker_config, run_id, settings=settings)
    if existing is not None:
        return existing
    bundle = await _build_native_bundle(settings, cleanup)
    if type(bundle.runtime.authorize) is not RegistryImportedMembershipAdmission:
        raise ValueError("registry_native_network_import_required")
    observed = await bundle.inputs_service()
    digest = _semantic_digest(
        observed["accepted_sources"],
        observed["approved_revision"],
        observed["address_generation"],
        observed["address_artifact_sha256"],
        [],
    )
    if observed["published_inputs_digest"] == digest:
        return None, {
            "state": "unchanged",
            "generation_id": observed["expected_serving_generation"],
            "manifest_sha256": observed["expected_serving_manifest_sha256"],
        }
    envelope = latest_network_refresh_envelope(observed, run_id)
    async with db.acquire_driver() as connection, connection.transaction(isolation="repeatable_read"):
        receipt = await enqueue_registry_membership_refresh(
            connection,
            envelope,
            authorize=bundle.runtime.authorize,
            address_profile=bundle.runtime.address_profile,
            control_schema=settings.control_schema,
        )
    return UUID(receipt["request_id"]), None


async def run_latest_network_import(config, run_id):
    """Own admission resources until the exact publication and its cleanup finish."""
    from process.registry_network_import_runtime import native_network_import_runtime, prepare_network_import_catalog
    from process.registry_publication_worker import run_registry_publication_worker

    async with asyncio.timeout(config.operation_seconds), AsyncExitStack() as cleanup:
        existing = await _existing_network_import_request(config, run_id)
        if existing is not None and existing[1] is not None:
            return existing[1]
        async with native_network_import_runtime(config, cleanup) as (catalog, runtime):
            await prepare_network_import_catalog(catalog, runtime, cleanup)
            request_id, unchanged = await admit_latest_network_import(config, cleanup, run_id)
            if unchanged is not None:
                return unchanged
            result = await run_registry_publication_worker(
                config, membership_refresh_request_id=request_id, freeze_preparation=runtime.freeze_preparation
            )
            if type(result) is not dict or result.get("state") != "completed":
                raise ValueError("registry_imported_publication_incomplete")
            return result
