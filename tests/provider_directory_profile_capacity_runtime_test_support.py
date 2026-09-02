# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Shared deterministic capacity-runtime witness fixtures."""

from __future__ import annotations

import copy

from process.provider_directory_profile_capacity_runtime_witness import (
    CAPACITY_RUNTIME_CONTROL_PLANE_IMAGE_DIGEST_FIELD,
    CAPACITY_RUNTIME_CONTROL_PLANE_SOURCE_COMMIT_FIELD,
)


# This revision is signed into the neutral cross-repository execution golden.
# Any repository head change requires regenerating that fixture and its hashes.
PROFILE_RUNTIME_WITNESS_MIGRATION_REVISION = (
    "20260901103000_plan_pricing_em_distance"
)
_RUNTIME_WITNESS_BY_FIELD = {
    "healthcare_source_commit": "12" * 20,
    "healthcare_image_digest": "sha256:" + "13" * 32,
    CAPACITY_RUNTIME_CONTROL_PLANE_SOURCE_COMMIT_FIELD: "14" * 20,
    CAPACITY_RUNTIME_CONTROL_PLANE_IMAGE_DIGEST_FIELD: "sha256:" + "15" * 32,
    "profile_migration_revision": PROFILE_RUNTIME_WITNESS_MIGRATION_REVISION,
    "profile_schema_version": 1,
    "profile_strategy_version": (
        "source-fact-role32-org32-member32-dataset-graph8-auth-npi5m-v6"
    ),
    "postgres_server_version_num": 180002,
}
_DEPLOYMENT_WITNESS_BY_FIELD = {
    "flux_revision": "main@sha1:" + "16" * 20,
    "bootstrap_config_sha256": "17" * 32,
    "kubernetes_snapshot_sha256": "18" * 32,
    "preflight_pod_name": "healthcare-mrf-api-abc123",
    "preflight_pod_uid": "11111111-2222-3333-4444-555555555555",
    "preflight_transport": "kubectl_exec_loopback_8080",
}


def golden_runtime_witnesses() -> tuple[dict[str, object], dict[str, object]]:
    """Return isolated copies of the deterministic signed witness fixtures."""

    return (
        copy.deepcopy(_RUNTIME_WITNESS_BY_FIELD),
        copy.deepcopy(_DEPLOYMENT_WITNESS_BY_FIELD),
    )


def golden_capacity_storage() -> tuple[list[dict[str, object]], ...]:
    """Return deterministic tablespace and volume evidence rows."""

    return (
        [
            {
                "tablespace_name": "pg_default",
                "tablespace_oid": 1663,
                "usage": "data",
                "volume_digest": "33" * 32,
            },
            {
                "tablespace_name": "pg_default",
                "tablespace_oid": 1663,
                "usage": "temp",
                "volume_digest": "33" * 32,
            },
        ],
        [
            {
                "available_after_all_reservations_bytes": 700_000_000_000,
                "available_bytes": 1_000_000_000_000,
                "reserved_bytes": 180_000_000_000,
                "volume_class": "data",
                "volume_digest": "33" * 32,
            },
            {
                "available_after_all_reservations_bytes": 700_000_000_000,
                "available_bytes": 1_000_000_000_000,
                "reserved_bytes": 20_000_000_000,
                "volume_class": "temp",
                "volume_digest": "33" * 32,
            },
            {
                "available_after_all_reservations_bytes": 300_000_000_000,
                "available_bytes": 500_000_000_000,
                "reserved_bytes": 150_000_000_000,
                "volume_class": "wal",
                "volume_digest": "44" * 32,
            },
        ],
    )
