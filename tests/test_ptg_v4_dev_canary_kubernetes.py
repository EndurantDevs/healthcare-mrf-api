# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from copy import deepcopy

import pytest

from scripts.ptg_v4_dev_canary_io import CanaryInfrastructureError
from scripts.ptg_v4_dev_canary_kubernetes import (
    FROZEN_V3_CANDIDATE_IMAGE,
    build_frozen_v3_runtime_evidence,
    require_frozen_v3_service_host,
    validate_frozen_v3_runtime_evidence,
)


def _deployment() -> dict[str, object]:
    return {
        "metadata": {
            "name": "healthcare-mrf-api-v3-candidate",
            "namespace": "healthporta-dev",
            "uid": "deployment-uid",
            "generation": 7,
        },
        "spec": {
            "replicas": 1,
            "template": {
                "spec": {
                    "containers": [
                        {
                            "name": "healthcare-mrf-api",
                            "image": FROZEN_V3_CANDIDATE_IMAGE,
                            "envFrom": [
                                {"configMapRef": {"name": "mrf-api-config"}},
                                {
                                    "configMapRef": {
                                        "name": "mrf-api-ptg-v3-candidate-config"
                                    }
                                },
                            ],
                        }
                    ]
                }
            },
        },
        "status": {
            "observedGeneration": 7,
            "replicas": 1,
            "readyReplicas": 1,
            "updatedReplicas": 1,
            "availableReplicas": 1,
        },
    }


def _pod_list() -> dict[str, object]:
    return {
        "items": [
            {
                "metadata": {
                    "name": "v3-candidate-pod",
                    "namespace": "healthporta-dev",
                    "uid": "pod-uid",
                },
                "status": {
                    "phase": "Running",
                    "conditions": [{"type": "Ready", "status": "True"}],
                    "containerStatuses": [
                        {
                            "name": "healthcare-mrf-api",
                            "ready": True,
                            "image": FROZEN_V3_CANDIDATE_IMAGE,
                            "imageID": (
                                "docker-pullable://ghcr.io/endurantdevs/"
                                f"healthcare-mrf-api-dev@sha256:{'a' * 64}"
                            ),
                        }
                    ],
                },
            }
        ]
    }


@pytest.mark.parametrize(
    "image_identity",
    (
        (
            "docker-pullable://ghcr.io/endurantdevs/"
            f"healthcare-mrf-api-dev@sha256:{'a' * 64}"
        ),
        f"sha256:{'a' * 64}",
    ),
)
def test_frozen_candidate_attestation_binds_service_tag_and_digest(
    image_identity: str,
) -> None:
    pod_list = _pod_list()
    pod_list["items"][0]["status"]["containerStatuses"][0][
        "imageID"
    ] = image_identity
    evidence = build_frozen_v3_runtime_evidence(
        _deployment(),
        pod_list,
    )

    assert validate_frozen_v3_runtime_evidence(evidence) == []
    assert evidence["image"] == FROZEN_V3_CANDIDATE_IMAGE
    assert evidence["image_digest"] == "a" * 64
    assert require_frozen_v3_service_host(
        "healthcare-mrf-api-v3-candidate.healthporta-dev.svc"
    )


@pytest.mark.parametrize("mutation", ("replicas", "image", "pod_digest"))
def test_frozen_candidate_attestation_rejects_mutable_or_wrong_runtime(
    mutation: str,
) -> None:
    deployment = deepcopy(_deployment())
    pod_list = deepcopy(_pod_list())
    if mutation == "replicas":
        deployment["spec"]["replicas"] = 2
    elif mutation == "image":
        deployment["spec"]["template"]["spec"]["containers"][0][
            "image"
        ] = "ghcr.io/example/current:v4"
    else:
        pod_list["items"][0]["status"]["containerStatuses"][0][
            "imageID"
        ] = "docker-pullable://ghcr.io/example/current:latest"

    with pytest.raises(CanaryInfrastructureError):
        build_frozen_v3_runtime_evidence(deployment, pod_list)


def test_reference_url_cannot_point_to_current_or_v4_service() -> None:
    for hostname in (
        "healthcare-mrf-api",
        "healthcare-mrf-api-v4-candidate",
        "127.0.0.1",
    ):
        with pytest.raises(CanaryInfrastructureError):
            require_frozen_v3_service_host(hostname)
