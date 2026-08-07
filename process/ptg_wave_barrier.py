# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fail-closed release gate used by an Indexed PTG wave worker."""

from __future__ import annotations

import inspect
import os
import re
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Mapping, Protocol, TypeVar

from api.ptg_wave_kubernetes import (
    PTGWaveContractError,
    PTG_WAVE_SLOT_COUNT,
    PTG_WAVE_WORKER_CLASS,
    queue_for_wave,
)


_DIGEST_RE = re.compile(r"^[0-9a-f]{64}$")
_IMAGE_RE = re.compile(r"^\S+@sha256:[0-9a-f]{64}$")
_RUNTIME_IMAGE_RE = re.compile(r"^sha256:[0-9a-f]{64}$")


class PTGWaveBarrier(Protocol):
    async def register_ready(self, identity: "PTGWaveWorkerIdentity") -> Mapping[str, Any]:
        """Persist this exact slot's readiness receipt before release."""

    async def wait_for_release(self, identity: "PTGWaveWorkerIdentity") -> Mapping[str, Any]:
        """Wait for and return the controller's matching release receipt."""


@dataclass(frozen=True)
class PTGWaveWorkerIdentity:
    wave_digest: str
    queue: str
    worker_class: str
    slot_index: int
    pod_uid: str
    manifest_digest: str
    jobs_digest: str
    job_count: int
    config_identity: str
    manifest_identity: str
    image_identity: str
    runtime_image_identity: str

    @classmethod
    def from_environment(cls, environ: Mapping[str, str] | None = None) -> "PTGWaveWorkerIdentity":
        """Read and validate only the closed Indexed-Job environment contract."""
        env = os.environ if environ is None else environ
        raw_slot_index = _required(env, "HLTHPRT_PTG_WAVE_SLOT_INDEX")
        try:
            slot_index = int(raw_slot_index)
        except ValueError as exc:
            raise PTGWaveContractError("wave slot index must be an integer") from exc
        if raw_slot_index != str(slot_index):
            raise PTGWaveContractError("wave slot index must be canonical decimal")
        raw_job_count = _required(env, "HLTHPRT_PTG_WAVE_JOB_COUNT")
        try:
            job_count = int(raw_job_count)
        except ValueError as exc:
            raise PTGWaveContractError("wave job count must be an integer") from exc
        if raw_job_count != str(job_count):
            raise PTGWaveContractError("wave job count must be canonical decimal")
        identity = cls(
            wave_digest=_required(env, "HLTHPRT_PTG_WAVE_DIGEST"),
            queue=_required(env, "HLTHPRT_ACTIVE_WORKER_QUEUE"),
            worker_class=_required(env, "HLTHPRT_ACTIVE_WORKER_CLASS"),
            slot_index=slot_index,
            pod_uid=_required(env, "HLTHPRT_PTG_WAVE_POD_UID"),
            manifest_digest=_required(
                env,
                "HLTHPRT_PTG_WAVE_REDIS_MANIFEST_DIGEST",
            ),
            jobs_digest=_required(env, "HLTHPRT_PTG_WAVE_JOBS_DIGEST"),
            job_count=job_count,
            config_identity=_required(env, "HLTHPRT_PTG_WAVE_CONFIG_IDENTITY"),
            manifest_identity=_required(env, "HLTHPRT_PTG_WAVE_MANIFEST_IDENTITY"),
            image_identity=_required(env, "HLTHPRT_PTG_WAVE_IMAGE_IDENTITY"),
            runtime_image_identity=_required(
                env,
                "HLTHPRT_PTG_WAVE_RUNTIME_IMAGE_IDENTITY",
            ),
        )
        identity.validate()
        return identity

    def validate(self) -> None:
        """Reject shared queues, non-PTG workers, or unpinned identities."""
        if self.slot_index not in range(PTG_WAVE_SLOT_COUNT):
            raise PTGWaveContractError("wave slot must be between zero and eleven")
        if self.queue != queue_for_wave(self.wave_digest):
            raise PTGWaveContractError("wave worker queue does not bind its wave digest")
        if self.worker_class != PTG_WAVE_WORKER_CLASS:
            raise PTGWaveContractError("wave worker class must be process.PTGSmall")
        for field_name, field_value in (
            ("manifest_digest", self.manifest_digest),
            ("jobs_digest", self.jobs_digest),
            ("config_identity", self.config_identity),
            ("manifest_identity", self.manifest_identity),
        ):
            if not _DIGEST_RE.fullmatch(field_value):
                raise PTGWaveContractError(
                    f"{field_name} must be a lowercase 64-hex digest"
                )
        if (
            isinstance(self.job_count, bool)
            or not isinstance(self.job_count, int)
            or not 1 <= self.job_count <= 4096
        ):
            raise PTGWaveContractError("job_count must be from 1 through 4096")
        if not _IMAGE_RE.fullmatch(self.image_identity):
            raise PTGWaveContractError("image_identity must be pinned by a sha256 digest")
        if not _RUNTIME_IMAGE_RE.fullmatch(self.runtime_image_identity):
            raise PTGWaveContractError(
                "runtime_image_identity must be a canonical sha256 digest"
            )
        for field_name, field_value in (
            ("pod_uid", self.pod_uid),
        ):
            if not field_value.strip() or field_value != field_value.strip():
                raise PTGWaveContractError(
                    f"{field_name} must be a non-empty trimmed string"
                )


T = TypeVar("T")


async def run_after_wave_release(
    identity: PTGWaveWorkerIdentity,
    barrier: PTGWaveBarrier,
    start_worker: Callable[[], T | Awaitable[T]],
) -> T:
    """Register this exact slot, await release, then and only then start ARQ."""

    identity.validate()
    await barrier.register_ready(identity)
    release = await barrier.wait_for_release(identity)
    _validate_release(identity, release)
    result = start_worker()
    if inspect.isawaitable(result):
        return await result
    return result


def _validate_release(identity: PTGWaveWorkerIdentity, release: Mapping[str, Any]) -> None:
    if release.get("released") is not True:
        raise PTGWaveContractError("wave release receipt is not released")
    expected_values_by_name = {
        "wave_digest": identity.wave_digest,
        "queue": identity.queue,
        "worker_class": identity.worker_class,
        "manifest_digest": identity.manifest_digest,
        "jobs_digest": identity.jobs_digest,
        "job_count": identity.job_count,
        "config_identity": identity.config_identity,
        "manifest_identity": identity.manifest_identity,
        "image_identity": identity.image_identity,
        "runtime_image_identity": identity.runtime_image_identity,
    }
    for name, value in expected_values_by_name.items():
        if release.get(name) != value:
            raise PTGWaveContractError(f"wave release receipt {name} does not match this slot")


def _required(environ: Mapping[str, str], name: str) -> str:
    value = environ.get(name)
    if not isinstance(value, str) or not value.strip() or value != value.strip():
        raise PTGWaveContractError(f"missing or invalid {name}")
    return value
