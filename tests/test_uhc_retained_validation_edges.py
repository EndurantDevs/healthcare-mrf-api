# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fail-closed edge coverage for retained UHC admission boundaries."""

from __future__ import annotations

import asyncio
from dataclasses import replace
import hashlib
import itertools
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

import process.uhc_retained_native as retained_native
import process.uhc_retained_native_process as native_process
import process.uhc_retained_range_manifest as range_manifest
import process.uhc_retained_registry_contract as registry_contract
import process.uhc_retained_registry_store as registry_store
import process.uhc_retained_source_registry as source_registry
import process.uhc_retained_types as retained_types
from process.uhc_retained_types import UHCRetainedAdmissionError
from tests.uhc_retained_registry_test_support import (
    records_payload,
    source_binding,
    write_retained_fixture,
)


def _verified_fixture(tmp_path: Path):
    fixture = write_retained_fixture(tmp_path, records_payload())
    raw_artifact, retained_ranges = range_manifest.load_verified_range_manifest(
        raw_path=fixture["raw_path"],
        manifest_path=fixture["manifest_path"],
        expected_artifact_sha256=fixture["artifact_sha256"],
        expected_artifact_bytes=fixture["artifact_byte_count"],
        expected_manifest_sha256=fixture["manifest_sha256"],
        expected_manifest_bytes=fixture["manifest_byte_count"],
        expected_range_count=fixture["range_count"],
        producer_build_id=fixture["producer_build_id"],
    )
    binding = source_binding(raw_artifact.sha256, raw_artifact.byte_count)
    return fixture, raw_artifact, retained_ranges, binding


@pytest.mark.parametrize(
    ("validator", "value"),
    (
        (lambda value: registry_contract.require_digest(value, "digest"), "BAD"),
        (registry_contract.safe_file_name, "../unsafe.json"),
        (registry_contract._canonical_catalog_url, " https://providermrf.uhc.com/x"),
        (registry_contract._canonical_catalog_url, "https://providermrf.uhc.com:bad/x"),
        (registry_contract._canonical_catalog_url, "http://providermrf.uhc.com/x"),
        (registry_contract._canonical_catalog_url, "https://PROVIDermrf.uhc.com/x"),
        (registry_contract._canonical_catalog_timestamp, ""),
        (registry_contract._canonical_catalog_timestamp, "not-a-date"),
        (registry_contract._canonical_catalog_timestamp, "2026-07-20T08:00:00"),
        (
            registry_contract._canonical_catalog_timestamp,
            "2026-07-20T10:00:00+02:00",
        ),
    ),
)
def test_catalog_identity_validators_reject_noncanonical_values(validator, value):
    with pytest.raises(UHCRetainedAdmissionError):
        validator(value)


def test_catalog_identity_rejects_boolean_file_size():
    with pytest.raises(UHCRetainedAdmissionError, match="byte count"):
        registry_contract.expected_catalog_file_hash_pair(
            family="ifp",
            collection_kind="provider_membership",
            file_name="JSON_Providers_AZDC.json",
            source_url=(
                "https://providermrf.uhc.com/api/stream/ui/ifp/providers/"
                "JSON_Providers_AZDC.json"
            ),
            catalog_modified_at="2026-07-20T08:00:00Z",
            size_bytes=True,
        )


def test_artifact_identity_and_file_verification_fail_closed(tmp_path, monkeypatch):
    digest = hashlib.sha256(b"safe").hexdigest()
    with pytest.raises(UHCRetainedAdmissionError, match="byte count"):
        retained_types._validate_artifact_identity(digest, True)
    with pytest.raises(UHCRetainedAdmissionError, match="contract version"):
        retained_types._validate_contract_version(0)
    with pytest.raises(UHCRetainedAdmissionError, match="unavailable"):
        retained_types._verify_artifact(tmp_path / "missing", digest, 4)

    artifact_path = tmp_path / "artifact.json"
    artifact_path.write_bytes(b"safe")
    artifact_path.chmod(0o666)
    with pytest.raises(UHCRetainedAdmissionError, match="permissions"):
        retained_types._verify_artifact(artifact_path, digest, 4)
    artifact_path.chmod(0o644)
    with pytest.raises(UHCRetainedAdmissionError, match="byte mismatch"):
        retained_types._verify_artifact(artifact_path, digest, 5)
    with pytest.raises(UHCRetainedAdmissionError, match="proof does not match"):
        retained_types._verify_artifact(artifact_path, "0" * 64, 4)

    real_fstat = retained_types.os.fstat
    observation_ordinals = itertools.count()

    def changed_fstat(descriptor):
        observation_ordinal = next(observation_ordinals)
        metadata = real_fstat(descriptor)
        if observation_ordinal == 0:
            return metadata
        return SimpleNamespace(
            st_dev=metadata.st_dev,
            st_ino=metadata.st_ino,
            st_size=metadata.st_size,
            st_mtime_ns=metadata.st_mtime_ns + 1,
            st_ctime_ns=metadata.st_ctime_ns,
            st_mode=metadata.st_mode,
            st_nlink=metadata.st_nlink,
        )

    monkeypatch.setattr(retained_types.os, "fstat", changed_fstat)
    with pytest.raises(UHCRetainedAdmissionError, match="changed while hashing"):
        retained_types._verify_artifact(artifact_path, digest, 4)


def test_native_file_and_input_identity_errors_are_wrapped(tmp_path):
    missing_path = tmp_path / "missing"
    with pytest.raises(UHCRetainedAdmissionError, match="unavailable"):
        retained_native._file_identity(missing_path)
    with pytest.raises(UHCRetainedAdmissionError, match="input is unavailable"):
        retained_native._validated_native_paths(missing_path, tmp_path)

    unsafe_path = tmp_path / "unsafe"
    unsafe_path.write_bytes(b"value")
    unsafe_path.chmod(0o666)
    with pytest.raises(UHCRetainedAdmissionError, match="permissions"):
        retained_native._file_identity(unsafe_path)


@pytest.mark.asyncio
async def test_native_spawn_and_pipe_cleanup_edges(monkeypatch):
    async def failed_spawn(*_arguments, **_options):
        raise OSError("injected spawn failure")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", failed_spawn)
    with pytest.raises(UHCRetainedAdmissionError, match="could not start"):
        await retained_native._run_native(("missing-scanner",))

    unavailable_pipes = SimpleNamespace(stdout=None, stderr=None)
    with pytest.raises(UHCRetainedAdmissionError, match="pipes"):
        await native_process.collect_process_output(
            unavailable_pipes,
            stdout_limit=1,
            stderr_limit=1,
        )

    async def failed_spawn_task():
        raise OSError("spawn task failed")

    spawn_task = asyncio.create_task(failed_spawn_task())
    await native_process.cleanup_native_process(None, spawn_task)


def test_manifest_descriptor_and_keyword_contract_edges(tmp_path, monkeypatch):
    empty_path = tmp_path / "empty"
    empty_path.write_bytes(b"")
    descriptor = os.open(empty_path, os.O_RDONLY)
    try:
        with pytest.raises(UHCRetainedAdmissionError, match="changed while reading"):
            range_manifest._read_descriptor_bytes(descriptor, 1)
    finally:
        os.close(descriptor)

    unsafe_path = tmp_path / "unsafe-manifest"
    unsafe_path.write_bytes(b"{}")
    unsafe_path.chmod(0o666)
    descriptor = os.open(unsafe_path, os.O_RDONLY)
    try:
        with pytest.raises(UHCRetainedAdmissionError, match="invalid"):
            range_manifest._read_manifest_descriptor(descriptor)
    finally:
        os.close(descriptor)

    stable_path = tmp_path / "stable-manifest"
    stable_path.write_bytes(b"{}")
    descriptor = os.open(stable_path, os.O_RDONLY)
    identity_ordinals = itertools.count(1)

    def changing_identity(_metadata):
        return (next(identity_ordinals),)

    monkeypatch.setattr(range_manifest, "_manifest_file_identity", changing_identity)
    try:
        with pytest.raises(UHCRetainedAdmissionError, match="changed while reading"):
            range_manifest._read_manifest_descriptor(descriptor)
    finally:
        os.close(descriptor)

    with pytest.raises(UHCRetainedAdmissionError, match="producer build id"):
        range_manifest._required_string({}, "producer_build_id")
    with pytest.raises(TypeError, match="unexpected"):
        range_manifest.load_verified_range_manifest(unexpected=True)
    with pytest.raises(TypeError, match="missing"):
        range_manifest.load_verified_range_manifest(raw_path=stable_path)


def test_source_and_store_proof_guards_reject_inconsistent_rows(tmp_path):
    _fixture, raw_artifact, retained_ranges, binding = _verified_fixture(tmp_path)
    with pytest.raises(UHCRetainedAdmissionError, match="not persisted"):
        registry_store._assert_row_matches(None, {}, label="raw artifact")
    for invalid_batch in (None, "{", "[]"):
        with pytest.raises(UHCRetainedAdmissionError, match="proof batch"):
            registry_store._decode_proof_rows(invalid_batch)
    with pytest.raises(registry_contract.UHCSourceBindingMismatch, match="range batch"):
        registry_store._verify_range_proof_rows({}, raw_artifact, [])

    binding_and_artifact_mutations = (
        (binding, replace(raw_artifact, sha256="0" * 64), "hashes differ"),
        (binding, replace(raw_artifact, byte_count=0), "proof is empty"),
        (replace(binding, family="other"), raw_artifact, "family"),
        (replace(binding, collection_kind="other"), raw_artifact, "collection kind"),
        (
            replace(binding, catalog_entry_sha256="0" * 64),
            raw_artifact,
            "catalog file identity",
        ),
    )
    for changed_binding, changed_artifact, message in binding_and_artifact_mutations:
        with pytest.raises(UHCRetainedAdmissionError, match=message):
            source_registry._validate_binding_identity(
                changed_binding,
                changed_artifact,
            )

    artifact_and_range_mutations = (
        (replace(raw_artifact, contract_version=1), retained_ranges, "contract"),
        (
            raw_artifact,
            (replace(retained_ranges[0], raw_byte_end=retained_ranges[0].raw_byte_start),)
            + retained_ranges[1:],
            "not contiguous",
        ),
        (replace(raw_artifact, record_count=raw_artifact.record_count + 1), retained_ranges, "cover every"),
        (
            replace(
                raw_artifact,
                canonical_byte_count=raw_artifact.canonical_byte_count + 1,
            ),
            retained_ranges,
            "canonical byte proof",
        ),
        (replace(raw_artifact, range_set_sha256="0" * 64), retained_ranges, "range-set"),
    )
    for changed_artifact, changed_ranges, message in artifact_and_range_mutations:
        with pytest.raises(UHCRetainedAdmissionError, match=message):
            source_registry._validate_range_proofs(changed_artifact, changed_ranges)


@pytest.mark.asyncio
async def test_store_rejects_an_empty_database_return(tmp_path):
    _fixture, raw_artifact, retained_ranges, binding = _verified_fixture(tmp_path)

    class EmptyConnection:
        async def fetchrow(self, *_arguments):
            return None

    with pytest.raises(UHCRetainedAdmissionError, match="not persisted"):
        await registry_store.persist_source_proofs(
            EmptyConnection(),
            binding=binding,
            raw_artifact=raw_artifact,
            ranges=retained_ranges,
        )
