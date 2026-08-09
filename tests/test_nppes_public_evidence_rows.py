# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact Python-to-asyncpg row codec tests for NPPES evidence."""

from __future__ import annotations

from datetime import datetime

import pytest

from public_evidence.nppes_registry_replay_contract import (
    build_nppes_manifest_from_rows,
)
from public_evidence.nppes_registry_storage_contract import (
    NppesRegistryArchiveObservation,
    NppesRegistryMemberEncoder,
    build_nppes_registry_admission_row,
)
from process.nppes_public_evidence_rows import (
    ADMISSION_COLUMNS,
    COMMON_COLUMNS,
    MEMBER_COLUMNS,
    NPI_ENUMERATION_COLUMNS,
    SOURCE_IDENTITY_COLUMNS,
    SOURCE_LINK_COLUMNS,
    SOURCE_RECORD_COLUMNS,
    SOURCE_RELEASE_COLUMNS,
    NppesRegistryDatabaseRowEncoder,
    _timestamp,
    admission_values,
    source_identity_values,
    source_release_values,
)
from tests.public_evidence_nppes_registry_support import (
    HEADER,
    active_type_1_row,
    archive_identity,
)


def _fixture_rows():
    manifest = build_nppes_manifest_from_rows(
        archive_identity(), HEADER, (active_type_1_row(),)
    )
    archive_observation = NppesRegistryArchiveObservation(
        "c3" * 32,
        7,
        "d4" * 32,
    )
    encoder = NppesRegistryMemberEncoder(
        manifest,
        HEADER,
        archive_observation,
    )
    persistence_rows, member = encoder.encode(active_type_1_row())
    assert encoder.finish() == manifest
    admission = build_nppes_registry_admission_row(
        manifest,
        archive_observation,
    )
    return manifest, persistence_rows, member, admission


def _mapped(columns, values):
    assert len(columns) == len(values)
    return dict(zip(columns, values, strict=True))


def test_release_and_identity_codecs_cover_every_non_generated_column() -> None:
    manifest, _persistence, _member, _admission = _fixture_rows()
    identity = _mapped(
        SOURCE_IDENTITY_COLUMNS,
        source_identity_values(manifest.release),
    )
    release = _mapped(
        SOURCE_RELEASE_COLUMNS,
        source_release_values(manifest.release),
    )
    assert identity["identity_ref"] == manifest.release.artifact_identity.identity_ref
    assert identity["content_sha256"] == bytes.fromhex(
        manifest.release.artifact_identity.content_sha256
    )
    assert release["source_release_ref"] == manifest.release.source_release_ref
    assert release["semantic_limits"] == list(manifest.release.semantic_limits)
    assert type(release["contract_sha256"]) is bytes
    assert type(release["observed_start_at"]) is datetime
    assert release["source_binding_contract_id"] is None


def test_streaming_codec_emits_exact_source_member_and_projected_shapes() -> None:
    manifest, persistence, member, admission = _fixture_rows()
    encoder = NppesRegistryDatabaseRowEncoder(manifest.release, admission)
    source = _mapped(
        SOURCE_RECORD_COLUMNS,
        encoder.source_record(persistence.source_record),
    )
    member_values = _mapped(MEMBER_COLUMNS, encoder.member(member))
    common, link, typed = encoder.projected(persistence)
    common_values = _mapped(COMMON_COLUMNS, common)
    link_values = _mapped(SOURCE_LINK_COLUMNS, link)
    typed_values = _mapped(NPI_ENUMERATION_COLUMNS, typed)
    assert source["source_record_ref"] == member_values["source_record_ref"]
    assert source["nppes_admission_ref"] == admission.admission_ref
    assert member_values["admission_ref"] == admission.admission_ref
    assert type(source["record_hmac_sha256"]) is bytes
    assert member_values["source_row_ordinal"] == 1
    assert type(member_values["leaf_sha256"]) is bytes
    assert common_values["evidence_ref"] == member_values["evidence_ref"]
    assert common_values["evidence_ref"] == link_values["evidence_ref"]
    assert common_values["evidence_ref"] == typed_values["evidence_ref"]
    assert common_values["nppes_admission_ref"] == admission.admission_ref
    assert link_values["nppes_admission_ref"] == admission.admission_ref
    assert typed_values["nppes_admission_ref"] == admission.admission_ref
    assert type(common_values["observed_at"]) is datetime


def test_admission_codec_converts_every_digest_and_timestamp() -> None:
    _manifest, _persistence, _member, admission = _fixture_rows()
    values = _mapped(ADMISSION_COLUMNS, admission_values(admission))
    for field_name in (
        "contract_sha256",
        "artifact_sha256",
        "zip_member_census_sha256",
        "header_sha256",
        "manifest_sha256",
        "evidence_root_sha256",
        "rights_proof_sha256",
    ):
        assert type(values[field_name]) is bytes
        assert len(values[field_name]) == 32
    assert type(values["minimum_effective_start_at"]) is datetime
    assert type(values["snapshot_at"]) is datetime


def test_nullable_timestamp_and_admission_owner_mismatch_fail_closed() -> None:
    manifest, _persistence, _member, admission = _fixture_rows()
    assert _timestamp(None) is None
    mismatched_admission = admission._replace(source_release_ref="perel1_wrong")
    with pytest.raises(ValueError, match="NPPES admission owner mismatch"):
        NppesRegistryDatabaseRowEncoder(manifest.release, mismatched_admission)
