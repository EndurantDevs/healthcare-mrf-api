# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Candidate release-gate proofs for protected multipart PTG imports."""

from __future__ import annotations

import copy
import hashlib

import pytest

from process.ptg_parts.frozen_rate_binding import (
    FROZEN_RATE_FILE_BINDING_OPTION,
    FrozenRateFileBindingMismatchError,
    frozen_rate_binding_from_params,
)
from process.ptg_parts.frozen_rate_candidate import (
    validate_frozen_candidate_evidence,
)
from process.ptg_parts.frozen_rate_files import (
    FROZEN_RATE_FILE_PROOF_CONTRACT,
    FROZEN_RATE_FILE_SET_CONTRACT,
    FrozenRateFileMismatchError,
    frozen_observed_logical_sha256,
    frozen_rate_file_proof_sha256,
    frozen_rate_file_set_sha256,
)


def _descriptor(ordinal: int) -> dict[str, object]:
    return {
        "source_type": "in_network",
        "canonical_url": (
            f"https://rates.example.test/2026-07/part-{ordinal}.json.gz"
        ),
        "content_length": 100 + ordinal,
        "etag": f'"part-{ordinal}"',
        "last_modified": None,
        "raw_sha256": hashlib.sha256(f"raw:{ordinal}".encode()).hexdigest(),
        "logical_sha256": hashlib.sha256(
            f"logical:{ordinal}".encode()
        ).hexdigest(),
        "logical_hash_deferred": False,
        "engine_source_identity_hash": f"{ordinal:016x}",
        "engine_source_file_version_id": f"{ordinal + 100:016x}",
        "ordinal": ordinal,
    }


def _frozen_binding(
    frozen_rate_files: list[dict[str, object]],
    frozen_set_digest: str,
) -> dict[str, object]:
    frozen_binding_by_name = frozen_rate_binding_from_params(
        {
            "source_file_import_id": "source-file-import-001",
            "import_id": "source-file-import-001",
            "source_key": "source-a",
            "import_month": "2026-07",
            "plan_ids": ["plan-a"],
            "plan_market_types": ["group"],
            "frozen_rate_file_set_contract": FROZEN_RATE_FILE_SET_CONTRACT,
            "frozen_rate_files": frozen_rate_files,
            "frozen_rate_file_set_sha256": frozen_set_digest,
            "frozen_rate_file_count": 2,
        }
    )
    assert frozen_binding_by_name is not None
    return frozen_binding_by_name


def _proof_rows(
    frozen_rate_files: list[dict[str, object]],
) -> list[dict[str, object]]:
    return [
        {
            "contract": FROZEN_RATE_FILE_PROOF_CONTRACT,
            **descriptor,
            "raw_byte_count": descriptor["content_length"],
            "verification_mode": "downloaded",
        }
        for descriptor in frozen_rate_files
    ]


def _source_version_rows(
    frozen_rate_files: list[dict[str, object]],
) -> list[dict[str, object]]:
    return [
        {
            **descriptor,
            "url": descriptor["canonical_url"],
            "logical_sha256": frozen_observed_logical_sha256(
                descriptor
            ),
            "raw_byte_count": descriptor["content_length"],
            "verification_mode": "downloaded",
        }
        for descriptor in frozen_rate_files
    ]


def _database_source_rows(
    frozen_rate_files: list[dict[str, object]],
) -> list[dict[str, object]]:
    return [
        {
            "source_key": zero_based_ordinal,
            "raw_container_sha256": descriptor["raw_sha256"],
            "source_file_version_count": 1,
            "source_file_version_id": descriptor[
                "engine_source_file_version_id"
            ],
            "version_source_identity_hash": descriptor[
                "engine_source_identity_hash"
            ],
            "version_source_type": descriptor["source_type"],
            "version_canonical_url": descriptor["canonical_url"],
            "version_raw_sha256": descriptor["raw_sha256"],
            "version_logical_sha256": (
                frozen_observed_logical_sha256(descriptor)
            ),
            "version_content_length": descriptor["content_length"],
            "version_etag": descriptor["etag"],
            "version_last_modified": descriptor["last_modified"],
            "version_verification_mode": "downloaded",
            "version_payload": {
                "raw_byte_count": descriptor["content_length"],
                "logical_hash_deferred": descriptor[
                    "logical_hash_deferred"
                ],
            },
        }
        for zero_based_ordinal, descriptor in enumerate(frozen_rate_files)
    ]


def _candidate_fixture(
    *,
    deferred_first: bool = False,
) -> tuple[
    dict[str, object],
    dict[str, object],
    list[dict[str, object]],
]:
    """Build one internally consistent manifest, binding, and DB source set."""

    frozen_rate_files = [_descriptor(1), _descriptor(2)]
    if deferred_first:
        frozen_rate_files[0]["logical_sha256"] = None
        frozen_rate_files[0]["logical_hash_deferred"] = True
    frozen_set_digest = frozen_rate_file_set_sha256(frozen_rate_files)
    frozen_binding_by_name = _frozen_binding(
        frozen_rate_files,
        frozen_set_digest,
    )
    proof_rows = _proof_rows(frozen_rate_files)
    manifest_by_name: dict[str, object] = {
        "source_file_import_id": "source-file-import-001",
        "frozen_rate_file_set_contract": FROZEN_RATE_FILE_SET_CONTRACT,
        "frozen_rate_files": frozen_rate_files,
        "frozen_rate_file_set_sha256": frozen_set_digest,
        "frozen_rate_file_count": 2,
        "frozen_rate_file_proof": proof_rows,
        "frozen_rate_file_proof_sha256": frozen_rate_file_proof_sha256(
            proof_rows
        ),
        "source_file_versions": _source_version_rows(frozen_rate_files),
        FROZEN_RATE_FILE_BINDING_OPTION: frozen_binding_by_name,
    }
    return (
        manifest_by_name,
        frozen_binding_by_name,
        _database_source_rows(frozen_rate_files),
    )


def _drift_proof_digest(manifest_by_name, _binding, _source_rows):
    manifest_by_name["frozen_rate_file_proof_sha256"] = "f" * 64


def _drift_proof_cardinality(manifest_by_name, _binding, _source_rows):
    manifest_by_name["frozen_rate_file_proof"] = manifest_by_name[
        "frozen_rate_file_proof"
    ][1:]


def _drift_version_cardinality(manifest_by_name, _binding, _source_rows):
    manifest_by_name["source_file_versions"] = manifest_by_name[
        "source_file_versions"
    ][1:]


def _drift_database_cardinality(
    _manifest_by_name,
    _binding,
    database_source_rows,
):
    database_source_rows.pop()


def _drift_database_version(
    _manifest_by_name,
    _binding,
    database_source_rows,
):
    database_source_rows[1]["source_file_version_id"] = "f" * 16


def _drift_database_hash(
    _manifest_by_name,
    _binding,
    database_source_rows,
):
    database_source_rows[1]["version_raw_sha256"] = "f" * 64


def _drift_database_raw_identity(
    _manifest_by_name,
    _binding,
    database_source_rows,
):
    database_source_rows[1]["raw_container_sha256"] = "f" * 64
    database_source_rows[1]["version_raw_sha256"] = "f" * 64


def _drift_database_source_identity(
    _manifest_by_name,
    _binding,
    database_source_rows,
):
    database_source_rows[1]["version_source_identity_hash"] = "f" * 16


def _drift_database_canonical_url(
    _manifest_by_name,
    _binding,
    database_source_rows,
):
    database_source_rows[1]["version_canonical_url"] = (
        "https://rates.example.test/changed.json.gz"
    )


def _drift_database_content_length(
    _manifest_by_name,
    _binding,
    database_source_rows,
):
    database_source_rows[1]["version_content_length"] += 1


def _drift_database_verification_mode(
    _manifest_by_name,
    _binding,
    database_source_rows,
):
    database_source_rows[1]["version_verification_mode"] = (
        "verified_local_sha256"
    )


def _drift_database_identity_pairs(
    _manifest_by_name,
    _binding,
    database_source_rows,
):
    first_version_id = database_source_rows[0]["source_file_version_id"]
    second_version_id = database_source_rows[1]["source_file_version_id"]
    database_source_rows[0]["source_file_version_id"] = second_version_id
    database_source_rows[1]["source_file_version_id"] = first_version_id


def _drift_database_source_density(
    _manifest_by_name,
    _binding,
    database_source_rows,
):
    database_source_rows[1]["source_key"] = 2


def _drift_database_source_key_type(
    _manifest_by_name,
    _binding,
    database_source_rows,
):
    database_source_rows[0]["source_key"] = False


def _drift_database_version_count_type(
    _manifest_by_name,
    _binding,
    database_source_rows,
):
    database_source_rows[0]["source_file_version_count"] = True


def _duplicate_database_version(
    _manifest_by_name,
    _binding,
    database_source_rows,
):
    database_source_rows[1]["source_file_version_id"] = (
        database_source_rows[0]["source_file_version_id"]
    )


def _duplicate_database_hash(
    _manifest_by_name,
    _binding,
    database_source_rows,
):
    database_source_rows[1]["raw_container_sha256"] = (
        database_source_rows[0]["raw_container_sha256"]
    )
    database_source_rows[1]["version_raw_sha256"] = (
        database_source_rows[0]["version_raw_sha256"]
    )


def _validate(
    manifest: dict[str, object],
    binding: dict[str, object] | None,
    sources: list[dict[str, object]] | None,
) -> str | None:
    return validate_frozen_candidate_evidence(
        manifest,
        candidate_run_id="ptg2:source-file-import-001",
        database_binding=binding,
        database_sources=sources,
    )


def test_candidate_recomputes_complete_frozen_proof_and_binding():
    manifest, binding, database_sources = _candidate_fixture()

    identity = _validate(manifest, binding, database_sources)

    assert identity is not None
    assert "ptg_frozen_candidate_identity_v1" in identity


def test_candidate_accepts_database_source_identity_order_independently():
    manifest, binding, _database_sources = _candidate_fixture()
    frozen_rate_files = manifest["frozen_rate_files"]
    database_sources = _database_source_rows(
        list(reversed(frozen_rate_files))
    )
    database_sources.reverse()

    identity = _validate(manifest, binding, database_sources)

    assert identity is not None
    assert "ptg_frozen_candidate_identity_v1" in identity


@pytest.mark.parametrize(
    "mutate_candidate",
    [
        _drift_proof_digest,
        _drift_proof_cardinality,
        _drift_version_cardinality,
        _drift_database_cardinality,
        _drift_database_version,
        _drift_database_hash,
        _drift_database_raw_identity,
        _drift_database_source_identity,
        _drift_database_canonical_url,
        _drift_database_content_length,
        _drift_database_verification_mode,
        _drift_database_identity_pairs,
        _drift_database_source_density,
        _drift_database_source_key_type,
        _drift_database_version_count_type,
        _duplicate_database_version,
        _duplicate_database_hash,
    ],
)
def test_candidate_rejects_proof_source_and_database_cardinality_drift(
    mutate_candidate,
):
    manifest, binding, database_sources = _candidate_fixture()
    mutate_candidate(manifest, binding, database_sources)

    with pytest.raises(FrozenRateFileMismatchError):
        _validate(manifest, binding, database_sources)


def test_candidate_rejects_binding_drift_and_legacy_replay():
    manifest, binding, database_sources = _candidate_fixture()
    drifted_binding_by_name = {**binding, "source_key": "source-b"}

    with pytest.raises(
        FrozenRateFileBindingMismatchError,
        match="binding changed",
    ):
        _validate(manifest, drifted_binding_by_name, database_sources)

    legacy_manifest_by_name = {
        key: value
        for key, value in copy.deepcopy(manifest).items()
        if not key.startswith("frozen_rate_")
        and key != "source_file_import_id"
    }
    with pytest.raises(
        FrozenRateFileBindingMismatchError,
        match="cannot be treated as legacy",
    ):
        _validate(legacy_manifest_by_name, binding, database_sources)


def test_candidate_legacy_is_accepted_only_when_tuple_and_binding_are_absent():
    assert (
        validate_frozen_candidate_evidence(
            {"source_file_versions": []},
            candidate_run_id="ptg2:legacy",
            database_binding=None,
            database_sources=None,
        )
        is None
    )


def test_candidate_frozen_identity_changes_when_complete_tuple_changes():
    manifest, binding, database_sources = _candidate_fixture()
    first_identity = _validate(manifest, binding, database_sources)
    changed_manifest, changed_binding, changed_sources = _candidate_fixture()
    changed_binding_by_name = {
        **changed_binding,
        "plan_ids": ["plan-b"],
    }
    changed_manifest[FROZEN_RATE_FILE_BINDING_OPTION] = (
        changed_binding_by_name
    )

    changed_identity = _validate(
        changed_manifest,
        changed_binding_by_name,
        changed_sources,
    )

    assert changed_identity != first_identity


@pytest.mark.parametrize(
    ("field_name", "changed_value"),
    [
        ("raw_byte_count", 1),
        ("verification_mode", "verified_local_sha256"),
        ("verification_mode", ""),
    ],
)
def test_candidate_requires_exact_source_version_byte_corroboration(
    field_name,
    changed_value,
):
    """Candidate proof and stored source-version byte evidence must agree."""

    manifest, binding, database_sources = _candidate_fixture()
    manifest["source_file_versions"][0][field_name] = changed_value

    with pytest.raises(
        FrozenRateFileMismatchError,
        match="byte proof|verification_mode",
    ):
        _validate(manifest, binding, database_sources)


@pytest.mark.parametrize("verification_mode", [None, "", "head_only"])
def test_candidate_rejects_unimplemented_proof_verification_modes(
    verification_mode,
):
    """A proof digest cannot legitimize an empty or unknown verifier name."""

    manifest, binding, database_sources = _candidate_fixture()
    manifest["frozen_rate_file_proof"][0][
        "verification_mode"
    ] = verification_mode
    manifest["frozen_rate_file_proof_sha256"] = (
        frozen_rate_file_proof_sha256(
            manifest["frozen_rate_file_proof"]
        )
    )

    with pytest.raises(
        FrozenRateFileMismatchError,
        match="verification_mode",
    ):
        _validate(manifest, binding, database_sources)
