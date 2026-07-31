"""Artifact boundary coverage for sealed V4 taxonomy evidence."""

from __future__ import annotations

from copy import deepcopy
import os
from pathlib import Path

import pytest

from process.ptg_parts import ptg2_v4_taxonomy_candidates as taxonomy
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError
from tests.test_ptg2_v4_taxonomy_candidates import _projection_row, _rules


def test_taxonomy_scalar_and_catalog_evidence_guards(monkeypatch) -> None:
    """Reject malformed digests, names, bounds, and catalog evidence."""

    for malformed_digest in ("not-hex", "ab"):
        with pytest.raises(ValueError, match="invalid"):
            taxonomy._strict_sha256_hex(malformed_digest, label="digest")
    with pytest.raises(ValueError, match="stage name"):
        taxonomy._safe_stage_table_name("bad-name")
    monkeypatch.setattr(taxonomy.os, "O_NOFOLLOW", None)
    with pytest.raises(RuntimeError, match="no-follow"):
        taxonomy._nofollow_flag()
    for bounds in ((True, 0), (-1, 0), (0, -1)):
        with pytest.raises(ValueError, match="dictionary bounds"):
            taxonomy._validated_taxonomy_root_bounds(*bounds)
    cases = (
        (
            ({"npi_key": 0, "npi": 999, "matched_taxonomy_codes": ("A",)},),
            "NPI is invalid",
        ),
        (
            ({"npi_key": 0, "npi": 1_000_000_000, "matched_taxonomy_codes": ("B",)},),
            "evidence changed",
        ),
        (
            (
                {"npi_key": 0, "npi": 1_000_000_000, "matched_taxonomy_codes": ("A",)},
                {"npi_key": 0, "npi": 1_000_000_001, "matched_taxonomy_codes": ("A",)},
            ),
            "keys are not strict",
        ),
    )
    for catalog_rows, message in cases:
        with pytest.raises(RuntimeError, match=message):
            taxonomy._candidate_evidence_rows(
                catalog_rows,
                rule_codes=frozenset({"A"}),
            )


def test_taxonomy_private_artifact_guards_fail_closed(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """Reject missing, public, unsafe, and stalled artifacts."""

    with pytest.raises(RuntimeError, match="directory is unavailable"):
        with taxonomy._open_private_artifact_parent(tmp_path / "missing" / "a"):
            raise AssertionError("unreachable")
    public_parent = tmp_path / "public"
    public_parent.mkdir(mode=0o755)
    with pytest.raises(RuntimeError, match="not private"):
        with taxonomy._open_private_artifact_parent(public_parent / "a"):
            raise AssertionError("unreachable")
    private_parent = tmp_path / "private"
    private_parent.mkdir(mode=0o700)
    original_open = taxonomy.os.open

    def reject_directory(path, flags, *args, **kwargs):
        if Path(path) == private_parent:
            raise PermissionError
        return original_open(path, flags, *args, **kwargs)

    monkeypatch.setattr(taxonomy.os, "open", reject_directory)
    with pytest.raises(RuntimeError, match="directory is unsafe"):
        with taxonomy._open_private_artifact_parent(private_parent / "a"):
            raise AssertionError("unreachable")
    monkeypatch.setattr(taxonomy.os, "open", original_open)
    descriptor = os.open(private_parent / "stall", os.O_WRONLY | os.O_CREAT, 0o600)
    monkeypatch.setattr(taxonomy.os, "write", lambda *_args: 0)
    try:
        with pytest.raises(OSError, match="write stalled"):
            taxonomy._write_fsynced_bytes(descriptor, b"x")
    finally:
        os.close(descriptor)


def _projection_manifest() -> dict:
    rule = _rules()[0]
    return taxonomy.shape_v4_inferred_taxonomy_projection_manifest(
        (_projection_row(rule),),
        npi_count=3,
        pattern_count=0,
    )


def test_projection_manifest_rejects_numeric_digest_and_status_drift() -> None:
    """Reject nonnumeric counts, malformed digests, and duplicate rule status."""

    manifest = _projection_manifest()
    mutations = (
        ({**manifest, "member_count": "bad"}, "counts are invalid"),
        (
            {
                **manifest,
                "rules": [{**manifest["rules"][0], "member_count": "bad"}],
            },
            "projection rule is invalid",
        ),
        ({**manifest, "rule_set_digest": "not-hex"}, "rule-set digest is invalid"),
        ({**manifest, "projection_digest": "not-hex"}, "projection digest is invalid"),
    )
    for changed, message in mutations:
        with pytest.raises(PTG2ManifestArtifactError, match=message):
            taxonomy._validate_projection_manifest(changed)
    duplicated = deepcopy(manifest)
    base_rule = duplicated["rules"][0]
    observed_count = taxonomy.PTG2_V4_MAX_ONLINE_INFERRED_TAXONOMY_CANDIDATES + 1
    duplicated["observe_only_rules"] = [
        {
            "rule_digest": base_rule["rule_digest"],
            "catalog_digest": base_rule["catalog_digest"],
            "member_digest": base_rule["member_digest"],
            "member_count": observed_count,
            "observed_count_lower_bound": observed_count,
            "packed_byte_count": observed_count * 4,
            "status": taxonomy.PTG2_V4_INFERRED_TAXONOMY_OBSERVE_STATUS,
            "reason": taxonomy.PTG2_V4_INFERRED_TAXONOMY_CANDIDATE_CAP_REASON,
            "representation": taxonomy.PTG2_V4_INFERRED_TAXONOMY_OBSERVE_REPRESENTATION,
            "pattern_member_digest": taxonomy.inferred_taxonomy_pattern_member_digest(
                bytes.fromhex(base_rule["rule_digest"]),
                representation=taxonomy.PTG2_V4_INFERRED_TAXONOMY_OBSERVE_REPRESENTATION,
                pattern_count=0,
                pattern_member_count=0,
                packed_pattern_payload=b"",
            ).hex(),
        }
    ]
    duplicated["observe_only_rule_count"] = 1
    duplicated["member_count"] += observed_count
    duplicated["packed_byte_count"] += observed_count * 4
    with pytest.raises(PTG2ManifestArtifactError, match="status is duplicated"):
        taxonomy._validate_projection_manifest(duplicated)


def test_taxonomy_copy_identity_and_open_guards(tmp_path: Path, monkeypatch) -> None:
    """Reject invalid copy sizes, missing files, symlinks, and node changes."""

    copy_path = tmp_path / "taxonomy.copy"
    copy_path.write_bytes(b"copy")
    copy_path.chmod(0o600)
    with pytest.raises(ValueError, match="byte count"):
        taxonomy._validated_copy_identity(copy_path, True, "0" * 64)
    with pytest.raises(ValueError, match="byte count"):
        taxonomy._validated_copy_identity(copy_path, 0, "0" * 64)
    with pytest.raises(RuntimeError, match="unavailable"):
        taxonomy._validated_copy_identity(tmp_path / "missing", 4, "0" * 64)
    symlink = tmp_path / "copy-link"
    symlink.symlink_to(copy_path)
    with pytest.raises(RuntimeError, match="invalid"):
        with taxonomy._open_taxonomy_copy(symlink, expected_byte_count=4):
            raise AssertionError("unreachable")
    monkeypatch.setattr(taxonomy, "_is_same_node", lambda *_args: False)
    with pytest.raises(RuntimeError, match="changed"):
        with taxonomy._open_taxonomy_copy(copy_path, expected_byte_count=4):
            raise AssertionError("unreachable")
