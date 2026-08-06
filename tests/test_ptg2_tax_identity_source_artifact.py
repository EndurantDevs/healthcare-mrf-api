# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pure proofs for the source-local PTG tax-identity projector."""

from __future__ import annotations

import hashlib
import os
from pathlib import Path
from unittest.mock import patch

import pytest

from process.ptg_parts.ptg2_tax_identity_source_artifact import (
    prepare_tax_identity_source_projection,
)
from process.ptg_parts.ptg2_tax_identity_source_projection import (
    TaxIdentitySourceProjectionError,
)

_POLICY = "ptg-tin-hmac-sha256-v1:test"
_ERROR = "ptg2_tax_identity_source_projection_invalid"


def _ordinal_digest(shard_ids: tuple[str, ...]) -> bytes:
    digest = hashlib.sha256()
    digest.update(b"PTG2V4TAXORD\x01")
    digest.update(len(shard_ids).to_bytes(4, "big"))
    for ordinal, shard_id in enumerate(shard_ids):
        encoded = shard_id.encode("utf-8")
        digest.update(len(encoded).to_bytes(4, "big"))
        digest.update(encoded)
        digest.update(ordinal.to_bytes(4, "big"))
    return digest.digest()


def _record(group: int, state: int, token_byte: int = 0) -> bytes:
    group_id = group.to_bytes(16, "big")
    if state == 1:
        full_hmac = bytes((token_byte,)) * 32
        return group_id + bytes((state,)) + full_hmac[:16] + full_hmac
    return group_id + bytes((state,)) + bytes(48)


def _sidecar(
    tmp_path: Path,
    *,
    source_key: int,
    shard_id: str,
    identity_digit: str,
    sidecar_records: tuple[bytes, ...],
) -> dict[str, object]:
    path = tmp_path / f"source-{source_key}.ptg2tax"
    sidecar_bytes = (
        b"PTG2TAX1"
        + (1).to_bytes(2, "little")
        + (65).to_bytes(2, "little")
        + bytes((len(_POLICY),))
        + _POLICY.encode("ascii")
        + b"".join(sidecar_records)
    )
    path.write_bytes(sidecar_bytes)
    counts_by_state = {
        "matched_ein_count": sum(
            sidecar_record[16] == 1 for sidecar_record in sidecar_records
        ),
        "missing_count": sum(
            sidecar_record[16] == 2 for sidecar_record in sidecar_records
        ),
        "malformed_count": sum(
            sidecar_record[16] == 3 for sidecar_record in sidecar_records
        ),
        "unsupported_type_count": sum(
            sidecar_record[16] == 4 for sidecar_record in sidecar_records
        ),
    }
    return {
        "name": "provider_group_tax_identity",
        "path": str(path),
        "record_format": "ptg2_provider_group_tax_identity_v1",
        "sha256": hashlib.sha256(sidecar_bytes).hexdigest(),
        "byte_count": len(sidecar_bytes),
        "row_count": len(sidecar_records),
        "provider_group_count": len(sidecar_records),
        **counts_by_state,
        "version": 1,
        "record_bytes": 65,
        "token_policy_id": _POLICY,
        "normalization_contract": "ein_ascii_digits_or_2_7_hyphen_v1",
        "hmac_contract": "hmac_sha256_ptg_tin_v1",
        "final": True,
        "source_shard_id": shard_id,
        "physical_source_binding": {
            "contract": "ptg2_tax_identity_rate_source_binding_v1",
            "source_type": "in_network",
            "identity_kind": "logical_json_sha256_v1",
            "identity_sha256": identity_digit * 64,
            "source_key": source_key,
        },
    }


def _prepare(
    tmp_path: Path,
    sidecars: tuple[dict[str, object], ...],
    *,
    output_name: str = "projection.copy",
):
    shard_ids = tuple(sorted(str(sidecar["source_shard_id"]) for sidecar in sidecars))
    source_ordinal_rows = tuple(
        {"shard_id": shard_id, "ordinal": ordinal}
        for ordinal, shard_id in enumerate(shard_ids)
    )
    return prepare_tax_identity_source_projection(
        sidecars,
        output_path=tmp_path / output_name,
        token_policy_id=_POLICY,
        token_policy_descriptor_sha256=b"p" * 32,
        source_ordinal_map=source_ordinal_rows,
        source_ordinal_map_digest=_ordinal_digest(shard_ids),
        aggregate_tax_content_digest=b"a" * 32,
    )


def test_prepare_authenticates_records_and_is_deterministic(tmp_path):
    first = _sidecar(
        tmp_path,
        source_key=0,
        shard_id="file:a",
        identity_digit="1",
        sidecar_records=(_record(1, 1, 7), _record(2, 2)),
    )
    second = _sidecar(
        tmp_path,
        source_key=1,
        shard_id="file:b",
        identity_digit="2",
        sidecar_records=(_record(1, 4), _record(3, 3)),
    )

    prepared = _prepare(tmp_path, (second, first))
    repeated = _prepare(
        tmp_path,
        (first, second),
        output_name="projection-repeated.copy",
    )

    assert prepared.source_count == 2
    assert prepared.provider_group_occurrence_count == 4
    assert prepared.matched_ein_count == 1
    assert prepared.missing_count == 1
    assert prepared.malformed_count == 1
    assert prepared.unsupported_type_count == 1
    assert prepared.content_digest == repeated.content_digest
    assert prepared.copy_sha256 == repeated.copy_sha256
    assert prepared.copy_path.read_bytes().startswith(b"PGCOPY\n\xff\r\n\0")
    assert prepared.copy_path.read_bytes().endswith(b"\xff\xff")
    assert prepared.copy_path.stat().st_mode & 0o777 == 0o600
    copy_metadata = prepared.copy_path.stat()
    assert prepared.copy_device == copy_metadata.st_dev
    assert prepared.copy_inode == copy_metadata.st_ino
    assert prepared.copy_byte_count == copy_metadata.st_size
    assert prepared.copy_mtime_ns == copy_metadata.st_mtime_ns
    assert str(first["path"]) not in repr(prepared)
    assert "1" * 32 not in repr(prepared)

    prepared.cleanup()
    repeated.cleanup()
    assert not prepared.copy_path.exists()
    assert not repeated.copy_path.exists()


def test_prepared_cleanup_is_idempotent_and_swallows_unlink_errors(tmp_path):
    sidecar = _sidecar(
        tmp_path,
        source_key=0,
        shard_id="file:a",
        identity_digit="8",
        sidecar_records=(_record(1, 2),),
    )
    prepared = _prepare(tmp_path, (sidecar,))

    with patch.object(Path, "unlink", side_effect=PermissionError("denied")):
        prepared.cleanup()

    assert prepared.copy_path.exists()
    prepared.cleanup()
    prepared.cleanup()
    assert not prepared.copy_path.exists()


def test_prepare_and_cleanup_preserve_files_owned_by_other_attempts(tmp_path):
    sidecar = _sidecar(
        tmp_path,
        source_key=0,
        shard_id="file:a",
        identity_digit="8",
        sidecar_records=(_record(1, 2),),
    )
    output_path = tmp_path / "projection.copy"
    output_path.write_bytes(b"preexisting-owner")
    with pytest.raises(TaxIdentitySourceProjectionError, match=_ERROR):
        _prepare(tmp_path, (sidecar,))
    assert output_path.read_bytes() == b"preexisting-owner"

    output_path.unlink()
    prepared = _prepare(tmp_path, (sidecar,))
    replacement_path = tmp_path / "replacement.copy"
    replacement_path.write_bytes(b"replacement-owner")
    os.replace(replacement_path, prepared.copy_path)
    prepared.cleanup()
    assert output_path.read_bytes() == b"replacement-owner"


def test_prepare_hashes_copy_through_creation_descriptor(tmp_path):
    sidecar = _sidecar(
        tmp_path,
        source_key=0,
        shard_id="file:a",
        identity_digit="8",
        sidecar_records=(_record(1, 2),),
    )

    with patch.object(Path, "open", side_effect=AssertionError("path reopen")):
        prepared = _prepare(tmp_path, (sidecar,))

    assert len(prepared.copy_sha256) == 64
    prepared.cleanup()


@pytest.mark.parametrize(
    "mutator",
    [
        lambda sidecar: sidecar.__setitem__("version", True),
        lambda sidecar: sidecar.__setitem__("row_count", True),
        lambda sidecar: sidecar.__setitem__("provider_group_count", True),
        lambda sidecar: sidecar["physical_source_binding"].__setitem__(
            "source_key", False
        ),
    ],
)
def test_prepare_rejects_boolean_numeric_contract_fields(tmp_path, mutator):
    sidecar = _sidecar(
        tmp_path,
        source_key=0,
        shard_id="file:a",
        identity_digit="3",
        sidecar_records=(_record(1, 2),),
    )
    mutator(sidecar)

    with pytest.raises(TaxIdentitySourceProjectionError, match=_ERROR):
        _prepare(tmp_path, (sidecar,))


def test_prepare_preserves_generic_error_when_cleanup_fails(tmp_path):
    sidecar = _sidecar(
        tmp_path,
        source_key=0,
        shard_id="file:a",
        identity_digit="3",
        sidecar_records=(_record(1, 2),),
    )
    sidecar["version"] = True
    sensitive_path = str(tmp_path / "private-projection.copy")

    with patch.object(
        Path,
        "unlink",
        side_effect=PermissionError(sensitive_path),
    ):
        with pytest.raises(TaxIdentitySourceProjectionError) as raised:
            _prepare(tmp_path, (sidecar,))

    assert str(raised.value) == _ERROR
    assert sensitive_path not in str(raised.value)


@pytest.mark.parametrize(
    "digest_field",
    [
        "token_policy_descriptor_sha256",
        "source_ordinal_map_digest",
        "aggregate_tax_content_digest",
    ],
)
def test_prepare_rejects_integer_digest_arguments(tmp_path, digest_field):
    sidecar = _sidecar(
        tmp_path,
        source_key=0,
        shard_id="file:a",
        identity_digit="3",
        sidecar_records=(_record(1, 2),),
    )
    arguments_by_name = {
        "token_policy_descriptor_sha256": b"p" * 32,
        "source_ordinal_map_digest": _ordinal_digest(("file:a",)),
        "aggregate_tax_content_digest": b"a" * 32,
    }
    arguments_by_name[digest_field] = 32

    with pytest.raises(TaxIdentitySourceProjectionError, match=_ERROR):
        prepare_tax_identity_source_projection(
            (sidecar,),
            output_path=tmp_path / "projection.copy",
            token_policy_id=_POLICY,
            source_ordinal_map=({"shard_id": "file:a", "ordinal": 0},),
            **arguments_by_name,
        )


@pytest.mark.parametrize(
    "mutator",
    [
        lambda payload: payload.__setitem__(slice(-48, -32), bytes((9,)) * 16),
        lambda payload: payload.__setitem__(-1, 1),
        lambda payload: payload.__setitem__(slice(-65, -49), (1).to_bytes(16, "big")),
        lambda payload: payload.__setitem__(-49, 9),
    ],
)
def test_prepare_rejects_semantically_invalid_records_without_detail(
    tmp_path,
    mutator,
):
    sidecar = _sidecar(
        tmp_path,
        source_key=0,
        shard_id="file:a",
        identity_digit="7",
        sidecar_records=(_record(1, 1, 7), _record(2, 2)),
    )
    path = Path(str(sidecar["path"]))
    payload = bytearray(path.read_bytes())
    mutator(payload)
    path.write_bytes(payload)
    sidecar["sha256"] = hashlib.sha256(payload).hexdigest()

    with pytest.raises(TaxIdentitySourceProjectionError) as raised:
        _prepare(tmp_path, (sidecar,))

    assert str(raised.value) == _ERROR
    assert str(path) not in str(raised.value)
    assert "7" * 16 not in str(raised.value)
    assert not (tmp_path / "projection.copy").exists()


def test_prepare_rejects_artifact_replacement_and_incomplete_binding(tmp_path):
    sidecar = _sidecar(
        tmp_path,
        source_key=0,
        shard_id="file:a",
        identity_digit="4",
        sidecar_records=(_record(1, 2),),
    )
    Path(str(sidecar["path"])).write_bytes(b"replaced")

    with pytest.raises(TaxIdentitySourceProjectionError, match=_ERROR):
        _prepare(tmp_path, (sidecar,))

    valid = _sidecar(
        tmp_path,
        source_key=1,
        shard_id="file:b",
        identity_digit="5",
        sidecar_records=(_record(1, 2),),
    )
    with pytest.raises(TaxIdentitySourceProjectionError, match=_ERROR):
        _prepare(tmp_path, (valid,))


def test_prepare_rejects_symlink_without_reading_target(tmp_path):
    sidecar = _sidecar(
        tmp_path,
        source_key=0,
        shard_id="file:a",
        identity_digit="6",
        sidecar_records=(_record(1, 2),),
    )
    original = Path(str(sidecar["path"]))
    link = tmp_path / "linked.ptg2tax"
    os.symlink(original, link)
    sidecar["path"] = str(link)

    with pytest.raises(TaxIdentitySourceProjectionError, match=_ERROR):
        _prepare(tmp_path, (sidecar,))
