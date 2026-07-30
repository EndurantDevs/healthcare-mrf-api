# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import hashlib
from pathlib import Path
import struct
from types import SimpleNamespace

import pytest

from process.ptg_parts import ptg2_shared_snapshot_publish as publisher
from process.ptg_parts import ptg2_v4_graph_compiler as compiler
from tests.ptg2_v4_graph_compiler_test_support import (
    _write_tax_identity,
    compiler_fixture,
)
from tests.ptg2_v4_summary_fixture_support import empty_tax_identity_summary


POLICY_ID = "ptg-tin-hmac-sha256-v1:release-1"


def _valid_tax_entry(tmp_path: Path) -> dict[str, object]:
    return _write_tax_identity(
        tmp_path / "tax.sidecar",
        shard_id="source-a",
        tax_observations=[
            (bytes.fromhex("10" * 16), 1, bytes.fromhex("11" * 32)),
            (bytes.fromhex("20" * 16), 2, None),
        ],
    )


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("record_format", "wrong"),
        ("version", 2),
        ("record_bytes", 64),
        ("normalization_contract", "wrong"),
        ("hmac_contract", "wrong"),
        ("final", False),
        ("token_policy_id", None),
        ("token_policy_id", "ptg-tin-hmac-sha256-v1:rélease"),
        ("token_policy_id", "x" * 56),
        ("token_policy_id", "UPPER"),
    ),
)
def test_compiler_tax_artifact_metadata_rejects_each_contract_drift(
    tmp_path: Path,
    field: str,
    value: object,
) -> None:
    entry = _valid_tax_entry(tmp_path)
    entry[field] = value

    with pytest.raises(RuntimeError, match="metadata is invalid"):
        compiler._validated_tax_identity_artifact_metadata(entry)


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("row_count", True),
        ("provider_group_count", -1),
        ("matched_ein_count", "1"),
        ("missing_count", None),
        ("malformed_count", -1),
        ("unsupported_type_count", False),
    ),
)
def test_compiler_tax_artifact_metadata_rejects_noncanonical_counts(
    tmp_path: Path,
    field: str,
    value: object,
) -> None:
    entry = _valid_tax_entry(tmp_path)
    entry[field] = value

    with pytest.raises(RuntimeError, match=f"tax input {field}"):
        compiler._validated_tax_identity_artifact_metadata(entry)


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("provider_group_count", 1),
        ("missing_count", 0),
    ),
)
def test_compiler_tax_artifact_metadata_rejects_inconsistent_state_totals(
    tmp_path: Path,
    field: str,
    value: int,
) -> None:
    entry = _valid_tax_entry(tmp_path)
    entry[field] = value

    with pytest.raises(RuntimeError, match="counts are inconsistent"):
        compiler._validated_tax_identity_artifact_metadata(entry)


@pytest.mark.parametrize(
    "field",
    ("byte_count", "sha256"),
)
def test_compiler_tax_artifact_authentication_rejects_changed_bytes(
    tmp_path: Path,
    field: str,
) -> None:
    entry = _valid_tax_entry(tmp_path)
    if field == "byte_count":
        entry[field] = int(entry[field]) + 1
    else:
        entry[field] = "00" * 32
    metadata = compiler._validated_tax_identity_artifact_metadata(entry)

    with pytest.raises(RuntimeError, match="authentication failed"):
        compiler._validate_tax_identity_artifact_file(
            Path(str(entry["path"])),
            metadata=metadata,
        )


@pytest.mark.parametrize(
    ("offset", "replacement"),
    (
        (0, b"BADMAGIC"),
        (8, struct.pack("<H", 2)),
        (10, struct.pack("<H", 64)),
        (12, b"\x00"),
        (13, b"x"),
    ),
)
def test_compiler_tax_artifact_authentication_rejects_header_drift(
    tmp_path: Path,
    offset: int,
    replacement: bytes,
) -> None:
    entry = _valid_tax_entry(tmp_path)
    path = Path(str(entry["path"]))
    payload = bytearray(path.read_bytes())
    payload[offset : offset + len(replacement)] = replacement
    path.write_bytes(payload)
    entry["sha256"] = hashlib.sha256(payload).hexdigest()
    metadata = compiler._validated_tax_identity_artifact_metadata(entry)

    with pytest.raises(RuntimeError, match="header is invalid"):
        compiler._validate_tax_identity_artifact_file(path, metadata=metadata)


def test_compiler_tax_manifest_rejects_path_and_shard_metadata(
    tmp_path: Path,
) -> None:
    entry = _valid_tax_entry(tmp_path)
    for value, message in (
        ("", "lacks a path"),
        (str(tmp_path / "missing"), "is unavailable"),
    ):
        invalid_entry_by_field = {**entry, "path": value}
        with pytest.raises(RuntimeError, match=message):
            compiler._tax_identity_artifact_manifest(invalid_entry_by_field)

    with pytest.raises(RuntimeError, match="invalid shard_id"):
        compiler._tax_identity_artifact_manifest({**entry, "shard_id": 7})


@pytest.mark.parametrize(
    ("entries", "message"),
    (
        ([object()], "no factor artifacts"),
        ([{"name": "unknown"}], "no factor artifacts"),
        ([{"name": "provider_set_component"}], "lacks a shard ID"),
    ),
)
def test_compiler_manifest_rejects_unusable_factor_collections(
    tmp_path: Path,
    entries: list[object],
    message: str,
) -> None:
    provider_map = tmp_path / "map.tsv"
    provider_map.write_text("map")
    with pytest.raises(RuntimeError, match=message):
        compiler.build_v4_graph_compiler_manifest(
            graph_artifact_entries=entries,
            provider_set_key_map_path=provider_map,
            output_directory=tmp_path,
        )


def test_compiler_manifest_rejects_duplicates_and_provider_map_drift(
    tmp_path: Path,
) -> None:
    artifacts, provider_map = compiler_fixture(tmp_path)
    with pytest.raises(RuntimeError, match="repeats factor"):
        compiler.build_v4_graph_compiler_manifest(
            graph_artifact_entries=[*artifacts, artifacts[0]],
            provider_set_key_map_path=provider_map,
            output_directory=tmp_path,
        )

    provider_map.unlink()
    with pytest.raises(RuntimeError, match="authoritative provider-set map"):
        compiler.build_v4_graph_compiler_manifest(
            graph_artifact_entries=artifacts,
            provider_set_key_map_path=provider_map,
            output_directory=tmp_path,
        )


def _valid_tax_summary() -> dict[str, object]:
    return empty_tax_identity_summary()


@pytest.mark.parametrize(
    ("field", "value", "message"),
    (
        ("contract", "wrong", "contract changed"),
        ("token_policy_id", 1, "contract changed"),
        ("token_policy_id", "ptg-tin-hmac-sha256-v1:rélease", "contract changed"),
        ("token_policy_id", "x" * 56, "contract changed"),
        ("normalization_contract", "wrong", "contract changed"),
        ("hmac_contract", "wrong", "contract changed"),
        ("candidate_prefix_contract", "wrong", "contract changed"),
        ("authority_contract", "wrong", "contract changed"),
        ("source_ordinal_contract", "wrong", "contract changed"),
        ("token_policy_descriptor_sha256", "00" * 32, "contract changed"),
        ("source_ordinal_map", None, "source map is invalid"),
        ("source_ordinal_map", [], "source map is invalid"),
        (
            "source_ordinal_map",
            [{"shard_id": "source", "ordinal": 1}],
            "source map is invalid",
        ),
        (
            "source_ordinal_map",
            [{"shard_id": "", "ordinal": 0}],
            "source map is invalid",
        ),
        (
            "source_ordinal_map",
            [
                {"shard_id": "source-b", "ordinal": 0},
                {"shard_id": "source-a", "ordinal": 1},
            ],
            "source map is not canonical",
        ),
        ("source_shard_count", 2, "source binding changed"),
        ("source_bitmap_bytes", 2, "source binding changed"),
        ("source_ordinal_map_digest", "00" * 32, "source binding changed"),
        ("provider_group_count", 1, "counts are inconsistent"),
        ("tax_identity_count", 1, "counts are inconsistent"),
    ),
)
def test_compiler_tax_summary_rejects_independent_contract_drift(
    field: str,
    value: object,
    message: str,
) -> None:
    summary = _valid_tax_summary()
    summary[field] = value

    with pytest.raises(RuntimeError, match=message):
        compiler._validate_tax_identity_summary(summary, expected=None)


def test_compiler_tax_summary_rejects_shape_input_binding_and_non_object() -> None:
    summary = _valid_tax_summary()
    summary["unexpected"] = True
    with pytest.raises(RuntimeError, match="summary shape changed"):
        compiler._validate_tax_identity_summary(summary, expected=None)

    summary = _valid_tax_summary()
    with pytest.raises(RuntimeError, match="input binding changed"):
        compiler._validate_tax_identity_summary(
            summary,
            expected={
                "token_policy_id": POLICY_ID,
                "source_shard_ids": ("different",),
            },
        )

    with pytest.raises(RuntimeError, match="invalid tax identity summary"):
        compiler._validate_tax_identity_summary([], expected=None)


def _copy_payload(rows: list[tuple[bytes | None, ...]]) -> bytes:
    payload = bytearray(compiler._PG_COPY_HEADER)
    for row in rows:
        payload.extend(struct.pack(">h", len(row)))
        for field in row:
            if field is None:
                payload.extend(struct.pack(">i", -1))
            else:
                payload.extend(struct.pack(">i", len(field)))
                payload.extend(field)
    payload.extend(struct.pack(">h", -1))
    return bytes(payload)


@pytest.mark.parametrize(
    ("payload", "message"),
    (
        (b"bad", "invalid COPY header"),
        (compiler._PG_COPY_HEADER + b"\x00", "truncates COPY rows"),
        (
            compiler._PG_COPY_HEADER + struct.pack(">h", 2),
            "wrong COPY width",
        ),
        (
            compiler._PG_COPY_HEADER
            + struct.pack(">h", 1)
            + b"\x00",
            "truncates COPY field",
        ),
        (
            compiler._PG_COPY_HEADER
            + struct.pack(">h", 1)
            + struct.pack(">i", -2),
            "invalid NULL COPY field",
        ),
        (
            compiler._PG_COPY_HEADER
            + struct.pack(">h", 1)
            + struct.pack(">i", 2)
            + b"x",
            "truncates COPY field",
        ),
        (
            compiler._PG_COPY_HEADER + struct.pack(">h", -1) + b"x",
            "trailing COPY bytes",
        ),
    ),
)
def test_compiler_copy_reader_fails_closed_on_structural_corruption(
    tmp_path: Path,
    payload: bytes,
    message: str,
) -> None:
    path = tmp_path / "rows.copy"
    path.write_bytes(payload)

    with pytest.raises(RuntimeError, match=message):
        tuple(compiler._iter_pg_binary_rows(path, expected_field_count=1))


@pytest.mark.parametrize(
    ("row", "message"),
    (
        ((None, b"matched_ein", struct.pack(">i", 0), b"\x01"), "NULL fields"),
        (
            (bytes(16), b"\xff", struct.pack(">i", 0), b"\x01"),
            "state is invalid",
        ),
        (
            (bytes(15), b"matched_ein", struct.pack(">i", 0), b"\x01"),
            "not canonical",
        ),
        (
            (bytes(16), b"matched_ein", struct.pack(">i", 0), b"\x80"),
            "out-of-range bits",
        ),
        (
            (bytes(16), b"matched_ein", None, b"\x01"),
            "matched tax identity key is invalid",
        ),
        (
            (bytes(16), b"missing", struct.pack(">i", 0), b"\x01"),
            "unavailable tax identity has a key",
        ),
    ),
)
def test_compiler_group_tax_rows_reject_noncanonical_state_bindings(
    row: tuple[bytes | None, ...],
    message: str,
) -> None:
    with pytest.raises(RuntimeError, match=message):
        compiler._validated_tax_group_copy_fields(
            row,
            previous_group=None,
            summary={
                "source_shard_count": 1,
                "source_bitmap_bytes": 1,
                "tax_identity_count": 1,
            },
        )


def _publisher_compilation() -> SimpleNamespace:
    tax_summary = _valid_tax_summary()
    return SimpleNamespace(
        summary={"tax_identity": tax_summary},
        observe={"group_count": 0},
        output_artifacts=(
            SimpleNamespace(name="provider_tax_identities", byte_count=1),
            SimpleNamespace(name="provider_group_tax_identities", byte_count=2),
        ),
    )


@pytest.mark.parametrize(
    ("field", "value", "message"),
    (
        ("token_policy_id", "UPPER", "contract changed"),
        ("source_ordinal_map", [], "source map changed"),
        (
            "source_ordinal_map",
            [{"shard_id": "source", "ordinal": 1}],
            "source ordinal map changed",
        ),
        ("source_ordinal_map_digest", "00" * 32, "descriptor changed"),
        ("source_shard_count", 0, "source shape changed"),
        ("source_bitmap_bytes", 2, "source shape changed"),
        ("provider_group_count", 1, "counts changed"),
        ("tax_identity_count", 1, "counts changed"),
    ),
)
def test_publisher_tax_contract_rejects_manifest_drift(
    field: str,
    value: object,
    message: str,
) -> None:
    compilation = _publisher_compilation()
    compilation.summary["tax_identity"][field] = value

    with pytest.raises(RuntimeError, match=message):
        publisher._validated_v4_tax_identity_contract(compilation)


def test_publisher_tax_contract_requires_summary_and_both_artifacts() -> None:
    compilation = _publisher_compilation()
    compilation.summary["tax_identity"] = None
    with pytest.raises(RuntimeError, match="summary is missing"):
        publisher._validated_v4_tax_identity_contract(compilation)

    compilation = _publisher_compilation()
    compilation.output_artifacts = compilation.output_artifacts[:1]
    with pytest.raises(RuntimeError, match="artifacts are missing"):
        publisher._v4_tax_artifact_byte_count(compilation)


@pytest.mark.parametrize(
    ("row", "message"),
    (
        ((bytes(15), "matched_ein", 0, b"\x01", True), "tax identity changed"),
        ((bytes(16), "invalid", None, b"\x01", True), "tax identity changed"),
        ((bytes(16), "matched_ein", None, b"\x01", True), "tax identity changed"),
        ((bytes(16), "missing", 0, b"\x01", True), "tax identity changed"),
        ((bytes(16), "missing", None, b"\x01", False), "tax identity changed"),
        ((bytes(16), "missing", None, b"\x80", True), "source bitmap changed"),
    ),
)
def test_publisher_group_tax_rows_reject_invalid_identity_and_bitmap(
    row: tuple[object, ...],
    message: str,
) -> None:
    contract = publisher._validated_v4_tax_identity_contract(
        _publisher_compilation()
    )
    with pytest.raises(RuntimeError, match=message):
        publisher._validated_v4_tax_group_row(
            row,
            previous_group_id=b"",
            contract=contract,
        )


@pytest.mark.parametrize(
    "value",
    (True, -1, "1", None),
)
def test_publisher_tax_counters_are_strict_nonnegative_integers(
    value: object,
) -> None:
    with pytest.raises(RuntimeError, match="count changed"):
        publisher._v4_tax_summary_count({"count": value}, "count")


@pytest.mark.parametrize(
    "value",
    (None, "0" * 63, "A" * 64, "z" * 64),
)
def test_publisher_tax_digests_require_exact_lowercase_sha256(
    value: object,
) -> None:
    with pytest.raises(RuntimeError, match="digest changed"):
        publisher._v4_tax_summary_digest(value, "digest")
