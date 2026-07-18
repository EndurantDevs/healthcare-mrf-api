from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from process.ptg_parts.ptg2_shared_finalize import (
    attach_v3_dictionary_contract,
    attach_v3_source_run_contract,
    write_v3_finalizer_input_manifest,
)
from tests.ptg2_shared_finalize_test_support import (
    _canonical_sha256,
    _contracted_dictionary_entries,
    _contracted_entries,
    _entry,
    _identity,
    _one_source_contracted_inputs,
    _provider_metadata_entries,
)


def _assert_serving_run_manifest(manifest_payload):
    serving_entry = manifest_payload["serving_run_partition_files"][0]
    assert serving_entry["path"].endswith("run.ready")
    assert serving_entry["source_key"] == 0
    assert serving_entry["source_count"] == 1
    assert serving_entry["sha256"] == hashlib.sha256(b"x" * 52).hexdigest()
    assert manifest_payload["source_count"] == 1
    assert manifest_payload["expected_serving_run_files"] == 1
    assert manifest_payload["expected_serving_run_rows"] == 1
    assert manifest_payload["expected_serving_run_bytes"] == 52
    assert manifest_payload["source_run_contracts"][0]["partition_rows"] == [1]
    assert len(manifest_payload["source_run_contract_set_sha256"]) == 64


def _expected_dictionary_contract(manifest_payload, dictionary_entries, identity):
    return {
        "source_key": 0,
        "contract_sha256": dictionary_entries[0][
            "code_dictionary_contract_sha256"
        ],
        "version": 1,
        "source_identity": identity,
        "source_run_contract_sha256": manifest_payload["source_run_contracts"][0][
            "contract_sha256"
        ],
        "file_count": 1,
        "row_count": 1,
        "byte_count": 64,
        "files": [
            {
                "row_count": 1,
                "bytes": 64,
                "sha256": hashlib.sha256(b"x" * 64).hexdigest(),
            }
        ],
    }


def _assert_dictionary_manifest(manifest_payload, expected_dictionary_contract):
    dictionary_entry = manifest_payload["serving_run_code_dictionary_files"][0]
    assert dictionary_entry["path"].endswith("codes.ready")
    assert dictionary_entry["source_key"] == 0
    assert dictionary_entry["source_count"] == 1
    assert dictionary_entry["sha256"] == hashlib.sha256(b"x" * 64).hexdigest()
    assert manifest_payload["expected_code_dictionary_files"] == 1
    assert manifest_payload["expected_code_dictionary_rows"] == 1
    assert manifest_payload["expected_code_dictionary_bytes"] == 64
    assert (
        manifest_payload["code_dictionary_source_contracts"][0]
        == expected_dictionary_contract
    )
    assert dictionary_entry["code_dictionary_contract_sha256"] == (
        expected_dictionary_contract["contract_sha256"]
    )


def _assert_dictionary_contract_digests(
    manifest_payload,
    expected_dictionary_contract,
):
    dictionary_entry = manifest_payload["serving_run_code_dictionary_files"][0]
    assert manifest_payload["code_dictionary_contract_set_sha256"] == (
        _canonical_sha256(
            {
                "code_dictionary_contracts": [
                    {
                        key: dictionary_entry[key]
                        for key in (
                            "source_key",
                            "row_count",
                            "bytes",
                            "sha256",
                            "source_run_contract_sha256",
                            "code_dictionary_contract_sha256",
                        )
                    }
                ]
            }
        )
    )
    assert manifest_payload["code_dictionary_source_contract_set_sha256"] == (
        _canonical_sha256(
            {"code_dictionary_source_contracts": [expected_dictionary_contract]}
        )
    )


def test_finalizer_input_manifest_is_explicit_and_path_validated(tmp_path):
    """Authenticate every explicit finalizer path and source-bound contract."""

    identity = _identity("a")
    serving_entries = _contracted_entries(
        [
            _entry(
                tmp_path / "run.ready",
                row_count=1,
                bytes=52,
                partition=0,
                partition_count=1,
                format="ptg2_v3_serving_run",
                version=1,
            )
        ],
        identity,
        partition_count=1,
    )
    dictionary_entries = _contracted_dictionary_entries(
        [
            _entry(
                tmp_path / "codes.ready",
                row_count=1,
                bytes=64,
                format="ptg2_v3_serving_code_dictionary",
                version=4,
            )
        ],
        identity,
        serving_entries,
    )
    manifest = write_v3_finalizer_input_manifest(
        tmp_path / "input.json",
        serving_run_entries=serving_entries,
        code_dictionary_entries=dictionary_entries,
        provider_set_metadata_entries=_provider_metadata_entries(
            tmp_path, (identity, serving_entries[0])
        ),
        expected_source_identities=[identity],
    )

    manifest_payload = json.loads(manifest.read_text(encoding="ascii"))
    expected_dictionary_contract = _expected_dictionary_contract(
        manifest_payload, dictionary_entries, identity
    )
    _assert_serving_run_manifest(manifest_payload)
    _assert_dictionary_manifest(manifest_payload, expected_dictionary_contract)
    _assert_dictionary_contract_digests(
        manifest_payload, expected_dictionary_contract
    )


def test_finalizer_accepts_multiple_dictionary_shards_for_one_source(tmp_path):
    identity = _identity("a")
    serving_entries = _contracted_entries(
        [
            _entry(
                tmp_path / "run.ready",
                row_count=1,
                bytes=52,
                partition=0,
                partition_count=1,
                format="ptg2_v3_serving_run",
                version=1,
            )
        ],
        identity,
        partition_count=1,
    )
    dictionary_entries = _contracted_dictionary_entries(
        [
            _entry(
                tmp_path / f"codes-{shard}.ready",
                row_count=1,
                bytes=64,
                format="ptg2_v3_serving_code_dictionary",
                version=4,
            )
            for shard in range(2)
        ],
        identity,
        serving_entries,
    )
    manifest = write_v3_finalizer_input_manifest(
        tmp_path / "multi-dictionary.json",
        serving_run_entries=serving_entries,
        code_dictionary_entries=dictionary_entries,
        provider_set_metadata_entries=_provider_metadata_entries(
            tmp_path, (identity, serving_entries[0])
        ),
        expected_source_identities=[identity],
    )

    dictionary_entries = json.loads(manifest.read_text(encoding="ascii"))[
        "serving_run_code_dictionary_files"
    ]
    assert len(dictionary_entries) == 2
    assert {entry["source_key"] for entry in dictionary_entries} == {0}
    assert len({entry["sha256"] for entry in dictionary_entries}) == 1
    manifest_payload = json.loads(manifest.read_text(encoding="ascii"))
    assert manifest_payload["code_dictionary_source_contracts"][0]["file_count"] == 2
    assert len(manifest_payload["code_dictionary_source_contracts"][0]["files"]) == 2


def test_dictionary_contract_attachment_requires_scanner_aggregates(tmp_path):
    identity, serving_entries, _dictionary_entries = _one_source_contracted_inputs(
        tmp_path,
        dictionary_file_count=1,
    )
    raw_entry = _entry(
        tmp_path / "uncontracted.ready",
        row_count=1,
        bytes=64,
        format="ptg2_v3_serving_code_dictionary",
        version=4,
    )

    with pytest.raises(RuntimeError, match="serving_code_dictionary_files"):
        attach_v3_dictionary_contract(
            [raw_entry],
            source_identity=identity,
            source_run_contract_sha256=serving_entries[0][
                "source_run_contract_sha256"
            ],
            scanner_summary={
                "serving_code_dictionary_rows": 1,
                "serving_code_dictionary_bytes": 64,
            },
        )


def test_dictionary_contract_attachment_rejects_omitted_scanner_shard(tmp_path):
    identity = _identity("a")
    serving_entries = _contracted_entries(
        [
            _entry(
                tmp_path / "run.ready",
                row_count=1,
                bytes=52,
                partition=0,
                partition_count=1,
                format="ptg2_v3_serving_run",
                version=1,
            )
        ],
        identity,
        partition_count=1,
    )
    raw_entries = [
        _entry(
            tmp_path / f"codes-{index}.ready",
            row_count=1,
            bytes=64,
            format="ptg2_v3_serving_code_dictionary",
            version=4,
        )
        for index in range(2)
    ]

    with pytest.raises(RuntimeError, match="scanner aggregate summary"):
        _contracted_dictionary_entries(
            raw_entries[:1],
            identity,
            serving_entries,
            files=2,
            rows=2,
            bytes=128,
        )


def test_finalizer_rejects_dictionary_shard_omitted_after_attachment(tmp_path):
    identity, serving_entries, dictionary_entries = _one_source_contracted_inputs(
        tmp_path
    )

    with pytest.raises(RuntimeError, match="aggregates|file digests"):
        write_v3_finalizer_input_manifest(
            tmp_path / "omitted-dictionary.json",
            serving_run_entries=serving_entries,
            code_dictionary_entries=dictionary_entries[:1],
            provider_set_metadata_entries=_provider_metadata_entries(
                tmp_path, (identity, serving_entries[0])
            ),
            expected_source_identities=[identity],
        )


def test_manifest_uses_scanner_dictionary_digest_without_rereading(tmp_path):
    identity, serving_entries, dictionary_entries = _one_source_contracted_inputs(
        tmp_path,
        dictionary_file_count=1,
    )
    scanner_digest = dictionary_entries[0]["sha256"]
    Path(dictionary_entries[0]["path"]).write_bytes(b"m" * 64)

    manifest_path = write_v3_finalizer_input_manifest(
        tmp_path / "mutated-dictionary.json",
        serving_run_entries=serving_entries,
        code_dictionary_entries=dictionary_entries,
        provider_set_metadata_entries=_provider_metadata_entries(
            tmp_path, (identity, serving_entries[0])
        ),
        expected_source_identities=[identity],
    )
    payload = json.loads(manifest_path.read_text(encoding="ascii"))
    assert payload["serving_run_code_dictionary_files"][0]["sha256"] == (
        scanner_digest
    )
    assert scanner_digest != hashlib.sha256(b"m" * 64).hexdigest()


def test_finalizer_rejects_duplicate_dictionary_contract_identity(tmp_path):
    identity, serving_entries, dictionary_entries = _one_source_contracted_inputs(
        tmp_path
    )
    dictionary_entries[1]["code_dictionary_source_contract"] = dict(
        dictionary_entries[0]["code_dictionary_source_contract"]
    )

    with pytest.raises(RuntimeError, match="repeat physical identity"):
        write_v3_finalizer_input_manifest(
            tmp_path / "duplicate-dictionary-contract.json",
            serving_run_entries=serving_entries,
            code_dictionary_entries=dictionary_entries,
            provider_set_metadata_entries=_provider_metadata_entries(
                tmp_path, (identity, serving_entries[0])
            ),
            expected_source_identities=[identity],
        )


def test_finalizer_requires_one_dictionary_contract_per_dense_source(tmp_path):
    identity, serving_entries, dictionary_entries = _one_source_contracted_inputs(
        tmp_path,
        dictionary_file_count=1,
    )
    dictionary_entries[0].pop("code_dictionary_source_contract")

    with pytest.raises(RuntimeError, match="missing a complete code-dictionary"):
        write_v3_finalizer_input_manifest(
            tmp_path / "missing-dictionary-contract.json",
            serving_run_entries=serving_entries,
            code_dictionary_entries=dictionary_entries,
            provider_set_metadata_entries=_provider_metadata_entries(
                tmp_path, (identity, serving_entries[0])
            ),
            expected_source_identities=[identity],
        )


def test_finalizer_rejects_extra_dictionary_contract_fields(tmp_path):
    identity, serving_entries, dictionary_entries = _one_source_contracted_inputs(
        tmp_path,
        dictionary_file_count=1,
    )
    contract = dictionary_entries[0]["code_dictionary_source_contract"]
    contract["unexpected"] = True
    contract_sha256 = _canonical_sha256(contract)
    dictionary_entries[0]["code_dictionary_contract_sha256"] = contract_sha256

    with pytest.raises(RuntimeError, match="fields are incompatible"):
        write_v3_finalizer_input_manifest(
            tmp_path / "extra-dictionary-contract-field.json",
            serving_run_entries=serving_entries,
            code_dictionary_entries=dictionary_entries,
            provider_set_metadata_entries=_provider_metadata_entries(
                tmp_path, (identity, serving_entries[0])
            ),
            expected_source_identities=[identity],
        )


@pytest.mark.parametrize("invalid_version", ["1", 1.0, True, (1 << 64)])
def test_finalizer_rejects_non_integer_source_contract_version(
    tmp_path,
    invalid_version,
):
    identity, serving_entries, dictionary_entries = _one_source_contracted_inputs(
        tmp_path,
        dictionary_file_count=1,
    )
    contract = serving_entries[0]["source_run_contract"]
    contract["version"] = invalid_version
    serving_entries[0]["source_run_contract_sha256"] = _canonical_sha256(contract)

    with pytest.raises(RuntimeError, match="source-run contract version"):
        write_v3_finalizer_input_manifest(
            tmp_path / "invalid-source-contract-version.json",
            serving_run_entries=serving_entries,
            code_dictionary_entries=dictionary_entries,
            provider_set_metadata_entries=_provider_metadata_entries(
                tmp_path, (identity, serving_entries[0])
            ),
            expected_source_identities=[identity],
        )


def test_finalizer_rejects_extra_source_contract_identity_fields(tmp_path):
    identity, serving_entries, dictionary_entries = _one_source_contracted_inputs(
        tmp_path,
        dictionary_file_count=1,
    )
    contract = serving_entries[0]["source_run_contract"]
    contract["source_identity"]["unexpected"] = "accepted-by-old-python"
    serving_entries[0]["source_run_contract_sha256"] = _canonical_sha256(contract)

    with pytest.raises(RuntimeError, match="bound to another physical source"):
        write_v3_finalizer_input_manifest(
            tmp_path / "extra-source-identity-field.json",
            serving_run_entries=serving_entries,
            code_dictionary_entries=dictionary_entries,
            provider_set_metadata_entries=_provider_metadata_entries(
                tmp_path, (identity, serving_entries[0])
            ),
            expected_source_identities=[identity],
        )


def test_source_contract_attachment_rejects_string_file_counts(tmp_path):
    identity = _identity("a")
    entry = _entry(
        tmp_path / "string-count.ready",
        row_count="1",
        bytes=52,
        partition=0,
        partition_count=1,
        format="ptg2_v3_serving_run",
        version=1,
    )

    with pytest.raises(RuntimeError, match="serving-run row_count"):
        attach_v3_source_run_contract(
            [entry],
            source_identity=identity,
            scanner_summary={
                "serving_run_files": 1,
                "serving_run_rows": 1,
                "serving_run_bytes": 52,
            },
            scanner_config={"serving_run_partition_count": 1},
        )


def test_finalizer_rejects_unsorted_dictionary_contract_descriptors(tmp_path):
    identity, serving_entries, dictionary_entries = _one_source_contracted_inputs(
        tmp_path
    )
    second_path = Path(dictionary_entries[1]["path"])
    second_path.write_bytes(b"z" * 64)
    dictionary_entries[1]["sha256"] = hashlib.sha256(b"z" * 64).hexdigest()
    dictionary_entries = _contracted_dictionary_entries(
        [
            {
                key: descriptor_value
                for key, descriptor_value in entry.items()
                if key
                not in {
                    "source_type",
                    "identity_kind",
                    "identity_sha256",
                    "source_run_contract_sha256",
                    "code_dictionary_contract_sha256",
                    "code_dictionary_source_contract",
                }
            }
            for entry in dictionary_entries
        ],
        identity,
        serving_entries,
    )
    contract = dictionary_entries[0]["code_dictionary_source_contract"]
    contract["files"].reverse()
    contract_sha256 = _canonical_sha256(contract)
    for entry in dictionary_entries:
        entry["code_dictionary_contract_sha256"] = contract_sha256

    with pytest.raises(RuntimeError, match="file order is incompatible"):
        write_v3_finalizer_input_manifest(
            tmp_path / "unsorted-dictionary-contract.json",
            serving_run_entries=serving_entries,
            code_dictionary_entries=dictionary_entries,
            provider_set_metadata_entries=_provider_metadata_entries(
                tmp_path, (identity, serving_entries[0])
            ),
            expected_source_identities=[identity],
        )
