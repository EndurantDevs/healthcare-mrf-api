# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Focused tests split from a shared contract fixture module."""

from __future__ import annotations

from tests.test_ptg2_source_witness_dictionary_bundle import (
    BytesIO,
    Path,
    WitnessPayloadLimitError,
    _record,
    _write_dictionary_bundle,
    hashlib,
    os,
    pytest,
    witness_materialize,
    witness_reader,
    witness_streaming_encode,
    zlib,
)


def test_streaming_encoder_reserves_stored_budget_before_stage_write(monkeypatch):
    raw_evidence = b'{"rate":1}'
    evidence_sha256 = hashlib.sha256(raw_evidence).hexdigest()
    stage_file = BytesIO()
    budget = witness_streaming_encode._StreamingBudget()
    monkeypatch.setattr(
        witness_streaming_encode,
        "PTG2_V3_SOURCE_WITNESS_MAX_PAYLOAD_BYTES",
        48,
    )

    with pytest.raises(WitnessPayloadLimitError, match="logical payload"):
        witness_streaming_encode._stage_evidence(
            stage_file,
            {},
            budget,
            evidence_sha256=evidence_sha256,
            raw_evidence=raw_evidence,
        )

    assert stage_file.getvalue() == b""


def test_streaming_encoder_rejects_per_entry_decoded_budget(monkeypatch):
    monkeypatch.setattr(
        witness_streaming_encode,
        "PTG2_V3_SOURCE_WITNESS_MAX_DECODED_RECORD_BYTES",
        3,
    )
    budget = witness_streaming_encode._StreamingBudget()

    with pytest.raises(WitnessPayloadLimitError, match="64 MiB per-entry"):
        budget.register_decoded_evidence("a" * 64, 4)


def test_locator_reader_rehashes_after_parse_even_when_stat_is_preserved(
    tmp_path,
    monkeypatch,
):
    bundle_entry = _write_dictionary_bundle(
        tmp_path,
        [
            _record(
                kind="rate_occurrence",
                priority=0,
                item_ordinal=0,
                raw_json=b'{"rate":1}',
                linked_provider_json=None,
            )
        ],
    )
    original_reader = witness_reader._read_current_dictionary_bundle

    def mutate_after_parse(bundle_file, bundle_identity, scanner_entry):
        result = original_reader(bundle_file, bundle_identity, scanner_entry)
        bundle_path = Path(bundle_identity.path)
        initial_stat = bundle_path.stat()
        with bundle_path.open("r+b") as mutable_bundle:
            mutable_bundle.seek(-1, os.SEEK_END)
            final_byte = mutable_bundle.read(1)
            mutable_bundle.seek(-1, os.SEEK_END)
            mutable_bundle.write(bytes([final_byte[0] ^ 1]))
            mutable_bundle.flush()
            os.fsync(mutable_bundle.fileno())
        os.utime(
            bundle_path,
            ns=(initial_stat.st_atime_ns, bundle_identity.mtime_ns),
        )
        return result

    monkeypatch.setattr(
        witness_reader,
        "_read_current_dictionary_bundle",
        mutate_after_parse,
    )

    with pytest.raises(RuntimeError, match="changed while reading"):
        witness_reader.read_scanner_bundle_locators(bundle_entry)


def test_materialization_rejects_replaced_authenticated_bundle(tmp_path):
    compressed_records = [
        _record(
            kind="rate_occurrence",
            priority=0,
            item_ordinal=0,
            raw_json=b'{"rate":1}',
            linked_provider_json=None,
        )
    ]
    bundle_entry = _write_dictionary_bundle(tmp_path, compressed_records)
    _header, locator_records = witness_reader.read_scanner_bundle_locators(
        bundle_entry
    )
    bundle_path = Path(str(bundle_entry["path"]))
    replacement_payload = bundle_path.read_bytes()
    replaced_path = tmp_path / "replaced.bin"
    bundle_path.replace(replaced_path)
    bundle_path.write_bytes(replacement_payload)

    try:
        witness_materialize.materialize_source_witness_locators(locator_records)
    except RuntimeError as exc:
        assert "changed before materialization" in str(exc)
    else:
        raise AssertionError("replaced authenticated bundle was accepted")


def test_materialization_rejects_in_place_record_rewrite_with_preserved_stat(
    tmp_path,
):
    compressed_records = [
        _record(
            kind="rate_occurrence",
            priority=0,
            item_ordinal=0,
            raw_json=b'{"rate":1}',
            linked_provider_json=None,
        )
    ]
    bundle_entry = _write_dictionary_bundle(tmp_path, compressed_records)
    _header, locator_records = witness_reader.read_scanner_bundle_locators(
        bundle_entry
    )
    locator = locator_records[0]
    bundle_path = Path(str(bundle_entry["path"]))
    authenticated_stat = bundle_path.stat()

    with bundle_path.open("r+b") as bundle_file:
        bundle_file.seek(locator.offset)
        compressed_record = bundle_file.read(locator.length)
        decoded_record = zlib.decompress(compressed_record)
        tampered_record = decoded_record.replace(
            b'"object_ordinal":1',
            b'"object_ordinal":2',
            1,
        )
        assert tampered_record != decoded_record
        tampered_compressed = zlib.compress(tampered_record, level=1)
        assert len(tampered_compressed) == locator.length
        bundle_file.seek(locator.offset)
        bundle_file.write(tampered_compressed)
        bundle_file.flush()
        os.fsync(bundle_file.fileno())
    os.utime(
        bundle_path,
        ns=(authenticated_stat.st_atime_ns, authenticated_stat.st_mtime_ns),
    )

    try:
        witness_materialize.materialize_source_witness_locators(locator_records)
    except RuntimeError as exc:
        assert "changed" in str(exc) or "digest" in str(exc)
    else:
        raise AssertionError("in-place rewritten authenticated bundle was accepted")
