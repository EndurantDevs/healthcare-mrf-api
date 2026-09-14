# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Focused checks for sealed custom-import source capture and decoding."""

from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
import gzip
from io import BytesIO
import json
from pathlib import Path
import subprocess
import sys
from typing import get_type_hints

import pytest

from process.custom_import import CaptureError, CaptureLimits, CustomImportDefinition
from process.custom_import import capture_stream, iter_records, load_json_definition, verify_capture


FIXTURES = Path(__file__).with_name("fixtures") / "custom_import"


def _stream(
    *,
    format_name: str = "csv",
    compression: str = "none",
    record_path: str | None = None,
):
    """Build one valid root stream using the existing synthetic definition fixture."""

    raw = load_json_definition((FIXTURES / "v1_valid.json").read_text())
    source = raw["streams"][0]
    source["format"] = format_name
    source["compression"] = compression
    if record_path is not None:
        source["record_path"] = record_path
    return CustomImportDefinition.from_mapping(raw).source_streams[0]


def _capture(payload: bytes, stream, *, limits: CaptureLimits | None = None):
    """Seal a small synthetic source payload with a stable external snapshot token."""

    return capture_stream(
        BytesIO(payload),
        stream,
        source_snapshot_token="snapshot-20260914",
        limits=limits or CaptureLimits(),
    )


def test_capture_seals_and_replays_delimited_source_records():
    """A CSV capture retains exact bytes, hashes, snapshot provenance, and labels."""

    stream = _stream()
    payload = b"Provider ID,Provider Name\n1234567893,Synthetic Provider\n"
    capture = _capture(payload, stream)

    assert capture.payload == payload
    assert capture.manifest.stream_id == "providers"
    assert capture.manifest.source_snapshot_token == "snapshot-20260914"
    assert capture.manifest.compressed_bytes == len(payload)
    assert capture.manifest.decoded_bytes == len(payload)
    records = list(iter_records(capture, stream))
    assert records[0].ordinal == 1
    assert records[0].values == {
        "Provider ID": "1234567893",
        "Provider Name": "Synthetic Provider",
    }
    with pytest.raises(TypeError):
        records[0].values["Provider ID"] = "different"


def test_capture_supports_gzip_replay_and_enforces_transport_limits():
    """Gzip is replayed from its sealed bytes and both byte limits fail closed."""

    stream = _stream(compression="gzip")
    payload = gzip.compress(b"Provider ID,Provider Name\n1234567893,Synthetic Provider\n")
    capture = _capture(payload, stream)
    assert list(iter_records(capture, stream))[0].values["Provider ID"] == "1234567893"

    with pytest.raises(CaptureError, match="compressed-byte"):
        _capture(payload, stream, limits=CaptureLimits(maximum_compressed_bytes=1))
    with pytest.raises(CaptureError, match="decoded-byte"):
        _capture(
            payload,
            stream,
            limits=CaptureLimits(maximum_decoded_bytes=32, maximum_record_bytes=16),
        )


def test_capture_detects_tampered_payload_or_manifest():
    """A persisted replay cannot substitute bytes or alter its source association."""

    stream = _stream()
    capture = _capture(b"Provider ID\n1234567893\n", stream)

    with pytest.raises(CaptureError, match="digest"):
        verify_capture(replace(capture, payload=b"Provider ID\n0000000000\n"), stream)
    with pytest.raises(CaptureError, match="does not belong"):
        verify_capture(capture, _stream(format_name="ndjson"))

    xml_stream = _stream(format_name="xml", record_path="provider")
    xml_capture = _capture(b"<providers><provider /></providers>", xml_stream)
    with pytest.raises(CaptureError, match="does not belong"):
        verify_capture(
            xml_capture,
            _stream(format_name="xml", record_path="different_provider"),
        )


@pytest.mark.parametrize(
    "payload, message",
    [
        (b"Provider ID,Provider ID\n1234567893,duplicate\n", "duplicate labels"),
        (b"Provider ID\n1234567893,unexpected\n", "field count"),
        (b"Provider ID,Provider Name\n1234567893\n", "field count"),
    ],
)
def test_delimited_decoder_rejects_ambiguous_source_columns(payload, message):
    """Delimited source shape cannot silently discard duplicated or extra values."""

    with pytest.raises(CaptureError, match=message):
        list(iter_records(_capture(payload, _stream()), _stream()))


def test_delimited_decoder_preserves_a_valid_overflow_named_source_column():
    """A source label that resembles an implementation sentinel remains ordinary data."""

    stream = _stream()
    payload = b"__overflow__,Provider ID\nretained,1234567893\n"
    records = list(iter_records(_capture(payload, stream), stream))
    assert records[0].values["__overflow__"] == "retained"


def test_delimited_decoder_rejects_nonprintable_unicode_source_labels():
    """Valid UTF-8 controls cannot enter later exact source-label matching."""

    stream = _stream()
    payload = b"Provider\xc2\x85ID,Provider Name\n1234567893,Synthetic Provider\n"

    with pytest.raises(CaptureError, match="printable text"):
        list(iter_records(_capture(payload, stream), stream))


def test_json_decoder_is_incremental_strict_and_preserves_decimal_values():
    """Top-level JSON arrays stream flat objects without losing decimal precision."""

    stream = _stream(format_name="json")
    payload = b'[{"npi":"1234567893","amount":12.50},{"npi":"1003000126","amount":0.01}]'

    records = list(iter_records(_capture(payload, stream), stream))
    assert [record.values["amount"] for record in records] == [Decimal("12.50"), Decimal("0.01")]
    with pytest.raises(CaptureError, match="top-level array"):
        list(iter_records(_capture(b'{"npi":"1234567893"}', stream), stream))
    with pytest.raises(CaptureError, match="duplicate JSON"):
        list(iter_records(_capture(b'[{"npi":"one","npi":"two"}]', stream), stream))
    with pytest.raises(CaptureError, match="valid UTF-8"):
        list(iter_records(_capture(b'[{"npi":"\\ud800"}]', stream), stream))


def test_json_decoder_errors_do_not_echo_duplicate_source_labels():
    """Malformed source labels remain out of capture errors and downstream logs."""

    stream = _stream(format_name="json")
    source_label = "Unreported Source Label"
    payload = (
        b'[{"'
        + source_label.encode("utf-8")
        + b'":"one","'
        + source_label.encode("utf-8")
        + b'":"two"}]'
    )

    with pytest.raises(CaptureError) as error:
        list(iter_records(_capture(payload, stream), stream))

    assert source_label not in str(error.value)


def test_json_oversized_integer_fails_closed_without_terminating_python():
    """JSON integer conversion must remain a normal capture failure on Python 3.14."""

    digit_limit = sys.get_int_max_str_digits()
    if digit_limit == 0 or digit_limit > 100_000:
        pytest.skip("the interpreter has no practical integer digit cap")
    subprocess_script = _oversized_json_subprocess_script(digit_limit)
    subprocess_result = subprocess.run(
        [sys.executable, "-c", subprocess_script],
        cwd=Path(__file__).resolve().parents[1],
        check=False,
        capture_output=True,
        text=True,
    )
    assert subprocess_result.returncode == 0, subprocess_result.stderr


def _oversized_json_subprocess_script(digit_limit: int) -> str:
    """Build an isolated parser invocation without embedding an oversized literal."""

    return "\n".join(
        (
            "from io import BytesIO",
            "from pathlib import Path",
            "from process.custom_import import CaptureError, CustomImportDefinition",
            "from process.custom_import import capture_stream, iter_records, load_json_definition",
            "raw = load_json_definition(Path('tests/fixtures/custom_import/v1_valid.json').read_text())",
            "raw['streams'][0]['format'] = 'json'",
            "stream = CustomImportDefinition.from_mapping(raw).source_streams[0]",
            f"payload = b'[{{\"number\":' + b'9' * {digit_limit + 1} + b'}}]'",
            "capture = capture_stream(BytesIO(payload), stream, source_snapshot_token='snapshot-1')",
            "try:",
            "    list(iter_records(capture, stream))",
            "except CaptureError:",
            "    pass",
            "else:",
            "    raise SystemExit('oversized integer was accepted')",
        )
    )


def test_ndjson_decoder_rejects_nested_values_and_record_overruns():
    """NDJSON admits only flat scalar objects within the declared per-record bound."""

    stream = _stream(format_name="ndjson")
    payload = b'\n{"npi":"1234567893","amount":12.50}\n'
    records = list(iter_records(_capture(payload, stream), stream))
    assert records[0].values["amount"] == Decimal("12.50")

    with pytest.raises(CaptureError, match="nested"):
        list(iter_records(_capture(b'{"npi":["1234567893"]}\n', stream), stream))
    with pytest.raises(CaptureError, match="valid UTF-8"):
        list(iter_records(_capture(b'{"npi":"\\ud800"}\n', stream), stream))
    tiny_limits = CaptureLimits(maximum_record_bytes=16, maximum_decoded_bytes=64)
    with pytest.raises(CaptureError, match="record exceeds"):
        list(iter_records(_capture(b'{"npi":"1234567893"}\n', stream, limits=tiny_limits), stream, limits=tiny_limits))


def test_xml_decoder_allows_flat_direct_child_records_and_clears_completed_roots():
    """XML records are streamed as direct root children without retaining prior children."""

    stream = _stream(format_name="xml", record_path="provider")
    xml_payload = b"<providers><provider><npi>1234567893</npi><name>Synthetic</name></provider></providers>"
    xml_records = list(iter_records(_capture(xml_payload, stream), stream))
    assert xml_records[0].values == {"npi": "1234567893", "name": "Synthetic"}
    repeated_xml_payload = (
        b"<providers><provider><npi>1234567893</npi></provider>"
        b"<provider><npi>1003000126</npi></provider></providers>"
    )
    assert [record.values["npi"] for record in iter_records(_capture(repeated_xml_payload, stream), stream)] == [
        "1234567893",
        "1003000126",
    ]


def test_xml_decoder_rejects_entities_and_non_utf8_encodings():
    """XML declaration preflight rejects entities across plain and gzip captures."""

    stream = _stream(format_name="xml", record_path="provider")
    entity_payload = b'<!DOCTYPE providers [<!ENTITY value "blocked">]><providers></providers>'
    with pytest.raises(CaptureError, match="entity declarations"):
        list(iter_records(_capture(entity_payload, stream), stream))
    split_limits = CaptureLimits(read_chunk_bytes=3)
    with pytest.raises(CaptureError, match="entity declarations"):
        list(
            iter_records(
                _capture(entity_payload, stream, limits=split_limits),
                stream,
                limits=split_limits,
            )
        )
    gzip_stream = _stream(format_name="xml", compression="gzip", record_path="provider")
    with pytest.raises(CaptureError, match="entity declarations"):
        list(iter_records(_capture(gzip.compress(entity_payload), gzip_stream), gzip_stream))
    encoded_entity_payload = entity_payload.decode().encode("utf-16")
    with pytest.raises(CaptureError, match="valid UTF-8"):
        list(iter_records(_capture(encoded_entity_payload, stream), stream))
    with pytest.raises(CaptureError, match="valid UTF-8"):
        list(
            iter_records(
                _capture(gzip.compress(encoded_entity_payload), gzip_stream),
                gzip_stream,
            )
        )


def test_xml_decoder_rejects_nested_or_unexpected_record_shapes():
    """Only flat records named by the direct-child selector can enter a stream."""

    stream = _stream(format_name="xml", record_path="provider")
    nested_payload = b"<providers><provider><npi><value>123</value></npi></provider></providers>"
    with pytest.raises(CaptureError, match="flat scalar"):
        list(iter_records(_capture(nested_payload, stream), stream))
    with pytest.raises(CaptureError, match="unexpected root child"):
        list(iter_records(_capture(b"<providers><metadata /></providers>", stream), stream))


def test_xml_record_annotation_can_be_resolved_under_python_314():
    """The XML helper must not use a runtime-invalid generic Element annotation."""

    from process.custom_import import capture

    assert get_type_hints(capture._xml_record_values)["element"].__name__ == "Element"


def test_decoder_limits_records_and_parquet_remains_explicitly_disabled():
    """Record-count limits and unavailable formats fail closed without implicit fallback."""

    json_stream = _stream(format_name="json")
    limits = CaptureLimits(maximum_records=1)
    payload = json.dumps([{"npi": "1234567893"}, {"npi": "1003000126"}]).encode()
    with pytest.raises(CaptureError, match="record limit"):
        list(iter_records(_capture(payload, json_stream, limits=limits), json_stream, limits=limits))

    parquet_stream = _stream(format_name="parquet")
    with pytest.raises(CaptureError, match="not enabled"):
        list(iter_records(_capture(b"not a parquet file", parquet_stream), parquet_stream))


def test_capture_rejects_unsafe_snapshot_tokens_and_nonbinary_sources():
    """Capture input never accepts control-bearing provenance or text-returning transports."""

    stream = _stream()
    with pytest.raises(CaptureError, match="control characters"):
        capture_stream(BytesIO(b"Provider ID\n1234567893\n"), stream, source_snapshot_token="bad\n")
    with pytest.raises(CaptureError, match="valid UTF-8"):
        capture_stream(BytesIO(b"Provider ID\n1234567893\n"), stream, source_snapshot_token="\ud800")

    class TextSource:
        """Minimal invalid source used to exercise binary transport enforcement."""

        def read(self, _size: int) -> str:
            """Return text instead of the binary protocol required by capture_stream."""

            return "text"

    with pytest.raises(CaptureError, match="must yield bytes"):
        capture_stream(TextSource(), stream, source_snapshot_token="snapshot-20260914")
