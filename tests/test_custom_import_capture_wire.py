# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Portable framing and declared-content identity, without acquisition or storage."""

import gzip
import hashlib
import json
from dataclasses import asdict, replace
from io import BytesIO
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from process.custom_import import capture
from process.custom_import import capture_wire as wire
from process.custom_import.capture import capture_stream, verify_capture
from process.custom_import.capture_limits import CaptureLimits
from process.custom_import.definition import MAX_CHILD_COLLECTIONS, CustomImportDefinition
from tests.test_custom_import_definition import _raw_definition


def _canonical(document):
    return json.dumps(document, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode("ascii")


async def _chunks(*parts):
    for part in parts:
        yield part


@pytest.fixture
def bundle():
    definition = CustomImportDefinition.from_mapping(_raw_definition())
    payloads = (b"id,name\n1,Synthetic\n", gzip.compress(b'{"id":1}\n', mtime=0))
    captures = tuple(
        capture_stream(BytesIO(payload), stream, source_snapshot_token="snapshot-\u00e9")
        for stream, payload in zip(definition.source_streams, payloads)
    )
    return definition, captures


def _header(captures):
    return {"captures": [asdict(item.manifest) for item in captures]}


def _frame(header, payload=b""):
    raw = header if type(header) is bytes else _canonical(header)
    return b"CICAP1\0\0" + len(raw).to_bytes(4, "big") + raw + payload


def _objects(captures):
    return tuple(
        SimpleNamespace(
            stream_id=item.manifest.stream_id,
            expected_bytes=len(item.payload),
            expected_sha256=hashlib.sha256(item.payload).hexdigest(),
        )
        for item in captures
    )


@pytest.mark.parametrize("chunk_size", (1, 2, 7, 8, 11, 12, 13, 64, 65536))
async def test_wire_round_trip_across_framing_and_object_boundaries(bundle, chunk_size):
    _, captures = bundle
    limits = replace(CaptureLimits(), read_chunk_bytes=chunk_size)
    pieces = [piece async for piece in wire.iter_capture_body(tuple(reversed(captures)), limits=limits)]
    assert all(type(piece) is bytes and 0 < len(piece) <= chunk_size for piece in pieces)
    body = b"".join(pieces)
    assert body == _frame(_header(tuple(reversed(captures))), b"".join(item.payload for item in reversed(captures)))
    assert b"\\u00e9" in body and b"\xc3\xa9" not in body
    assert await wire.read_capture_body(_chunks(*pieces), limits=limits) == tuple(reversed(captures))
    assert await wire.read_capture_body(_chunks(body), limits=limits) == tuple(reversed(captures))


async def test_encoder_freezes_submission_order_before_first_yield(bundle):
    _, captures = bundle
    submitted_captures = list(captures)
    body = wire.iter_capture_body(submitted_captures, limits=CaptureLimits())
    first = await anext(body)
    submitted_captures.reverse()
    pieces = [first, *[piece async for piece in body]]
    assert await wire.read_capture_body(_chunks(*pieces), limits=CaptureLimits()) == captures


async def test_empty_original_is_distinct_from_an_empty_bundle(bundle):
    definition, _ = bundle
    empty = capture_stream(BytesIO(b""), definition.source_streams[0], source_snapshot_token="snapshot")
    pieces = [piece async for piece in wire.iter_capture_body((empty,), limits=CaptureLimits())]
    assert await wire.read_capture_body(_chunks(*pieces), limits=CaptureLimits()) == (empty,)
    with pytest.raises(wire.CaptureWireError):
        await anext(wire.iter_capture_body((), limits=CaptureLimits()))
    with pytest.raises(wire.CaptureWireError):
        await wire.read_capture_body(_chunks(_frame({"captures": []})), limits=CaptureLimits())


@pytest.mark.parametrize("count", (MAX_CHILD_COLLECTIONS + 1, MAX_CHILD_COLLECTIONS + 2))
async def test_wire_stream_count_is_bounded(bundle, count):
    _, captures = bundle
    expanded_captures = tuple(
        replace(captures[0], manifest=replace(captures[0].manifest, stream_id=f"stream_{index}"))
        for index in range(count)
    )
    if count > MAX_CHILD_COLLECTIONS + 1:
        with pytest.raises(wire.CaptureWireError):
            await anext(wire.iter_capture_body(expanded_captures, limits=CaptureLimits()))
        with pytest.raises(wire.CaptureWireError):
            await wire.read_capture_body(_chunks(_frame(_header(expanded_captures))), limits=CaptureLimits())
    else:
        pieces = [piece async for piece in wire.iter_capture_body(expanded_captures, limits=CaptureLimits())]
        assert await wire.read_capture_body(_chunks(*pieces), limits=CaptureLimits()) == expanded_captures


@pytest.mark.parametrize("size", (0, wire.HEADER_MAX + 1, 2**32 - 1))
async def test_header_length_refuses_before_reading_header(size):
    async def source():
        yield b"CICAP1\0\0" + size.to_bytes(4, "big")
        pytest.fail("invalid length pulled a header")

    with pytest.raises(wire.CaptureWireError):
        await wire.read_capture_body(source(), limits=CaptureLimits())


@pytest.mark.parametrize("failure", ("magic", "prefix", "length", "header", "payload", "trailing", "late-trailing"))
async def test_truncated_or_trailing_frame_is_refused(bundle, failure):
    _, captures = bundle
    frame = _frame(_header(captures), b"".join(item.payload for item in captures))
    cuts_by_kind = {
        "prefix": 7,
        "length": 11,
        "header": 12 + len(_canonical(_header(captures))) - 1,
        "payload": len(frame) - 1,
    }
    if failure == "magic":
        parts = (b"X" + frame[1:],)
    elif failure in cuts_by_kind:
        parts = (frame[: cuts_by_kind[failure]],)
    else:
        parts = (frame + b"X",) if failure == "trailing" else (frame, b"X")
    with pytest.raises(wire.CaptureWireError):
        await wire.read_capture_body(_chunks(*parts), limits=CaptureLimits())


@pytest.mark.parametrize("failure", ("whitespace", "duplicate", "unknown", "missing", "utf8", "float", "shape"))
async def test_header_requires_closed_canonical_ascii_json(bundle, failure):
    _, captures = bundle
    document = _header(captures)
    raw = _canonical(document)
    raw_by_kind = {
        "whitespace": raw + b" ",
        "duplicate": b'{"captures":[],"captures":' + raw[len(b'{"captures":') :],
        "unknown": _canonical({**document, "extra": None}),
        "missing": b"{}",
        "utf8": json.dumps(document, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8"),
        "shape": b'{"captures":{}}',
    }
    document["captures"][0]["compressed_bytes"] = 1.0
    raw_by_kind["float"] = _canonical(document)
    with pytest.raises(wire.CaptureWireError):
        await wire.read_capture_body(_chunks(_frame(raw_by_kind[failure])), limits=CaptureLimits())


@pytest.mark.parametrize(
    "field,value",
    (
        ("stream_id", "UPPER"),
        ("stream_id", "a" * 64),
        ("stream_id", None),
        ("format", "other"),
        ("format", []),
        ("compression", "GZIP"),
        ("source_snapshot_token", None),
        ("source_snapshot_token", ""),
        ("source_snapshot_token", "a" * 1025),
        ("source_snapshot_token", "line\n"),
        ("source_snapshot_token", "\ud800"),
        ("stream_sha256", "A" * 64),
        ("compressed_sha256", "0" * 63),
        ("decoded_sha256", None),
        ("capture_sha256", 7),
        ("compressed_bytes", True),
        ("compressed_bytes", -1),
        ("decoded_bytes", False),
        ("decoded_bytes", CaptureLimits().maximum_decoded_bytes + 1),
    ),
)
async def test_malformed_manifest_is_refused_before_payload(bundle, field, value):
    _, captures = bundle
    document = _header(captures)
    document["captures"][0][field] = value

    async def source():
        yield _frame(document)
        pytest.fail("malformed metadata pulled a payload")

    with pytest.raises(wire.CaptureWireError):
        await wire.read_capture_body(source(), limits=CaptureLimits())
    malformed = replace(captures[0], manifest=replace(captures[0].manifest, **{field: value}))
    with pytest.raises(wire.CaptureWireError):
        await anext(wire.iter_capture_body((malformed, captures[1]), limits=CaptureLimits()))


@pytest.mark.parametrize("failure", ("missing-field", "unknown-field", "duplicate-stream", "mixed-snapshot"))
async def test_manifest_roster_is_closed_and_unambiguous(bundle, failure):
    _, captures = bundle
    document = _header(captures)
    if failure == "missing-field":
        del document["captures"][0]["capture_sha256"]
    elif failure == "unknown-field":
        document["captures"][0]["extra"] = "untrusted"
    elif failure == "duplicate-stream":
        document["captures"][1]["stream_id"] = document["captures"][0]["stream_id"]
    else:
        document["captures"][1]["source_snapshot_token"] = "different"
    with pytest.raises(wire.CaptureWireError):
        await wire.read_capture_body(_chunks(_frame(document)), limits=CaptureLimits())


@pytest.mark.parametrize("budget", ("compressed", "decoded"))
async def test_aggregate_budgets_refuse_before_payload(bundle, budget):
    _, captures = bundle
    largest = max(getattr(item.manifest, f"{budget}_bytes") for item in captures)
    limits = replace(CaptureLimits(), **{f"maximum_{budget}_bytes": largest}, maximum_record_bytes=1)

    async def source():
        yield _frame(_header(captures))
        pytest.fail("over-budget metadata pulled a payload")

    with pytest.raises(wire.CaptureWireError):
        await wire.read_capture_body(source(), limits=limits)
    with pytest.raises(wire.CaptureWireError):
        await anext(wire.iter_capture_body(captures, limits=limits))


@pytest.mark.parametrize(
    "limits",
    (
        None,
        replace(CaptureLimits(), maximum_compressed_bytes=CaptureLimits().maximum_compressed_bytes + 1),
        replace(CaptureLimits(), maximum_decoded_bytes=CaptureLimits().maximum_decoded_bytes + 1),
        replace(CaptureLimits(), read_chunk_bytes=CaptureLimits().read_chunk_bytes + 1),
    ),
)
async def test_limits_cannot_raise_existing_store_ceilings(bundle, limits):
    _, captures = bundle

    async def source():
        pytest.fail("invalid limits pulled transport bytes")
        yield b"unreachable"

    with pytest.raises(wire.CaptureWireError):
        await wire.read_capture_body(source(), limits=limits)
    with pytest.raises(wire.CaptureWireError):
        await anext(wire.iter_capture_body(captures, limits=limits))


@pytest.mark.parametrize("fragment", (b"", "text", bytearray(b"bytes"), memoryview(b"bytes")))
async def test_transport_requires_nonempty_immutable_byte_fragments(fragment):
    with pytest.raises(wire.CaptureWireError):
        await wire.read_capture_body(_chunks(fragment), limits=CaptureLimits())


async def test_codec_does_not_replace_registrar_payload_verification(bundle, monkeypatch):
    definition, captures = bundle
    damaged = replace(captures[0], payload=b"X" + captures[0].payload[1:])
    forbidden = Mock(side_effect=AssertionError("codec decoded or verified payload"))
    with monkeypatch.context() as patch:
        patch.setattr(capture, "verify_capture", forbidden)
        patch.setattr(capture, "_decoded_metrics", forbidden)
        pieces = [piece async for piece in wire.iter_capture_body((damaged,), limits=CaptureLimits())]
        result = await wire.read_capture_body(_chunks(*pieces), limits=CaptureLimits())
    assert result == (damaged,)
    forbidden.assert_not_called()
    with pytest.raises(capture.CaptureError, match="payload digest"):
        verify_capture(result[0], definition.source_streams[0])


async def test_encoder_requires_exact_declared_payload_lengths(bundle):
    _, captures = bundle
    with pytest.raises(wire.CaptureWireError):
        await anext(
            wire.iter_capture_body((replace(captures[0], payload=captures[0].payload + b"X"),), limits=CaptureLimits())
        )


def test_content_digest_matches_independent_material_and_ignores_order(bundle):
    definition, captures = bundle
    manifests = tuple(sealed.manifest for sealed in captures)
    objects = _objects(captures)
    entries = [
        {
            "stream_id": sealed.manifest.stream_id,
            "stream_sha256": sealed.manifest.stream_sha256,
            "source_snapshot_token": sealed.manifest.source_snapshot_token,
            "expected_bytes": len(sealed.payload),
            "expected_sha256": hashlib.sha256(sealed.payload).hexdigest(),
        }
        for sealed in captures
    ]
    expected = hashlib.sha256(
        b"custom-import/capture-content/v1\0"
        + _canonical(
            {
                "captures": sorted(entries, key=lambda entry: entry["stream_id"]),
            }
        )
    ).hexdigest()
    for order in (objects, tuple(reversed(objects))):
        assert wire.expected_content_sha256(definition, order, "snapshot-\u00e9") == expected
    assert wire.manifested_content_sha256(definition, manifests) == expected
    assert wire.manifested_content_sha256(definition, tuple(reversed(manifests))) == expected
    assert (
        wire.manifested_content_sha256(
            replace(definition, source_streams=tuple(reversed(definition.source_streams))), manifests
        )
        == expected
    )


def test_content_digest_excludes_transport_and_client_identity(bundle):
    definition, captures = bundle
    objects = _objects(captures)
    expected = wire.expected_content_sha256(definition, objects, "snapshot")
    for item in objects:
        item.url = "https://unrelated.example.invalid/private"
        item.path = "/synthetic/staged-object"
        item.client_id = "another-client"
    assert wire.expected_content_sha256(definition, objects, "snapshot") == expected


@pytest.mark.parametrize("field,value", (("expected_bytes", 1), ("expected_sha256", "0" * 64)))
def test_content_digest_changes_when_approved_content_changes(bundle, field, value):
    definition, captures = bundle
    objects = _objects(captures)
    expected = wire.expected_content_sha256(definition, objects, "snapshot")
    setattr(objects[0], field, value)
    assert wire.expected_content_sha256(definition, objects, "snapshot") != expected


@pytest.mark.parametrize("field,value", (("compressed_bytes", 1), ("compressed_sha256", "0" * 64)))
def test_manifest_digest_changes_when_declared_payload_changes(bundle, field, value):
    definition, captures = bundle
    manifests = tuple(sealed.manifest for sealed in captures)
    expected = wire.expected_content_sha256(definition, _objects(captures), manifests[0].source_snapshot_token)
    assert wire.manifested_content_sha256(definition, manifests) == expected
    changed = (replace(manifests[0], **{field: value}), manifests[1])
    assert wire.manifested_content_sha256(definition, changed) != expected


def test_content_digest_binds_snapshot_and_persisted_stream_shape(bundle):
    definition, captures = bundle
    objects = _objects(captures)
    expected = wire.expected_content_sha256(definition, objects, "snapshot")
    assert wire.expected_content_sha256(definition, objects, "different") != expected
    changed = replace(
        definition, source_streams=(replace(definition.source_streams[0], format="tsv"), definition.source_streams[1])
    )
    assert wire.expected_content_sha256(changed, objects, "snapshot") != expected
    with pytest.raises(wire.CaptureWireError):
        wire.manifested_content_sha256(changed, tuple(item.manifest for item in captures))


@pytest.mark.parametrize("failure", ("missing", "duplicate", "unknown", "boolean-size", "digest"))
def test_expected_content_requires_exact_complete_approved_streams(bundle, failure):
    definition, captures = bundle
    objects = list(_objects(captures))
    if failure == "missing":
        objects.pop()
    elif failure == "duplicate":
        objects[1] = objects[0]
    elif failure == "unknown":
        objects[0].stream_id = "unrecognized"
    elif failure == "boolean-size":
        objects[0].expected_bytes = True
    else:
        objects[0].expected_sha256 = "bad"
    with pytest.raises(wire.CaptureWireError):
        wire.expected_content_sha256(definition, objects, "snapshot")


@pytest.mark.parametrize("failure", ("missing", "duplicate", "unknown", "snapshot", "shape", "format", "compression"))
def test_manifested_content_refuses_substitution(bundle, failure):
    definition, captures = bundle
    manifests = [item.manifest for item in captures]
    if failure == "missing":
        manifests.pop()
    elif failure == "duplicate":
        manifests[1] = manifests[0]
    else:
        field, value = {
            "unknown": ("stream_id", "unrecognized"),
            "snapshot": ("source_snapshot_token", "different"),
            "shape": ("stream_sha256", "0" * 64),
            "format": ("format", "tsv"),
            "compression": ("compression", "gzip"),
        }[failure]
        manifests[0] = replace(manifests[0], **{field: value})
    with pytest.raises(wire.CaptureWireError):
        wire.manifested_content_sha256(definition, manifests)


@pytest.mark.parametrize("failure", ("capture-type", "payload-type", "manifest-type"))
async def test_encoder_refuses_untrusted_objects_before_first_byte(bundle, failure):
    _, captures = bundle
    original = captures[0]
    invalid_by_kind = {
        "capture-type": SimpleNamespace(payload=original.payload, manifest=original.manifest),
        "payload-type": replace(original, payload=bytearray(original.payload)),
        "manifest-type": replace(original, manifest=SimpleNamespace(**asdict(original.manifest))),
    }
    with pytest.raises(wire.CaptureWireError, match="^custom_import_capture_wire_invalid$"):
        await anext(wire.iter_capture_body((invalid_by_kind[failure],), limits=CaptureLimits()))


async def test_decoder_requires_an_async_transport_without_leaking_type_errors():
    with pytest.raises(wire.CaptureWireError, match="^custom_import_capture_wire_invalid$") as rejected:
        await wire.read_capture_body([b"untrusted synchronous input"], limits=CaptureLimits())
    assert rejected.value.__cause__ is None
    assert rejected.value.__suppress_context__ is True


async def test_fragment_budget_rejects_before_retaining_oversized_transport_data():
    reader = wire._Reader(_chunks(b"1234"), maximum=3)
    with pytest.raises(wire.CaptureWireError, match="^custom_import_capture_wire_invalid$"):
        await reader._has_bytes()
    assert reader.pending.nbytes == 0
    assert reader.received == 4


@pytest.mark.parametrize("failure", ("unparsed", "stream-type", "duplicate-stream"))
def test_content_identity_requires_typed_unique_definition_streams(bundle, failure):
    definition, captures = bundle
    invalid_by_kind = {
        "unparsed": SimpleNamespace(source_streams=definition.source_streams),
        "stream-type": replace(definition, source_streams=(SimpleNamespace(stream_id="root"),)),
        "duplicate-stream": replace(definition, source_streams=(definition.source_streams[0],) * 2),
    }
    invalid = invalid_by_kind[failure]
    with pytest.raises(wire.CaptureWireError, match="^custom_import_capture_wire_invalid$"):
        wire.expected_content_sha256(invalid, _objects(captures), "snapshot")
    with pytest.raises(wire.CaptureWireError, match="^custom_import_capture_wire_invalid$"):
        wire.manifested_content_sha256(invalid, tuple(item.manifest for item in captures))


def test_expected_content_refuses_combined_object_bytes_above_the_capture_budget(bundle):
    definition, captures = bundle
    objects = _objects(captures)
    for item in objects:
        item.expected_bytes = CaptureLimits().maximum_compressed_bytes
    with pytest.raises(wire.CaptureWireError, match="^custom_import_capture_wire_invalid$"):
        wire.expected_content_sha256(definition, objects, "snapshot")
