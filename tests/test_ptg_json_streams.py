# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import io
import json

import pytest

from process.ptg_parts import json_streams


def test_byte_scanner_extracts_only_selected_top_level_array_objects():
    """Keep nested syntax and escaped string bytes intact across tiny reads."""

    source_bytes = (
        b'{"ignored":[{"skip":1}],'
        b'"in_network":{"not":"an array"},'
        b'"note":"in_network",'
        b'"provider_references" : [{"provider_groups":[{"npi":[1,2]}]}],'
        b'"in_network":[{"text":"brace } and \\"quote\\"",'
        b'"nested":{"items":[1,{"value":2}]}},{"code":"99213"}]}'
    )

    observed_entry_list = list(
        json_streams._iter_top_level_object_bytes(
            io.BytesIO(source_bytes),
            {"in_network", "provider_references"},
            chunk_size=1,
        )
    )

    assert [name for name, _raw in observed_entry_list] == [
        "provider_references",
        "in_network",
        "in_network",
    ]
    assert [json.loads(raw) for _name, raw in observed_entry_list] == [
        {"provider_groups": [{"npi": [1, 2]}]},
        {
            "text": 'brace } and "quote"',
            "nested": {"items": [1, {"value": 2}]},
        },
        {"code": "99213"},
    ]


def test_byte_scanner_uses_configured_chunk_size_and_reports_large_reads(
    monkeypatch,
):
    """Honor the configured buffer and preserve the progress callback contract."""

    class ProgressSizedBytes(bytes):
        def __len__(self):
            return 64 * 1024 * 1024

    class OneChunkStream:
        def __init__(self):
            self.read_sizes = []
            self.returned = False

        def read(self, size):
            self.read_sizes.append(size)
            if self.returned:
                return b""
            self.returned = True
            return ProgressSizedBytes(b'{"in_network":[{"code":"1"}]}')

    stream = OneChunkStream()
    progress_calls = []
    monkeypatch.setattr(json_streams, "_stream_buffer_bytes", lambda: 17)

    observed_entry_list = list(
        json_streams._iter_top_level_object_bytes(
            stream,
            {"in_network"},
            progress_callback=lambda: progress_calls.append(True),
        )
    )

    assert observed_entry_list == [("in_network", b'{"code":"1"}')]
    assert stream.read_sizes == [17, 17]
    assert progress_calls == [True]


def test_ijson_iterator_builds_selected_maps_and_arrays(monkeypatch):
    """Exercise the object-builder fallback without materializing other arrays."""

    events = []
    for index in range(100_001):
        events.append(("ignored.item", "number", index))
    events.extend(
        [
            ("in_network.item", "start_map", None),
            ("in_network.item", "map_key", "code"),
            ("in_network.item.code", "string", "99213"),
            ("in_network.item", "end_map", None),
            ("provider_references.item", "start_array", None),
            ("provider_references.item.item", "number", 1),
            ("provider_references.item", "end_array", None),
        ]
    )
    monkeypatch.setattr(
        json_streams.ijson,
        "parse",
        lambda _file_obj, use_float: iter(events),
    )
    progress_calls = []

    observed_entry_list = list(
        json_streams._iter_top_level_objects(
            object(),
            {
                "network": "in_network.item",
                "providers": "provider_references.item",
            },
            progress_callback=lambda: progress_calls.append(True),
        )
    )

    assert observed_entry_list == [
        ("network", {"code": "99213"}),
        ("providers", [1]),
    ]
    assert progress_calls == [True]


def test_jsondecoder_iterator_handles_split_keys_utf8_raw_and_decoded_values():
    """Decode selected arrays incrementally and optionally retain exact object JSON."""

    payload = (
        '{"lookalike":"in_network","not_array":{},'
        '"provider_references":[{"name":"M\u00fcnchen"}],'
        '"in_network" : [ {"code":"99213"}, {"code":"99214"} ]}'
    ).encode("utf-8")

    decoded_entry_list = list(
        json_streams._iter_top_level_objects_jsondecoder(
            io.BytesIO(payload),
            {
                "network": "in_network.item",
                "providers": "provider_references.item",
                "not_array": "not_array.item",
                "ignored": "not-an-item-prefix",
            },
            chunk_size=2,
            raw_object_names={"providers"},
        )
    )

    assert decoded_entry_list[0][0] == "providers"
    assert json.loads(decoded_entry_list[0][1]) == {"name": "M\u00fcnchen"}
    assert decoded_entry_list[1:] == [
        ("network", {"code": "99213"}),
        ("network", {"code": "99214"}),
    ]


def test_jsondecoder_iterator_empty_contract_and_truncated_object_fail_closed():
    assert list(
        json_streams._iter_top_level_objects_jsondecoder(
            io.BytesIO(b"{}"),
            {"ignored": "not-an-item-prefix"},
        )
    ) == []

    with pytest.raises(json.JSONDecodeError):
        list(
            json_streams._iter_top_level_objects_jsondecoder(
                io.BytesIO(b'{"in_network":[{"code":'),
                {"network": "in_network.item"},
                chunk_size=3,
            )
        )


@pytest.mark.parametrize(
    "payload",
    [
        b'{"in_network"',
        b'{"in_network":',
        b'{"in_network":[ ',
    ],
)
def test_jsondecoder_iterator_stops_before_an_incomplete_array_object(payload):
    assert list(
        json_streams._iter_top_level_objects_jsondecoder(
            io.BytesIO(payload),
            {"network": "in_network.item"},
            chunk_size=3,
        )
    ) == []


def test_jsondecoder_iterator_reports_large_reads():
    class ProgressSizedBytes(bytes):
        def __len__(self):
            return 64 * 1024 * 1024

    class OneChunkStream:
        def __init__(self):
            self.returned = False

        def read(self, _size):
            if self.returned:
                return b""
            self.returned = True
            return ProgressSizedBytes(b'{"in_network":[{"code":"1"}]}')

    progress_calls = []
    assert list(
        json_streams._iter_top_level_objects_jsondecoder(
            OneChunkStream(),
            {"network": "in_network.item"},
            progress_callback=lambda: progress_calls.append(True),
            chunk_size=17,
        )
    ) == [("network", {"code": "1"})]
    assert progress_calls == [True]


def test_fast_iterator_selects_decoder_or_byte_scanner(monkeypatch):
    payload = b'{"in_network":[{"code":"99213"}]}'
    item_prefix_by_name = {"network": "in_network.item"}

    monkeypatch.setattr(json_streams, "_env_bool", lambda *_args: True)
    assert list(
        json_streams._iter_top_level_objects_fast(
            io.BytesIO(payload),
            item_prefix_by_name,
        )
    ) == [("network", {"code": "99213"})]

    monkeypatch.setattr(json_streams, "_env_bool", lambda *_args: False)
    monkeypatch.setattr(json_streams, "orjson", None)
    assert list(
        json_streams._iter_top_level_objects_fast(
            io.BytesIO(payload),
            item_prefix_by_name,
        )
    ) == [("network", {"code": "99213"})]


def test_json_loads_uses_available_accelerator(monkeypatch):
    calls = []

    class FakeOrjson:
        @staticmethod
        def loads(value):
            calls.append(value)
            return {"decoded": True}

    monkeypatch.setattr(json_streams, "orjson", FakeOrjson())
    monkeypatch.setattr(json_streams, "_env_bool", lambda *_args: True)

    assert json_streams._json_loads(b"{}") == {"decoded": True}
    assert calls == [b"{}"]
