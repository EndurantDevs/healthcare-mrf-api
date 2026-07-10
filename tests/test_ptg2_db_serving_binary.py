# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import hashlib
import time
import zlib
from pathlib import Path
from types import SimpleNamespace

import pytest

import api.ptg2_db_sidecars as db_sidecars
from api.ptg2_db_sidecars import (
    lookup_atoms_by_price_id,
    lookup_atoms_by_price_key,
    lookup_binary_price_atoms_from_db,
    lookup_serving_binary_by_code_from_db,
    lookup_serving_binary_by_provider_set_patterns_from_db,
    lookup_serving_binary_by_provider_sets_patterns_from_db,
)
from process.ptg_parts import ptg2_serving_binary as serving_binary_writer


@pytest.fixture(autouse=True)
def clear_binary_sidecar_caches():
    db_sidecars._BINARY_DICTIONARY_CACHE.clear()
    db_sidecars._BINARY_DICTIONARY_CACHE_STATE["byte_count"] = 0
    db_sidecars._BINARY_BLOCK_CACHE.clear()
    db_sidecars._BINARY_BLOCK_CACHE_STATE["byte_count"] = 0


def test_serving_binary_skips_low_value_dictionary_compression(monkeypatch):
    payload = b"".join(
        sorted(hashlib.md5(str(item_key).encode("ascii")).digest() for item_key in range(65_536))
    )
    monkeypatch.setenv(serving_binary_writer.PTG2_SERVING_BINARY_PAYLOAD_COMPRESSION_ENV, "zlib")
    monkeypatch.setenv(
        serving_binary_writer.PTG2_SERVING_BINARY_PAYLOAD_COMPRESSION_MIN_BYTES_ENV,
        "0",
    )
    monkeypatch.setenv(
        serving_binary_writer.PTG2_SERVING_BINARY_PAYLOAD_COMPRESSION_MIN_SAVINGS_PCT_ENV,
        "2",
    )

    stored_payload, compression, raw_payload_bytes = (
        serving_binary_writer._serving_binary_payload_for_storage(payload)
    )

    assert stored_payload == payload
    assert compression == "none"
    assert raw_payload_bytes == 0


class FakeResult:
    def __init__(self, rows=None):
        self._rows = list(rows or [])

    def first(self):
        return self._rows[0] if self._rows else None

    def __iter__(self):
        return iter(self._rows)


class FakeServingBinarySession:
    def __init__(self, records_by_kind):
        self.records_by_kind = records_by_kind
        self.calls = []
        self.statements = []

    def _dictionary_metadata_result(self, artifact_kind):
        dictionary_records = self.records_by_kind.get(artifact_kind, [])
        if not dictionary_records:
            return FakeResult()
        dictionary_record = dictionary_records[0]
        return FakeResult(
            rows=[
                {
                    "payload_compression": dictionary_record.get("payload_compression", "none"),
                    "raw_payload_bytes": dictionary_record.get("raw_payload_bytes", 0),
                    "payload_bytes": len(dictionary_record.get("payload") or b""),
                }
            ]
        )

    def _dictionary_slice_result(self, artifact_kind, item_keys):
        dictionary_records = self.records_by_kind.get(artifact_kind, [])
        if not dictionary_records:
            return FakeResult()
        dictionary_payload = dictionary_records[0].get("payload") or b""
        return FakeResult(
            rows=[
                {
                    "item_key": item_key,
                    "item_value": dictionary_payload[item_key * 16 : item_key * 16 + 16],
                }
                for item_key in item_keys
            ]
        )

    async def execute(self, _statement, params=None):
        query_params_dict = dict(params or {})
        self.calls.append(query_params_dict)
        statement_text = str(_statement)
        self.statements.append(statement_text)
        artifact_kind = query_params_dict.get("artifact_kind")
        if "octet_length(binary_block.payload) AS payload_bytes" in statement_text:
            return self._dictionary_metadata_result(artifact_kind)
        if "substring(" in statement_text and "requested.item_key" in statement_text:
            return self._dictionary_slice_result(
                artifact_kind,
                query_params_dict.get("item_keys") or [],
            )
        if "block_keys" in query_params_dict:
            records = []
            for block_key in query_params_dict.get("block_keys") or []:
                records.extend(
                    {**record, "block_key": block_key}
                    for record in self.records_by_kind.get((artifact_kind, block_key), [])
                )
            return FakeResult(rows=records)
        block_key = query_params_dict.get("block_key")
        records = self.records_by_kind.get((artifact_kind, block_key), self.records_by_kind.get(artifact_kind, []))
        return FakeResult(rows=list(records))


class FakeTransaction:
    def __init__(self, driver):
        self.driver = driver

    async def __aenter__(self):
        self.driver.events.append("begin")

    async def __aexit__(self, exc_type, exc, tb):
        self.driver.events.append("rollback" if exc_type else "commit")


class FakeCopyDriver:
    def __init__(self):
        self.events = []

    def transaction(self):
        return FakeTransaction(self)

    async def execute(self, statement):
        self.events.append(statement)

    async def copy_from_query(self, sql, *, output, **copy_options):
        self.events.append((sql, copy_options["format"], copy_options["delimiter"], copy_options["null"]))
        await output(b"copy-row\n")
        return "copied"


class FakeProcessStdin:
    def __init__(self):
        self.chunks = []
        self.closed = False

    def write(self, data):
        self.chunks.append(data)

    async def drain(self):
        return None

    def close(self):
        self.closed = True

    async def wait_closed(self):
        return None


class FakeRustProcess:
    def __init__(self):
        self.stdin = FakeProcessStdin()


def _uvarint(value: int) -> bytes:
    payload = bytearray()
    value = int(value)
    while value >= 0x80:
        payload.append((value & 0x7F) | 0x80)
        value >>= 7
    payload.append(value)
    return bytes(payload)


@pytest.mark.asyncio
async def test_db_serving_binary_dictionary_reads_metadata_without_json_encoding_row():
    price_set_id = bytes.fromhex("00000000000000000000000000000041")
    fake_session = FakeServingBinarySession(
        {
            "by_code_price_dictionary": [
                {
                    "payload": price_set_id,
                    "payload_compression": "none",
                    "raw_payload_bytes": 0,
                }
            ]
        }
    )

    payload = await db_sidecars._serving_binary_dictionary_payload(
        fake_session,
        "mrf.ptg2_serving_binary_large_dictionary",
        artifact_kind="by_code_price_dictionary",
    )

    assert payload == price_set_id
    assert all("to_jsonb(binary_block)" not in statement for statement in fake_session.statements)
    assert "binary_block.payload_compression" in fake_session.statements[-1]
    assert "binary_block.raw_payload_bytes" in fake_session.statements[-1]


def test_serving_binary_copy_work_mem_default_and_rollback(monkeypatch):
    monkeypatch.delenv(serving_binary_writer.PTG2_SERVING_BINARY_COPY_WORK_MEM_ENV, raising=False)
    assert serving_binary_writer._serving_binary_copy_work_mem() == "128MB"

    monkeypatch.setenv(serving_binary_writer.PTG2_SERVING_BINARY_COPY_WORK_MEM_ENV, "0")
    assert serving_binary_writer._serving_binary_copy_work_mem() is None


def test_serving_binary_copy_work_mem_rejects_unsafe_values(monkeypatch):
    monkeypatch.setenv(serving_binary_writer.PTG2_SERVING_BINARY_COPY_WORK_MEM_ENV, "128MB; DROP TABLE x")
    assert serving_binary_writer._serving_binary_copy_work_mem() == "128MB"


@pytest.mark.asyncio
async def test_copy_pg_query_to_rust_sets_local_work_mem(monkeypatch):
    monkeypatch.setenv(serving_binary_writer.PTG2_SERVING_BINARY_COPY_WORK_MEM_ENV, "256MB")
    driver = FakeCopyDriver()
    process = FakeRustProcess()
    metrics_by_name = {}

    result = await serving_binary_writer._copy_pg_query_to_rust(
        driver,
        "SELECT 1",
        process,
        metrics=metrics_by_name,
        started_at=time.monotonic(),
    )

    assert result == "copied"
    assert driver.events == [
        "begin",
        "SET LOCAL work_mem TO '256MB'",
        ("SELECT 1", "text", "\t", "\\N"),
        "commit",
    ]
    assert process.stdin.chunks == [b"copy-row\n"]
    assert process.stdin.closed is True
    assert metrics_by_name["source_copy_bytes"] == len(b"copy-row\n")
    assert metrics_by_name["source_first_byte_seconds"] >= 0
    assert metrics_by_name["source_copy_complete_seconds"] >= metrics_by_name["source_first_byte_seconds"]


@pytest.mark.asyncio
async def test_rust_stdout_chunks_records_target_copy_metrics():
    class FakeStdout:
        def __init__(self):
            self.chunks = [b"first", b"second", b""]

        async def read(self, _size):
            return self.chunks.pop(0)

    metrics_by_name = {}
    chunks = [
        chunk
        async for chunk in serving_binary_writer._rust_stdout_chunks(
            SimpleNamespace(stdout=FakeStdout()),
            metrics=metrics_by_name,
            started_at=time.monotonic(),
        )
    ]

    assert chunks == [b"first", b"second"]
    assert metrics_by_name["target_copy_bytes"] == len(b"firstsecond")
    assert metrics_by_name["target_first_byte_seconds"] >= 0
    assert metrics_by_name["target_copy_complete_seconds"] >= metrics_by_name["target_first_byte_seconds"]


@pytest.mark.asyncio
async def test_rust_stream_cancellation_kills_and_drains_process(monkeypatch):
    class FakeStderr:
        async def read(self):
            return b""

    class CancelledRustProcess:
        def __init__(self):
            self.stdin = FakeProcessStdin()
            self.stdout = object()
            self.stderr = FakeStderr()
            self.returncode = None
            self.killed = False
            self.waited = False
            self.wait_started = asyncio.Event()

        def kill(self):
            self.killed = True
            self.returncode = -9

        async def wait(self):
            self.waited = True
            if self.returncode is None:
                self.wait_started.set()
                await asyncio.Event().wait()
            return self.returncode

    process = CancelledRustProcess()

    async def fake_create_subprocess_exec(*_args, **_kwargs):
        return process

    async def fake_copy_through_rust_process(**_kwargs):
        return None

    monkeypatch.setattr(serving_binary_writer, "_ptg2_rust_scanner_binary", lambda: Path("scanner"))
    monkeypatch.setattr(serving_binary_writer.asyncio, "create_subprocess_exec", fake_create_subprocess_exec)
    monkeypatch.setattr(serving_binary_writer, "_copy_through_rust_process", fake_copy_through_rust_process)
    stream_task = asyncio.create_task(
        serving_binary_writer._run_rust_stream_copy(
            encoder_kind="by_code",
            failure_label="test",
            sql="SELECT 1",
            schema_name="mrf",
            target_table="serving",
            source_copy_format="text",
            target_copy_format="text",
        )
    )
    await process.wait_started.wait()

    stream_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await stream_task

    assert process.killed is True
    assert process.waited is True
    assert process.stdin.closed is True


@pytest.mark.asyncio
async def test_db_serving_binary_by_code_reads_matching_provider_sets():
    first_price_set_id = bytes.fromhex("00000000000000000000000000000051")
    second_price_set_id = bytes.fromhex("00000000000000000000000000000052")
    by_code_payload = b"".join(_uvarint(payload_part) for payload_part in (3, 10, 0, 2, 20, 1))
    session = FakeServingBinarySession(
        {
            ("by_code", 7): [{"block_no": 0, "entry_count": 2, "payload": by_code_payload}],
            "by_code_price_dictionary": [{"payload": first_price_set_id + second_price_set_id}],
        }
    )

    matched_serving_rows = await lookup_serving_binary_by_code_from_db(
        session,
        "mrf.ptg2_serving_binary_test",
        7,
        provider_set_keys=[5],
    )

    assert [
        (serving_row.code_key, serving_row.provider_set_key, serving_row.provider_count, serving_row.price_set_global_id_128)
        for serving_row in matched_serving_rows
    ] == [(7, 5, 20, second_price_set_id.hex())]
    assert any(call.get("block_nos") == [0] for call in session.calls)
    assert not any("substring(" in statement for statement in session.statements)


@pytest.mark.asyncio
async def test_db_serving_binary_uses_uncompressed_manifest_dictionary_hint():
    price_set_id = bytes.fromhex("00000000000000000000000000000052")
    by_code_payload = b"".join(_uvarint(payload_part) for payload_part in (5, 20, 0))
    session = FakeServingBinarySession(
        {
            ("by_code", 7): [{"block_no": 0, "entry_count": 1, "payload": by_code_payload}],
            "by_code_price_dictionary": [{"payload": price_set_id}],
        }
    )

    rows = await lookup_serving_binary_by_code_from_db(
        session,
        "mrf.ptg2_serving_binary_hint_test",
        7,
        provider_set_keys=[5],
        price_dictionary_item_count=1,
        price_dictionary_block_bytes=16,
        price_dictionary_compressed_records=0,
    )

    assert rows[0].price_set_global_id_128 == price_set_id.hex()
    assert not any("octet_length(binary_block.payload)" in statement for statement in session.statements)
    assert any(call.get("block_nos") == [0] for call in session.calls)
    assert not any("substring(" in statement for statement in session.statements)


@pytest.mark.asyncio
async def test_db_serving_binary_by_code_reads_compressed_payloads():
    first_price_set_id = bytes.fromhex("00000000000000000000000000000091")
    second_price_set_id = bytes.fromhex("00000000000000000000000000000092")
    uncompressed_by_code_payload = b"".join(_uvarint(payload_part) for payload_part in (3, 10, 0, 2, 20, 1))
    uncompressed_price_dictionary = first_price_set_id + second_price_set_id
    session = FakeServingBinarySession(
        {
            ("by_code", 7): [
                {
                    "block_no": 0,
                    "entry_count": 2,
                    "payload": zlib.compress(uncompressed_by_code_payload),
                    "payload_compression": "zlib",
                    "raw_payload_bytes": len(uncompressed_by_code_payload),
                }
            ],
            "by_code_price_dictionary": [
                {
                    "payload": zlib.compress(uncompressed_price_dictionary),
                    "payload_compression": "zlib",
                    "raw_payload_bytes": len(uncompressed_price_dictionary),
                }
            ],
        }
    )

    matched_serving_rows = await lookup_serving_binary_by_code_from_db(
        session,
        "mrf.ptg2_serving_binary_compressed_test",
        7,
        provider_set_keys=[5],
    )

    assert [
        (serving_row.code_key, serving_row.provider_set_key, serving_row.provider_count, serving_row.price_set_global_id_128)
        for serving_row in matched_serving_rows
    ] == [(7, 5, 20, second_price_set_id.hex())]


@pytest.mark.asyncio
async def test_db_serving_binary_cached_compressed_blocks_skip_repeated_decompression(monkeypatch):
    first_price_set_id = bytes.fromhex("00000000000000000000000000000093")
    second_price_set_id = bytes.fromhex("00000000000000000000000000000094")
    uncompressed_by_code_payload = b"".join(_uvarint(payload_part) for payload_part in (3, 10, 0, 2, 20, 1))
    uncompressed_price_dictionary = first_price_set_id + second_price_set_id
    fake_session = FakeServingBinarySession(
        {
            ("by_code", 7): [
                {
                    "block_no": 0,
                    "entry_count": 2,
                    "payload": zlib.compress(uncompressed_by_code_payload),
                    "payload_compression": "zlib",
                    "raw_payload_bytes": len(uncompressed_by_code_payload),
                }
            ],
            "by_code_price_dictionary": [
                {
                    "payload": zlib.compress(uncompressed_price_dictionary),
                    "payload_compression": "zlib",
                    "raw_payload_bytes": len(uncompressed_price_dictionary),
                }
            ],
        }
    )
    decompress_call_count_by_name = {"count": 0}
    original_decompress = db_sidecars.zlib.decompress

    def counting_decompress(payload):
        decompress_call_count_by_name["count"] += 1
        return original_decompress(payload)

    monkeypatch.setattr(db_sidecars.zlib, "decompress", counting_decompress)

    await lookup_serving_binary_by_code_from_db(fake_session, "mrf.ptg2_serving_binary_compressed_cache", 7)
    await lookup_serving_binary_by_code_from_db(fake_session, "mrf.ptg2_serving_binary_compressed_cache", 7)

    assert decompress_call_count_by_name["count"] == 2


@pytest.mark.asyncio
async def test_db_serving_binary_by_code_reads_provider_count_dictionary():
    first_price_set_id = bytes.fromhex("00000000000000000000000000000071")
    second_price_set_id = bytes.fromhex("00000000000000000000000000000072")
    by_code_payload = b"".join(_uvarint(payload_part) for payload_part in (3, 0, 2, 1))
    count_dictionary_payload = b"".join(_uvarint(payload_part) for payload_part in (3, 10, 2, 20))
    fake_session = FakeServingBinarySession(
        {
            ("by_code", 7): [{"block_no": 0, "entry_count": 2, "payload": by_code_payload}],
            "by_code_price_dictionary": [{"payload": first_price_set_id + second_price_set_id}],
            "provider_set_count_dictionary": [{"entry_count": 2, "payload": count_dictionary_payload}],
        }
    )

    decoded_rows = await lookup_serving_binary_by_code_from_db(
        fake_session,
        "mrf.ptg2_serving_binary_test",
        7,
        provider_set_keys=[5],
    )

    assert [
        (decoded_row.code_key, decoded_row.provider_set_key, decoded_row.provider_count, decoded_row.price_set_global_id_128)
        for decoded_row in decoded_rows
    ] == [(7, 5, 20, second_price_set_id.hex())]


@pytest.mark.asyncio
async def test_db_serving_binary_by_code_reads_grouped_provider_prices():
    first_price_set_id = bytes.fromhex("000000000000000000000000000000a1")
    second_price_set_id = bytes.fromhex("000000000000000000000000000000a2")
    third_price_set_id = bytes.fromhex("000000000000000000000000000000a3")
    grouped_payload = b"".join(_uvarint(payload_part) for payload_part in (5, 2, 0, 1, 2, 1, 2))
    count_dictionary_payload = b"".join(_uvarint(payload_part) for payload_part in (5, 20, 2, 30))
    fake_session = FakeServingBinarySession(
        {
            ("by_code_grouped", 7): [{"block_no": 0, "entry_count": 2, "payload": grouped_payload}],
            "by_code_price_dictionary": [
                {"payload": first_price_set_id + second_price_set_id + third_price_set_id}
            ],
            "provider_set_count_dictionary": [{"entry_count": 2, "payload": count_dictionary_payload}],
        }
    )

    decoded_rows = await lookup_serving_binary_by_code_from_db(
        fake_session,
        "mrf.ptg2_serving_binary_grouped_test",
        7,
        provider_set_keys=[5],
    )

    assert [
        (decoded_row.code_key, decoded_row.provider_set_key, decoded_row.provider_count, decoded_row.price_set_global_id_128)
        for decoded_row in decoded_rows
    ] == [
        (7, 5, 20, first_price_set_id.hex()),
        (7, 5, 20, second_price_set_id.hex()),
    ]


@pytest.mark.asyncio
async def test_db_serving_binary_reads_price_set_atom_blocks():
    first_price_set_id = bytes.fromhex("000000000000000000000000000000b1")
    second_price_set_id = bytes.fromhex("000000000000000000000000000000b2")
    first_atom_id = bytes.fromhex("000000000000000000000000000000c1")
    second_atom_id = bytes.fromhex("000000000000000000000000000000c2")
    third_atom_id = bytes.fromhex("000000000000000000000000000000c3")
    atom_payload = b"".join(
        (
            _uvarint(0),
            _uvarint(2),
            first_atom_id,
            second_atom_id,
            _uvarint(1),
            _uvarint(1),
            third_atom_id,
        )
    )
    fake_session = FakeServingBinarySession(
        {
            "by_code_price_dictionary": [{"payload": first_price_set_id + second_price_set_id}],
            ("price_set_atoms", 0): [{"block_key": 0, "block_no": 0, "entry_count": 2, "payload": atom_payload}],
        }
    )

    members_by_price_set = await lookup_binary_price_atoms_from_db(
        fake_session,
        "mrf.ptg2_serving_binary_price_atoms_test",
        [second_price_set_id.hex(), first_price_set_id.hex()],
    )

    assert members_by_price_set == {
        first_price_set_id.hex(): (first_atom_id.hex(), second_atom_id.hex()),
        second_price_set_id.hex(): (third_atom_id.hex(),),
    }


@pytest.mark.asyncio
async def test_db_serving_binary_reads_price_set_atom_blocks_by_known_key():
    first_price_set_id = "000000000000000000000000000000b1"
    second_price_set_id = "000000000000000000000000000000b2"
    first_atom_id = bytes.fromhex("000000000000000000000000000000c1")
    second_atom_id = bytes.fromhex("000000000000000000000000000000c2")
    third_atom_id = bytes.fromhex("000000000000000000000000000000c3")
    atom_payload = b"".join(
        (
            _uvarint(0),
            _uvarint(2),
            first_atom_id,
            second_atom_id,
            _uvarint(1),
            _uvarint(1),
            third_atom_id,
        )
    )
    fake_session = FakeServingBinarySession(
        {
            ("price_set_atoms", 0): [{"block_key": 0, "block_no": 0, "entry_count": 2, "payload": atom_payload}],
        }
    )

    members_by_price_set = await lookup_atoms_by_price_key(
        fake_session,
        "mrf.ptg2_serving_binary_price_atoms_test",
        {second_price_set_id: 1, first_price_set_id: 0},
    )

    assert members_by_price_set == {
        first_price_set_id: (first_atom_id.hex(), second_atom_id.hex()),
        second_price_set_id: (third_atom_id.hex(),),
    }
    assert "by_code_price_dictionary" not in [call.get("artifact_kind") for call in fake_session.calls]


@pytest.mark.asyncio
async def test_db_serving_binary_reads_price_set_atom_blocks_by_price_id():
    first_price_set_id = bytes.fromhex("000000000000000000000000000000b1")
    second_price_set_id = bytes.fromhex("000000000000000000000000000000b2")
    first_atom_id = bytes.fromhex("000000000000000000000000000000c1")
    second_atom_id = bytes.fromhex("000000000000000000000000000000c2")
    third_atom_id = bytes.fromhex("000000000000000000000000000000c3")
    atom_payload = b"".join(
        (
            first_price_set_id,
            _uvarint(2),
            first_atom_id,
            second_atom_id,
            second_price_set_id,
            _uvarint(1),
            third_atom_id,
        )
    )
    fake_session = FakeServingBinarySession(
        {
            ("price_set_atoms_by_id", 0): [
                {"block_key": 0, "block_no": 0, "entry_count": 2, "payload": atom_payload}
            ],
        }
    )

    members_by_price_set = await lookup_atoms_by_price_id(
        fake_session,
        "mrf.ptg2_serving_binary_price_atoms_test",
        [second_price_set_id.hex(), first_price_set_id.hex()],
    )

    assert members_by_price_set == {
        first_price_set_id.hex(): (first_atom_id.hex(), second_atom_id.hex()),
        second_price_set_id.hex(): (third_atom_id.hex(),),
    }
    assert "by_code_price_dictionary" not in [call.get("artifact_kind") for call in fake_session.calls]


@pytest.mark.asyncio
async def test_db_serving_binary_reads_v2_price_set_atom_prefix_bucket():
    price_set_id = bytes.fromhex("123400000000000000000000000000b1")
    price_atom_id = bytes.fromhex("000000000000000000000000000000c1")
    atom_payload = b"".join((price_set_id, _uvarint(1), price_atom_id))
    fake_session = FakeServingBinarySession(
        {
            ("price_set_atoms_by_id_v2", 0x1234): [
                {"block_key": 0x1234, "block_no": 0, "entry_count": 1, "payload": atom_payload}
            ],
        }
    )

    members_by_price_set = await lookup_atoms_by_price_id(
        fake_session,
        "mrf.ptg2_serving_binary_price_atoms_v2_test",
        [price_set_id.hex()],
    )

    assert members_by_price_set == {price_set_id.hex(): (price_atom_id.hex(),)}
    assert "price_set_atoms_by_id" not in [call.get("artifact_kind") for call in fake_session.calls]


@pytest.mark.asyncio
async def test_db_serving_binary_reuses_cached_dictionaries_for_same_table():
    price_set_id = bytes.fromhex("00000000000000000000000000000081")
    first_payload = b"".join(_uvarint(payload_part) for payload_part in (1, 10, 0))
    second_payload = b"".join(_uvarint(payload_part) for payload_part in (2, 20, 0))
    fake_session = FakeServingBinarySession(
        {
            ("by_code", 7): [{"block_no": 0, "entry_count": 1, "payload": first_payload}],
            ("by_code", 8): [{"block_no": 0, "entry_count": 1, "payload": second_payload}],
            "by_code_price_dictionary": [{"payload": price_set_id}],
        }
    )

    first_rows = await lookup_serving_binary_by_code_from_db(fake_session, "mrf.ptg2_serving_binary_cache_test", 7)
    dictionary_calls_after_first_lookup = sum(
        call.get("artifact_kind") == "by_code_price_dictionary"
        for call in fake_session.calls
    )
    second_rows = await lookup_serving_binary_by_code_from_db(fake_session, "mrf.ptg2_serving_binary_cache_test", 8)

    assert [serving_row.provider_count for serving_row in first_rows] == [10]
    assert [serving_row.provider_count for serving_row in second_rows] == [20]
    assert dictionary_calls_after_first_lookup == 2
    assert sum(
        call.get("artifact_kind") == "by_code_price_dictionary"
        for call in fake_session.calls
    ) == dictionary_calls_after_first_lookup
    assert sum(call.get("artifact_kind") == "provider_set_count_dictionary" for call in fake_session.calls) == 1


@pytest.mark.asyncio
async def test_db_serving_binary_reuses_cached_block_rows_for_same_table():
    price_set_id = bytes.fromhex("00000000000000000000000000000082")
    by_code_payload = b"".join(_uvarint(payload_part) for payload_part in (1, 10, 0))
    fake_session = FakeServingBinarySession(
        {
            ("by_code", 7): [{"block_no": 0, "entry_count": 1, "payload": by_code_payload}],
            "by_code_price_dictionary": [{"payload": price_set_id}],
        }
    )

    first_rows = await lookup_serving_binary_by_code_from_db(fake_session, "mrf.ptg2_serving_binary_block_cache", 7)
    second_rows = await lookup_serving_binary_by_code_from_db(fake_session, "mrf.ptg2_serving_binary_block_cache", 7)

    assert [serving_row.provider_count for serving_row in first_rows] == [10]
    assert [serving_row.provider_count for serving_row in second_rows] == [10]
    assert sum(call.get("artifact_kind") == "by_code_grouped" for call in fake_session.calls) == 1
    assert sum(call.get("artifact_kind") == "by_code" for call in fake_session.calls) == 1


@pytest.mark.asyncio
async def test_db_serving_binary_provider_set_patterns_support_reverse_lookup():
    first_price_set_id = bytes.fromhex("00000000000000000000000000000061")
    second_price_set_id = bytes.fromhex("00000000000000000000000000000062")
    by_provider_payload = b"".join(_uvarint(payload_part) for payload_part in (2, 7, 2, 2, 10, 0, 20, 1))
    session = FakeServingBinarySession(
        {
            ("by_provider_set", 5): [{"block_no": 0, "entry_count": 1, "payload": by_provider_payload}],
            "by_provider_set_price_dictionary": [{"payload": first_price_set_id + second_price_set_id}],
        }
    )

    patterns = await lookup_serving_binary_by_provider_set_patterns_from_db(
        session,
        "mrf.ptg2_serving_binary_test",
        5,
        code_keys=[9],
    )

    assert len(patterns) == 1
    assert patterns[0].code_keys == (9,)
    assert patterns[0].entries == ((10, first_price_set_id.hex()), (20, second_price_set_id.hex()))


@pytest.mark.asyncio
async def test_db_serving_binary_batches_reverse_lookup_for_provider_sets():
    first_price_set_id = bytes.fromhex("00000000000000000000000000000071")
    second_price_set_id = bytes.fromhex("00000000000000000000000000000072")
    first_provider_payload = b"".join(
        _uvarint(payload_part) for payload_part in (2, 7, 2, 2, 10, 0, 20, 1)
    )
    second_provider_payload = b"".join(
        _uvarint(payload_part) for payload_part in (1, 9, 1, 30, 1)
    )
    session = FakeServingBinarySession(
        {
            ("by_provider_set", 5): [
                {"block_no": 0, "entry_count": 1, "payload": first_provider_payload}
            ],
            ("by_provider_set", 6): [
                {"block_no": 0, "entry_count": 1, "payload": second_provider_payload}
            ],
            "by_provider_set_price_dictionary": [
                {"payload": first_price_set_id + second_price_set_id}
            ],
        }
    )

    patterns_by_provider_set = await lookup_serving_binary_by_provider_sets_patterns_from_db(
        session,
        "mrf.ptg2_serving_binary_test",
        [5, 6],
        code_keys=[9],
    )

    assert patterns_by_provider_set[5][0].entries == (
        (10, first_price_set_id.hex()),
        (20, second_price_set_id.hex()),
    )
    assert patterns_by_provider_set[6][0].entries == ((30, second_price_set_id.hex()),)
    reverse_calls = [
        call for call in session.calls if call.get("artifact_kind") == "by_provider_set"
    ]
    assert len(reverse_calls) == 1
    assert reverse_calls[0]["block_keys"] == [5, 6]
