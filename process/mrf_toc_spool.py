# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Disk-backed TOC parsing and awaited row batches for discovery."""

from __future__ import annotations

import asyncio
import codecs
import datetime as dt
import gzip
import json
import sqlite3
import sys
import tempfile
import zlib
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass, fields
from decimal import Decimal
from pathlib import Path
from typing import Any, AsyncIterator, Awaitable, Callable

import ijson
from ijson.common import ObjectBuilder

from process.ptg_parts.canonical import canonical_json_dumps
from process.ptg_parts.domain import PTG2SourceCatalogEntry
from process.ptg_parts.source_jobs import _normalize_plan_payload, parse_toc_catalog_entries
from process.ptg_parts.toc_entries import _flat_toc_section_type

_CHUNK_BYTES = 64 * 1024
_SQLITE_DISK_FACTOR = 6  # Main pages, rollback journal and conservative SQLite allocation overhead.
_PLAN_KEYS = ("reportingPlans", "reporting_plans", "plans", "planInfo", "plan_info")
_FILE_KEYS = (
    "in_network_files",
    "allowed_amount_file",
    "allowed_amount_files",
    "drug_file",
    "drug_files",
    "ndc_file",
    "ndc_files",
    "prescription_drug_file",
    "prescription_drug_files",
    "payer_specific_drug_files",
)
_METADATA_KEYS = ("reporting_entity_name", "reporting_entity_type", "last_updated_on", "version", "description")
_METADATA_LISTS = ("files", "items", "results", "data.files", "data.items", "data.results", "data")


class IncompleteToc(ValueError):
    """The complete TOC cannot be represented within the admitted budgets."""


class DecodedTocBudget:
    """Share one pre-charset decoded-byte allowance across a container's documents."""

    def __init__(self, limit: int):
        if type(limit) is not int or limit < 1:
            raise IncompleteToc("decoded TOC byte budget must be positive")
        self.limit = limit
        self.used = 0

    def claim(self, byte_count: int) -> None:
        """Charge only JSON bytes after any nested gzip decoding."""
        if self.used + byte_count > self.limit:
            raise IncompleteToc("decoded TOC members exceed the combined discovery byte limit")
        self.used += byte_count


class _StorageBudget:
    def __init__(self, limit: int):
        if type(limit) is not int or limit < 1:
            raise IncompleteToc("TOC spool byte budget must be positive")
        self.limit = limit
        self.used = 0
        self.connection = None

    def claim(self, byte_count: int) -> None:
        """Reserve file bytes alongside conservative SQLite temporary overhead."""
        if self.used + byte_count + self._database_bytes() > self.limit:
            raise IncompleteToc("TOC spool byte budget exhausted")
        self.used += byte_count

    def release(self, byte_count: int) -> None:
        """Release only bytes belonging to an exact removed scratch file."""
        self.used -= byte_count

    def _database_bytes(self) -> int:
        if self.connection is None:
            return 0
        page_size = self.connection.execute("PRAGMA page_size").fetchone()[0]
        page_count = self.connection.execute("PRAGMA page_count").fetchone()[0]
        return _SQLITE_DISK_FACTOR * page_size * page_count

    def limit_database(self, repair_reserve: int = 0) -> None:
        """Bound SQLite pages before writes, including reserved repair bytes."""
        page_size = self.connection.execute("PRAGMA page_size").fetchone()[0]
        pages = (self.limit - self.used - repair_reserve) // (_SQLITE_DISK_FACTOR * page_size)
        if pages < 1 or self.connection.execute(f"PRAGMA max_page_count={pages}").fetchone()[0] > pages:
            raise IncompleteToc("TOC spool has insufficient database space")


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _heap_size(value: Any) -> int:
    """Count a bounded row's owned containers as well as its serialized bytes."""
    seen_ids: set[int] = set()

    def visit(item: Any) -> int:
        """Count each referenced container once within this row."""
        identity = id(item)
        if identity in seen_ids:
            return 0
        seen_ids.add(identity)
        size = sys.getsizeof(item)
        if isinstance(item, dict):
            size += sum(visit(key) + visit(child) for key, child in item.items())
        elif isinstance(item, (list, tuple)):
            size += sum(visit(child) for child in item)
        return size

    return visit(value)


def _row_memory_bytes(payload: str, encoded_extra: int, heap_extra: int) -> int:
    """Account for the decoded row that the sink will receive, including repeated strings."""
    return len(payload.encode("utf-8")) + encoded_extra + heap_extra + _heap_size(json.loads(payload)) + 8


class _Capture:
    def __init__(self, prefix: str, limit: int):
        self.prefix = prefix
        self.limit = limit
        self.builder = ObjectBuilder()
        self.depth = 0
        self.allocated = 0

    def is_complete_after(self, event: str, value: Any) -> bool:
        """Consume one event while bounding the captured object's allocations."""
        if isinstance(value, Decimal):
            value = float(value)
        self.allocated += sys.getsizeof(value) + (72 if event == "map_key" else 8)
        if event in {"start_map", "start_array"}:
            self.depth += 1
            self.allocated += sys.getsizeof({} if event == "start_map" else [])
        elif event in {"end_map", "end_array"}:
            self.depth -= 1
        if self.allocated > self.limit:
            raise IncompleteToc("TOC object exceeds the row memory budget")
        self.builder.event(event, value)
        return self.depth == 0

    def captured_value(self) -> Any:
        """Return one fully captured value within its encoded byte budget."""
        value = self.builder.value
        if len(_json(value).encode("utf-8")) > self.limit:
            raise IncompleteToc("TOC object exceeds the row byte budget")
        return value


async def _decoded_file(
    reader: Any,
    directory: Path,
    max_bytes: int,
    charset: str | None,
    storage: _StorageBudget,
    *,
    decoded_budget: DecodedTocBudget | None = None,
) -> Path:
    raw = directory / "response"
    total = 0
    with raw.open("wb") as output:
        while chunk := await reader.read(_CHUNK_BYTES):
            total += len(chunk)
            if total > max_bytes:
                raise IncompleteToc("TOC response exceeds the discovery byte limit")
            storage.claim(len(chunk))
            output.write(chunk)
    with raw.open("rb") as input_stream:
        is_compressed = input_stream.read(2) == b"\x1f\x8b"
    decoded = directory / "document.json"
    decoder = codecs.getincrementaldecoder(charset)(errors="replace") if charset is not None else None
    total = 0
    try:
        with gzip.open(raw, "rb") if is_compressed else raw.open("rb") as input_stream, decoded.open("wb") as output:
            while chunk := input_stream.read(_CHUNK_BYTES):
                total += len(chunk)
                if total > max_bytes:
                    raise IncompleteToc("decoded TOC exceeds the discovery byte limit")
                if decoded_budget is not None:
                    decoded_budget.claim(len(chunk))
                encoded = decoder.decode(chunk).encode("utf-8") if decoder is not None else chunk
                storage.claim(len(encoded))
                output.write(encoded)
                await asyncio.sleep(0)
            encoded = decoder.decode(b"", final=True).encode("utf-8") if decoder is not None else b""
            storage.claim(len(encoded))
            output.write(encoded)
    except (EOFError, OSError, zlib.error) as exc:
        raise IncompleteToc("invalid or truncated compressed TOC") from exc
    storage.release(raw.stat().st_size)
    raw.unlink()
    return decoded


def _repair_tic_file(source_path: Path, destination: Path, storage: _StorageBudget) -> int:
    markers = {b'"reporting_structure"', b'"reporting_plans"'}
    file_markers = {b'"in_network_files"', b'"allowed_amount_file"', b'"allowed_amount_files"'}
    found_markers: set[bytes] = set()
    overlap = b""
    with source_path.open("rb") as stream:
        while chunk := stream.read(_CHUNK_BYTES):
            window = overlap + chunk.lower()
            found_markers.update(marker for marker in markers | file_markers if marker in window)
            overlap = window[-64:]
    if not markers <= found_markers or not found_markers & file_markers:
        return 0
    state = _CommaRepairState()
    with source_path.open("rb") as stream, destination.open("wb") as output:
        while chunk := stream.read(_CHUNK_BYTES):
            repaired = state.repair_chunk(chunk)
            storage.claim(len(repaired))
            output.write(repaired)
    return state.repairs


class _CommaRepairState:
    def __init__(self):
        self.is_quoted = self.is_escaped = False
        self.previous = None
        self.repairs = 0

    def repair_chunk(self, chunk: bytes) -> bytes:
        """Insert only adjacent-object commas outside JSON strings."""
        repaired = bytearray()
        for character in chunk:
            if not self.is_quoted and character == 123 and self.previous == 125:
                repaired.append(44)
                self.repairs += 1
            repaired.append(character)
            self._track_quotation(character)
            if not self.is_quoted and character not in b" \t\r\n":
                self.previous = character
        return bytes(repaired)

    def _track_quotation(self, character: int) -> None:
        if self.is_escaped:
            self.is_escaped = False
        elif self.is_quoted and character == 92:
            self.is_escaped = True
        elif character == 34:
            self.is_quoted = not self.is_quoted


class TocSpool:
    """Retain plans and file occurrences separately until valid document EOF."""

    def __init__(self, directory: Path, url: str, row_bytes: int, storage: _StorageBudget, plan_predicate=None):
        self.directory = directory
        self.url = url
        self.row_bytes = max(1, row_bytes)
        self.plan_predicate = plan_predicate
        self.storage = storage
        self.connection = sqlite3.connect(directory / "catalog.sqlite")
        self.storage.connection = self.connection
        try:
            self.storage.limit_database()
            self._initialize_tables()
        except BaseException:
            self.connection.close()
            raise
        self.metadata: dict[str, Any] = {}
        self.arrays: set[str] = set()
        self.root_keys: list[str] = []
        self.structure_count = 0
        self._ordinal = 0
        self._entry_ordinal = 0
        self._row_ordinal = 0
        self.structure_error = None
        self._document_ordinal = 0
        self._entry_key = self._entry_identity = None
        self._raw_plan_key = self._normalized_plan_key = self._stored_plan_key = None
        self._raw_plans = self._normalized_plans = ()
        self._stored_plan_sizes = (0, 0)

    def _initialize_tables(self) -> None:
        self.connection.executescript("""
            PRAGMA auto_vacuum=FULL;
            PRAGMA cache_size=-1024;
            PRAGMA temp_store=FILE;
            CREATE TABLE plans (group_id TEXT, field TEXT, ordinal INTEGER, identity TEXT, payload TEXT);
            CREATE INDEX plans_group ON plans(group_id,field,ordinal);
            CREATE INDEX plans_identity ON plans(group_id,field,identity,ordinal);
            CREATE TABLE blocks (ordinal INTEGER PRIMARY KEY, kind TEXT, section TEXT, group_id TEXT,
                                 scope INTEGER, key_order INTEGER, payload TEXT);
            CREATE INDEX blocks_order ON blocks((CASE kind WHEN 'metadata' THEN 0 WHEN 'flat' THEN 1 ELSE 2 END),scope,key_order,ordinal);
            CREATE INDEX blocks_metadata ON blocks(kind,section,ordinal);
            CREATE INDEX blocks_group ON blocks(group_id);
            CREATE TABLE entries (identity TEXT PRIMARY KEY, ordinal INTEGER, payload TEXT);
            CREATE INDEX entries_order ON entries(ordinal);
            CREATE TABLE plan_groups (group_id TEXT PRIMARY KEY);
            CREATE TABLE group_plans (group_id TEXT, plan_identity TEXT, ordinal INTEGER, payload TEXT,
                                      PRIMARY KEY(plan_identity,group_id));
            CREATE INDEX group_plans_order ON group_plans(group_id,ordinal);
            CREATE TABLE entry_groups (identity TEXT, group_id TEXT, ordinal INTEGER, PRIMARY KEY(identity,group_id));
            CREATE INDEX entry_groups_order ON entry_groups(identity,ordinal);
            CREATE TABLE entry_sets (identity TEXT PRIMARY KEY, set_id TEXT, ordinal INTEGER);
            CREATE INDEX entry_sets_order ON entry_sets(set_id,ordinal);
            CREATE TABLE plan_sets (set_id TEXT PRIMARY KEY,payload TEXT,encoded_bytes INTEGER,heap_bytes INTEGER);
            CREATE TABLE rows (kind TEXT, identity TEXT, ordinal INTEGER, payload TEXT, date_fields TEXT,plan_set TEXT,
                               PRIMARY KEY(kind,identity));
            CREATE INDEX rows_order ON rows(kind DESC,ordinal);
        """)

    @property
    def is_healthsparq(self) -> bool:
        """Return whether the original root-files adapter owns this layout."""
        return "files" in self.arrays

    def close(self) -> None:
        """Close local SQLite handles before removing their temporary directory."""
        self.connection.close()

    def _plans(self, group_id: str, field: str) -> list[dict[str, Any]]:
        query = "SELECT payload FROM plans p WHERE group_id=? AND field=?"
        rows = self._ordered(query + " ORDER BY ordinal", (group_id, field))
        values = []
        allocated = 0
        for (payload,) in rows:
            value = json.loads(payload)
            allocated += _heap_size(value) + len(payload.encode("utf-8"))
            if allocated + sys.getsizeof(values) > self.row_bytes:
                raise IncompleteToc("TOC file plan metadata exceeds the row budget")
            values.append(value)
        return values

    def _ordered(self, query: str, parameters: tuple = ()):
        """Reject unaccounted temporary sort/index files before running a query."""
        plan = self.connection.execute("EXPLAIN QUERY PLAN " + query, parameters)
        if any("TEMP B-TREE" in step[3] or "AUTOMATIC" in step[3] for step in plan):
            raise IncompleteToc("TOC query requires unsupported temporary storage")
        return self.connection.execute(query, parameters)

    def _metadata_path(self) -> str | None:
        return next((path for path in _METADATA_LISTS if path in self.arrays), None)

    async def read_document(
        self,
        reader: Any,
        *,
        max_bytes: int,
        charset: str = "utf-8",
        decoded_budget: DecodedTocBudget | None = None,
    ) -> None:
        """Load another complete document while retaining the target's final rows."""
        for table in ("plans", "blocks", "entries", "entry_groups", "group_plans", "plan_groups", "entry_sets"):
            self.connection.execute(f"DELETE FROM {table}")
        self.connection.commit()
        self.metadata.clear()
        self.arrays.clear()
        self.root_keys.clear()
        self.structure_count = self._ordinal = self._entry_ordinal = 0
        self._document_ordinal += 1
        self._entry_key = self._entry_identity = None
        self._raw_plan_key = self._normalized_plan_key = self._stored_plan_key = None
        self._raw_plans = self._normalized_plans = ()
        self.structure_error = None
        path = await _decoded_file(
            reader, self.directory, max(1, max_bytes), charset, self.storage, decoded_budget=decoded_budget
        )
        await self.parse(path)

    async def save_archive(self, reader: Any, *, max_bytes: int) -> Path:
        """Store an admitted ZIP without materializing the transport body."""
        decoded = await _decoded_file(reader, self.directory, max_bytes, None, self.storage)
        path = self.directory / "container.zip"
        decoded.rename(path)
        return path

    def _discard_blocks(self, where: str, parameters: tuple) -> None:
        self.connection.execute(
            f"DELETE FROM plans WHERE EXISTS (SELECT 1 FROM blocks WHERE blocks.group_id=plans.group_id AND kind='metadata' AND ({where}))",
            parameters,
        )
        self.connection.execute(f"DELETE FROM blocks WHERE {where}", parameters)

    async def parse(self, path: Path) -> None:
        """Validate EOF, applying only the existing narrow TiC repair if needed."""
        self.storage.limit_database(repair_reserve=2 * path.stat().st_size)
        self.connection.execute("BEGIN")
        try:
            await self._parse_document_events(path)
        except ijson.JSONError as exc:
            self.connection.rollback()
            self.metadata.clear()
            self.arrays.clear()
            self.root_keys.clear()
            self.structure_count = self._ordinal = 0
            self.structure_error = None
            repaired = self.directory / "repaired.json"
            if not _repair_tic_file(path, repaired, self.storage):
                raise IncompleteToc("invalid or incomplete TOC JSON") from exc
            self.connection.execute("BEGIN")
            try:
                await self._parse_document_events(repaired)
            except ijson.JSONError as repaired_error:
                raise IncompleteToc("invalid or incomplete TOC JSON") from repaired_error
        if not self.is_healthsparq and self.structure_error is not None:
            raise IncompleteToc(self.structure_error[1])
        self.connection.commit()
        for document in (path, self.directory / "repaired.json"):
            if document.exists():
                self.storage.release(document.stat().st_size)
                document.unlink()
        self.storage.limit_database()

    async def _parse_document_events(self, path: Path) -> None:
        """Read all events before admitting any normalized output rows."""
        parser = _TocEventParser(self)
        depth = event_count = 0
        with path.open("rb") as stream:
            for event_count, (prefix, event, json_value) in enumerate(ijson.parse(stream)):
                if event_count % 512 == 0:
                    await asyncio.sleep(0)
                if event_count == 0 and (prefix, event) != ("", "start_map"):
                    raise IncompleteToc("expected TOC JSON object")
                depth += event in {"start_map", "start_array"}
                depth -= event in {"end_map", "end_array"}
                if depth > min(1000, sys.getrecursionlimit() - 50):
                    raise IncompleteToc("TOC nesting exceeds the JSON depth budget")
                parser.consume(prefix, event, json_value)
        if not event_count:
            raise IncompleteToc("empty TOC JSON")

    async def healthsparq_files(self) -> AsyncIterator[dict[str, Any]]:
        """Yield bounded root-files fragments with the selected reporting-plan list."""
        for payload, group_id in self._ordered(
            "SELECT payload,group_id FROM blocks WHERE kind='metadata' AND section='files' ORDER BY ordinal"
        ):
            file = json.loads(payload)
            field = next((key for key in _PLAN_KEYS if file.get(key) not in (None, "")), None)
            if field is not None and isinstance(file[field], list):
                file[field] = self._plans(group_id, field)
            yield file
            await asyncio.sleep(0)

    def _add_entry(self, entry: PTG2SourceCatalogEntry, plan_group: str | None = None) -> None:
        identity = _json((entry.source_type, entry.domain, entry.canonical_url))
        payload = {field.name: getattr(entry, field.name) for field in fields(entry) if field.name != "plan_info"}
        entry_json = _json(payload)
        if len(entry_json.encode("utf-8")) > self.row_bytes or _heap_size(payload) > self.row_bytes:
            raise IncompleteToc("TOC file metadata exceeds the row budget")
        self._entry_ordinal += 1
        self.connection.execute(
            "INSERT INTO entries VALUES (?,?,?) ON CONFLICT(identity) DO UPDATE SET payload=excluded.payload",
            (identity, self._entry_ordinal, entry_json),
        )
        if plan_group is None and entry.plan_info:
            plan_group = f"e:{self._entry_ordinal}"
            self.connection.execute("INSERT INTO plan_groups VALUES (?)", (plan_group,))
            self._store_group_plans(plan_group, entry.plan_info)
        if plan_group is not None:
            self.connection.execute(
                "INSERT OR IGNORE INTO entry_groups VALUES (?,?,?)", (identity, plan_group, self._entry_ordinal)
            )

    def _store_group_plans(self, group_id: str, plans) -> None:
        for ordinal, plan in enumerate(plans):
            self.connection.execute(
                "INSERT OR IGNORE INTO group_plans VALUES (?,?,?,?)",
                (group_id, canonical_json_dumps(plan), ordinal, _json(plan)),
            )

    def _is_structure_group_ready(self, group_id: str) -> bool:
        if not self.connection.execute("SELECT 1 FROM plan_groups WHERE group_id=?", (group_id,)).fetchone():
            self.connection.execute("INSERT INTO plan_groups VALUES (?)", (group_id,))
            if self.plan_predicate is None:
                self.connection.execute(
                    "INSERT OR IGNORE INTO group_plans SELECT group_id,identity,ordinal,payload FROM plans "
                    "WHERE group_id=? AND field='reporting_plans' ORDER BY ordinal",
                    (group_id,),
                )
            else:
                plans = (
                    json.loads(payload)
                    for (payload,) in self._ordered(
                        "SELECT payload FROM plans WHERE group_id=? AND field='reporting_plans' ORDER BY ordinal",
                        (group_id,),
                    )
                )
                self._store_group_plans(group_id, (plan for plan in plans if self.plan_predicate(plan)))
        return (
            self.connection.execute("SELECT 1 FROM group_plans WHERE group_id=? LIMIT 1", (group_id,)).fetchone()
            is not None
        )

    async def catalog_entries(self) -> AsyncIterator[PTG2SourceCatalogEntry]:
        """Match ordinary parsing's global order, final fields and distinct plans."""
        if self.is_healthsparq:
            raise ValueError("root-files TOC requires its HealthSparq row adapter")
        if not self.structure_count and any(key in self.root_keys for key in ("provider_urls", "plan_urls")):
            return
        for entry in parse_toc_catalog_entries(self.metadata, self.url):
            self._add_entry(entry)
        await self._build_catalog_entries()
        self._prepare_entry_sets()
        self.connection.execute("DELETE FROM plans")
        self.connection.execute("DELETE FROM blocks")
        self.connection.commit()
        async for entry in self._merged_catalog_entries():
            yield entry

    async def _build_catalog_entries(self) -> None:
        block_query = """SELECT kind,section,group_id,payload FROM blocks
                         WHERE kind<>'metadata' OR section=?
                         ORDER BY CASE kind WHEN 'metadata' THEN 0 WHEN 'flat' THEN 1 ELSE 2 END,
                                  scope,key_order,ordinal"""
        for kind, section, group_id, file_json in self._ordered(block_query, (self._metadata_path(),)):
            if kind == "structure" and not self._is_structure_group_ready(group_id):
                continue
            fragment_dict = self._block_fragment(kind, section, group_id, file_json)
            predicate = None if kind == "structure" else self.plan_predicate
            for entry in parse_toc_catalog_entries(fragment_dict, self.url, plan_predicate=predicate):
                self._add_entry(
                    entry, group_id if kind == "structure" and entry.source_type != "table-of-contents" else None
                )
            await asyncio.sleep(0)

    def _block_fragment(self, kind: str, section: str, group_id: str, file_json: str) -> dict:
        file_dict = json.loads(file_json)
        fragment_dict = dict(self.metadata)
        if kind == "structure":
            fragment_dict["reporting_structure"] = [{"reporting_plans": [{}], section: [file_dict]}]
        elif kind == "flat":
            fragment_dict[section] = [file_dict]
        else:
            field = next((key for key in _PLAN_KEYS if file_dict.get(key) not in (None, "")), None)
            if field is not None and isinstance(file_dict[field], list):
                file_dict[field] = self._plans(group_id, field)
            fragment_dict["items"] = [file_dict]
        return fragment_dict

    def _prepare_entry_sets(self) -> None:
        for identity, ordinal in self._ordered("SELECT identity,ordinal FROM entries ORDER BY ordinal"):
            groups = []
            reference_bytes = len(f"{self._document_ordinal}:[]".encode()) + sys.getsizeof("")
            if reference_bytes + sys.getsizeof(groups) > self.row_bytes:
                raise IncompleteToc("TOC plan group references exceed the row budget")
            for (group,) in self._ordered(
                "SELECT group_id FROM entry_groups WHERE identity=? ORDER BY ordinal", (identity,)
            ):
                reference_bytes += sys.getsizeof(group) + len(_json(group).encode()) + bool(groups)
                groups.append(group)
                if reference_bytes + sys.getsizeof(groups) > self.row_bytes:
                    raise IncompleteToc("TOC plan group references exceed the row budget")
            key = f"{self._document_ordinal}:" + _json(groups)
            self.connection.execute("INSERT INTO entry_sets VALUES (?,?,?)", (identity, key, ordinal))

    def _merged_plans(self, identity: str):
        plans, allocated = [], 0
        for group_id, ordinal in self._ordered(
            "SELECT group_id,ordinal FROM entry_groups WHERE identity=? ORDER BY ordinal", (identity,)
        ):
            query = """SELECT p.payload FROM group_plans p WHERE p.group_id=? AND NOT EXISTS(
                SELECT 1 FROM group_plans seen CROSS JOIN entry_groups e
                WHERE seen.plan_identity=p.plan_identity AND e.group_id=seen.group_id
                AND e.identity=? AND e.ordinal<?) ORDER BY p.ordinal"""
            for (plan_json,) in self._ordered(query, (group_id, identity, ordinal)):
                plan = json.loads(plan_json)
                allocated += _heap_size(plan) + len(plan_json.encode())
                if allocated + sys.getsizeof(plans) > self.row_bytes:
                    raise IncompleteToc("merged TOC file plan metadata exceeds the row budget")
                plans.append(plan)
        return tuple(plans)

    async def _merged_catalog_entries(self) -> AsyncIterator[PTG2SourceCatalogEntry]:
        for identity, entry_json in self._ordered("SELECT identity,payload FROM entries ORDER BY ordinal"):
            self._entry_identity = identity
            self._entry_key = self.connection.execute(
                "SELECT set_id FROM entry_sets WHERE identity=?", (identity,)
            ).fetchone()[0]
            if self._entry_key != self._raw_plan_key:
                self._raw_plans = self._merged_plans(identity)
                self._raw_plan_key = self._entry_key
            yield PTG2SourceCatalogEntry(**json.loads(entry_json), plan_info=self._raw_plans)
            await asyncio.sleep(0)

    def normalized_entry_plans(self, entry, normalize):
        """Reuse one bounded normalized union for files sharing their plan groups."""
        if self._normalized_plan_key != self._entry_key:
            self._normalized_plans = normalize(entry.plan_info)
            self._normalized_plan_key = self._entry_key
        return self._normalized_plans

    def is_plan_rows_write_needed(self) -> bool:
        """Keep the first row's order and the last row's values for a shared union."""
        ordinal = self.connection.execute(
            "SELECT ordinal FROM entry_sets WHERE identity=?", (self._entry_identity,)
        ).fetchone()[0]
        return any(
            not self.connection.execute(
                f"SELECT 1 FROM entry_sets WHERE set_id=? AND ordinal{comparison}? LIMIT 1", (self._entry_key, ordinal)
            ).fetchone()
            for comparison in ("<", ">")
        )

    def add_rows(self, plan_rows: list[dict[str, Any]], file_rows: list[dict[str, Any]]) -> None:
        """Retain first identity order and final row value without an unbounded dict."""
        for kind, rows, key in (("plan", plan_rows, "mrf_plan_id"), ("file", file_rows, "mrf_file_id")):
            for row in rows:
                payload, dates, plan_set = self._pack_row(kind, row)
                self._row_ordinal += 1
                self.connection.execute(
                    "INSERT INTO rows VALUES (?,?,?,?,?,?) ON CONFLICT(kind,identity) DO UPDATE SET payload=excluded.payload,date_fields=excluded.date_fields,plan_set=excluded.plan_set",
                    (kind, str(row[key]), self._row_ordinal, payload, _json(dates), plan_set),
                )

    def _pack_row(self, kind, row):
        dates = [key for key, value in row.items() if isinstance(value, dt.datetime)]
        stored_by_field = {key: value.isoformat() if key in dates else value for key, value in row.items()}
        plan_set = None
        encoded_extra = heap_extra = 0
        metadata = row.get("metadata_json")
        if kind == "file" and isinstance(metadata, dict) and metadata.get("plan_info") is self._normalized_plans:
            plan_set = self._store_plan_set(row)
            stored_by_field = {
                key: value
                for key, value in stored_by_field.items()
                if key not in {"plan_ids", "plan_names", "market_types"}
            }
            stored_by_field["metadata_json"] = {key: value for key, value in metadata.items() if key != "plan_info"}
            encoded_extra, heap_extra = self._stored_plan_sizes
        payload = _json(stored_by_field)
        if _row_memory_bytes(payload, encoded_extra, heap_extra) > self.row_bytes:
            raise IncompleteToc("normalized TOC row exceeds the row budget")
        return payload, dates, plan_set

    def _store_plan_set(self, row):
        if self._stored_plan_key != self._entry_key:
            retained = self.connection.execute(
                "SELECT encoded_bytes,heap_bytes FROM plan_sets WHERE set_id=?", (self._entry_key,)
            ).fetchone()
            if retained is not None:
                self._stored_plan_key = self._entry_key
                self._stored_plan_sizes = retained
                return self._stored_plan_key
            fields_by_name = {name: row[name] for name in ("plan_ids", "plan_names", "market_types")}
            fields_by_name["plan_info"] = row["metadata_json"]["plan_info"]
            payload = _json(fields_by_name)
            self._stored_plan_sizes = len(payload.encode("utf-8")), _heap_size(json.loads(payload))
            if max(self._stored_plan_sizes) > self.row_bytes:
                raise IncompleteToc("normalized TOC plan fields exceed the row budget")
            self.connection.execute(
                "INSERT OR IGNORE INTO plan_sets VALUES (?,?,?,?)",
                (self._entry_key, payload, *self._stored_plan_sizes),
            )
            self._stored_plan_key = self._entry_key
        return self._stored_plan_key

    def _unpack_row(self, payload, plan_set, cache):
        row = json.loads(payload)
        encoded_extra = heap_extra = 0
        if plan_set is not None:
            if cache.get("key") != plan_set:
                packed, encoded, heap = self.connection.execute(
                    "SELECT payload,encoded_bytes,heap_bytes FROM plan_sets WHERE set_id=?", (plan_set,)
                ).fetchone()
                cache.update(key=plan_set, fields=json.loads(packed), encoded=encoded, heap=heap)
            fields_by_name = cache["fields"]
            for name in ("plan_ids", "plan_names", "market_types"):
                row[name] = fields_by_name[name]
            row["metadata_json"]["plan_info"] = fields_by_name["plan_info"]
            encoded_extra, heap_extra = cache["encoded"], cache["heap"]
        return row, encoded_extra, heap_extra

    async def drain_rows(
        self, sink: Callable[[list, list], Awaitable[None]], *, batch_size: int, batch_bytes: int
    ) -> tuple[int, int]:
        """Await every bounded write; return unique durable row counts only."""
        if batch_size < 1 or batch_bytes < 1:
            raise IncompleteToc("TOC write budgets must be positive")
        plans, files = [], []
        plan_cache_by_field = {}
        pending_bytes = plan_count = file_count = 0
        for kind, payload, dates, plan_set in self._ordered(
            "SELECT kind,payload,date_fields,plan_set FROM rows ORDER BY kind DESC,ordinal"
        ):
            row, encoded_extra, heap_extra = self._unpack_row(payload, plan_set, plan_cache_by_field)
            for field in json.loads(dates):
                row[field] = dt.datetime.fromisoformat(row[field])
            size = _row_memory_bytes(payload, encoded_extra, heap_extra)
            if size > batch_bytes:
                raise IncompleteToc("normalized TOC row exceeds the write memory budget")
            if pending_bytes + size > batch_bytes or len(plans) + len(files) >= batch_size:
                await sink(plans, files)
                plans, files, pending_bytes = [], [], 0
            (plans if kind == "plan" else files).append(row)
            plan_count += kind == "plan"
            file_count += kind == "file"
            pending_bytes += size
        if plans or files:
            await sink(plans, files)
        return plan_count, file_count


@dataclass(frozen=True)
class _FileBlock:
    kind: str
    section: str
    group_id: str
    scope: int
    key_order: int
    ordinal: int


class _TocEventParser:
    """Route bounded individual values, never whole structures, into SQLite."""

    def __init__(self, spool: TocSpool):
        self.spool = spool
        self.capture = self.plan_capture = self.metadata_capture = None
        self.file_block = None
        self.plan_field = None
        self.is_plan_array_open = False
        self.plan_context = None
        self.structure = -1
        self.plan_ordinal = 0
        self.root_key_bytes = 0
        self.root_field = self.data_field = self.structure_field = None

    def consume(self, prefix: str, event: str, json_value: Any) -> None:
        """Store one token without retaining an enclosing structure or document."""
        if self.metadata_capture is not None:
            if self.metadata_capture.is_complete_after(event, json_value):
                self.spool.metadata[self.metadata_capture.prefix] = self.metadata_capture.captured_value()
                self.metadata_capture = None
        elif self.capture is not None:
            self._consume_file_event(prefix, event, json_value)
        elif self.plan_capture is not None:
            self._consume_plan_event(event, json_value)
        else:
            self._consume_root_event(prefix, event, json_value)

    def _consume_plan_event(self, event: str, json_value: Any) -> None:
        if not self.plan_capture.is_complete_after(event, json_value):
            return
        group_id, field, is_structure_plan = self.plan_context
        plan_dict = self.plan_capture.captured_value()
        if is_structure_plan:
            plan_dict = _normalize_plan_payload(plan_dict)
        self.plan_ordinal += 1
        self.spool.connection.execute(
            "INSERT INTO plans VALUES (?,?,?,?,?)",
            (group_id, field, self.plan_ordinal, canonical_json_dumps(plan_dict), _json(plan_dict)),
        )
        self.plan_capture = self.plan_context = None

    def _begin_plan(self, prefix: str, event: str, json_value: Any, context: tuple) -> None:
        self.plan_capture = _Capture(prefix, self.spool.row_bytes)
        self.plan_capture.is_complete_after(event, json_value)
        self.plan_context = context

    def _is_plan_array_event(self, prefix: str, event: str, json_value: Any) -> bool:
        if self.plan_field is None:
            return False
        array_prefix = self.capture.prefix + "." + self.plan_field
        if not prefix.startswith(array_prefix):
            return False
        if prefix == array_prefix and event == "start_array":
            self.capture.is_complete_after("start_array", None)
            self.capture.is_complete_after("end_array", None)
            self.is_plan_array_open = True
            return True
        if not self.is_plan_array_open:
            self.plan_field = None
            return False
        if self.plan_capture is None and prefix == array_prefix + ".item" and event == "start_map":
            self._begin_plan(prefix, event, json_value, (self.file_block.group_id, self.plan_field, False))
        elif self.plan_capture is not None:
            self._consume_plan_event(event, json_value)
        if prefix == array_prefix and event == "end_array":
            self.plan_field = None
            self.is_plan_array_open = False
        return True

    def _consume_file_event(self, prefix: str, event: str, json_value: Any) -> None:
        block = self.file_block
        if (
            block.kind == "metadata"
            and event == "map_key"
            and prefix == self.capture.prefix
            and json_value in _PLAN_KEYS
        ):
            self.plan_field = str(json_value)
            self.spool.connection.execute(
                "DELETE FROM plans WHERE group_id=? AND field=?", (block.group_id, self.plan_field)
            )
        if self._is_plan_array_event(prefix, event, json_value):
            return
        if self.capture.is_complete_after(event, json_value):
            self.spool.connection.execute(
                "INSERT INTO blocks VALUES (?,?,?,?,?,?,?)",
                (
                    block.ordinal,
                    block.kind,
                    block.section,
                    block.group_id,
                    block.scope,
                    block.key_order,
                    _json(self.capture.captured_value()),
                ),
            )
            self.capture = self.file_block = None

    def _record_root_key(self, key: str) -> None:
        known_keys = {
            *_METADATA_KEYS,
            "provider_urls",
            "plan_urls",
            "files",
            "items",
            "results",
            "data",
            "reporting_structure",
        }
        if key not in known_keys and _flat_toc_section_type(key) is None:
            return
        if key not in self.spool.root_keys:
            self.root_key_bytes += sys.getsizeof(key) + len(key.encode("utf-8"))
            if self.root_key_bytes > self.spool.row_bytes:
                raise IncompleteToc("TOC section metadata exceeds the row budget")
            self.spool.root_keys.append(key)
            return
        if key == "reporting_structure":
            self.spool._discard_blocks("kind='structure'", ())
            self.spool.connection.execute("DELETE FROM plans WHERE group_id LIKE 's:%'")
            self.structure = -1
            self.spool.structure_count = 0
            self.spool.structure_error = None
        else:
            self.spool._discard_blocks("section=? OR substr(section,1,?)=?", (key, len(key) + 1, key + "."))
        self.spool.arrays.difference_update(
            path for path in tuple(self.spool.arrays) if path == key or path.startswith(key + ".")
        )

    def _select_file_block(self, prefix: str) -> _FileBlock | None:
        ordinal = self.spool._ordinal + 1
        for key_order, section in enumerate(_FILE_KEYS):
            if self.structure_field == section and prefix in {
                f"reporting_structure.item.{section}",
                f"reporting_structure.item.{section}.item",
            }:
                return _FileBlock("structure", section, f"s:{self.structure}", self.structure, key_order, ordinal)
        for scope, metadata_path in enumerate(_METADATA_LISTS):
            if (
                self._is_metadata_array_path(metadata_path)
                and metadata_path in self.spool.arrays
                and prefix == metadata_path + ".item"
            ):
                return _FileBlock("metadata", metadata_path, f"f:{ordinal}", scope, 0, ordinal)
        for scope, section in enumerate(self.spool.root_keys):
            if (
                self.root_field == section
                and prefix == section + ".item"
                and _flat_toc_section_type(section) is not None
            ):
                return _FileBlock("flat", section, "", scope, 0, ordinal)
        return None

    def _is_root_metadata_event(self, prefix: str, event: str, json_value: Any) -> bool:
        if prefix not in _METADATA_KEYS or event in {"map_key", "end_map", "end_array"}:
            return False
        if event in {"start_map", "start_array"}:
            self.metadata_capture = _Capture(prefix, self.spool.row_bytes)
            self.metadata_capture.is_complete_after(event, json_value)
        else:
            self.spool.metadata[prefix] = float(json_value) if isinstance(json_value, Decimal) else json_value
        return True

    def _replace_structure_field(self, field: str) -> None:
        if field == "reporting_plans":
            if self.spool.structure_error is not None and self.spool.structure_error[0] == f"plans:{self.structure}":
                self.spool.structure_error = None
            self.spool.connection.execute("DELETE FROM plans WHERE group_id=?", (f"s:{self.structure}",))
        elif field in _FILE_KEYS:
            self.spool._discard_blocks("kind='structure' AND group_id=? AND section=?", (f"s:{self.structure}", field))

    def _validate_structure_event(self, prefix: str, event: str, json_value: Any) -> None:
        if self.spool.structure_error is not None:
            return
        if prefix in {"reporting_structure", "reporting_structure.item.reporting_plans"}:
            if event == "map_key" or (event in {"string", "number", "boolean"} and json_value):
                context = "root" if prefix == "reporting_structure" else f"plans:{self.structure}"
                self.spool.structure_error = (context, "unsupported TOC structure or plan layout")
        if prefix == "reporting_structure.item" and event in {"string", "number", "boolean", "null", "start_array"}:
            self.spool.structure_error = ("root", "unsupported reporting structure")
        if (
            self.structure_field == "reporting_plans"
            and prefix == "reporting_structure.item.reporting_plans.item"
            and event
            not in {
                "start_map",
                "end_map",
                "map_key",
            }
        ):
            self.spool.structure_error = (f"plans:{self.structure}", "reporting plan must be an object")

    def _consume_root_event(self, prefix: str, event: str, json_value: Any) -> None:
        if prefix == "" and event == "map_key":
            self.root_field = str(json_value)
            self.data_field = self.structure_field = None
            self._record_root_key(str(json_value))
        if self.root_field is not None and self.root_field not in self.spool.root_keys:
            return
        if prefix == "data" and event == "map_key":
            self.data_field = str(json_value)
        if prefix == "data" and event == "map_key" and json_value in {"files", "items", "results"}:
            self.spool._discard_blocks("kind='metadata' AND section=?", ("data." + str(json_value),))
            self.spool.arrays.discard("data." + str(json_value))
        if prefix in _METADATA_LISTS and event == "start_array" and self._is_metadata_array_path(prefix):
            self.spool.arrays.add(prefix)
        if self._is_root_metadata_event(prefix, event, json_value):
            return
        self._validate_structure_event(prefix, event, json_value)
        if prefix == "reporting_structure.item" and event == "map_key":
            self.structure_field = str(json_value)
            self._replace_structure_field(str(json_value))
        if prefix == "reporting_structure.item" and event == "start_map":
            self.structure_field = None
            self.structure += 1
            self.spool.structure_count += 1
        elif (
            self.structure_field == "reporting_plans"
            and prefix == "reporting_structure.item.reporting_plans.item"
            and event == "start_map"
        ):
            self._begin_plan(prefix, event, json_value, (f"s:{self.structure}", "reporting_plans", True))
        elif event == "start_map":
            self.file_block = self._select_file_block(prefix)
            if self.file_block is not None:
                self.spool._ordinal = self.file_block.ordinal
                self.capture = _Capture(prefix, self.spool.row_bytes)
                self.capture.is_complete_after(event, json_value)

    def _is_metadata_array_path(self, prefix: str) -> bool:
        if self.root_field in {"files", "items", "results"}:
            return prefix == self.root_field
        if self.root_field != "data":
            return False
        if self.data_field is None:
            return prefix == "data"
        return self.data_field in {"files", "items", "results"} and prefix == "data." + self.data_field


@contextmanager
def _owned_directory(parent):
    """Treat scratch creation and cleanup failures as incomplete source work."""
    try:
        with tempfile.TemporaryDirectory(prefix="mrf-toc-", dir=parent) as directory:
            yield directory
    except OSError as exc:
        raise IncompleteToc("TOC scratch storage is unavailable") from exc


@asynccontextmanager
async def open_toc_store(
    url: str,
    *,
    row_bytes: int,
    spool_bytes: int,
    plan_predicate=None,
    temporary_parent=None,
):
    """Own all temporary files before receiving or parsing untrusted TOC bytes."""
    with _owned_directory(temporary_parent) as temporary:
        try:
            spool = TocSpool(Path(temporary), url, row_bytes, _StorageBudget(spool_bytes), plan_predicate)
        except Exception as exc:
            raise IncompleteToc("TOC spool could not be initialized") from exc
        try:
            yield spool
        except IncompleteToc:
            raise
        except sqlite3.DatabaseError as exc:
            message = (
                "TOC spool byte budget exhausted"
                if getattr(exc, "sqlite_errorcode", None) == sqlite3.SQLITE_FULL
                else "TOC spool database did not complete"
            )
            raise IncompleteToc(message) from exc
        except Exception as exc:
            raise IncompleteToc("TOC processing did not complete") from exc
        finally:
            spool.close()


@asynccontextmanager
async def open_toc_spool(
    reader: Any,
    url: str,
    *,
    max_bytes: int,
    row_bytes: int,
    spool_bytes: int,
    charset: str = "utf-8",
    plan_predicate=None,
    temporary_parent=None,
):
    """Validate one bounded document and own its complete temporary lifetime."""
    async with open_toc_store(
        url,
        row_bytes=row_bytes,
        spool_bytes=spool_bytes,
        plan_predicate=plan_predicate,
        temporary_parent=temporary_parent,
    ) as spool:
        await spool.read_document(reader, max_bytes=max_bytes, charset=charset)
        yield spool
