# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native local proof of an exact address copy with physical column renumbering."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, replace

from sqlalchemy import text
from sqlalchemy.dialects import postgresql

from process import entity_address_snapshot_receipt as receipt
from process.entity_address_snapshot_ownership import verify_entity_address_archive_stage_ownership

_BRAND = object()
_EVIDENCE = "entity_address_evidence"
_VERSION = "registry_cms_address_copy.v1"


class RegistryCMSAddressEquivalenceError(ValueError):
    """A captured address family or its native copy differs."""


@dataclass(frozen=True)
class RegistryCMSAddressCopySource:
    """In-process token minted only from the caller's authenticated export pin."""

    schema_name: str
    relation_oids: tuple[tuple[str, int], ...]
    semantic_receipt: receipt.EntityAddressArchiveReceipt
    catalog_json: str
    logical_sha256: str
    ordinals: tuple[tuple[str, tuple[tuple[str, int, int], ...]], ...]
    sequence_json: str
    _brand: object


@dataclass(frozen=True)
class RegistryCMSAddressCopyWitness:
    """Compact immutable comparison retaining both exact legacy receipts."""

    source: RegistryCMSAddressCopySource
    clone_schema_name: str
    clone_schema_oid: int
    clone_relation_oids: tuple[tuple[str, int], ...]
    clone_receipt: receipt.EntityAddressArchiveReceipt
    clone_catalog_sha256: str
    clone_ordinals: tuple[tuple[str, tuple[tuple[str, int, int], ...]], ...]
    clone_sequence_json: str

    def as_dict(self) -> dict:
        """Return a local versioned witness without changing legacy receipt bytes."""
        return {
            "version": _VERSION,
            "logical_sha256": self.source.logical_sha256,
            "source": {
                "schema_name": self.source.schema_name,
                "schema_oid": json.loads(self.source.catalog_json)["schema_oid"],
                "relation_oids": self.source.relation_oids,
                "receipt": self.source.semantic_receipt.as_dict(),
                "catalog_sha256": receipt._canonical_digest(json.loads(self.source.catalog_json)),
                "ordinals": self.source.ordinals,
                "sequence": json.loads(self.source.sequence_json),
            },
            "clone": {
                "schema_name": self.clone_schema_name,
                "schema_oid": self.clone_schema_oid,
                "relation_oids": self.clone_relation_oids,
                "receipt": self.clone_receipt.as_dict(),
                "catalog_sha256": self.clone_catalog_sha256,
                "ordinals": self.clone_ordinals,
                "sequence": json.loads(self.clone_sequence_json),
            },
        }


def _json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False, default=receipt._json_scalar)


def _declared_type(column) -> str:
    value = str(column.type.compile(dialect=postgresql.dialect())).lower()
    return value.replace("varchar", "character varying").replace("float", "double precision").replace(", ", ",")


def _numbers(value: str | None, ordinal_by_attnum: dict[int, int], *, array: bool = False) -> str | None:
    if value is None:
        return None
    parts = value.strip("{}").split(",") if array else value.split()
    try:
        normalized_numbers = [str(ordinal_by_attnum[int(part)]) for part in parts if part]
    except (ValueError, KeyError) as error:
        raise RegistryCMSAddressEquivalenceError("address catalog references an inactive column") from error
    return "{" + ",".join(normalized_numbers) + "}" if array else " ".join(normalized_numbers)


def _logical_catalog(catalog_by_table: dict, sequence: dict) -> tuple[str, tuple]:
    """Normalize only active-column numbering and a verified Evidence nextval."""
    models_by_table = {model.__tablename__: model for model in receipt._models()}
    if set(catalog_by_table) != set(models_by_table):
        raise RegistryCMSAddressEquivalenceError("address catalog family differs")
    logical_by_table = {}
    ordinals = []
    for table_name, native_by_field in sorted(catalog_by_table.items()):
        table = json.loads(_json(native_by_field))
        columns = table["columns"]
        type_by_column = {
            column.name: _declared_type(column) for column in models_by_table[table_name].__table__.columns
        }
        if len(columns) != len(type_by_column) or {column["attname"] for column in columns} != set(type_by_column):
            raise RegistryCMSAddressEquivalenceError("address declared columns differ")
        ordinal_by_attnum = {0: 0}
        entries = []
        for ordinal, column in enumerate(columns, 1):
            original = column["attnum"]
            if type(original) is not int or original <= 0 or original in ordinal_by_attnum:
                raise RegistryCMSAddressEquivalenceError("address column numbering differs")
            if column["type"] != type_by_column[column["attname"]]:
                raise RegistryCMSAddressEquivalenceError("address declared column type differs")
            ordinal_by_attnum[original] = ordinal
            entries.append((column["attname"], original, ordinal))
            column["attnum"] = ordinal
            if table_name == _EVIDENCE and column["attname"] == "evidence_id":
                if (
                    original != sequence["attribute_number"]
                    or column["default_expression"] != sequence["default_expression"]
                ):
                    raise RegistryCMSAddressEquivalenceError("address Evidence default binding differs")
                column["default_expression"] = "owned_evidence_nextval"
        for constraint in table["constraints"]:
            if constraint["referenced_table"] is not None or constraint["referenced_columns"] is not None:
                raise RegistryCMSAddressEquivalenceError("address unexpected foreign constraint")
            constraint["key_columns"] = _numbers(constraint["key_columns"], ordinal_by_attnum, array=True)
        for index in table["indexes"]:
            index["keys"] = _numbers(index["keys"], ordinal_by_attnum)
            for attribute in index["key_attributes"]:
                try:
                    attribute["attribute_number"] = ordinal_by_attnum[attribute["attribute_number"]]
                except KeyError as error:
                    raise RegistryCMSAddressEquivalenceError("address index references an inactive column") from error
        table["constraints"] = sorted(table["constraints"], key=_json)
        table["indexes"] = sorted(table["indexes"], key=_json)
        logical_by_table[table_name] = table
        ordinals.append((table_name, tuple(entries)))
    return receipt._canonical_digest({"tables": logical_by_table, "sequence_settings": sequence["settings"]}), tuple(
        ordinals
    )


async def _owned_sequence(session, schema_name: str, table_oid: int) -> dict:
    sequence_records = list(
        (
            await session.execute(
                text("""SELECT sequence.oid AS sequence_oid, namespace.nspname AS schema_name,
                sequence.relname AS sequence_name, owned.refobjsubid AS attribute_number,
                pg_catalog.pg_get_expr(default_value.adbin, default_value.adrelid, true) AS default_expression,
                pg_catalog.format('nextval(%L::regclass)', sequence.oid::regclass::text) AS expected_expression,
                settings.seqtypid::integer AS type_oid, settings.seqstart, settings.seqincrement,
                settings.seqmax, settings.seqmin, settings.seqcache, settings.seqcycle
                FROM pg_catalog.pg_class AS sequence
                JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid=sequence.relnamespace
                JOIN pg_catalog.pg_sequence AS settings ON settings.seqrelid=sequence.oid
                JOIN pg_catalog.pg_depend AS owned ON owned.classid='pg_class'::regclass
                    AND owned.objid=sequence.oid AND owned.refclassid='pg_class'::regclass
                    AND owned.refobjid=:table_oid AND owned.deptype='a'
                JOIN pg_catalog.pg_attribute AS attribute ON attribute.attrelid=:table_oid
                    AND attribute.attnum=owned.refobjsubid AND attribute.attname='evidence_id'
                    AND NOT attribute.attisdropped
                JOIN pg_catalog.pg_attrdef AS default_value ON default_value.adrelid=:table_oid
                    AND default_value.adnum=attribute.attnum
                WHERE sequence.relkind='S' AND namespace.nspname=:schema_name
                    AND EXISTS (SELECT 1 FROM pg_catalog.pg_depend AS used
                        WHERE used.classid='pg_attrdef'::regclass AND used.objid=default_value.oid
                        AND used.refclassid='pg_class'::regclass AND used.refobjid=sequence.oid AND used.deptype='n')
                    AND (SELECT count(*) FROM pg_catalog.pg_depend AS used
                        WHERE used.classid='pg_attrdef'::regclass AND used.objid=default_value.oid
                        AND used.refclassid='pg_class'::regclass AND used.deptype='n') = 1"""),
                {"schema_name": schema_name, "table_oid": table_oid},
            )
        ).mappings()
    )
    if (
        len(sequence_records) != 1
        or sequence_records[0]["default_expression"] != sequence_records[0]["expected_expression"]
    ):
        raise RegistryCMSAddressEquivalenceError("address Evidence sequence is not its native owned nextval")
    native_by_field = dict(sequence_records[0])
    settings_by_name = {
        key: native_by_field.pop(key)
        for key in ("type_oid", "seqstart", "seqincrement", "seqmax", "seqmin", "seqcache", "seqcycle")
    }
    native_by_field.pop("expected_expression")
    return {**native_by_field, "table_oid": table_oid, "settings": settings_by_name}


async def _physical_catalog(session, oid: int) -> dict:
    """Bind native heap and ready/live index OIDs without normalizing their identity."""
    relation_records = list(
        (
            await session.execute(
                text("""SELECT relation.relowner::integer AS owner_oid,
        relation.reltablespace::integer AS tablespace_oid, relation.reltoastrelid::integer AS toast_oid,
        method.amname AS method FROM pg_catalog.pg_class AS relation
        JOIN pg_catalog.pg_am AS method ON method.oid=relation.relam WHERE relation.oid=:oid"""),
                {"oid": oid},
            )
        ).mappings()
    )
    if len(relation_records) != 1:
        raise RegistryCMSAddressEquivalenceError("address native heap is unavailable")
    relation = relation_records[0]
    if relation["method"] != "heap":
        raise RegistryCMSAddressEquivalenceError("address native relation is not a heap")
    indexes = list(
        (
            await session.execute(
                text("""SELECT indexrelid::integer AS oid,
        indisvalid, indisready, indislive FROM pg_catalog.pg_index
        WHERE indrelid=:oid ORDER BY indexrelid"""),
                {"oid": oid},
            )
        ).mappings()
    )
    if any(
        not index_record["indisvalid"] or not index_record["indisready"] or not index_record["indislive"]
        for index_record in indexes
    ):
        raise RegistryCMSAddressEquivalenceError("address native index is not ready, live and valid")
    return {"oid": oid, **dict(relation), "indexes": [dict(index_record) for index_record in indexes]}


def portable_registry_cms_address_stage_receipt(captured, stage_table_names):
    """Keep exact stage receipt fingerprints while restoring portable model names."""
    table_names = receipt._validated_stage_table_names_by_source(stage_table_names)
    names_by_stage = {stage: logical for logical, stage in table_names.items()}
    tables = tuple(replace(entry, table_name=names_by_stage[entry.table_name]) for entry in captured.tables)
    schema_sha256 = receipt._canonical_digest(
        [{key: getattr(entry, key) for key in ("model_name", "table_name", "schema_sha256")} for entry in tables]
    )
    return receipt.EntityAddressArchiveReceipt(
        tables, schema_sha256, receipt._content_identity(tables, captured.main_input_sha256), captured.main_input_sha256
    )


async def _source_semantic_receipt(session, schema_name, stage_table_names):
    if stage_table_names is None:
        return await receipt.capture_entity_address_archive_receipt(session, schema_name=schema_name)
    captured = await receipt.capture_entity_address_stage_integrity_receipt(
        session, schema_name=schema_name, stage_table_names=stage_table_names
    )
    return portable_registry_cms_address_stage_receipt(captured, stage_table_names)


async def _captured_owned_sequence(session, schema_name, expected_oids):
    sequence = await _owned_sequence(session, schema_name, dict(expected_oids)[_EVIDENCE])
    owned_sequence_records = list(
        (
            await session.execute(
                text("""SELECT sequence.oid::integer AS sequence_oid
        FROM pg_catalog.pg_class AS sequence JOIN pg_catalog.pg_depend AS owned
        ON owned.classid='pg_class'::regclass AND owned.objid=sequence.oid
        AND owned.refclassid='pg_class'::regclass AND owned.refobjid=ANY(:oids)
        AND owned.deptype IN ('a','i') WHERE sequence.relkind='S' ORDER BY sequence.oid"""),
                {"oids": [oid for _, oid in expected_oids]},
            )
        ).mappings()
    )
    if [entry["sequence_oid"] for entry in owned_sequence_records] != [sequence["sequence_oid"]]:
        raise RegistryCMSAddressEquivalenceError("address family has an unexpected owned sequence")
    return sequence


async def _native_capture(
    session, schema_name: str, expected_oids: tuple, *, verify_content=True, stage_table_names=None
) -> tuple:
    """Pin and capture exact seven-table native identity before comparing logical shape."""
    schema_name = receipt._schema_name(schema_name)
    models_by_table = receipt._models()
    names = tuple(sorted(model.__tablename__ for model in models_by_table))
    if (
        type(expected_oids) is not tuple
        or any(type(entry) is not tuple or len(entry) != 2 for entry in expected_oids)
        or tuple(name for name, _ in expected_oids) != names
        or any(type(oid) is not int or oid <= 0 for _, oid in expected_oids)
        or len({oid for _, oid in expected_oids}) != 7
    ):
        raise RegistryCMSAddressEquivalenceError("address expected native OIDs are invalid")
    table_names = (
        {name: name for name in names}
        if stage_table_names is None
        else receipt._validated_stage_table_names_by_source(stage_table_names)
    )
    await receipt._normalize_receipt_session(session, schema_name)
    semantic = (
        await _source_semantic_receipt(session, schema_name, None if stage_table_names is None else table_names)
        if verify_content
        else None
    )
    schema_oid = await session.scalar(
        text("SELECT oid FROM pg_catalog.pg_namespace WHERE nspname=:schema"), {"schema": schema_name}
    )
    catalog_by_table = {}
    physical_by_table = {}
    for table_name, expected_oid in expected_oids:
        oid = await receipt._relation_oid(session, schema_name, table_names[table_name])
        if oid != expected_oid:
            raise RegistryCMSAddressEquivalenceError("address native relation OID differs")
        physical_by_table[table_name] = await _physical_catalog(session, oid)
        catalog_by_table[table_name] = {
            "columns": await receipt._catalog_columns(session, oid),
            "constraints": await receipt._catalog_constraints(session, oid, schema_name),
            "indexes": await receipt._catalog_indexes(session, oid),
        }
    sequence = await _captured_owned_sequence(session, schema_name, expected_oids)
    logical_hash, ordinals = _logical_catalog(catalog_by_table, sequence)
    catalog_by_field = {"schema_oid": schema_oid, "logical_catalog": catalog_by_table, "physical": physical_by_table}
    if stage_table_names is not None:
        catalog_by_field["stage_table_names"] = table_names
    return (
        semantic,
        _json(catalog_by_field),
        logical_hash,
        ordinals,
        _json(sequence),
    )


async def capture_registry_cms_address_copy_source(
    session, *, source_schema: str, expected_relation_oids: tuple, stage_table_names=None
) -> RegistryCMSAddressCopySource:
    """Read native Source proof while the caller holds its authenticated export pin."""
    captured = await _native_capture(
        session, source_schema, expected_relation_oids, stage_table_names=stage_table_names
    )
    return RegistryCMSAddressCopySource(source_schema, expected_relation_oids, *captured, _BRAND)


async def validate_registry_cms_address_copy(
    session, *, source_capture: RegistryCMSAddressCopySource, clone_ownership
) -> RegistryCMSAddressCopyWitness:
    """Validate the actual UUID-owned clone against the native pinned Source token."""
    if not isinstance(source_capture, RegistryCMSAddressCopySource) or source_capture._brand is not _BRAND:
        raise RegistryCMSAddressEquivalenceError("address source token was not natively captured")
    clone_ownership = await verify_entity_address_archive_stage_ownership(session, owner=clone_ownership)
    cloned, catalog_by_table, logical_hash, ordinals, sequence = await _native_capture(
        session, clone_ownership.schema_name, clone_ownership.relation_oids
    )
    original = source_capture.semantic_receipt
    if (
        tuple(
            (table_receipt.model_name, table_receipt.table_name, table_receipt.row_count, table_receipt.row_sha256)
            for table_receipt in original.tables
        )
        != tuple(
            (table_receipt.model_name, table_receipt.table_name, table_receipt.row_count, table_receipt.row_sha256)
            for table_receipt in cloned.tables
        )
        or original.main_input_sha256 != cloned.main_input_sha256
    ):
        raise RegistryCMSAddressEquivalenceError("address copy contents differ")
    if logical_hash != source_capture.logical_sha256:
        raise RegistryCMSAddressEquivalenceError("address copy native logical catalog differs")
    return RegistryCMSAddressCopyWitness(
        source_capture,
        clone_ownership.schema_name,
        clone_ownership.schema_oid,
        clone_ownership.relation_oids,
        cloned,
        receipt._canonical_digest(json.loads(catalog_by_table)),
        ordinals,
        sequence,
    )


def _closed(document, keys):
    if type(document) is not dict or set(document) != set(keys):
        raise RegistryCMSAddressEquivalenceError("address witness fields differ")


def _positive_oid(value):
    if type(value) is not int or not 0 < value < 2**32:
        raise RegistryCMSAddressEquivalenceError("address witness native OID is invalid")


def _decode_ordinals(document):
    models_by_table = {model.__tablename__: model for model in receipt._models()}
    if type(document) is not list or len(document) != 7:
        raise RegistryCMSAddressEquivalenceError("address witness ordinal family differs")
    ordinals_by_table = {}
    for entry in document:
        if type(entry) is not list or len(entry) != 2 or entry[0] not in models_by_table or type(entry[1]) is not list:
            raise RegistryCMSAddressEquivalenceError("address witness ordinal entry differs")
        table_name, columns = entry
        declared_names = {column.name for column in models_by_table[table_name].__table__.columns}
        previous = 0
        observed_names = []
        for ordinal, column in enumerate(columns, 1):
            if (
                type(column) is not list
                or len(column) != 3
                or type(column[0]) is not str
                or type(column[1]) is not int
                or not previous < column[1] <= 32767
                or type(column[2]) is not int
                or column[2] != ordinal
            ):
                raise RegistryCMSAddressEquivalenceError("address witness column mapping differs")
            observed_names.append(column[0])
            previous = column[1]
        if (
            len(columns) != len(declared_names)
            or set(observed_names) != declared_names
            or table_name in ordinals_by_table
        ):
            raise RegistryCMSAddressEquivalenceError("address witness declared columns differ")
        ordinals_by_table[table_name] = columns
    if [entry[0] for entry in document] != sorted(models_by_table):
        raise RegistryCMSAddressEquivalenceError("address witness ordinal order differs")
    return ordinals_by_table


def _decode_sequence(sequence, side, oid_by_table, ordinals_by_table):
    _closed(
        sequence,
        {
            "sequence_oid",
            "schema_name",
            "sequence_name",
            "attribute_number",
            "default_expression",
            "table_oid",
            "settings",
        },
    )
    _positive_oid(sequence["sequence_oid"])
    _positive_oid(sequence["table_oid"])
    name = receipt._schema_name(sequence["sequence_name"])
    evidence_number = next(column[1] for column in ordinals_by_table[_EVIDENCE] if column[0] == "evidence_id")
    if (
        sequence["schema_name"] != side["schema_name"]
        or sequence["table_oid"] != oid_by_table[_EVIDENCE]
        or type(sequence["attribute_number"]) is not int
        or sequence["attribute_number"] != evidence_number
        or sequence["default_expression"] not in {f"nextval('{name}'::regclass)", f"nextval('\"{name}\"'::regclass)"}
    ):
        raise RegistryCMSAddressEquivalenceError("address witness owned sequence binding differs")
    settings = sequence["settings"]
    _closed(settings, {"type_oid", "seqstart", "seqincrement", "seqmax", "seqmin", "seqcache", "seqcycle"})
    _positive_oid(settings["type_oid"])
    if (
        any(
            type(settings[key]) is not int or not -(2**63) <= settings[key] < 2**63
            for key in ("seqstart", "seqincrement", "seqmax", "seqmin", "seqcache")
        )
        or not settings["seqmin"] <= settings["seqstart"] <= settings["seqmax"]
        or settings["seqmin"] >= settings["seqmax"]
        or settings["seqincrement"] == 0
        or settings["seqcache"] <= 0
        or type(settings["seqcycle"]) is not bool
    ):
        raise RegistryCMSAddressEquivalenceError("address witness sequence settings differ")


def _decode_side(side):
    _closed(side, {"schema_name", "schema_oid", "relation_oids", "receipt", "catalog_sha256", "ordinals", "sequence"})
    receipt._schema_name(side["schema_name"])
    _positive_oid(side["schema_oid"])
    if type(side["catalog_sha256"]) is not str or re.fullmatch(r"[0-9a-f]{64}", side["catalog_sha256"]) is None:
        raise RegistryCMSAddressEquivalenceError("address witness catalog digest differs")
    names = sorted(model.__tablename__ for model in receipt._models())
    oids = side["relation_oids"]
    if (
        type(oids) is not list
        or len(oids) != 7
        or any(type(entry) is not list or len(entry) != 2 for entry in oids)
        or [entry[0] for entry in oids] != names
    ):
        raise RegistryCMSAddressEquivalenceError("address witness relation family differs")
    for _, oid in oids:
        _positive_oid(oid)
    if len({oid for _, oid in oids}) != 7:
        raise RegistryCMSAddressEquivalenceError("address witness relation OIDs repeat")
    semantic = receipt.validate_entity_address_archive_receipt(side["receipt"])
    if any(
        type(table.row_sha256) is not str or type(table.schema_sha256) is not str or table.row_count >= 2**63
        for table in semantic.tables
    ):
        raise RegistryCMSAddressEquivalenceError("address witness receipt scalar type differs")
    ordinals_by_table = _decode_ordinals(side["ordinals"])
    _decode_sequence(side["sequence"], side, dict(oids), ordinals_by_table)
    return semantic


def _decode_witness(document) -> str:
    """Decode bounded wire evidence without minting an authenticated native Source token."""
    _closed(document, {"version", "logical_sha256", "source", "clone"})
    if (
        document["version"] != _VERSION
        or type(document["logical_sha256"]) is not str
        or re.fullmatch(r"[0-9a-f]{64}", document["logical_sha256"]) is None
        or len(_json(document).encode()) > 32768
    ):
        raise RegistryCMSAddressEquivalenceError("address witness version or bound differs")
    original = _decode_side(document["source"])
    cloned = _decode_side(document["clone"])
    if [(table.model_name, table.table_name, table.row_count, table.row_sha256) for table in original.tables] != [
        (table.model_name, table.table_name, table.row_count, table.row_sha256) for table in cloned.tables
    ] or original.main_input_sha256 != cloned.main_input_sha256:
        raise RegistryCMSAddressEquivalenceError("address witness contents differ")
    if document["source"]["sequence"]["settings"] != document["clone"]["sequence"]["settings"] or any(
        [column[0] for column in source[1]] != [column[0] for column in clone[1]]
        for source, clone in zip(document["source"]["ordinals"], document["clone"]["ordinals"], strict=True)
    ):
        raise RegistryCMSAddressEquivalenceError("address witness logical mapping differs")
    return _json(document)


def decode_registry_cms_address_copy_witness(document) -> str:
    """Reject malformed JSON shapes and return canonical evidence, never a native Source token."""
    try:
        return _decode_witness(document)
    except (TypeError, KeyError, StopIteration, OverflowError) as error:
        raise RegistryCMSAddressEquivalenceError("address witness structure differs") from error


async def require_registry_cms_address_copy_witness(session, document, *, clone_ownership, verify_content=True):
    """Revalidate actual clone identity; protected pair custody authenticates persisted Source evidence."""
    if type(verify_content) is not bool:
        raise RegistryCMSAddressEquivalenceError("address witness verification mode differs")
    document = json.loads(decode_registry_cms_address_copy_witness(document))
    side = document["clone"]
    clone_ownership = await verify_entity_address_archive_stage_ownership(session, owner=clone_ownership)
    if (
        side["schema_name"] != clone_ownership.schema_name
        or side["schema_oid"] != clone_ownership.schema_oid
        or side["relation_oids"] != [list(entry) for entry in clone_ownership.relation_oids]
    ):
        raise RegistryCMSAddressEquivalenceError("address witness clone owner differs")
    semantic, catalog, logical_hash, ordinals, sequence = await _native_capture(
        session, clone_ownership.schema_name, clone_ownership.relation_oids, verify_content=verify_content
    )
    if (
        (verify_content and semantic.as_dict() != side["receipt"])
        or receipt._canonical_digest(json.loads(catalog)) != side["catalog_sha256"]
        or logical_hash != document["logical_sha256"]
        or _json(ordinals) != _json(side["ordinals"])
        or json.loads(sequence) != side["sequence"]
    ):
        raise RegistryCMSAddressEquivalenceError("address witness native clone differs")
