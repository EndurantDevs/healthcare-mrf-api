# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native local proof of an exact address copy with physical column renumbering."""

from __future__ import annotations

import json
import re
from contextlib import suppress
from dataclasses import dataclass, replace

from sqlalchemy import text
from sqlalchemy.dialects import postgresql

from process import entity_address_snapshot_receipt as receipt
from process.entity_address_snapshot_ownership import verify_entity_address_archive_stage_ownership

_EVIDENCE = "entity_address_evidence"
_VERSION = "registry_cms_address_copy.v2"
_LEGACY_VERSION = "registry_cms_address_copy.v1"


class RegistryCMSAddressEquivalenceError(ValueError):
    """A captured address family or its native copy differs."""


@dataclass(frozen=True)
class RegistryCMSAddressCopySource:
    """Validated source content and portable layout at the declared selected names."""

    schema_name: str
    relation_names: tuple[str, ...]
    semantic_receipt: receipt.EntityAddressArchiveReceipt
    catalog_json: str
    logical_sha256: str
    ordinals: tuple
    sequence_json: str

    @property
    def stage_table_names(self):
        """Return the complete logical-target to held-relation selection."""
        return json.loads(self.catalog_json).get("stage_table_names", {name: name for name in self.relation_names})

    @property
    def relations(self):
        """Expose stable logical targets and their actual qualified names."""
        return tuple((logical, self.schema_name, self.stage_table_names[logical]) for logical in self.relation_names)


@dataclass(frozen=True)
class RegistryCMSAddressCopyWitness:
    """The complete content/layout comparison contains no database catalog identities."""

    source: RegistryCMSAddressCopySource
    clone_schema_name: str
    clone_relation_names: tuple[str, ...]
    clone_receipt: receipt.EntityAddressArchiveReceipt
    clone_catalog_sha256: str
    clone_ordinals: tuple
    clone_sequence_json: str

    def as_dict(self) -> dict:
        """Return fresh named evidence while old envelopes remain decoder-only."""
        source = self.source
        return {
            "version": _VERSION,
            "logical_sha256": source.logical_sha256,
            "source": _named_side(
                source.schema_name,
                source.stage_table_names,
                source.semantic_receipt,
                receipt._canonical_digest(json.loads(source.catalog_json)),
                source.ordinals,
                source.sequence_json,
            ),
            "clone": _named_side(
                self.clone_schema_name,
                {name: name for name in self.clone_relation_names},
                self.clone_receipt,
                self.clone_catalog_sha256,
                self.clone_ordinals,
                self.clone_sequence_json,
            ),
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


def _normalized_catalog(catalog_by_table: dict, sequence: dict) -> tuple[dict, tuple]:
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
    return logical_by_table, tuple(ordinals)


async def _owned_sequence(session, schema_name: str, table_name: str) -> dict:
    """Resolve native sequence ownership by the declared Evidence relation, storing only names."""
    relation = receipt._quoted(schema_name) + "." + receipt._quoted(table_name)
    sequence_records = list(
        (
            await session.execute(
                text("""SELECT namespace.nspname AS schema_name,
        sequence.relname AS sequence_name,owned.refobjsubid AS attribute_number,
        pg_get_expr(d.adbin,d.adrelid,true) AS default_expression,
        format('nextval(%L::regclass)',sequence.oid::regclass::text) AS expected_expression,
        format_type(settings.seqtypid,NULL) AS type,settings.seqstart,settings.seqincrement,
        settings.seqmax,settings.seqmin,settings.seqcache,settings.seqcycle
        FROM pg_class sequence JOIN pg_namespace namespace ON namespace.oid=sequence.relnamespace
        JOIN pg_sequence settings ON settings.seqrelid=sequence.oid
        JOIN pg_depend owned ON owned.classid='pg_class'::regclass AND owned.objid=sequence.oid
          AND owned.refclassid='pg_class'::regclass AND owned.refobjid=to_regclass(:relation) AND owned.deptype='a'
        JOIN pg_attribute attribute ON attribute.attrelid=owned.refobjid AND attribute.attnum=owned.refobjsubid
          AND attribute.attname='evidence_id' AND NOT attribute.attisdropped
        JOIN pg_attrdef d ON d.adrelid=attribute.attrelid AND d.adnum=attribute.attnum
        WHERE sequence.relkind='S' AND namespace.nspname=:schema_name
          AND (SELECT count(*) FROM pg_depend used WHERE used.classid='pg_attrdef'::regclass
               AND used.objid=d.oid AND used.refclassid='pg_class'::regclass AND used.deptype='n')=1
          AND EXISTS(SELECT FROM pg_depend used WHERE used.classid='pg_attrdef'::regclass
              AND used.objid=d.oid AND used.refclassid='pg_class'::regclass AND used.refobjid=sequence.oid AND used.deptype='n')"""),
                {"schema_name": schema_name, "relation": relation},
            )
        ).mappings()
    )
    if (
        len(sequence_records) != 1
        or sequence_records[0]["default_expression"] != sequence_records[0]["expected_expression"]
    ):
        raise RegistryCMSAddressEquivalenceError("address Evidence sequence is not its native owned nextval")
    sequence_by_field = dict(sequence_records[0])
    sequence_by_field.pop("expected_expression")
    settings_by_name = {
        key: sequence_by_field.pop(key)
        for key in ("type", "seqstart", "seqincrement", "seqmax", "seqmin", "seqcache", "seqcycle")
    }
    return {**sequence_by_field, "table_name": table_name, "settings": settings_by_name}


async def _require_native_layout(session, schema_name, table_name):
    """Require a persistent standalone heap and valid native indexes at a qualified name."""
    relation = receipt._quoted(schema_name) + "." + receipt._quoted(table_name)
    valid = await session.scalar(
        text("""SELECT c.relkind='r' AND c.relpersistence='p'
        AND NOT c.relrowsecurity AND NOT c.relforcerowsecurity AND am.amname='heap'
        AND NOT EXISTS(SELECT FROM pg_inherits WHERE inhrelid=c.oid OR inhparent=c.oid)
        AND NOT EXISTS(SELECT FROM pg_rewrite WHERE ev_class=c.oid)
        AND NOT EXISTS(SELECT FROM pg_index WHERE indrelid=c.oid AND NOT(indisvalid AND indisready AND indislive))
        FROM pg_class c JOIN pg_am am ON am.oid=c.relam WHERE c.oid=to_regclass(:relation)"""),
        {"relation": relation},
    )
    if valid is not True:
        raise RegistryCMSAddressEquivalenceError("address native heap or index is unavailable")


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
        return await receipt.capture_entity_address_archive_receipt(session, schema_name=schema_name, read_only=True)
    captured = await receipt.capture_entity_address_stage_integrity_receipt(
        session, schema_name=schema_name, stage_table_names=stage_table_names, read_only=True
    )
    return portable_registry_cms_address_stage_receipt(captured, stage_table_names)


async def _captured_owned_sequence(session, schema_name, table_names):
    sequence = await _owned_sequence(session, schema_name, table_names[_EVIDENCE])
    relations = [receipt._quoted(schema_name) + "." + receipt._quoted(name) for name in table_names.values()]
    owned_sequences = list(
        (
            await session.execute(
                text("""SELECT n.nspname,c.relname FROM pg_class c
        JOIN pg_namespace n ON n.oid=c.relnamespace JOIN pg_depend d ON d.objid=c.oid
        AND d.classid='pg_class'::regclass AND d.refclassid='pg_class'::regclass
        WHERE c.relkind='S' AND d.deptype IN('a','i') AND d.refobjid IN
        (SELECT to_regclass(name) FROM unnest(CAST(:relations AS text[])) AS name)
        ORDER BY n.nspname,c.relname"""),
                {"relations": relations},
            )
        ).mappings()
    )
    if [(row["nspname"], row["relname"]) for row in owned_sequences] != [
        (sequence["schema_name"], sequence["sequence_name"])
    ]:
        raise RegistryCMSAddressEquivalenceError("address family has an unexpected owned sequence")
    return sequence


async def _native_capture(
    session, schema_name: str, expected_relations: tuple, *, verify_content=True, stage_table_names=None
) -> tuple:
    """Preserve reader budgets while canonicalizing catalog names for cheap checks."""
    schema_name = receipt._schema_name(schema_name)
    models_by_table = receipt._models()
    names = tuple(sorted(model.__tablename__ for model in models_by_table))
    selected_names = tuple(
        entry[0] if isinstance(entry, (tuple, list)) and len(entry) == 2 else entry for entry in expected_relations
    )
    if selected_names != names:
        raise RegistryCMSAddressEquivalenceError("address expected named family is invalid")
    table_names = (
        {name: name for name in names}
        if stage_table_names is None
        else receipt._validated_stage_table_names_by_source(stage_table_names)
    )
    if verify_content:
        await receipt._normalize_receipt_session(session, schema_name)
        return await _native_capture_catalog(
            session,
            schema_name,
            expected_relations,
            table_names,
            verify_content=True,
            stage_table_names=stage_table_names,
        )
    original_path = await session.scalar(text("SELECT current_setting('search_path')"))
    try:
        await session.execute(
            text("SELECT pg_catalog.set_config('search_path',:path,true)"),
            {"path": '"' + schema_name + '",pg_catalog'},
        )
        captured = await _native_capture_catalog(
            session,
            schema_name,
            expected_relations,
            table_names,
            verify_content=False,
            stage_table_names=stage_table_names,
        )
    except BaseException:
        # An aborted transaction discards its local setting on rollback.
        with suppress(Exception):
            await session.execute(
                text("SELECT pg_catalog.set_config('search_path',:path,true)"), {"path": original_path}
            )
        raise
    await session.execute(text("SELECT pg_catalog.set_config('search_path',:path,true)"), {"path": original_path})
    return captured


async def _native_capture_catalog(
    session, schema_name, expected_relations, table_names, *, verify_content, stage_table_names
):
    """Capture full logical content and layout while native pins hold the selected names."""
    from process.network_cms_registry_address_capture import _require_native_constraint_triggers, native_driver

    await receipt._lock_relation_family(session, schema_name, tuple(table_names.values()), read_only=True)
    await _require_native_constraint_triggers(await native_driver(session), schema_name, tuple(table_names.values()))
    semantic = (
        await _source_semantic_receipt(session, schema_name, None if stage_table_names is None else table_names)
        if verify_content
        else None
    )
    catalog_by_table = {}
    for logical in sorted(table_names):
        selected = table_names[logical]
        await _require_native_layout(session, schema_name, selected)
        local_handle = await receipt._relation_oid(session, schema_name, selected)
        catalog_by_table[logical] = {
            "columns": await receipt._catalog_columns(session, local_handle),
            "constraints": await receipt._catalog_constraints(session, local_handle, schema_name),
            "indexes": await receipt._catalog_indexes(session, local_handle),
        }
    sequence = await _captured_owned_sequence(session, schema_name, table_names)
    normalized, ordinals = _normalized_catalog(catalog_by_table, sequence)
    catalog_by_field = {"logical_catalog": normalized, "stage_table_names": table_names}
    return semantic, _json(catalog_by_field), _logical_catalog(catalog_by_table, sequence)[0], ordinals, _json(sequence)


async def capture_registry_cms_address_copy_source(
    session, *, source_schema: str, expected_relation_names=None, expected_relation_oids=None, stage_table_names=None
) -> RegistryCMSAddressCopySource:
    """Validate the complete declared named source; old tokens are ignored compatibility input."""
    if (expected_relation_names is None) == (expected_relation_oids is None):
        raise RegistryCMSAddressEquivalenceError("address expected named family is invalid")
    selected = expected_relation_names if expected_relation_names is not None else expected_relation_oids
    captured = await _native_capture(session, source_schema, selected, stage_table_names=stage_table_names)
    names = tuple(sorted(model.__tablename__ for model in receipt._models()))
    return RegistryCMSAddressCopySource(source_schema, names, *captured)


async def validate_registry_cms_address_copy(
    session, *, source_capture: RegistryCMSAddressCopySource, clone_ownership
) -> RegistryCMSAddressCopyWitness:
    """Validate the actual UUID-owned clone against the native pinned Source token."""
    if not isinstance(source_capture, RegistryCMSAddressCopySource):
        raise RegistryCMSAddressEquivalenceError("address source content record is invalid")
    clone_ownership = await verify_entity_address_archive_stage_ownership(session, owner=clone_ownership)
    cloned, catalog_by_table, logical_hash, ordinals, sequence = await _native_capture(
        session, clone_ownership.schema_name, clone_ownership.relation_names
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
        clone_ownership.relation_names,
        cloned,
        receipt._canonical_digest(json.loads(catalog_by_table)),
        ordinals,
        sequence,
    )


def _closed(document, keys):
    if type(document) is not dict or set(document) != set(keys):
        raise RegistryCMSAddressEquivalenceError("address witness fields differ")


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


def _decode_sequence(sequence, side, ordinals_by_table):
    is_named = "table_name" in sequence
    fields = {"schema_name", "sequence_name", "attribute_number", "default_expression", "settings"}
    _closed(sequence, fields | ({"table_name"} if is_named else {"sequence_oid", "table_oid"}))
    name = receipt._schema_name(sequence["sequence_name"])
    evidence_number = next(column[1] for column in ordinals_by_table[_EVIDENCE] if column[0] == "evidence_id")
    if (
        sequence["schema_name"] != side["schema_name"]
        or type(sequence["attribute_number"]) is not int
        or sequence["attribute_number"] != evidence_number
        or sequence["default_expression"] not in {f"nextval('{name}'::regclass)", f"nextval('\"{name}\"'::regclass)"}
    ):
        raise RegistryCMSAddressEquivalenceError("address witness owned sequence binding differs")
    settings = sequence["settings"]
    _closed(
        settings,
        {"type" if is_named else "type_oid", "seqstart", "seqincrement", "seqmax", "seqmin", "seqcache", "seqcycle"},
    )
    if (
        (is_named and settings["type"] != "bigint")
        or any(
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
    semantic = _semantic_side_receipt(side)
    ordinals_by_table = _decode_ordinals(side["ordinals"])
    _decode_sequence(side["sequence"], side, ordinals_by_table)
    return semantic


def _decode_witness(document) -> str:
    """Decode bounded wire evidence without minting an authenticated native Source token."""
    _closed(document, {"version", "logical_sha256", "source", "clone"})
    if (
        document["version"] not in {_VERSION, _LEGACY_VERSION}
        or type(document["logical_sha256"]) is not str
        or re.fullmatch(r"[0-9a-f]{64}", document["logical_sha256"]) is None
        or len(_json(document).encode()) > 32768
    ):
        raise RegistryCMSAddressEquivalenceError("address witness version or bound differs")
    decoder = _decode_side if document["version"] == _LEGACY_VERSION else _decode_named_side
    original = decoder(document["source"])
    cloned = decoder(document["clone"])
    if [(table.model_name, table.table_name, table.row_count, table.row_sha256) for table in original.tables] != [
        (table.model_name, table.table_name, table.row_count, table.row_sha256) for table in cloned.tables
    ] or original.main_input_sha256 != cloned.main_input_sha256:
        raise RegistryCMSAddressEquivalenceError("address witness contents differ")
    if _sequence_settings(document["source"]["sequence"]) != _sequence_settings(document["clone"]["sequence"]) or any(
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
    """Check actual named data/layout; old physical hashes never authenticate the current database."""
    if type(verify_content) is not bool:
        raise RegistryCMSAddressEquivalenceError("address witness verification mode differs")
    document = json.loads(decode_registry_cms_address_copy_witness(document))
    side = document["clone"]
    owner = await verify_entity_address_archive_stage_ownership(session, owner=clone_ownership)
    if side["schema_name"] != owner.schema_name:
        raise RegistryCMSAddressEquivalenceError("address witness clone name differs")
    semantic, catalog, logical_hash, _ordinals, sequence = await _native_capture(
        session, owner.schema_name, owner.relation_names, verify_content=verify_content
    )
    if document["version"] == _VERSION:
        if side["relations"] != _named_relations(owner.schema_name, {name: name for name in owner.relation_names}):
            raise RegistryCMSAddressEquivalenceError("address witness clone name differs")
        has_matching_layout = (
            logical_hash == document["logical_sha256"]
            and receipt._canonical_digest(json.loads(catalog)) == side["catalog_sha256"]
        )
    elif logical_hash == document["logical_sha256"]:
        # This exact commitment covers the complete normalized layout, independent of old catalog ordering.
        has_matching_layout = True
    else:
        old_hashes = await _legacy_schema_hashes(
            session, json.loads(catalog)["logical_catalog"], side["ordinals"], side["sequence"]
        )
        has_matching_layout = all(
            entry["schema_sha256"] in old_hashes[entry["table_name"]] for entry in side["receipt"]["tables"]
        )
    current_sequence = json.loads(sequence)
    if any(current_sequence[field] != side["sequence"][field] for field in ("schema_name", "sequence_name")):
        raise RegistryCMSAddressEquivalenceError("address witness sequence name differs")
    if not has_matching_layout or _sequence_settings(json.loads(sequence)) != _sequence_settings(side["sequence"]):
        raise RegistryCMSAddressEquivalenceError("address witness named layout differs")
    if verify_content and _content_fields(semantic.as_dict()) != _content_fields(side["receipt"]):
        raise RegistryCMSAddressEquivalenceError("address witness contents differ")


def _sequence_settings(sequence):
    settings_by_name = {key: value for key, value in sequence["settings"].items() if key != "type_oid"}
    settings_by_name.setdefault("type", "bigint")
    return settings_by_name


def _logical_catalog(catalog_by_table, sequence):
    normalized, ordinals = _normalized_catalog(catalog_by_table, sequence)
    return receipt._canonical_digest(
        {"tables": normalized, "sequence_settings": _sequence_settings(sequence)}
    ), ordinals


def _named_relations(schema_name, table_names):
    return [
        {"logical_name": logical, "schema_name": schema_name, "relation_name": table_names[logical]}
        for logical in sorted(table_names)
    ]


def _named_side(schema, table_names, semantic, catalog_sha256, ordinals, sequence_json):
    return {
        "schema_name": schema,
        "relations": _named_relations(schema, table_names),
        "receipt": semantic.as_dict(),
        "catalog_sha256": catalog_sha256,
        "ordinals": json.loads(_json(ordinals)),
        "sequence": json.loads(sequence_json),
    }


def _content_fields(semantic):
    return [
        (entry["model_name"], entry["table_name"], entry["row_count"], entry["row_sha256"])
        for entry in semantic["tables"]
    ], semantic["main_input_sha256"]


def _decode_named_side(side):
    _closed(side, {"schema_name", "relations", "receipt", "catalog_sha256", "ordinals", "sequence"})
    schema = receipt._schema_name(side["schema_name"])
    names = sorted(model.__tablename__ for model in receipt._models())
    relations = side["relations"]
    if type(relations) is not list or len(relations) != 7:
        raise RegistryCMSAddressEquivalenceError("address witness relation family differs")
    for entry in relations:
        _closed(entry, {"logical_name", "schema_name", "relation_name"})
        receipt._schema_name(entry["relation_name"])
    if (
        [entry["logical_name"] for entry in relations] != names
        or any(entry["schema_name"] != schema for entry in relations)
        or len({entry["relation_name"] for entry in relations}) != 7
    ):
        raise RegistryCMSAddressEquivalenceError("address witness relation family differs")
    if type(side["catalog_sha256"]) is not str or not re.fullmatch(r"[0-9a-f]{64}", side["catalog_sha256"]):
        raise RegistryCMSAddressEquivalenceError("address witness catalog digest differs")
    if (
        side["sequence"]["table_name"]
        != dict((entry["logical_name"], entry["relation_name"]) for entry in relations)[_EVIDENCE]
    ):
        raise RegistryCMSAddressEquivalenceError("address witness sequence relation differs")
    _decode_sequence(side["sequence"], side, _decode_ordinals(side["ordinals"]))
    return _semantic_side_receipt(side)


def _legacy_schema_catalog(catalog_by_table, old_ordinals, sequence):
    """Reconstruct old semantic column slots, never old physical catalog commitments."""
    saved = _decode_ordinals(old_ordinals)
    legacy_by_table = {}
    for logical, table in json.loads(_json(catalog_by_table)).items():
        old_number_by_name = {name: number for name, number, _canonical in saved[logical]}
        number_by_current = {
            0: 0,
            **{column["attnum"]: old_number_by_name[column["attname"]] for column in table["columns"]},
        }
        for column in table["columns"]:
            column["attnum"] = number_by_current[column["attnum"]]
            if (logical, column["attname"]) == (_EVIDENCE, "evidence_id"):
                column["default_expression"] = sequence["default_expression"]
        for constraint in table["constraints"]:
            constraint["key_columns"] = _numbers(constraint["key_columns"], number_by_current, array=True)
        for index in table["indexes"]:
            index["keys"] = _numbers(index["keys"], number_by_current)
            for attribute in index["key_attributes"]:
                attribute["attribute_number"] = number_by_current[attribute["attribute_number"]]
        table["indexes"] = sorted(table["indexes"], key=_json)
        table["constraints"] = sorted(
            table["constraints"],
            key=lambda entry: tuple(
                (entry[key] is None, entry[key] or "")
                for key in ("contype", "key_columns", "referenced_columns", "referenced_table")
            ),
        )
        legacy_by_table[logical] = {"table_name": logical, **table}
    return legacy_by_table


async def _legacy_schema_hashes(session, catalog_by_table, old_ordinals, sequence):
    """Retain complete old schema commitments in byte order or their original SQL collation."""
    legacy_by_table = _legacy_schema_catalog(catalog_by_table, old_ordinals, sequence)
    hashes_by_table = {name: {receipt._canonical_digest(table)} for name, table in legacy_by_table.items()}
    ordered = (
        await session.execute(
            text("""SELECT tables.key AS table_name,
              COALESCE(jsonb_agg(constraints.value ORDER BY
                constraints.value->>'contype',constraints.value->>'key_columns',
                constraints.value->>'referenced_columns',constraints.value->>'referenced_table')
                FILTER (WHERE constraints.value IS NOT NULL),'[]'::jsonb) AS constraints
              FROM jsonb_each(CAST(:catalog AS jsonb)) AS tables
              LEFT JOIN LATERAL jsonb_array_elements(tables.value->'constraints') AS constraints(value) ON true
              GROUP BY tables.key"""),
            {"catalog": _json(legacy_by_table)},
        )
    ).mappings()
    observed_names = set()
    for row in ordered:
        name = row["table_name"]
        if name not in legacy_by_table or name in observed_names:
            raise RegistryCMSAddressEquivalenceError("address legacy constraint inventory differs")
        observed_names.add(name)
        constraints = row["constraints"]
        legacy_by_table[name]["constraints"] = json.loads(constraints) if isinstance(constraints, str) else constraints
        hashes_by_table[name].add(receipt._canonical_digest(legacy_by_table[name]))
    if observed_names != set(legacy_by_table):
        raise RegistryCMSAddressEquivalenceError("address legacy constraint inventory differs")
    return hashes_by_table


def _semantic_side_receipt(side):
    semantic = receipt.validate_entity_address_archive_receipt(side["receipt"])
    if any(
        type(table.row_sha256) is not str or type(table.schema_sha256) is not str or table.row_count >= 2**63
        for table in semantic.tables
    ):
        raise RegistryCMSAddressEquivalenceError("address witness receipt scalar type differs")
    return semantic
