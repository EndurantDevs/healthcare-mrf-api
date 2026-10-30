# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Private candidate-local inventory storage; raw reads and receipts confer no Source authority.

Native writer closure, custody, root linkage, publication and serving integration
remain external. This store does not treat a target DTO or a table OID as a capability.
"""

import hashlib
from dataclasses import dataclass, replace
from uuid import UUID

from process.network_address_projection import _identifier
from process.network_compact_layout import CompactAddressRelation
from process.network_fhir_membership_source import PinnedFHIRMembershipSource
from process.network_membership_candidate_lifecycle import (
    _control_namespace,
    _locked_candidate,
    _require_open,
    _require_transaction,
)
from process.network_read_identity import NetworkReadIdentityError, verify_consumed_network_relations
from process.registry_source_contribution_store import MAX_RECEIPT_BYTES
from process.registry_source_inventory_contract import (
    MAX_INVENTORY_ENTRIES,
    MAX_INVENTORY_PAGE_BYTES,
    RegistrySourceInventoryError,
    SourceInventoryAccumulator,
    SourceInventoryEntry,
    SourceInventoryRoot,
    encode_source_inventory_entry,
    source_inventory_page,
)
from process.registry_source_inventory_reference import verify_source_recipe_metadata
from process.registry_source_office_summary_v3 import MAX_V3_RECEIPT_BYTES
from process.registry_source_recipe_store import MAX_SOURCE_RECIPE_BYTES, decode_registry_source_recipes

INVENTORY_TABLE = "network_source_inventory"
_SCOPE_FIELDS = ("source_system", "source_id", "dataset_schema", "dataset_id", "producer_id", "edition_id")
_ENTRY_FIELDS = ("ordinal", "recipe_json", "contribution_json", "office_derivation_json")
_DERIVED_FIELDS = ("recipe_sha256", *_SCOPE_FIELDS, "fhir_scope_key", "fhir_scope_value")
_COLUMNS = (*_ENTRY_FIELDS, *_DERIVED_FIELDS)
_SELECT_COLUMNS = ",".join(_COLUMNS)
_ENTRY_OVERHEAD_BYTES = 2 + len(_ENTRY_FIELDS) - 1 + sum(len(field) + 3 for field in _ENTRY_FIELDS)
_ENCODED_ENTRY_BYTES_SQL = f"{_ENTRY_OVERHEAD_BYTES}+octet_length(ordinal::text)+" + "+".join(
    f"octet_length(coalesce(to_jsonb({field})::text,'null'))" for field in _ENTRY_FIELDS[1:]
)
_DERIVED_TRANSFER_BYTES_SQL = f"{4 * len(_DERIVED_FIELDS)}+" + "+".join(
    f"coalesce(octet_length({field}),0)" for field in _DERIVED_FIELDS
)
_TRANSFER_BYTES_SQL = f"({_ENCODED_ENTRY_BYTES_SQL})+({_DERIVED_TRANSFER_BYTES_SQL})"
MAX_NATIVE_WIRE_BYTES = MAX_INVENTORY_PAGE_BYTES + 4096
_NATIVE_PROTOCOL_RESERVE = 1024


@dataclass(frozen=True)
class SourceInventoryStoreReceipt:
    """Observed storage identity and commitment; not native custody or admission proof."""

    table_oid: int
    root: SourceInventoryRoot


def _relation(copy_target):
    return _identifier(copy_target.schema_name) + "." + INVENTORY_TABLE


def source_inventory_record(entry):
    """Derive every indexed value from the existing recipe codec, never optional JSON metadata."""
    encode_source_inventory_entry(entry)
    singleton = "[" + entry.recipe_json + "]"
    (recipe,) = decode_registry_source_recipes(singleton)
    scope_key = scope_value = None
    if type(recipe.source_pin) is PinnedFHIRMembershipSource:
        capture = recipe.source_pin.retained_capture
        scope_key, scope_value = (
            ("alias_scope", recipe.source_pin.alias_scope)
            if capture is None
            else ("capture_sha256", capture.capture_sha256)
        )
    return (
        *(getattr(entry, field) for field in _ENTRY_FIELDS),
        hashlib.sha256(singleton.encode()).hexdigest(),
        *recipe.binding_coordinates.sql_parameters,
        scope_key,
        scope_value,
    )


def source_inventory_entry(record):
    """Revalidate both canonical row fields and independently stored typed/indexed derivations."""
    try:
        entry = SourceInventoryEntry(*(record[field] for field in _ENTRY_FIELDS))
        if tuple(record[field] for field in _COLUMNS) != source_inventory_record(entry):
            raise ValueError
        return entry
    except ValueError, TypeError, KeyError, UnicodeError, RecursionError:
        raise RegistrySourceInventoryError("registry_source_inventory_stored_row_invalid") from None


def source_inventory_record_bytes(entry):
    """Count the canonical entry plus indexed-text payload and native length fields.

    Canonical entry framing conservatively covers the four base columns' native
    row framing. A separate reserve bounds scalar preflight and protocol messages.
    """
    record = source_inventory_record(entry)
    derived = record[len(_ENTRY_FIELDS) :]
    return (
        len(encode_source_inventory_entry(entry))
        + 4 * len(derived)
        + sum(len(value.encode()) for value in derived if value is not None)
    )


async def _create_inventory(connection, relation):
    await connection.execute(f"""CREATE TABLE {relation}(
      ordinal integer PRIMARY KEY CHECK(ordinal BETWEEN 1 AND {MAX_INVENTORY_ENTRIES}),
      recipe_json text NOT NULL CHECK(octet_length(recipe_json) BETWEEN 1 AND {MAX_SOURCE_RECIPE_BYTES - 2}),
      contribution_json text CHECK(octet_length(contribution_json)<={MAX_RECEIPT_BYTES}),
      office_derivation_json text CHECK(octet_length(office_derivation_json)<={MAX_V3_RECEIPT_BYTES}),
      recipe_sha256 text NOT NULL UNIQUE CHECK(recipe_sha256 ~ '^[0-9a-f]{{64}}$'),
      source_system text NOT NULL CHECK(source_system IN ('fhir','aca','ptg')),
      source_id text NOT NULL CHECK(octet_length(source_id) BETWEEN 1 AND 128),
      dataset_schema text NOT NULL CHECK(dataset_schema ~ '^[A-Za-z_][A-Za-z0-9_]{{0,62}}$'),
      dataset_id text NOT NULL CHECK(octet_length(dataset_id) BETWEEN 1 AND 128),
      producer_id text NOT NULL CHECK(octet_length(producer_id) BETWEEN 1 AND 128),
      edition_id text NOT NULL CHECK(octet_length(edition_id) BETWEEN 1 AND 128),
      fhir_scope_key text,
      fhir_scope_value text,
      CHECK((source_system='fhir' AND fhir_scope_key IS NOT NULL AND fhir_scope_value IS NOT NULL
          AND fhir_scope_key IN ('capture_sha256','alias_scope') AND octet_length(fhir_scope_value) BETWEEN 1 AND 512)
        OR (source_system<>'fhir' AND fhir_scope_key IS NULL AND fhir_scope_value IS NULL)))""")
    await connection.execute(
        f"CREATE INDEX source_inventory_scope_idx ON {relation} "
        f"({','.join((*_SCOPE_FIELDS, 'fhir_scope_key', 'fhir_scope_value'))})"
    )


def _page_arguments(after_ordinal, row_limit, byte_limit):
    source_inventory_page((), after_ordinal=after_ordinal, row_limit=row_limit, byte_limit=byte_limit)


async def _copy_inventory(connection, copy_target, entries, expected_root, row_limit, byte_limit):
    accumulator = SourceInventoryAccumulator()
    records, encoded_bytes, transfer_bytes = [], 2, _NATIVE_PROTOCOL_RESERVE
    async for entry in _inventory_entries(entries):
        encoded = encode_source_inventory_entry(entry)
        row_transfer_bytes = source_inventory_record_bytes(entry)
        if len(encoded) + 2 > byte_limit:
            raise RegistrySourceInventoryError("registry_source_inventory_page_too_small")
        if row_transfer_bytes + _NATIVE_PROTOCOL_RESERVE > MAX_NATIVE_WIRE_BYTES:
            raise RegistrySourceInventoryError("registry_source_inventory_unsupported_bound")
        if records and (
            len(records) == row_limit
            or encoded_bytes + 1 + len(encoded) > byte_limit
            or transfer_bytes + row_transfer_bytes > MAX_NATIVE_WIRE_BYTES
        ):
            await connection.copy_records_to_table(
                INVENTORY_TABLE, schema_name=copy_target.schema_name, columns=_COLUMNS, records=records
            )
            records, encoded_bytes, transfer_bytes = [], 2, _NATIVE_PROTOCOL_RESERVE
        accumulator.accept(entry)
        encoded_bytes += len(encoded) + bool(records)
        transfer_bytes += row_transfer_bytes
        records.append(source_inventory_record(entry))
    if records:
        await connection.copy_records_to_table(
            INVENTORY_TABLE, schema_name=copy_target.schema_name, columns=_COLUMNS, records=records
        )
    if accumulator.finish() != expected_root:
        raise RegistrySourceInventoryError("registry_source_inventory_root_changed")


async def _inventory_entries(entries):
    """Adapt sync or async entry streams without collecting pages or complete inventories."""
    if hasattr(entries, "__aiter__"):
        async for entry in entries:
            yield entry
    else:
        for entry in entries:
            yield entry


async def _verify_existing_inventory(connection, relation, entries, expected_root, row_limit, byte_limit):
    actual = await connection.fetchrow(
        "SELECT oid::bigint,relkind::text,relpersistence::text FROM pg_class WHERE oid=to_regclass($1)", relation
    )
    if actual is None:
        return None
    if tuple(actual)[1:] != ("r", "p"):
        raise RegistrySourceInventoryError("registry_source_inventory_physical_identity_invalid")
    await connection.execute(f"LOCK TABLE ONLY {relation} IN ACCESS EXCLUSIVE MODE NOWAIT")
    if await connection.fetchval("SELECT to_regclass($1)::oid::bigint", relation) != actual["oid"]:
        raise RegistrySourceInventoryError("registry_source_inventory_physical_identity_invalid")
    accumulator = SourceInventoryAccumulator()
    async for entry in _inventory_entries(entries):
        accumulator.accept(entry)
    if accumulator.finish() != expected_root:
        raise RegistrySourceInventoryError("registry_source_inventory_root_changed")
    await _fold_inventory(connection, relation, expected_root, row_limit, byte_limit)
    return SourceInventoryStoreReceipt(actual["oid"], expected_root)


def _fitted_metadata(metadata, row_limit, byte_limit):
    selected, encoded_bytes, transfer_bytes = [], 2, _NATIVE_PROTOCOL_RESERVE
    for row in metadata:
        if len(selected) == row_limit:
            break
        required = row["encoded_bytes"] + bool(selected)
        if encoded_bytes + required > byte_limit:
            if not selected:
                raise RegistrySourceInventoryError("registry_source_inventory_page_too_small")
            break
        if transfer_bytes + row["transfer_bytes"] > MAX_NATIVE_WIRE_BYTES:
            if not selected:
                raise RegistrySourceInventoryError("registry_source_inventory_unsupported_bound")
            break
        selected.append(row)
        encoded_bytes += required
        transfer_bytes += row["transfer_bytes"]
    return selected


async def _verify_api_consumption(connection, read_identity):
    if read_identity is None:
        return
    if type(read_identity) is not tuple or len(read_identity) != 2:
        raise RegistrySourceInventoryError("registry_source_inventory_physical_identity_invalid")
    try:
        await verify_consumed_network_relations(connection, (read_identity[0],), expected_backend_pid=read_identity[1])
    except NetworkReadIdentityError as error:
        raise RegistrySourceInventoryError("registry_source_inventory_physical_identity_invalid") from error


async def _read_page(connection, relation, after_ordinal, row_limit, byte_limit, *, read_identity=None):
    metadata = await connection.fetch(
        f"SELECT ordinal,({_ENCODED_ENTRY_BYTES_SQL}) AS encoded_bytes,"
        f"({_TRANSFER_BYTES_SQL}) AS transfer_bytes FROM {relation} WHERE ordinal>$1 ORDER BY ordinal LIMIT $2",
        after_ordinal,
        row_limit + 1,
    )
    await _verify_api_consumption(connection, read_identity)
    if any(
        type(preflight_row["ordinal"]) is not int
        or not 1 <= preflight_row["ordinal"] <= MAX_INVENTORY_ENTRIES
        or preflight_row["ordinal"] != after_ordinal + index
        for index, preflight_row in enumerate(metadata, 1)
    ):
        raise RegistrySourceInventoryError("registry_source_inventory_stored_ordinals_invalid")
    fitted = _fitted_metadata(metadata, row_limit, byte_limit)
    if not fitted:
        return source_inventory_page((), after_ordinal=after_ordinal, row_limit=row_limit, byte_limit=byte_limit)
    stored_rows = await connection.fetch(
        f"SELECT {_SELECT_COLUMNS} FROM {relation} WHERE ordinal>$1 AND ordinal<=$2 ORDER BY ordinal",
        after_ordinal,
        fitted[-1]["ordinal"],
    )
    await _verify_api_consumption(connection, read_identity)
    entries = tuple(source_inventory_entry(stored_row) for stored_row in stored_rows)
    if len(entries) != len(fitted) or any(
        len(encode_source_inventory_entry(entry)) != sizes["encoded_bytes"]
        or source_inventory_record_bytes(entry) != sizes["transfer_bytes"]
        for entry, sizes in zip(entries, fitted, strict=True)
    ):
        raise RegistrySourceInventoryError("registry_source_inventory_stored_bytes_invalid")
    page = source_inventory_page(entries, after_ordinal=after_ordinal, row_limit=row_limit, byte_limit=byte_limit)
    if len(page.payload) != 2 + sum(preflight_row["encoded_bytes"] for preflight_row in fitted) + len(fitted) - 1:
        raise RegistrySourceInventoryError("registry_source_inventory_stored_bytes_invalid")
    if page.entries and metadata[-1]["ordinal"] > page.entries[-1].ordinal:
        page = replace(page, next_after_ordinal=page.entries[-1].ordinal)
    return page


async def _fold_inventory(
    connection, relation, expected_root, row_limit, byte_limit, receive=None, *, read_identity=None
):
    physical = await connection.fetchrow(
        f"SELECT count(*) AS total,count(*) FILTER(WHERE ordinal IS NULL OR ordinal<1 "
        f"OR ordinal>{MAX_INVENTORY_ENTRIES}) AS invalid FROM {relation}"
    )
    await _verify_api_consumption(connection, read_identity)
    if physical["total"] != expected_root.entry_count or physical["invalid"] != 0:
        raise RegistrySourceInventoryError("registry_source_inventory_root_changed")
    accumulator, after = SourceInventoryAccumulator(), 0
    while True:
        page = await _read_page(
            connection,
            relation,
            after,
            row_limit,
            byte_limit,
            **({"read_identity": read_identity} if read_identity is not None else {}),
        )
        for entry in page.entries:
            accumulator.accept(entry)
            if receive is not None:
                receive(entry)
        if page.next_after_ordinal is None:
            break
        after = page.next_after_ordinal
    if accumulator.finish() != expected_root:
        raise RegistrySourceInventoryError("registry_source_inventory_root_changed")
    return expected_root


async def write_source_inventory(
    connection,
    copy_target,
    entries,
    *,
    expected_root,
    control_schema=None,
    row_limit=100,
    byte_limit=MAX_INVENTORY_PAGE_BYTES,
):
    """Create and verify one complete inventory inside a caller-owned transaction/savepoint.

    Existing complete-scope/Open control-row authority is mandatory. Any page,
    COPY or verification failure rolls back the whole inventory, including DDL.
    Successful metadata remains uncommitted; closing writers and custody are external.
    byte_limit bounds the encoded entry list; indexed-column/native transfer has
    a separate fixed ceiling of MAX_INVENTORY_PAGE_BYTES + 4096 bytes.
    entries accepts a sync or async stream of entries; flatten each source page
    as it arrives. The caller retains ownership of its source reader/iterator.
    """
    _require_transaction(connection, copy_target)
    _page_arguments(0, row_limit, byte_limit)
    if type(expected_root) is not SourceInventoryRoot:
        raise RegistrySourceInventoryError("registry_source_inventory_invalid")
    async with connection.transaction():
        candidate = await _locked_candidate(connection, copy_target, _control_namespace(control_schema))
        _require_open(candidate)
        registered = verify_source_recipe_metadata(candidate)
        if registered.inventory is None or registered.inventory.root != expected_root:
            raise RegistrySourceInventoryError("registry_source_inventory_root_changed")
        relation = _relation(copy_target)
        existing = await _verify_existing_inventory(connection, relation, entries, expected_root, row_limit, byte_limit)
        if existing is not None:
            return existing
        await _create_inventory(connection, relation)
        await _copy_inventory(connection, copy_target, entries, expected_root, row_limit, byte_limit)
        await _fold_inventory(connection, relation, expected_root, row_limit, byte_limit)
        table_oid = await connection.fetchval("SELECT to_regclass($1)::oid::bigint", relation)
        return SourceInventoryStoreReceipt(table_oid, expected_root)


async def _physical_context(connection, copy_target, expected_table_oid, control_schema, *, api_read=False):
    _require_transaction(connection, copy_target)
    if type(api_read) is not bool:
        raise RegistrySourceInventoryError("registry_source_inventory_physical_identity_invalid")
    if type(expected_table_oid) is not int or not 0 < expected_table_oid < 2**32:
        raise RegistrySourceInventoryError("registry_source_inventory_physical_identity_invalid")
    candidate = await connection.fetchrow(
        f"SELECT dataset_id,schema_id,producer_id,candidate_id,schema_name "
        f"FROM {_control_namespace(control_schema)}.network_membership_candidate WHERE candidate_id=$1",
        UUID(copy_target.candidate_id),
    )
    if candidate is None or any(str(candidate[field]) != getattr(copy_target, field) for field in candidate.keys()):
        raise RegistrySourceInventoryError("registry_source_inventory_scope_invalid")
    relation = _relation(copy_target)
    actual = await connection.fetchrow(
        "SELECT c.oid::bigint,c.relkind::text,c.relpersistence::text FROM pg_class c "
        "JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1 AND c.relname=$2",
        copy_target.schema_name,
        INVENTORY_TABLE,
    )
    if actual is None or tuple(actual) != (expected_table_oid, "r", "p"):
        raise RegistrySourceInventoryError("registry_source_inventory_physical_identity_invalid")
    if api_read:
        return relation
    await connection.execute(f"LOCK TABLE ONLY {relation} IN ACCESS SHARE MODE NOWAIT")
    pinned = await connection.fetchrow(
        "SELECT to_regclass($1)::oid::bigint AS current_oid,EXISTS(SELECT FROM pg_locks "
        "WHERE pid=pg_backend_pid() AND locktype='relation' AND relation=$2::oid "
        "AND mode='AccessShareLock' AND granted) AS has_lock",
        relation,
        expected_table_oid,
    )
    if tuple(pinned) != (expected_table_oid, True):
        raise RegistrySourceInventoryError("registry_source_inventory_physical_identity_invalid")
    return relation


async def _read_context(connection, copy_target, expected_table_oid, control_schema):
    _require_transaction(connection, copy_target)
    settings = await connection.fetchrow(
        "SELECT current_setting('transaction_isolation'),current_setting('transaction_read_only')"
    )
    if tuple(settings) not in (("repeatable read", "on"), ("serializable", "on")):
        raise RegistrySourceInventoryError("registry_source_inventory_snapshot_invalid")
    return await _physical_context(connection, copy_target, expected_table_oid, control_schema)


async def _load_locked_source_inventory(connection, copy_target, candidate, *, expected_table_oid, control_schema=None):
    """Fold metadata using the orchestrator's locked native OID; this grants no Source custody.

    The custodian supplies its actual locked catalog OID. Retained callers must
    first verify exact native custody and use the inventory OID in that seal.
    Keep that transaction and its locks through all subsequent source use.
    """
    _require_transaction(connection, copy_target)
    if await connection.fetchval("SHOW transaction_isolation") not in {"repeatable read", "serializable"}:
        raise RegistrySourceInventoryError("registry_source_inventory_snapshot_invalid")
    inventory = verify_source_recipe_metadata(candidate).inventory
    if inventory is None or any(
        str(candidate[field]) != getattr(copy_target, field)
        for field in ("candidate_id", "dataset_id", "schema_id", "producer_id", "schema_name")
    ):
        raise RegistrySourceInventoryError("registry_source_inventory_scope_invalid")
    relation = await _physical_context(connection, copy_target, expected_table_oid, control_schema)
    actual = await connection.fetchrow(
        f"SELECT source_recipes_json,source_generations FROM {_control_namespace(control_schema)}.network_membership_candidate WHERE candidate_id=$1",
        UUID(copy_target.candidate_id),
    )
    if actual is None or verify_source_recipe_metadata(dict(actual)).inventory != inventory:
        raise RegistrySourceInventoryError("registry_source_inventory_root_changed")
    entries = []
    await _fold_inventory(connection, relation, inventory.root, 100, MAX_INVENTORY_PAGE_BYTES, entries.append)
    return inventory, tuple(entries)


async def _registered_inventory_entries(connection, copy_target, candidate, *, control_schema=None):
    """Load producer validation metadata; this current OID never substitutes for sealed custody."""
    candidate_by_field = {**candidate, "source_recipes_json": candidate.get("source_recipes_json", [])}
    if verify_source_recipe_metadata(candidate_by_field).inventory is None:
        return None
    expected_oid = await connection.fetchval("SELECT to_regclass($1)::oid::bigint", _relation(copy_target))
    _inventory, entries = await _load_locked_source_inventory(
        connection, copy_target, candidate_by_field, expected_table_oid=expected_oid, control_schema=control_schema
    )
    return entries


async def load_source_inventory_for_api(connection, copy_target, candidate, *, control_schema=None):
    """Load a complete inventory after sealed API metadata proof, without explicit reader locks.

    Actual count, preflight and payload reads are checked before interpretation;
    the caller retains this original RR/RO transaction through all subsequent use.
    """
    from process.network_candidate_custody import verify_network_candidate_read_metadata

    _require_transaction(connection, copy_target)
    schema = _control_namespace(control_schema)[1:-1]
    custody = await verify_network_candidate_read_metadata(connection, copy_target, control_schema=schema)
    expected_oid = custody["receipt"]["closure"]["relation_oids"][INVENTORY_TABLE]
    inventory = verify_source_recipe_metadata(candidate).inventory
    if inventory is None or any(
        str(candidate[field]) != getattr(copy_target, field)
        for field in ("candidate_id", "dataset_id", "schema_id", "producer_id", "schema_name")
    ):
        raise RegistrySourceInventoryError("registry_source_inventory_scope_invalid")
    relation = await _physical_context(connection, copy_target, expected_oid, schema, api_read=True)
    actual = await connection.fetchrow(
        f"SELECT source_recipes_json,source_generations FROM {_control_namespace(schema)}.network_membership_candidate WHERE candidate_id=$1",
        UUID(copy_target.candidate_id),
    )
    if actual is None or verify_source_recipe_metadata(dict(actual)).inventory != inventory:
        raise RegistrySourceInventoryError("registry_source_inventory_root_changed")
    pid = await connection.fetchval("SELECT pg_backend_pid()")
    read_identity = (CompactAddressRelation(copy_target.schema_name, INVENTORY_TABLE, expected_oid), pid)
    entries = []
    await _fold_inventory(
        connection, relation, inventory.root, 100, MAX_INVENTORY_PAGE_BYTES, entries.append, read_identity=read_identity
    )
    return inventory, tuple(entries)


async def read_source_inventory_page(
    connection,
    copy_target,
    *,
    expected_table_oid,
    after_ordinal=0,
    row_limit=100,
    byte_limit=MAX_INVENTORY_PAGE_BYTES,
    control_schema=None,
):
    """Read bounded raw metadata under exact scope/OID and RR/RO, without authenticating any Source.

    A caller must separately establish native custody and authorization. Requery
    from the returned last ordinal; byte/count lookahead is never a continuation iterator.
    byte_limit counts exact encoded-list bytes. Native scalar preflight and each
    payload transfer are separately bounded; indexed fields can shorten a page.
    """
    _page_arguments(after_ordinal, row_limit, byte_limit)
    relation = await _read_context(connection, copy_target, expected_table_oid, control_schema)
    return await _read_page(connection, relation, after_ordinal, row_limit, byte_limit)


async def verify_source_inventory(
    connection,
    copy_target,
    *,
    expected_table_oid,
    expected_root,
    control_schema=None,
    row_limit=100,
    byte_limit=MAX_INVENTORY_PAGE_BYTES,
):
    """Verify every stored row and derived column against a complete independently retained root.

    This raw metadata check neither seals the candidate nor authenticates Source custody.
    """
    _page_arguments(0, row_limit, byte_limit)
    if type(expected_root) is not SourceInventoryRoot:
        raise RegistrySourceInventoryError("registry_source_inventory_invalid")
    relation = await _read_context(connection, copy_target, expected_table_oid, control_schema)
    return await _fold_inventory(connection, relation, expected_root, row_limit, byte_limit)
