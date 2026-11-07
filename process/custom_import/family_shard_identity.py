# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded stable dictionaries inside an already authorized owner transaction.

The caller supplies admitted typed keys or explicitly proven retained lexical
evidence, and owns the transaction's lease checks and rollback. Exact key text
is hashed as supplied; this module never normalizes retained identity bytes.
Existing global constraints and write guards remain active.
"""

import hmac
from dataclasses import dataclass

from process.custom_import.family import _is_valid_npi
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from process.custom_import.family_block_codec import FamilyBlockResourceLimit
from process.custom_import.family_block_store import _control_schema, _execute
from process.custom_import.runner_codec import digest_text, root_key_contract_hash
from process.custom_import.runner_graph import entity_value_digest

_INPUT = (
    "WITH input AS MATERIALIZED (SELECT * FROM unnest("
    "CAST(:root_keys AS text[]),CAST(:root_hashes AS bytea[]),"
    "CAST(:entity_values AS text[]),CAST(:entity_hashes AS bytea[])) "
    "WITH ORDINALITY AS i(root_key,root_hash,entity_value,entity_hash,ordinality)) "
)
_ROOT_INPUT = (
    "WITH input AS MATERIALIZED (SELECT * FROM unnest("
    "CAST(:root_keys AS text[]),CAST(:root_hashes AS bytea[])) "
    "WITH ORDINALITY AS i(root_key,root_hash,ordinality)) "
)


@dataclass(frozen=True)
class GlobalIdentityKey:
    root_key_text: str
    root_key_sha256: bytes
    entity_value: str


@dataclass(frozen=True)
class GlobalIdentity:
    root_record_id: int
    entity_binding_id: int


def _parameters(definition, dataset_id, keys, maximum_rows, maximum_bytes):
    if type(dataset_id) is not int or not 0 < dataset_id < 2**63:
        raise ValueError("global identity dataset differs")
    if any(type(budget) is not int or budget <= 0 for budget in (maximum_rows, maximum_bytes)):
        raise ValueError("global identity batch budgets must be positive")
    if type(keys) is not tuple:
        raise ValueError("global identities require a frozen key batch")
    if len(keys) > maximum_rows:
        raise FamilyBlockResourceLimit("global identity row budget exceeded")
    parameter_by_name = {
        "dataset_id": dataset_id,
        "root_keys": [],
        "root_hashes": [],
        "entity_values": [],
        "entity_hashes": [],
    }
    size, text_by_hash = 0, {}
    for key in keys:
        if (
            type(key) is not GlobalIdentityKey
            or type(key.root_key_text) is not str
            or not key.root_key_text
            or "\x00" in key.root_key_text
            or type(key.root_key_sha256) is not bytes
            or len(key.root_key_sha256) != 32
            or type(key.entity_value) is not str
            or not _is_valid_npi(key.entity_value)
        ):
            raise ValueError("global identity key evidence differs")
        if size + len(key.root_key_text) + len(key.entity_value) + 80 > maximum_bytes:
            raise FamilyBlockResourceLimit("global identity byte budget exceeded")
        try:
            root_bytes = key.root_key_text.encode("utf-8")
            entity_bytes = key.entity_value.encode("utf-8")
        except UnicodeEncodeError as error:
            raise ValueError("global identity key evidence is not UTF-8") from error
        # Bound encoded evidence, two digests, ordinal and scalar identity cells.
        size += len(root_bytes) + len(entity_bytes) + 80
        if size > maximum_bytes:
            raise FamilyBlockResourceLimit("global identity byte budget exceeded")
        if not hmac.compare_digest(digest_text("root-key", key.root_key_text), key.root_key_sha256):
            raise FamilyBlockUnavailable("global root key digest differs")
        previous = text_by_hash.setdefault(key.root_key_sha256, root_bytes)
        if previous != root_bytes:
            raise FamilyBlockUnavailable("global root key digest collision")
        parameter_by_name["root_keys"].append(key.root_key_text)
        parameter_by_name["root_hashes"].append(key.root_key_sha256)
        parameter_by_name["entity_values"].append(key.entity_value)
        parameter_by_name["entity_hashes"].append(entity_value_digest(key.entity_value))
    if keys:
        parameter_by_name["key_contract"] = root_key_contract_hash(definition)
    return parameter_by_name


def _checked_identities(keys, rows):
    if len(rows) != len(keys):
        raise FamilyBlockUnavailable("global identity result count differs")
    result, roots, entities = [], {}, {}
    for ordinal, (key, row) in enumerate(zip(keys, rows, strict=True), 1):
        root_id, entity_id = row["root_record_id"], row["entity_binding_id"]
        if (
            type(row["ordinality"]) is not int
            or row["ordinality"] != ordinal
            or row["valid"] is not True
            or any(type(value) is not int or not 0 < value < 2**63 for value in (root_id, entity_id))
        ):
            raise FamilyBlockUnavailable("global identity scope, bytes or result order differs")
        root_identity = key.root_key_sha256, key.root_key_text
        if (
            roots.setdefault(root_identity, root_id) != root_id
            or entities.setdefault(key.entity_value, entity_id) != entity_id
        ):
            raise FamilyBlockUnavailable("global identity repeated key differs")
        result.append(GlobalIdentity(root_id, entity_id))
    return tuple(result)


async def intern_global_identities(session, definition, dataset_id, keys, *, maximum_rows, maximum_bytes):
    """Return every observed root/binding ID in input order, including duplicates.

    Three set statements retain the existing global uniqueness domains. The
    final read is a separate statement so READ COMMITTED can see a concurrent
    ON CONFLICT winner. No conflict updates, ID ranges or transaction commits
    are used. Every statement retains the caller's native statement budget.
    """
    parameters = _parameters(definition, dataset_id, keys, maximum_rows, maximum_bytes)
    if not keys:
        return ()
    quote, schema = await _control_schema(session)
    root = f"{quote.quote_schema(schema)}.{quote.quote('custom_import_root_record')}"
    entity = f"{quote.quote_schema(schema)}.{quote.quote('custom_import_entity_binding')}"
    await _execute(
        session,
        _INPUT + f"INSERT INTO {root}(dataset_id,key_contract_sha256,canonical_logical_key,logical_key_sha256) "
        "SELECT :dataset_id,:key_contract,i.root_key,i.root_hash FROM input i GROUP BY i.root_key,i.root_hash "
        "ORDER BY i.root_hash,convert_to(i.root_key,'UTF8') "
        "ON CONFLICT(dataset_id,key_contract_sha256,logical_key_sha256) DO NOTHING",
        parameters,
    )
    await _execute(
        session,
        _INPUT + f"INSERT INTO {entity}(dataset_id,adapter_id,canonical_value,value_sha256) "
        "SELECT :dataset_id,'npi',i.entity_value,i.entity_hash FROM input i GROUP BY i.entity_value,i.entity_hash "
        "ORDER BY convert_to(i.entity_value,'UTF8'),i.entity_hash "
        "ON CONFLICT(dataset_id,adapter_id,canonical_value) DO NOTHING",
        parameters,
    )
    identity_result = await _execute(
        session,
        _INPUT + "SELECT i.ordinality,r.root_record_id,b.entity_binding_id,"
        "(r.dataset_id=:dataset_id AND r.key_contract_sha256=:key_contract AND r.logical_key_sha256=i.root_hash "
        "AND convert_to(r.canonical_logical_key,'UTF8')=convert_to(i.root_key,'UTF8') "
        "AND b.dataset_id=:dataset_id AND b.adapter_id='npi' "
        "AND convert_to(b.canonical_value,'UTF8')=convert_to(i.entity_value,'UTF8') "
        "AND b.value_sha256=i.entity_hash) AS valid FROM input i "
        f"LEFT JOIN {root} r ON r.dataset_id=:dataset_id AND r.key_contract_sha256=:key_contract "
        "AND r.logical_key_sha256=i.root_hash "
        f"LEFT JOIN {entity} b ON b.dataset_id=:dataset_id AND b.adapter_id='npi' AND b.canonical_value=i.entity_value "
        "ORDER BY i.ordinality",
        parameters,
    )
    return _checked_identities(keys, identity_result.mappings().all())


def _root_parameters(definition, dataset_id, keys, maximum_rows, maximum_bytes):
    if type(dataset_id) is not int or not 0 < dataset_id < 2**63:
        raise ValueError("root dictionary dataset differs")
    if any(type(budget) is not int or budget <= 0 for budget in (maximum_rows, maximum_bytes)):
        raise ValueError("root dictionary budgets must be positive")
    if type(keys) is not tuple:
        raise ValueError("root dictionary requires frozen key evidence")
    if len(keys) > maximum_rows:
        raise FamilyBlockResourceLimit("root dictionary row budget exceeded")
    texts, digests, seen = [], [], {}
    size = 0
    for key in keys:
        if type(key) is not tuple or len(key) != 2:
            raise ValueError("root dictionary key shape differs")
        canonical, digest = key
        if (
            type(canonical) is not str
            or not canonical
            or "\x00" in canonical
            or type(digest) is not bytes
            or len(digest) != 32
        ):
            raise ValueError("root dictionary key evidence differs")
        if size + len(canonical) + 48 > maximum_bytes:
            raise FamilyBlockResourceLimit("root dictionary byte budget exceeded")
        try:
            encoded = canonical.encode("utf-8")
        except UnicodeEncodeError as error:
            raise ValueError("root dictionary key is not UTF-8") from error
        size += len(encoded) + 48
        if size > maximum_bytes:
            raise FamilyBlockResourceLimit("root dictionary byte budget exceeded")
        if not hmac.compare_digest(digest_text("root-key", canonical), digest):
            raise FamilyBlockUnavailable("root dictionary digest differs")
        if seen.setdefault(digest, encoded) != encoded:
            raise FamilyBlockUnavailable("root dictionary digest collision")
        texts.append(canonical)
        digests.append(digest)
    return {
        "dataset_id": dataset_id,
        "key_contract": root_key_contract_hash(definition),
        "root_keys": texts,
        "root_hashes": digests,
    }


def _source_root_input(parameters, batch_ordinals, key_count):
    input_sql, grouping = _ROOT_INPUT, "root_key,root_hash"
    if batch_ordinals is not None:
        if (
            type(batch_ordinals) is not tuple
            or len(batch_ordinals) != key_count
            or any(type(batch_ordinal) is not int or not 0 <= batch_ordinal < 2**63 for batch_ordinal in batch_ordinals)
            or tuple(sorted(batch_ordinals)) != batch_ordinals
        ):
            raise ValueError("SOURCE root original batch ordinals differ")
        parameters["batch_ordinals"] = list(batch_ordinals)
        input_sql = (
            "WITH input AS MATERIALIZED (SELECT * FROM unnest("
            "CAST(:root_keys AS text[]),CAST(:root_hashes AS bytea[]),CAST(:batch_ordinals AS bigint[])) "
            "WITH ORDINALITY AS i(root_key,root_hash,batch_ordinal,ordinality)) "
        )
        # Preserve an INSERT attempt for a repeated key in EACH original batch.
        grouping += ",batch_ordinal"
    return input_sql, grouping


async def intern_root_records(
    session, definition, dataset_id, keys, *, maximum_rows, maximum_bytes, batch_ordinals=None
):
    """Intern ALL typed SOURCE keys, even rejected roots and orphan parents.

    SOURCE's root dictionary does not require an admitted entity. Binding an
    entity here would reject existing orphan inputs or create unused bindings.
    First occurrence order preserves the existing dictionary allocation order.
    """
    parameters = _root_parameters(definition, dataset_id, keys, maximum_rows, maximum_bytes)
    if not keys:
        return ()
    input_sql, grouping = _source_root_input(parameters, batch_ordinals, len(keys))
    quote, schema = await _control_schema(session)
    root = f"{quote.quote_schema(schema)}.{quote.quote('custom_import_root_record')}"
    await _execute(
        session,
        input_sql + f"INSERT INTO {root}(dataset_id,key_contract_sha256,canonical_logical_key,logical_key_sha256) "
        "SELECT :dataset_id,:key_contract,root_key,root_hash FROM input GROUP BY " + grouping + " "
        "ORDER BY min(ordinality) ON CONFLICT(dataset_id,key_contract_sha256,logical_key_sha256) DO NOTHING",
        parameters,
    )
    root_result = await _execute(
        session,
        input_sql + "SELECT i.ordinality,r.root_record_id,"
        "(r.dataset_id=:dataset_id AND r.key_contract_sha256=:key_contract AND r.logical_key_sha256=i.root_hash "
        "AND convert_to(r.canonical_logical_key,'UTF8')=convert_to(i.root_key,'UTF8')) AS valid "
        f"FROM input i LEFT JOIN {root} r ON r.dataset_id=:dataset_id "
        "AND r.key_contract_sha256=:key_contract AND r.logical_key_sha256=i.root_hash ORDER BY i.ordinality",
        parameters,
    )
    identity_rows, identities = root_result.mappings().all(), []
    if len(identity_rows) != len(keys):
        raise FamilyBlockUnavailable("root dictionary result census differs")
    root_id_by_key = {}
    for ordinal, (key, identity_row) in enumerate(zip(keys, identity_rows, strict=True), 1):
        identity = identity_row["root_record_id"]
        if (
            type(identity_row["ordinality"]) is not int
            or identity_row["ordinality"] != ordinal
            or identity_row["valid"] is not True
            or type(identity) is not int
            or not 0 < identity < 2**63
            or root_id_by_key.setdefault(key, identity) != identity
        ):
            raise FamilyBlockUnavailable("root dictionary scope, bytes or identity differs")
        identities.append(identity)
    return tuple(identities)
