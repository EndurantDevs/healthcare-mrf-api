# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native custody checks for explicitly authorized terminal-owner cleanup.

Callers must authenticate the terminal run/attempt and stopped worker, and hold
the shared capture lock before opening this transaction. Neither an expired
lease nor a capture UUID authorizes deletion. This module establishes physical
custody only; it does not infer execution ownership from an untagged v1 stamp.
"""

import json
import re
from dataclasses import fields, replace
from uuid import UUID

from process.entity_address_result_generation import RELATION_NAMES
from process.entity_address_snapshot_ownership import (
    EntityAddressArchiveOwnershipError,
    cleanup_entity_address_archive_stage,
    validate_entity_address_archive_stage_ownership,
)
from process.entity_address_snapshot_receipt import (
    EntityAddressArchiveReceiptError,
    validate_entity_address_archive_receipt,
)
from process.network_address_projection import _identifier
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.network_cms_registry_address_capture import native_driver
from process.network_cms_registry_source_pair import (
    _COMMENT,
    _COMMENT_V2,
    _json,
    decode_registry_cms_source_pair,
    require_registry_cms_source_pair,
)
from process.network_custom_address_source import _require_transaction
from process.network_fhir_membership_source import PinnedFHIRMembershipSource
from process.network_registry_cms_prepared_pair import (
    _PREPARED_COMMENT,
    _PREPARED_COMMENT_V2,
    PreparedRegistryCMSSourcePair,
    RegistryCMSRetentionRequest,
    _require_prepared_pair,
    _validate_preparation_request,
)
from process.network_registry_cms_source_attempt import RegistryCMSSourceAttempt
from process.registry_source_recipe_store import decode_registry_source_recipes

MAX_DOCUMENT_BYTES = 65536


def _invalid():
    raise ValueError("registry_cms_prepared_capture_invalid")


def _unique_object(entries):
    object_by_key = {}
    for key, value in entries:
        if key in object_by_key:
            _invalid()
        object_by_key[key] = value
    return object_by_key


def _document(comment, prefix):
    if type(comment) is not str or not comment.startswith(prefix):
        _invalid()
    encoded = comment[len(prefix) :]
    if len(encoded.encode()) > MAX_DOCUMENT_BYTES:
        _invalid()
    return json.loads(encoded, object_pairs_hook=_unique_object, parse_constant=lambda _value: _invalid())


def _closed(document, model):
    if type(document) is not dict or set(document) != {entry.name for entry in fields(model)}:
        _invalid()
    return document


def _request(document):
    request_by_field = dict(_closed(document, RegistryCMSRetentionRequest))
    pin_by_field = dict(_closed(request_by_field.pop("source_pin"), PinnedFHIRMembershipSource))
    if type(pin_by_field["custody_runtime_roles"]) is not list or pin_by_field["retained_epoch"] is not None:
        _invalid()
    pin_by_field["custody_runtime_roles"] = tuple(pin_by_field["custody_runtime_roles"])
    coordinates = _closed(request_by_field.pop("binding_coordinates"), RegistryNetworkSourceCoordinates)
    return RegistryCMSRetentionRequest(
        PinnedFHIRMembershipSource(**pin_by_field), RegistryNetworkSourceCoordinates(**coordinates), **request_by_field
    )


def _stages(entries):
    if type(entries) is not list or len(entries) != len(RELATION_NAMES):
        _invalid()
    for entry in entries:
        if type(entry) is not list or len(entry) != 3 or type(entry[2]) is not int or not 0 < entry[2] < 2**32:
            _invalid()
        _identifier(entry[0])
        _identifier(entry[1])
    stages = tuple(tuple(entry) for entry in entries)
    if (
        stages != tuple(sorted(stages))
        or {entry[0] for entry in stages} != set(RELATION_NAMES)
        or len({entry[1] for entry in stages}) != len(stages)
        or len({entry[2] for entry in stages}) != len(stages)
    ):
        _invalid()
    return stages


def _require_pair_scope(pair):
    epoch = pair.recipe.source_pin.retained_epoch
    if epoch is None:
        _invalid()
    _validate_preparation_request(pair.request, epoch.epoch_id, pair.owner_role, pair.runtime_roles)
    if (
        len(pair.runtime_roles) > 64
        or replace(pair.recipe.source_pin, retained_epoch=None) != pair.request.source_pin
        or pair.recipe.binding_coordinates != pair.request.binding_coordinates
        or pair.address_ownership.dataset_id != epoch.epoch_id
        or not 0 < pair.address_ownership.schema_oid < 2**32
        or (pair.owner_role, pair.runtime_roles) != (epoch.owner_role, epoch.runtime_roles)
        or pair.request.expected_admission_sha256 != epoch.admission_sha256
        or pair.request.expected_metadata_sha256 != epoch.metadata_sha256
    ):
        _invalid()
    for digest in (
        pair.address_catalog_sha256,
        pair.capacity_geometry_hash,
        pair.lease_digest,
        pair.native_address_input_hash,
    ):
        if type(digest) is not str or re.fullmatch(r"[0-9a-f]{64}", digest) is None:
            _invalid()
    oids = [oid for _name, oid in epoch.relation_oids + pair.address_ownership.relation_oids]
    oids += [entry[2] for entry in pair.address_stages]
    if any(type(oid) is not int or not 0 < oid < 2**32 for oid in oids) or len(oids) != len(set(oids)):
        _invalid()


def decode_prepared_registry_cms_source_pair(comment):
    """Decode a bounded, closed preparation stamp; confer no cleanup authority."""
    try:
        is_tagged = type(comment) is str and comment.startswith(_PREPARED_COMMENT_V2)
        document = _document(comment, _PREPARED_COMMENT_V2 if is_tagged else _PREPARED_COMMENT)
        expected_fields = {entry.name for entry in fields(PreparedRegistryCMSSourcePair)}
        if not is_tagged:
            expected_fields.remove("source_attempt")
        if type(document) is not dict or set(document) != expected_fields:
            _invalid()
        recipes = decode_registry_source_recipes([document["recipe"]])
        if len(recipes) != 1 or type(document["runtime_roles"]) is not list:
            _invalid()
        pair = PreparedRegistryCMSSourcePair(
            _request(document["request"]),
            recipes[0],
            validate_entity_address_archive_stage_ownership(document["address_ownership"]),
            validate_entity_address_archive_receipt(document["address_receipt"]),
            document["address_catalog_sha256"],
            _stages(document["address_stages"]),
            document["owner_role"],
            tuple(document["runtime_roles"]),
            document["capacity_geometry_hash"],
            document["lease_digest"],
            document["native_address_input_hash"],
            RegistryCMSSourceAttempt.from_dict(document["source_attempt"]) if is_tagged else None,
        )
        _require_pair_scope(pair)
        if json.loads(_json(pair.as_dict())) != document:
            _invalid()
        return pair
    except (
        ValueError,
        TypeError,
        KeyError,
        AttributeError,
        UnicodeError,
        RecursionError,
        EntityAddressArchiveOwnershipError,
        EntityAddressArchiveReceiptError,
    ):
        raise ValueError("registry_cms_prepared_capture_invalid") from None


async def _caller_connection(session):
    if not session.in_transaction():
        raise ValueError("registry_cms_prepared_capture_requires_transaction")
    connection = await native_driver(session)
    await _require_transaction(connection)
    return connection


async def _require_native_custody(session, pair):
    try:
        if type(pair) is PreparedRegistryCMSSourcePair:
            await _require_prepared_pair(session, pair)
        else:
            await require_registry_cms_source_pair(session, pair, verify_content=False)
    except EntityAddressArchiveOwnershipError, EntityAddressArchiveReceiptError:
        raise ValueError("registry_cms_prepared_capture_invalid") from None


async def probe_registry_cms_source_capture(session, *, capture_id, owner_role, runtime_roles):
    """Return absent, prepared or finalized custody on the caller's fresh snapshot."""
    if type(capture_id) is not UUID or not capture_id.int:
        _invalid()
    if (
        type(runtime_roles) is not tuple
        or not 1 <= len(runtime_roles) <= 64
        or tuple(sorted(set(runtime_roles))) != runtime_roles
        or owner_role in runtime_roles
    ):
        _invalid()
    for role in (owner_role, *runtime_roles):
        _identifier(role)
    connection = await _caller_connection(session)
    schemas = ("entity_address_archive_" + capture_id.hex, "registry_cms_epoch_" + capture_id.hex)
    schema_records = await connection.fetch(
        """SELECT n.nspname, n.oid::bigint, r.rolname,
          CASE WHEN octet_length(obj_description(n.oid,'pg_namespace')) <= $2
          THEN obj_description(n.oid,'pg_namespace') END AS comment
          FROM pg_namespace n JOIN pg_roles r ON r.oid=n.nspowner
          WHERE n.nspname=ANY($1::text[]) ORDER BY n.nspname""",
        list(schemas),
        MAX_DOCUMENT_BYTES + len(_PREPARED_COMMENT),
    )
    if not schema_records:
        return None
    if len(schema_records) != 2 or any(
        schema_record["rolname"] != owner_role or type(schema_record["comment"]) is not str
        for schema_record in schema_records
    ):
        _invalid()
    comment = next(
        schema_record["comment"] for schema_record in schema_records if schema_record["nspname"] == schemas[0]
    )
    if comment.startswith((_PREPARED_COMMENT, _PREPARED_COMMENT_V2)):
        pair = decode_prepared_registry_cms_source_pair(comment)
    else:
        prefix = next(
            (comment_prefix for comment_prefix in (_COMMENT, _COMMENT_V2) if comment.startswith(comment_prefix)), None
        )
        if prefix is None:
            _invalid()
        pair = decode_registry_cms_source_pair(_document(comment, prefix))
    await _require_native_custody(session, pair)
    if pair.recipe.source_pin.retained_epoch.epoch_id != capture_id or (pair.owner_role, pair.runtime_roles) != (
        owner_role,
        runtime_roles,
    ):
        _invalid()
    return pair


async def cleanup_prepared_registry_cms_source_pair(session, prepared_pair):
    """Remove exact prepared copies only after private terminal-owner authorization.

    The caller must prove the authenticated owner/attempt terminal and its worker
    stopped, then hold the shared capture lock through this transaction. Native
    custody is necessary but does not replace those checks. Finalized pairs are
    preserved. Return the closed status: removed, absent or finalized.
    """
    if type(prepared_pair) is not PreparedRegistryCMSSourcePair:
        _invalid()
    _require_pair_scope(prepared_pair)
    connection = await _caller_connection(session)
    epoch = prepared_pair.recipe.source_pin.retained_epoch
    observed = await probe_registry_cms_source_capture(
        session,
        capture_id=epoch.epoch_id,
        owner_role=prepared_pair.owner_role,
        runtime_roles=prepared_pair.runtime_roles,
    )
    if observed is None:
        return "absent"
    if type(observed) is not PreparedRegistryCMSSourcePair:
        return "finalized"
    if _json(observed.as_dict()) != _json(prepared_pair.as_dict()):
        _invalid()
    async with connection.transaction():
        tables = [
            _identifier(schema) + "." + _identifier(name)
            for schema, relations in (
                (prepared_pair.address_ownership.schema_name, prepared_pair.address_ownership.relation_oids),
                (epoch.schema_name, epoch.relation_oids),
            )
            for name, _oid in relations
        ]
        await connection.execute("LOCK TABLE " + ",".join(sorted(tables)) + " IN ACCESS EXCLUSIVE MODE NOWAIT")
        await _require_prepared_pair(session, prepared_pair)
        await cleanup_entity_address_archive_stage(session, owner=prepared_pair.address_ownership)
        raw_tables = ",".join(
            _identifier(epoch.schema_name) + "." + _identifier(name) for name, _oid in epoch.relation_oids
        )
        await connection.execute("DROP TABLE " + raw_tables + " RESTRICT")
        await connection.execute("DROP SCHEMA " + _identifier(epoch.schema_name) + " RESTRICT")
    return "removed"
