# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retain one committed CMS source/address pair behind native closed custody."""

import hashlib
import json
import re
from dataclasses import asdict, dataclass, fields, replace
from uuid import UUID

from sqlalchemy import text

from process import provider_directory_cms_serving_receipt as serving
from process.entity_address_snapshot_ownership import (
    capture_created_entity_address_archive_stage,
    cleanup_entity_address_archive_stage,
    validate_entity_address_archive_stage_ownership,
)
from process.entity_address_snapshot_receipt import (
    _models,
    _normalize_receipt_session,
    validate_entity_address_archive_receipt,
)
from process.entity_address_snapshot_receipt import (
    capture_entity_address_archive_receipt as capture_entity_address_archive_receipt,
)
from process.entity_address_snapshot_source import (
    entity_address_archive_stage_schema,
    export_entity_address_archive_stage,
)
from process.network_address_projection import PinnedAddressSource, _identifier
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.network_cms_registry_address_capture import (
    NativeReceiptSession,
    address_clone_identity,
    freeze_address_clone,
    native_driver,
)
from process.network_cms_registry_address_equivalence import (
    capture_registry_cms_address_copy_source,
    decode_registry_cms_address_copy_witness,
    require_registry_cms_address_copy_witness,
    validate_registry_cms_address_copy,
)
from process.network_fhir_membership_source import PinnedFHIRMembershipSource
from process.network_fhir_source_epoch import (
    capture_retained_cms_fhir_source_epoch,
    require_retained_cms_fhir_source_epoch,
)
from process.registry_source_recipe_store import (
    RegistrySourceMembershipRecipe,
    canonical_registry_source_recipes,
    decode_registry_source_recipes,
)
from process.uhc_flex_practitioner_async_safety import drain_operation

_CMS_FIELDS = (
    "source_id",
    "endpoint_id",
    "dataset_id",
    "dataset_hash",
    "acquisition_root_run_id",
    "release_id",
    "proof_version",
)
_COMMENT = "registry-cms-source-pair-v1:"
_COMMENT_V2 = "registry-cms-source-pair-v2:"
_STAGE_COMMENT = "registry-cms-source-stage-v1:"


def _json(document):
    return json.dumps(document, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _digest(document):
    return hashlib.sha256(_json(document).encode()).hexdigest()


@dataclass(frozen=True)
class RegistryCMSPublicationProof:
    """Independently authenticated acquisition and Profile inputs, never browser authority."""

    source_pin: PinnedFHIRMembershipSource
    binding_coordinates: RegistryNetworkSourceCoordinates
    serving_receipt_id: str
    cms_authority: tuple
    publication_xid: str
    profile_generation_id: str
    selection_proof_id: str
    expected_admission_sha256: str
    expected_metadata_sha256: str

    def __post_init__(self):
        pin = self.source_pin
        if (
            type(pin) is not PinnedFHIRMembershipSource
            or pin.source_id != "cms-npd"
            or pin.retained_epoch is not None
            or pin.custody_owner_role is not None
        ):
            raise ValueError("registry_cms_publication_proof_invalid")
        RegistrySourceMembershipRecipe(pin, self.binding_coordinates)
        if (
            type(self.cms_authority) is not tuple
            or len(self.cms_authority) != 7
            or self.cms_authority[:4] != (pin.source_id, pin.endpoint_id, pin.dataset_id, pin.dataset_sha256)
            or self.cms_authority[5] != pin.release_id
            or type(self.cms_authority[6]) is not int
            or self.cms_authority[6] != 2
        ):
            raise ValueError("registry_cms_publication_proof_invalid")
        if any(
            type(field_value) is not str or re.fullmatch(r"[0-9a-f]{64}", field_value) is None
            for field_value in (
                self.serving_receipt_id,
                self.selection_proof_id,
                self.expected_admission_sha256,
                self.expected_metadata_sha256,
            )
        ):
            raise ValueError("registry_cms_publication_proof_invalid")
        if (
            type(self.publication_xid) is not str
            or re.fullmatch(r"[1-9][0-9]{0,19}", self.publication_xid) is None
            or type(self.profile_generation_id) is not str
            or re.fullmatch(r"pdprofile_[0-9a-f]{32}", self.profile_generation_id) is None
            or type(self.cms_authority[4]) is not str
            or not 1 <= len(self.cms_authority[4]) <= 64
        ):
            raise ValueError("registry_cms_publication_proof_invalid")

    def as_dict(self):
        """Return exact authenticated publication inputs as a closed document."""
        document = asdict(self)
        document["source_pin"] = self.source_pin.coordinates
        document["cms_authority"] = list(self.cms_authority)
        return document


@dataclass(frozen=True)
class RegistryCMSRetainedSourcePair:
    proof: RegistryCMSPublicationProof
    recipe: RegistrySourceMembershipRecipe
    address_source: PinnedAddressSource
    address_ownership: object
    address_receipt: object
    address_catalog_sha256: str
    cms_receipt_payload: str
    owner_role: str
    runtime_roles: tuple[str, ...]
    witness_json: str | None = None

    def __post_init__(self):
        epoch = self.recipe.source_pin.retained_epoch
        receipt_payload = serving.validate_receipt_payload(json.loads(self.cms_receipt_payload))
        if (
            epoch is None
            or replace(self.recipe.source_pin, retained_epoch=None) != self.proof.source_pin
            or self.recipe.binding_coordinates != self.proof.binding_coordinates
            or self.address_source.schema_name != self.address_ownership.schema_name
            or self.address_source.table_name != "entity_address_unified"
            or self.address_source.generation_id != _digest(receipt_payload["address"])
            or str(self.address_ownership.dataset_id) != str(epoch.epoch_id)
            or self.owner_role != epoch.owner_role
            or self.runtime_roles != epoch.runtime_roles
            or tuple(receipt_payload["cms"][field] for field in _CMS_FIELDS) != self.proof.cms_authority
            or receipt_payload["selection"]["proof_id"] != self.proof.selection_proof_id
            or receipt_payload["profile"]["profile_as_of"] != self.proof.source_pin.as_of
        ):
            raise ValueError("registry_cms_source_pair_invalid")
        if self.witness_json is not None:
            _bound_copy_witness(self, receipt_payload)

    def as_dict(self):
        """Return the complete bounded checkpoint without database handles."""
        pair_by_field = {
            "proof": self.proof.as_dict(),
            "recipes": json.loads(canonical_registry_source_recipes((self.recipe,))),
            "address_source": asdict(self.address_source),
            "address_ownership": self.address_ownership.as_dict(),
            "address_receipt": self.address_receipt.as_dict(),
            "address_catalog_sha256": self.address_catalog_sha256,
            "cms_receipt_payload": json.loads(self.cms_receipt_payload),
            "owner_role": self.owner_role,
            "runtime_roles": list(self.runtime_roles),
        }
        if self.witness_json is not None:
            pair_by_field["address_copy_witness"] = json.loads(self.witness_json)
        return pair_by_field

    @property
    def pair_sha256(self):
        """Bind every retained custody and publication field in the checkpoint."""
        return _digest(self.as_dict())


async def _publication(session, proof, *, current):
    namespace = _identifier(proof.source_pin.schema_name)
    receipt_row = (
        (
            await session.execute(
                text(
                    f"SELECT receipt_id,payload,publication_xid::text AS publication_xid,profile_generation_id FROM {namespace}.provider_directory_cms_serving_receipt WHERE receipt_id=:receipt AND publication_xid IS DISTINCT FROM pg_current_xact_id_if_assigned()"
                ),
                {"receipt": proof.serving_receipt_id},
            )
        )
        .mappings()
        .one_or_none()
    )
    if receipt_row is None:
        raise ValueError("registry_cms_publication_unavailable")
    receipt_payload = serving.validate_receipt_payload(receipt_row["payload"])
    if (
        tuple(receipt_payload["cms"][field] for field in _CMS_FIELDS) != proof.cms_authority
        or receipt_row["publication_xid"] != proof.publication_xid
        or receipt_row["profile_generation_id"] != proof.profile_generation_id
        or receipt_payload["selection"]["proof_id"] != proof.selection_proof_id
        or receipt_payload["profile"]["profile_as_of"] != proof.source_pin.as_of
    ):
        raise ValueError("registry_cms_publication_unavailable")
    if current and await serving.read_serving_receipt(session, proof.source_pin.schema_name) != {
        "receipt_id": proof.serving_receipt_id,
        "payload": receipt_payload,
    }:
        raise ValueError("registry_cms_publication_unavailable")
    return receipt_payload


async def _sealed_scope(connection, proof):
    """Bind all source coordinates to immutable native admission metadata."""
    pin = proof.source_pin
    receipt_row = await connection.fetchrow(
        f"SELECT acquisition_root_run_id,publication_metadata_summary_json FROM {_identifier(pin.schema_name)}.provider_directory_endpoint_dataset WHERE dataset_id=$1",
        pin.dataset_id,
    )
    metadata = receipt_row["publication_metadata_summary_json"] if receipt_row is not None else None
    if isinstance(metadata, str):
        metadata = json.loads(metadata)
    binding_declaration_by_field = {
        **asdict(proof.binding_coordinates),
        "source_key_kind": "organization_resource_id",
        "alias_scope": pin.alias_scope,
    }
    if (
        receipt_row is None
        or receipt_row["acquisition_root_run_id"] != proof.cms_authority[4]
        or type(metadata) is not dict
        or metadata.get("network_bindings") != binding_declaration_by_field
        or metadata.get("semantic_projection_as_of") != pin.as_of
    ):
        raise ValueError("registry_cms_publication_unavailable")


async def require_registry_cms_source_pair(session, pair, *, verify_content=True):
    """Verify retained custody independently of later origin cutovers."""
    if type(pair) is not RegistryCMSRetainedSourcePair or type(verify_content) is not bool:
        raise ValueError("registry_cms_source_pair_invalid")
    await _normalize_receipt_session(session, pair.address_ownership.schema_name)
    connection = await native_driver(session)
    await require_retained_cms_fhir_source_epoch(
        connection, pair.proof.source_pin, pair.recipe.source_pin.retained_epoch, verify_content=verify_content
    )
    receipt, catalog = await address_clone_identity(
        session, pair.address_ownership, pair.owner_role, pair.runtime_roles, verify_content=verify_content
    )
    comment = await connection.fetchval(
        "SELECT obj_description($1::oid,'pg_namespace')", pair.address_ownership.schema_oid
    )
    if (
        (verify_content and receipt != pair.address_receipt)
        or catalog != pair.address_catalog_sha256
        or comment != _pair_comment(pair) + _json(pair.as_dict())
    ):
        raise ValueError("registry_cms_source_pair_invalid")
    if pair.witness_json is not None:
        if await _publication(session, pair.proof, current=False) != json.loads(pair.cms_receipt_payload):
            raise ValueError("registry_cms_source_pair_invalid")
        await require_registry_cms_address_copy_witness(
            session,
            json.loads(pair.witness_json),
            clone_ownership=pair.address_ownership,
            verify_content=False,
        )
    return pair


async def require_registry_cms_source_pair_connection(connection, pair, *, verify_content=True):
    """Validate on the exact native publisher transaction without opening another."""
    return await require_registry_cms_source_pair(NativeReceiptSession(connection), pair, verify_content=verify_content)


def decode_registry_cms_source_pair(document):
    """Decode a bounded closed checkpoint; native revalidation remains mandatory."""
    keys = {field.name for field in fields(RegistryCMSRetainedSourcePair)} - {"recipe", "witness_json"} | {"recipes"}
    if type(document) is dict and "address_copy_witness" in document:
        keys |= {"address_copy_witness"}
    if type(document) is not dict or set(document) != keys or len(_json(document).encode()) > 65536:
        raise ValueError("registry_cms_source_pair_invalid")
    proof_document = document["proof"]
    if type(proof_document) is not dict or set(proof_document) != {
        field.name for field in fields(RegistryCMSPublicationProof)
    }:
        raise ValueError("registry_cms_source_pair_invalid")
    proof = RegistryCMSPublicationProof(
        **{
            key: field_value
            for key, field_value in proof_document.items()
            if key not in {"source_pin", "binding_coordinates", "cms_authority"}
        },
        source_pin=PinnedFHIRMembershipSource(**proof_document["source_pin"]),
        binding_coordinates=RegistryNetworkSourceCoordinates(**proof_document["binding_coordinates"]),
        cms_authority=tuple(proof_document["cms_authority"]),
    )
    recipes = decode_registry_source_recipes(_json(document["recipes"]))
    if len(recipes) != 1 or type(document["runtime_roles"]) is not list:
        raise ValueError("registry_cms_source_pair_invalid")
    pair = RegistryCMSRetainedSourcePair(
        proof,
        recipes[0],
        PinnedAddressSource(**document["address_source"]),
        validate_entity_address_archive_stage_ownership(document["address_ownership"]),
        validate_entity_address_archive_receipt(document["address_receipt"]),
        document["address_catalog_sha256"],
        _json(serving.validate_receipt_payload(document["cms_receipt_payload"])),
        document["owner_role"],
        tuple(document["runtime_roles"]),
        decode_registry_cms_address_copy_witness(document["address_copy_witness"])
        if "address_copy_witness" in document
        else None,
    )
    if pair.as_dict() != document:
        raise ValueError("registry_cms_source_pair_invalid")
    return pair


async def _retained_pair(session, proof, schema, owner_role, runtime_roles):
    comment = await session.scalar(
        text("SELECT obj_description(oid,'pg_namespace') FROM pg_namespace WHERE nspname=:schema"), {"schema": schema}
    )
    if comment is None:
        return None
    if comment.startswith(_STAGE_COMMENT):
        stage = json.loads(comment[len(_STAGE_COMMENT) :])
        if (
            set(stage) != {"proof", "owner_role", "runtime_roles", "ownership"}
            or stage["proof"] != proof.as_dict()
            or stage["owner_role"] != owner_role
            or stage["runtime_roles"] != list(runtime_roles)
        ):
            raise ValueError("registry_cms_source_pair_invalid")
        ownership = validate_entity_address_archive_stage_ownership(stage["ownership"])
        if ownership.schema_name != schema:
            raise ValueError("registry_cms_source_pair_invalid")
        await cleanup_entity_address_archive_stage(session, owner=ownership)
        return None
    prefix = next((prefix for prefix in (_COMMENT, _COMMENT_V2) if comment.startswith(prefix)), None)
    if prefix is None:
        raise ValueError("registry_cms_source_pair_invalid")
    document = json.loads(comment[len(prefix) :])
    if (
        document["proof"] != proof.as_dict()
        or document["owner_role"] != owner_role
        or document["runtime_roles"] != list(runtime_roles)
    ):
        raise ValueError("registry_cms_source_pair_invalid")
    pair = decode_registry_cms_source_pair(document)
    await require_registry_cms_source_pair(session, pair)
    return pair


async def capture_registry_cms_source_pair(session_factory, proof, *, capture_id, owner_role, runtime_roles):
    """Retain a pinned complete pair; caller authenticates dispatch and Profile ancestry."""
    if (
        type(proof) is not RegistryCMSPublicationProof
        or type(capture_id) is not UUID
        or not capture_id.int
        or type(runtime_roles) is not tuple
        or not 1 <= len(runtime_roles) <= 64
        or tuple(sorted(set(runtime_roles))) != runtime_roles
        or owner_role in runtime_roles
    ):
        raise ValueError("registry_cms_source_pair_invalid")
    for role in (owner_role, *runtime_roles):
        _identifier(role)
    async with session_factory() as session, session.begin():
        await session.execute(
            text("SELECT pg_advisory_xact_lock(:key)"),
            {"key": int.from_bytes(capture_id.bytes[:8], "big", signed=True)},
        )
        return await _capture_pair(session_factory, proof, capture_id, owner_role, runtime_roles)


async def _capture_pair(session_factory, proof, capture_id, owner_role, runtime_roles):
    """Serialize deterministic replay and recover only an exact stamped unfinished clone."""
    schema = entity_address_archive_stage_schema(capture_id)
    async with session_factory() as session, session.begin():
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
        retained = await _retained_pair(session, proof, schema, owner_role, runtime_roles)
        if retained is not None:
            return retained
    ownership, receipt_payload, source_capture = [], [], []

    async def captured(session, _capture):
        """Pin the actual committed native receipt before any address copy."""
        receipt_payload.append(await _publication(session, proof, current=True))
        native = await serving.capture_native_dependencies(session, proof.source_pin.schema_name, lock=True)
        if native["address"] != receipt_payload[0]["address"]:
            raise ValueError("registry_cms_address_authority_changed")
        source_capture.append(
            await capture_registry_cms_address_copy_source(
                session,
                source_schema=proof.source_pin.schema_name,
                expected_relation_oids=_address_oid_vector(receipt_payload[0]),
            )
        )

    async def created(session):
        """Persist exact clone OIDs atomically for safe interrupted-capture recovery."""
        ownership.append(await capture_created_entity_address_archive_stage(session, dataset_id=capture_id))
        await _stamp_stage(session, schema, proof, owner_role, runtime_roles, ownership[0])

    async def copied(_capture):
        """Retain the pinned native clone without a portable dump."""
        return None

    try:
        await export_entity_address_archive_stage(
            session_factory,
            schema_name=proof.source_pin.schema_name,
            dataset_id=capture_id,
            archive_copy=copied,
            stage_created=created,
            source_captured=captured,
        )
        return await _freeze_pair(
            session_factory,
            proof,
            capture_id,
            owner_role,
            runtime_roles,
            ownership[0],
            source_capture[0],
            receipt_payload[0],
        )
    except BaseException:
        if ownership:
            await drain_operation(_cleanup_capture(session_factory, ownership[0]), preserve_cancellation=True)
        raise


async def _stamp_stage(session, schema, proof, owner_role, runtime_roles, ownership):
    """Bind interrupted clone cleanup to its exact native OIDs and requested scope."""
    stage_by_field = {
        "proof": proof.as_dict(),
        "owner_role": owner_role,
        "runtime_roles": list(runtime_roles),
        "ownership": ownership.as_dict(),
    }
    await (await native_driver(session)).execute(
        f"COMMENT ON SCHEMA {_identifier(schema)} IS '"
        + (_STAGE_COMMENT + _json(stage_by_field)).replace("'", "''")
        + "'"
    )


async def _cleanup_capture(session_factory, ownership):
    """Clean only the exact clone; a committed pair survives lost acknowledgement."""
    async with session_factory() as session, session.begin():
        comment = await session.scalar(
            text("SELECT obj_description(oid,'pg_namespace') FROM pg_namespace WHERE oid=:oid"),
            {"oid": ownership.schema_oid},
        )
        if comment is None or not comment.startswith((_COMMENT, _COMMENT_V2)):
            await cleanup_entity_address_archive_stage(session, owner=ownership)


async def _freeze_pair(
    session_factory,
    proof,
    capture_id,
    owner_role,
    runtime_roles,
    ownership,
    source_capture,
    receipt_payload,
):
    """Commit the independently admitted epoch and immutable address correspondence together."""
    schema = ownership.schema_name
    async with session_factory() as session, session.begin():
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
        await _normalize_receipt_session(session, schema)
        connection = await native_driver(session)
        await _sealed_scope(connection, proof)
        epoch = await capture_retained_cms_fhir_source_epoch(
            connection,
            proof.source_pin,
            epoch_id=capture_id,
            owner_role=owner_role,
            runtime_roles=runtime_roles,
            expected_admission_sha256=proof.expected_admission_sha256,
            expected_metadata_sha256=proof.expected_metadata_sha256,
        )
        receipt, catalog = await freeze_address_clone(session, ownership, owner_role, runtime_roles)
        witness_json = None
        if receipt != source_capture.semantic_receipt:
            witness = await validate_registry_cms_address_copy(
                session, source_capture=source_capture, clone_ownership=ownership
            )
            witness_json = decode_registry_cms_address_copy_witness(json.loads(_json(witness.as_dict())))
        recipe = RegistrySourceMembershipRecipe(
            replace(proof.source_pin, retained_epoch=epoch), proof.binding_coordinates
        )
        pair = RegistryCMSRetainedSourcePair(
            proof,
            recipe,
            PinnedAddressSource(schema, "entity_address_unified", _digest(receipt_payload["address"])),
            ownership,
            receipt,
            catalog,
            _json(receipt_payload),
            owner_role,
            runtime_roles,
            witness_json,
        )
        if len(_json(pair.as_dict()).encode()) > 65536:
            raise ValueError("registry_cms_source_pair_invalid")
        await connection.execute(
            f"COMMENT ON SCHEMA {_identifier(schema)} IS '"
            + (_pair_comment(pair) + _json(pair.as_dict())).replace("'", "''")
            + "'"
        )
        await require_registry_cms_source_pair(session, pair)
    return pair


def _pair_comment(pair):
    return _COMMENT if pair.witness_json is None else _COMMENT_V2


def _address_oid_vector(payload):
    names = tuple(model.__tablename__ for model in _models())
    oids = payload["address"]["relation_oids"]
    if type(oids) is not list or len(oids) != len(names):
        raise ValueError("registry_cms_address_authority_changed")
    return tuple(sorted(zip(names, oids, strict=True)))


def _bound_copy_witness(pair, receipt_payload):
    if type(pair.witness_json) is not str:
        raise ValueError("registry_cms_source_pair_invalid")
    document = json.loads(pair.witness_json)
    if (
        decode_registry_cms_address_copy_witness(document) != pair.witness_json
        or document["source"]["schema_name"] != pair.proof.source_pin.schema_name
        or document["source"]["relation_oids"] != [list(entry) for entry in _address_oid_vector(receipt_payload)]
        or document["clone"]["schema_name"] != pair.address_ownership.schema_name
        or document["clone"]["schema_oid"] != pair.address_ownership.schema_oid
        or document["clone"]["relation_oids"] != [list(entry) for entry in pair.address_ownership.relation_oids]
        or document["clone"]["receipt"] != pair.address_receipt.as_dict()
    ):
        raise ValueError("registry_cms_source_pair_invalid")
