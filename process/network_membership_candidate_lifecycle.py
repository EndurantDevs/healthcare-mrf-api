# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Atomic control of isolated membership candidates and bounded batch receipts."""

from __future__ import annotations

import hashlib
import json
import os
import re
from typing import Any
from uuid import UUID

from db.registry_schema import registry_schema
from process.network_membership_copy import (
    MAX_INPUT_BYTES,
    MembershipCopyError,
    MembershipCopyReceipt,
    MembershipCopyTarget,
    copy_network_membership_batch,
)


class MembershipCandidateError(MembershipCopyError):
    """Candidate identity, replay metadata or aggregate accounting was rejected."""


def _control_namespace(control_schema: str | None) -> str:
    schema_name = control_schema if control_schema is not None else registry_schema()
    if type(schema_name) is not str or re.fullmatch(r"[a-z_][a-z0-9_]{0,62}", schema_name) is None:
        raise MembershipCandidateError("Control schema must be a valid explicit identifier")
    return f'"{schema_name}"'


def _require_transaction(connection: Any, copy_target: MembershipCopyTarget) -> None:
    if type(copy_target) is not MembershipCopyTarget:
        raise MembershipCandidateError("Exact isolated candidate context is required")
    if not connection.is_in_transaction():
        raise MembershipCandidateError("Candidate operations require a caller-owned transaction")


def _nonnegative_bigint(counter: int) -> int:
    if type(counter) is not int or not 0 <= counter <= 9_223_372_036_854_775_807:
        raise MembershipCandidateError("Candidate revisions and counts must be nonnegative bigint values")
    return counter


def _source_json(source_generations: dict) -> str:
    if type(source_generations) is not dict:
        raise MembershipCandidateError("Source generations must be a JSON object")
    try:
        return json.dumps(source_generations, sort_keys=True, separators=(",", ":"), allow_nan=False)
    except TypeError, ValueError:
        raise MembershipCandidateError("Source generations must contain JSON values") from None


async def _locked_candidate(connection: Any, copy_target: MembershipCopyTarget, namespace: str) -> dict:
    candidate_record = await connection.fetchrow(
        f"SELECT * FROM {namespace}.network_membership_candidate WHERE candidate_id=$1 FOR UPDATE",
        UUID(copy_target.candidate_id),
    )
    if candidate_record is None:
        raise MembershipCandidateError("Candidate is not registered")
    for field in ("dataset_id", "schema_id", "producer_id", "candidate_id"):
        if str(candidate_record[field]) != getattr(copy_target, field):
            raise MembershipCandidateError("Candidate ownership scope does not match")
    if candidate_record["schema_name"] != copy_target.schema_name:
        raise MembershipCandidateError("Candidate relation does not match its registered scope")
    return dict(candidate_record)


def _require_open(candidate_record: dict) -> None:
    if candidate_record["state"] != "open":
        raise MembershipCandidateError("Candidate is closed to new membership batches")


async def require_candidate_authority(connection: Any, copy_target: MembershipCopyTarget) -> MembershipCopyTarget:
    """Verify complete candidate scope under the configured control row lock."""
    _require_transaction(connection, copy_target)
    async with connection.transaction():
        candidate_record = await _locked_candidate(connection, copy_target, _control_namespace(None))
        _require_open(candidate_record)
    return copy_target


async def _create_raw_membership(connection: Any, copy_target: MembershipCopyTarget) -> None:
    namespace = f'"{copy_target.schema_name}"'
    await connection.execute(f"CREATE SCHEMA {namespace}")
    await connection.execute(
        f"""CREATE TABLE {namespace}.network_membership (
        network_id INTEGER NOT NULL CHECK (network_id > 0),
        provider_system TEXT NOT NULL CHECK (provider_system IN ('npi','provider_directory','manual')),
        provider_id TEXT NOT NULL CHECK (btrim(provider_id) <> '' AND octet_length(provider_id) <= 1024),
        location_id UUID NOT NULL CHECK (location_id <> '00000000-0000-0000-0000-000000000000'::uuid),
        evidence_id TEXT NOT NULL CHECK (btrim(evidence_id) <> '' AND octet_length(evidence_id) <= 1024)
        )"""
    )


async def _insert_candidate(
    connection: Any, copy_target: MembershipCopyTarget, namespace: str, metadata: dict
) -> UUID | None:
    return await connection.fetchval(
        f"""INSERT INTO {namespace}.network_membership_candidate
        (candidate_id,dataset_id,schema_id,producer_id,schema_name,source_generations,
         approved_custom_revision,expected_head,expected_rows,source_recipes_json)
        VALUES($1,$2,$3,$4,$5,$6::jsonb,$7,$8,$9,$10::jsonb)
        ON CONFLICT(candidate_id) DO NOTHING RETURNING candidate_id""",
        UUID(copy_target.candidate_id),
        UUID(copy_target.dataset_id),
        UUID(copy_target.schema_id),
        UUID(copy_target.producer_id),
        copy_target.schema_name,
        metadata["source_generations"],
        metadata["approved_custom_revision"],
        metadata["expected_head"],
        metadata["expected_rows"],
        metadata["source_recipes_json"],
    )


def _candidate_source_metadata(source_generations, source_recipes):
    from process.registry_source_recipe_store import (
        RECIPE_DIGEST_KEY,
        canonical_registry_source_recipes,
        registry_source_recipes_sha256,
    )

    try:
        canonical_recipes = canonical_registry_source_recipes(source_recipes)
        generations = json.loads(_source_json(source_generations))
        digest = registry_source_recipes_sha256(canonical_recipes) if source_recipes else None
        if RECIPE_DIGEST_KEY in generations and generations[RECIPE_DIGEST_KEY] != digest:
            raise ValueError("Source recipe digest differs")
        if digest is not None:
            generations[RECIPE_DIGEST_KEY] = digest
        elif RECIPE_DIGEST_KEY in generations:
            raise ValueError("Empty recipes cannot declare a digest")
        return _source_json(generations), canonical_recipes
    except ValueError, TypeError:
        raise MembershipCandidateError("Candidate source recipes are invalid") from None


def _verify_candidate_metadata(candidate, metadata):
    from process.registry_source_recipe_store import canonical_registry_source_recipes, verify_registry_source_recipes

    try:
        stored_recipes = canonical_registry_source_recipes(verify_registry_source_recipes(candidate))
        stored_sources = candidate["source_generations"]
        if type(stored_sources) is str:
            stored_sources = json.loads(stored_sources)
        if (
            stored_recipes != metadata["source_recipes_json"]
            or _source_json(stored_sources) != metadata["source_generations"]
            or any(
                candidate[field] != metadata[field]
                for field in ("approved_custom_revision", "expected_head", "expected_rows")
            )
        ):
            raise ValueError("Metadata differs")
    except ValueError, TypeError:
        raise MembershipCandidateError("Candidate creation metadata is immutable") from None


async def create_network_candidate(
    connection: Any,
    copy_target: MembershipCopyTarget,
    *,
    source_generations: dict,
    approved_custom_revision: int,
    expected_head: int,
    expected_rows: int,
    control_schema: str | None = None,
    source_recipes: tuple = (),
) -> dict:
    """Create an isolated relation, or return an exact immutable metadata replay."""
    _require_transaction(connection, copy_target)
    namespace = _control_namespace(control_schema)
    source_json, recipe_json = _candidate_source_metadata(source_generations, source_recipes)
    metadata = {
        "source_generations": source_json,
        "source_recipes_json": recipe_json,
        "approved_custom_revision": _nonnegative_bigint(approved_custom_revision),
        "expected_head": _nonnegative_bigint(expected_head),
        "expected_rows": _nonnegative_bigint(expected_rows),
    }
    async with connection.transaction():
        inserted = await _insert_candidate(connection, copy_target, namespace, metadata)
        candidate_record = await _locked_candidate(connection, copy_target, namespace)
        _verify_candidate_metadata(candidate_record, metadata)
        if inserted:
            await _create_raw_membership(connection, copy_target)
        elif not await connection.fetchval(
            "SELECT to_regclass($1) IS NOT NULL", f'"{copy_target.schema_name}".network_membership'
        ):
            raise MembershipCandidateError("Registered candidate relation is missing")
    return candidate_record


def _validate_batch_input(batch_id: UUID, input_bytes: bytes, expected_input_sha256: str) -> None:
    if type(batch_id) is not UUID or batch_id.int == 0:
        raise MembershipCandidateError("Batch identity must be a nonzero UUID")
    if type(input_bytes) is not bytes or len(input_bytes) > MAX_INPUT_BYTES:
        raise MembershipCandidateError("Membership input must be bounded bytes")
    if type(expected_input_sha256) is not str or re.fullmatch(r"[0-9a-f]{64}", expected_input_sha256) is None:
        raise MembershipCandidateError("Expected input digest is invalid")
    if hashlib.sha256(input_bytes).hexdigest() != expected_input_sha256:
        raise MembershipCandidateError("Membership input digest mismatch")


def _replayed_receipt(
    copy_target: MembershipCopyTarget, batch_record: Any, input_bytes: bytes, input_digest: str
) -> MembershipCopyReceipt:
    if batch_record["input_sha256"] != input_digest or batch_record["input_bytes"] != len(input_bytes):
        raise MembershipCandidateError("Batch identity was already used for different input")
    return MembershipCopyReceipt(
        copy_target,
        batch_record["row_count"],
        batch_record["input_sha256"],
        batch_record["copy_sha256"],
        batch_record["input_bytes"],
        batch_record["copy_bytes"],
    )


async def _store_receipt(connection: Any, namespace: str, batch_id: UUID, receipt: MembershipCopyReceipt) -> None:
    candidate_id = UUID(receipt.target.candidate_id)
    await connection.execute(
        f"""INSERT INTO {namespace}.network_membership_batch
        (candidate_id,batch_id,row_count,input_sha256,copy_sha256,input_bytes,copy_bytes)
        VALUES($1,$2,$3,$4,$5,$6,$7)""",
        candidate_id,
        batch_id,
        receipt.row_count,
        receipt.input_sha256,
        receipt.copy_sha256,
        receipt.input_byte_count,
        receipt.copy_byte_count,
    )
    await connection.execute(
        f"UPDATE {namespace}.network_membership_candidate SET accepted_rows=accepted_rows+$2 WHERE candidate_id=$1",
        candidate_id,
        receipt.row_count,
    )


async def admit_network_membership_batch(
    connection: Any,
    copy_target: MembershipCopyTarget,
    *,
    batch_id: UUID,
    input_bytes: bytes,
    expected_input_sha256: str,
    control_schema: str | None = None,
) -> MembershipCopyReceipt:
    """Admit once under a control lock; replay never writes or advances counts."""
    _require_transaction(connection, copy_target)
    _validate_batch_input(batch_id, input_bytes, expected_input_sha256)
    namespace = _control_namespace(control_schema)
    async with connection.transaction():
        candidate_record = await _locked_candidate(connection, copy_target, namespace)
        batch_record = await connection.fetchrow(
            f"SELECT * FROM {namespace}.network_membership_batch WHERE candidate_id=$1 AND batch_id=$2",
            UUID(copy_target.candidate_id),
            batch_id,
        )
        if batch_record is not None:
            return _replayed_receipt(copy_target, batch_record, input_bytes, expected_input_sha256)
        _require_open(candidate_record)

        async def held_authority(
            locked_connection: Any, requested_target: MembershipCopyTarget
        ) -> MembershipCopyTarget:
            """Reuse the complete scope locked for this single admission transaction."""
            if locked_connection is not connection or requested_target != copy_target:
                raise MembershipCandidateError("Held candidate authority does not match")
            _require_open(candidate_record)
            return copy_target

        receipt = await copy_network_membership_batch(
            connection,
            copy_target=copy_target,
            input_bytes=input_bytes,
            expected_input_sha256=expected_input_sha256,
            require_candidate_authority=held_authority,
        )
        if candidate_record["accepted_rows"] + receipt.row_count > candidate_record["expected_rows"]:
            raise MembershipCandidateError("Membership batch exceeds the candidate's expected rows")
        await _store_receipt(connection, namespace, batch_id, receipt)
    return receipt


async def seal_network_candidate(
    connection: Any, copy_target: MembershipCopyTarget, *, control_schema: str | None = None
) -> dict:
    """Close new writes only after receipts, counter, expectation and raw rows agree."""
    _require_transaction(connection, copy_target)
    namespace = _control_namespace(control_schema)
    async with connection.transaction():
        candidate_record = await _locked_candidate(connection, copy_target, namespace)
        if candidate_record["state"] == "rejected":
            raise MembershipCandidateError("Rejected candidates cannot be sealed")
        batch_rows = await connection.fetchval(
            f"SELECT COALESCE(SUM(row_count),0) FROM {namespace}.network_membership_batch WHERE candidate_id=$1",
            UUID(copy_target.candidate_id),
        )
        membership_rows = await connection.fetchval(
            f'SELECT COUNT(*) FROM "{copy_target.schema_name}".network_membership'
        )
        if not batch_rows == candidate_record["accepted_rows"] == candidate_record["expected_rows"] == membership_rows:
            raise MembershipCandidateError("Candidate aggregate membership accounting does not reconcile")
        if candidate_record["state"] == "open":
            await connection.execute(
                f"UPDATE {namespace}.network_membership_candidate SET state='sealed' WHERE candidate_id=$1",
                UUID(copy_target.candidate_id),
            )
            candidate_record["state"] = "sealed"
    return candidate_record
