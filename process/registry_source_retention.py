# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Registered source memberships retained independently of effective output."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping

import asyncpg

from process.network_membership_copy import MembershipCopyTarget

SUMMARY_KEY = "registry_source_retention"
SOURCE_TABLE = "network_membership_source"
MAX_RECEIPT_BYTES = 4096
_COUNTERS = {"source_rows", "add_rows", "remove_rows", "suppressed_source_rows", "effective_rows"}
_FIELDS = _COUNTERS | {"component", "revision", "table_name", "source_generation_sha256"}


class RegistrySourceRetentionError(ValueError):
    """Source registration, native construction or retained accounting differs."""


def _canonical(receipt):
    return json.dumps(receipt, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _unique_object(pairs):
    fields_by_name = {}
    for name, value in pairs:
        if name in fields_by_name:
            raise RegistrySourceRetentionError("registry_source_retention_invalid")
        fields_by_name[name] = value
    return fields_by_name


def validate_registry_source_retention_receipt(receipt) -> dict:
    """Validate the closed provenance receipt; its digest is not a heap checksum."""
    try:
        if type(receipt) is str:
            if len(receipt.encode()) > MAX_RECEIPT_BYTES:
                raise ValueError
            receipt = json.loads(receipt, object_pairs_hook=_unique_object)
        if type(receipt) is not dict or set(receipt) != _FIELDS:
            raise ValueError
        if (
            type(receipt["component"]) is not str
            or receipt["component"] != SUMMARY_KEY
            or type(receipt["revision"]) is not int
            or receipt["revision"] != 1
            or type(receipt["table_name"]) is not str
            or receipt["table_name"] != SOURCE_TABLE
            or type(receipt["source_generation_sha256"]) is not str
            or re.fullmatch(r"[0-9a-f]{64}", receipt["source_generation_sha256"]) is None
            or any(type(receipt[field]) is not int or not 0 <= receipt[field] < 2**63 for field in _COUNTERS)
            or receipt["suppressed_source_rows"] > receipt["source_rows"]
            or receipt["effective_rows"]
            != receipt["source_rows"] - receipt["suppressed_source_rows"] + receipt["add_rows"]
            or len(_canonical(receipt).encode()) > MAX_RECEIPT_BYTES
        ):
            raise ValueError
        return dict(receipt)
    except ValueError, TypeError, KeyError, UnicodeError, RecursionError:
        raise RegistrySourceRetentionError("registry_source_retention_invalid") from None


def registry_source_retention_sha256(receipt) -> str:
    """Hash only the canonical closed provenance receipt."""
    return hashlib.sha256(_canonical(validate_registry_source_retention_receipt(receipt)).encode()).hexdigest()


def verify_registry_source_retention(candidate: Mapping) -> dict | None:
    """Require paired receipt/hash and exact effective count; legacy has neither."""
    try:
        if not isinstance(candidate, (Mapping, asyncpg.Record)):
            raise ValueError
        generations = candidate["source_generations"]
        report = candidate.get("validation_json")
        report = {} if report is None else report
        generations = json.loads(generations) if type(generations) is str else generations
        report = json.loads(report) if type(report) is str else report
        if type(generations) is not dict or type(report) is not dict:
            raise ValueError
        if SUMMARY_KEY not in generations and SUMMARY_KEY not in report:
            return None
        receipt = validate_registry_source_retention_receipt(report[SUMMARY_KEY])
        if (
            generations[SUMMARY_KEY] != registry_source_retention_sha256(receipt)
            or type(candidate["expected_rows"]) is not int
            or candidate["expected_rows"] != receipt["effective_rows"]
        ):
            raise ValueError
        return receipt
    except ValueError, TypeError, KeyError, UnicodeError, RecursionError:
        raise RegistrySourceRetentionError("registry_source_retention_invalid") from None


def _stage_relation(stage, receipt):
    from process.registry_source_recipe_composition import RegistrySourceRecipeStage

    if (
        type(stage) is not RegistrySourceRecipeStage
        or type(stage.table_name) is not str
        or re.fullmatch(r"registry_source_page_[0-9a-f]{32}", stage.table_name) is None
        or type(stage.membership_rows) is not int
        or stage.membership_rows != receipt["source_rows"]
        or stage.generation_sha256 != receipt["source_generation_sha256"]
    ):
        raise RegistrySourceRetentionError("registry_source_retention_stage_invalid")
    return f'pg_temp."{stage.table_name}"'


def _stage_projection(stage_relation):
    return f"""SELECT member.* FROM {stage_relation} page CROSS JOIN LATERAL
      jsonb_to_record(page.input_json::jsonb) AS member(network_id integer,provider_system text,
        provider_id text,location_id uuid,evidence_id text)"""


async def retain_registry_source_memberships(connection, copy_target, stage, *, control_schema=None):
    """Set-insert the verified page stage once; replay preserves duplicate evidence."""
    from process.network_membership_candidate_lifecycle import (
        _control_namespace,
        _locked_candidate,
        _require_open,
        _require_transaction,
    )

    _require_transaction(connection, copy_target)
    async with connection.transaction():
        candidate = await _locked_candidate(connection, copy_target, _control_namespace(control_schema))
        _require_open(candidate)
        receipt = verify_registry_source_retention(candidate)
        if receipt is None:
            raise RegistrySourceRetentionError("registry_source_retention_unregistered")
        stage_relation = _stage_relation(stage, receipt)
        source_relation = f'"{copy_target.schema_name}".{SOURCE_TABLE}'
        try:
            await connection.execute(f"LOCK TABLE {source_relation} IN ACCESS EXCLUSIVE MODE")
            counts = await connection.fetchrow(
                f"SELECT (SELECT count(*) FROM {source_relation}) AS source_rows,"
                f"(SELECT count(*) FROM {stage_relation}) AS stage_rows"
            )
            if counts["stage_rows"] != receipt["source_rows"]:
                raise RegistrySourceRetentionError("registry_source_retention_accounting_invalid")
            if counts["source_rows"] == 0:
                await connection.execute(f"INSERT INTO {source_relation} {_stage_projection(stage_relation)}")
            await verify_registry_source_retention_rows(connection, copy_target, candidate, source_stage=stage)
        except asyncpg.PostgresError as error:
            raise RegistrySourceRetentionError("registry_source_retention_source_invalid") from error
        return receipt


async def verify_registry_source_retention_rows(connection, copy_target, candidate, *, source_stage=None):
    """Check native source_relation counts and optional stage multiset parity before sealing."""
    receipt = verify_registry_source_retention(candidate)
    if receipt is None:
        if source_stage is not None:
            raise RegistrySourceRetentionError("registry_source_retention_unregistered")
        return None
    fields = ("dataset_id", "schema_id", "producer_id", "candidate_id", "schema_name")
    if (
        type(copy_target) is not MembershipCopyTarget
        or not connection.is_in_transaction()
        or any(str(candidate[field]) != getattr(copy_target, field) for field in fields)
    ):
        raise RegistrySourceRetentionError("registry_source_retention_scope_invalid")
    source_relation = f'"{copy_target.schema_name}".{SOURCE_TABLE}'
    if candidate["state"] == "open" and source_stage is None:
        raise RegistrySourceRetentionError("registry_source_retention_stage_required")
    try:
        await connection.execute(f"LOCK TABLE {source_relation} IN SHARE MODE")
        if await connection.fetchval(f"SELECT count(*) FROM {source_relation}") != receipt["source_rows"]:
            raise RegistrySourceRetentionError("registry_source_retention_accounting_invalid")
        if source_stage is not None:
            projection = _stage_projection(_stage_relation(source_stage, receipt))
            differs = await connection.fetchval(f"""WITH staged AS MATERIALIZED ({projection}), differences AS (
              (SELECT * FROM {source_relation} EXCEPT ALL SELECT * FROM staged)
              UNION ALL (SELECT * FROM staged EXCEPT ALL SELECT * FROM {source_relation}))
              SELECT EXISTS(SELECT 1 FROM differences)""")
            if differs:
                raise RegistrySourceRetentionError("registry_source_retention_parity_invalid")
    except asyncpg.PostgresError as error:
        raise RegistrySourceRetentionError("registry_source_retention_source_invalid") from error
    return receipt
