# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded canonical plan evidence for typed semantic network references."""

import json
import re
from collections import OrderedDict
from contextlib import asynccontextmanager
from uuid import uuid4

from process.network_address_projection import _identifier
from process.network_fhir_typed_contract import require
from process.registry_fhir_plan_tier import role_plan_tier, validate_binding_scope, validate_plan_scope
from process.uhc_flex_practitioner_async_safety import drain_operation

REFERENCE_PREFIX = "UHCPlanTier/"
_CONTRACT = "healthporta.uhc.provider-directory-canonical.v2"
_SELECTOR_FIELDS = ("plan_scope", "network_key_id", "network_tier", "insurance_plan_refs")
PLAN_EVIDENCE_DDL = (
    "resource_id text PRIMARY KEY,payload_sha256 text NOT NULL,declared_id text,contract text,"
    "plan_scope jsonb,network_refs jsonb"
)


def _payload(value):
    document = _decoded(value)
    require(type(document) is dict, "plan_tier_payload")
    return document


def _decoded(value):
    return json.loads(value) if type(value) is str else value


def _references(references):
    if references is None:
        references = []
    require(
        type(references) is list and all(type(reference) is str for reference in references), "plan_tier_references"
    )
    require(
        not any(reference.startswith(REFERENCE_PREFIX) for reference in references), "plan_tier_reference_namespace"
    )
    return tuple(dict.fromkeys(references))


def semantic_network_references(payload, plan_references):
    """The raw checker derives the typed key without consulting producer cells."""
    original = payload.get("network_refs")
    original = [] if original is None else original
    _references(original)
    selected = original + [reference for reference in plan_references if reference not in original]
    key = payload.get("network_key_id")
    if key is not None:
        require(type(key) is str and re.fullmatch(r"[0-9a-f]{64}", key), "plan_tier_key")
        selected.append(REFERENCE_PREFIX + key)
    return selected


class _PlanProjection:
    def __init__(self, connection, table):
        self.connection, self.table = connection, table
        self.cache = OrderedDict()

    async def _plans(self, page):
        records = []
        for row in page:
            if row["resource_type"] != "InsurancePlan":
                continue
            payload = _payload(row["payload_json"])
            plan = payload.get("plan_json")
            plan = {} if plan is None else plan
            require(type(plan) is dict, "plan_tier_plan")
            records.append(
                (
                    row["resource_id"],
                    row["payload_hash"],
                    payload.get("resource_id"),
                    plan.get("canonical_contract_id"),
                    json.dumps(plan.get("plan_key")),
                    json.dumps(_references(payload.get("network_refs"))),
                )
            )
        if records:
            await self.connection.copy_records_to_table(self.table, schema_name="pg_temp", records=records)

    async def prepare(self, page, source_rows):
        """Validate distinct role selectors against original plan evidence before adding references."""
        await self._plans(page)
        selections_by_resource, pending_by_selector = {}, {}
        for source_row in page:
            if source_row["resource_type"] not in {"PractitionerRole", "OrganizationAffiliation"}:
                continue
            role_payload = _payload(source_row["payload_json"])
            _references(role_payload.get("network_refs"))
            selector_by_field = {field: role_payload.get(field) for field in _SELECTOR_FIELDS}
            if not any(selector_value is not None for selector_value in selector_by_field.values()):
                continue
            encoded = json.dumps(selector_by_field, sort_keys=True, separators=(",", ":"))
            selections_by_resource[(source_row["resource_type"], source_row["resource_id"])] = encoded
            if encoded in self.cache:
                self.cache.move_to_end(encoded)
            elif encoded not in pending_by_selector:
                plan_scope = validate_plan_scope(selector_by_field["plan_scope"])
                role_plan_tier(role_payload)
                references = selector_by_field["insurance_plan_refs"]
                expected_id = "uhcplan-" + plan_scope.plan_key_id[:48]
                require(references == ["InsurancePlan/" + expected_id], "plan_tier_plan_reference")
                pending_by_selector[encoded] = (expected_id, selector_by_field["plan_scope"])
        if pending_by_selector:
            plan_records = await self.connection.fetch(
                f"SELECT * FROM pg_temp.{_identifier(self.table)} WHERE resource_id=ANY($1::text[])",
                sorted({selection[0] for selection in pending_by_selector.values()}),
            )
            plans_by_id = {plan_record["resource_id"]: plan_record for plan_record in plan_records}
            for encoded, (identity, scope) in pending_by_selector.items():
                canonical_plan = plans_by_id.get(identity)
                require(
                    canonical_plan is not None
                    and canonical_plan["declared_id"] == identity
                    and canonical_plan["contract"] == _CONTRACT
                    and re.fullmatch(r"[0-9a-f]{64}", canonical_plan["payload_sha256"])
                    and _decoded(canonical_plan["plan_scope"]) == scope,
                    "plan_tier_plan_changed",
                )
                self.cache[encoded] = _references(_decoded(canonical_plan["network_refs"]))
        prepared_rows = []
        for source_row in source_rows:
            selected = selections_by_resource.get((source_row["resource_type"], source_row["resource_id"]))
            prepared_rows.append(
                {**source_row, "plan_network_refs": list(self.cache[selected]) if selected is not None else []}
            )
        while len(self.cache) > 4096:
            self.cache.popitem(last=False)
        return prepared_rows


async def validate_plan_tier_graph_bindings(connection, bindings, arguments):
    """Validate distinct metadata in bounded pages before any provider-row mapping."""
    from process.uhc_provider_file_source_identity import UHC_PROVIDER_FILE_SOURCE_ID

    after = ""
    while rows := await connection.fetch(
        "WITH " + bindings + " SELECT record_key,record_json::text AS document FROM approved_network_bindings "
        "WHERE record_json->'source_scope_json'->>'kind'='uhc_plan_tier_v1' AND $8::text IS NOT NULL "
        "AND record_key>$9 ORDER BY record_key LIMIT 500",
        *arguments,
        after,
    ):
        for row in rows:
            document = json.loads(row["document"])
            require(document["source_id"] == UHC_PROVIDER_FILE_SOURCE_ID, "plan_tier_source")
            tier = validate_binding_scope(document["source_scope_json"], "registry-fhir-catalog-v1")
            require(
                document["producer_id"] == "registry-fhir-catalog-v1"
                and document["source_key"] == REFERENCE_PREFIX + tier["network_key_id"]
                and document["source_scope_json"]["source_generation"] == arguments[7],
                "plan_tier_binding",
            )
        after = rows[-1]["record_key"]


@asynccontextmanager
async def plan_tier_source_projection(connection, *, table_name=None):
    """A pass owns its temporary evidence and its bounded distinct-identity cache."""
    if table_name is not None:
        table = "pg_temp." + _identifier(table_name)
        require(await connection.fetchval("SELECT NOT EXISTS(SELECT FROM " + table + ")"), "plan_tier_restart")
        yield _PlanProjection(connection, table_name)
        await connection.execute("DELETE FROM " + table)
        return
    name = "typed_plan_evidence_" + uuid4().hex
    async with connection.transaction():
        await connection.execute(
            "CREATE TEMP TABLE " + _identifier(name) + "(" + PLAN_EVIDENCE_DDL + ") ON COMMIT DROP"
        )
        yield _PlanProjection(connection, name)
        await drain_operation(connection.execute("DROP TABLE pg_temp." + _identifier(name)), preserve_cancellation=True)
