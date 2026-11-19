# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Prepare a chunk of saved bulk NDJSON lines into the rows staging receives, outside the event loop."""

from __future__ import annotations

import datetime
import importlib
import json
from dataclasses import dataclass, field
from typing import Any

from process.provider_directory_role_merge import fold_merged_roles


@dataclass(frozen=True)
class PrepareContext:
    """Everything the parse and row preparation read; picklable, so a worker process can receive it."""

    source_id: str
    resource_type: str
    run_id: str | None
    log_identity: str | None  # the sanitized output identity, never the signed URL
    dataset_id: str
    resource_hash_contract: str
    semantic_projection_as_of: str | None
    merge_roles: bool
    strict: bool  # an output with a progress checkpoint fails on a non-object or foreign line instead of skipping it


@dataclass
class PreparedLines:
    rows: list[dict[str, Any]] = field(default_factory=list)
    rows_fetched: int = 0
    error: str | None = None


def _parsed_resources(lines: list[bytes], ctx: PrepareContext, fhir: Any) -> PreparedLines:
    """Parse lines with the codes and skips of the streaming bulk reader's line handling."""

    model = fhir.RESOURCE_MODELS_BY_TYPE[ctx.resource_type]
    projection_as_of = ctx.semantic_projection_as_of
    acquisition = fhir.FHIRAcquisitionContext(
        fetch_url=ctx.log_identity,
        fetch_mode="bulk_ndjson",
        semantic_projection_as_of=datetime.date.fromisoformat(projection_as_of) if projection_as_of else None,
    )
    parsed = PreparedLines()
    for raw_line in lines:
        line = raw_line.strip()
        if not line:
            continue
        try:
            # The stream's decoder: orjson would turn integers beyond 64 bits into floats.
            resource = json.loads(line.decode("utf-8-sig", errors="replace"))
        except json.JSONDecodeError:
            return PreparedLines(rows_fetched=parsed.rows_fetched, error="invalid_ndjson")
        if not isinstance(resource, dict) or resource.get("resourceType") != ctx.resource_type:
            if not ctx.strict:
                continue
            error = "invalid_ndjson" if not isinstance(resource, dict) else "bulk_export_output_resource_type_mismatch"
            return PreparedLines(rows_fetched=parsed.rows_fetched, error=error)
        parsed_resource = fhir.parse_fhir_resource(
            ctx.source_id,
            resource,
            resource_url=ctx.log_identity,
            acquisition=acquisition,
            run_id=ctx.run_id,
            # Batched Location rows get their contacts attached once per batch below.
            normalize_location_contacts=ctx.resource_type != "Location",
        )
        if parsed_resource and parsed_resource[0] is model:
            parsed.rows.append(parsed_resource[1])
            parsed.rows_fetched += 1
    return parsed


def prepare_lines(lines: list[bytes], ctx: PrepareContext) -> PreparedLines:
    """Raw NDJSON lines of one output in, prepared rows out; pure apart from the clock, which no row keeps."""

    # Imported by module name: the package attribute of the same name is its command.
    fhir = importlib.import_module("process.provider_directory_fhir")
    prepared = _parsed_resources(lines, ctx, fhir)
    if prepared.error or not prepared.rows:
        return PreparedLines(rows_fetched=prepared.rows_fetched, error=prepared.error)
    model = fhir.RESOURCE_MODELS_BY_TYPE[ctx.resource_type]
    source_rows = fhir._rows_for_compatibility_source(prepared.rows, ctx.source_id)
    source_rows = fhir._resource_rows_with_location_contacts(model, source_rows)
    retained_rows = fhir._endpoint_dataset_resource_rows(
        model, source_rows, dataset_id=ctx.dataset_id, resource_hash_contract=ctx.resource_hash_contract
    )
    if ctx.merge_roles:
        retained_rows = fold_merged_roles(retained_rows, ctx.resource_hash_contract)
    fhir._assert_practitioner_semantic_projection_as_of(
        model, source_rows, ctx.resource_hash_contract, ctx.semantic_projection_as_of
    )
    return PreparedLines(rows=retained_rows, rows_fetched=prepared.rows_fetched)


async def persist_prepared_rows(
    directory: Any,
    prepared_rows: list[dict[str, Any]],
    *,
    dataset_id: str | None,
    resource_hash_contract: str,
    semantic_projection_as_of: str | None,
) -> list[dict[str, Any]]:
    """Append prepared rows and their proof shard in the one publisher transaction scope of a batch."""

    from process.provider_directory_generic_source_adapters import persist_source_rows, require_source_transaction
    from process.provider_directory_generic_source_policy import source_transaction

    async with source_transaction(directory.db):
        require_source_transaction()
        if not prepared_rows:
            return prepared_rows
        await persist_source_rows(
            directory,
            prepared_rows,
            resource_hash_contract=resource_hash_contract,
            semantic_projection_as_of=semantic_projection_as_of,
        )
        await directory.persist_dataset_proof_shard(
            directory.db,
            directory._schema(),
            prepared_rows,
            dataset_id=dataset_id,
            expected_resource_hash_contract=resource_hash_contract,
        )
    return prepared_rows
