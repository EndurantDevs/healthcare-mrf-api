# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Resolve exact Profile Source lineage without replacing it with current selection."""

import importlib
import json
import re
from collections.abc import Mapping

from sqlalchemy import text

_UNAVAILABLE = "provider_directory_profile_lineage_unavailable"
_AMBIGUOUS = "provider_directory_profile_lineage_ambiguous"


async def _query_rows(npi_module, fhir, statement, parameters, session):
    """Keep both lookups on the caller's existing read snapshot and executor."""
    try:
        result = await npi_module._execute_stmt(text(statement), session=session, params=parameters)
        return [fhir._pagination_checkpoint_row_mapping(row) for row in result.all()]
    except Exception as error:
        raise RuntimeError(_UNAVAILABLE) from error


def _canonical_result(fhir, raw_result, generation_id):
    """Use the original dated or undated producer contract without inventing a date."""
    try:
        result = json.loads(raw_result) if isinstance(raw_result, str) else raw_result
        if not isinstance(result, Mapping):
            raise ValueError("result_required")
        if "profile_as_of" in result:
            canonical = fhir._provider_directory_profile_adoption_result(result, generation_id=generation_id)
        else:
            from process.provider_directory_profile_initial_contract import validated_legacy_result

            canonical = validated_legacy_result(result)
        if canonical["profile_generation_id"] != generation_id:
            raise ValueError("generation_changed")
        return canonical
    except (KeyError, TypeError, ValueError, RuntimeError) as error:
        raise RuntimeError(_UNAVAILABLE) from error


async def profile_source_datasets(npi_module, generation_id, *, session):
    """Read one genuine Profile generation's exact historical Source dataset vector."""
    if type(generation_id) is not str or re.fullmatch(r"pdprofile_[0-9a-f]{32}", generation_id) is None:
        raise RuntimeError(_UNAVAILABLE)
    fhir = importlib.import_module("process.provider_directory_fhir")
    schema = npi_module._runtime_db_schema()
    parameters_by_name = {"profile_generation_id": generation_id}
    heads = await _query_rows(
        npi_module,
        fhir,
        f"SELECT * FROM {schema}.provider_directory_profile_serving_generation "
        "WHERE singleton_key='global' AND status='published' AND operation='publish' "
        "AND generation_id=:profile_generation_id",
        parameters_by_name,
        session,
    )
    if len(heads) > 1:
        raise RuntimeError(_AMBIGUOUS)
    if heads:
        try:
            state = fhir._provider_directory_profile_serving_state_from_row(heads[0])
            if state.generation_id != generation_id or (state.status, state.operation) != ("published", "publish"):
                raise ValueError("generation_changed")
            return state.source_vector
        except (KeyError, TypeError, ValueError, RuntimeError) as error:
            raise RuntimeError(_UNAVAILABLE) from error
    terminal_rows = await _query_rows(
        npi_module,
        fhir,
        f"SELECT metrics::jsonb -> :metric AS result FROM {schema}.import_run "
        "WHERE importer='provider-directory-fhir' AND status='succeeded' AND finished_at IS NOT NULL "
        "AND (metrics::jsonb -> :metric ->> 'profile_generation_id')=:profile_generation_id",
        parameters_by_name | {"metric": fhir.PROFILE_SELECTION_RESULT_METRIC},
        session,
    )
    if not terminal_rows:
        raise RuntimeError(_UNAVAILABLE)
    canonical_results = [
        _canonical_result(fhir, terminal_row.get("result"), generation_id) for terminal_row in terminal_rows
    ]
    if any(canonical_result != canonical_results[0] for canonical_result in canonical_results[1:]):
        raise RuntimeError(_AMBIGUOUS)
    return fhir._provider_directory_profile_source_vector(
        (pair["source_id"], pair["dataset_id"]) for pair in canonical_results[0]["pairs"]
    )
