# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Selected-artifact helpers for the disposable UHC receipt proof."""

import asyncpg

from process.formulary_fhir.source_artifact_contract import VerifiedSourceArtifactSet
from process.formulary_fhir.source_artifact_contract import artifact_set_sha256
from process.formulary_fhir.uhc_drug_receipt import uhc_drug_receipt_id
from process.formulary_fhir.uhc_drug_receipt_store import UHC_DRUG_PARTIAL_EXCLUSION_CODE
from process.formulary_fhir.uhc_source import UHC_FORMULARY_SOURCE_ID
from tests.formulary_fhir_twin_admission_pg_support import assert_sqlstate, quoted
from tests.uhc_receipt_postgres_support import CANDIDATE_DATASET_ID
from tests.uhc_receipt_postgres_support import SOURCE_OBSERVATION_SHA256
from tests.uhc_receipt_postgres_support import SPOOL_CONTENT_SHA256
from tests.uhc_receipt_postgres_support import verify_artifacts


def selected_artifact_set(
    exact_set: VerifiedSourceArtifactSet, selected_file_count: int
) -> VerifiedSourceArtifactSet:
    if not 1 <= selected_file_count <= len(exact_set.artifacts):
        raise ValueError("selected artifact count is invalid")
    selected_artifacts = exact_set.artifacts[:selected_file_count]
    return VerifiedSourceArtifactSet(
        source_id=exact_set.source_id,
        source_file_set_sha256=exact_set.source_file_set_sha256,
        raw_listing_projection_sha256=exact_set.raw_listing_projection_sha256,
        artifacts=selected_artifacts,
        artifact_set_sha256=artifact_set_sha256(selected_artifacts),
    )


def canonical_partial_receipt_id(selected_set: VerifiedSourceArtifactSet) -> str:
    return uhc_drug_receipt_id(
        UHC_FORMULARY_SOURCE_ID,
        CANDIDATE_DATASET_ID,
        SOURCE_OBSERVATION_SHA256,
        selected_set.source_file_set_sha256,
        selected_set.artifact_set_sha256,
        SPOOL_CONTENT_SHA256,
        selected_source_file_ids_value=tuple(
            artifact.identity.source_file_id for artifact in selected_set.artifacts
        ),
        exclusion_code=UHC_DRUG_PARTIAL_EXCLUSION_CODE,
    )


def partial_receipt_insert_sql(
    schema_name: str,
    selected_set: VerifiedSourceArtifactSet,
    *,
    selected_source_file_ids: tuple[str, ...] | None = None,
    receipt_id: str | None = None,
) -> str:
    selected_ids = selected_source_file_ids or tuple(
        artifact.identity.source_file_id for artifact in selected_set.artifacts
    )
    selected_id_sql = ",".join(f"'{source_file_id}'" for source_file_id in selected_ids)
    file_count = len(selected_ids)
    schema = quoted(schema_name)
    return f"""INSERT INTO {schema}.fhir_formulary_uhc_admission_receipt
      (receipt_id, source_id, source_observation_sha256,
       source_file_set_sha256, artifact_set_sha256, candidate_dataset_id,
       spool_content_sha256, file_count, expected_file_count,
       excluded_file_count, selected_source_file_ids, exclusion_code,
       raw_record_count, raw_plan_entry_count, plan_count,
       medication_membership_count, duplicate_count, superseded_count,
       max_last_updated_at)
    VALUES ('{receipt_id or canonical_partial_receipt_id(selected_set)}',
      '{UHC_FORMULARY_SOURCE_ID}', '{SOURCE_OBSERVATION_SHA256}',
      '{selected_set.source_file_set_sha256}',
      '{selected_set.artifact_set_sha256}', '{CANDIDATE_DATASET_ID}',
      '{SPOOL_CONTENT_SHA256}', {file_count}, 48, {48 - file_count},
      ARRAY[{selected_id_sql}]::varchar(64)[],
      '{UHC_DRUG_PARTIAL_EXCLUSION_CODE}', {file_count}, {file_count},
      2, 5, 0, 0, transaction_timestamp() - interval '1 day')"""


def _omitted_artifact_set(
    exact_set: VerifiedSourceArtifactSet, selected_set: VerifiedSourceArtifactSet
) -> VerifiedSourceArtifactSet:
    omitted_artifacts = exact_set.artifacts[len(selected_set.artifacts) :]
    return VerifiedSourceArtifactSet(
        source_id=exact_set.source_id,
        source_file_set_sha256=exact_set.source_file_set_sha256,
        raw_listing_projection_sha256=exact_set.raw_listing_projection_sha256,
        artifacts=omitted_artifacts,
        artifact_set_sha256=artifact_set_sha256(omitted_artifacts),
    )


async def verify_selected_root_and_insert_receipt(
    connection: asyncpg.Connection,
    schema_name: str,
    exact_set: VerifiedSourceArtifactSet,
    selected_set: VerifiedSourceArtifactSet,
) -> str:
    selected_ids = tuple(artifact.identity.source_file_id for artifact in selected_set.artifacts)
    hash_function = f"{quoted(schema_name)}.fhir_formulary_source_artifact_selection_sha256($1, $2, $3)"
    hash_arguments = selected_set.source_id, selected_set.source_file_set_sha256, selected_ids
    assert await connection.fetchval(f"SELECT {hash_function}", *hash_arguments) is None
    await assert_sqlstate(connection, "23514", partial_receipt_insert_sql(schema_name, selected_set))
    await verify_artifacts(connection, schema_name, selected_set)
    assert await connection.fetchval(f"SELECT {hash_function}", *hash_arguments) == selected_set.artifact_set_sha256
    await assert_sqlstate(
        connection,
        "23514",
        partial_receipt_insert_sql(
            schema_name, selected_set, selected_source_file_ids=tuple(reversed(selected_ids))
        ),
    )
    receipt_id = canonical_partial_receipt_id(selected_set)
    assert await connection.execute(partial_receipt_insert_sql(schema_name, selected_set)) == "INSERT 0 1"
    await verify_artifacts(connection, schema_name, _omitted_artifact_set(exact_set, selected_set))
    stored = await connection.fetchrow(
        "SELECT receipt_id, file_count, expected_file_count, excluded_file_count, "
        "selected_source_file_ids, exclusion_code FROM "
        f"{quoted(schema_name)}.fhir_formulary_uhc_admission_receipt"
    )
    assert dict(stored) == {
        "receipt_id": receipt_id,
        "file_count": len(selected_ids),
        "expected_file_count": 48,
        "excluded_file_count": 48 - len(selected_ids),
        "selected_source_file_ids": list(selected_ids),
        "exclusion_code": UHC_DRUG_PARTIAL_EXCLUSION_CODE,
    }
    return receipt_id


__all__ = ("selected_artifact_set", "verify_selected_root_and_insert_receipt")
