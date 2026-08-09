# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Focused PostgreSQL assertions for reviewed subset abandonment."""

from __future__ import annotations

from tests.provider_directory_fhir_subset_abandonment_pg_support import (
    DATASET_ID,
    SERVING_ENDPOINT_ID,
    SOURCE_ID,
)
from tests.provider_directory_subset_completion_pg_support import (
    VALID_SOURCE_SCOPE_SHA256,
)


async def prove_collapsed_scope_domains_are_rejected(
    scenario,
    database,
    abandonment_module,
) -> None:
    """Reject a verification digest copied from the pagination domain."""

    await scenario.connection.execute(
        f"""
        UPDATE {scenario.quoted_schema}.provider_directory_endpoint_dataset
           SET publication_metadata_json = pg_catalog.jsonb_set(
                   publication_metadata_json::jsonb,
                   '{{verification_source_scope_hash}}',
                   pg_catalog.to_jsonb($1::text)
               )
         WHERE dataset_id = $2
        """,
        "1" * 64,
        DATASET_ID,
    )
    try:
        try:
            await abandonment_module.abandon_reviewed_subset_expired_root(
                database=database
            )
        except abandonment_module.ReviewedSubsetAbandonmentError as error:
            assert error.code == "evidence"
        else:
            raise AssertionError("collapsed scope domains were accepted")
    finally:
        await scenario.connection.execute(
            f"""
            UPDATE {scenario.quoted_schema}.provider_directory_endpoint_dataset
               SET publication_metadata_json = pg_catalog.jsonb_set(
                       publication_metadata_json::jsonb,
                       '{{verification_source_scope_hash}}',
                       pg_catalog.to_jsonb($1::text)
                   )
             WHERE dataset_id = $2
            """,
            VALID_SOURCE_SCOPE_SHA256,
            DATASET_ID,
        )


async def assert_serving_alias_and_decoy_preserved(scenario) -> None:
    """Require the serving alias and unrelated pre-v3 row to stay unchanged."""

    source_endpoint_id = await scenario.connection.fetchval(
        f"""
        SELECT endpoint_id
          FROM {scenario.quoted_schema}.provider_directory_source
         WHERE source_id = $1
        """,
        SOURCE_ID,
    )
    assert source_endpoint_id == SERVING_ENDPOINT_ID
    serving_alias_count = await scenario.connection.fetchval(
        f"""
        SELECT count(*)
          FROM {scenario.quoted_schema}.provider_directory_source
         WHERE endpoint_id = $1
        """,
        SERVING_ENDPOINT_ID,
    )
    assert serving_alias_count == 2
    serving_decoy = await scenario.connection.fetchrow(
        f"""
        SELECT endpoint_id, status, is_current, resource_count,
               completion_proof_required_version,
               completion_proof_json, completion_proof_sha256
          FROM {scenario.quoted_schema}.provider_directory_endpoint_dataset
         WHERE dataset_id = 'dataset-serving-decoy'
        """
    )
    assert dict(serving_decoy) == {
        "endpoint_id": SERVING_ENDPOINT_ID,
        "status": "failed",
        "is_current": False,
        "resource_count": 0,
        "completion_proof_required_version": None,
        "completion_proof_json": None,
        "completion_proof_sha256": None,
    }


async def assert_scope_domains_are_distinct(scenario) -> None:
    """Require retained pagination and verification domains to stay distinct."""

    scope_pair = await scenario.connection.fetchrow(
        f"""
        SELECT dataset.publication_metadata_json::jsonb
                   ->> 'verification_source_scope_hash' AS verification_scope,
               min(checkpoint.source_scope_hash) AS checkpoint_scope
          FROM {scenario.quoted_schema}.provider_directory_endpoint_dataset AS dataset
          JOIN {scenario.quoted_schema}.provider_directory_pagination_checkpoint
               AS checkpoint USING (dataset_id)
         WHERE dataset.dataset_id = $1
         GROUP BY dataset.dataset_id
        """,
        DATASET_ID,
    )
    assert scope_pair["verification_scope"] != scope_pair["checkpoint_scope"]
