# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Publication-time source-fence PostgreSQL cases for subset completion."""

from __future__ import annotations

from tests.provider_directory_subset_completion_pg_setup import (
    replace_subset_source,
)
from tests.tin_npi_connector_postgres_support import expect_postgres_error


def _publish_subset_sql(scenario):
    return f"""
        UPDATE {scenario.quoted_schema}.provider_directory_endpoint_dataset
           SET status = 'published', is_current = true,
               published_at = transaction_timestamp()
         WHERE dataset_id = 'dataset-matched'
    """


async def _expect_source_mutations_rejected(scenario, publish_sql):
    source_mutations = (
        {"provider_directory_manual_only": False},
        {"provider_directory_verification_campaign_id": "campaign-drift"},
        {"provider_directory_current_version_census_page_count": 249},
    )
    for metadata_changes in source_mutations:
        await replace_subset_source(
            scenario,
            "verified_two_matching_reviewed_subset_acquisitions",
            **metadata_changes,
        )
        await expect_postgres_error(
            scenario.connection,
            "provider_directory_subset_published_source_invalid",
            publish_sql,
        )
    await replace_subset_source(
        scenario,
        "verified_two_matching_reviewed_subset_acquisitions",
        remove_metadata=("provider_directory_manual_only",),
    )
    await expect_postgres_error(
        scenario.connection,
        "provider_directory_subset_published_source_invalid",
        publish_sql,
    )
    await replace_subset_source(
        scenario,
        "verified_two_matching_reviewed_subset_acquisitions",
        source_changes={
            "canonical_api_base": "https://drift.example.test/fhir",
        },
    )
    await expect_postgres_error(
        scenario.connection,
        "provider_directory_subset_published_source_invalid",
        publish_sql,
    )


async def _expect_extra_alias_rejected(scenario, publish_sql):
    await replace_subset_source(
        scenario,
        "verified_two_matching_reviewed_subset_acquisitions",
    )
    await scenario.connection.execute(
        f"""
        INSERT INTO {scenario.quoted_schema}.provider_directory_source (
            source_id, endpoint_id, canonical_api_base,
            requires_registration, requires_api_key, auth_type, metadata_json
        ) VALUES (
            'synthetic-extra-source', 'endpoint-a',
            'https://extra.example.test/fhir', false, false, 'none', '{{}}'::jsonb
        )
        """
    )
    await expect_postgres_error(
        scenario.connection,
        "provider_directory_subset_published_source_invalid",
        publish_sql,
    )
    await scenario.connection.execute(
        f"""
        DELETE FROM {scenario.quoted_schema}.provider_directory_source
         WHERE source_id = 'synthetic-extra-source'
        """
    )


async def prove_publish_source_fence(scenario):
    """Reject source drift, then publish only from the exact verified source."""

    publish_sql = _publish_subset_sql(scenario)
    await expect_postgres_error(
        scenario.connection,
        "provider_directory_subset_published_source_invalid",
        publish_sql,
    )
    await scenario.connection.execute(
        f"DELETE FROM {scenario.quoted_schema}.provider_directory_source"
    )
    await expect_postgres_error(
        scenario.connection,
        "provider_directory_subset_published_source_invalid",
        publish_sql,
    )
    await _expect_source_mutations_rejected(scenario, publish_sql)
    await _expect_extra_alias_rejected(scenario, publish_sql)
    await scenario.connection.execute(publish_sql)
    await scenario.connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
    await scenario.connection.execute(
        f"""
        UPDATE {scenario.quoted_schema}.provider_directory_source
           SET metadata_json = pg_catalog.jsonb_set(
                metadata_json,
                '{{unrelated_review_note}}',
                '"allowed"'::jsonb,
                true
           )
         WHERE source_id = 'synthetic-source'
        """
    )
    for source_mutation in (
        f"""
        UPDATE {scenario.quoted_schema}.provider_directory_source
           SET canonical_api_base = 'https://drift.example.test/fhir'
         WHERE source_id = 'synthetic-source'
        """,
        f"""
        DELETE FROM {scenario.quoted_schema}.provider_directory_source
         WHERE source_id = 'synthetic-source'
        """,
    ):
        await expect_postgres_error(
            scenario.connection,
            "provider_directory_subset_published_source_mutation_invalid",
            source_mutation,
        )
    for truncate_suffix in ("", " CASCADE"):
        await expect_postgres_error(
            scenario.connection,
            "provider_directory_subset_published_source_mutation_invalid",
            f"TRUNCATE TABLE {scenario.quoted_schema}.provider_directory_source"
            f"{truncate_suffix}",
        )
    await scenario.connection.execute("SET CONSTRAINTS ALL DEFERRED")
