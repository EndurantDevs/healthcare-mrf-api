# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""PostgreSQL assertions for terminal scope-binding evidence."""

from __future__ import annotations


async def _scope_record(scenario, migration):
    return await scenario.connection.fetchrow(
        f"""
        SELECT publication_metadata_json::jsonb
                   ->> 'verification_source_scope_hash' AS verification_scope,
               publication_metadata_json::jsonb
                   #>> '{{completion_proof_v1,verification_source_scope_hash}}'
                   AS completion_scope,
               publication_metadata_json::jsonb
                   #>> '{{{migration._MARKER},source_scope_sha256}}'
                   AS marker_scope
          FROM {scenario.quoted_schema}.provider_directory_endpoint_dataset
         WHERE dataset_id = 'dataset-a'
        """
    )


async def _checkpoint_scope_record(scenario):
    return await scenario.connection.fetchrow(
        f"""
        SELECT pg_catalog.count(DISTINCT source_scope_hash) AS scope_count,
               pg_catalog.min(source_scope_hash) AS checkpoint_scope
          FROM {scenario.quoted_schema}.provider_directory_pagination_checkpoint
         WHERE dataset_id = 'dataset-a'
        """
    )


async def _invalid_serial_count(scenario):
    return await scenario.connection.fetchval(
        f"""
        SELECT pg_catalog.count(*)
          FROM {scenario.quoted_schema}.provider_directory_endpoint_dataset
          CROSS JOIN LATERAL pg_catalog.jsonb_each(
               publication_metadata_json::jsonb -> 'resource_diagnostics'
          ) AS diagnostic(resource_type, evidence)
         WHERE dataset_id = 'dataset-a'
           AND (
                pg_catalog.jsonb_typeof(
                    diagnostic.evidence
                        -> 'resource_scan_concurrency_requested'
                ) <> 'number'
                OR diagnostic.evidence
                     #>> '{{resource_scan_concurrency_requested}}' <> '1'
                OR pg_catalog.jsonb_typeof(
                    diagnostic.evidence
                        -> 'resource_scan_concurrency_effective'
                ) <> 'number'
                OR diagnostic.evidence
                     #>> '{{resource_scan_concurrency_effective}}' <> '1'
           )
        """
    )


async def assert_bound_scope_and_serial_evidence(scenario, migration) -> None:
    """Assert persisted verification/checkpoint domains and serial shape."""

    scope_record = await _scope_record(scenario, migration)
    checkpoint_scopes = await _checkpoint_scope_record(scenario)
    invalid_serial_count = await _invalid_serial_count(scenario)

    assert scope_record["verification_scope"] == scope_record["completion_scope"]
    assert scope_record["verification_scope"] != scope_record["marker_scope"]
    assert checkpoint_scopes["scope_count"] == 1
    assert checkpoint_scopes["checkpoint_scope"] == scope_record["marker_scope"]
    assert invalid_serial_count == 0


__all__ = ("assert_bound_scope_and_serial_evidence",)
