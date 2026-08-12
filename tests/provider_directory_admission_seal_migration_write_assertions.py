# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Write-path assertions for the admission-seal migration proof."""

from __future__ import annotations

import json

import pytest

from tests.provider_directory_admission_seal_migration_fixture import (
    _digest_call,
    _expect_error,
    asyncpg,
)


_INVALID_RECEIPT_CASES = (
    (
        "provider_directory_endpoint_dataset_admission_version_invalid",
        {}, 2, "generic", "a" * 64, ["Location"], None,
    ),
    (
        "provider_directory_endpoint_dataset_admission_kind_invalid",
        {}, 1, "other", "a" * 64, ["Location"], None,
    ),
    (
        "provider_directory_endpoint_dataset_admission_proof_sha256_invalid",
        {}, 1, "generic", "A" * 64, ["Location"], None,
    ),
    (
        "provider_directory_endpoint_dataset_admission_summary_invalid",
        [], 1, "generic", "a" * 64, ["Location"], None,
    ),
    (
        "provider_directory_endpoint_dataset_admission_summary_unbounded",
        {"payload": "x" * (1024 * 1024)},
        1, "generic", "a" * 64, ["Location"], None,
    ),
    (
        "provider_directory_endpoint_dataset_admission_resources_invalid",
        {}, 1, "generic", "a" * 64, ["Location", "Location"], None,
    ),
    (
        "provider_directory_endpoint_dataset_admission_resources_invalid",
        {}, 1, "generic", "a" * 64,
        [f"Resource{index:02d}" for index in range(65)], None,
    ),
    (
        "provider_directory_endpoint_dataset_admission_metadata_sha256_invalid",
        {}, 1, "generic", "a" * 64, ["Location"], "0" * 64,
    ),
)


async def _assert_catalog_contract(connection, schema: str) -> None:
    trigger_rows = await connection.fetch(
        """
        SELECT trigger_row.tgname, trigger_row.tgenabled
          FROM pg_catalog.pg_trigger AS trigger_row
          JOIN pg_catalog.pg_class AS relation
            ON relation.oid = trigger_row.tgrelid
          JOIN pg_catalog.pg_namespace AS relation_namespace
            ON relation_namespace.oid = relation.relnamespace
         WHERE relation_namespace.nspname = $1
           AND relation.relname = 'provider_directory_endpoint_dataset'
           AND trigger_row.tgname LIKE
                   'provider_directory_endpoint_dataset_admission%'
           AND trigger_row.tgisinternal IS FALSE
         ORDER BY trigger_row.tgname
        """,
        schema,
    )
    assert [
        (trigger_row["tgname"], trigger_row["tgenabled"])
        for trigger_row in trigger_rows
    ] == [
        (
            "provider_directory_endpoint_dataset_admission_raw_guard",
            b"A",
        ),
        (
            "provider_directory_endpoint_dataset_admission_seal_guard",
            b"A",
        ),
        (
            "provider_directory_endpoint_dataset_admission_truncate_guard",
            b"A",
        ),
    ]
    for signature in (
        "provider_directory_endpoint_dataset_admission_metadata_sha256"
        "(jsonb,smallint,text,text,character varying[])",
        "guard_provider_directory_endpoint_dataset_admission_seal()",
        "guard_provider_directory_endpoint_dataset_admission_truncate()",
    ):
        assert not await connection.fetchval(
            "SELECT pg_catalog.has_function_privilege("
            "'public', pg_catalog.to_regprocedure($1), 'EXECUTE')",
            f'"{schema}".{signature}',
        )


async def _assert_invalid_write_paths(connection, schema: str) -> None:
    """Reject partial receipt inserts, copies, upserts, and resource arrays."""

    await _assert_partial_receipt_writes(connection, schema)
    await _assert_invalid_resource_arrays(connection, schema)


async def _assert_partial_receipt_writes(connection, schema: str) -> None:
    """Reject partial seals through values, COPY, and conflict updates."""

    table = f'"{schema}".provider_directory_endpoint_dataset'
    await _expect_error(
        connection,
        "provider_directory_endpoint_dataset_admission_seal_partial",
        f"INSERT INTO {table} (dataset_id, content_proof_admission_version) "
        "VALUES ('dataset_values_partial', 1)",
    )
    with pytest.raises(
        asyncpg.PostgresError,
        match="provider_directory_endpoint_dataset_admission_seal_partial",
    ):
        await connection.copy_records_to_table(
            "provider_directory_endpoint_dataset",
            schema_name=schema,
            records=[("dataset_copy_partial", 1)],
            columns=["dataset_id", "content_proof_admission_version"],
        )
    await connection.execute(
        f"INSERT INTO {table} (dataset_id) VALUES ('dataset_upsert_partial')"
    )
    await _expect_error(
        connection,
        "provider_directory_endpoint_dataset_admission_seal_partial",
        f"""
        INSERT INTO {table} (dataset_id)
        VALUES ('dataset_upsert_partial')
        ON CONFLICT (dataset_id) DO UPDATE
        SET content_proof_admission_version = 1
        """,
    )


async def _assert_invalid_resource_arrays(connection, schema: str) -> None:
    """Reject unsorted and overlong receipt resource types."""

    table = f'"{schema}".provider_directory_endpoint_dataset'
    await _expect_error(
        connection,
        "provider_directory_endpoint_dataset_admission_resources_invalid",
        f"""
        UPDATE {table}
           SET publication_metadata_summary_json = '{{}}'::jsonb,
               publication_metadata_sha256 =
                   "{schema}".provider_directory_endpoint_dataset_admission_metadata_sha256(
                       '{{}}'::jsonb, 1::smallint, 'generic'::text,
                       repeat('a', 64),
                       ARRAY['Organization', 'Location']::varchar[]
                   ),
               content_proof_admission_version = 1,
               content_proof_admission_kind = 'generic',
               content_proof_admission_sha256 = repeat('a', 64),
               content_proof_resource_types =
                   ARRAY['Organization', 'Location']::varchar[]
         WHERE dataset_id = 'dataset_upsert_partial'
        """,
    )
    oversized_resource = "é" * 33
    await _expect_error(
        connection,
        "provider_directory_endpoint_dataset_admission_resources_invalid",
        f"""
        UPDATE {table}
           SET publication_metadata_summary_json = '{{}}'::jsonb,
               publication_metadata_sha256 =
                   "{schema}".provider_directory_endpoint_dataset_admission_metadata_sha256(
                       '{{}}'::jsonb, 1::smallint, 'generic'::text,
                       repeat('a', 64),
                       ARRAY[$1]::varchar[]
                   ),
               content_proof_admission_version = 1,
               content_proof_admission_kind = 'generic',
               content_proof_admission_sha256 = repeat('a', 64),
               content_proof_resource_types = ARRAY[$1]::varchar[]
         WHERE dataset_id = 'dataset_upsert_partial'
        """,
        oversized_resource,
    )


async def _assert_invalid_complete_receipts(connection, schema: str) -> None:
    """Reject every invalid field of an otherwise complete receipt tuple."""

    table = f'"{schema}".provider_directory_endpoint_dataset'
    for index, (
        marker,
        summary_value,
        version,
        kind,
        proof_sha256,
        resource_types,
        digest_override,
    ) in enumerate(_INVALID_RECEIPT_CASES):
        dataset_id = f"dataset_invalid_receipt_{index}"
        await connection.execute(
            f"INSERT INTO {table} (dataset_id) VALUES ($1)",
            dataset_id,
        )
        await _expect_error(
            connection,
            marker,
            f"""
            UPDATE {table}
               SET publication_metadata_json = $2::json,
                   publication_metadata_summary_json = $2::jsonb,
                   publication_metadata_sha256 = COALESCE(
                       $7::varchar,
                       "{schema}".provider_directory_endpoint_dataset_admission_metadata_sha256(
                           $2::jsonb,
                           $3::smallint,
                           $4::text,
                           $5::text,
                           $6::varchar[]
                       )
                   ),
                   content_proof_admission_version = $3::smallint,
                   content_proof_admission_kind = $4,
                   content_proof_admission_sha256 = $5,
                   content_proof_resource_types = $6::varchar[]
             WHERE dataset_id = $1
            """,
            dataset_id,
            json.dumps(summary_value),
            version,
            kind,
            proof_sha256,
            resource_types,
            digest_override,
        )


async def _assert_sealed_mutations(connection, schema: str) -> None:
    """Permit additive summaries while rejecting sealed identity mutation."""

    await _assert_raw_and_summary_immutable(connection, schema)
    await _assert_additive_summary_only(connection, schema)
    await _assert_digest_and_truncate_immutable(connection, schema)


async def _assert_raw_and_summary_immutable(connection, schema: str) -> None:
    """Reject direct raw metadata and immutable summary changes."""

    table = f'"{schema}".provider_directory_endpoint_dataset'
    await connection.execute(
        f"UPDATE {table} SET status = 'published' "
        "WHERE dataset_id = 'dataset_sealed'"
    )
    await _expect_error(
        connection,
        "provider_directory_endpoint_dataset_admission_raw_metadata_immutable",
        f"""
        UPDATE {table}
           SET publication_metadata_json = pg_catalog.jsonb_set(
                   publication_metadata_json::jsonb,
                   '{{reviewed}}',
                   'true'::jsonb,
                   true
               )::json
         WHERE dataset_id = 'dataset_sealed'
        """,
    )
    await _expect_error(
        connection,
        "provider_directory_endpoint_dataset_admission_receipt_immutable",
        f"""
        UPDATE {table}
           SET publication_metadata_summary_json =
                   publication_metadata_summary_json || '{{"reviewed":true}}',
               publication_metadata_sha256 =
                   "{schema}".provider_directory_endpoint_dataset_admission_metadata_sha256(
                       publication_metadata_summary_json || '{{"reviewed":true}}',
                       content_proof_admission_version,
                       content_proof_admission_kind,
                       content_proof_admission_sha256,
                       content_proof_resource_types
                   )
         WHERE dataset_id = 'dataset_sealed'
        """,
    )


async def _assert_additive_summary_only(connection, schema: str) -> None:
    """Allow one approved summary key but reject a combined raw mutation."""

    table = f'"{schema}".provider_directory_endpoint_dataset'
    await connection.execute(
        f"""
        UPDATE {table}
           SET publication_metadata_summary_json = jsonb_set(
                   publication_metadata_summary_json,
                   '{{outcome_resource_counts_v1}}',
                   '{{"complete":true}}'::jsonb,
                   true
               ),
               publication_metadata_sha256 =
                   "{schema}".provider_directory_endpoint_dataset_admission_metadata_sha256(
                       jsonb_set(
                           publication_metadata_summary_json,
                           '{{outcome_resource_counts_v1}}',
                           '{{"complete":true}}'::jsonb,
                           true
                       ),
                       content_proof_admission_version,
                       content_proof_admission_kind,
                       content_proof_admission_sha256,
                       content_proof_resource_types
                   )
         WHERE dataset_id = 'dataset_sealed'
        """,
    )
    await _expect_error(
        connection,
        "provider_directory_endpoint_dataset_admission_raw_metadata_immutable",
        f"""
        UPDATE {table}
           SET publication_metadata_json = pg_catalog.jsonb_set(
                   publication_metadata_json::jsonb,
                   '{{reviewed}}',
                   'true'::jsonb,
                   true
               )::json,
               publication_metadata_summary_json =
                   publication_metadata_summary_json || '{{"raw":true}}',
               publication_metadata_sha256 =
                   "{schema}".provider_directory_endpoint_dataset_admission_metadata_sha256(
                       publication_metadata_summary_json || '{{"raw":true}}',
                       content_proof_admission_version,
                       content_proof_admission_kind,
                       content_proof_admission_sha256,
                       content_proof_resource_types
                   )
         WHERE dataset_id = 'dataset_sealed'
        """
    )


async def _assert_digest_and_truncate_immutable(connection, schema: str) -> None:
    """Reject proof-digest replacement and table truncation after sealing."""

    table = f'"{schema}".provider_directory_endpoint_dataset'
    await _expect_error(
        connection,
        "provider_directory_endpoint_dataset_admission_receipt_immutable",
        f"""
        UPDATE {table}
           SET content_proof_admission_sha256 = repeat('b', 64),
               publication_metadata_sha256 =
                   "{schema}".provider_directory_endpoint_dataset_admission_metadata_sha256(
                       publication_metadata_summary_json,
                       content_proof_admission_version,
                       content_proof_admission_kind,
                       repeat('b', 64),
                       content_proof_resource_types
                   )
         WHERE dataset_id = 'dataset_sealed'
        """,
    )
    await _expect_error(
        connection,
        "provider_directory_endpoint_dataset_admission_truncate_forbidden",
        f"TRUNCATE {table}",
    )
