# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Frozen native storage contract for accepted ACA publication notifications."""

CONTRACT = "registry-aca-source-notification/v1"
TABLE = "registry_aca_source_notification"
INSERT_COLUMNS = (
    "capture_id",
    "source_schema",
    "source_schema_oid",
    "recipe_sha256",
    "notification_sha256",
    "notification_json",
)
MAX_BYTES = 600 * 1024
CREATE_TABLE_SQL = """CREATE TABLE {namespace}.registry_aca_source_notification (
 capture_id uuid PRIMARY KEY CHECK(capture_id<>'00000000-0000-0000-0000-000000000000'::uuid),
 source_schema text NOT NULL UNIQUE CHECK(source_schema='reference_family_archive_'||replace(capture_id::text,'-','')),
 source_schema_oid oid NOT NULL CHECK(source_schema_oid<>0),
 recipe_sha256 text NOT NULL CHECK(recipe_sha256 ~ '^[0-9a-f]{{64}}$'),
 notification_sha256 text NOT NULL CHECK(notification_sha256 ~ '^[0-9a-f]{{64}}$'),
 notification_json text NOT NULL CHECK(octet_length(notification_json)<=614400),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 CHECK(notification_sha256=encode(sha256(convert_to(notification_json,'UTF8')),'hex'))
)"""
