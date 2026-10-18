# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native immutable registry-pair slot on the existing serving receipt."""

TABLE = "provider_directory_cms_serving_receipt"
COLUMN = "registry_source_pair_json"
MAX_BYTES = 65536
CHECK_NAME = "cms_serving_registry_pair_slot"
INDEX_NAME = "cms_serving_registry_capture_unique"
CHECK_SQL = f"""{COLUMN} IS NULL OR COALESCE(
    jsonb_typeof({COLUMN})='object'
    AND {COLUMN} ?& ARRAY['protocol','capture_id','prepared_pair']
    AND ({COLUMN} - ARRAY['protocol','capture_id','prepared_pair'])='{{}}'::jsonb
    AND jsonb_typeof({COLUMN}->'protocol')='number'
    AND {COLUMN}->>'protocol'='1'
    AND jsonb_typeof({COLUMN}->'capture_id')='string'
    AND {COLUMN}->>'capture_id' ~ '^[0-9a-f]{{8}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{12}}$'
    AND {COLUMN}->>'capture_id'<>'00000000-0000-0000-0000-000000000000'
    AND jsonb_typeof({COLUMN}->'prepared_pair')='object'
    AND octet_length({COLUMN}::text)<={MAX_BYTES},false)"""

# Exact PostgreSQL pretty form used by existing physical receipt attestation.
NATIVE_CHECK_SQL = "CHECK (registry_source_pair_json IS NULL OR COALESCE(jsonb_typeof(registry_source_pair_json) = 'object'::text AND registry_source_pair_json ?& ARRAY['protocol'::text, 'capture_id'::text, 'prepared_pair'::text] AND (registry_source_pair_json - ARRAY['protocol'::text, 'capture_id'::text, 'prepared_pair'::text]) = '{}'::jsonb AND jsonb_typeof(registry_source_pair_json -> 'protocol'::text) = 'number'::text AND (registry_source_pair_json ->> 'protocol'::text) = '1'::text AND jsonb_typeof(registry_source_pair_json -> 'capture_id'::text) = 'string'::text AND (registry_source_pair_json ->> 'capture_id'::text) ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'::text AND (registry_source_pair_json ->> 'capture_id'::text) <> '00000000-0000-0000-0000-000000000000'::text AND jsonb_typeof(registry_source_pair_json -> 'prepared_pair'::text) = 'object'::text AND octet_length(registry_source_pair_json::text) <= 65536, false))"
