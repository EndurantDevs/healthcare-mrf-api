# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native immutable metadata storage, independent of provider membership heaps."""

VIEW = "fhir-plans-v1"
PROJECTOR_VERSION = "fhir-catalog/v1"
MAX_CORE_BYTES = 786432
NESTED_LIMIT = 5
TABLES = (
    "registry_fhir_catalog_source",
    "registry_fhir_catalog_observation",
    "registry_fhir_catalog_snapshot",
    "registry_fhir_catalog_network",
    "registry_fhir_catalog_relationship",
    "registry_fhir_catalog_binding",
)
SNAPSHOT_COLUMNS = (
    "metadata_generation",
    "schema_name",
    "custody_json",
    "network_generation",
    "manifest_sha256",
    "metadata_approved_revision",
    "membership_mapping_sha256",
    "mapping_sha256",
    "standing_grant_sha256",
    "source_vector_json",
    "projector_version",
    "as_of",
    "accounting_json",
    "published_at",
)


_TABLE_DEFINITIONS = {
    "registry_fhir_catalog_source": """
      source_generation text COLLATE "C" PRIMARY KEY CHECK(source_generation ~ '^[0-9a-f]{64}$'),
      source_id varchar(128) NOT NULL CHECK(btrim(source_id)<>''),
      descriptor_json jsonb NOT NULL CHECK(jsonb_typeof(descriptor_json)='object'),
      profile text NOT NULL CHECK(profile IN ('raw_fhir','published_normalized','retained_normalized')),
      source_rows bigint NOT NULL CHECK(source_rows>=0),
      observation_rows bigint NOT NULL CHECK(observation_rows>=0),
      accepted_at timestamptz NOT NULL,
      created_at timestamptz NOT NULL DEFAULT clock_timestamp()
    """,
    "registry_fhir_catalog_observation": """
      source_generation text COLLATE "C" NOT NULL,
      resource_type varchar(64) COLLATE "C" NOT NULL,
      resource_id varchar(64) COLLATE "C" NOT NULL,
      observation_kind text COLLATE "C" NOT NULL CHECK(observation_kind IN ('organization','edge')),
      ordinal integer NOT NULL CHECK(ordinal>=0),
      document_json jsonb NOT NULL CHECK(jsonb_typeof(document_json)='object'
        AND octet_length(document_json::text)<=33554432),
      PRIMARY KEY(source_generation,resource_type,resource_id,observation_kind,ordinal)
    """,
    "registry_fhir_catalog_snapshot": """
      metadata_generation text COLLATE "C" PRIMARY KEY CHECK(metadata_generation ~ '^[0-9a-f]{64}$'),
      schema_name varchar(63) NOT NULL UNIQUE CHECK(schema_name ~ '^fhir_catalog_[0-9a-f]{32}$'),
      custody_json jsonb NOT NULL CHECK(jsonb_typeof(custody_json)='object'),
      network_generation bigint CHECK(network_generation>0),
      manifest_sha256 text CHECK(manifest_sha256 ~ '^[0-9a-f]{64}$'),
      CHECK((network_generation IS NULL)=(manifest_sha256 IS NULL)),
      metadata_approved_revision bigint NOT NULL CHECK(metadata_approved_revision>=0),
      membership_mapping_sha256 text NOT NULL CHECK(membership_mapping_sha256 ~ '^[0-9a-f]{64}$'),
      mapping_sha256 text NOT NULL CHECK(mapping_sha256 ~ '^[0-9a-f]{64}$'),
      standing_grant_sha256 text NOT NULL CHECK(standing_grant_sha256 ~ '^[0-9a-f]{64}$'),
      source_vector_json jsonb NOT NULL CHECK(jsonb_typeof(source_vector_json)='array'),
      projector_version text NOT NULL,
      as_of date NOT NULL,
      accounting_json jsonb NOT NULL CHECK(jsonb_typeof(accounting_json)='object'),
      published_at timestamptz
    """,
    "registry_fhir_catalog_network": """
      metadata_generation text COLLATE "C" NOT NULL,
      network_id integer NOT NULL CHECK(network_id>0),
      display_name varchar(512) NOT NULL CHECK(btrim(display_name)<>''),
      aliases jsonb NOT NULL CHECK(jsonb_typeof(aliases)='array'),
      archived boolean NOT NULL,
      revision bigint NOT NULL CHECK(revision>0),
      benefit_codes text[],
      search_name text NOT NULL,
      PRIMARY KEY(metadata_generation,network_id)
    """,
    "registry_fhir_catalog_relationship": """
      metadata_generation text COLLATE "C" NOT NULL,
      relationship_id text COLLATE "C" NOT NULL CHECK(relationship_id ~ '^[0-9a-f]{64}$'),
      network_id integer NOT NULL CHECK(network_id>0),
      source_id varchar(128) NOT NULL,
      source_generation text COLLATE "C" NOT NULL CHECK(source_generation ~ '^[0-9a-f]{64}$'),
      binding_id uuid,
      CHECK(binding_id IS NOT NULL OR source_id='registry-manual'),
      company_id uuid,
      group_id uuid,
      company_search text NOT NULL,
      plan_search text NOT NULL,
      document_json jsonb NOT NULL CHECK(jsonb_typeof(document_json)='object'
        AND octet_length(document_json::text)<=32768),
      PRIMARY KEY(metadata_generation,relationship_id)
    """,
    "registry_fhir_catalog_binding": """
      metadata_generation text COLLATE "C" NOT NULL,
      network_id integer NOT NULL CHECK(network_id>0),
      binding_id uuid NOT NULL,
      source_id varchar(128) NOT NULL,
      document_json jsonb NOT NULL CHECK(jsonb_typeof(document_json)='object'
        AND octet_length(document_json::text)<=32768),
      PRIMARY KEY(metadata_generation,binding_id)
    """,
}


def table_sql(namespace, table, *, trigram_schema="public"):
    """Return frozen native definitions for fresh creation and owner adoption."""
    if table not in _TABLE_DEFINITIONS:
        raise ValueError("registry_fhir_catalog_table_invalid")
    statements = [f"CREATE TABLE {namespace}.{table} ({_TABLE_DEFINITIONS[table]})"]
    indexes_by_table = {
        "registry_fhir_catalog_observation": (
            (
                "registry_fhir_catalog_observation_network_idx",
                "(source_generation,(document_json->>'network_resource_id')) WHERE observation_kind='edge'",
            ),
        ),
        "registry_fhir_catalog_network": (
            ("registry_fhir_catalog_name_idx", '(metadata_generation,lower(display_name) COLLATE "C",network_id)'),
            ("registry_fhir_catalog_name_trgm", f"USING gin (search_name {trigram_schema}.gin_trgm_ops)"),
        ),
        "registry_fhir_catalog_relationship": (
            ("registry_fhir_catalog_edge_network_idx", "(metadata_generation,network_id,relationship_id)"),
            ("registry_fhir_catalog_edge_company_idx", "(metadata_generation,company_id,network_id)"),
            ("registry_fhir_catalog_edge_group_idx", "(metadata_generation,group_id,network_id)"),
            ("registry_fhir_catalog_plan_trgm", f"USING gin (plan_search {trigram_schema}.gin_trgm_ops)"),
            ("registry_fhir_catalog_company_trgm", f"USING gin (company_search {trigram_schema}.gin_trgm_ops)"),
        ),
        "registry_fhir_catalog_binding": (
            ("registry_fhir_catalog_binding_network_idx", "(metadata_generation,network_id,binding_id)"),
        ),
    }
    statements.extend(
        f"CREATE INDEX {name} ON {namespace}.{table} {definition}"
        for name, definition in indexes_by_table.get(table, ())
    )
    return tuple(statements)
