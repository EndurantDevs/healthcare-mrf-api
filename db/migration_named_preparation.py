# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Current generic named preparation metadata with application-owned relationships."""

from sqlalchemy import text

TABLES = ("preparation", "preparation_relation", "preparation_claim", "relation")

CATALOG = (
    """CREATE TABLE hp_snapshot_retention.preparation (
	preparation_id UUID NOT NULL,
	node_id VARCHAR(128) NOT NULL,
	importer_id VARCHAR(128) NOT NULL,
	dataset_key VARCHAR(128) NOT NULL,
	run_id VARCHAR(128) NOT NULL,
	generation_id UUID NOT NULL,
	publication_fence UUID NOT NULL,
	admission_sha256 VARCHAR(64) NOT NULL,
	admission JSONB NOT NULL,
	state VARCHAR(16) DEFAULT 'admitted' NOT NULL,
	expected_generation_id UUID,
	expected_fence_token UUID,
	predecessor_pin_id UUID,
	attempt_id VARCHAR(128),
	handoff JSONB,
	intake_inventory JSONB,
	intake_sha256 VARCHAR(64),
	frozen_inventory JSONB,
	frozen_sha256 VARCHAR(64),
	validation JSONB,
	validation_sha256 VARCHAR(64),
	publication JSONB,
	publication_sha256 VARCHAR(64),
	published_at TIMESTAMP WITH TIME ZONE,
	published_fence_token UUID,
	cancellation_requested BOOLEAN DEFAULT false NOT NULL,
	abandonment_sha256 VARCHAR(64),
	claim_attempt INTEGER DEFAULT 0 NOT NULL,
	claim_token UUID,
	claim_expires_at TIMESTAMP WITH TIME ZONE,
	created_at TIMESTAMP WITH TIME ZONE DEFAULT clock_timestamp() NOT NULL,
	updated_at TIMESTAMP WITH TIME ZONE DEFAULT clock_timestamp() NOT NULL,
	PRIMARY KEY (preparation_id),
	CONSTRAINT snapshot_native_preparation_run_key UNIQUE (node_id, importer_id, run_id, dataset_key),
	CONSTRAINT snapshot_native_preparation_generation_key UNIQUE (generation_id),
	CONSTRAINT snapshot_native_preparation_fence_key UNIQUE (publication_fence),
	CONSTRAINT snapshot_native_preparation_state CHECK (state IN ('admitted','handed_off','frozen','validated','published','abandoned')),
	CONSTRAINT snapshot_native_preparation_predecessor CHECK ((expected_generation_id IS NULL) = (expected_fence_token IS NULL) AND (expected_generation_id IS NULL) = (predecessor_pin_id IS NULL)),
	CONSTRAINT snapshot_native_preparation_intake_shape CHECK ((attempt_id IS NULL) = (handoff IS NULL) AND (attempt_id IS NULL) = (intake_inventory IS NULL) AND (attempt_id IS NULL) = (intake_sha256 IS NULL)),
	CONSTRAINT snapshot_native_preparation_admitted_shape CHECK (state <> 'admitted' OR attempt_id IS NULL),
	CONSTRAINT snapshot_native_preparation_intake_required CHECK (state NOT IN ('handed_off','frozen','validated','published') OR intake_inventory IS NOT NULL),
	CONSTRAINT snapshot_native_preparation_frozen_shape CHECK ((frozen_inventory IS NULL) = (frozen_sha256 IS NULL)),
	CONSTRAINT snapshot_native_preparation_frozen_required CHECK (state NOT IN ('frozen','validated','published') OR frozen_inventory IS NOT NULL),
	CONSTRAINT snapshot_native_preparation_frozen_order CHECK (frozen_inventory IS NULL OR (intake_inventory IS NOT NULL AND state IN ('frozen','validated','published','abandoned'))),
	CONSTRAINT snapshot_native_preparation_validation_shape CHECK ((validation IS NULL) = (validation_sha256 IS NULL)),
	CONSTRAINT snapshot_native_preparation_validation_required CHECK (state NOT IN ('validated','published') OR validation IS NOT NULL),
	CONSTRAINT snapshot_native_preparation_validation_order CHECK (validation IS NULL OR (frozen_inventory IS NOT NULL AND state IN ('validated','published','abandoned'))),
	CONSTRAINT snapshot_native_preparation_abandonment CHECK ((state = 'abandoned' AND cancellation_requested AND abandonment_sha256 IS NOT NULL) OR (state <> 'abandoned' AND abandonment_sha256 IS NULL)),
	CONSTRAINT snapshot_native_preparation_publication_shape CHECK (
            (state='published') = (publication IS NOT NULL)
            AND (publication IS NULL) = (publication_sha256 IS NULL)
            AND (publication IS NULL) = (published_at IS NULL)
            AND (published_fence_token IS NULL) = (publication IS NULL OR COALESCE(importer_id IN ('catalog','network','office') AND admission->>'kind'=importer_id,false))
            AND (state <> 'published' OR (NOT cancellation_requested AND CASE
              WHEN importer_id IN ('catalog','network','office') AND admission->>'kind'=importer_id THEN COALESCE(
                publication->>'kind'=importer_id AND publication->>'candidate_id'=generation_id::text
                AND publication->>'fence_token'=publication_fence::text
                AND jsonb_typeof(publication->'data')='object' AND publication->'data'<>'{}'::jsonb,false)
              ELSE published_fence_token=publication_fence END))),
	CONSTRAINT snapshot_native_preparation_claim_shape CHECK ((claim_attempt = 0 AND claim_token IS NULL AND claim_expires_at IS NULL) OR (claim_attempt > 0 AND claim_token IS NOT NULL AND claim_expires_at IS NOT NULL)),
	CONSTRAINT snapshot_native_preparation_admission_sha256 CHECK (admission_sha256 IS NULL OR admission_sha256 ~ '^[0-9a-f]{64}$'),
	CONSTRAINT snapshot_native_preparation_intake_sha256 CHECK (intake_sha256 IS NULL OR intake_sha256 ~ '^[0-9a-f]{64}$'),
	CONSTRAINT snapshot_native_preparation_frozen_sha256 CHECK (frozen_sha256 IS NULL OR frozen_sha256 ~ '^[0-9a-f]{64}$'),
	CONSTRAINT snapshot_native_preparation_validation_sha256 CHECK (validation_sha256 IS NULL OR validation_sha256 ~ '^[0-9a-f]{64}$'),
	CONSTRAINT snapshot_native_preparation_abandonment_sha256 CHECK (abandonment_sha256 IS NULL OR abandonment_sha256 ~ '^[0-9a-f]{64}$'),
	CONSTRAINT snapshot_native_preparation_publication_sha256 CHECK (publication_sha256 IS NULL OR publication_sha256 ~ '^[0-9a-f]{64}$'),
	CONSTRAINT snapshot_native_preparation_admission_object CHECK (admission IS NULL OR jsonb_typeof(admission) = 'object'),
	CONSTRAINT snapshot_native_preparation_handoff_object CHECK (handoff IS NULL OR jsonb_typeof(handoff) = 'object'),
	CONSTRAINT snapshot_native_preparation_intake_inventory_object CHECK (intake_inventory IS NULL OR jsonb_typeof(intake_inventory) = 'object'),
	CONSTRAINT snapshot_native_preparation_frozen_inventory_object CHECK (frozen_inventory IS NULL OR jsonb_typeof(frozen_inventory) = 'object'),
	CONSTRAINT snapshot_native_preparation_validation_object CHECK (validation IS NULL OR jsonb_typeof(validation) = 'object'),
	CONSTRAINT snapshot_native_preparation_publication_object CHECK (publication IS NULL OR jsonb_typeof(publication) = 'object')
)""",
    """CREATE TABLE hp_snapshot_retention.preparation_relation (
	schema_name VARCHAR(63) NOT NULL, relation_name VARCHAR(63) NOT NULL,
	preparation_id UUID NOT NULL,
	PRIMARY KEY (schema_name, relation_name)
)""",
    """CREATE INDEX snapshot_native_preparation_relation_preparation_idx ON hp_snapshot_retention.preparation_relation (preparation_id)""",
    """CREATE TABLE hp_snapshot_retention.preparation_claim (
	lease_token UUID NOT NULL,
	preparation_id UUID NOT NULL,
	attempt INTEGER NOT NULL,
	lease_seconds INTEGER NOT NULL,
	claimed_at TIMESTAMP WITH TIME ZONE DEFAULT clock_timestamp() NOT NULL,
	PRIMARY KEY (lease_token),
	CONSTRAINT snapshot_native_preparation_claim_attempt_key UNIQUE (preparation_id, attempt),
	CONSTRAINT snapshot_native_preparation_claim_attempt CHECK (attempt > 0),
	CONSTRAINT snapshot_native_preparation_claim_seconds CHECK (lease_seconds BETWEEN 1 AND 120)
)""",
    """CREATE TABLE hp_snapshot_retention.relation (
 schema_name varchar(63) NOT NULL,
 relation_name varchar(63) NOT NULL,
 generation_id uuid NOT NULL,
 PRIMARY KEY(schema_name,relation_name)
)""",
    """CREATE INDEX snapshot_retention_relation_generation_idx ON hp_snapshot_retention.relation(generation_id)""",
    """REVOKE ALL ON TABLE hp_snapshot_retention.preparation FROM PUBLIC""",
    """REVOKE ALL ON TABLE hp_snapshot_retention.preparation_relation FROM PUBLIC""",
    """REVOKE ALL ON TABLE hp_snapshot_retention.preparation_claim FROM PUBLIC""",
    """REVOKE ALL ON TABLE hp_snapshot_retention.relation FROM PUBLIC""",
)
# Ordinary importers check retained claims before rotation; they read only the claimed names.
CLAIM_READ_GRANTS = (
    """GRANT USAGE ON SCHEMA hp_snapshot_retention TO PUBLIC""",
    """GRANT SELECT (schema_name,relation_name) ON TABLE hp_snapshot_retention.relation,
 hp_snapshot_retention.preparation_relation TO PUBLIC""",
)


def upgrade_named_preparation_catalog(bind):
    """Create a fresh catalog or validate the shape of a shared catalog.

    Before migrating an externally managed catalog, its owner must provision
    the ordinary importers' retained-claim reads through an authorized claim
    view or the named columns. Named publishers need direct access to their
    preparation inventory under their own role.
    """
    relation_kind_by_name = dict(
        bind.execute(
            text("""SELECT c.relname, c.relkind::text
        FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'hp_snapshot_retention' AND c.relname::text = ANY(CAST(:names AS text[]))
        """),
            {"names": list(TABLES)},
        ).all()
    )
    if relation_kind_by_name:
        if relation_kind_by_name.keys() != set(TABLES):
            raise RuntimeError("named_preparation_catalog_incomplete")
        if any(kind != "r" for kind in relation_kind_by_name.values()):
            raise RuntimeError("named_preparation_catalog_shape_changed")
    else:
        bind.execute(text("CREATE SCHEMA IF NOT EXISTS hp_snapshot_retention"))
        for statement in CATALOG:
            bind.execute(text(statement))
    validate_named_preparation_catalog(bind)
    # An externally administered catalog already has its own claim access policy.
    if not relation_kind_by_name or _can_grant_claim_reads(bind):
        for statement in CLAIM_READ_GRANTS:
            bind.execute(text(statement))


def _can_grant_claim_reads(bind):
    return bind.execute(
        text("""SELECT pg_catalog.has_schema_privilege('hp_snapshot_retention', 'USAGE WITH GRANT OPTION')
          AND bool_and(pg_catalog.has_column_privilege(relation, column_name, 'SELECT WITH GRANT OPTION'))
        FROM (VALUES
          ('hp_snapshot_retention.preparation_relation', 'schema_name'),
          ('hp_snapshot_retention.preparation_relation', 'relation_name'),
          ('hp_snapshot_retention.relation', 'schema_name'),
          ('hp_snapshot_retention.relation', 'relation_name')
        ) AS claims(relation, column_name)""")
    ).scalar_one()


def validate_named_preparation_catalog(bind):
    """Require current named columns_by_table without interpreting stored physical identities."""
    required_columns_by_table = {
        "preparation": {
            "preparation_id",
            "node_id",
            "importer_id",
            "dataset_key",
            "run_id",
            "generation_id",
            "publication_fence",
            "admission_sha256",
            "admission",
            "state",
            "expected_generation_id",
            "expected_fence_token",
            "predecessor_pin_id",
            "attempt_id",
            "handoff",
            "intake_inventory",
            "intake_sha256",
            "frozen_inventory",
            "frozen_sha256",
            "validation",
            "validation_sha256",
            "publication",
            "publication_sha256",
            "published_at",
            "published_fence_token",
            "cancellation_requested",
            "abandonment_sha256",
            "claim_attempt",
            "claim_token",
            "claim_expires_at",
            "created_at",
            "updated_at",
        },
        "preparation_relation": {"schema_name", "relation_name", "preparation_id"},
        "preparation_claim": {"lease_token", "preparation_id", "attempt", "lease_seconds", "claimed_at"},
        "relation": {"schema_name", "relation_name", "generation_id"},
    }
    columns_by_table = {name: set() for name in TABLES}
    for table, column in bind.execute(
        text("""SELECT c.relname, a.attname
        FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid
        WHERE n.nspname = 'hp_snapshot_retention' AND c.relkind = 'r'
          AND c.relname::text = ANY(CAST(:names AS text[])) AND a.attnum > 0 AND NOT a.attisdropped"""),
        {"names": list(TABLES)},
    ):
        columns_by_table[table].add(column)
    if columns_by_table != required_columns_by_table:
        raise RuntimeError("named_preparation_catalog_shape_changed")
