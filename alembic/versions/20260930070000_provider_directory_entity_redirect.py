# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Add exact reviewed organization/site redirects without changing source bindings."""

import os

import sqlalchemy as sa

from alembic import op

revision = "20260930070000_provider_directory_entity_redirect"
down_revision = "20260930060000_cms_doctors_site_binding"
branch_labels = None
depends_on = None

_DECISIONS = "provider_directory_entity_redirect_decision"
_ACTIVE = "provider_directory_entity_redirect"


def _schema():
    return '"' + (os.getenv("HLTHPRT_DB_SCHEMA") or "mrf").replace('"', '""') + '"'


def _create_tables(schema):
    op.execute(f"""CREATE TABLE {schema}.{_DECISIONS} (
        decision_id uuid PRIMARY KEY, action varchar(8) NOT NULL,
        source_id varchar(64) NOT NULL, resource_type varchar(16) NOT NULL,
        old_entity_id uuid NOT NULL, canonical_entity_id uuid NOT NULL,
        old_resource_id varchar(256) NOT NULL, canonical_resource_id varchar(256) NOT NULL,
        old_release_id varchar(256) NOT NULL, canonical_release_id varchar(256) NOT NULL,
        old_payload_sha256 varchar(64) NOT NULL, canonical_payload_sha256 varchar(64) NOT NULL,
        prior_decision_id uuid UNIQUE,
        review_receipt_id varchar(160) NOT NULL, review_receipt_sha256 varchar(64) NOT NULL,
        review_actor varchar(160) NOT NULL, reviewed_at timestamptz NOT NULL,
        UNIQUE (decision_id, source_id, resource_type, old_entity_id, canonical_entity_id),
        CONSTRAINT pd_entity_redirect_kind_check CHECK (resource_type IN ('Organization', 'Location')),
        CONSTRAINT pd_entity_redirect_distinct_check CHECK (old_entity_id <> canonical_entity_id),
        CONSTRAINT pd_entity_redirect_action_check CHECK (
            (action='redirect' AND prior_decision_id IS NULL) OR (action='close' AND prior_decision_id IS NOT NULL)),
        CONSTRAINT pd_entity_redirect_hash_check CHECK (
            old_payload_sha256 ~ '^[0-9a-f]{{64}}$' AND canonical_payload_sha256 ~ '^[0-9a-f]{{64}}$'
            AND review_receipt_sha256 ~ '^[0-9a-f]{{64}}$'),
        CONSTRAINT pd_entity_redirect_text_check CHECK (
            source_id <> '' AND source_id=btrim(source_id)
            AND old_resource_id <> '' AND old_resource_id=btrim(old_resource_id)
            AND canonical_resource_id <> '' AND canonical_resource_id=btrim(canonical_resource_id)
            AND old_release_id <> '' AND old_release_id=btrim(old_release_id)
            AND canonical_release_id <> '' AND canonical_release_id=btrim(canonical_release_id)
            AND review_receipt_id <> '' AND review_receipt_id=btrim(review_receipt_id)
            AND review_actor <> '' AND review_actor=btrim(review_actor)),
        CONSTRAINT pd_entity_redirect_old_evidence_fkey
            FOREIGN KEY (source_id, resource_type, old_resource_id, old_release_id)
            REFERENCES {schema}.provider_directory_entity_release_evidence ON DELETE RESTRICT,
        CONSTRAINT pd_entity_redirect_canonical_evidence_fkey
            FOREIGN KEY (source_id, resource_type, canonical_resource_id, canonical_release_id)
            REFERENCES {schema}.provider_directory_entity_release_evidence ON DELETE RESTRICT,
        CONSTRAINT pd_entity_redirect_prior_fkey
            FOREIGN KEY (prior_decision_id, source_id, resource_type, old_entity_id, canonical_entity_id)
            REFERENCES {schema}.{_DECISIONS}
                (decision_id, source_id, resource_type, old_entity_id, canonical_entity_id) ON DELETE RESTRICT
    )""")
    for side in ("old", "canonical"):
        op.execute(
            f"CREATE INDEX pd_entity_redirect_{side}_evidence_idx ON {schema}.{_DECISIONS} "
            f"(source_id, resource_type, {side}_resource_id, {side}_release_id)"
        )
    op.execute(f"""CREATE TABLE {schema}.{_ACTIVE} (
        source_id varchar(64) NOT NULL, resource_type varchar(16) NOT NULL,
        old_entity_id uuid NOT NULL, canonical_entity_id uuid NOT NULL,
        decision_id uuid NOT NULL UNIQUE, created_at timestamptz NOT NULL,
        PRIMARY KEY (source_id, resource_type, old_entity_id),
        CONSTRAINT pd_entity_redirect_active_kind_check CHECK (resource_type IN ('Organization', 'Location')),
        CONSTRAINT pd_entity_redirect_active_distinct_check CHECK (old_entity_id <> canonical_entity_id),
        CONSTRAINT pd_entity_redirect_active_decision_fkey
            FOREIGN KEY (decision_id, source_id, resource_type, old_entity_id, canonical_entity_id)
            REFERENCES {schema}.{_DECISIONS}
                (decision_id, source_id, resource_type, old_entity_id, canonical_entity_id) ON DELETE RESTRICT
    )""")
    op.execute(
        f"CREATE INDEX pd_entity_redirect_target_idx ON {schema}.{_ACTIVE} "
        "(source_id, resource_type, canonical_entity_id)"
    )


def _create_review_guards(schema):
    op.execute(f"""CREATE FUNCTION {schema}.pd_entity_redirect_review_guard() RETURNS trigger
        LANGUAGE plpgsql SET search_path = pg_catalog AS $$ DECLARE parent record; BEGIN
        IF TG_OP <> 'INSERT' THEN RAISE EXCEPTION 'entity_redirect_review_immutable'; END IF;
        IF current_setting('transaction_isolation') <> 'read committed' THEN
            RAISE EXCEPTION 'entity_redirect_requires_read_committed'; END IF;
        PERFORM pg_advisory_xact_lock(hashtextextended(
            'entity-redirect:' || NEW.source_id || ':' || NEW.resource_type, 0));
        FOR parent IN SELECT * FROM (VALUES
            (NEW.old_entity_id, NEW.old_resource_id, NEW.old_release_id, NEW.old_payload_sha256),
            (NEW.canonical_entity_id, NEW.canonical_resource_id, NEW.canonical_release_id, NEW.canonical_payload_sha256)
        ) AS pair(entity_id, resource_id, release_id, payload_sha256) LOOP
            PERFORM 1 FROM {schema}.provider_directory_entity_release_evidence e
            JOIN {schema}.provider_directory_entity_source_binding b USING (source_id, resource_type, resource_id)
            WHERE e.source_id=NEW.source_id AND e.resource_type=NEW.resource_type
                AND e.resource_id=parent.resource_id AND e.release_id=parent.release_id
                AND e.payload_sha256=parent.payload_sha256
                AND CASE WHEN NEW.resource_type='Organization' THEN b.organization_id ELSE b.site_id END=parent.entity_id
            FOR SHARE OF e, b;
            IF NOT FOUND THEN RAISE EXCEPTION 'entity_redirect_exact_evidence_missing_or_changed'; END IF;
        END LOOP;
        IF NEW.action='close' AND NOT EXISTS (
            SELECT 1 FROM {schema}.{_ACTIVE} a WHERE a.decision_id=NEW.prior_decision_id
            AND a.source_id=NEW.source_id AND a.resource_type=NEW.resource_type
            AND a.old_entity_id=NEW.old_entity_id AND a.canonical_entity_id=NEW.canonical_entity_id
        ) THEN RAISE EXCEPTION 'entity_redirect_close_target_invalid'; END IF;
        RETURN NEW; END $$""")
    op.execute(
        f"CREATE TRIGGER pd_entity_redirect_review_guard BEFORE INSERT OR UPDATE OR DELETE "
        f"ON {schema}.{_DECISIONS} FOR EACH ROW EXECUTE FUNCTION {schema}.pd_entity_redirect_review_guard()"
    )
    for table in (_DECISIONS, _ACTIVE):
        op.execute(
            f"CREATE TRIGGER pd_entity_redirect_truncate_guard BEFORE TRUNCATE ON {schema}.{table} "
            f"FOR EACH STATEMENT EXECUTE FUNCTION {schema}.pd_entity_redirect_review_guard()"
        )


def _create_active_guard(schema):
    op.execute(f"""CREATE FUNCTION {schema}.pd_entity_redirect_active_guard() RETURNS trigger
        LANGUAGE plpgsql SET search_path = pg_catalog AS $$ BEGIN
        IF TG_OP='UPDATE' THEN RAISE EXCEPTION 'entity_redirect_active_immutable'; END IF;
        IF current_setting('transaction_isolation') <> 'read committed' THEN
            RAISE EXCEPTION 'entity_redirect_requires_read_committed'; END IF;
        IF TG_OP='DELETE' THEN
            PERFORM pg_advisory_xact_lock(hashtextextended(
                'entity-redirect:' || OLD.source_id || ':' || OLD.resource_type, 0));
            IF NOT EXISTS (SELECT 1 FROM {schema}.{_DECISIONS} d
                WHERE d.prior_decision_id=OLD.decision_id AND d.action='close') THEN
                RAISE EXCEPTION 'entity_redirect_close_decision_required'; END IF;
            RETURN OLD;
        END IF;
        PERFORM pg_advisory_xact_lock(hashtextextended(
            'entity-redirect:' || NEW.source_id || ':' || NEW.resource_type, 0));
        IF NOT EXISTS (SELECT 1 FROM {schema}.{_DECISIONS} d
            WHERE d.decision_id=NEW.decision_id AND d.action='redirect' AND d.source_id=NEW.source_id
              AND d.resource_type=NEW.resource_type AND d.old_entity_id=NEW.old_entity_id
              AND d.canonical_entity_id=NEW.canonical_entity_id AND d.reviewed_at=NEW.created_at)
            OR EXISTS (SELECT 1 FROM {schema}.{_DECISIONS} d WHERE d.prior_decision_id=NEW.decision_id) THEN
            RAISE EXCEPTION 'entity_redirect_open_decision_required'; END IF;
        IF EXISTS (SELECT 1 FROM {schema}.{_ACTIVE} a
            WHERE a.source_id=NEW.source_id AND a.resource_type=NEW.resource_type
              AND (a.old_entity_id=NEW.canonical_entity_id OR a.canonical_entity_id=NEW.old_entity_id)) THEN
            RAISE EXCEPTION 'entity_redirect_one_hop_required'; END IF;
        RETURN NEW; END $$""")
    op.execute(
        f"CREATE TRIGGER pd_entity_redirect_active_guard BEFORE INSERT OR UPDATE OR DELETE "
        f"ON {schema}.{_ACTIVE} FOR EACH ROW EXECUTE FUNCTION {schema}.pd_entity_redirect_active_guard()"
    )
    op.execute(f"""CREATE FUNCTION {schema}.pd_entity_redirect_atomic_guard() RETURNS trigger
        LANGUAGE plpgsql SET search_path = pg_catalog AS $$ BEGIN
        IF NEW.action='redirect' AND NOT EXISTS (
            SELECT 1 FROM {schema}.{_DECISIONS} d WHERE d.prior_decision_id=NEW.decision_id
        ) AND NOT EXISTS (SELECT 1 FROM {schema}.{_ACTIVE} a WHERE a.decision_id=NEW.decision_id) THEN
            RAISE EXCEPTION 'entity_redirect_active_pointer_required'; END IF;
        IF NEW.action='close' AND EXISTS (
            SELECT 1 FROM {schema}.{_ACTIVE} a WHERE a.decision_id=NEW.prior_decision_id
        ) THEN RAISE EXCEPTION 'entity_redirect_closed_pointer_remains'; END IF;
        RETURN NULL; END $$""")
    op.execute(
        f"CREATE CONSTRAINT TRIGGER pd_entity_redirect_atomic_guard AFTER INSERT ON {schema}.{_DECISIONS} "
        "DEFERRABLE INITIALLY DEFERRED FOR EACH ROW "
        f"EXECUTE FUNCTION {schema}.pd_entity_redirect_atomic_guard()"
    )


def _protect_reviewed_parents(schema):
    op.execute(f"""CREATE FUNCTION {schema}.pd_entity_redirect_parent_guard() RETURNS trigger
        LANGUAGE plpgsql SET search_path = pg_catalog AS $$ BEGIN
        IF current_setting('transaction_isolation') <> 'read committed' THEN
            RAISE EXCEPTION 'entity_redirect_requires_read_committed'; END IF;
        IF TG_TABLE_NAME='provider_directory_entity_source_binding' THEN
            IF EXISTS (SELECT 1 FROM {schema}.{_DECISIONS} d
                WHERE d.source_id=OLD.source_id AND d.resource_type=OLD.resource_type
                  AND (d.old_resource_id=OLD.resource_id OR d.canonical_resource_id=OLD.resource_id)) THEN
                RAISE EXCEPTION 'entity_redirect_reviewed_parent_immutable'; END IF;
        ELSE
            IF EXISTS (SELECT 1 FROM {schema}.{_DECISIONS} d
                WHERE d.source_id=OLD.source_id AND d.resource_type=OLD.resource_type
                  AND ((d.old_resource_id=OLD.resource_id AND d.old_release_id=OLD.release_id)
                    OR (d.canonical_resource_id=OLD.resource_id AND d.canonical_release_id=OLD.release_id))) THEN
                RAISE EXCEPTION 'entity_redirect_reviewed_parent_immutable'; END IF;
        END IF;
        IF TG_OP='DELETE' THEN RETURN OLD; END IF;
        RETURN NEW; END $$""")
    for table in ("provider_directory_entity_source_binding", "provider_directory_entity_release_evidence"):
        op.execute(
            f"CREATE TRIGGER pd_entity_redirect_parent_guard BEFORE UPDATE OR DELETE ON {schema}.{table} "
            f"FOR EACH ROW EXECUTE FUNCTION {schema}.pd_entity_redirect_parent_guard()"
        )


def upgrade():
    """Add an empty review ledger and enforce one-hop, atomic reviewed transitions."""
    schema = _schema()
    _create_tables(schema)
    _create_review_guards(schema)
    _create_active_guard(schema)
    _protect_reviewed_parents(schema)


def downgrade():
    """Permit removal only before any review history has been recorded."""
    schema = _schema()
    op.execute(f"LOCK TABLE {schema}.{_DECISIONS}, {schema}.{_ACTIVE} IN ACCESS EXCLUSIVE MODE")
    if op.get_bind().execute(sa.text(f"SELECT EXISTS (SELECT 1 FROM {schema}.{_DECISIONS})")).scalar_one():
        raise RuntimeError("entity_redirect_downgrade_requires_empty_history")
    for table in ("provider_directory_entity_source_binding", "provider_directory_entity_release_evidence"):
        op.execute(f"DROP TRIGGER pd_entity_redirect_parent_guard ON {schema}.{table}")
    op.execute(f"DROP TABLE {schema}.{_ACTIVE}")
    op.execute(f"DROP TABLE {schema}.{_DECISIONS}")
    for name in ("review", "active", "atomic", "parent"):
        op.execute(f"DROP FUNCTION {schema}.pd_entity_redirect_{name}_guard()")
