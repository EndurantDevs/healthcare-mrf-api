# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Install a first-use protected slice without modifying its legacy catalog."""

from types import SimpleNamespace

from sqlalchemy import text

from process import reference_family_archive as native
from process import scoped_catalog_named_generation as generations
from process import scoped_catalog_named_storage as storage
from process import scoped_catalog_publication as publication
from process.scoped_catalog_layout import CONTROL_SCHEMA, DATA_SCHEMA, FENCE_OWNER_ROLE, FENCE_SCHEMA, FENCE_VIEW


async def _require_unused_catalog(session):
    """Lock out new control reservations while validating the first-use boundary."""
    await native._lock_family(session, CONTROL_SCHEMA, ("import_run",), "SHARE ROW EXCLUSIVE", nowait=True)
    if (
        await session.scalar(
            text(
                "SELECT EXISTS(SELECT 1 FROM mrf.import_run WHERE importer IN ('code-sets','ms-drg') AND "
                "(status NOT IN ('succeeded','failed','canceled','cancelled','dead_letter') "
                "OR metrics::jsonb ?| ARRAY['scoped_catalog_publication','code_sets_mutable_publication']))"
                " OR EXISTS(SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
                "WHERE c.relname=ANY(CAST(:controls AS text[])) AND n.nspname<>:schema)"
            ),
            {"controls": list(generations.TABLES.values()), "schema": CONTROL_SCHEMA},
        )
        is not False
    ):
        raise RuntimeError("existing catalog attempts or retained generations prevent split")
    vector = await generations.read_generations(session, CONTROL_SCHEMA, lock=True)
    if set(vector) != set(generations.TABLES) or any(
        part["local_generation"] != 0 or part["origin_generation"] is not None for part in vector.values()
    ):
        raise RuntimeError("published catalog generations prevent split")


async def _copy_controls(session):
    for importer, table in generations.TABLES.items():
        source = storage.qualified(CONTROL_SCHEMA, table)
        destination = storage.qualified(DATA_SCHEMA, table)
        # Defaults may refer to a sequence owned by the legacy schema. This control is always a singleton.
        await session.execute(
            text(f"CREATE TABLE {destination} (LIKE {source} INCLUDING CONSTRAINTS INCLUDING INDEXES)")
        )
        if importer == "code-sets":
            await session.execute(
                text(f"ALTER TABLE {destination} DROP CONSTRAINT IF EXISTS code_sets_generation_shape_check")
            )
            await session.execute(
                text(
                    f"ALTER TABLE {destination} ADD CONSTRAINT code_sets_logical_generation_shape_check CHECK ("
                    "(origin_lineage_id IS NULL AND origin_generation IS NULL AND published_at IS NULL "
                    "AND row_count IS NULL AND row_sha256 IS NULL) OR "
                    "(origin_lineage_id IS NOT NULL AND origin_generation IS NOT NULL AND origin_generation>0 "
                    "AND published_at IS NOT NULL AND row_count IS NOT NULL AND row_count>=0 "
                    "AND row_sha256 IS NOT NULL AND row_sha256 ~ '^[0-9a-f]{64}$'))"
                )
            )
        await session.execute(text(f"INSERT INTO {destination} SELECT * FROM {source}"))


async def _install_legacy_catalog_fence(session, publisher_role):
    """Expose only a named transaction lock on the three legacy heaps."""
    schema = native._quoted(FENCE_SCHEMA)
    owner = native._quoted(FENCE_OWNER_ROLE)
    publisher = native._quoted(publisher_role)
    view = storage.qualified(FENCE_SCHEMA, FENCE_VIEW)
    relations = " CROSS JOIN ".join(
        storage.qualified(CONTROL_SCHEMA, name) for name in sorted(model.__tablename__ for model in storage.MODELS)
    )
    await session.execute(text(f"CREATE SCHEMA {schema} AUTHORIZATION {owner}"))
    await session.execute(text(f"REVOKE ALL ON SCHEMA {schema} FROM PUBLIC"))
    await session.execute(text(f"CREATE VIEW {view} AS SELECT 1 AS marker FROM {relations} WHERE false"))
    await session.execute(text(f"ALTER VIEW {view} OWNER TO {owner}"))
    await session.execute(text(f"REVOKE ALL ON {view} FROM PUBLIC"))
    await session.execute(text(f"GRANT USAGE ON SCHEMA {schema} TO {publisher}"))
    await session.execute(text(f"GRANT MAINTAIN ON {view} TO {publisher}"))


async def bootstrap_catalog_slice(session, policy):
    """Seed one scoped catalog under the migration role in its owning transaction."""
    native._require_transaction(session)
    if type(policy) is not publication.CatalogPublicationPolicy or policy.schema != DATA_SCHEMA:
        raise ValueError("protected catalog bootstrap configuration differs")
    await _require_unused_catalog(session)
    await storage.require_tables(session, CONTROL_SCHEMA, storage.MODELS, lock="SHARE")
    await _install_legacy_catalog_fence(session, policy.owner_role)
    await session.execute(text(f"CREATE SCHEMA {native._quoted(DATA_SCHEMA)}"))
    for model in storage.MODELS:
        table = model.__tablename__
        source, destination = storage.qualified(CONTROL_SCHEMA, table), storage.qualified(DATA_SCHEMA, table)
        await session.execute(text(f"CREATE TABLE {destination} (LIKE {source} INCLUDING ALL)"))
        await session.execute(
            text(f"INSERT INTO {destination} SELECT * FROM {source} WHERE source=ANY(CAST(:sources AS text[]))"),
            {"sources": list(publication.catalog_sources(model))},
        )
    await _copy_controls(session)
    native_spec = native.ReferenceFamilySpec("scoped-catalog", storage.MODELS)
    await publication.validate_catalog_semantics(session, native_spec, DATA_SCHEMA)
    vector = await generations.read_generations(session, DATA_SCHEMA)
    await generations.validate_published_content(session, DATA_SCHEMA, vector)
    controls = tuple(SimpleNamespace(__tablename__=name) for name in generations.TABLES.values())
    await storage.protect_tables(session, DATA_SCHEMA, (*storage.MODELS, *controls), policy, candidate=True)
    return await storage.table_receipts(session, DATA_SCHEMA, storage.MODELS)
