# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Authenticated prospective canonical coverage for selected native families."""

from collections.abc import Mapping
from contextlib import asynccontextmanager
from uuid import uuid4

from sqlalchemy import text


async def _selection_projection(session, schema_name, owner_oid, selected_relations, authenticators, alias_authority):
    """Authenticate and fence the exact base and selected native inventories."""
    from process import reference_family_archive as archive
    from process.entity_address_snapshot_preparation import _publisher_authority
    from process.mrf_address_publication import (
        canonical_archive_projection,
        require_canonical_output_base,
        require_canonical_publication_catalog,
        validate_canonical_contributions,
    )

    authenticate_inventory, authenticate_base = authenticators
    archive._require_transaction(session)
    if not callable(authenticate_inventory) or await _publisher_authority(session) != owner_oid:
        raise archive.ReferenceFamilyArchiveError("selected canonical publisher authority differs")
    inventories = await authenticate_inventory(session)
    if not isinstance(inventories, Mapping) or not inventories or not set(inventories) <= {"npi", "mrf"}:
        raise archive.ReferenceFamilyArchiveError("selected canonical contribution scope differs")
    relations, contributions = archive._selected_canonical_sources(inventories, selected_relations, owner_oid)
    base = f"{archive._quoted(schema_name)}.{archive._quoted(archive.archive_table_name())}"
    await session.execute(text(f"LOCK TABLE {base} IN SHARE ROW EXCLUSIVE MODE NOWAIT"))
    if authenticate_base is None:
        await require_canonical_publication_catalog(session, base)
    else:
        await require_canonical_output_base(session, base, await authenticate_base(session, base))
    await session.execute(
        text(
            "LOCK TABLE "
            + ",".join(
                f"{archive._quoted(relation['schema_name'])}.{archive._quoted(relation['relation_name'])}"
                for relation in sorted(relations, key=lambda relation: relation["relation_oid"])
            )
            + " IN ACCESS SHARE MODE NOWAIT"
        )
    )
    await archive._require_selected_mrf_catalog(
        session, sorted(relations, key=lambda relation: relation["relation_oid"]), owner_oid
    )
    alias_arguments = {} if alias_authority is None else {"npi_alias_authority": alias_authority}
    await validate_canonical_contributions(session, contributions, base, **alias_arguments)
    return canonical_archive_projection(contributions, base, **alias_arguments)


@asynccontextmanager
async def selected_canonical_archive(
    session,
    *,
    schema_name,
    owner_oid,
    selected_relations,
    authenticate_inventory,
    authenticate_base=None,
    npi_alias_authority=None,
):
    """Use complete authenticated NPI/MRF heaps for prospective canonical coverage."""
    from process import reference_family_archive as archive

    projection = await _selection_projection(
        session,
        schema_name,
        owner_oid,
        selected_relations,
        (authenticate_inventory, authenticate_base),
        npi_alias_authority,
    )
    view_name = "address_canonical_validation_" + uuid4().hex
    await session.execute(text(f"CREATE TEMP VIEW {archive._quoted(view_name)} AS " + projection))
    temporary_schema = await session.scalar(
        text("SELECT nspname::text FROM pg_catalog.pg_namespace WHERE oid=pg_catalog.pg_my_temp_schema()")
    )
    try:
        yield (archive._schema_name(temporary_schema), view_name)
    except BaseException:
        raise
    else:
        await session.execute(
            text(f"DROP VIEW {archive._quoted(temporary_schema)}.{archive._quoted(view_name)} RESTRICT")
        )
