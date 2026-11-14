# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Transaction-pinned pricing reads across native family and shared code slices."""

from sqlalchemy import and_, bindparam, exists, func, select, text, union, union_all


async def pin_claims_reader(session, *, api_read=False):
    """Pin every installed physician/Rx family before the request's first data read."""
    if type(api_read) is not bool:
        raise ValueError("claims reader mode is invalid")
    if api_read:
        return await _pin_claims_api_reader(session)
    from process.mrf_address_publication import require_native_read_catalog
    from process.reference_family_archive import (
        TERMINAL_CAPABILITIES,
        _lock_family,
        _relation_oid,
        reference_family_receive_spec,
    )

    await session.connection(execution_options={"isolation_level": "REPEATABLE READ"})
    pairs_by_importer = {}
    for importer, capability in TERMINAL_CAPABILITIES.items():
        spec = reference_family_receive_spec(importer)
        pairs = tuple([(name, await _relation_oid(session, "mrf", name)) for name in sorted(spec.table_names)])
        scoped_names = {model.__tablename__ for model in (*capability.dictionary_models, *capability.effect_models)}
        if not any(oid for name, oid in pairs if name in scoped_names):
            continue
        if any(oid is None for _name, oid in pairs):
            raise RuntimeError("installed claims reader family is incomplete")
        pairs_by_importer[importer] = pairs
    if not pairs_by_importer:
        await session.rollback()  # Discard only this catalog probe; ordinary requests retain default isolation.
        return None  # Ordinary, uninstalled pricing continues through its existing entrypoint.
    pairs = tuple(sorted(pair for family_pairs in pairs_by_importer.values() for pair in family_pairs))
    await _lock_family(
        session,
        "mrf",
        tuple(name for name, _oid in pairs) + ("code_catalog", "code_crosswalk"),
        "ACCESS SHARE",
        nowait=True,
    )
    # regclass resolves the actually locked current name, not an older catalog row version.
    actual_pairs = tuple(
        [
            (name, await session.scalar(text("SELECT to_regclass(:name)::oid"), {"name": f'"mrf"."{name}"'}))
            for name, _oid in pairs
        ]
    )
    if actual_pairs != pairs:
        raise RuntimeError("installed claims reader identity changed")
    await require_native_read_catalog(session, tuple(oid for _name, oid in pairs))
    for importer, family_pairs in pairs_by_importer.items():
        if importer == "claims-pricing":
            await _require_claims_reader_binding(session, family_pairs)
        else:
            await _require_claims_reader_binding(session, family_pairs, importer_id=importer)
    await _set_claims_reader_dictionary(session)
    if "drug-claims" in pairs_by_importer:
        await _require_dictionary_overlap(session, tuple(pairs_by_importer))
        session.info["snapshot_claims_dictionary_tables"] = _claims_dictionary_overlay(tuple(pairs_by_importer))
    return pairs


def _claims_relation_sql(session, schema, table):
    from process.network_compact_layout import compact_identifier

    return compact_identifier(schema) + "." + compact_identifier(table)


async def _claims_api_snapshot(session):
    """Require the caller's read-only repeatable transaction."""
    transaction = session.get_transaction()
    if transaction is None:
        raise RuntimeError("claims reader snapshot is unavailable")
    modes = (
        await session.execute(
            text("SELECT current_setting('transaction_isolation'),current_setting('transaction_read_only')")
        )
    ).one()
    if tuple(modes) not in (("repeatable read", "on"), ("serializable", "on")):
        raise RuntimeError("claims reader requires a read-only repeatable-read snapshot")
    return transaction


async def _claims_api_inventory(session):
    """Census every terminal-family, shared-dictionary and binding name, including absence."""
    from process.reference_family_archive import TERMINAL_CAPABILITIES, reference_family_receive_spec

    async def available(schema, name):
        """Report whether the qualified family relation is installed."""
        return await session.scalar(text("SELECT to_regclass(:name) IS NOT NULL"), {"name": f'"{schema}"."{name}"'})

    pairs_by_importer, oid_by_name = {}, {}
    for importer, capability in TERMINAL_CAPABILITIES.items():
        spec = reference_family_receive_spec(importer)
        pairs = tuple([(name, await available("mrf", name)) for name in sorted(spec.table_names)])
        oid_by_name.update({("mrf", name): oid for name, oid in pairs})
        scoped_names = {model.__tablename__ for model in (*capability.dictionary_models, *capability.effect_models)}
        if not any(oid for name, oid in pairs if name in scoped_names):
            continue
        if any(not oid for _name, oid in pairs):
            raise RuntimeError("installed claims reader family is incomplete")
        pairs_by_importer[importer] = pairs
    lookup_groups = [("mrf", ("code_catalog", "code_crosswalk"))]
    if pairs_by_importer:
        lookup_groups.append(("hp_snapshot_retention", ("current_generation", "relation")))
    for schema, names in lookup_groups:
        for name in names:
            oid_by_name[(schema, name)] = await available(schema, name)
    return pairs_by_importer, oid_by_name


async def _pin_claims_api_reader(session):
    """Validate installed family completeness and dictionary relationships in one snapshot."""
    transaction = await _claims_api_snapshot(session)
    pairs_by_importer, _availability = await _claims_api_inventory(session)
    if not pairs_by_importer:
        return None
    pairs = tuple(sorted(pair for family_pairs in pairs_by_importer.values() for pair in family_pairs))
    for importer, family_pairs in pairs_by_importer.items():
        await _require_claims_reader_binding(session, family_pairs, importer_id=importer)
    await _set_claims_reader_dictionary(session)
    if "drug-claims" in pairs_by_importer:
        await _require_dictionary_overlap(session, tuple(pairs_by_importer))
        session.info["snapshot_claims_dictionary_tables"] = _claims_dictionary_overlay(tuple(pairs_by_importer))
    if session.get_transaction() is not transaction:
        raise RuntimeError("claims reader snapshot changed")
    return pairs


async def _set_claims_reader_dictionary(session):
    """A later ordinary shared writer must not substitute code data for this pinned family."""
    session.info["snapshot_claims_dictionary_tables"] = _claims_dictionary_overlay()


def claims_filter_crosswalk_query(session):
    """Use the admitted native dictionaries without changing ordinary provider filters."""
    crosswalk_relation = _claims_relation_sql(session, "mrf", "code_crosswalk")
    sql = text(
        f"""
        SELECT DISTINCT to_code
          FROM {crosswalk_relation}
         WHERE UPPER(from_system) = :from_system
           AND UPPER(from_code) = ANY(:input_codes)
           AND UPPER(to_system) = :target_system
        UNION
        SELECT DISTINCT from_code
          FROM {crosswalk_relation}
        WHERE UPPER(to_system) = :from_system
           AND UPPER(to_code) = ANY(:input_codes)
           AND UPPER(from_system) = :target_system
        """
    )
    if session is not None and "snapshot_claims_dictionary_tables" in getattr(session, "info", {}):
        _catalog, crosswalk = claims_dictionary_tables(session)
        sql = union(
            select(crosswalk.c.to_code).where(
                func.upper(crosswalk.c.from_system) == bindparam("from_system"),
                func.upper(crosswalk.c.from_code).in_(bindparam("input_codes", expanding=True)),
                func.upper(crosswalk.c.to_system) == bindparam("target_system"),
            ),
            select(crosswalk.c.from_code).where(
                func.upper(crosswalk.c.to_system) == bindparam("from_system"),
                func.upper(crosswalk.c.to_code).in_(bindparam("input_codes", expanding=True)),
                func.upper(crosswalk.c.from_system) == bindparam("target_system"),
            ),
        )
    return sql


def claims_dictionary_tables(session):
    """Use pinned scoped rows when admitted; preserve ordinary dictionary entrypoints otherwise."""
    from db.models import CodeCatalog, CodeCrosswalk

    return getattr(session, "info", {}).get(
        "snapshot_claims_dictionary_tables", (CodeCatalog.__table__, CodeCrosswalk.__table__)
    )


async def _query_catalog_neighbors(session, pairs: set[tuple[str, str]]) -> set[tuple[str, str]]:
    """Return same-name neighbors from the request's pinned code catalog."""

    code_catalog_table = claims_dictionary_tables(session)[0]
    if not pairs:
        return set()

    named_pairs = set()
    for system, code in pairs:
        name_result = await session.execute(
            select(code_catalog_table.c.display_name)
            .where(
                and_(
                    func.upper(code_catalog_table.c.code_system) == system,
                    func.upper(code_catalog_table.c.code) == code,
                )
            )
            .limit(1),
        )
        display_name = name_result.scalar()
        if not display_name:
            continue
        named_pairs.add((system, code))
        neighbors = await session.execute(
            select(code_catalog_table.c.code_system, code_catalog_table.c.code).where(
                func.lower(code_catalog_table.c.display_name) == str(display_name).strip().lower()
            ),
        )
        for neighbor_row in neighbors:
            pair = (str(neighbor_row[0]).upper(), str(neighbor_row[1]).upper())
            named_pairs.add(pair)
    return named_pairs


def _claims_dictionary_overlay(importers=("claims-pricing",)):
    """Pinned native keys win; unrelated shared rows remain available through indexed anti-joins."""
    from db.models import CodeCatalog, CodeCrosswalk
    from process.reference_family_archive import _CLAIMS_DICTIONARY_SOURCE, TERMINAL_CAPABILITIES

    tables = []
    for position, model in enumerate((CodeCatalog, CodeCrosswalk)):
        live = model.__table__
        mirrors = [TERMINAL_CAPABILITIES[importer].dictionary_models[position].__table__ for importer in importers]
        selectors = []
        for source in (live, *mirrors):
            exclusions = mirrors if source is live else mirrors[: mirrors.index(source)]
            selector = select(*source.c)
            if source is live and "claims-pricing" in importers:
                selector = selector.where(live.c.source.is_distinct_from(_CLAIMS_DICTIONARY_SOURCE))
            for mirror in exclusions:
                collision = and_(
                    *(source.c[column.name] == mirror.c[column.name] for column in live.primary_key.columns)
                )
                selector = selector.where(~exists(select(1).where(collision).correlate(source)))
            selectors.append(selector)
        tables.append(union_all(*selectors).subquery(live.name))
    return tuple(tables)


async def _require_dictionary_overlap(session, importers):
    """Shared immutable keys cannot silently select different installed payloads."""
    from process.reference_family_archive import TERMINAL_CAPABILITIES, _dictionary_key_join

    if len(importers) < 2:
        return
    for left, right in zip(
        TERMINAL_CAPABILITIES[importers[0]].dictionary_models,
        TERMINAL_CAPABILITIES[importers[1]].dictionary_models,
        strict=True,
    ):
        left_relation = _claims_relation_sql(session, "mrf", left.__tablename__)
        right_relation = _claims_relation_sql(session, "mrf", right.__tablename__)
        if await session.scalar(
            text(
                f"SELECT EXISTS(SELECT 1 FROM {left_relation} left_row JOIN {right_relation} right_row ON {_dictionary_key_join(left, 'left_row', 'right_row')} WHERE to_jsonb(left_row) IS DISTINCT FROM to_jsonb(right_row))"
            )
        ):
            raise RuntimeError("installed claims dictionary payloads conflict")


async def _require_claims_reader_binding(session, pairs, *, importer_id="claims-pricing"):
    """Require one current logical publication for the selected claims family."""
    generations = _claims_relation_sql(session, "hp_snapshot_retention", "current_generation")
    binding_rows = (
        await session.execute(
            text(
                f"SELECT generation_id::text FROM {generations} WHERE importer_id=:importer AND dataset_key=:importer"
            ),
            {"importer": importer_id},
        )
    ).all()
    if len(binding_rows) != 1 or not binding_rows[0][0]:
        raise RuntimeError("installed claims reader publication changed")
