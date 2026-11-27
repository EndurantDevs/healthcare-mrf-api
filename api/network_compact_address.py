# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Build identical raw and typed address reads from verified compact coordinates."""

import re

from sqlalchemy import String, bindparam, column, text
from sqlalchemy.dialects.postgresql import ARRAY, INTEGER
from sqlalchemy.dialects.postgresql.asyncpg import PGDialect_asyncpg

from db.models import EntityAddressUnified
from process.network_compact_layout import CompactAddressRelation, CompactNetworkLayout, compact_identifier


def _relation(relation):
    return compact_identifier(relation.schema_name) + "." + compact_identifier(relation.table_name)


def compact_address_parameters(layout):
    """Return reserved physical-artifact binds for the trusted request integration."""
    if type(layout) is not CompactNetworkLayout:
        raise ValueError("Verified compact layout is required")
    return {f"_compact_artifact_{index}": artifact.candidate_id for index, artifact in enumerate(layout.artifacts)}


def compact_address_read_relations(layout):
    """List only physical relations rendered by the actual address query."""
    if type(layout) is not CompactNetworkLayout:
        raise ValueError("Verified compact layout is required")
    relations = [layout.base]
    if layout.custom is not None:
        relations.append(layout.custom)
    if layout.artifacts:
        relations.append(layout.ref_map)
        relations.extend(
            CompactAddressRelation(artifact.schema_name, "network_office_edge") for artifact in layout.artifacts
        )
    return tuple(relations)


def _membership_arms(layout, *, selected, location_lookup=False):
    arms = []
    for index, artifact in enumerate(layout.artifacts):
        ref_map = _relation(layout.ref_map)
        edge = compact_identifier(artifact.schema_name) + ".network_office_edge"
        selector = "AND ref.network_id=ANY(CAST(:_canonical_network_ids AS integer[]))" if selected else ""
        artifact_filter = f"ref.artifact_candidate_id=CAST(:_compact_artifact_{index} AS uuid)"
        if selected and location_lookup:
            # Probe the location index once with all selected slices in its bucket.
            arms.append(f"""SELECT edge.location_key FROM ONLY {edge} edge
                WHERE edge.location_key=office.location_key AND edge.slice_id=ANY(ARRAY(
                    SELECT ref.slice_id FROM ONLY {ref_map} ref
                    WHERE {artifact_filter} {selector}
                    AND ref.office_bucket=(get_byte(sha256(convert_to(office.location_key,'UTF8')),0)&63)
                ))""")
            continue
        projected = "edge.location_key" if selected else "ref.network_id"
        office = "" if selected else "AND edge.location_key=office.location_key"
        arms.append(f"""SELECT {projected} FROM ONLY {ref_map} ref JOIN ONLY {edge} edge
            ON edge.slice_id=ref.slice_id WHERE {artifact_filter} {office} {selector}""")
    memberships = " UNION ALL ".join(arms)
    if selected and location_lookup and memberships:
        # Keep membership probes inside each already-bounded office lookup.
        return "SELECT location_key FROM (" + memberships + ") bucket_members OFFSET 0"
    return memberships


def _selected_office_predicate(layout, *, location_lookup):
    selected = _membership_arms(layout, selected=True, location_lookup=location_lookup)
    if not selected:
        return "FALSE"
    membership = f"office.location_key IN ({selected})"
    if location_lookup:
        # Keep the correlated probe after the office filters instead of flattening it into a join.
        return f"({membership}) IS TRUE"
    return f"EXISTS ({selected}) AND {membership}"


def _validate_address_selection(layout, network_ids, visible_membership_sql, location_lookup):
    compact_address_parameters(layout)
    if type(location_lookup) is not bool:
        raise ValueError("Compact location lookup must be boolean")
    if network_ids is not None and (
        type(network_ids) is not tuple
        or not 1 <= len(network_ids) <= 100
        or any(type(network_id) is not int or not 1 <= network_id <= 2147483647 for network_id in network_ids)
        or network_ids != tuple(sorted(set(network_ids)))
    ):
        raise ValueError("Compact network selector is invalid")
    if visible_membership_sql is not None and (
        type(visible_membership_sql) is not str or not visible_membership_sql.strip()
    ):
        raise ValueError("Trusted membership visibility predicate is invalid")


def _projection_column_names(columns):
    model_names = tuple(col.name for col in EntityAddressUnified.__table__.columns)
    if columns is None:
        return model_names
    if type(columns) is not tuple or not columns:
        raise ValueError("Compact address projection is invalid")
    if any(type(name) is not str or name not in model_names for name in columns) or len(set(columns)) != len(columns):
        raise ValueError("Compact address projection is invalid")
    return columns


def _office_rows(layout, physical_columns):
    base = f"SELECT {physical_columns} FROM ONLY {_relation(layout.base)} base WHERE row_origin<>'network_custom'"
    if layout.custom is not None:
        base += f" AND NOT EXISTS(SELECT 1 FROM ONLY {_relation(layout.custom)} custom WHERE custom.location_key=base.location_key)"
        base += f" UNION ALL SELECT {physical_columns} FROM ONLY {_relation(layout.custom)}"
    return base


def compact_selected_offices(layout, network_ids):
    """Read each selected office once for nationwide page discovery."""
    _validate_address_selection(layout, network_ids, None, False)
    selected = _membership_arms(layout, selected=True)
    if not selected:
        selected = "SELECT NULL::text AS location_key WHERE FALSE"
    keys = f"""selected_office_keys AS MATERIALIZED (SELECT DISTINCT location_key FROM ({selected}) selected),
        selected_membership_bound AS MATERIALIZED (
            SELECT COALESCE(SUM(ref.row_count),0) AS upper_bound FROM ONLY {_relation(layout.ref_map)} ref
            WHERE ref.network_id=ANY(CAST(:_canonical_network_ids AS integer[]))
        )"""
    columns = ",".join(
        compact_identifier(col.name)
        for col in EntityAddressUnified.__table__.columns
        if col.name != "canonical_network_ids"
    )
    base = _office_rows(layout, columns)
    offices = (
        "(SELECT office.* FROM selected_office_keys key CROSS JOIN LATERAL "
        f"(SELECT native.* FROM ({base}) native WHERE native.location_key=key.location_key OFFSET 0) office)"
    )
    return keys, offices


def compact_city_page(layout, network_ids, address_type, provider_probe=""):
    """Intersect selected offices and state before fetching each matching city's first providers."""
    search = compact_city_search_relation(layout, network_ids)
    if search is not None:
        # A provider-level probe filters in index order, so the page still stops at its limit.
        page = f"""SELECT DISTINCT coalesce(c.npi,c.inferred_npi) AS npi FROM {search} c {provider_probe}
          WHERE c.state_name=:state AND c.city_name=:city AND {address_type}
          ORDER BY npi LIMIT :limit OFFSET :start"""
        return None, page, "FALSE"
    selected = _membership_arms(layout, selected=True) or "SELECT NULL::text AS location_key WHERE FALSE"
    offices = _office_rows(layout, "location_key,npi,inferred_npi,state_name,type")
    cities = _office_rows(layout, "location_key,city_name")
    refs = _relation(layout.ref_map)
    # Cap nationwide key enumeration. The ordinary provider page handles larger selections.
    small_selection = "(SELECT total <= 500000 FROM selected_office_bound)"
    keys = f"""selected_office_bound AS MATERIALIZED (
      SELECT coalesce(sum(row_count),0) AS total FROM ONLY {refs}
      WHERE network_id=ANY(CAST(:_canonical_network_ids AS integer[]))
    ), selected_city_keys AS MATERIALIZED (
      SELECT DISTINCT location_key FROM ({selected}) keys WHERE {small_selection}
    ), selected_state_offices AS MATERIALIZED (
      SELECT c.location_key,coalesce(c.npi,c.inferred_npi) AS npi
      FROM selected_city_keys k JOIN ({offices}) c USING(location_key)
      WHERE c.state_name=:state AND {address_type} ORDER BY npi
    )"""
    page = f"""SELECT DISTINCT ON (office.npi) office.npi FROM selected_state_offices office
      JOIN LATERAL (SELECT FROM ({cities}) address WHERE address.location_key=office.location_key
        AND address.city_name=:city LIMIT 1) city ON true
      ORDER BY office.npi LIMIT :limit OFFSET :start"""
    return keys, page, f"NOT {small_selection}"


def compact_city_search_relation(layout, network_ids):
    """Read the complete effective office projection for the pinned network selection."""
    _validate_address_selection(layout, network_ids, None, False)
    if layout.city_search is None or network_ids is None:
        return None
    return (
        f"(SELECT npi,inferred_npi,state_name,city_name,type FROM ONLY {_relation(layout.city_search)} "
        "WHERE canonical_network_ids && CAST(:_canonical_network_ids AS integer[]))"
    )


def compact_npi_page(layout, network_ids, address_type, full_page):
    """Fill a small page from the indexed office prefix, or run the full selection."""
    _validate_address_selection(layout, network_ids, None, True)
    offices = _office_rows(layout, "location_key,npi,inferred_npi,type")
    if layout.geo_search is not None and layout.artifacts:
        membership = (
            f"EXISTS (SELECT FROM ONLY {_relation(layout.geo_search)} projected "
            "WHERE projected.location_key=office.location_key "
            "AND projected.canonical_network_ids OPERATOR(public.&&) CAST(:_canonical_network_ids AS integer[]) "
            "OFFSET 0)"
        )
    else:
        membership = _selected_office_predicate(layout, location_lookup=True)
    # A full page is exact: every office with a lower NPI was in the prefix.
    return f"""
    WITH prefix_page AS MATERIALIZED (
        SELECT DISTINCT office.effective_npi AS npi
          FROM (
              SELECT c.location_key, COALESCE(c.npi, c.inferred_npi) AS effective_npi
                FROM ({offices}) c
               WHERE {address_type} AND COALESCE(c.npi, c.inferred_npi) IS NOT NULL
               ORDER BY effective_npi
               LIMIT 32768
          ) office
         WHERE {membership}
         ORDER BY npi
         LIMIT :limit OFFSET :start
    ), prefix_filled AS (SELECT COUNT(*) = :limit AS filled FROM prefix_page)
    SELECT npi FROM prefix_page WHERE (SELECT filled FROM prefix_filled)
    UNION ALL
    SELECT npi FROM ({full_page}) full_page WHERE NOT (SELECT filled FROM prefix_filled)
    """


def _address_query(layout, network_ids, visible_membership_sql, location_lookup, columns=None, *, zip_lookup=False):
    _validate_address_selection(layout, network_ids, visible_membership_sql, location_lookup)
    requested_columns = _projection_column_names(columns)
    hidden_columns = (
        ("location_key",) if network_ids is not None or "canonical_network_ids" in requested_columns else ()
    )
    if network_ids is None:
        hidden_columns += ("row_origin",)
    working_columns = requested_columns + tuple(name for name in hidden_columns if name not in requested_columns)
    physical_columns = ",".join(compact_identifier(name) for name in working_columns if name != "canonical_network_ids")
    base = _office_rows(layout, physical_columns)
    memberships = _membership_arms(layout, selected=False) if "canonical_network_ids" in working_columns else ""
    arrays = (
        f"ARRAY(SELECT DISTINCT membership.network_id FROM ({memberships}) membership ORDER BY membership.network_id)"
        if memberships
        else "'{}'::integer[]"
    )
    output_columns = ",".join(
        arrays + " AS canonical_network_ids"
        if name == "canonical_network_ids"
        else "office." + compact_identifier(name)
        for name in working_columns
    )
    query = f"SELECT {output_columns} FROM ({base}) office"
    if network_ids is not None and zip_lookup and layout.geo_search is not None and layout.artifacts:
        # Probe membership by office key after ZIP filtering, including offices without coordinates.
        scoped = (
            query
            + f""" WHERE EXISTS (
            SELECT FROM ONLY {_relation(layout.geo_search)} projected
             WHERE projected.location_key=office.location_key
               AND projected.canonical_network_ids OPERATOR(public.&&) CAST(:_canonical_network_ids AS integer[])
             OFFSET 0)"""
        )
    elif network_ids is not None:
        scoped = query + " WHERE " + _selected_office_predicate(layout, location_lookup=location_lookup)
    else:
        visibility = "row_origin IN ('base','ptg_overlay')"
        if visible_membership_sql is not None:
            visibility += " OR (" + visible_membership_sql + ")"
        scoped = f"SELECT * FROM ({query}) visible_office WHERE {visibility}"
    if working_columns != requested_columns:
        requested = ",".join(compact_identifier(name) for name in requested_columns)
        return f"SELECT {requested} FROM ({scoped}) projected_office"
    return scoped


def compact_address_relation_sql(
    layout, *, network_ids=None, visible_membership_sql=None, location_lookup=False, zip_lookup=False
):
    """Return a parenthesized relation; selector/visibility binds belong to the caller.

    Layout and visibility SQL come only from trusted manifest/policy integration.
    Neither this query builder nor its DTOs establish custody or authorize reads.
    Location lookups require an independently bounded outer address query.
    """
    return (
        "(" + _address_query(layout, network_ids, visible_membership_sql, location_lookup, zip_lookup=zip_lookup) + ")"
    )


def _bound_address_query(layout, network_ids, visible_membership_sql, *, location_lookup=False, columns=None):
    sql = _address_query(layout, network_ids, visible_membership_sql, location_lookup, columns)
    query = text(sql)
    for name, value in compact_address_parameters(layout).items():
        if name in query._bindparams:
            query = query.bindparams(bindparam(name, value=value, type_=String()))
    if network_ids is not None and layout.artifacts:
        query = query.bindparams(bindparam("_canonical_network_ids", value=list(network_ids), type_=ARRAY(INTEGER)))
    return query


def compact_address_selectable(layout, *, network_ids=None, visible_membership_sql=None, location_lookup=False):
    """Expose model-column parity with raw SQL, including typed reserved binds."""
    query = _bound_address_query(layout, network_ids, visible_membership_sql, location_lookup=location_lookup)
    columns = [column(col.name, col.type) for col in EntityAddressUnified.__table__.columns]
    return query.columns(*columns).subquery("entity_address_unified")


def compact_address_native_sql(
    layout, *, first_parameter, network_ids=None, visible_membership_sql=None, location_lookup=False, columns=None
):
    """Append typed binds to existing asyncpg parameters for the same logical rows.

    Only the common generated SQL is compiled; selectors and artifact UUIDs stay
    bound. Visibility expressions are trusted policy SQL without additional binds.
    """
    if type(first_parameter) is not int or first_parameter < 1:
        raise ValueError("Compact native parameter position is invalid")
    compiled = _bound_address_query(
        layout, network_ids, visible_membership_sql, location_lookup=location_lookup, columns=columns
    ).compile(dialect=PGDialect_asyncpg())
    names = compiled.positiontup or ()
    if first_parameter + len(names) > 32768 or any(compiled.params[name] is None for name in names):
        raise ValueError("Compact native parameters are invalid")
    query = re.sub(r"\$(\d+)", lambda match: "$" + str(int(match[1]) + first_parameter - 1), str(compiled))
    return "(" + query + ")", tuple(compiled.params[name] for name in names)
