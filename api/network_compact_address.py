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
            CompactAddressRelation(artifact.schema_name, "network_office_edge", artifact.table_oid)
            for artifact in layout.artifacts
        )
    return tuple(relations)


def _membership_arms(layout, *, selected):
    arms = []
    for index, artifact in enumerate(layout.artifacts):
        edge = compact_identifier(artifact.schema_name) + ".network_office_edge"
        selector = "AND ref.network_id=ANY(CAST(:_canonical_network_ids AS integer[]))" if selected else ""
        arms.append(f"""SELECT ref.network_id FROM ONLY {_relation(layout.ref_map)} ref JOIN ONLY {edge} edge
            ON edge.slice_id=ref.slice_id WHERE ref.artifact_candidate_id=CAST(:_compact_artifact_{index} AS uuid)
            AND edge.location_key=office.location_key {selector}""")
    return " UNION ALL ".join(arms)


def _address_query(layout, network_ids, visible_membership_sql):
    compact_address_parameters(layout)
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
    physical_columns = ",".join(
        compact_identifier(col.name)
        for col in EntityAddressUnified.__table__.columns
        if col.name != "canonical_network_ids"
    )
    base = f"SELECT {physical_columns} FROM ONLY {_relation(layout.base)} base WHERE row_origin<>'network_custom'"
    if layout.custom is not None:
        base += f" AND NOT EXISTS(SELECT 1 FROM ONLY {_relation(layout.custom)} custom WHERE custom.location_key=base.location_key)"
        base += f" UNION ALL SELECT {physical_columns} FROM ONLY {_relation(layout.custom)}"
    memberships = _membership_arms(layout, selected=False)
    arrays = (
        f"ARRAY(SELECT DISTINCT membership.network_id FROM ({memberships}) membership ORDER BY membership.network_id)"
        if memberships
        else "'{}'::integer[]"
    )
    output_columns = ",".join(
        arrays + " AS canonical_network_ids"
        if col.name == "canonical_network_ids"
        else "office." + compact_identifier(col.name)
        for col in EntityAddressUnified.__table__.columns
    )
    query = f"SELECT {output_columns} FROM ({base}) office"
    if network_ids is not None:
        selected = _membership_arms(layout, selected=True)
        return query + (
            f" WHERE EXISTS(SELECT 1 FROM ({selected}) selected_membership)" if selected else " WHERE FALSE"
        )
    visibility = "row_origin IN ('base','ptg_overlay')"
    if visible_membership_sql is not None:
        visibility += " OR (" + visible_membership_sql + ")"
    return f"SELECT * FROM ({query}) visible_office WHERE {visibility}"


def compact_address_relation_sql(layout, *, network_ids=None, visible_membership_sql=None):
    """Return a parenthesized relation; selector/visibility binds belong to the caller.

    Layout and visibility SQL come only from trusted manifest/policy integration.
    Neither this query builder nor its DTOs establish custody or authorize reads.
    """
    return "(" + _address_query(layout, network_ids, visible_membership_sql) + ")"


def _bound_address_query(layout, network_ids, visible_membership_sql, read_statements=None):
    sql = _address_query(layout, network_ids, visible_membership_sql)
    if read_statements is not None:
        sql = read_statements.marker(compact_address_read_relations(layout)) + " " + sql
    query = text(sql)
    for name, value in compact_address_parameters(layout).items():
        query = query.bindparams(bindparam(name, value=value, type_=String()))
    if network_ids is not None and layout.artifacts:
        query = query.bindparams(bindparam("_canonical_network_ids", value=list(network_ids), type_=ARRAY(INTEGER)))
    return query


def compact_address_selectable(layout, *, network_ids=None, visible_membership_sql=None, read_statements=None):
    """Expose model-column parity with raw SQL, including typed reserved binds."""
    query = _bound_address_query(layout, network_ids, visible_membership_sql, read_statements)
    columns = [column(col.name, col.type) for col in EntityAddressUnified.__table__.columns]
    return query.columns(*columns).subquery("entity_address_unified")


def compact_address_native_sql(layout, *, first_parameter, network_ids=None, visible_membership_sql=None):
    """Append typed binds to existing asyncpg parameters for the same logical rows.

    Only the common generated SQL is compiled; selectors and artifact UUIDs stay
    bound. Visibility expressions are trusted policy SQL without additional binds.
    """
    if type(first_parameter) is not int or first_parameter < 1:
        raise ValueError("Compact native parameter position is invalid")
    compiled = _bound_address_query(layout, network_ids, visible_membership_sql).compile(dialect=PGDialect_asyncpg())
    names = compiled.positiontup or ()
    if first_parameter + len(names) > 32768 or any(compiled.params[name] is None for name in names):
        raise ValueError("Compact native parameters are invalid")
    query = re.sub(r"\$(\d+)", lambda match: "$" + str(int(match[1]) + first_parameter - 1), str(compiled))
    return "(" + query + ")", tuple(compiled.params[name] for name in names)
