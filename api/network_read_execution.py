# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Execute bounded network queries using the request's SQLAlchemy session."""

from sanic.exceptions import ServiceUnavailable

from process.network_compact_layout import compact_identifier


def qualified_relation_sql(schema, table):
    """Render a validated schema-qualified query target."""
    return compact_identifier(schema) + "." + compact_identifier(table)


async def _read_driver(session):
    """Borrow the active session's native query interface."""
    if session is None or session.get_transaction() is None:
        raise ServiceUnavailable(
            "Canonical network data is unavailable.", headers={"Cache-Control": "private, no-store"}
        )
    return (await (await session.connection()).get_raw_connection()).driver_connection


async def bounded_network_read_rows(session, query, *parameters):
    """Reuse the existing bounded aggregate decoder for native query parameters."""
    from api.provider_service_source_policy import _bounded_rows

    return await _bounded_rows(await _read_driver(session), query, *parameters)
