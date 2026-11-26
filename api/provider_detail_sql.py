# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""SQL builders for provider detail address identity and taxonomy."""

from operator import eq
from typing import Any, Sequence

from sqlalchemy import BigInteger, String, any_, bindparam, func, or_, select
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.sql import literal_column

from db.models import EntityAddressUnified, ProviderDirectorySource


def any_bigint_filter(column: Any, values: Sequence[int], param_name: str = "npis") -> Any:
    """Build a typed PostgreSQL BIGINT[] ANY() predicate for stable prepared plans."""
    return eq(column, any_(bindparam(param_name, value=list(values), type_=ARRAY(BigInteger))))


def any_string_filter(column: Any, values: Sequence[str], param_name: str = "values") -> Any:
    """Build a typed PostgreSQL VARCHAR[] ANY() predicate for stable prepared plans."""
    return eq(column, any_(bindparam(param_name, value=list(values), type_=ARRAY(String))))


def _provider_directory_source_detail_statement(source_ids: Sequence[str]) -> Any:
    table = ProviderDirectorySource.__table__
    source_ids_param = bindparam("source_ids", value=list(source_ids), type_=ARRAY(String))
    selected_endpoints = (
        select(
            table.c.endpoint_id.label("endpoint_id"),
            table.c.canonical_api_base.label("canonical_api_base"),
        )
        .where(table.c.source_id == any_(source_ids_param))
        .subquery()
    )
    return (
        select(
            table.c.source_id,
            table.c.endpoint_id,
            table.c.canonical_api_base,
            table.c.org_name,
            table.c.plan_name,
        )
        .where(
            or_(
                table.c.source_id == any_(source_ids_param),
                table.c.endpoint_id.in_(
                    select(selected_endpoints.c.endpoint_id).where(selected_endpoints.c.endpoint_id.is_not(None))
                ),
                table.c.canonical_api_base.in_(
                    select(selected_endpoints.c.canonical_api_base).where(
                        selected_endpoints.c.endpoint_id.is_(None),
                        selected_endpoints.c.canonical_api_base.is_not(None),
                    )
                ),
            )
        )
        .order_by(table.c.source_id)
    )


def _npi_taxonomy_batch_aggregate(
    taxonomy_model: Any,
    npis: Sequence[int],
    alias: str,
    *,
    npis_param: Any = None,
) -> Any:
    taxonomy_table = taxonomy_model.__table__
    target_npis_param = (
        npis_param
        if npis_param is not None
        else bindparam("npis", value=list(npis), type_=ARRAY(BigInteger), unique=True)
    )
    return (
        select(
            taxonomy_table.c.npi,
            func.json_agg(literal_column(f'distinct "{taxonomy_model.__tablename__}"')).label("rows"),
        )
        .where(taxonomy_table.c.npi == any_(target_npis_param))
        .group_by(taxonomy_table.c.npi)
        .subquery(alias)
    )


def _npi_detail_address_filters(
    address_model: Any,
    address_table: Any,
    npi: int,
    address_key: str | None,
    address_row_identities: Sequence[str] | None,
) -> list[Any]:
    base_address_filters = [address_table.c.npi == npi]
    if address_model is EntityAddressUnified:
        base_address_filters[0] = func.coalesce(address_table.c.npi, address_table.c.inferred_npi) == npi
    if address_key is not None:
        base_address_filters.append(address_table.c.address_key == address_key)
    if address_row_identities is not None:
        if address_model is EntityAddressUnified:
            selected_location_keys = sorted(
                str(identity).split(":", 1)[1]
                for identity in address_row_identities
                if str(identity).startswith("location:")
            )
            base_address_filters.append(
                address_table.c.location_key
                == any_(bindparam("selected_location_keys", value=selected_location_keys, type_=ARRAY(String)))
            )
        else:
            selected_checksums = sorted(
                int(str(identity).rsplit(":", 1)[1])
                for identity in address_row_identities
                if str(identity).startswith("legacy:") and str(identity).rsplit(":", 1)[1].lstrip("-").isdigit()
            )
            base_address_filters.append(
                address_table.c.checksum
                == any_(bindparam("selected_checksums", value=selected_checksums, type_=ARRAY(BigInteger)))
            )
    return base_address_filters


def _npi_detail_taxonomy_aggregate(
    taxonomy_model: Any,
    npi: int,
    alias: str,
) -> Any:
    taxonomy_table = taxonomy_model.__table__
    return (
        select(
            taxonomy_table.c.npi,
            func.json_agg(literal_column(f'distinct "{taxonomy_model.__tablename__}"')).label("rows"),
        )
        .select_from(taxonomy_table)
        .where(taxonomy_table.c.npi == npi)
        .group_by(taxonomy_table.c.npi)
        .subquery(alias)
    )


def detail_address_subquery(
    address_model: Any,
    address_table: Any,
    address_columns: Sequence[Any],
    address_filters: Sequence[Any],
    address_limit: int | None,
) -> Any:
    """Build the bounded detail address relation using the endpoint query hooks."""
    from api.endpoint import npi

    npi_address_rows = npi.select(*address_columns).where(*address_filters).offset(0).subquery("npi_address_rows")
    address_subquery_base = (
        npi.select(*npi_address_rows.c)
        .where(npi._provider_detail_address_type_clause(address_model, npi_address_rows))
        # Deterministic order keeps bounded identity hydration stable.
        .order_by(
            npi_address_rows.c.type,
            npi_address_rows.c.first_line,
            npi_address_rows.c.city_name,
        )
    )
    if address_limit is not None:
        address_subquery_base = address_subquery_base.limit(address_limit)
    try:
        return address_subquery_base.alias("address_list")
    except NameError:
        return address_subquery_base
