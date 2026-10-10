# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""SQL builders for provider detail address identity and taxonomy."""

from typing import Any, Sequence

from sqlalchemy import func, select
from sqlalchemy.sql import literal_column

from db.models import EntityAddressUnified


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
            base_address_filters.append(address_table.c.location_key.in_(selected_location_keys))
        else:
            selected_checksums = sorted(
                int(str(identity).rsplit(":", 1)[1])
                for identity in address_row_identities
                if str(identity).startswith("legacy:") and str(identity).rsplit(":", 1)[1].lstrip("-").isdigit()
            )
            base_address_filters.append(address_table.c.checksum.in_(selected_checksums))
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
