# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import os
from typing import Any

import sanic.exceptions


PROVIDER_SEX_CODES = frozenset({"M", "F", "U", "X"})


def normalize_provider_sex_code(raw: Any) -> str | None:
    """Normalize the canonical individual-provider sex filter."""

    if raw is None:
        return None
    value = str(raw).strip().upper()
    if not value:
        return None
    if value not in PROVIDER_SEX_CODES:
        raise sanic.exceptions.InvalidUsage(
            "provider_sex_code must be one of: F, M, U, X"
        )
    return value


def provider_sex_exists_sql(
    npi_sql: str,
    params: dict[str, Any],
    param_prefix: str,
    provider_sex_code: str | None,
    *,
    schema: str | None = None,
) -> str:
    """Build an indexable NPI-to-sex predicate shared by provider APIs."""

    normalized_code = normalize_provider_sex_code(provider_sex_code)
    if normalized_code is None:
        return ""
    parameter_name = f"{param_prefix}_provider_sex_code"
    params[parameter_name] = normalized_code
    provider_alias = f"{param_prefix}_npi"
    schema_name = schema or os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    return f"""EXISTS (
                    SELECT 1
                      FROM {schema_name}.npi {provider_alias}
                     WHERE {provider_alias}.npi = {npi_sql}
                       AND {provider_alias}.provider_sex_code = :{parameter_name}
                )"""
