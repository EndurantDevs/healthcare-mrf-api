# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Provider membership and frozen-cell materialization for projection v3."""

from __future__ import annotations

from typing import Any, Iterable, Mapping

from sqlalchemy import text

from api.plan_pricing_projection_contract import table
from api.plan_pricing_projection_materialize import digest_row
from api.plan_pricing_projection_source import BindingProjection
from api.plan_pricing_projection_v3_types import _BuildState, _insert_batches


PROVIDER_SET_BATCH_SIZE = 32
MAX_PROVIDER_NPIS_PER_SET = 16_384
MAX_PROJECTION_PROVIDER_SETS = 200_000
MAX_PROJECTION_PROVIDER_MEMBERSHIPS = 8_000_000

_PROVIDER_SET_STAGE_INSERT_SQL = """
    INSERT INTO plan_pricing_provider_set_stage (
        binding_ordinal, provider_set_key, provider_set_id, membership_count
    ) VALUES (
        :binding_ordinal, :provider_set_key, :provider_set_id, :membership_count
    )
"""
_PROVIDER_MEMBER_STAGE_INSERT_SQL = """
    INSERT INTO plan_pricing_provider_member_stage (
        binding_ordinal, provider_set_key, npi
    ) VALUES (:binding_ordinal, :provider_set_key, :npi)
"""
_PENDING_PROVIDER_NPI_INSERT_SQL = """
    INSERT INTO plan_pricing_provider_npi_pending_stage (npi)
    SELECT DISTINCT member.npi
      FROM plan_pricing_provider_member_stage member
      LEFT JOIN plan_pricing_provider_npi_materialized_stage done
        ON done.npi = member.npi
     WHERE member.binding_ordinal = :binding_ordinal
       AND member.provider_set_key = ANY(CAST(:provider_set_keys AS bigint[]))
       AND done.npi IS NULL
    ON CONFLICT DO NOTHING
"""
_STAGE_TABLE_SQL = (
    """
            CREATE TEMP TABLE plan_pricing_provider_set_stage (
                binding_ordinal integer NOT NULL,
                provider_set_key bigint NOT NULL,
                provider_set_id varchar(32) NOT NULL,
                membership_count integer NOT NULL,
                PRIMARY KEY (binding_ordinal, provider_set_key)
            ) ON COMMIT DROP
    """,
    """
            CREATE TEMP TABLE plan_pricing_provider_member_stage (
                binding_ordinal integer NOT NULL,
                provider_set_key bigint NOT NULL,
                npi bigint NOT NULL,
                PRIMARY KEY (binding_ordinal, provider_set_key, npi)
            ) ON COMMIT DROP
    """,
    """
            CREATE TEMP TABLE plan_pricing_provider_npi_materialized_stage (
                npi bigint PRIMARY KEY
            ) ON COMMIT DROP
    """,
    """
            CREATE TEMP TABLE plan_pricing_provider_npi_pending_stage (
                npi bigint PRIMARY KEY
            ) ON COMMIT DROP
    """,
    """
            CREATE TEMP TABLE plan_pricing_code_occurrence_stage (
                binding_ordinal integer NOT NULL,
                provider_set_key bigint NOT NULL,
                price_set_id varchar(32) NOT NULL,
                occurrence_count bigint NOT NULL,
                PRIMARY KEY (
                    binding_ordinal, provider_set_key, price_set_id
                )
            ) ON COMMIT DROP
    """,
    """
            CREATE TEMP TABLE plan_pricing_price_rate_stage (
                binding_ordinal integer NOT NULL,
                price_set_id varchar(32) NOT NULL,
                negotiated_rate numeric NOT NULL,
                rate_multiplicity bigint NOT NULL,
                PRIMARY KEY (
                    binding_ordinal, price_set_id, negotiated_rate
                )
            ) ON COMMIT DROP
    """,
    """
            CREATE TEMP TABLE plan_pricing_rate_frequency_stage (
                binding_ordinal integer NOT NULL,
                provider_set_key bigint NOT NULL,
                negotiated_rate numeric NOT NULL,
                join_row_count bigint NOT NULL,
                multiplicity bigint NOT NULL,
                PRIMARY KEY (
                    binding_ordinal, provider_set_key, negotiated_rate
                )
            ) ON COMMIT DROP
    """,
    """
            CREATE TEMP TABLE plan_pricing_provider_cell_stage (
                projection_id varchar(64) NOT NULL,
                geo_cell varchar(5) NOT NULL,
                npi bigint NOT NULL,
                entity_type_code smallint NULL,
                taxonomy_codes varchar[] NOT NULL,
                fragment bytea NOT NULL,
                PRIMARY KEY (projection_id, npi, geo_cell)
            ) ON COMMIT DROP
    """,
    """
            CREATE TEMP TABLE plan_pricing_eligible_member_cell_stage (
                binding_ordinal integer NOT NULL,
                provider_set_key bigint NOT NULL,
                geo_cell varchar(5) NOT NULL,
                npi bigint NOT NULL,
                PRIMARY KEY (
                    binding_ordinal, provider_set_key, geo_cell, npi
                )
            ) ON COMMIT DROP
    """,
    """
            CREATE TEMP TABLE plan_pricing_set_cell_stage (
                binding_ordinal integer NOT NULL,
                provider_set_key bigint NOT NULL,
                geo_cell varchar(5) NOT NULL,
                PRIMARY KEY (binding_ordinal, provider_set_key, geo_cell)
            ) ON COMMIT DROP
    """,
)


async def _create_stage_tables(session: Any) -> None:
    for statement in _STAGE_TABLE_SQL:
        await session.execute(text(statement))


def _binding_ordinal(binding: BindingProjection) -> int:
    raw_ordinal = binding.binding.get(
        "ordinal", binding.binding.get("binding_ordinal")
    )
    if isinstance(raw_ordinal, bool):
        raise ValueError("pricing projection binding ordinal is invalid")
    try:
        binding_ordinal = int(raw_ordinal)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("pricing projection binding ordinal is invalid") from exc
    if binding_ordinal < 0:
        raise ValueError("pricing projection binding ordinal is invalid")
    return binding_ordinal


def _validated_binding_ordinals(
    bindings: list[BindingProjection],
) -> tuple[int, ...]:
    binding_ordinals = tuple(_binding_ordinal(binding) for binding in bindings)
    if not binding_ordinals or len(set(binding_ordinals)) != len(binding_ordinals):
        raise ValueError("pricing projection binding ordinals are not unique")
    return binding_ordinals


def _provider_set_ids_by_key(
    serving_rows: Iterable[Mapping[str, Any]],
    provider_set_keys: set[int],
) -> dict[int, str]:
    from api import ptg2_serving as serving

    provider_set_ids_by_key: dict[int, str] = {}
    for serving_row in serving_rows:
        raw_key = serving_row.get("_ptg_provider_set_key")
        if isinstance(raw_key, bool):
            raise ValueError("pricing projection provider-set identity is invalid")
        try:
            provider_set_key = int(raw_key)
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError(
                "pricing projection provider-set identity is invalid"
            ) from exc
        if provider_set_key not in provider_set_keys:
            continue
        provider_set_id = serving._ptg2_manifest_id(
            serving_row.get("provider_set_global_id_128")
        )
        if not provider_set_id:
            raise ValueError("pricing projection provider-set identity is invalid")
        prior_id = provider_set_ids_by_key.setdefault(
            provider_set_key, provider_set_id
        )
        if prior_id != provider_set_id:
            raise ValueError(
                "pricing projection provider-set identity is inconsistent"
            )
    if set(provider_set_ids_by_key) != provider_set_keys:
        raise ValueError("pricing projection provider-set identity is incomplete")
    return provider_set_ids_by_key


async def _existing_provider_set_ids(
    session: Any,
    binding_ordinal: int,
    provider_set_keys: tuple[int, ...],
) -> dict[int, str]:
    if not provider_set_keys:
        return {}
    result = await session.execute(
        text(
            """
            SELECT provider_set_key, provider_set_id
              FROM plan_pricing_provider_set_stage
             WHERE binding_ordinal = :binding_ordinal
               AND provider_set_key = ANY(CAST(:provider_set_keys AS bigint[]))
            """
        ),
        {
            "binding_ordinal": binding_ordinal,
            "provider_set_keys": list(provider_set_keys),
        },
    )
    return {
        int(row["provider_set_key"]): str(row["provider_set_id"])
        for row in result.mappings()
    }


def _validate_provider_set_memberships(
    provider_sets: list[dict[str, Any]],
    metadata_by_id: Mapping[str, Any],
    npis_by_set: Mapping[str, tuple[int, ...]],
) -> None:
    expected_key_by_id = {
        str(provider_set["provider_set_id"]): int(
            provider_set["provider_set_key"]
        )
        for provider_set in provider_sets
    }
    if (
        len(expected_key_by_id) != len(provider_sets)
        or set(metadata_by_id) != set(expected_key_by_id)
        or set(npis_by_set) != set(expected_key_by_id)
    ):
        raise ValueError("pricing projection provider membership is incomplete")
    for provider_set_id, provider_set_key in expected_key_by_id.items():
        metadata = metadata_by_id[provider_set_id]
        provider_npis = npis_by_set[provider_set_id]
        if (
            type(metadata.provider_set_key) is not int
            or metadata.provider_set_key != provider_set_key
            or type(metadata.provider_count) is not int
            or metadata.provider_count < 0
            or len(provider_npis) != metadata.provider_count
        ):
            raise ValueError(
                "pricing projection provider membership is incomplete"
            )
        if (
            len(provider_npis) > MAX_PROVIDER_NPIS_PER_SET
            or any(type(npi) is not int or npi <= 0 for npi in provider_npis)
            or len(set(provider_npis)) != len(provider_npis)
        ):
            raise ValueError(
                "pricing projection provider membership exceeds its bound"
            )


def _staged_membership_rows(
    binding_ordinal: int,
    provider_sets: list[dict[str, Any]],
    npis_by_set: Mapping[str, tuple[int, ...]],
    state: _BuildState,
) -> Iterable[dict[str, int]]:
    key_by_id = {
        str(provider_set["provider_set_id"]): int(
            provider_set["provider_set_key"]
        )
        for provider_set in provider_sets
    }
    for provider_set_id in sorted(key_by_id, key=key_by_id.get):
        provider_set_key = key_by_id[provider_set_id]
        for npi in sorted(npis_by_set[provider_set_id]):
            digest_row(
                state.content_digest,
                "provider-membership",
                (binding_ordinal, provider_set_key, npi),
                b"",
            )
            state.provider_membership_count += 1
            yield {
                "binding_ordinal": binding_ordinal,
                "provider_set_key": provider_set_key,
                "npi": npi,
            }


async def _stage_provider_set_batch(
    session: Any,
    binding: BindingProjection,
    provider_sets: list[dict[str, Any]],
    state: _BuildState,
    *,
    insert_batches: Any = _insert_batches,
) -> None:
    """Stage one bounded provider-set membership batch."""

    binding_ordinal, npis_by_set = await _bounded_provider_memberships(
        session, binding, provider_sets, state
    )
    await _stage_provider_membership_batch(
        session,
        binding_ordinal,
        provider_sets,
        npis_by_set,
        state,
        insert_batches,
    )


async def _bounded_provider_memberships(
    session: Any,
    binding: BindingProjection,
    provider_sets: list[dict[str, Any]],
    state: _BuildState,
) -> tuple[int, Mapping[str, tuple[int, ...]]]:
    """Resolve complete memberships while enforcing the release-wide bound."""

    from api import ptg2_serving as serving

    binding_ordinal = _binding_ordinal(binding)
    provider_set_ids = tuple(
        str(provider_set["provider_set_id"])
        for provider_set in provider_sets
    )
    metadata_by_id = await serving._provider_set_metadata_for_ids(
        session,
        binding.serving_tables,
        provider_set_ids,
    )
    npis_by_set = await serving._provider_npis_for_sets(
        session,
        binding.serving_tables,
        provider_set_ids,
        limit_per_set=MAX_PROVIDER_NPIS_PER_SET + 1,
    )
    _validate_provider_set_memberships(
        provider_sets,
        metadata_by_id,
        npis_by_set,
    )
    membership_count = sum(map(len, npis_by_set.values()))
    if (
        state.provider_membership_count + membership_count
        > MAX_PROJECTION_PROVIDER_MEMBERSHIPS
    ):
        raise ValueError("pricing projection membership bound exceeded")
    return binding_ordinal, npis_by_set


async def _stage_provider_membership_batch(
    session: Any,
    binding_ordinal: int,
    provider_sets: list[dict[str, Any]],
    npis_by_set: Mapping[str, tuple[int, ...]],
    state: _BuildState,
    insert_batches: Any,
) -> None:
    """Store one validated membership batch in transaction-local stages."""

    provider_set_rows = [
        {
            "binding_ordinal": binding_ordinal,
            "provider_set_key": int(provider_set["provider_set_key"]),
            "provider_set_id": str(provider_set["provider_set_id"]),
            "membership_count": len(
                npis_by_set[str(provider_set["provider_set_id"])]
            ),
        }
        for provider_set in provider_sets
    ]
    await insert_batches(
        session,
        _PROVIDER_SET_STAGE_INSERT_SQL,
        provider_set_rows,
    )
    await insert_batches(
        session,
        _PROVIDER_MEMBER_STAGE_INSERT_SQL,
        _staged_membership_rows(
            binding_ordinal,
            provider_sets,
            npis_by_set,
            state,
        ),
    )
    provider_set_keys = [
        int(provider_set["provider_set_key"])
        for provider_set in provider_sets
    ]
    await session.execute(
        text(_PENDING_PROVIDER_NPI_INSERT_SQL),
        {
            "binding_ordinal": binding_ordinal,
            "provider_set_keys": provider_set_keys,
        },
    )
    state.staged_provider_set_count += len(provider_sets)


async def _stage_code_provider_sets(
    session: Any,
    binding: BindingProjection,
    serving_rows: Iterable[Mapping[str, Any]],
    provider_set_keys: set[int],
    state: _BuildState,
    *,
    stage_provider_set_batch: Any = _stage_provider_set_batch,
) -> None:
    if not provider_set_keys:
        return
    binding_ordinal = _binding_ordinal(binding)
    provider_set_ids_by_key = _provider_set_ids_by_key(
        serving_rows, provider_set_keys
    )
    ordered_keys = tuple(sorted(provider_set_ids_by_key))
    existing_ids_by_key = await _existing_provider_set_ids(
        session, binding_ordinal, ordered_keys
    )
    if any(
        existing_ids_by_key[key] != provider_set_ids_by_key[key]
        for key in existing_ids_by_key
    ):
        raise ValueError("pricing projection provider-set identity is inconsistent")
    new_provider_sets = [
        {
            "provider_set_key": key,
            "provider_set_id": provider_set_ids_by_key[key],
        }
        for key in ordered_keys
        if key not in existing_ids_by_key
    ]
    if (
        state.staged_provider_set_count + len(new_provider_sets)
        > MAX_PROJECTION_PROVIDER_SETS
    ):
        raise ValueError("pricing projection provider-set bound exceeded")
    for start in range(0, len(new_provider_sets), PROVIDER_SET_BATCH_SIZE):
        await stage_provider_set_batch(
            session,
            binding,
            new_provider_sets[start : start + PROVIDER_SET_BATCH_SIZE],
            state,
        )


async def _persist_provider_projection(
    session: Any,
    projection_id: str,
) -> None:
    """Copy fully admitted provider stages into the immutable projection."""

    await session.execute(
        text(
            f"""
            INSERT INTO {table('plan_pricing_provider_membership')} (
                projection_id, binding_ordinal, provider_set_key, npi
            )
            SELECT :projection_id, binding_ordinal, provider_set_key, npi
              FROM plan_pricing_provider_member_stage
             ORDER BY binding_ordinal, provider_set_key, npi
            """
        ),
        {"projection_id": projection_id},
    )
    await session.execute(
        text(
            f"""
            INSERT INTO {table('plan_pricing_provider_cell')} (
                projection_id, geo_cell, npi, entity_type_code,
                taxonomy_codes, fragment
            )
            SELECT projection_id, geo_cell, npi, entity_type_code,
                   taxonomy_codes, fragment
              FROM plan_pricing_provider_cell_stage
             WHERE projection_id = :projection_id
             ORDER BY geo_cell, npi
            """
        ),
        {"projection_id": projection_id},
    )
