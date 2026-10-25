# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Compare all retained rate terms with fresh bounded native codec reads."""

import json
from collections import Counter

from sqlalchemy import text

from api import plan_pricing_projection_v3_price as prices
from api.plan_pricing_projection_v4_occurrence import rate_occurrence_rows
from process import reference_family_archive as native
from process.ptg_parts.ptg2_manifest_artifacts import ManifestReadLimitError


async def _bounded_rate_batches(session, binding, price_keys):
    """Split fresh native reads without relaxing the per-key hydration limit."""
    price_items = sorted(price_keys.items())
    pending_batches = [tuple(price_items[start : start + 100]) for start in range(0, len(price_items), 100)]
    pending_batches.reverse()
    while pending_batches:
        batch = pending_batches.pop()
        try:
            rates = await prices._price_batch_rates(session, binding, batch)
        except ManifestReadLimitError:
            if len({price_key for _, price_key in batch}) == 1:
                raise
            midpoint = len(batch) // 2
            pending_batches.extend((batch[midpoint:], batch[:midpoint]))
            continue
        yield rates


async def verify_rate_inputs(session, ownership, inputs):
    """Validate every occurrence and price atom, including duplicate multiplicity."""
    from api import plan_pricing_code_build as build
    from api import ptg2_serving as serving
    from api.plan_pricing_code_oracle import _equal, require_budget

    schema = native._quoted(ownership.schema_name)
    await session.execute(
        text(
            f"CREATE TEMP TABLE code_calibration_occurrences ON COMMIT DROP AS SELECT * FROM {schema}.plan_pricing_code_occurrence WHERE FALSE"
        )
    )
    await session.execute(
        text(
            "CREATE TEMP TABLE code_calibration_rates(binding_ordinal integer,price_set_id varchar(32),negotiated_rate numeric,rate_multiplicity bigint) ON COMMIT DROP"
        )
    )
    for binding, _found in inputs:
        serving_rows, price_keys = await build._binding_code_rows(
            session, binding, next(iter(binding.code_rows_by_identity.values()))
        )
        retained_price_ids = set()
        async for rates in _bounded_rate_batches(session, binding, price_keys):
            for price_id, numeric_rates in rates.items():
                retained_price_ids.add(price_id)
                await _copy(
                    session,
                    "code_calibration_rates",
                    ("binding_ordinal", "price_set_id", "negotiated_rate", "rate_multiplicity"),
                    [
                        (binding.binding["ordinal"], price_id, rate, count)
                        for rate, count in Counter(numeric_rates).items()
                    ],
                )
            await require_budget(session, ownership.schema_name)
        await _store_occurrences(session, serving, binding, serving_rows, retained_price_ids)
    fields = "binding_ordinal,occurrence_ordinal,provider_set_key,provider_set_ref,price_key,price_set_ref,rate_pack_ref,source_artifact_key,provider_count,group_fragment,occurrence_multiplicity"
    await _equal(
        session,
        f"SELECT {fields} FROM {schema}.plan_pricing_code_occurrence",
        f"SELECT {fields} FROM code_calibration_occurrences",
    )
    fields = "binding_ordinal,price_set_id,negotiated_rate,rate_multiplicity"
    await _equal(
        session, f"SELECT {fields} FROM plan_pricing_price_rate_stage", f"SELECT {fields} FROM code_calibration_rates"
    )
    await _verify_profiles(session, schema)
    return {"occurrence_multisets": True, "price_atom_multisets": True, "rate_profiles": True}


async def _copy(session, name, columns, occurrence_records):
    """COPY only fixed private temporary oracle tables on the current transaction."""
    connection = (await (await session.connection()).get_raw_connection()).driver_connection
    for start in range(0, len(occurrence_records), 1000):
        await connection.copy_records_to_table(
            name, schema_name="pg_temp", columns=columns, records=occurrence_records[start : start + 1000]
        )


async def _store_occurrences(session, serving, binding, source_rows, retained_price_ids):
    """Use the accepted native codec without reusing grouped intermediates."""
    from api.plan_pricing_code_oracle import require_budget

    columns = (
        "binding_ordinal",
        "occurrence_ordinal",
        "provider_set_key",
        "provider_set_ref",
        "price_key",
        "price_set_ref",
        "rate_pack_ref",
        "source_artifact_key",
        "provider_count",
        "group_fragment",
        "occurrence_multiplicity",
    )
    occurrence_records = []
    for ordinal, occurrence in enumerate(
        rate_occurrence_rows(serving, binding.binding["ordinal"], source_rows, retained_price_ids)
    ):
        fragment = occurrence["group_fragment"]
        fragment = fragment if isinstance(fragment, str) else json.dumps(fragment)
        occurrence_records.append(
            (
                occurrence["binding_ordinal"],
                ordinal,
                occurrence["provider_set_key"],
                occurrence["provider_set_ref"],
                occurrence["price_key"],
                occurrence["price_set_ref"],
                occurrence["rate_pack_ref"],
                occurrence["source_artifact_key"],
                occurrence["provider_count"],
                fragment,
                occurrence["occurrence_multiplicity"],
            )
        )
        if len(occurrence_records) == 1000:
            await _copy(session, "code_calibration_occurrences", columns, occurrence_records)
            occurrence_records.clear()
            await require_budget(session)
    if occurrence_records:
        await _copy(session, "code_calibration_occurrences", columns, occurrence_records)


async def _verify_profiles(session, schema):
    from api.plan_pricing_code_oracle import _equal

    fields = "binding_ordinal,provider_set_key,membership_count,minimum_negotiated_rate,maximum_negotiated_rate,rate_count,negotiated_rates,rate_multiplicities"
    expected = """WITH frequency AS (
        SELECT occ.binding_ordinal,occ.provider_set_key,rate.negotiated_rate,
               sum(occ.occurrence_multiplicity*rate.rate_multiplicity)::bigint AS multiplicity
          FROM code_calibration_occurrences occ JOIN code_calibration_rates rate
            ON rate.binding_ordinal=occ.binding_ordinal AND rate.price_set_id=occ.price_set_ref
         GROUP BY occ.binding_ordinal,occ.provider_set_key,rate.negotiated_rate
        ), members AS (
        SELECT binding_ordinal,provider_set_key,count(*)::bigint AS membership_count
          FROM code_calibration_members member WHERE EXISTS(SELECT 1 FROM code_calibration_raw raw
            WHERE raw.npi=member.npi AND raw.oracle_evidence IS NOT NULL)
         GROUP BY binding_ordinal,provider_set_key)
        SELECT frequency.binding_ordinal,frequency.provider_set_key,membership_count,
               min(negotiated_rate) AS minimum_negotiated_rate,max(negotiated_rate) AS maximum_negotiated_rate,
               sum(multiplicity)::bigint AS rate_count,array_agg(negotiated_rate ORDER BY negotiated_rate) AS negotiated_rates,
               array_agg(multiplicity ORDER BY negotiated_rate) AS rate_multiplicities
          FROM frequency JOIN members USING(binding_ordinal,provider_set_key)
         GROUP BY frequency.binding_ordinal,frequency.provider_set_key,membership_count"""
    await _equal(session, f"SELECT {fields} FROM {schema}.plan_pricing_code_rate_profile", expected)
