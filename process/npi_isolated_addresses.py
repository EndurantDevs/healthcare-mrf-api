# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded ordinary address resolution into an issued private NPI contribution."""

from __future__ import annotations

import hashlib
import re
import time
from dataclasses import asdict

from db.models import AddressArchiveV2
from process.ext import address_alias_sql as aliases
from process.ext import address_canon as canon
from process.ext.address_format import ADDRESS_FORMAT_VERSION, render_formatted_address_v2
from process.npi_alias_witness import INPUTS, SETS, WITNESS_CONTRACT, observation_time, validate_npi_address_witness
from process.npi_isolated_execution import INPUT_NAMES, NpiAddressPreparation, NpiInputRelation

_BATCH = 2048
_KEYED = '"npi_alias_keyed"'
_DEDUP = '"npi_alias_observed"'
_SELECTED = '"npi_alias_selected"'
_EXPECTED = '"npi_alias_expected"'
_FORMATTED = '"npi_alias_formatted"'
_IDENTIFIER = re.compile(r"[a-z_][a-z0-9_]{0,62}\Z", re.ASCII)
_FIELDS = {
    "first_line": "first_line",
    "second_line": "second_line",
    "city": "city_name",
    "state": "state_name",
    "zip": "postal_code",
    "country": "COALESCE(NULLIF(country_code, ''), 'US')",
}
_DISPLAY = ("first_line", "second_line", "city_name", "state_name", "postal_code")


def _require(condition):
    if not condition:
        raise RuntimeError("NPI isolated address preparation differs")


def qualified(schema, table):
    """Quote the closed native schema and relation identifier shape."""
    _require(type(schema) is str and _IDENTIFIER.fullmatch(schema))
    _require(type(table) is str and _IDENTIFIER.fullmatch(table))
    return f'"{schema}"."{table}"'


def require_runtime_inputs(pinned_inputs, *, openaddresses_backfill=False):
    """Validate the complete issued inventory; unsupported backfill is never skipped."""
    _require(set(pinned_inputs) == INPUT_NAMES and not openaddresses_backfill)
    for name in INPUTS:
        _require(len(pinned_inputs[name]) == 1 and type(pinned_inputs[name][0]) is NpiInputRelation)
        _require(pinned_inputs[name][0].table_name == name)
    _require(pinned_inputs[INPUTS[1]][0].schema_name == pinned_inputs[INPUTS[2]][0].schema_name)


def _trim(value):
    return value.strip(" ") or None if value is not None else None


def normalized_record(address_row):
    """Reuse the production Python encoding, including its original strict key."""
    display_values = tuple(_trim(address_row[name]) for name in _DISPLAY)
    first, second, city, state, postal = display_values
    country = address_row["raw_country_code"]
    relation_identity = canon.identity_key_v1(first, second, city, state, postal, country)
    computed = canon.key_from_identity(relation_identity)
    staged = address_row["staged_address_key"]
    _require(staged is None or staged == computed)
    return (
        address_row["rn"],
        address_row["source_ctid"],
        staged,
        staged or computed,
        computed,
        relation_identity,
        canon.key_from_identity(canon.premise_identity_key_v1(first, second, city, state, postal, country)),
        canon.street_norm(first, second),
        canon.unit_norm(first, second),
        canon.city_norm(city),
        (canon.state_code(state) or "")[:32] or None,
        canon.zip5_norm(postal),
        re.sub(r"[^0-9]", "", postal or "")[5:9] or None,
        canon.country_code(country),
        first,
        second,
        city,
        state,
        postal,
    )


async def _batches(connection, query):
    """Exhaust and close native portals before reusing temporary model heaps."""
    batch_rows = []
    async for address_row in connection.cursor(query, prefetch=_BATCH):
        batch_rows.append(address_row)
        if len(batch_rows) == _BATCH:
            yield batch_rows
            batch_rows = []
    if batch_rows:
        yield batch_rows


async def _copy_normalized(connection, staging, cancel_check):
    fields = ",".join(_DISPLAY)
    query = (
        "SELECT row_number() OVER(ORDER BY ctid) AS rn,ctid::text AS source_ctid,"
        "address_key AS staged_address_key," + fields + ","
        "COALESCE(NULLIF(country_code,''),'US') AS raw_country_code FROM " + staging
    )
    await connection.execute(canon._keyed_temp_table_ddl(_KEYED))
    async for address_rows in _batches(connection, query):
        if cancel_check is not None:
            await cancel_check()
        await connection.copy_records_to_table(
            "npi_alias_keyed",
            schema_name="pg_temp",
            columns=canon.KEYED_COPY_COLUMNS,
            records=[normalized_record(address_row) for address_row in address_rows],
        )


async def _verify_inputs(connection, input_map):
    for relation in input_map.values():
        await connection.execute("LOCK TABLE " + relation.qualified + " IN ACCESS SHARE MODE NOWAIT")
        value = await connection.fetchrow(
            "SELECT oid::bigint,relfilenode::bigint,relkind::text,relpersistence::text,relrowsecurity,relforcerowsecurity "
            "FROM pg_catalog.pg_class WHERE oid=to_regclass($1)",
            relation.qualified,
        )
        _require(
            value is not None and (value["oid"], value["relfilenode"]) == (relation.relation_oid, relation.relfilenode)
        )
        _require(value["relkind"] == "r" and value["relpersistence"] == "p")
        _require(not value["relrowsecurity"] and not value["relforcerowsecurity"])


async def _apply_aliases(connection, input_map):
    input_relations = tuple(input_map[name] for name in INPUTS)
    baseline, alias, state = input_relations
    address_rows = await connection.fetch(
        "SELECT singleton,schema_version,active_ruleset_version,generation FROM " + state.qualified
    )
    _require(len(address_rows) == 1 and address_rows[0]["singleton"] is True)
    _require((address_rows[0]["schema_version"], address_rows[0]["active_ruleset_version"]) == (2, 1))
    await connection.execute(
        aliases.existing_address_aliases_sql(
            schema=alias.schema_name,
            keyed_table=_KEYED,
            archive=baseline.qualified,
        ).replace("address_existing_aliases", "npi_issued_aliases")
    )
    violation = await connection.fetchrow(
        aliases.existing_address_alias_violation_sql(
            schema=alias.schema_name,
        ).replace("address_existing_aliases", "npi_issued_aliases")
    )
    _require(violation is None)
    await connection.execute(
        aliases.update_existing_alias_keyed_rows_sql(
            keyed_table=_KEYED,
        ).replace("address_existing_aliases", "npi_issued_aliases")
    )
    return address_rows[0]["generation"]


def _dedup_sql():
    order = ",".join(f"{name} IS NULL,length(COALESCE({name},'')) DESC" for name in _DISPLAY[:3])
    tail = ",".join(f"COALESCE({name},'')" for name in _DISPLAY)
    return (
        f"SELECT DISTINCT ON(address_key) * FROM {_KEYED} WHERE address_key IS NOT NULL AND identity_key IS NOT NULL "
        f"ORDER BY address_key,{order},{tail},identity_key"
    )


async def _selected_keys(connection, staging, baseline):
    await connection.execute(
        f"CREATE TEMP TABLE {_SELECTED} ON COMMIT DROP AS WITH RECURSIVE selected(address_key) AS ("
        f"(SELECT DISTINCT s.address_key FROM {staging} s JOIN {baseline} b USING(address_key) "
        f"UNION SELECT address_key FROM {_DEDUP}) UNION "
        f"SELECT b.merged_into FROM {baseline} b JOIN selected USING(address_key) WHERE b.merged_into IS NOT NULL) "
        "SELECT address_key FROM selected"
    )
    await connection.execute(f"CREATE UNIQUE INDEX ON {_SELECTED}(address_key)")
    invalid = await connection.fetchval(
        f"SELECT EXISTS(SELECT 1 FROM {baseline} b JOIN {_SELECTED} USING(address_key) WHERE b.merged_into IS NOT NULL "
        f"AND NOT EXISTS(SELECT 1 FROM {baseline} target WHERE target.address_key=b.merged_into))"
    )
    _require(invalid is False)


async def _format_rows(connection, baseline, cancel_check):
    await connection.execute(
        f"CREATE TEMP TABLE {_FORMATTED}(address_key uuid PRIMARY KEY,formatted_address text) ON COMMIT DROP"
    )
    columns = ",".join(
        f"CASE WHEN b.address_key IS NULL OR b.display_priority>0 THEN d.{name} ELSE b.{name} END AS {name}"
        for name in _DISPLAY
    )
    query = (
        f"SELECT d.address_key,{columns},COALESCE(b.country_code,d.country_code) AS country_code "
        f"FROM {_DEDUP} d LEFT JOIN {baseline} b USING(address_key)"
    )
    async for address_rows in _batches(connection, query):
        if cancel_check is not None:
            await cancel_check()
        await connection.copy_records_to_table(
            "npi_alias_formatted",
            schema_name="pg_temp",
            columns=("address_key", "formatted_address"),
            records=[
                (
                    address_row["address_key"],
                    render_formatted_address_v2(
                        *(address_row[name] for name in _DISPLAY),
                        address_row["country_code"],
                    ),
                )
                for address_row in address_rows
            ],
        )


async def _projection(connection, *, schema, table, input_map, observed_at, write_keys, cancel_check=None):
    from process.mrf_address_publication import canonical_resolution_projection

    _require(connection.is_in_transaction())
    staging = qualified(schema, table)
    await connection.execute("SET LOCAL TIME ZONE 'UTC'")
    await _verify_inputs(connection, input_map)
    for name in (_EXPECTED, _FORMATTED, _SELECTED, _DEDUP, _KEYED, '"npi_issued_aliases"', '"npi_alias_strict"'):
        await connection.execute("DROP TABLE IF EXISTS pg_temp." + name)
    await _copy_normalized(connection, staging, cancel_check)
    if write_keys:
        await connection.execute(
            f"UPDATE {staging} s SET address_key=k.computed_address_key FROM {_KEYED} k "
            "WHERE s.ctid=k.source_ctid::tid AND s.address_key IS NULL AND k.computed_address_key IS NOT NULL"
        )
    generation = await _apply_aliases(connection, input_map)
    await connection.execute(f"CREATE TEMP TABLE {_DEDUP} ON COMMIT DROP AS " + _dedup_sql())
    await connection.execute(f"CREATE UNIQUE INDEX ON {_DEDUP}(address_key)")
    _require(
        await connection.fetchval(
            f"SELECT EXISTS(SELECT 1 FROM {_KEYED} WHERE address_key IS NOT NULL GROUP BY address_key "
            "HAVING count(DISTINCT identity_key)>1)"
        )
        is False
    )
    baseline = input_map["address_archive_v2"].qualified
    _require(
        await connection.fetchval(
            f"SELECT EXISTS(SELECT 1 FROM {_DEDUP} d JOIN {baseline} b USING(address_key) "
            "WHERE d.identity_key IS DISTINCT FROM b.identity_key)"
        )
        is False
    )
    await _selected_keys(connection, staging, baseline)
    await _format_rows(connection, baseline, cancel_check)
    await connection.execute('CREATE TEMP TABLE "npi_alias_strict"(address_key uuid,identity_key text) ON COMMIT DROP')
    selected_baseline = f"(SELECT b.* FROM {baseline} b JOIN {_SELECTED} USING(address_key))"
    sql = canonical_resolution_projection(
        "npi",
        archive=selected_baseline,
        deduplicated=_DEDUP,
        strict='"npi_alias_strict"',
        formatted=_FORMATTED,
        observed_at=observed_at,
    )
    await connection.execute(f"CREATE TEMP TABLE {_EXPECTED} ON COMMIT DROP AS " + sql)
    await connection.execute(f"CREATE UNIQUE INDEX ON {_EXPECTED}(address_key)")
    return generation


async def _digest_query(connection, query):
    checksum, count = hashlib.sha256(), 0
    async for address_rows in _batches(
        connection, "SELECT row_json FROM (" + query + ') rows ORDER BY row_json COLLATE "C"'
    ):
        for address_row in address_rows:
            checksum.update(address_row["row_json"].encode() + b"\n")
            count += 1
    return count, checksum.hexdigest()


async def _projection_identity(connection, schema, table, baseline):
    staging = qualified(schema, table)
    raw = ",".join(("address_key", *_DISPLAY, "country_code"))
    effective = ",".join(
        name
        for name in canon.KEYED_COPY_COLUMNS
        if name not in {"rn", "source_ctid", "staged_address_key", "computed_address_key"}
    )
    query_map = {
        "raw": f"SELECT pg_catalog.to_jsonb(r)::text AS row_json FROM (SELECT {raw} FROM {staging}) r",
        "strict": f"SELECT pg_catalog.to_jsonb(r)::text AS row_json FROM (SELECT DISTINCT address_key FROM {staging} WHERE address_key IS NOT NULL) r",
        "effective": f"SELECT pg_catalog.to_jsonb(r)::text AS row_json FROM (SELECT {effective} FROM {_DEDUP}) r",
        "aliases": "SELECT pg_catalog.to_jsonb(r)::text AS row_json FROM (SELECT DISTINCT source_address_key,staged_source_identity_key,target_address_key,target_identity_key FROM npi_issued_aliases) r",
        "absent_strict": f"SELECT pg_catalog.to_jsonb(r)::text AS row_json FROM (SELECT DISTINCT s.address_key FROM {staging} s WHERE s.address_key IS NOT NULL AND NOT EXISTS(SELECT 1 FROM {baseline} b WHERE b.address_key=s.address_key)) r",
        "canonical": f"SELECT pg_catalog.to_jsonb(r)::text AS row_json FROM {_EXPECTED} r",
    }
    digest_map = {name: await _digest_query(connection, query_map[name]) for name in SETS}
    return {name: value[0] for name, value in digest_map.items()}, {
        name: value[1] for name, value in digest_map.items()
    }


def _address_witness(ctx, scope, input_map, runtime_options, generation, counts, hashes):
    """Seal the genuine attempt instant and the independently reproducible row sets."""
    context = ctx.get("context") or {}
    return validate_npi_address_witness(
        {
            "contract": WITNESS_CONTRACT,
            "run_id": scope.run_id,
            "attempt_id": context.get("_control_attempt_id"),
            "attempt_started_at": context.get("_control_attempt_started_at"),
            "observation_time": observation_time(runtime_options["observed_at"]).isoformat(timespec="microseconds"),
            "input_receipt_sha256": runtime_options["input_receipt_sha256"],
            "inputs": {name: asdict(relation) for name, relation in input_map.items()},
            "alias_generation": generation,
            "counts": counts,
            "hashes": hashes,
            "identity_version": canon.CURRENT_ADDRESS_IDENTITY_VERSION,
            "ruleset_version": canon.CURRENT_ADDRESS_CANON_RULESET_VERSION,
            "formatter_version": ADDRESS_FORMAT_VERSION,
            "strict_observations": 0,
        }
    )


async def _resolve_stats(connection, staging, baseline, counts, started):
    """Count the same effective changes as the ordinary NPI resolver."""
    inserted = await connection.fetchval(
        f"SELECT count(*) FROM {_DEDUP} d WHERE NOT EXISTS(SELECT 1 FROM {baseline} b WHERE b.address_key=d.address_key)"
    )
    updated = await connection.fetchval(
        f"SELECT count(*) FROM {_DEDUP} d JOIN {baseline} b USING(address_key) "
        "WHERE (b.source_bits & 1)=0 OR b.display_priority>0"
    )
    return canon.ResolveStats(
        counts["raw"],
        counts["effective"],
        inserted,
        updated,
        await connection.fetchval(f"SELECT count(*) FROM {staging} WHERE address_key IS NULL"),
        eligible_key_rows=await connection.fetchval(f"SELECT count(*) FROM {staging} WHERE address_key IS NOT NULL"),
        elapsed_seconds=time.monotonic() - started,
    )


def _validate_runtime_options(runtime_options):
    _require(
        {"observed_at", "input_receipt_sha256"}
        <= set(runtime_options)
        <= {
            "observed_at",
            "input_receipt_sha256",
            "openaddresses_backfill",
            "cancel_check",
        }
    )


async def prepare_addresses(
    ctx,
    *,
    scope,
    connection,
    staging_table,
    field_map,
    output_schema,
    pinned_inputs,
    **runtime_options,
):
    """Prepare private output using required observed_at/input_receipt_sha256 runtime options.

    Optional openaddresses_backfill/cancel_check retain the existing resolver call shape.
    """
    _validate_runtime_options(runtime_options)
    started = time.monotonic()
    cancel_check = runtime_options.get("cancel_check")
    require_runtime_inputs(pinned_inputs, openaddresses_backfill=runtime_options.get("openaddresses_backfill", False))
    _require(dict(field_map) == _FIELDS and dict(pinned_inputs) == dict(scope.inputs))
    _require(output_schema == scope.output_schema and staging_table == "npi_address")
    await scope.verify(ctx, phase="addresses_core")
    input_map = {name: pinned_inputs[name][0] for name in INPUTS}
    async with connection.transaction():
        await _restore_zip(connection, qualified(output_schema, staging_table), pinned_inputs, cancel_check)
        generation = await _projection(
            connection,
            schema=output_schema,
            table=staging_table,
            input_map=input_map,
            observed_at=observation_time(runtime_options["observed_at"]),
            write_keys=True,
            cancel_check=cancel_check,
        )
        columns = ",".join(column.name for column in AddressArchiveV2.__table__.columns)
        canonical_table = qualified(output_schema, "npi_canonical_address")
        await connection.execute("DELETE FROM " + canonical_table)
        await connection.execute(f"INSERT INTO {canonical_table}({columns}) SELECT {columns} FROM {_EXPECTED}")
        counts, hashes = await _projection_identity(
            connection, output_schema, staging_table, input_map[INPUTS[0]].qualified
        )
        witness_data = _address_witness(ctx, scope, input_map, runtime_options, generation, counts, hashes)
        relation_identity = await connection.fetchrow(
            "SELECT oid::bigint,relfilenode::bigint FROM pg_catalog.pg_class WHERE oid=to_regclass($1)",
            canonical_table,
        )
        stats = await _resolve_stats(
            connection, qualified(output_schema, staging_table), input_map[INPUTS[0]].qualified, counts, started
        )
    return NpiAddressPreparation(
        NpiInputRelation(
            output_schema, "npi_canonical_address", relation_identity["oid"], relation_identity["relfilenode"]
        ),
        stats,
        witness_data.as_dict(),
    )


async def verify_address_projection(connection, *, schema, witness):
    """Recompute all protected sets and full row parity without changing closed heaps."""
    witness_data = witness
    witness_data = validate_npi_address_witness(witness_data).as_dict()
    input_map = {name: NpiInputRelation(**value) for name, value in witness_data["inputs"].items()}
    generation = await _projection(
        connection,
        schema=schema,
        table="npi_address",
        input_map=input_map,
        observed_at=observation_time(witness_data["observation_time"]),
        write_keys=False,
    )
    counts, hashes = await _projection_identity(connection, schema, "npi_address", input_map[INPUTS[0]].qualified)
    _require(
        (generation, counts, hashes)
        == (witness_data["alias_generation"], witness_data["counts"], witness_data["hashes"])
    )
    canonical_table = qualified(schema, "npi_canonical_address")
    _require(
        await connection.fetchval(
            f"SELECT NOT EXISTS((SELECT * FROM {canonical_table} EXCEPT ALL SELECT * FROM {_EXPECTED}) "
            f"UNION ALL (SELECT * FROM {_EXPECTED} EXCEPT ALL SELECT * FROM {canonical_table}))"
        )
        is True
    )


async def _restore_zip(connection, staging, pinned_inputs, cancel_check):
    """Preserve the native single-distinct-ZIP match using only issued geography."""
    needed = await connection.fetchval(
        f"SELECT EXISTS(SELECT 1 FROM {staging} WHERE address_key IS NULL AND NULLIF(trim(postal_code),'') IS NULL "
        "AND lat IS NOT NULL AND long IS NOT NULL)"
    )
    if not needed:
        return
    _require(len(pinned_inputs["geo_zip_lookup"]) == 1)
    await _verify_inputs(
        connection,
        {
            str(index): relation
            for index, relation in enumerate((*pinned_inputs["tiger"], *pinned_inputs["geo_zip_lookup"]))
        },
    )
    candidates = []
    for relation in pinned_inputs["tiger"]:
        columns = set(
            await connection.fetchval(
                "SELECT array_agg(attname::text) FROM pg_catalog.pg_attribute WHERE attrelid=$1 AND attnum>0 AND NOT attisdropped",
                relation.relation_oid,
            )
        )
        if {"zcta5ce", "the_geom"} <= columns:
            candidates.append(relation)
    _require(len(candidates) == 1)
    await _copy_zip_candidates(connection, staging, cancel_check)
    await connection.execute(
        "WITH matches AS (SELECT r.source_ctid,z.zcta5ce AS zip5 FROM npi_zip_restore r JOIN "
        + candidates[0].qualified
        + " z ON public.ST_Covers(z.the_geom,public.ST_SetSRID(public.ST_Point(r.longitude,r.latitude),4269)) JOIN "
        + pinned_inputs["geo_zip_lookup"][0].qualified
        + " l ON l.zip_code=z.zcta5ce AND l.state=r.state_code "
        "WHERE r.country_code='US' AND r.state_code IS NOT NULL),one_zip AS (SELECT source_ctid,min(zip5) AS zip5 "
        "FROM matches GROUP BY source_ctid HAVING count(DISTINCT zip5)=1) UPDATE "
        + staging
        + " s SET postal_code=z.zip5 FROM one_zip z WHERE s.ctid=z.source_ctid::tid AND s.address_key IS NULL"
    )
    await connection.execute("DROP TABLE npi_zip_restore")


async def _copy_zip_candidates(connection, staging, cancel_check):
    """Normalize eligible coordinate rows through bounded native COPY."""
    await connection.execute(
        "CREATE TEMP TABLE npi_zip_restore(source_ctid text,state_code text,country_code text,latitude float8,longitude float8) ON COMMIT DROP"
    )
    query = (
        f"SELECT ctid::text AS source_ctid,state_name,country_code,lat AS latitude,long AS longitude FROM {staging} "
        "WHERE address_key IS NULL AND NULLIF(trim(postal_code),'') IS NULL AND lat IS NOT NULL AND long IS NOT NULL"
    )
    async for address_rows in _batches(connection, query):
        if cancel_check is not None:
            await cancel_check()
        await connection.copy_records_to_table(
            "npi_zip_restore",
            schema_name="pg_temp",
            records=[
                (
                    address_row["source_ctid"],
                    canon.state_code(address_row["state_name"]),
                    canon.country_code(address_row["country_code"]),
                    float(address_row["latitude"]),
                    float(address_row["longitude"]),
                )
                for address_row in address_rows
            ],
        )
