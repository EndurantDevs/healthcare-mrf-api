# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Replay source-only facility observations against bounded destination rows."""

from sqlalchemy import text

from process.entity_address_snapshot_alias import capture_entity_address_alias_semantic_receipt
from process.ext.address_canon import (
    ADDRESS_FORMAT_FUNCTION,
    ADDRESS_FORMAT_SOURCE,
    ADDRESS_FORMAT_VERSION,
    CURRENT_ADDRESS_IDENTITY_VERSION,
    _apply_persisted_address_aliases,
    _key_from_identity_sql,
    _qtable,
    archive_table_name,
    current_canon_version,
)
from process.facility_address_contribution_capture import (
    CANONICAL_COLUMNS,
    CONTRACT,
    METADATA_KEY,
    require_capture_bounds,
)

KEYED = "pg_temp.facility_contribution_keyed"
SCRATCH = "pg_temp.facility_contribution_after"
KEYS = "pg_temp.facility_contribution_keys"
DISPLAY_COLUMNS = ("first_line", "second_line", "city_name", "state_name", "postal_code")


def require(condition, reason):
    """Reject unsupported or changed contribution state without payload output."""
    if not condition:
        raise RuntimeError("Facility address contribution " + reason)


async def validate_observations(session, *, stage_schema, schema, bind_alias=True):
    """Validate the bounded source contract and portable alias dependency."""
    table = _qtable(stage_schema, "facility_address_contribution")
    await require_capture_bounds(session, schema=stage_schema, contribution_table="facility_address_contribution")
    metadata_rows = (
        await session.execute(text(f"SELECT address_key,payload FROM {table} WHERE kind='metadata'"))
    ).all()
    require(len(metadata_rows) == 1 and str(metadata_rows[0].address_key) == METADATA_KEY, "metadata is invalid")
    metadata = metadata_rows[0].payload
    require(
        isinstance(metadata, dict)
        and set(metadata)
        == {
            "contract",
            "enabled",
            "canon_version",
            "alias_semantics",
            "source_bit",
            "priority",
        },
        "metadata is invalid",
    )
    require(
        metadata["contract"] == CONTRACT
        and type(metadata["enabled"]) is bool
        and metadata["canon_version"] == current_canon_version()
        and type(metadata["source_bit"]) is int
        and metadata["source_bit"] == 8
        and type(metadata["priority"]) is int
        and metadata["priority"] == 4,
        "semantics differ",
    )
    await _validate_payloads(session, table)
    if not metadata["enabled"]:
        count = await session.scalar(text(f"SELECT count(*) FROM {table} WHERE kind <> 'metadata'"))
        require(count == 0 and metadata["alias_semantics"] is None, "disabled payload is invalid")
        return metadata, None
    if not bind_alias:
        require(isinstance(metadata["alias_semantics"], dict), "alias semantics are missing")
        return metadata, None
    alias = await capture_entity_address_alias_semantic_receipt(session, schema_name=schema)
    require(alias.portable_identity() == metadata["alias_semantics"], "alias semantics differ")
    return metadata, alias.as_dict()


async def _validate_payloads(session, table):
    invalid = await session.scalar(
        text(f"""
        SELECT EXISTS (SELECT 1 FROM {table} WHERE
          kind NOT IN ('metadata','canonical','geocode') OR jsonb_typeof(payload) <> 'object'
          OR (kind='canonical' AND (
            (SELECT array_agg(k ORDER BY k) FROM jsonb_object_keys(payload) k) IS DISTINCT FROM :fields
            OR EXISTS (SELECT 1 FROM jsonb_each(payload) v
                       WHERE jsonb_typeof(v.value) NOT IN ('string','null'))
            OR payload->>'identity_key' IS NULL))
          OR (kind='geocode' AND (
            (SELECT array_agg(k ORDER BY k) FROM jsonb_object_keys(payload) k) IS DISTINCT FROM ARRAY['lat','long']
            OR jsonb_typeof(payload->'lat') IS DISTINCT FROM 'number'
            OR jsonb_typeof(payload->'long') IS DISTINCT FROM 'number'
            OR (payload->>'lat')::numeric NOT BETWEEN -90 AND 90
            OR (payload->>'long')::numeric NOT BETWEEN -180 AND 180)))
    """),
        {"fields": sorted(CANONICAL_COLUMNS)},
    )
    require(not invalid, "payload is invalid")


async def project_observations(session, *, stage_schema, schema):
    """Use native alias rules, then apply native merge policy to affected rows only."""
    table = _qtable(stage_schema, "facility_address_contribution")
    archive = _qtable(schema, archive_table_name())
    fields = ",".join(
        f"(payload->>'{name}')::uuid AS {name}" if name == "premise_key" else f"payload->>'{name}' AS {name}"
        for name in CANONICAL_COLUMNS
    )
    await session.execute(
        text(
            f"CREATE TEMP TABLE facility_contribution_keyed ON COMMIT DROP AS "
            f"SELECT row_number() OVER (ORDER BY address_key) rn, address_key, "
            f"address_key computed_address_key,{fields} FROM {table} WHERE kind='canonical'"
        )
    )
    invalid = await session.scalar(
        text(
            f"SELECT EXISTS (SELECT 1 FROM {KEYED} WHERE "
            f"address_key IS DISTINCT FROM {_key_from_identity_sql(schema, 'identity_key')})"
        )
    )
    require(not invalid, "key identity differs")
    await _apply_persisted_address_aliases(session, schema=schema, keyed_table=KEYED, archive=archive)
    await session.execute(
        text(
            f"CREATE TEMP TABLE facility_contribution_keys ON COMMIT DROP AS "
            f"SELECT address_key FROM {KEYED} UNION SELECT address_key FROM {table} WHERE kind='geocode'"
        )
    )
    await session.execute(text(f"CREATE UNIQUE INDEX ON {KEYS}(address_key)"))
    await session.execute(
        text(f"CREATE TEMP TABLE facility_contribution_after (LIKE {archive} INCLUDING ALL) ON COMMIT DROP")
    )
    await session.execute(text(f"INSERT INTO {SCRATCH} SELECT a.* FROM {archive} a JOIN {KEYS} k USING(address_key)"))
    await _require_identity_collision_free(session)
    await _merge_canonical(session, schema)
    await _merge_geocodes(session, table)


async def _require_identity_collision_free(session):
    invalid = await session.scalar(
        text(f"""
        SELECT EXISTS (SELECT 1 FROM {KEYED} GROUP BY address_key HAVING count(DISTINCT identity_key)>1)
        OR EXISTS (SELECT 1 FROM {KEYED} k JOIN {SCRATCH} a USING(address_key)
                   WHERE a.identity_key IS DISTINCT FROM k.identity_key)
    """)
    )
    require(not invalid, "key collision")


def _dedup_sql():
    """The native resolver's display-quality ordering after alias projection."""
    return f"""SELECT DISTINCT ON (address_key) * FROM {KEYED} ORDER BY address_key,
        first_line IS NULL,length(COALESCE(first_line,'')) DESC,
        second_line IS NULL,length(COALESCE(second_line,'')) DESC,
        city_name IS NULL,length(COALESCE(city_name,'')) DESC,
        COALESCE(first_line,''),COALESCE(second_line,''),COALESCE(city_name,''),
        COALESCE(state_name,''),COALESCE(postal_code,''),identity_key"""


async def _merge_canonical(session, schema):
    renderer = _qtable(schema, ADDRESS_FORMAT_FUNCTION)
    value_expressions = ",".join(
        "COALESCE(d.unit_norm,'')" if name == "unit_norm" else f"d.{name}" for name in CANONICAL_COLUMNS
    )
    parameter_by_name = {"version": ADDRESS_FORMAT_VERSION, "source": ADDRESS_FORMAT_SOURCE}
    await session.execute(
        text(f"""
        INSERT INTO {SCRATCH} (address_key,{",".join(CANONICAL_COLUMNS)},identity_version,precision,
          formatted_address,formatted_address_version,formatted_address_source,source_bits,strict_source_bits,display_priority)
        SELECT d.address_key,{value_expressions},{CURRENT_ADDRESS_IDENTITY_VERSION},
          CASE WHEN split_part(d.identity_key,'|',8)='city_zip' THEN 'city_zip' ELSE 'street' END,
          {renderer}(d.first_line,d.second_line,d.city_name,d.state_name,d.postal_code,d.country_code),
          :version,:source,8,0,4 FROM ({_dedup_sql()}) d
        WHERE NOT EXISTS (SELECT 1 FROM {SCRATCH} a WHERE a.address_key=d.address_key)
        ON CONFLICT (address_key) DO NOTHING
    """),
        parameter_by_name,
    )
    updates = ",".join(
        f"{name}=CASE WHEN 4<a.display_priority THEN d.{name} ELSE a.{name} END" for name in DISPLAY_COLUMNS
    )
    rendered = ",".join(f"CASE WHEN 4<a.display_priority THEN d.{name} ELSE a.{name} END" for name in DISPLAY_COLUMNS)
    await session.execute(
        text(f"""
        UPDATE {SCRATCH} a SET source_bits=a.source_bits|8,last_seen_at=now(),
          display_priority=CASE WHEN 4<a.display_priority THEN 4 ELSE a.display_priority END,{updates},
          formatted_address={renderer}({rendered},a.country_code),
          formatted_address_version=:version,formatted_address_source=:source
        FROM ({_dedup_sql()}) d WHERE a.address_key=d.address_key
          AND (a.source_bits & 8=0 OR 4<a.display_priority)
    """),
        parameter_by_name,
    )


async def _merge_geocodes(session, table):
    await session.execute(
        text(f"""
        UPDATE {SCRATCH} a SET lat=(g.payload->>'lat')::numeric(11,8),
          long=(g.payload->>'long')::numeric(11,8),geo_source=COALESCE(a.geo_source,'manual'),
          geocode_source=COALESCE(a.geocode_source,'facility_anchor'),
          geocode_quality=COALESCE(a.geocode_quality,'facility_anchor'),geocoded_at=COALESCE(a.geocoded_at,now())
        FROM {table} g WHERE g.kind='geocode' AND a.address_key=g.address_key AND a.lat IS NULL AND a.long IS NULL
    """)
    )
