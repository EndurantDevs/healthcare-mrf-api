# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Connected alias candidate equivalence and native-key reuse checks."""

from uuid import UUID

from process import entity_address_batch as batch
from tests import entity_address_rows_preparation_support as fixtures
from tests.entity_address_batch_postgres_support import create_archive, database_fixture

_CASES = (
    ("equal", "equal", None),
    ("different", "different", None),
    ("stored_null", "stored_null", None),
    ("computed_null", "computed_null", None),
    ("both_null", "both_null", None),
    ("revoked", "equal", None),
    ("source_identity_mismatch", "equal", "source_identity_mismatch"),
    ("target_identity_mismatch", "equal", "target_identity_mismatch"),
    ("missing_or_merged_target", "equal", "missing_or_merged_target"),
    ("merged_target", "equal", "missing_or_merged_target"),
    ("multi_hop_alias", "equal", "multi_hop_alias"),
    ("unicode", "equal", None),
    ("ptg", "stored_null", None),
)


async def _seed_alias_cases(database, schema):
    """Give each key/guard case a separate checksum and target."""
    await create_archive(database, schema)
    await database.status(f"TRUNCATE {schema}.address_alias_v1,{schema}.address_archive_v2")
    await database.status(
        f"CREATE TABLE {schema}.raw_probe(checksum bigint,address_source text,address_key uuid,"
        + ",".join(f"{field} text" for field in batch.stage.ADDRESS_FIELDS)
        + ")"
    )
    for ordinal, (kind, mode, _) in enumerate(_CASES, 1):
        canonical, stored_key = await _seed_raw_case(database, schema, ordinal, kind, mode)
        await _seed_case_aliases(database, schema, ordinal, kind, canonical, stored_key)
    await database.status(f"INSERT INTO {schema}.raw_probe SELECT * FROM {schema}.raw_probe")


async def _seed_raw_case(database, schema, ordinal, kind, mode):
    """Retain exact source tuples while varying the stored/computed key pair."""
    values_by_field = dict(
        first_line=f"{ordinal} {'Café' if kind == 'unicode' else 'Example'} Street",
        second_line=None,
        city_name="Example",
        state_name="TX",
        postal_code="75001",
        country_code="CA" if mode in {"computed_null", "both_null"} else "US",
    )
    expressions = ",".join(":" + field for field in batch.stage.ADDRESS_FIELDS)
    canonical = await database.first(
        f"SELECT {schema}.addr_key_v1({expressions}) AS key,{schema}.addr_identity_key_v1({expressions}) AS identity",
        **values_by_field,
    )
    stored_key = canonical.key if mode == "equal" else UUID(int=1000 + ordinal)
    if mode in {"stored_null", "both_null"}:
        stored_key = None
    await database.status(
        f"INSERT INTO {schema}.raw_probe VALUES (:ordinal,:source,CAST(:stored AS uuid),{expressions})",
        ordinal=ordinal,
        source="ptg" if kind == "ptg" else "synthetic",
        stored=stored_key,
        **values_by_field,
    )
    return canonical, stored_key


async def _seed_case_aliases(database, schema, ordinal, kind, canonical, stored_key):
    """Keep each alias rule independent, including missing and merged targets."""
    target_key = UUID(int=2000 + ordinal)
    target_identity = f"target-{ordinal}"
    await database.status(
        f"INSERT INTO {schema}.address_archive_v2(address_key,identity_key,merged_into) "
        "VALUES(CAST(:key AS uuid),:identity,CAST(:merged AS uuid))",
        key=target_key,
        identity=target_identity,
        merged=UUID(int=9000) if kind == "merged_target" else None,
    )
    candidate_keys = {stored_key, canonical.key} - {None}
    for candidate_key in candidate_keys:
        await database.status(
            f"INSERT INTO {schema}.address_alias_v1 VALUES(CAST(:key AS uuid),CAST(:target AS uuid),"
            ":source_identity,:target_identity,CASE WHEN :revoked THEN NOW() ELSE NULL END)",
            key=candidate_key,
            target=UUID(int=9001) if kind == "missing_or_merged_target" else target_key,
            source_identity="changed" if kind == "source_identity_mismatch" else canonical.identity,
            target_identity="changed" if kind == "target_identity_mismatch" else target_identity,
            revoked=kind == "revoked",
        )
    if kind == "multi_hop_alias":
        await database.status(
            f"INSERT INTO {schema}.address_alias_v1 VALUES(CAST(:key AS uuid),CAST(:target AS uuid),"
            ":identity,'downstream',NULL)",
            key=target_key,
            target=UUID(int=9002),
            identity=target_identity,
        )


def _alias_statement(sql, schema, ordinal):
    """Use the production formatter with a counted SQL-only key arm."""
    fallback = sql._address_key_expr(schema, True, address_source="raw.address_source", table_alias="raw")
    counted = f"CASE WHEN nextval('{schema}.source_reads')>0 THEN {fallback} END"
    computed = batch.key_sql(counted, "raw.address_source", True)
    where = f"WHERE raw.checksum={ordinal}"
    statement = batch.ALIAS_INTEGRITY_SQL.format(
        db_schema=schema,
        raw_table="raw_probe",
        alias_table="address_alias_v1",
        archive_relation="address_archive_v2",
        computed_address_key=computed,
        checksum_where=where,
        source_identity_key=batch.identity_sql(schema, "raw"),
        batch_join=batch.join_sql("raw"),
    )
    return statement, computed, where


async def _matching_rows(database, schema, statement):
    """Measure fallback evaluation while retaining complete candidate cardinality."""
    await database.status(f"SELECT setval('{schema}.source_reads',1,false)")
    rows = await database.all(statement)
    counter = await database.first(f"SELECT last_value,is_called FROM {schema}.source_reads")
    return sorted(tuple(row) for row in rows), int(counter.last_value) if counter.is_called else 0


async def assert_alias_candidate_equivalence(monkeypatch, *, native):
    """Retain original IN-set behavior, all alias guards and fallback bounds."""
    async with database_fixture(monkeypatch) as (sql, database, schema):
        await _seed_alias_cases(database, schema)

        @batch.scope
        async def compare():
            if native:
                await batch.prepare_raw(sql, schema, "raw_probe")
                assert batch._TABLE.get() is not None
            for ordinal, (kind, _, violation) in enumerate(_CASES, 1):
                statement, computed, where = _alias_statement(sql, schema, ordinal)
                projection = (
                    "active.source_address_key,active.target_address_key,active.source_identity_key,"
                    "active.target_identity_key," + batch.identity_sql(schema, "raw") + ","
                    "target.identity_key,target.address_key"
                )
                oracle = f"""SELECT {projection}
                    FROM (SELECT DISTINCT address_key,address_source,{",".join(batch.stage.ADDRESS_FIELDS)}
                        FROM {schema}.raw_probe raw {where}) raw
                    {batch.join_sql("raw")}
                    JOIN {schema}.address_alias_v1 active ON active.source_address_key IN(raw.address_key,{computed})
                        AND active.revoked_at IS NULL
                    LEFT JOIN {schema}.address_archive_v2 target ON target.address_key=active.target_address_key
                        AND target.merged_into IS NULL"""
                expected, old_calls = await _matching_rows(database, schema, oracle)
                matching = statement.split("), violations AS (", 1)[0] + ") SELECT * FROM matching"
                actual, calls = await _matching_rows(database, schema, matching)
                assert actual == expected, kind
                assert len(actual) == (
                    2 if kind == "different" else 0 if kind in {"both_null", "revoked", "ptg"} else 1
                )
                assert calls <= old_calls, (kind, calls, old_calls)
                if native and kind != "unicode":
                    assert calls == 0, kind
                if kind == "unicode":
                    assert calls == 1, (native, calls)
                found = await database.first(statement)
                assert (found.violation_kind if found else None) == violation, kind

        await compare()


async def assert_native_archive_keys_skip_sql(monkeypatch):
    """Accepted rows must never enter SQL key/identity arms in either consumer."""
    async with database_fixture(monkeypatch) as (sql, database, schema):
        await create_archive(database, schema)
        expressions = ",".join(batch.stage.ADDRESS_FIELDS)
        await database.status(
            f"UPDATE {schema}.address_alias_v1 SET source_identity_key=source.identity,"
            "target_identity_key=source.identity,target_address_key=source.key "
            f"FROM(SELECT {schema}.addr_key_v1({expressions}) AS key,"
            f"{schema}.addr_identity_key_v1({expressions}) AS identity "
            f"FROM {schema}.source_rows WHERE entity_id='office-one' LIMIT 1) source"
        )
        await database.status(sql._insert_raw_from_source_sql(schema, "raw_actual", fixtures.source_select_sql(schema)))
        original_key = sql._address_key_expr
        original_identity = batch.identity_sql

        def tripwire(expression, alias, *, identity=False):
            """The unsupported Unicode row retains the real SQL fallback."""
            invalid_key = f"(({alias}.first_line || '!sql-fallback')::uuid)"
            if identity:
                invalid_key += "::text"
            return f"CASE WHEN {alias}.first_line LIKE '%K%' THEN {expression} ELSE {invalid_key} END"

        def guarded_key(schema, available, *, address_source=None, table_alias=None):
            return tripwire(
                original_key(schema, available, address_source=address_source, table_alias=table_alias), table_alias
            )

        def guarded_identity(schema, alias):
            expressions = ",".join(f"{alias}.{field}" for field in batch.stage.ADDRESS_FIELDS)
            return batch.field_sql(
                "identity_key", tripwire(f"{schema}.addr_identity_key_v1({expressions})", alias, identity=True)
            )

        @batch.scope
        async def consume():
            await batch.prepare_raw(sql, schema, "raw_actual")
            assert batch._TABLE.get() is not None
            monkeypatch.setattr(sql, "_address_key_expr", guarded_key)
            monkeypatch.setattr(batch, "identity_sql", guarded_identity)
            await sql._validate_raw_alias_integrity(schema, "raw_actual", is_address_canon_available=True)
            await database.status(sql._enrich_raw_stage_sql(schema, "raw_actual"))
            await sql._validate_raw_alias_integrity(schema, "raw_actual", is_address_canon_available=True)

        try:
            await consume()
        finally:
            monkeypatch.setattr(sql, "_address_key_expr", original_key)
            monkeypatch.setattr(batch, "identity_sql", original_identity)
