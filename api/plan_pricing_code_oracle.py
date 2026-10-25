# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Independent scalar-input proof for one nonpublishing scoped calibration."""

import hashlib

from sqlalchemy import text

from api.plan_pricing_projection_contract import table
from process import reference_family_archive as native

MAX_CALIBRATION_BYTES = 256 * 1024 * 1024
MAX_ENCODED_BYTES = 384 * 1024 * 1024
MAX_FRAME_BYTES = 40000
NPI_BATCH = 100
RAW_FIELDS = "npi,geo_cell,location_key,type,address_key,premise_key,address_source_mask,geo_assurance_version,geo_evidence_source_id"


def _raw_projection(serving):
    zip5 = serving._ptg2_address_zip5_sql("addr", unified=True)
    evidence = serving._ptg2_geo_evidence_level_sql("addr")
    return f"""SELECT addr.npi,{zip5} AS geo_cell,addr.location_key,addr.type,addr.address_key,
        addr.premise_key,addr.address_source_mask,addr.geo_assurance_version,addr.geo_evidence_source_id,
        ({evidence}) AS oracle_evidence
        FROM {table("entity_address_unified")} addr"""


def _factor_queries(binding, found):
    from api import ptg2_serving as serving

    schema = native._quoted(found[0].schema_name)
    dictionary = serving._ptg2_npi_scope_table(binding.serving_tables)
    first = f"""SELECT dictionary.npi,first.member_key AS factor FROM {dictionary} dictionary
        JOIN {schema}.ptg_membership_first first ON first.owner_key=dictionary.npi_key
        WHERE dictionary.snapshot_key=:snapshot AND dictionary.npi=ANY(CAST(:npis AS bigint[]))"""
    priced = f"""SELECT second.owner_key AS factor,second.member_key AS provider_set_key
        FROM {schema}.ptg_membership_second second
        WHERE second.owner_key IN (SELECT factor FROM ({first}) selected)
        AND EXISTS(SELECT 1 FROM {schema}.ptg_membership_code code
            WHERE code.provider_set_key=second.member_key AND code.code_key=ANY(CAST(:code_keys AS bigint[])))"""
    return first, priced


async def _equal(session, left, right):
    equal = await session.scalar(
        text(f"SELECT NOT EXISTS((({left}) EXCEPT ALL ({right})) UNION ALL (({right}) EXCEPT ALL ({left})))")
    )
    if equal is not True:
        raise ValueError("code calibration independent input multiset differs")


async def require_budget(session, schema=None):
    """Measure only this run's temporary/native stage, never alter unrelated resources."""
    size = int(
        await session.scalar(
            text("""SELECT coalesce(sum(pg_total_relation_size(c.oid)),0)
        FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE c.relkind IN ('r','m') AND
        ((c.relnamespace=pg_my_temp_schema() AND (c.relname LIKE 'code_calibration_%'
            OR c.relname LIKE 'plan_pricing_%')) OR n.nspname=:schema)"""),
            {"schema": schema},
        )
    )
    if size > MAX_CALIBRATION_BYTES:
        raise ValueError("code calibration candidate byte budget exceeded")
    return size


class CodeInputOracle:
    """Own complete scalar evidence and measured budgets inside one rollback transaction."""

    def __init__(self, session, identity, inputs):
        self.session, self.identity, self.inputs = session, identity, inputs
        self.work = dict.fromkeys(("raw_address", "classified_address", "address", "first", "priced", "factor"), 0)
        self.batch_count = 0
        self.verified_batches = 0
        self.schema = None

    async def prepare(self):
        """Exhaust every server-selected cell, including raw nonmatches and duplicates."""
        from api import ptg2_serving as serving

        projection = _raw_projection(serving)
        await self.session.execute(
            text(f"CREATE TEMP TABLE code_calibration_raw ON COMMIT DROP AS {projection} WHERE FALSE")
        )
        await self.session.execute(
            text("CREATE TEMP TABLE code_calibration_seen(npi bigint PRIMARY KEY) ON COMMIT DROP")
        )
        await self.session.execute(
            text(
                "CREATE TEMP TABLE code_calibration_members(binding_ordinal integer,provider_set_key bigint,npi bigint,PRIMARY KEY(binding_ordinal,provider_set_key,npi)) ON COMMIT DROP"
            )
        )
        await self.session.execute(
            text(
                "CREATE TEMP TABLE code_calibration_batches(batch integer,npi bigint,raw_rows bigint,PRIMARY KEY(batch,npi)) ON COMMIT DROP"
            )
        )
        for cell in self.identity["scope"]["geo_cells"]:
            await self._cell(serving, projection, cell)
        await self._finish_counts()
        return {
            "raw_address": max(1, self.work["raw_address"]),
            "address": max(1, self.work["classified_address"], self.work["address"]),
            **{name: max(1, self.work[name]) for name in ("first", "priced", "factor")},
            "member_cell": max(1, self.membership_count, self.member_cell_count),
        }

    async def _cell(self, serving, projection, cell):
        position = 0
        zip5 = serving._ptg2_address_zip5_sql("addr", unified=True)
        while True:
            group_rows = (
                await self.session.execute(
                    text(f"""SELECT addr.npi,count(*)::bigint AS raw_rows
                FROM {table("entity_address_unified")} addr
                WHERE addr.type IN ('practice','primary','secondary','site') AND addr.npi>0
                AND {zip5}=:cell AND addr.npi>:position
                GROUP BY addr.npi ORDER BY addr.npi LIMIT {NPI_BATCH}"""),
                    {"cell": cell, "position": position},
                )
            ).all()
            if not group_rows:
                return
            npis = [int(evidence_row[0]) for evidence_row in group_rows]
            await self._raw_batch(projection, zip5, cell, group_rows, npis)
            position = npis[-1]

    async def _raw_batch(self, projection, zip5, cell, group_rows, npis):
        parameters_by_name = {"cell": cell, "npis": npis}
        selected = (
            projection
            + f" WHERE addr.type IN ('practice','primary','secondary','site') AND {zip5}=:cell AND addr.npi=ANY(CAST(:npis AS bigint[]))"
        )
        projected_bytes = int(
            await self.session.scalar(
                text(f"SELECT coalesce(sum(pg_column_size(raw)),0) FROM ({selected}) raw"), parameters_by_name
            )
        )
        if await require_budget(self.session) + projected_bytes > MAX_CALIBRATION_BYTES:
            raise ValueError("code calibration raw byte budget exceeded")
        await self.session.execute(text("INSERT INTO code_calibration_raw " + selected), parameters_by_name)
        driver = (await (await self.session.connection()).get_raw_connection()).driver_connection
        await driver.copy_records_to_table(
            "code_calibration_batches",
            schema_name="pg_temp",
            columns=("batch", "npi", "raw_rows"),
            records=[(self.batch_count, int(npi), int(count)) for npi, count in group_rows],
        )
        pending = (
            (
                await self.session.execute(
                    text("""INSERT INTO code_calibration_seen
            SELECT unnest(CAST(:npis AS bigint[])) ON CONFLICT DO NOTHING RETURNING npi"""),
                    parameters_by_name,
                )
            )
            .scalars()
            .all()
        )
        for binding, found in self.inputs:
            await self._members(binding, found, list(pending))
        self.work["raw_address"] += sum(int(evidence_row[1]) for evidence_row in group_rows)
        self.batch_count += 1
        await require_budget(self.session)

    async def _members(self, binding, found, npis):
        from api import ptg2_serving as serving

        first, priced = _factor_queries(binding, found)
        parameters_by_name = {
            "npis": npis,
            "snapshot": serving._required_shared_snapshot_key(binding.serving_tables),
            "code_keys": [
                int(evidence_row["code_key"])
                for group_rows in binding.code_rows_by_identity.values()
                for evidence_row in group_rows
            ],
            "ordinal": binding.binding["ordinal"],
        }
        first_count = int(
            await self.session.scalar(text(f"SELECT count(*) FROM ({first}) original"), parameters_by_name)
        )
        priced_count = int(
            await self.session.scalar(text(f"SELECT count(*) FROM ({priced}) original"), parameters_by_name)
        )
        joined = f"FROM ({first}) original JOIN ({priced}) selected USING(factor)"
        factor_count = int(await self.session.scalar(text("SELECT count(*) " + joined), parameters_by_name))
        for name, count in (("first", first_count), ("priced", priced_count), ("factor", factor_count)):
            self.work[name] += count
        members = "SELECT DISTINCT CAST(:ordinal AS integer) AS binding_ordinal,provider_set_key,npi " + joined
        size = int(
            await self.session.scalar(
                text(f"SELECT coalesce(sum(pg_column_size(member)),0) FROM ({members}) member"), parameters_by_name
            )
        )
        if await require_budget(self.session) + size > MAX_CALIBRATION_BYTES:
            raise ValueError("code calibration membership byte budget exceeded")
        await self.session.execute(text("INSERT INTO code_calibration_members " + members), parameters_by_name)

    async def _finish_counts(self):
        matched = "EXISTS(SELECT 1 FROM code_calibration_members member WHERE member.npi=raw.npi)"
        self.work["classified_address"] = int(
            await self.session.scalar(text(f"SELECT count(*) FROM code_calibration_raw raw WHERE {matched}"))
        )
        self.work["address"] = int(
            await self.session.scalar(
                text(f"SELECT count(*) FROM code_calibration_raw raw WHERE {matched} AND oracle_evidence IS NOT NULL")
            )
        )
        self.membership_count = int(await self.session.scalar(text("SELECT count(*) FROM code_calibration_members")))
        self.member_cell_count = int(
            await self.session.scalar(
                text("""SELECT count(*) FROM
            (SELECT DISTINCT npi,geo_cell FROM code_calibration_raw WHERE oracle_evidence IS NOT NULL) cells
            JOIN code_calibration_members USING(npi)""")
            )
        )

    async def verify_batch(self, session, cell):
        """Compare the complete grouped batch with independent scalar evidence."""
        batch = self.verified_batches
        await _equal(
            session,
            "SELECT npi,raw_rows FROM plan_pricing_zip_npi",
            f"SELECT npi,raw_rows FROM code_calibration_batches WHERE batch={batch}",
        )
        wanted = f"npi IN (SELECT npi FROM code_calibration_batches WHERE batch={batch})"
        matched = "EXISTS(SELECT 1 FROM code_calibration_members member WHERE member.npi=raw.npi)"
        raw = f"SELECT {RAW_FIELDS} FROM code_calibration_raw raw WHERE {wanted} AND geo_cell='{cell}' AND {matched}"
        await _equal(session, f"SELECT {RAW_FIELDS} FROM plan_pricing_zip_address", raw)
        assured = f"SELECT npi,geo_cell FROM code_calibration_raw raw WHERE {wanted} AND geo_cell='{cell}' AND {matched} AND oracle_evidence IS NOT NULL"
        await _equal(session, "SELECT npi,geo_cell FROM plan_pricing_zip_assured", assured)
        self.verified_batches += 1
        await require_budget(session, self.schema)

    async def verify(self, audit, ownership):
        """Require exact six-unit work and full membership/cell multisets before rollback."""
        observed = audit["zip_input_work"]
        if (
            observed["work"] != self.work
            or not observed["complete"]
            or not observed["exhausted"]
            or observed["membership_count"] != self.membership_count
            or observed["member_cell_count"] != self.member_cell_count
            or self.verified_batches != self.batch_count
        ):
            raise ValueError("code calibration independent work census differs")
        schema = native._quoted(ownership.schema_name)
        members = "SELECT binding_ordinal,provider_set_key,npi FROM code_calibration_members member WHERE EXISTS(SELECT 1 FROM code_calibration_raw raw WHERE raw.npi=member.npi AND oracle_evidence IS NOT NULL)"
        await _equal(
            self.session,
            f"SELECT binding_ordinal,provider_set_key,npi FROM {schema}.plan_pricing_code_membership",
            members,
        )
        cells = "SELECT DISTINCT raw.npi,geo_cell FROM code_calibration_raw raw WHERE oracle_evidence IS NOT NULL AND EXISTS(SELECT 1 FROM code_calibration_members member WHERE member.npi=raw.npi)"
        await _equal(self.session, f"SELECT npi,geo_cell FROM {schema}.plan_pricing_code_provider_cell", cells)
        from api.plan_pricing_code_oracle_rates import verify_rate_inputs

        rate_proof = await verify_rate_inputs(self.session, ownership, self.inputs)
        input_digest = await self._digest()
        return {
            "rates": rate_proof,
            "work": dict(self.work),
            "membership_count": self.membership_count,
            "member_cell_count": self.member_cell_count,
            "batches": self.batch_count,
            "input_multiset_sha256": input_digest,
            "encoded_input_bytes": self.encoded_bytes,
            "complete": True,
        }

    async def _digest(self):
        digest = hashlib.sha256()
        self.encoded_bytes = 0
        statement = text(
            'SELECT row_to_json(raw)::text FROM code_calibration_raw raw ORDER BY row_to_json(raw)::text COLLATE "C"'
        )
        result = await self.session.stream(statement)
        try:
            async for evidence_row in result:
                encoded = evidence_row[0].encode("utf-8")
                if len(encoded) > MAX_FRAME_BYTES:
                    raise ValueError("code calibration evidence record exceeds its byte bound")
                self.encoded_bytes += 4 + len(encoded)
                if self.encoded_bytes > MAX_ENCODED_BYTES:
                    raise ValueError("code calibration encoded input exceeds its byte bound")
                digest.update(len(encoded).to_bytes(4, "big") + encoded)
        finally:
            await result.close()
        return digest.hexdigest()
