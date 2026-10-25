# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Complete ZIP NPI groups and indexed procedure membership intersections."""

from sqlalchemy import text

from api.plan_pricing_projection_contract import table
from process import reference_family_archive as native
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError

# Independent actual-work units; synthetic parity does not qualify these limits.
MAX_ZIP_RAW_ADDRESS_WORK_ROWS: int | None = None
MAX_ZIP_ADDRESS_WORK_ROWS: int | None = None
MAX_ZIP_FIRST_FACTOR_ROWS: int | None = None
MAX_ZIP_PRICED_FACTOR_ROWS: int | None = None
MAX_ZIP_FACTOR_JOIN_WORK_ROWS: int | None = None
MAX_ZIP_MEMBER_CELL_WORK_ROWS: int | None = None


def work_limits():
    """Refuse preparation until every scoped work unit has native qualification."""
    limits_by_name = {
        "raw_address": MAX_ZIP_RAW_ADDRESS_WORK_ROWS,
        "address": MAX_ZIP_ADDRESS_WORK_ROWS,
        "first": MAX_ZIP_FIRST_FACTOR_ROWS,
        "priced": MAX_ZIP_PRICED_FACTOR_ROWS,
        "factor": MAX_ZIP_FACTOR_JOIN_WORK_ROWS,
        "member_cell": MAX_ZIP_MEMBER_CELL_WORK_ROWS,
    }
    if any(type(value) is not int or value <= 0 for value in limits_by_name.values()):
        raise ValueError("code pricing ZIP work bounds are not calibrated")
    return limits_by_name


async def _bounded_count(session, name, limit):
    count = int(await session.scalar(text(f"SELECT count(*) FROM {name}")))
    if count > limit:
        raise ValueError("code pricing ZIP " + name.removeprefix("plan_pricing_zip_") + " work bound exceeded")
    return count


async def _create_zip_stages(session):
    from api import ptg2_serving as serving

    zip5 = serving._ptg2_address_zip5_sql("addr", unified=True)
    await session.execute(
        text(f"""CREATE TEMP TABLE plan_pricing_zip_address ON COMMIT DROP AS
        SELECT addr.npi,{zip5} AS geo_cell,addr.location_key,addr.type,addr.address_key,
               addr.premise_key,addr.address_source_mask,addr.geo_assurance_version,addr.geo_evidence_source_id
          FROM {table("entity_address_unified")} addr WHERE FALSE""")
    )
    for name, columns in (
        ("npi", "npi bigint PRIMARY KEY,raw_rows bigint NOT NULL"),
        ("assured", "npi bigint NOT NULL,geo_cell varchar(5) NOT NULL"),
        ("first", "npi bigint NOT NULL,factor bigint NOT NULL,PRIMARY KEY(npi,factor)"),
        ("priced", "factor bigint NOT NULL,provider_set_key bigint NOT NULL,PRIMARY KEY(factor,provider_set_key)"),
    ):
        await session.execute(text(f"CREATE TEMP TABLE plan_pricing_zip_{name} ({columns}) ON COMMIT DROP"))


async def _stage_address_batch(session, cell, position, remaining):
    """Enumerate whole NPI groups and charge every raw row, including nonmatches."""
    from api import ptg2_serving as serving
    from api.plan_pricing_code_build import CODE_PROVIDER_READ_BATCH_SIZE

    zip5 = serving._ptg2_address_zip5_sql("addr", unified=True)
    await session.execute(
        text("TRUNCATE plan_pricing_zip_address,plan_pricing_zip_npi,plan_pricing_provider_npi_pending_stage")
    )
    await session.execute(
        text(f"""INSERT INTO plan_pricing_zip_npi
        SELECT addr.npi,count(*)::bigint
          FROM {table("entity_address_unified")} addr
         WHERE addr.type IN ('practice','primary','secondary','site') AND addr.npi>0
           AND {zip5}=:cell AND addr.npi>:after_npi
         GROUP BY addr.npi ORDER BY addr.npi LIMIT :probe_limit"""),
        {"cell": cell, "after_npi": position, "probe_limit": min(CODE_PROVIDER_READ_BATCH_SIZE, remaining + 1)},
    )
    count = int(await session.scalar(text("SELECT coalesce(sum(raw_rows),0) FROM plan_pricing_zip_npi")))
    if count > remaining:
        raise ValueError("code pricing ZIP raw address work bound exceeded")
    if not count:
        return 0, position
    last = int(await session.scalar(text("SELECT max(npi) FROM plan_pricing_zip_npi")))
    await session.execute(
        text("""INSERT INTO plan_pricing_provider_npi_pending_stage
        SELECT wanted.npi FROM plan_pricing_zip_npi wanted
         WHERE NOT EXISTS(SELECT 1 FROM plan_pricing_provider_npi_materialized_stage done WHERE done.npi=wanted.npi)""")
    )
    return count, last


async def _stage_matching_addresses(session, cell, remaining):
    """Fetch every raw address only after its complete procedure membership matched."""
    from api import ptg2_serving as serving

    matched = """SELECT wanted.npi FROM plan_pricing_zip_npi wanted
        WHERE EXISTS(SELECT 1 FROM plan_pricing_provider_member_stage member WHERE member.npi=wanted.npi)"""
    expected = int(
        await session.scalar(
            text(f"SELECT coalesce(sum(raw_rows),0) FROM plan_pricing_zip_npi WHERE npi IN ({matched})")
        )
    )
    if expected > remaining:
        raise ValueError("code pricing ZIP classified address work bound exceeded")
    zip5 = serving._ptg2_address_zip5_sql("addr", unified=True)
    await session.execute(
        text(f"""INSERT INTO plan_pricing_zip_address
        SELECT addr.npi,{zip5},addr.location_key,addr.type,addr.address_key,
               addr.premise_key,addr.address_source_mask,addr.geo_assurance_version,addr.geo_evidence_source_id
          FROM {table("entity_address_unified")} addr
         WHERE addr.type IN ('practice','primary','secondary','site') AND addr.npi>0
           AND {zip5}=:cell AND addr.npi IN ({matched})"""),
        {"cell": cell},
    )
    actual = int(await session.scalar(text("SELECT count(*) FROM plan_pricing_zip_address")))
    if actual != expected:
        raise ValueError("code pricing ZIP grouped raw address coverage differs")


async def _stage_provider_cells(session, candidate_id, remaining, cell_remaining):
    """Classify the complete raw batch only after its exact procedure intersection."""
    from api import ptg2_serving as serving

    located = """SELECT addr.* FROM plan_pricing_zip_address addr
        WHERE EXISTS(SELECT 1 FROM plan_pricing_provider_member_stage member WHERE member.npi=addr.npi)"""
    classified_count = int(await session.scalar(text(f"SELECT count(*) FROM ({located}) matched")))
    if classified_count > remaining:
        raise ValueError("code pricing ZIP classified address work bound exceeded")
    await session.execute(text("TRUNCATE plan_pricing_zip_assured"))
    classified = serving._complete_unified_address_assurance_ctes_sql(located)
    await session.execute(
        text(f"""INSERT INTO plan_pricing_zip_assured {classified}
        SELECT npi,geo_cell FROM classified_locations WHERE geo_evidence_level IS NOT NULL""")
    )
    assured_count = await _bounded_count(session, "plan_pricing_zip_assured", remaining)
    cells = """SELECT DISTINCT addr.npi,addr.geo_cell FROM plan_pricing_zip_assured addr
        WHERE NOT EXISTS(SELECT 1 FROM plan_pricing_provider_cell_stage prior
            WHERE prior.projection_id=:id AND prior.npi=addr.npi AND prior.geo_cell=addr.geo_cell)"""
    member_cells = int(
        await session.scalar(
            text(f"""WITH cells AS ({cells}), members AS (
        SELECT npi,count(*)::numeric AS count FROM plan_pricing_provider_member_stage
         WHERE npi IN (SELECT npi FROM cells) GROUP BY npi)
        SELECT coalesce(sum(members.count),0) FROM cells JOIN members USING(npi)"""),
            {"id": candidate_id},
        )
    )
    if member_cells > cell_remaining:
        raise ValueError("code pricing ZIP member-cell work bound exceeded")
    await session.execute(
        text(f"""WITH cells AS MATERIALIZED ({cells}), traits AS MATERIALIZED (
            SELECT wanted.npi,n.entity_type_code,ARRAY(
                SELECT DISTINCT upper(btrim(nt.healthcare_provider_taxonomy_code))
                  FROM {table("npi_taxonomy")} nt WHERE nt.npi=wanted.npi
                   AND NULLIF(btrim(nt.healthcare_provider_taxonomy_code),'') IS NOT NULL
                 ORDER BY 1) AS taxonomy_codes
              FROM (SELECT DISTINCT npi FROM cells) wanted
              LEFT JOIN {table("npi")} n USING(npi)
        ) INSERT INTO plan_pricing_provider_cell_stage
          (projection_id,npi,geo_cell,entity_type_code,taxonomy_codes,fragment,state_fragment)
        SELECT :id,cells.npi,cells.geo_cell,traits.entity_type_code,traits.taxonomy_codes,
               CAST('' AS bytea),NULL FROM cells JOIN traits USING(npi)"""),
        {"id": candidate_id},
    )
    return classified_count, assured_count, member_cells


def _record_batch_maxima(maxima_by_name, measured_by_name):
    for name, count in measured_by_name.items():
        maxima_by_name[name] = max(maxima_by_name.get(name, 0), count)


def _zip_input_audit(scope, audit_by_name):
    measured_by_name = {
        "work": dict.fromkeys(("raw_address", "classified_address", "address", "first", "priced", "factor"), 0),
        "membership_count": 0,
        "member_cell_count": 0,
        "enumeration_pages": 0,
        "batches_started": 0,
        "batches_completed": 0,
        "cells": len(scope["geo_cells"]),
        "exhausted_cells": 0,
        "exhausted": False,
        "complete": False,
        "batch_maxima": {},
    }
    if audit_by_name is not None:
        audit_by_name["zip_input_work"] = measured_by_name
    return measured_by_name


async def _stage_pending_memberships(session, inputs, code_identity, limits, measured_by_name):
    work_by_name = measured_by_name["work"]
    batch_by_name = dict.fromkeys(("first", "priced", "factor", "membership"), 0)
    pending_inputs = (
        inputs
        if await session.scalar(text("SELECT EXISTS(SELECT 1 FROM plan_pricing_provider_npi_pending_stage)"))
        else ()
    )
    for binding, found in pending_inputs:
        remaining_by_name = {name: limits[name] - work_by_name[name] for name in ("first", "priced", "factor")}
        remaining_by_name["member_cell"] = limits["member_cell"] - measured_by_name["membership_count"]
        count, factor_work_by_name = await stage_memberships(
            session, binding, found, binding.code_rows_by_identity[code_identity], remaining_by_name
        )
        measured_by_name["membership_count"] += count
        batch_by_name["membership"] += count
        for name, count in factor_work_by_name.items():
            work_by_name[name] += count
            batch_by_name[name] += count
        _record_batch_maxima(measured_by_name["batch_maxima"], batch_by_name)
    await session.execute(
        text("""INSERT INTO plan_pricing_provider_npi_materialized_stage
        SELECT npi FROM plan_pricing_provider_npi_pending_stage""")
    )


async def _stage_input_batch(session, candidate_id, cell, inputs, code_identity, state, limits, measured_by_name):
    from api.plan_pricing_code_build import _audit_phase

    audit_by_name = getattr(state, "audit", None)
    timings_by_phase = getattr(state, "phase_timings", None)
    work_by_name = measured_by_name["work"]
    with _audit_phase(audit_by_name, timings_by_phase, "zip.membership"):
        await _stage_pending_memberships(session, inputs, code_identity, limits, measured_by_name)
    with _audit_phase(audit_by_name, timings_by_phase, "zip.addresses"):
        await _stage_matching_addresses(session, cell, limits["address"] - work_by_name["classified_address"])
    with _audit_phase(audit_by_name, timings_by_phase, "zip.assurance"):
        classified_count, assured_count, cell_count = await _stage_provider_cells(
            session,
            candidate_id,
            limits["address"] - work_by_name["classified_address"],
            limits["member_cell"] - measured_by_name["member_cell_count"],
        )
    work_by_name["classified_address"] += classified_count
    work_by_name["address"] += assured_count
    measured_by_name["member_cell_count"] += cell_count
    _record_batch_maxima(
        measured_by_name["batch_maxima"],
        {
            "classified_address": classified_count,
            "address": assured_count,
            "member_cell": cell_count,
        },
    )
    if calibration := getattr(state, "calibration", None):
        await calibration.verify_batch(session, cell)
    measured_by_name["batches_completed"] += 1


async def stage_inputs(session, candidate_id, scope, inputs, code_identity, state, limits):
    """Keep partial pre-filter work separate from post-filter stored cardinality."""
    from api.plan_pricing_code_build import _audit_phase

    audit_by_name = getattr(state, "audit", None)
    timings_by_phase = getattr(state, "phase_timings", None)
    measured_by_name = _zip_input_audit(scope, audit_by_name)
    work_by_name = measured_by_name["work"]
    with _audit_phase(audit_by_name, timings_by_phase, "zip.setup"):
        await _create_zip_stages(session)
    for cell in scope["geo_cells"]:
        position = 0
        while True:
            measured_by_name["enumeration_pages"] += 1
            with _audit_phase(audit_by_name, timings_by_phase, "zip.enumerate"):
                raw_count, position = await _stage_address_batch(
                    session, cell, position, limits["raw_address"] - work_by_name["raw_address"]
                )
            if not raw_count:
                measured_by_name["exhausted_cells"] += 1
                break
            measured_by_name["batches_started"] += 1
            work_by_name["raw_address"] += raw_count
            _record_batch_maxima(measured_by_name["batch_maxima"], {"raw_address": raw_count})
            await _stage_input_batch(
                session, candidate_id, cell, inputs, code_identity, state, limits, measured_by_name
            )
    measured_by_name["exhausted"] = True
    with _audit_phase(audit_by_name, timings_by_phase, "zip.finalize"):
        await session.execute(
            text("""DELETE FROM plan_pricing_provider_member_stage member
            WHERE NOT EXISTS(SELECT 1 FROM plan_pricing_provider_cell_stage cell WHERE cell.npi=member.npi)""")
        )
        for binding, _found in inputs:
            await _store_provider_sets(session, binding)
        state.provider_membership_count = int(
            await session.scalar(text("SELECT count(*) FROM plan_pricing_provider_member_stage"))
        )
        state.provider_cell_count = int(
            await session.scalar(text("SELECT count(*) FROM plan_pricing_provider_cell_stage"))
        )
    measured_by_name["complete"] = True
    return {"factor_join_work_rows": work_by_name["factor"], "zip_work": work_by_name}


async def _stage_local_factors(session, binding, found, parameters_by_name, limits):
    """Intersect the complete scalar population with indexed code factors."""
    from api import ptg2_serving as serving

    schema = native._quoted(found[0].schema_name)
    await session.execute(text("TRUNCATE plan_pricing_zip_first,plan_pricing_zip_priced"))
    await session.execute(
        text(f"""INSERT INTO plan_pricing_zip_first
        SELECT wanted.npi,first.member_key
          FROM plan_pricing_provider_npi_pending_stage wanted
          JOIN {serving._ptg2_npi_scope_table(binding.serving_tables)} dictionary
            ON dictionary.snapshot_key=:snapshot AND dictionary.npi=wanted.npi
          JOIN {schema}.ptg_membership_first first ON first.owner_key=dictionary.npi_key
         LIMIT :first_probe"""),
        parameters_by_name,
    )
    first = await _bounded_count(session, "plan_pricing_zip_first", limits["first"])
    await session.execute(
        text(f"""INSERT INTO plan_pricing_zip_priced
        SELECT second.owner_key,second.member_key
          FROM (SELECT DISTINCT factor FROM plan_pricing_zip_first) wanted
          JOIN {schema}.ptg_membership_second second ON second.owner_key=wanted.factor
         WHERE EXISTS(SELECT 1 FROM {schema}.ptg_membership_code code
                       WHERE code.provider_set_key=second.member_key
                         AND code.code_key=ANY(CAST(:code_keys AS bigint[])))
         LIMIT :priced_probe"""),
        parameters_by_name,
    )
    priced = await _bounded_count(session, "plan_pricing_zip_priced", limits["priced"])
    factor = int(
        await session.scalar(
            text("""WITH first AS (
        SELECT factor,count(*)::numeric AS members FROM plan_pricing_zip_first GROUP BY factor
        ), priced AS (
        SELECT factor,count(*)::numeric AS sets FROM plan_pricing_zip_priced GROUP BY factor)
        SELECT coalesce(sum(members*sets),0) FROM first JOIN priced USING(factor)""")
        )
    )
    if factor > limits["factor"]:
        raise ValueError("code pricing ZIP pre-deduplication factor join work bound exceeded")
    return {"first": first, "priced": priced, "factor": factor}


async def stage_memberships(session, binding, found, code_rows, limits):
    """Admit complete memberships before storing their scoped set cardinality."""
    from api import ptg2_serving as serving

    parameters_by_name = {
        "ordinal": binding.binding["ordinal"],
        "snapshot": serving._required_shared_snapshot_key(binding.serving_tables),
        "code_keys": [int(code_row["code_key"]) for code_row in code_rows],
        "first_probe": limits["first"] + 1,
        "priced_probe": limits["priced"] + 1,
        "member_probe": limits["member_cell"] + 1,
    }
    measured = await _stage_local_factors(session, binding, found, parameters_by_name, limits)
    count = int(
        await session.scalar(
            text("""WITH inserted AS (
        INSERT INTO plan_pricing_provider_member_stage
        SELECT :ordinal,priced.provider_set_key,first.npi
          FROM plan_pricing_zip_first first JOIN plan_pricing_zip_priced priced USING(factor)
         GROUP BY priced.provider_set_key,first.npi LIMIT :member_probe RETURNING 1)
        SELECT count(*) FROM inserted"""),
            parameters_by_name,
        )
    )
    if count > limits["member_cell"]:
        raise ValueError("code pricing ZIP membership work bound exceeded")
    return count, measured


async def _store_provider_sets(session, binding):
    from api import ptg2_serving as serving

    parameters_by_name = {
        "ordinal": binding.binding["ordinal"],
        "snapshot": serving._required_shared_snapshot_key(binding.serving_tables),
    }
    source_sets = serving._shared_v3_provider_set_table(binding.serving_tables)
    if await session.scalar(
        text(f"""SELECT EXISTS(
        SELECT 1 FROM plan_pricing_provider_member_stage member
        LEFT JOIN {source_sets} source ON source.snapshot_key=:snapshot
             AND source.provider_set_key=member.provider_set_key
        WHERE member.binding_ordinal=:ordinal
          AND (source.provider_set_key IS NULL OR source.provider_count <= 0))"""),
        parameters_by_name,
    ):
        raise PTG2ManifestArtifactError("code pricing ZIP membership source coverage differs")
    await session.execute(
        text(f"""INSERT INTO plan_pricing_provider_set_stage
        SELECT :ordinal,member.provider_set_key,encode(source.provider_set_global_id_128,'hex'),count(*)
          FROM plan_pricing_provider_member_stage member JOIN {source_sets} source
            ON source.snapshot_key=:snapshot AND source.provider_set_key=member.provider_set_key
         WHERE member.binding_ordinal=:ordinal
         GROUP BY member.provider_set_key,source.provider_set_global_id_128"""),
        parameters_by_name,
    )
