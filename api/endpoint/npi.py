import os
import asyncio
import json
import random
from datetime import datetime
from process.ext.utils import download_it
import urllib.parse
from sqlalchemy import or_, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.sql import func, tuple_, text, literal_column, distinct

import sanic.exceptions
from sanic import response
from sanic import Blueprint

from api.utils import square_poly

from db.models import db, Issuer, Plan, PlanNPIRaw, NPIData, NPIAddress, AddressArchive, NPIDataTaxonomy, \
    NPIDataTaxonomyGroup, NUCCTaxonomy

blueprint = Blueprint('npi', url_prefix='/npi', version=1)


@blueprint.get('/')
async def npi_index_status(request):
    async def get_npi_count():
        """
    The get_npi_count function returns the number of NPI records in the database.

    :return: The number of records in the npidata table
    :doc-author: Trelent
    """
    async with db.acquire():
            return await db.func.count(NPIData.npi).gino.scalar()

    async def get_npi_address_count():
        async with db.acquire():
            return await db.func.count(tuple_(NPIAddress.npi, NPIAddress.checksum, NPIAddress.type)).gino.scalar()


    npi_count, npi_address_count = await asyncio.gather(get_npi_count(), get_npi_address_count())
    data = {
        'date': datetime.utcnow().isoformat(),
        'release': request.app.config.get('RELEASE'),
        'environment': request.app.config.get('ENVIRONMENT'),
        'product_count': npi_count,
        'import_log_errors': npi_address_count,
    }

    return response.json(data)

@blueprint.get('/active_pharmacists')
async def active_pharmacists(request):
    state = request.args.get("state", None)
    specialization = request.args.get("specialization", None)
    if state and len(state) == 2:
        state = state.upper()
    else:
        state = None

    sql = text(
        """
        WITH pharmacy_taxonomy AS (
            SELECT ARRAY_AGG(int_code) AS codes
            FROM mrf.nucc_taxonomy
            WHERE classification = 'Pharmacy'
        ),
        pharmacist_taxonomy AS (
            SELECT ARRAY_AGG(int_code) AS codes
            FROM mrf.nucc_taxonomy
            WHERE 
               """
        + (
            "specialization= :specialization" if specialization
            else "classification = 'Pharmacist'"
        )
        + """
        )
        SELECT COUNT(DISTINCT phm.npi) AS active_pharmacist_count
        FROM mrf.npi_address ph
        JOIN mrf.npi_address phm ON ph.telephone_number = phm.telephone_number AND phm.type = 'primary' AND ph.type = 'primary'
               AND ph.state_name = phm.state_name
        WHERE ph.taxonomy_array && (SELECT codes FROM pharmacy_taxonomy)
        AND phm.taxonomy_array && (SELECT codes FROM pharmacist_taxonomy)
        """
        + ("AND ph.state_name = :state" if state else "")
    )

    async with db.acquire() as conn:
        result = await conn.first(sql, state=state, specialization=specialization)
    return response.json({"count": result[0] if result else 0})

@blueprint.get('/pharmacists_per_pharmacy')
async def pharmacists_per_pharmacy(request):
    state = request.args.get("state", None)
    if state and len(state) == 2:
        state = state.upper()
    else:
        state = None

    sql = text("""
        WITH pharmacy_taxonomy AS (
            SELECT ARRAY_AGG(int_code) AS codes
            FROM mrf.nucc_taxonomy
            WHERE classification = 'Pharmacy'
        ),
        pharmacist_taxonomy AS (
            SELECT ARRAY_AGG(int_code) AS codes
            FROM mrf.nucc_taxonomy
            WHERE classification = 'Pharmacist'
        ),
        pharmacy_counts AS (
            SELECT ph.npi AS pharmacy_npi, COUNT(DISTINCT phm.npi) AS pharmacist_count
            FROM mrf.npi_address ph
            JOIN mrf.npi_address phm ON ph.telephone_number = phm.telephone_number AND phm.type = 'primary' AND ph.type = 'primary' 
               AND ph.state_name = phm.state_name
            WHERE ph.taxonomy_array && (SELECT codes FROM pharmacy_taxonomy)
            AND phm.taxonomy_array && (SELECT codes FROM pharmacist_taxonomy)
            """ + ("AND ph.state_name = :state" if state else "") + """
            GROUP BY ph.npi
        )
        SELECT 
        CASE 
            WHEN pharmacist_count = 1 THEN '1'
            WHEN pharmacist_count = 2 THEN '2'
            WHEN pharmacist_count = 3 THEN '3'
            WHEN pharmacist_count = 4 THEN '4'
            WHEN pharmacist_count = 5 THEN '5'
            WHEN pharmacist_count = 6 THEN '6'
            WHEN pharmacist_count = 7 THEN '7'
            WHEN pharmacist_count = 8 THEN '8'
            WHEN pharmacist_count = 9 THEN '9'
            WHEN pharmacist_count = 10 THEN '10'
            WHEN pharmacist_count = 11 THEN '11'
            WHEN pharmacist_count = 12 THEN '12'
            WHEN pharmacist_count = 13 THEN '13'
            WHEN pharmacist_count = 14 THEN '14'
            WHEN pharmacist_count = 15 THEN '15'
            WHEN pharmacist_count = 16 THEN '16'
            WHEN pharmacist_count = 17 THEN '17'
            WHEN pharmacist_count = 18 THEN '18'
            WHEN pharmacist_count = 19 THEN '19'
            WHEN pharmacist_count = 20 THEN '20'
            WHEN pharmacist_count = 21 THEN '21'
            WHEN pharmacist_count = 22 THEN '22'
            WHEN pharmacist_count = 23 THEN '23'
            WHEN pharmacist_count = 24 THEN '24'
            WHEN pharmacist_count = 25 THEN '25'
            ELSE '25+'
        END AS pharmacist_group,
        COUNT(*) AS pharmacy_count
        FROM pharmacy_counts
        GROUP BY pharmacist_group
        ORDER BY pharmacist_group DESC
    """)

    async with db.acquire() as conn:
        result = await conn.all(sql, state=state)
    data = [{"pharmacist_group": row[0], "pharmacy_count": row[1]} for row in result]
    return response.json(data)


@blueprint.get("/pharmacists_in_pharmacies")
async def pharmacists_in_pharmacies(request):
    name_like = request.args.get("name_like", "").lower()
    if not name_like:
        return response.json({"count": 0})

    sql = text("""
        WITH pharmacy_taxonomy AS (
            SELECT ARRAY_AGG(int_code) AS codes
            FROM mrf.nucc_taxonomy
            WHERE classification = 'Pharmacy'
        ),
        pharmacist_taxonomy AS (
            SELECT ARRAY_AGG(int_code) AS codes
            FROM mrf.nucc_taxonomy
            WHERE classification = 'Pharmacist'
        )
        SELECT COUNT(DISTINCT phm.npi) AS pharmacist_count
        FROM mrf.npi_address ph
        JOIN mrf.npi_address phm ON ph.telephone_number = phm.telephone_number AND phm.type = 'primary' AND ph.type = 'primary' AND ph.state_name = phm.state_name
        JOIN mrf.npi d ON ph.npi = d.npi
        WHERE ph.taxonomy_array && (SELECT codes FROM pharmacy_taxonomy)
        AND phm.taxonomy_array && (SELECT codes FROM pharmacist_taxonomy)
        AND LOWER(COALESCE(d.provider_first_name,'') || ' ' || COALESCE(d.provider_last_name,'') || ' ' || COALESCE(d.provider_organization_name,'') || ' ' || COALESCE(d.provider_other_organization_name,'')) LIKE :name_like
    """)

    async with db.acquire() as conn:
        result = await conn.first(sql, name_like=f"%{name_like}%")
    return response.json({"count": result[0] if result else 0})

@blueprint.get('/all')
async def get_all(request):
    count_only = float(request.args.get("count_only", 0))
    response_format = request.args.get("format", None)
    name_like = request.args.get('name_like')
    start = float(request.args.get("start", 0))
    limit = float(request.args.get("limit", 0))
    classification = request.args.get("classification")
    specialization = request.args.get("specialization")
    section = request.args.get("section")
    display_name = request.args.get("display_name")
    plan_network = request.args.get("plan_network")
    has_insurance = request.args.get("has_insurance")
    city = request.args.get("city")
    state = request.args.get("state")


    codes = request.args.get("codes")
    if codes:
        codes = [x.strip() for x in codes.split(',')]

    if plan_network:
        plan_network = [int(x) for x in plan_network.split(',')]

    async def get_count(classification, section, display_name, plan_network=None, name_like=None, codes=None, has_insurance=None,
                        city=None, state=None, response_format=None, specialization=None):
        where = []
        if classification:
            where.append('classification = :classification')
        if specialization:
            where.append('specialization = :specialization')
        if section:
            where.append('section = :section')
        if display_name:
            where.append('display_name = :display_name')
        if codes:
            where.append('code = ANY(:codes)')

        tables = ['mrf.npi_address']
        #main_where = ['healthcare_provider_taxonomy_code = ANY(codes)']
        main_where = []

        if plan_network:
            main_where.append('plans_network_array && :plan_network_array')


        if name_like:
            tables.append('mrf.npi as d')
            name_like = f'%{name_like.lower()}%'
            main_where.append('mrf.npi_address.npi = d.npi')
            main_where.append("LOWER(COALESCE(provider_first_name,'') || ' ' || COALESCE(provider_last_name,'') || ' ' || COALESCE(provider_organization_name,'') || ' ' || COALESCE(provider_other_organization_name,'')) LIKE :name_like")

        if has_insurance:
            main_where.append('not(plans_network_array @@ \'0\'::query_int)')

        if city:
            city = city.upper()
            main_where.append('city_name = :city')


        if state:
            state = state.upper()
            main_where.append('state_name = :state')

        tables = list(set(tables))
        q = f"""
                    from {','.join(tables)}"""

        if response_format:
            if not where:
                where = ['1=1']
            if response_format == 'full_taxonomy':
                main_where.append('taxonomy_array && ARRAY[int_code]')
                q += f""",
                (select code, int_code from mrf.nucc_taxonomy where {' and '.join(where)}) as q
                """
            else:
                main_where.append('taxonomy_array && q.int_codes')
                q += f""",
                (select ARRAY_AGG(code) as codes, ARRAY_AGG(int_code) as int_codes, classification from mrf.nucc_taxonomy GROUP BY classification) as q
                """
        else:
            main_where.append('taxonomy_array && q.int_codes')
            if not where:
                where.append('1=1')
            q += f""",
            (select ARRAY_AGG(code) as codes, ARRAY_AGG(int_code) as int_codes from mrf.nucc_taxonomy where {' and '.join(where)}) as q
            """

        if main_where:
            q += f"""
                where  {' and '.join(main_where)}"""



        if response_format:
            if response_format == 'full_taxonomy':
                q = f"select int_code, count(distinct (mrf.npi_address.npi)) as count " + q
                q += f""" group by int_code"""
            else:
                q = f"select q.classification, count(distinct (mrf.npi_address.npi)) as count " + q
                q += f""" group by classification"""

            q = text(q)
            async with db.acquire() as conn:
                return dict(await conn.all(q, classification=classification, section=section, display_name=display_name,
                                       plan_network_array=plan_network, name_like=name_like, codes=codes, city=city,
                                       state=state, specialization=specialization))
        else:
            q = f"select count(distinct (mrf.npi_address.npi)) " + q
            q = text(q)
            async with db.acquire() as conn:
                return (await conn.all(q, classification=classification, section=section, display_name=display_name,
                                       plan_network_array=plan_network, name_like=name_like, codes=codes, city=city, state=state, specialization=specialization))[0][0]

    async def get_results(start, limit, classification, section, display_name, plan_network, name_like=None, specialization=None):
        where = []
        main_where = ["b.npi=g.npi"]
        tables = ['mrf.npi_address']

        address_where = ["c.taxonomy_array && q.codes", "c.type = 'primary'"]
        if classification:
            where.append('classification = :classification')
        if specialization:
            where.append('specialization = :specialization')
        if section:
            where.append('section = :section')
        if display_name:
            where.append('display_name = :display_name')
        if plan_network:
            address_where.append("plans_network_array && :plan_network_array")
        if name_like:
            tables.append('mrf.npi as d')
            name_like = f'%{name_like.lower()}%'
            main_where.append('mrf.npi_address.npi = d.npi')
            main_where.append("LOWER(COALESCE(provider_first_name,'') || ' ' || COALESCE(provider_last_name,'') || ' ' || COALESCE(provider_organization_name,'') || ' ' || COALESCE(provider_other_organization_name,'')) LIKE :name_like")

        q = text(f"""
        WITH sub_s AS(select b.npi as npi_code, b.*, g.* from  mrf.npi as b, (select c.*
    from 
         mrf.npi_address as c,
         (select ARRAY_AGG(int_code) as codes from mrf.nucc_taxonomy where {' and '.join(where)}) as q
    where {' and '.join(address_where)}
    ORDER BY c.npi
    limit :limit offset :start) as g WHERE {' and '.join(main_where)}
    )
    
    select sub_s.*, t.* from sub_s, mrf.npi_taxonomy as t 
            where sub_s.npi_code = t.npi;
    """)

        res = {}
        async with db.acquire() as conn:
            for r in await conn.all(q, start=start, limit=limit, classification=classification, section=section,
                                    display_name=display_name, plan_network_array=plan_network, name_like=name_like, specialization=specialization):
                obj = {'taxonomy_list': []}
                count = 0
                for c in NPIData.__table__.columns:
                    count += 1
                    obj[c.key] = r[count]

                for c in NPIAddress.__table__.columns:
                    count += 1
                    obj[c.key] = r[count]

                if obj['npi'] in res:
                    obj = res[obj['npi']]
                taxonomy = {}
                for c in NPIDataTaxonomy.__table__.columns:
                    count += 1
                    if c.key in ('npi', 'checksum'):
                        continue
                    taxonomy[c.key] = r[count]
                obj['taxonomy_list'].append(taxonomy)
                res[obj['npi']] = obj
        res = [x for x in res.values()]
        return res

    if count_only:
        rows = await get_count(classification, section, display_name, plan_network, name_like, codes, has_insurance,
                               city, state, response_format, specialization=specialization)
        return response.json({'rows': rows}, default=str)

    total, rows = await asyncio.gather(
        get_count(classification, section, display_name, plan_network, specialization=specialization),
        get_results(start, limit, classification, section, display_name, plan_network, specialization=specialization)
    )
    return response.json({'total': total, 'rows': rows}, default=str)


@blueprint.get('/near/')
async def get_near_npi(request):
    in_long, in_lat = None, None
    if request.args.get("long"):
        in_long = float(request.args.get("long"))
    if request.args.get("lat"):
        in_lat = float(request.args.get("lat"))

    t1 = datetime.now()
    codes = request.args.get("codes")
    if codes:
        codes = [x.strip() for x in codes.split(',')]

    plan_network = request.args.get("plan_network")
    if plan_network:
        plan_network = [int(x) for x in plan_network.split(',')]
    classification = request.args.get("classification")
    section = request.args.get('section')
    display_name = request.args.get('display_name')
    name_like = request.args.get('name_like')
    exclude_npi = int(request.args.get("exclude_npi", 0))
    limit = int(request.args.get('limit', 5))
    zip_codes = []
    for zip_c in request.args.get('zip_codes', '').split(','):
        if not zip_c:
            continue
        zip_codes.append(zip_c.strip().rjust(5, '0'))
    radius = int(request.args.get('radius', 10))

    async with db.acquire() as conn:
        if (not (in_long and in_lat)) and zip_codes and zip_codes[0]:
            q = "select intptlat, intptlon from zcta5 where zcta5ce=:zip_code limit 1;"
            for r in await conn.all(text(q), zip_code=zip_codes[0]):
                in_long = float(r['intptlon'])
                in_lat = float(r['intptlat'])

        res = {}
        extended_where = ""
        ilike_name = ""
        if exclude_npi:
            extended_where += " and a.npi <> :exclude_npi"
        if plan_network:
            extended_where += " and a.plans_network_array && (:plan_network_array)"

        where = []
        if zip_codes:
            ## fix the issue with blank in_long!!
            ## add center of zip code radius by default
            radius = 1000
            extended_where += " and SUBSTRING(a.postal_code, 1, 5) = ANY (:zip_codes)"
        if classification:
            where.append('classification = :classification')
        if section:
            where.append('section = :section')
        if display_name:
            where.append('display_name = :display_name')
        if codes:
            where.append('code = ANY(:codes)')
        if name_like:
            name_like = f'%{name_like}%'
            ilike_name += " and (LOWER(COALESCE(d.provider_first_name,'') || ' ' || COALESCE(d.provider_last_name,'') || ' ' || COALESCE(d.provider_organization_name,'') || ' ' || COALESCE(d.provider_other_organization_name,'')) LIKE :name_like)"

        # bnd = square_poly(in_lat, in_long, radius)
        # x_y = list(bnd.bounds)
        # extended_where += " and lat between (:y_min) and (:y_max) and long between (:x_min) and (:x_max) "

#         q = f"""
#         WITH sub_s AS(
#         select d.npi as npi_code, round(cast(st_distance(Geography(ST_MakePoint(q.long, q.lat)),
#                               Geography(ST_MakePoint(:in_long, :in_lat))) / 1609.34 as numeric), 2) as distance,
#        q.*, d.*
# from mrf.npi as d,
# (select a.* from mrf.npi_address as a,
#      (select ARRAY_AGG(int_code) as codes from mrf.nucc_taxonomy where {' and '.join(where)}) as g
# where ST_DWithin(Geography(ST_MakePoint(long, lat)),
#                  Geography(ST_MakePoint(:in_long, :in_lat)),
#                  :radius * 1609.34)
#   and a.taxonomy_array && g.codes
#   and a.type = 'primary'
#   {extended_where}
# ORDER by round(cast(st_distance(Geography(ST_MakePoint(a.long, a.lat)),
#                               Geography(ST_MakePoint(:in_long, :in_lat))) / 1609.34 as numeric), 2) asc LIMIT :limit) as q WHERE q.npi=d.npi{ilike_name}
# )
#
# select sub_s.*, t.* from sub_s, mrf.npi_taxonomy as t
#             where sub_s.npi_code = t.npi;
# """

        q = f"""
                WITH sub_s AS(
                select d.npi as npi_code,
               q.*, d.*
        from mrf.npi as d,
        (select round(cast(st_distance(Geography(ST_MakePoint(a.long, a.lat)),
                                      Geography(ST_MakePoint(:in_long, :in_lat))) / 1609.34 as numeric), 
                                      2) as distance, a.* from mrf.npi_address as a,
             (select ARRAY_AGG(int_code) as codes from mrf.nucc_taxonomy where {' and '.join(where)}) as g
        where ST_DWithin(Geography(ST_MakePoint(long, lat)),
                         Geography(ST_MakePoint(:in_long, :in_lat)),
                         :radius * 1609.34)
          and a.taxonomy_array && g.codes
          and (a.type = 'primary' or a.type = 'secondary')
          {extended_where}
        ORDER by distance asc) as q WHERE q.npi=d.npi{ilike_name} LIMIT :limit
        )

        select sub_s.*, t.* from sub_s, mrf.npi_taxonomy as t 
                    where sub_s.npi_code = t.npi;                              
        """



        res_q = await conn.all(text(q), in_long=in_long, in_lat=in_lat, classification=classification, limit=limit,
                       radius=radius,
                       exclude_npi=exclude_npi, section=section, display_name=display_name, name_like=name_like,
                       codes=codes, zip_codes=zip_codes, plan_network_array=plan_network,
                               # y_min=x_y[1], y_max=x_y[3], x_min=x_y[0], x_max=x_y[2]
                    )
        t2 = datetime.now()
        for r in res_q:
            obj = {'taxonomy_list': []}
            count = 1
            obj['distance'] = r[count]
            temp = NPIAddress.__table__.columns

            for c in temp:
                count += 1
                obj[c.key] = r[count]
            for c in NPIData.__table__.columns:
                count += 1
                if c.key in ('npi', 'checksum'):
                    continue
                obj[c.key] = r[count]

            if obj['npi'] in res:
                obj = res[obj['npi']]
            taxonomy = {}
            for c in NPIDataTaxonomy.__table__.columns:
                count += 1
                if c.key in ('npi', 'checksum'):
                    continue
                taxonomy[c.key] = r[count]
            obj['taxonomy_list'].append(taxonomy)

            res[obj['npi']] = obj

        res = [x for x in res.values()]
        t3 = datetime.now()

        # print(f"TIME_First: {t2 - t1}")
        # print(f"TIME_Last: {t3-t2}")
        # print(f"TIME_Full: {t3-t1}")
    return response.json(res, default=str)

@blueprint.get('/id/<npi>/full_taxonomy')
async def get_full_taxonomy_list(request, npi):
    t = []
    npi = int(npi)
    # plan_data = await db.select(
    #     [Plan.marketing_name, Plan.plan_id, PlanAttributes.full_plan_id, Plan.year]).select_from(
    #     Plan.join(PlanAttributes, ((Plan.plan_id == func.substr(PlanAttributes.full_plan_id, 1, 14)) & (
    #                 Plan.year == PlanAttributes.year)))). \
    #     group_by(PlanAttributes.full_plan_id, Plan.plan_id, Plan.marketing_name, Plan.year).gino.all()
    data = []
    async with db.acquire() as conn:
        for x in await db.select([NPIDataTaxonomy.__table__.columns,NUCCTaxonomy.__table__.columns]).where(NPIDataTaxonomy.npi == npi).where(NUCCTaxonomy.code == NPIDataTaxonomy.healthcare_provider_taxonomy_code).gino.all():
            t.append(x.to_json_dict())
    return response.json(t)


@blueprint.get('/plans_by_npi/<npi>')
async def get_plans_by_npi(request, npi):

    data = []
    plan_data = []
    issuer_data = []
    npi = int(npi)

    # async def get_plans_list(plan_arr):
    #     t = {}
    #     q = Plan.query.where(Plan.plan_id == db.func.any(plan_arr)).where(Plan.year == int(2023)).gino
    #     async with db.acquire() as conn:
    #         for x in await q.all():
    #             t[x.plan_id] = x.to_json_dict()
    #     return t

    q = db.select([PlanNPIRaw, Issuer]).where(Issuer.issuer_id == PlanNPIRaw.issuer_id).where(
        PlanNPIRaw.npi == npi).order_by(PlanNPIRaw.issuer_id.desc()).gino.load((PlanNPIRaw, Issuer))

    async with db.acquire() as conn:
        async with conn.transaction():
            async for x in q.iterate():
                data.append({'npi_info': x[0].to_json_dict(), 'issuer_info': x[1].to_json_dict()})


    return response.json({'npi_data': data, 'plan_data': plan_data, 'issuer_data': issuer_data})


@blueprint.get('/id/<npi>')
async def get_npi(request, npi):
    force_address_update = request.args.get('force_address_update', 0)
    async def update_addr_coordinates(checksum, long, lat, formatted_address, place_id):
        async with db.acquire() as conn:
            async with conn.transaction() as tx:
                await NPIAddress.update.values(long=long,
                                               lat=lat,
                                               formatted_address=formatted_address,
                                               place_id=place_id)\
                    .where(NPIAddress.checksum == checksum).gino.status()
                temp = AddressArchive.__table__.columns
                x = await NPIAddress.query.where(NPIAddress.checksum == checksum).gino.first()
                obj = {}
                t = x.to_dict()
                for c in temp:
                    obj[c.key] = t[c.key]

        # long = long,
        # lat = lat,
        # formatted_address = formatted_address,
        # place_id = place_id
        #         del obj['checksum']
                # await AddressArchive.update.values(obj) \
                #     .where(AddressArchive.checksum == checksum).gino.status()
                try:
                    await insert(AddressArchive).values([obj]).on_conflict_do_update(
                        index_elements=AddressArchive.__my_index_elements__,
                        set_=obj
                    ).gino.model(AddressArchive).status()
                except Exception as e:
                    print(f"exception: {e}")


    async def _update_address(x):
        if x.get('lat'):
            return x
        postal_code = x.get('postal_code')
        if postal_code and len(postal_code)>5:
            postal_code = f"{postal_code[0:5]}-{postal_code[5:]}"
        t_addr = ', '.join([x.get('first_line',''), x.get('second_line',''), x.get('city_name',''), f"{x.get('state_name','')} {postal_code}"])
        t_addr = t_addr.replace(' , ', ' ')

        d = x
        if force_address_update:
            d['long'] = None
            d['lat'] = None
            d['formatted_address'] = None
            d['place_id'] = None
        
        if (not d['lat']):

            # try:
            #     raw_sql = text(f"""SELECT
            #            g.rating,
            #            ST_X(g.geomout) As lon,
            #            ST_Y(g.geomout) As lat,
            #             pprint_addy(g.addy) as formatted_address
            #             from mrf.npi,
            #             standardize_address('us_lex',
            #                  'us_gaz', 'us_rules', :addr) as addr,
            #             geocode((
            #                 (addr).house_num,  --address
            #                 null,              --predirabbrev
            #                 (addr).name,       --streetname
            #                 (addr).suftype,    --streettypeabbrev
            #                 null,              --postdirabbrev
            #                 (addr).unit,       --internal
            #                 (addr).city,       --location
            #                 (addr).state,      --stateabbrev
            #                 (addr).postcode,   --zip
            #                 true,               --parsed
            #                 null,               -- zip4
            #                 (addr).house_num    -- address_alphanumeric
            #             )::norm_addy) as g
            #            where npi = :npi""")
            #     addr = await conn.status(raw_sql, addr=t_addr, npi=npi)
            #
            #     if addr and len(addr[-1]) and addr[-1][0] and addr[-1][0][0] < 2:
            #         d['long'] = addr[-1][0][1]
            #         d['lat'] = addr[-1][0][2]
            #         d['formatted_address'] = addr[-1][0][3]
            #         d['place_id'] = None
            # except:
            #     pass
            update_geo = False
            if request.app.config.get('NPI_API_UPDATE_GEOCODE') and not d['lat']:
                update_geo = True

            if (not d['lat']) and (not force_address_update):
                try:
                    res = await AddressArchive.query.where(AddressArchive.checksum == x.checksum).gino.first()
                    if res:
                        d['long'] = res.long
                        d['lat'] = res.lat
                        d['formatted_address'] = res.formatted_address
                        d['place_id'] = res.place_id
                except:
                    pass

            if not d['lat']:
                try:
                    params = {
                        request.app.config.get('GEOCODE_MAPBOX_STYLE_KEY_PARAM'):
                            random.choice(json.loads(request.app.config.get('GEOCODE_MAPBOX_STYLE_KEY')))
                    }
                    encoded_params = '.json?'.join(
                        (urllib.parse.quote_plus(t_addr), urllib.parse.urlencode(params, doseq=True),))
                    if qp:=request.app.config.get('GEOCODE_MAPBOX_STYLE_ADDITIONAL_QUERY_PARAMS'):
                        encoded_params = '&'.join((encoded_params,qp,))
                    url = request.app.config.get('GEOCODE_MAPBOX_STYLE_URL')+encoded_params
                    resp = await download_it(url, local_timeout=5)
                    geo_data = json.loads(resp)
                    if geo_data.get('features', []):
                        d['long'] = geo_data['features'][0]['geometry']['coordinates'][0]
                        d['lat'] = geo_data['features'][0]['geometry']['coordinates'][1]
                        if t2 := geo_data['features'][0].get('matching_place_name'):
                            d['formatted_address'] = t2
                        else:
                            d['formatted_address'] = geo_data['features'][0]['place_name']
                        d['place_id'] = None
                except:
                    pass

            if not d['lat']:
                try:
                    params = {request.app.config.get('GEOCODE_GOOGLE_STYLE_ADDRESS_PARAM'): t_addr,
                        request.app.config.get('GEOCODE_GOOGLE_STYLE_KEY_PARAM'): request.app.config.get(
                            'GEOCODE_GOOGLE_STYLE_KEY')}
                    encoded_params = urllib.parse.urlencode(params, doseq=True)
                    if qp:=request.app.config.get('GEOCODE_GOOGLE_STYLE_ADDITIONAL_QUERY_PARAMS'):
                        encoded_params = '&'.join((encoded_params,qp,))
                    url = '?'.join((request.app.config.get('GEOCODE_GOOGLE_STYLE_URL'), encoded_params,))
                    resp = await download_it(url)
                    geo_data = json.loads(resp)
                    if geo_data.get('results', []):
                        d['long'] = geo_data['results'][0]['geometry']['location']['lng']
                        d['lat'] = geo_data['results'][0]['geometry']['location']['lat']
                        d['formatted_address'] = geo_data['results'][0]['formatted_address']
                        d['place_id'] = geo_data['results'][0]['place_id']
                except Exception as e:
                    print(f"Error in geocoding Google: {t_addr}, {e}")
                    pass

            if update_geo and d.get('lat'):
                request.app.add_task(update_addr_coordinates(x['checksum'], d['long'], d['lat'], d['formatted_address'], d['place_id']))

        return d

    # async def get_npi_data(npi):
    #     async with db.acquire():
    #         npi_agg = db.func.array_agg(NPIData).label('npi_data')
    #         npi_taxonomy_agg = db.func.array_agg(NPIDataTaxonomy).label('npi_taxonomy')
    #         r = await db.select(
    #             [
    #                 npi_agg,
    #                 npi_taxonomy_agg
    #             ]).select_from(
    #             select(npi_agg)NPIData,
    #         join(NPIDataTaxonomy, NPIData.npi == NPIDataTaxonomy.npi)).where(NPIData.npi == npi).group_by(
    #             NPIData.npi).gino.first()
    #
    #         print(r)
    #        # t = conn.db.session.query(NPIData, NPIDataTaxonomy).join(NPIDataTaxonomy, NPIData.npi == NPIDataTaxonomy.npi)
    #
    #         # t = NPIData.query
    #         #t  t.join(NPIDataTaxonomy, NPIData.npi == NPIDataTaxonomy.npi)
    #         #t = await t.where(NPIData.npi == npi).gino.first()
    #     return ''
    #     #t.to_json_dict()

    async def get_npi_data(npi):
        async with db.acquire():
            t = await NPIData.query.where(NPIData.npi == npi).gino.first()
        return t.to_json_dict()
    
    async def get_taxonomy_list(npi):
        t = []
        async with db.acquire():
            for x in await NPIDataTaxonomy.query.where(NPIDataTaxonomy.npi == npi).gino.all():
                t.append(x.to_json_dict())
        return t

    async def get_taxonomy_group_list(npi):
        t = []
        async with db.acquire():
            for x in await NPIDataTaxonomyGroup.query.where(NPIDataTaxonomyGroup.npi == npi).gino.all():
                t.append(x.to_json_dict())
        return t


    async def get_address_list(npi):
        t = []
        g = NPIAddress.query.where(
            (NPIAddress.npi == npi) & or_(NPIAddress.type == 'primary', NPIAddress.type == 'secondary')).order_by(
            NPIAddress.type)
        async with db.acquire():
            for x in await g.gino.all():
                t.append(x.to_json_dict())
        return t

    async def test_combined(npi):
        async with db.acquire() as conn:

            g = db.select([NPIAddress]).where(
                (NPIAddress.npi == npi) & or_(NPIAddress.type == 'primary', NPIAddress.type == 'secondary')).order_by(
                NPIAddress.type).alias('address_list')

            query = db.select(
                [NPIData, func.json_agg(literal_column('distinct "'+ NPIDataTaxonomy.__tablename__+'"')), func.json_agg(literal_column('distinct "'+ NPIDataTaxonomyGroup.__tablename__+'"')),
                    func.json_agg(literal_column('distinct "'+ 'address_list' +'"'))
                ]).select_from(
                NPIData.outerjoin(NPIDataTaxonomy, NPIData.npi == NPIDataTaxonomy.npi).outerjoin(NPIDataTaxonomyGroup, NPIData.npi == NPIDataTaxonomyGroup.npi).outerjoin(g, NPIData.npi == g.c.npi)
            ).where(NPIData.npi == npi).group_by(NPIData.npi)

            r = await query.gino.all()
            if not r:
                return {}
            r = r[0]
            obj = {'taxonomy_list': [], 'taxonomy_group_list': [], 'address_list': []}
            count = 0
            for c in NPIData.__table__.columns:
                obj[c.key] = r[count]
                count += 1

            if r[count]:
                obj['taxonomy_list'].extend([q for q in r[count] if q])
            count += 1
            if r[count]:
                obj['taxonomy_group_list'].extend([q for q in r[count] if q])

            count += 1
            if r[count]:
                obj['address_list'] = r[count]

            return obj

    npi = int(npi)

    data = await test_combined(npi)

    if data['address_list']:
        new_address_list = []
        update_address_tasks = []
        for a in data['address_list']:
            # if not a['lat']:
            if a:
                update_address_tasks.append(_update_address(a))
            # else:
            #     new_address_list.append(a)
        if update_address_tasks:
            data['address_list'] = list(await asyncio.gather(*update_address_tasks))

    if not data:
        raise sanic.exceptions.NotFound

    # data.update({
    #     'address_list': address_list,
    # })

    return response.json(data, default=str)
