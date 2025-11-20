# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import os
from sqlalchemy import DateTime, Numeric, DATE, Column,\
    String, Integer, Float, BigInteger, Boolean, ARRAY, JSON, TIMESTAMP, TEXT, SMALLINT, PrimaryKeyConstraint, text
from sqlalchemy.orm import declared_attr

from db.connection import Base, db
from db.json_mixin import JSONOutputMixin

NAME_SEARCH_VECTOR = (
    "LOWER("
    "COALESCE(provider_first_name,'') || ' ' || "
    "COALESCE(provider_last_name,'') || ' ' || "
    "COALESCE(provider_organization_name,'') || ' ' || "
    "COALESCE(provider_other_organization_name,'') || ' ' || "
    "COALESCE(do_business_as_text,'')"
    ")"
)

NAME_SEARCH_VECTOR_WITH_OP = f"{NAME_SEARCH_VECTOR} gin_trgm_ops"


class ImportHistory(Base, JSONOutputMixin):
    __tablename__ = 'history'
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint('import_id'),
        {'schema': os.getenv('HLTHPRT_DB_SCHEMA') or 'mrf', 'extend_existing': True},
    )
    __my_index_elements__ = ['import_id']
    import_id = Column(String)
    json_status = Column(JSON)
    when = Column(DateTime)

class ImportLog(Base, JSONOutputMixin):
    __tablename__ = 'log'
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint('issuer_id', 'checksum'),
        {'schema': os.getenv('HLTHPRT_DB_SCHEMA') or 'mrf', 'extend_existing': True},
    )
    __my_index_elements__ = ['issuer_id', 'checksum']
    issuer_id = Column(Integer)
    checksum = Column(Integer)
    type = Column(String(4))
    text = Column(String)
    url = Column(String)
    source = Column(String) #plans, index, providers, etc.
    level = Column(String)  #network, json, etc.

class Issuer(Base, JSONOutputMixin):
    __tablename__ = 'issuer'
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint('issuer_id'),
        {'schema': os.getenv('HLTHPRT_DB_SCHEMA') or 'mrf', 'extend_existing': True},
    )
    __my_index_elements__ = ['issuer_id']
    state = Column(String(2))
    issuer_id = Column(Integer)
    issuer_name = Column(String)
    issuer_marketing_name = Column(String)
    mrf_url = Column(String)
    data_contact_email = Column(String)

class PlanFormulary(Base, JSONOutputMixin):
    __tablename__ = 'plan_formulary'
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint('plan_id', 'year', 'drug_tier', 'pharmacy_type'),
        {'schema': os.getenv('HLTHPRT_DB_SCHEMA') or 'mrf', 'extend_existing': True},
    )
    __my_index_elements__ = ['plan_id', 'year', 'drug_tier', 'pharmacy_type']
    plan_id = Column(String(14), nullable=False)
    year = Column(Integer)
    drug_tier = Column(String)
    mail_order = Column(Boolean)
    pharmacy_type = Column(String)
    copay_amount = Column(Float)
    copay_opt = Column(String)
    coinsurance_rate = Column(Float)
    coinsurance_opt = Column(String)


class PlanIndividual(Base, JSONOutputMixin):
    __tablename__ = 'plan_individual'
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint('plan_id', 'year', 'drug_tier', 'pharmacy_type'),
        {'schema': os.getenv('HLTHPRT_DB_SCHEMA') or 'mrf', 'extend_existing': True},
    )
    __my_index_elements__ = ['plan_id', 'year', 'drug_tier', 'pharmacy_type']
    __my_additional_indexes__ = [{'index_elements': ('int_code',)}, {'index_elements': ('display_name',)}]
    plan_id = Column(String(14), nullable=False)
    year = Column(Integer)
    drug_tier = Column(String)
    mail_order = Column(Boolean)
    pharmacy_type = Column(String)
    copay_amount = Column(Float)
    copay_opt = Column(String)
    coinsurance_rate = Column(Float)
    coinsurance_opt = Column(String)


class PlanFacility(Base, JSONOutputMixin):
    __tablename__ = 'plan_facility'
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint('plan_id', 'year', 'drug_tier', 'pharmacy_type'),
        {'schema': os.getenv('HLTHPRT_DB_SCHEMA') or 'mrf', 'extend_existing': True},
    )
    __my_index_elements__ = ['plan_id', 'year', 'drug_tier', 'pharmacy_type']
    __my_additional_indexes__ = [{'index_elements': ('int_code',)}, {'index_elements': ('display_name',)}]
    plan_id = Column(String(14), nullable=False)
    year = Column(Integer)
    drug_tier = Column(String)
    mail_order = Column(Boolean)
    pharmacy_type = Column(String)
    copay_amount = Column(Float)
    copay_opt = Column(String)
    coinsurance_rate = Column(Float)
    coinsurance_opt = Column(String)

class Plan(Base, JSONOutputMixin):
    __tablename__ = 'plan'
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint('plan_id', 'year'),
        {'schema': os.getenv('HLTHPRT_DB_SCHEMA') or 'mrf', 'extend_existing': True},
    )
    __my_index_elements__ = ['plan_id', 'year']
    plan_id = Column(String(14), nullable=False)  # len == 14
    year = Column(Integer)
    issuer_id = Column(Integer)
    state = Column(String(2))
    plan_id_type = Column(String)
    marketing_name = Column(String)
    summary_url = Column(String)
    marketing_url = Column(String)
    formulary_url = Column(String)
    plan_contact = Column(String)
    network = Column(ARRAY(String))
    benefits = Column(ARRAY(JSON))
    last_updated_on = Column(TIMESTAMP)
    checksum = Column(Integer)

class PlanAttributes(Base, JSONOutputMixin):
    __tablename__ = 'plan_attributes'
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint('full_plan_id', 'year', 'attr_name'),
        {'schema': os.getenv('HLTHPRT_DB_SCHEMA') or 'mrf', 'extend_existing': True},
    )
    __my_index_elements__ = ['full_plan_id', 'year', 'attr_name']
    __my_additional_indexes__ = [
        {'index_elements': ('full_plan_id gin_trgm_ops', 'year'),
            'using': 'gin',
            'name': 'find_all_variants'}]
    plan_id = Column(String(14))
    full_plan_id = Column(String(17), nullable=False)
    year = Column(Integer)
    attr_name = Column(String)
    attr_value = Column(String)

class PlanBenefits(Base, JSONOutputMixin):
    __tablename__ = 'plan_benefits'
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint('full_plan_id', 'year', 'benefit_name'),
        {'schema': os.getenv('HLTHPRT_DB_SCHEMA') or 'mrf', 'extend_existing': True},
    )
    __my_index_elements__ = ['full_plan_id', 'year', 'benefit_name']
    plan_id = Column(String(14), nullable=False)
    full_plan_id = Column(String(17), nullable=False)
    year = Column(Integer)
    benefit_name = Column(String)
    copay_inn_tier1 = Column(String)
    copay_inn_tier2 = Column(String)
    copay_outof_net = Column(String)
    coins_inn_tier1 = Column(String)
    coins_inn_tier2 = Column(String)
    coins_outof_net = Column(String)
    is_ehb = Column(Boolean)
    is_covered = Column(Boolean)
    quant_limit_on_svc = Column(Boolean)
    limit_qty = Column(Float)
    limit_unit = Column(String)
    exclusions = Column(String)
    explanation = Column(String)
    ehb_var_reason = Column(String)
    is_excl_from_inn_mo = Column(Boolean)
    is_excl_from_oon_mo = Column(Boolean)


class PlanRatingAreas(Base, JSONOutputMixin):
    __tablename__ = 'plan_rating_areas'
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint('county', 'zip3', 'state', 'market'),
        {'schema': os.getenv('HLTHPRT_DB_SCHEMA') or 'mrf', 'extend_existing': True},
    )
    __my_index_elements__ = ['county', 'zip3', 'state', 'market']

    state = Column(String(2))
    rating_area_id = Column(String)
    county = Column(String)
    zip3 = Column(String)
    market = Column(String)


class PlanPrices(Base, JSONOutputMixin):
    __tablename__ = 'plan_prices'
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint('plan_id', 'year', 'checksum'),
        {'schema': os.getenv('HLTHPRT_DB_SCHEMA') or 'mrf', 'extend_existing': True},
    )
    __my_index_elements__ = ['plan_id', 'year', 'checksum', ]
    __my_additional_indexes__ = [
        {'index_elements': ('state', 'year', 'min_age', 'max_age', 'rating_area_id', 'couple'),
            'using': 'gin',
            'name': 'find_plan'}]

    plan_id = Column(String(14), nullable=False)
    year = Column(Integer)
    state = Column(String(2))
    checksum = Column(Integer)
    rate_effective_date = Column(DATE)
    rate_expiration_date = Column(DATE)
    rating_area_id = Column(String)
    tobacco = Column(String)
    min_age = Column(SMALLINT)
    max_age = Column(SMALLINT)
    individual_rate = Column(Numeric(scale=2, precision=8, asdecimal=False, decimal_return_scale=None))
    individual_tobacco_rate = Column(Numeric(scale=2, precision=8, asdecimal=False, decimal_return_scale=None))
    couple = Column(Numeric(scale=2, precision=8, asdecimal=False, decimal_return_scale=None))
    primary_subscriber_and_one_dependent = Column(
        Numeric(scale=2, precision=8, asdecimal=False, decimal_return_scale=None))
    primary_subscriber_and_two_dependents = Column(
        Numeric(scale=2, precision=8, asdecimal=False, decimal_return_scale=None))
    primary_subscriber_and_three_or_more_dependents = Column(
        Numeric(scale=2, precision=8, asdecimal=False, decimal_return_scale=None))
    couple_and_one_dependent = Column(Numeric(scale=2, precision=8, asdecimal=False, decimal_return_scale=None))
    couple_and_two_dependents = Column(Numeric(scale=2, precision=8, asdecimal=False, decimal_return_scale=None))
    couple_and_three_or_more_dependents = Column(
        Numeric(scale=2, precision=8, asdecimal=False, decimal_return_scale=None))


class PlanTransparency(Base, JSONOutputMixin):
    __tablename__ = 'plan_transparency'
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint('plan_id', 'year'),
        {'schema': os.getenv('HLTHPRT_DB_SCHEMA') or 'mrf', 'extend_existing': True},
    )
    __my_index_elements__ = ['plan_id', 'year']
    state = Column(String(2))
    issuer_name = Column(String)
    issuer_id = Column(Integer)
    new_issuer_to_exchange = Column(Boolean)
    sadp_only = Column(Boolean)
    plan_id = Column(String(14), nullable=False)  # len == 14
    year = Column(Integer)
    qhp_sadp = Column(String)
    plan_type = Column(String)
    metal = Column(String)
    claims_payment_policies_url = Column(String)


class PlanNPIRaw(Base, JSONOutputMixin):
    __tablename__ = 'plan_npi_raw'
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint('npi', 'checksum_network'),
        {'schema': os.getenv('HLTHPRT_DB_SCHEMA') or 'mrf', 'extend_existing': True},
    )
    __my_index_elements__ = ['npi', 'checksum_network']
    __my_additional_indexes__ = [
        {'index_elements': ('issuer_id', 'network_tier', 'year'), 'using': 'gin'},]


    npi = Column(Integer)
    checksum_network = Column(Integer)
    type =  Column(String)
    last_updated_on = Column(TIMESTAMP)
    network_tier = Column(String)
    issuer_id = Column(Integer)
    year = Column(Integer)
    name_or_facility_name = Column(String)
    prefix = Column(String)
    first_name = Column(String)
    middle_name = Column(String)
    last_name = Column(String)
    suffix = Column(String)
    addresses = Column(ARRAY(JSON))
    specialty_or_facility_type = Column(ARRAY(String))
    accepting = Column(String)
    gender = Column(String)
    languages = Column(ARRAY(String))



class PlanNetworkTierRaw(Base, JSONOutputMixin):
    __tablename__ = 'plan_networktier'
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint('plan_id', 'checksum_network'),
        {'schema': os.getenv('HLTHPRT_DB_SCHEMA') or 'mrf', 'extend_existing': True},
    )
    __my_index_elements__ = ['plan_id', 'checksum_network']
    __my_additional_indexes__ = [
        {'index_elements': ('issuer_id', 'network_tier', 'year'), 'using': 'gin'},
        {'index_elements': ('checksum_network'), 'using': 'gin'}, ]


    plan_id = Column(String(14))
    network_tier = Column(String)
    issuer_id = Column(Integer)
    year = Column(Integer)
    checksum_network = Column(Integer)


class PlanDrugRaw(Base, JSONOutputMixin):
    __tablename__ = 'plan_drug_raw'
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint('plan_id', 'rxnorm_id'),
        {'schema': os.getenv('HLTHPRT_DB_SCHEMA') or 'mrf', 'extend_existing': True},
    )
    __my_index_elements__ = ['plan_id', 'rxnorm_id']
    __my_additional_indexes__ = [
        {
            'index_elements': ('plan_id', 'drug_tier'),
            'using': 'gin',
            'name': 'plan_drug_tier_lookup',
        },
        {
            'index_elements': ('rxnorm_id',),
            'name': 'plan_drug_rxnorm_lookup',
        },
    ]

    plan_id = Column(String(14), nullable=False)
    plan_id_type = Column(String)
    rxnorm_id = Column(String, nullable=False)
    drug_name = Column(String)
    drug_tier = Column(String)
    prior_authorization = Column(Boolean)
    step_therapy = Column(Boolean)
    quantity_limit = Column(Boolean)
    last_updated_on = Column(TIMESTAMP)


class NPIData(Base, JSONOutputMixin):
    __tablename__ = 'npi'
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint('npi'),
        {'schema': os.getenv('HLTHPRT_DB_SCHEMA') or 'mrf', 'extend_existing': True},
    )
    __my_index_elements__ = ['npi']
    __my_additional_indexes__ = [
        {'index_elements': (
            'npi',
            NAME_SEARCH_VECTOR_WITH_OP,
            'entity_type_code'),
            'using': 'gin',
            'name': 'partial_search_helper'}]
    npi = Column(Integer, primary_key=True)
    employer_identification_number = Column(String)
    entity_type_code = Column(Integer)
    replacement_npi = Column(Integer)
    provider_organization_name = Column(String)
    provider_last_name = Column(String)
    provider_first_name = Column(String)
    provider_middle_name = Column(String)
    provider_name_prefix_text = Column(String)
    provider_name_suffix_text = Column(String)
    provider_sex_code = Column(String)
    provider_credential_text = Column(String)
    provider_other_organization_name = Column(String)
    provider_other_organization_name_type_code = Column(String)
    provider_other_last_name = Column(String)
    provider_other_first_name = Column(String)
    provider_other_middle_name = Column(String)
    provider_other_name_prefix_text = Column(String)
    provider_other_name_suffix_text = Column(String)
    provider_other_credential_text = Column(String)
    provider_other_last_name_type_code = Column(String)
    provider_enumeration_date = Column(DATE)
    last_update_date = Column(DATE)
    npi_deactivation_reason_code = Column(String)
    npi_deactivation_date = Column(DATE)
    npi_reactivation_date = Column(DATE)
    authorized_official_last_name = Column(String)
    authorized_official_first_name = Column(String)
    authorized_official_middle_name = Column(String)
    authorized_official_title_or_position = Column(String)
    authorized_official_telephone_number = Column(String)
    is_sole_proprietor = Column(String)
    is_organization_subpart = Column(String)
    parent_organization_lbn = Column(String)
    parent_organization_tin = Column(String)
    authorized_official_name_prefix_text = Column(String)
    authorized_official_name_suffix_text = Column(String)
    authorized_official_credential_text = Column(String)
    certification_date = Column(DATE)
    do_business_as = Column(
        ARRAY(String),
        nullable=False,
        server_default=text("ARRAY[]::varchar[]"),
        default=list,
    )
    do_business_as_text = Column(String)


class NPIDataTaxonomy(Base, JSONOutputMixin):
    __tablename__ = 'npi_taxonomy'
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint('npi', 'checksum'),
        {'schema': os.getenv('HLTHPRT_DB_SCHEMA') or 'mrf', 'extend_existing': True},
    )
    __my_index_elements__ = ['npi', 'checksum']
    __my_additional_indexes__ = [{'index_elements': ('healthcare_provider_taxonomy_code', 'npi',)}, ]

    npi = Column(Integer)
    checksum = Column(Integer)
    healthcare_provider_taxonomy_code = Column(String)
    provider_license_number = Column(String)
    provider_license_number_state_code = Column(String)
    healthcare_provider_primary_taxonomy_switch = Column(String)

class NPIDataOtherIdentifier(Base, JSONOutputMixin):
    __tablename__ = 'npi_other_identifier'
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint('npi', 'checksum'),
        {'schema': os.getenv('HLTHPRT_DB_SCHEMA') or 'mrf', 'extend_existing': True},
    )
    __my_index_elements__ = ['npi', 'checksum']

    npi = Column(Integer, primary_key=True)
    checksum = Column(Integer, primary_key=True)
    other_provider_identifier = Column(String)
    other_provider_identifier_type_code = Column(String)
    other_provider_identifier_state = Column(String)
    other_provider_identifier_issuer = Column(String)

class NPIDataTaxonomyGroup(Base, JSONOutputMixin):
    __tablename__ = 'npi_taxonomy_group'
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint('npi', 'checksum'),
        {'schema': os.getenv('HLTHPRT_DB_SCHEMA') or 'mrf', 'extend_existing': True},
    )
    __my_index_elements__ = ['npi', 'checksum']

    npi = Column(Integer)
    checksum = Column(Integer)
    healthcare_provider_taxonomy_group = Column(String)


class NUCCTaxonomy(Base, JSONOutputMixin):
    __tablename__ = 'nucc_taxonomy'
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint('code'),
        {'schema': os.getenv('HLTHPRT_DB_SCHEMA') or 'mrf', 'extend_existing': True},
    )
    __my_index_elements__ = ['code']
    __my_additional_indexes__ = [{'index_elements': ('int_code',)}, {'index_elements': ('display_name',)},
        {'index_elements': ('classification','section'), 'using': 'gin'} ]

    int_code = Column(Integer)
    code = Column(String)
    grouping = Column(String)
    classification = Column(String)
    specialization = Column(String)
    definition = Column(TEXT)
    notes = Column(TEXT)
    display_name = Column(String)
    section = Column(String)



class AddressPrototype(Base, JSONOutputMixin):
    __abstract__ = True

    @declared_attr
    def __table_args__(cls):
        return {'schema': os.getenv('HLTHPRT_DB_SCHEMA') or 'mrf', 'extend_existing': True}

    checksum = Column(Integer, primary_key=True)
    first_line = Column(String)
    second_line  = Column(String)
    city_name = Column(String)
    state_name = Column(String)
    postal_code = Column(String)
    country_code = Column(String)
    telephone_number = Column(String)
    fax_number = Column(String)
    formatted_address = Column(String)
    lat = Column(Numeric(scale=8, precision=11, asdecimal=False, decimal_return_scale=None))
    long = Column(Numeric(scale=8, precision=11, asdecimal=False, decimal_return_scale=None))
    date_added = Column(DATE)
    place_id = Column(String)


class AddressArchive(AddressPrototype):
    __tablename__ = 'address_archive'
    __main_table__ = __tablename__
    __my_index_elements__ = ['checksum']
    # __my_additional_indexes__ = [{'index_elements': ('healthcare_provider_taxonomy_code', 'npi',)}, ]


class NPIAddress(AddressPrototype):
    __tablename__ = 'npi_address'
    __main_table__ = __tablename__
    __my_index_elements__ = ['npi', 'type', 'checksum']
    #__my_initial_indexes__ = [{'index_elements': ('npi', 'type'), 'unique': True, 'where': "type='primary'"}] #  or type='secondary'
    __my_initial_indexes__ = [{'index_elements': ('checksum',), 'using': 'gin'}]

    __my_additional_indexes__ = [
        {'index_elements': ('postal_code',)},
        {'index_elements': ('city_name', 'state_name', 'country_code'), 'using': 'gin'},
        {'index_elements': (
            'taxonomy_array gin__int_ops',
            'plans_network_array gin__int_ops'),
            'using': 'gin',
            'name': 'taxonomy_plans_network'},
        {'index_elements': (
            'telephone_number',
            'taxonomy_array gin__int_ops',
            'type',
            'npi'
            ),
            'using': 'gin',
            'name': 'phone_taxonomy_type_npi'},
        {'index_elements': (
            'taxonomy_array gin__int_ops',
            'telephone_number',
            'plans_network_array gin__int_ops',
        ),
            'using': 'gin',
            'name': 'plans_network_taxonomy_phone'},
        {'index_elements': (
        'Geography(ST_MakePoint(long, lat))', 'taxonomy_array gist__intbig_ops', 'plans_network_array gist__intbig_ops'),
            'using': 'gist',
            'name': 'geo_index_with_taxonomy_and_plans','where': "type='primary' or type='secondary'"}]

    npi = Column(Integer, primary_key=True)
    type = Column(String, primary_key=True)
    taxonomy_array = Column(ARRAY(Integer), nullable=False, server_default="{0}")
    plans_network_array = Column(ARRAY(Integer), nullable=False, server_default="{0}")

    # NPI	Provider Secondary Practice Location Address- Address Line 1	Provider Secondary Practice Location Address-  Address Line 2	Provider Secondary Practice Location Address - City Name	Provider Secondary Practice Location Address - State Name	Provider Secondary Practice Location Address - Postal Code	Provider Secondary Practice Location Address - Country Code (If outside U.S.)	Provider Secondary Practice Location Address - Telephone Number	Provider Secondary Practice Location Address - Telephone Extension	Provider Practice Location Address - Fax Number
