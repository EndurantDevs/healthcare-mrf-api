# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import os

from sqlalchemy import (ARRAY, DATE, JSON, SMALLINT, TEXT, TIMESTAMP,
                        BigInteger, Boolean, Column, DateTime, Float, Integer,
                        Numeric, PrimaryKeyConstraint, String, text)
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
    __my_additional_indexes__ = [
        {'index_elements': ('state',), 'name': 'plan_lookup_by_state'},
        {'index_elements': ('issuer_id',), 'name': 'plan_lookup_by_issuer'},
        {'index_elements': ('year',), 'name': 'plan_lookup_by_year'},
        {'index_elements': ('issuer_id', 'year'), 'name': 'plan_lookup_by_issuer_year'},
    ]
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
        {
            'index_elements': ('full_plan_id gin_trgm_ops', 'year'),
            'using': 'gin',
            'name': 'find_all_variants',
        },
        {
            'index_elements': ('plan_id', 'year', 'attr_name'),
            'name': 'plan_attributes_plan_year_attr_idx',
        },
        {
            'index_elements': ('plan_id', 'year'),
            'name': 'plan_attributes_plan_year_idx',
        },
    ]
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
    __my_additional_indexes__ = [
        {
            'index_elements': ('plan_id', 'year', 'benefit_name'),
            'name': 'plan_benefits_plan_year_benefit_idx',
        },
        {
            'index_elements': ('plan_id', 'year'),
            'name': 'plan_benefits_plan_year_idx',
        },
    ]
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


class GeoZipLookup(Base, JSONOutputMixin):
    __tablename__ = 'geo_zip_lookup'
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint('zip_code'),
        {'schema': os.getenv('HLTHPRT_DB_SCHEMA') or 'mrf', 'extend_existing': True},
    )
    __my_index_elements__ = ['zip_code']
    __my_additional_indexes__ = [
        {'index_elements': ('state',), 'name': 'geo_zip_lookup_state_idx'},
        {'index_elements': ('city_lower', 'state'), 'name': 'geo_zip_lookup_city_state_idx'},
        {'index_elements': ('city_lower',), 'name': 'geo_zip_lookup_city_idx'},
    ]

    zip_code = Column(String(5), nullable=False)
    city = Column(String(128))
    city_lower = Column(String(128))
    state = Column(String(2))
    state_name = Column(String(128))
    county_name = Column(String(128))
    county_code = Column(String(8))
    latitude = Column(Float)
    longitude = Column(Float)
    timezone = Column(String(64))
    population = Column(Integer)


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


class PlanDrugStats(Base, JSONOutputMixin):
    __tablename__ = 'plan_drug_stats'
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint('plan_id'),
        {'schema': os.getenv('HLTHPRT_DB_SCHEMA') or 'mrf', 'extend_existing': True},
    )
    __my_index_elements__ = ['plan_id']
    plan_id = Column(String(14), nullable=False)
    total_drugs = Column(Integer, nullable=False, default=0)
    auth_required = Column(Integer, nullable=False, default=0)
    auth_not_required = Column(Integer, nullable=False, default=0)
    step_required = Column(Integer, nullable=False, default=0)
    step_not_required = Column(Integer, nullable=False, default=0)
    quantity_limit = Column(Integer, nullable=False, default=0)
    quantity_no_limit = Column(Integer, nullable=False, default=0)
    last_updated_on = Column(TIMESTAMP)


class PlanDrugTierStats(Base, JSONOutputMixin):
    __tablename__ = 'plan_drug_tier_stats'
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint('plan_id', 'drug_tier'),
        {'schema': os.getenv('HLTHPRT_DB_SCHEMA') or 'mrf', 'extend_existing': True},
    )
    __my_index_elements__ = ['plan_id', 'drug_tier']
    plan_id = Column(String(14), nullable=False)
    drug_tier = Column(String, nullable=False)
    drug_count = Column(Integer, nullable=False, default=0)


class PlanSearchSummary(Base, JSONOutputMixin):
    __tablename__ = 'plan_search_summary'
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint('plan_id', 'year'),
        {'schema': os.getenv('HLTHPRT_DB_SCHEMA') or 'mrf', 'extend_existing': True},
    )
    __my_index_elements__ = ['plan_id', 'year']
    __my_additional_indexes__ = [
        {'index_elements': ('state', 'year'), 'name': 'plan_search_summary_state_year_idx'},
        {'index_elements': ('issuer_id', 'year'), 'name': 'plan_search_summary_issuer_year_idx'},
    ]

    plan_id = Column(String(14), nullable=False)
    year = Column(Integer, nullable=False)
    state = Column(String(2))
    issuer_id = Column(Integer)
    marketing_name = Column(String)
    market_coverage = Column(String)
    is_on_exchange = Column(Boolean)
    is_off_exchange = Column(Boolean)
    is_hsa = Column(Boolean)
    is_dental_only = Column(Boolean)
    is_catastrophic = Column(Boolean)
    has_adult_dental = Column(Boolean)
    has_child_dental = Column(Boolean)
    has_adult_vision = Column(Boolean)
    has_child_vision = Column(Boolean)
    telehealth_supported = Column(Boolean)
    deductible_inn_individual = Column(Float)
    moop_inn_individual = Column(Float)
    premium_min = Column(Float)
    premium_max = Column(Float)
    plan_type = Column(String)
    metal_level = Column(String)
    csr_variation = Column(String)
    attributes = Column(JSON)
    plan_benefits = Column(JSON)
    updated_at = Column(TIMESTAMP)


class NPIData(Base, JSONOutputMixin):
    __tablename__ = 'npi'
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint('npi'),
        {'schema': os.getenv('HLTHPRT_DB_SCHEMA') or 'mrf', 'extend_existing': True},
    )
    __my_index_elements__ = ['npi']
    __my_additional_indexes__ = [
        {
            'index_elements': (NAME_SEARCH_VECTOR_WITH_OP,),
            'using': 'gin',
            'name': 'name_search_trgm',
        },
        {
            'index_elements': (
                "LOWER(COALESCE(provider_organization_name,'') || ' ' || "
                "COALESCE(provider_other_organization_name,'') || ' ' || "
                "COALESCE(do_business_as_text,'')) gin_trgm_ops",
            ),
            'using': 'gin',
            'name': 'organization_search_trgm',
        },
        {
            'index_elements': ("LOWER(COALESCE(provider_first_name,'')) gin_trgm_ops",),
            'using': 'gin',
            'name': 'first_name_trgm',
        },
        {
            'index_elements': ("LOWER(COALESCE(provider_last_name,'')) gin_trgm_ops",),
            'using': 'gin',
            'name': 'last_name_trgm',
        },
        {
            'index_elements': ('entity_type_code',),
            'name': 'entity_type_code',
        },
    ]
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
    __my_initial_indexes__ = [{'index_elements': ('checksum',)}]

    __my_additional_indexes__ = [
        {'index_elements': ('postal_code',)},
        {'index_elements': ("LEFT(postal_code, 5)",), 'name': 'postal_code_5'},
        {'index_elements': ('city_name', 'state_name', 'country_code'), 'using': 'gin'},
        {'index_elements': ('type', 'state_name', 'city_name'), 'name': 'type_state_city'},
        {
            'index_elements': ('city_name', 'state_name', 'npi'),
            'name': 'primary_city_state_npi',
            'where': "type='primary'",
        },
        {'index_elements': ('type', "LEFT(postal_code, 5)"), 'name': 'type_postal_code_5'},
        {
            'index_elements': ("regexp_replace(COALESCE(telephone_number, ''), '[^0-9]', '', 'g')",),
            'name': 'telephone_digits',
        },
        {'index_elements': (
            'taxonomy_array gin__int_ops',
            'plans_network_array gin__int_ops'),
            'using': 'gin',
            'name': 'taxonomy_plans_network'},
        {'index_elements': (
            'procedures_array gin__int_ops',
        ),
            'using': 'gin',
            'name': 'procedures_array'},
        {'index_elements': (
            'medications_array gin__int_ops',
        ),
            'using': 'gin',
            'name': 'medications_array'},
        {'index_elements': (
            'taxonomy_array gin__int_ops',
            'plans_network_array gin__int_ops',
            'procedures_array gin__int_ops',
        ),
            'using': 'gin',
            'name': 'taxonomy_plans_procedures'},
        {'index_elements': (
            'taxonomy_array gin__int_ops',
            'plans_network_array gin__int_ops',
            'medications_array gin__int_ops',
        ),
            'using': 'gin',
            'name': 'taxonomy_plans_medications'},
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
        'Geography(ST_MakePoint((long)::double precision, (lat)::double precision))', 'taxonomy_array gist__intbig_ops', 'plans_network_array gist__intbig_ops'),
            'using': 'gist',
            'name': 'geo_index_with_taxonomy_and_plans','where': "type='primary' or type='secondary'"},
        {'index_elements': (
        'Geography(ST_MakePoint((long)::double precision, (lat)::double precision))', 'taxonomy_array gist__intbig_ops', 'plans_network_array gist__intbig_ops', 'procedures_array gist__intbig_ops'),
            'using': 'gist',
            'name': 'geo_index_with_taxonomy_plans_and_procedures','where': "type='primary' or type='secondary'"},
        {'index_elements': (
        'Geography(ST_MakePoint((long)::double precision, (lat)::double precision))', 'taxonomy_array gist__intbig_ops', 'plans_network_array gist__intbig_ops', 'medications_array gist__intbig_ops'),
            'using': 'gist',
            'name': 'geo_index_with_taxonomy_plans_and_medications','where': "type='primary' or type='secondary'"},
        {'index_elements': (
        'Geography(ST_MakePoint((long)::double precision, (lat)::double precision))', 'procedures_array gist__intbig_ops', 'plans_network_array gist__intbig_ops', 'taxonomy_array gist__intbig_ops',),
            'using': 'gist',
            'name': 'geo_index_with_taxonomy_plans_and_medications','where': "type='primary' or type='secondary'"},
        {'index_elements': (
        'Geography(ST_MakePoint((long)::double precision, (lat)::double precision))', 'medications_array gist__intbig_ops', 'plans_network_array gist__intbig_ops', 'taxonomy_array gist__intbig_ops',),
            'using': 'gist',
            'name': 'geo_index_with_taxonomy_plans_and_medications','where': "type='primary' or type='secondary'"},
    ]

    npi = Column(Integer, primary_key=True)
    type = Column(String, primary_key=True)
    taxonomy_array = Column(ARRAY(Integer), nullable=False, server_default="{0}")
    plans_network_array = Column(ARRAY(Integer), nullable=False, server_default="{0}")
    procedures_array = Column(ARRAY(Integer), nullable=False, server_default="{0}")
    medications_array = Column(ARRAY(Integer), nullable=False, server_default="{0}")

    # NPI	Provider Secondary Practice Location Address- Address Line 1	Provider Secondary Practice Location Address-  Address Line 2	Provider Secondary Practice Location Address - City Name	Provider Secondary Practice Location Address - State Name	Provider Secondary Practice Location Address - Postal Code	Provider Secondary Practice Location Address - Country Code (If outside U.S.)	Provider Secondary Practice Location Address - Telephone Number	Provider Secondary Practice Location Address - Telephone Extension	Provider Practice Location Address - Fax Number


class PTGFile(Base, JSONOutputMixin):
    __tablename__ = 'ptg_file'
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint('file_id'),
        {'schema': os.getenv('HLTHPRT_DB_SCHEMA') or 'mrf', 'extend_existing': True},
    )
    __my_index_elements__ = ['file_id']
    __my_additional_indexes__ = [
        {'index_elements': ('file_type',), 'name': 'ptg_file_type_idx'},
        {'index_elements': ('url',), 'name': 'ptg_file_url_idx'},
    ]

    file_id = Column(BigInteger)
    file_type = Column(String(64))
    url = Column(String)
    description = Column(String)
    reporting_entity_name = Column(String)
    reporting_entity_type = Column(String)
    last_updated_on = Column(DATE)
    version = Column(String(32))
    plan_name = Column(String)
    plan_id_type = Column(String(16))
    plan_id = Column(String(32))
    plan_market_type = Column(String(32))
    issuer_name = Column(String)
    plan_sponsor_name = Column(String)
    from_index_url = Column(String)


class PTGProviderGroup(Base, JSONOutputMixin):
    __tablename__ = 'ptg_provider_group'
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint('provider_group_hash'),
        {'schema': os.getenv('HLTHPRT_DB_SCHEMA') or 'mrf', 'extend_existing': True},
    )
    __my_index_elements__ = ['provider_group_hash']
    __my_additional_indexes__ = [
        {'index_elements': ('provider_group_ref',), 'name': 'ptg_provider_group_ref_idx'},
        {'index_elements': ('tin_value',), 'name': 'ptg_provider_group_tin_idx'},
    ]

    provider_group_hash = Column(BigInteger)
    provider_group_ref = Column(Integer)
    file_id = Column(BigInteger)
    network_names = Column(ARRAY(String))
    tin_type = Column(String(8))
    tin_value = Column(String(32))
    tin_business_name = Column(String)
    npi = Column(ARRAY(BigInteger))


class PTGInNetworkItem(Base, JSONOutputMixin):
    __tablename__ = 'ptg_in_network_item'
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint('item_hash'),
        {'schema': os.getenv('HLTHPRT_DB_SCHEMA') or 'mrf', 'extend_existing': True},
    )
    __my_index_elements__ = ['item_hash']
    __my_additional_indexes__ = [
        {'index_elements': ('billing_code',), 'name': 'ptg_item_billing_code_idx'},
        {'index_elements': ('billing_code_type',), 'name': 'ptg_item_code_type_idx'},
    ]

    item_hash = Column(BigInteger)
    file_id = Column(BigInteger)
    negotiation_arrangement = Column(String(32))
    name = Column(String)
    billing_code_type = Column(String(32))
    billing_code_type_version = Column(String(32))
    billing_code = Column(String(64))
    description = Column(String)
    severity_of_illness = Column(String)
    plan_name = Column(String)
    plan_id_type = Column(String(16))
    plan_id = Column(String(32))
    plan_market_type = Column(String(32))
    issuer_name = Column(String)
    plan_sponsor_name = Column(String)


class PTGBillingCode(Base, JSONOutputMixin):
    __tablename__ = 'ptg_billing_code'
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint('code_hash'),
        {'schema': os.getenv('HLTHPRT_DB_SCHEMA') or 'mrf', 'extend_existing': True},
    )
    __my_index_elements__ = ['code_hash']
    __my_additional_indexes__ = [{'index_elements': ('item_hash',), 'name': 'ptg_billing_code_item_idx'}]

    code_hash = Column(BigInteger)
    item_hash = Column(BigInteger)
    code_role = Column(String(16))  # bundle | covered
    billing_code_type = Column(String(32))
    billing_code_type_version = Column(String(32))
    billing_code = Column(String(64))
    description = Column(String)


class PTGNegotiatedRate(Base, JSONOutputMixin):
    __tablename__ = 'ptg_negotiated_rate'
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint('rate_hash'),
        {'schema': os.getenv('HLTHPRT_DB_SCHEMA') or 'mrf', 'extend_existing': True},
    )
    __my_index_elements__ = ['rate_hash']
    __my_additional_indexes__ = [
        {'index_elements': ('item_hash',), 'name': 'ptg_negotiated_item_idx'},
        {'index_elements': ('provider_group_hash',), 'name': 'ptg_negotiated_group_idx'},
    ]

    rate_hash = Column(BigInteger)
    item_hash = Column(BigInteger)
    provider_group_ref = Column(Integer)
    provider_group_hash = Column(BigInteger)


class PTGNegotiatedPrice(Base, JSONOutputMixin):
    __tablename__ = 'ptg_negotiated_price'
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint('price_hash'),
        {'schema': os.getenv('HLTHPRT_DB_SCHEMA') or 'mrf', 'extend_existing': True},
    )
    __my_index_elements__ = ['price_hash']
    __my_additional_indexes__ = [
        {'index_elements': ('rate_hash',), 'name': 'ptg_price_rate_idx'},
        {'index_elements': ('billing_class',), 'name': 'ptg_price_class_idx'},
    ]

    price_hash = Column(BigInteger)
    rate_hash = Column(BigInteger)
    negotiated_type = Column(String(32))
    negotiated_rate = Column(Numeric)
    expiration_date = Column(DATE)
    service_code = Column(ARRAY(String))
    billing_class = Column(String(32))
    setting = Column(String(32))
    billing_code_modifier = Column(ARRAY(String))
    additional_information = Column(String)


class PTGAllowedItem(Base, JSONOutputMixin):
    __tablename__ = 'ptg_allowed_item'
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint('allowed_item_hash'),
        {'schema': os.getenv('HLTHPRT_DB_SCHEMA') or 'mrf', 'extend_existing': True},
    )
    __my_index_elements__ = ['allowed_item_hash']
    __my_additional_indexes__ = [{'index_elements': ('billing_code',), 'name': 'ptg_allowed_code_idx'}]

    allowed_item_hash = Column(BigInteger)
    file_id = Column(BigInteger)
    name = Column(String)
    billing_code_type = Column(String(32))
    billing_code_type_version = Column(String(32))
    billing_code = Column(String(64))
    description = Column(String)
    plan_name = Column(String)
    plan_id_type = Column(String(16))
    plan_id = Column(String(32))
    plan_market_type = Column(String(32))
    issuer_name = Column(String)
    plan_sponsor_name = Column(String)


class PTGAllowedPayment(Base, JSONOutputMixin):
    __tablename__ = 'ptg_allowed_payment'
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint('payment_hash'),
        {'schema': os.getenv('HLTHPRT_DB_SCHEMA') or 'mrf', 'extend_existing': True},
    )
    __my_index_elements__ = ['payment_hash']
    __my_additional_indexes__ = [
        {'index_elements': ('allowed_item_hash',), 'name': 'ptg_allowed_payment_item_idx'},
        {'index_elements': ('tin_value',), 'name': 'ptg_allowed_payment_tin_idx'},
    ]

    payment_hash = Column(BigInteger)
    allowed_item_hash = Column(BigInteger)
    tin_type = Column(String(8))
    tin_value = Column(String(32))
    service_code = Column(ARRAY(String))
    billing_class = Column(String(32))
    setting = Column(String(32))
    allowed_amount = Column(Numeric)
    billing_code_modifier = Column(ARRAY(String))


class PTGAllowedProviderPayment(Base, JSONOutputMixin):
    __tablename__ = 'ptg_allowed_provider_payment'
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint('provider_payment_hash'),
        {'schema': os.getenv('HLTHPRT_DB_SCHEMA') or 'mrf', 'extend_existing': True},
    )
    __my_index_elements__ = ['provider_payment_hash']
    __my_additional_indexes__ = [{'index_elements': ('payment_hash',), 'name': 'ptg_allowed_provider_payment_idx'}]

    provider_payment_hash = Column(BigInteger)
    payment_hash = Column(BigInteger)
    billed_charge = Column(Numeric)
    npi = Column(ARRAY(BigInteger))


class PricingProvider(Base, JSONOutputMixin):
    __tablename__ = "pricing_provider"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("provider_key"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["provider_key"]
    __my_additional_indexes__ = [
        {"index_elements": ("npi",), "name": "pricing_provider_npi_idx"},
        {"index_elements": ("year",), "name": "pricing_provider_year_idx"},
        {"index_elements": ("year", "npi"), "name": "pricing_provider_year_npi_idx"},
        {"index_elements": ("state", "city"), "name": "pricing_provider_state_city_idx"},
        {"index_elements": ("year", "state", "city"), "name": "pricing_provider_year_state_city_idx"},
        {"index_elements": ("year", "state", "city", "provider_type"), "name": "pricing_provider_year_state_city_type_idx"},
        {"index_elements": ("provider_type",), "name": "pricing_provider_type_idx"},
        {"index_elements": ("year", "lower(provider_type)"), "name": "pricing_provider_year_provider_type_lower_idx"},
        {"index_elements": ("year", "lower(provider_name)"), "name": "pricing_provider_year_provider_name_lower_idx"},
        {"index_elements": ("year", "total_allowed_amount DESC"), "name": "pricing_provider_year_total_allowed_amount_desc_idx"},
        {"index_elements": ("year", "total_services DESC"), "name": "pricing_provider_year_total_services_desc_idx"},
    ]

    provider_key = Column(BigInteger)
    npi = Column(BigInteger, nullable=False)
    year = Column(Integer, nullable=False)
    provider_name = Column(String)
    first_name = Column(String)
    last_org_name = Column(String)
    credentials = Column(String)
    provider_type = Column(String)
    city = Column(String)
    state = Column(String(2))
    zip5 = Column(String(5))
    country = Column(String)
    total_services = Column(Float)
    total_distinct_hcpcs_codes = Column(Float)
    total_allowed_amount = Column(Numeric(scale=2, precision=16, asdecimal=False, decimal_return_scale=None))
    total_submitted_charges = Column(Float)
    total_beneficiaries = Column(Float)


class PricingProcedure(Base, JSONOutputMixin):
    __tablename__ = "pricing_procedure"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("procedure_code"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["procedure_code"]
    __my_additional_indexes__ = [
        {"index_elements": ("service_description",), "name": "pricing_procedure_service_description_idx"},
        {"index_elements": ("reported_code",), "name": "pricing_procedure_reported_code_idx"},
        {"index_elements": ("lower(service_description)",), "name": "pricing_procedure_service_description_lower_idx"},
        {"index_elements": ("lower(reported_code)",), "name": "pricing_procedure_reported_code_lower_idx"},
        {"index_elements": ("source_year", "lower(service_description)"), "name": "pricing_procedure_year_service_description_lower_idx"},
    ]

    procedure_code = Column(BigInteger)
    service_description = Column(String)
    reported_code = Column(String)
    avg_submitted_charge = Column(Float)
    avg_allowed_amount = Column(Float)
    avg_payment_amount = Column(Float)
    avg_standardized_amount = Column(Float)
    total_allowed_amount = Column(Float)
    total_services = Column(Float)
    total_beneficiaries = Column(Float)
    source_year = Column(Integer)


class PricingProviderProcedure(Base, JSONOutputMixin):
    __tablename__ = "pricing_provider_procedure"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("npi", "year", "procedure_code"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["npi", "year", "procedure_code"]
    __my_additional_indexes__ = [
        {"index_elements": ("year", "procedure_code"), "name": "pricing_provider_proc_year_idx"},
        {"index_elements": ("year", "procedure_code", "npi"), "name": "pricing_provider_proc_year_npi_idx"},
        {"index_elements": ("reported_code",), "name": "pricing_provider_proc_reported_code_idx"},
        {"index_elements": ("service_description",), "name": "pricing_provider_proc_service_description_idx"},
        {"index_elements": ("year", "lower(reported_code)"), "name": "pricing_provider_proc_year_reported_code_lower_idx"},
        {"index_elements": ("year", "lower(service_description)"), "name": "pricing_provider_proc_year_service_description_lower_idx"},
        {"index_elements": ("year", "npi", "total_allowed_amount DESC"), "name": "pricing_provider_proc_year_npi_total_allowed_amount_desc_idx"},
    ]

    npi = Column(BigInteger, nullable=False)
    year = Column(Integer, nullable=False)
    procedure_code = Column(BigInteger, nullable=False)
    service_description = Column(String)
    reported_code = Column(String)
    total_services = Column(Float)
    total_beneficiary_day_services = Column(Float)
    total_submitted_charges = Column(Float)
    total_allowed_amount = Column(Numeric(scale=2, precision=16, asdecimal=False, decimal_return_scale=None))
    total_beneficiaries = Column(Float)
    ge65_total_services = Column(Float)
    ge65_total_allowed_amount = Column(Float)
    ge65_total_beneficiaries = Column(Float)


class PricingProviderProcedureLocation(Base, JSONOutputMixin):
    __tablename__ = "pricing_provider_procedure_location"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("location_key"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["location_key"]
    __my_additional_indexes__ = [
        {"index_elements": ("npi", "year", "procedure_code"), "name": "pricing_provider_loc_lookup_idx"},
        {"index_elements": ("state", "city"), "name": "pricing_provider_loc_state_city_idx"},
        {"index_elements": ("year", "state", "city"), "name": "pricing_provider_loc_year_state_city_idx"},
        {"index_elements": ("year", "zip5"), "name": "pricing_provider_loc_year_zip5_idx"},
    ]

    location_key = Column(BigInteger)
    npi = Column(BigInteger, nullable=False)
    year = Column(Integer, nullable=False)
    procedure_code = Column(BigInteger, nullable=False)
    place_of_service = Column(String(8))
    city = Column(String)
    state = Column(String(2))
    zip5 = Column(String(5))
    state_fips = Column(String(4))
    country = Column(String)


class PricingProviderProcedureCostProfile(Base, JSONOutputMixin):
    __tablename__ = "pricing_provider_procedure_cost_profile"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint(
            "npi",
            "year",
            "procedure_code",
            "geography_scope",
            "geography_value",
            "specialty_key",
            "setting_key",
        ),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = [
        "npi",
        "year",
        "procedure_code",
        "geography_scope",
        "geography_value",
        "specialty_key",
        "setting_key",
    ]
    __my_additional_indexes__ = [
        {"index_elements": ("year", "npi", "procedure_code"), "name": "pricing_provider_proc_cost_year_npi_proc_idx"},
        {
            "index_elements": ("year", "procedure_code", "geography_scope", "geography_value", "specialty_key", "setting_key"),
            "name": "pricing_provider_proc_cost_lookup_idx",
        },
        {
            "index_elements": (
                "year",
                "geography_scope",
                "geography_value",
                "specialty_key",
                "setting_key",
                "avg_submitted_charge",
            ),
            "name": "pricing_provider_proc_cost_geo_avg_idx",
        },
    ]

    npi = Column(BigInteger, nullable=False)
    year = Column(Integer, nullable=False)
    procedure_code = Column(BigInteger, nullable=False)
    geography_scope = Column(String(16), nullable=False)
    geography_value = Column(String(128), nullable=False)
    specialty_key = Column(String(128), nullable=False)
    setting_key = Column(String(64), nullable=False)
    claim_count = Column(Float)
    total_submitted_charge = Column(Numeric(scale=2, precision=18, asdecimal=False, decimal_return_scale=None))
    avg_submitted_charge = Column(Numeric(scale=4, precision=18, asdecimal=False, decimal_return_scale=None))
    updated_at = Column(DateTime)


class PricingProcedurePeerStats(Base, JSONOutputMixin):
    __tablename__ = "pricing_procedure_peer_stats"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint(
            "procedure_code",
            "year",
            "geography_scope",
            "geography_value",
            "specialty_key",
            "setting_key",
        ),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = [
        "procedure_code",
        "year",
        "geography_scope",
        "geography_value",
        "specialty_key",
        "setting_key",
    ]
    __my_additional_indexes__ = [
        {
            "index_elements": ("year", "procedure_code", "geography_scope", "geography_value", "specialty_key", "setting_key"),
            "name": "pricing_proc_peer_stats_lookup_idx",
        },
        {
            "index_elements": ("year", "geography_scope", "geography_value", "specialty_key", "setting_key"),
            "name": "pricing_proc_peer_stats_geo_idx",
        },
    ]

    procedure_code = Column(BigInteger, nullable=False)
    year = Column(Integer, nullable=False)
    geography_scope = Column(String(16), nullable=False)
    geography_value = Column(String(128), nullable=False)
    specialty_key = Column(String(128), nullable=False)
    setting_key = Column(String(64), nullable=False)
    provider_count = Column(Integer)
    min_claim_count = Column(Float)
    max_claim_count = Column(Float)
    p10 = Column(Numeric(scale=4, precision=18, asdecimal=False, decimal_return_scale=None))
    p20 = Column(Numeric(scale=4, precision=18, asdecimal=False, decimal_return_scale=None))
    p40 = Column(Numeric(scale=4, precision=18, asdecimal=False, decimal_return_scale=None))
    p50 = Column(Numeric(scale=4, precision=18, asdecimal=False, decimal_return_scale=None))
    p60 = Column(Numeric(scale=4, precision=18, asdecimal=False, decimal_return_scale=None))
    p80 = Column(Numeric(scale=4, precision=18, asdecimal=False, decimal_return_scale=None))
    p90 = Column(Numeric(scale=4, precision=18, asdecimal=False, decimal_return_scale=None))
    updated_at = Column(DateTime)


class PricingProcedureGeoBenchmark(Base, JSONOutputMixin):
    __tablename__ = "pricing_procedure_geo_benchmark"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("procedure_code", "year", "geography_scope", "geography_value"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["procedure_code", "year", "geography_scope", "geography_value"]
    __my_additional_indexes__ = [
        {"index_elements": ("year", "procedure_code", "geography_scope"), "name": "pricing_proc_geo_year_proc_scope_idx"},
        {"index_elements": ("year", "geography_scope", "geography_value"), "name": "pricing_proc_geo_year_scope_value_idx"},
    ]

    procedure_code = Column(BigInteger, nullable=False)
    year = Column(Integer, nullable=False)
    geography_scope = Column(String(16), nullable=False)  # national | state
    geography_value = Column(String(16), nullable=False)  # US or 2-char state
    total_services = Column(Float)
    avg_submitted_charge = Column(Float)
    avg_payment_amount = Column(Float)
    avg_standardized_amount = Column(Float)
    updated_at = Column(DateTime)


class PricingPrescription(Base, JSONOutputMixin):
    __tablename__ = "pricing_prescription"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("rx_code_system", "rx_code"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["rx_code_system", "rx_code"]
    __my_additional_indexes__ = [
        {"index_elements": ("rx_name",), "name": "pricing_prescription_name_idx"},
        {"index_elements": ("generic_name",), "name": "pricing_prescription_generic_idx"},
        {"index_elements": ("brand_name",), "name": "pricing_prescription_brand_idx"},
        {"index_elements": ("source_year",), "name": "pricing_prescription_source_year_idx"},
        {"index_elements": ("lower(rx_name)",), "name": "pricing_prescription_name_lower_idx"},
        {"index_elements": ("lower(generic_name)",), "name": "pricing_prescription_generic_lower_idx"},
        {"index_elements": ("lower(brand_name)",), "name": "pricing_prescription_brand_lower_idx"},
        {"index_elements": ("source_year", "lower(rx_name)"), "name": "pricing_prescription_year_name_lower_idx"},
    ]

    rx_code_system = Column(String(32), nullable=False)
    rx_code = Column(String(64), nullable=False)
    rx_name = Column(String)
    generic_name = Column(String)
    brand_name = Column(String)
    total_claims = Column(Float)
    total_30day_fills = Column(Float)
    total_day_supply = Column(Float)
    total_drug_cost = Column(Numeric(scale=2, precision=16, asdecimal=False, decimal_return_scale=None))
    total_benes = Column(Float)
    source_year = Column(Integer)


class PricingProviderPrescription(Base, JSONOutputMixin):
    __tablename__ = "pricing_provider_prescription"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("npi", "year", "rx_code_system", "rx_code"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["npi", "year", "rx_code_system", "rx_code"]
    __my_additional_indexes__ = [
        {"index_elements": ("year", "rx_code_system", "rx_code"), "name": "pricing_provider_rx_year_code_idx"},
        {"index_elements": ("year", "rx_code_system", "rx_code", "npi"), "name": "pricing_provider_rx_year_code_npi_idx"},
        {"index_elements": ("year", "state", "city"), "name": "pricing_provider_rx_year_state_city_idx"},
        {"index_elements": ("year", "state", "city", "provider_type"), "name": "pricing_provider_rx_year_state_city_type_idx"},
        {"index_elements": ("rx_name",), "name": "pricing_provider_rx_name_idx"},
        {"index_elements": ("generic_name",), "name": "pricing_provider_rx_generic_idx"},
        {"index_elements": ("brand_name",), "name": "pricing_provider_rx_brand_idx"},
        {"index_elements": ("year", "lower(rx_name)"), "name": "pricing_provider_rx_year_name_lower_idx"},
        {"index_elements": ("year", "lower(generic_name)"), "name": "pricing_provider_rx_year_generic_lower_idx"},
        {"index_elements": ("year", "lower(brand_name)"), "name": "pricing_provider_rx_year_brand_lower_idx"},
        {"index_elements": ("year", "npi", "total_drug_cost DESC"), "name": "pricing_provider_rx_year_npi_total_drug_cost_desc_idx"},
        {"index_elements": ("year", "total_drug_cost DESC"), "name": "pricing_provider_rx_year_total_drug_cost_desc_idx"},
    ]

    npi = Column(BigInteger, nullable=False)
    year = Column(Integer, nullable=False)
    rx_code_system = Column(String(32), nullable=False)
    rx_code = Column(String(64), nullable=False)
    rx_name = Column(String)
    generic_name = Column(String)
    brand_name = Column(String)
    provider_name = Column(String)
    provider_type = Column(String)
    city = Column(String)
    state = Column(String(2))
    zip5 = Column(String(5))
    country = Column(String)
    total_claims = Column(Float)
    total_30day_fills = Column(Float)
    total_day_supply = Column(Float)
    total_drug_cost = Column(Numeric(scale=2, precision=16, asdecimal=False, decimal_return_scale=None))
    total_benes = Column(Float)
    ge65_total_claims = Column(Float)
    ge65_total_30day_fills = Column(Float)
    ge65_total_day_supply = Column(Float)
    ge65_total_drug_cost = Column(Float)
    ge65_total_benes = Column(Float)


class CodeCatalog(Base, JSONOutputMixin):
    __tablename__ = "code_catalog"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("code_system", "code"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["code_system", "code"]
    __my_additional_indexes__ = [
        {"index_elements": ("code", "code_system"), "name": "code_catalog_code_system_idx"},
        {"index_elements": ("code_checksum",), "name": "code_catalog_code_checksum_idx"},
        {"index_elements": ("code_system", "display_name"), "name": "code_catalog_system_display_idx"},
        {"index_elements": ("code_system", "lower(display_name)"), "name": "code_catalog_system_display_lower_idx"},
        {"index_elements": ("lower(display_name)",), "name": "code_catalog_display_lower_idx"},
        {"index_elements": ("lower(short_description)",), "name": "code_catalog_short_description_lower_idx"},
        {"index_elements": ("source",), "name": "code_catalog_source_idx"},
        {"index_elements": ("code_system", "source"), "name": "code_catalog_system_source_idx"},
        {"index_elements": ("source", "code_system", "lower(display_name)"), "name": "code_catalog_source_system_display_lower_idx"},
    ]

    code_system = Column(String(32), nullable=False)
    code = Column(String(128), nullable=False)
    code_checksum = Column(Integer)
    display_name = Column(String)
    short_description = Column(String)
    long_description = Column(TEXT)
    is_active = Column(Boolean)
    source = Column(String(128))
    updated_at = Column(DateTime)


class CodeCrosswalk(Base, JSONOutputMixin):
    __tablename__ = "code_crosswalk"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("from_system", "from_code", "to_system", "to_code"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["from_system", "from_code", "to_system", "to_code"]
    __my_additional_indexes__ = [
        {"index_elements": ("from_system", "from_code"), "name": "code_crosswalk_from_idx"},
        {"index_elements": ("from_checksum",), "name": "code_crosswalk_from_checksum_idx"},
        {"index_elements": ("to_system", "to_code"), "name": "code_crosswalk_to_idx"},
        {"index_elements": ("to_checksum",), "name": "code_crosswalk_to_checksum_idx"},
        {"index_elements": ("upper(from_system)", "upper(from_code)"), "name": "code_crosswalk_upper_from_idx"},
        {"index_elements": ("upper(to_system)", "upper(to_code)"), "name": "code_crosswalk_upper_to_idx"},
    ]

    from_system = Column(String(32), nullable=False)
    from_code = Column(String(128), nullable=False)
    from_checksum = Column(Integer)
    to_system = Column(String(32), nullable=False)
    to_code = Column(String(128), nullable=False)
    to_checksum = Column(Integer)
    match_type = Column(String(32))
    confidence = Column(Numeric(scale=4, precision=6, asdecimal=False, decimal_return_scale=None))
    source = Column(String(128))
    updated_at = Column(DateTime)
