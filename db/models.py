import os
from sqlalchemy import DateTime, DATE, Column, String, Integer, Float, BigInteger, Boolean, ARRAY, JSON, TIMESTAMP, TEXT

from db.connection import db
from db.json_mixin import JSONOutputMixin

class ImportHistory(db.Model, JSONOutputMixin):
    __tablename__ = 'history'
    __main_table__ = __tablename__
    __table_args__ = (
        {'schema': os.getenv('DB_SCHEMA') or 'mrf', 'extend_existing': True},
    )
    __my_index_elements__ = ['import_id']
    import_id = Column(String)
    json_status = Column(JSON)
    when = Column(DateTime)

class ImportLog(db.Model, JSONOutputMixin):
    __tablename__ = 'log'
    __main_table__ = __tablename__
    __table_args__ = (
        {'schema': os.getenv('DB_SCHEMA') or 'mrf', 'extend_existing': True},
    )
    __my_index_elements__ = ['issuer_id', 'checksum']
    issuer_id = Column(Integer)
    checksum = Column(BigInteger)
    type = Column(String(4))
    text = Column(String)
    url = Column(String)
    source = Column(String) #plans, index, providers, etc.
    level = Column(String)  #network, json, etc.

class Issuer(db.Model, JSONOutputMixin):
    __tablename__ = 'issuer'
    __main_table__ = __tablename__
    __table_args__ = (
        {'schema': os.getenv('DB_SCHEMA') or 'mrf', 'extend_existing': True},
    )
    state = Column(String(2))
    issuer_id = Column(Integer)
    issuer_name = Column(String)
    issuer_marketing_name = Column(String)
    mrf_url = Column(String)
    data_contact_email = Column(String)

class PlanFormulary(db.Model, JSONOutputMixin):
    __tablename__ = 'plan_formulary'
    __main_table__ = __tablename__
    __table_args__ = (
        {'schema': os.getenv('DB_SCHEMA') or 'mrf', 'extend_existing': True},
    )
    __my_index_elements__ = ['plan_id', 'year', 'drug_tier', 'pharmacy_type']
    plan_id = Column(db.String(14), nullable=False)
    year = Column(Integer)
    drug_tier = Column(String)
    mail_order = Column(Boolean)
    pharmacy_type = Column(String)
    copay_amount = Column(Float)
    copay_opt = Column(String)
    coinsurance_rate = Column(Float)
    coinsurance_opt = Column(String)


class Plan(db.Model, JSONOutputMixin):
    __tablename__ = 'plan'
    __main_table__ = __tablename__
    __table_args__ = (
        {'schema': os.getenv('DB_SCHEMA') or 'mrf', 'extend_existing': True},
    )
    __my_index_elements__ = ['plan_id', 'year']
    plan_id = Column(db.String(14), nullable=False)  # len == 14
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

class PlanAttributes(db.Model, JSONOutputMixin):
    __tablename__ = 'plan_attributes'
    __main_table__ = __tablename__
    __table_args__ = (
        {'schema': os.getenv('DB_SCHEMA') or 'mrf', 'extend_existing': True},
    )
    __my_index_elements__ = ['full_plan_id', 'year', 'attr_name']
    full_plan_id = Column(db.String(17), nullable=False)
    year = Column(Integer)
    attr_name = Column(String)
    attr_value = Column(String)



class PlanTransparency(db.Model, JSONOutputMixin):
    __tablename__ = 'plan_transparency'
    __main_table__ = __tablename__
    __table_args__ = (
        {'schema': os.getenv('DB_SCHEMA') or 'mrf', 'extend_existing': True},
    )
    __my_index_elements__ = ['plan_id', 'year']
    state = Column(String(2))
    issuer_name = Column(String)
    issuer_id = Column(Integer)
    new_issuer_to_exchange = Column(Boolean)
    sadp_only = Column(Boolean)
    plan_id = Column(db.String(14), nullable=False)  # len == 14
    year = Column(Integer)
    qhp_sadp = Column(String)
    plan_type = Column(String)
    metal = Column(String)
    claims_payment_policies_url = Column(String)

class NPIData(db.Model, JSONOutputMixin):
    __tablename__ = 'npi'
    __main_table__ = __tablename__
    __table_args__ = (
        {'schema': os.getenv('DB_SCHEMA') or 'mrf', 'extend_existing': True},
    )
    __my_index_elements__ = ['npi']
    npi = Column(BigInteger)
    employer_identification_number = Column(String)
    entity_type_code = Column(Integer)
    replacement_npi = Column(BigInteger)
    provider_organization_name = Column(String)
    provider_last_name = Column(String)
    provider_first_name = Column(String)
    provider_middle_name = Column(String)
    provider_name_prefix_text = Column(String)
    provider_name_suffix_text = Column(String)
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
    provider_first_line_business_mailing_address = Column(String)
    provider_second_line_business_mailing_address = Column(String)
    provider_business_mailing_address_city_name = Column(String)
    provider_business_mailing_address_state_name = Column(String)
    provider_business_mailing_address_postal_code = Column(String)
    provider_business_mailing_address_country_code = Column(String)
    provider_business_mailing_address_telephone_number = Column(String)
    provider_business_mailing_address_fax_number = Column(String)
    provider_first_line_business_practice_location_address = Column(String)
    provider_second_line_business_practice_location_address = Column(String)
    provider_business_practice_location_address_city_name = Column(String)
    provider_business_practice_location_address_state_name = Column(String)
    provider_business_practice_location_address_postal_code = Column(String)
    provider_business_practice_location_address_country_code = Column(String)
    provider_business_practice_location_address_telephone_number = Column(String)
    provider_business_practice_location_address_fax_number = Column(String)
    provider_enumeration_date = Column(DATE)
    last_update_date = Column(DATE)
    npi_deactivation_reason_code = Column(String)
    npi_deactivation_date = Column(DATE)
    npi_reactivation_date = Column(DATE)
    provider_gender_code = Column(String)
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

class NPIDataTaxonomy(db.Model, JSONOutputMixin):
    __tablename__ = 'npi_taxonomy'
    __main_table__ = __tablename__
    __table_args__ = (
        {'schema': os.getenv('DB_SCHEMA') or 'mrf', 'extend_existing': True},
    )
    __my_index_elements__ = ['npi', 'checksum']

    npi = Column(BigInteger)
    checksum = Column(BigInteger)
    healthcare_provider_taxonomy_code = Column(String)
    provider_license_number = Column(String)
    provider_license_number_state_code = Column(String)
    healthcare_provider_primary_taxonomy_switch = Column(String)

class NPIDataOtherIdentifier(db.Model, JSONOutputMixin):
    __tablename__ = 'npi_other_identifier'
    __main_table__ = __tablename__
    __table_args__ = (
        {'schema': os.getenv('DB_SCHEMA') or 'mrf', 'extend_existing': True},
    )
    __my_index_elements__ = ['npi', 'checksum']

    npi = Column(BigInteger)
    checksum = Column(BigInteger)
    other_provider_identifier = Column(String)
    other_provider_identifier_type_code = Column(String)
    other_provider_identifier_state = Column(String)
    other_provider_identifier_issuer = Column(String)

class NPIDataTaxonomyGroup(db.Model, JSONOutputMixin):
    __tablename__ = 'npi_taxonomy_group'
    __main_table__ = __tablename__
    __table_args__ = (
        {'schema': os.getenv('DB_SCHEMA') or 'mrf', 'extend_existing': True},
    )
    __my_index_elements__ = ['npi', 'checksum']

    npi = Column(BigInteger)
    checksum = Column(BigInteger)
    healthcare_provider_taxonomy_group = Column(String)
