# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Static built-in record declarations for already verified FHIR payloads."""

from sqlalchemy.dialects.postgresql import dialect
from sqlalchemy.sql import sqltypes

from db.models.system import (
    ProviderDirectoryEndpoint,
    ProviderDirectoryHealthcareService,
    ProviderDirectoryOrganization,
    ProviderDirectoryOrganizationAffiliation,
    ProviderDirectoryPractitioner,
    ProviderDirectoryPractitionerRole,
)

PROFILE_RESOURCE_MODELS = {
    "practitioner": ProviderDirectoryPractitioner,
    "role": ProviderDirectoryPractitionerRole,
    "organization": ProviderDirectoryOrganization,
    "affiliation": ProviderDirectoryOrganizationAffiliation,
    "service": ProviderDirectoryHealthcareService,
    "endpoint": ProviderDirectoryEndpoint,
}
_BUILTIN_TYPES = {
    sqltypes.String,
    sqltypes.TEXT,
    sqltypes.JSON,
    sqltypes.BigInteger,
    sqltypes.Integer,
    sqltypes.Boolean,
    sqltypes.TIMESTAMP,
}


def profile_resource_record_columns(resource):
    """Preserve every model field, type modifier and conversion without a live row type.

    This declares values only: defaults and table constraints are not applied,
    matching jsonb_populate_record. Source/policy validation remains external.
    """
    if type(resource) is not str or resource not in PROFILE_RESOURCE_MODELS:
        raise ValueError("profile_resource_record_invalid")
    postgres = dialect()
    declarations = []
    for column in PROFILE_RESOURCE_MODELS[resource].__table__.columns:
        if type(column.type) not in _BUILTIN_TYPES:
            raise ValueError("profile_resource_record_type_unsupported")
        declarations.append(
            postgres.identifier_preparer.quote_identifier(column.name) + " " + column.type.compile(dialect=postgres)
        )
    return ",".join(declarations)
