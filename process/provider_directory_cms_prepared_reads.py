# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fixed canonical reads for private CMS identity preparation.

The administrator must authenticate the profile, native targets and exact ACL
delta before applying the returned SQL. This module issues no authority and
never executes grants; the worker only checks its existing read access.
"""

from db.plan_release_storage import storage_identifier, storage_relation
from process.provider_directory_cms_identity_candidate import CMS_IDENTITY_COLUMNS
from process.provider_directory_resource_identity_candidate import RESOURCE_IDENTITY_COLUMNS, RESOURCE_IDENTITY_ROOT

READ_COLUMNS = {
    **CMS_IDENTITY_COLUMNS,
    RESOURCE_IDENTITY_ROOT: RESOURCE_IDENTITY_COLUMNS,
    "provider_directory_organization_identity": ("organization_id",),
    "provider_directory_site_identity": ("site_id",),
    "provider_directory_insurance_network_identity": ("network_id",),
}
_READ_SQL = """SELECT c.relkind IN ('r','p') AND NOT c.relrowsecurity AND NOT c.relforcerowsecurity
 AND pg_catalog.has_schema_privilege(c.relnamespace,'USAGE')
 AND NOT EXISTS(SELECT FROM pg_catalog.unnest($2::text[]) wanted(name)
   LEFT JOIN pg_catalog.pg_attribute a ON a.attrelid=c.oid AND a.attname=wanted.name
     AND a.attnum>0 AND NOT a.attisdropped
   WHERE a.attnum IS NULL OR NOT pg_catalog.has_column_privilege(c.oid,a.attnum,'SELECT'))
 FROM pg_catalog.pg_class c WHERE c.oid=pg_catalog.to_regclass($1)"""


def cms_preparation_read_grants(catalog_schema, publisher_role):
    """Render only the eight fixed column SELECTs for an authenticated admin cut."""
    publisher = storage_identifier(publisher_role)
    return tuple(
        f"GRANT SELECT({','.join(storage_identifier(column) for column in columns)}) "
        f"ON {storage_relation(catalog_schema, root)} TO {publisher}"
        for root, columns in READ_COLUMNS.items()
    )


async def require_cms_preparation_reads(connection, catalog_schema):
    """Refuse missing native reads before building any private output.

    The caller authenticates the original Publisher and owns its bounded
    transaction. Passing this check proves neither writer closure nor content.
    """
    if not connection.is_in_transaction() or connection.is_closed():
        raise ValueError("cms_prepared_reads_transaction_required")
    for root, columns in READ_COLUMNS.items():
        if await connection.fetchval(_READ_SQL, storage_relation(catalog_schema, root), list(columns)) is not True:
            raise ValueError("cms_prepared_reads_missing_" + root)
