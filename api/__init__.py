# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from sanic.blueprints import Blueprint

from api.endpoint.formulary import blueprint as v1_formulary
from api.endpoint.coverage import blueprint as v1_coverage
from api.endpoint.codes import blueprint as v1_codes
from api.endpoint.geo import blueprint as v1_geo
from api.endpoint.healthcheck import blueprint as v1_healthcheck
from api.endpoint.importer import blueprint as v1_import
from api.endpoint.issuer import blueprint as v1_issuer
from api.endpoint.npi import blueprint as v1_npi
from api.endpoint.nucc import blueprint as v1_nucc
from api.endpoint.plan import blueprint as v1_plan
from api.endpoint.pricing import blueprint as v1_pricing
from api.endpoint.partd_formulary import blueprint as v1_partd_formulary
from api.endpoint.pharmacy_license import blueprint as v1_pharmacy_license
from api.endpoint.reports import blueprint as v1_reports
from api.endpoint.site_intelligence import blueprint as v1_site_intelligence
from db.connection import db


def init_api(api):
    db.init_app(api)
    api_bluenprint = Blueprint.group(
        [
            v1_healthcheck,
            v1_coverage,
            v1_plan,
            v1_formulary,
            v1_codes,
            v1_import,
            v1_issuer,
            v1_npi,
            v1_nucc,
            v1_geo,
            v1_pricing,
            v1_partd_formulary,
            v1_pharmacy_license,
            v1_reports,
            v1_site_intelligence,
        ],
        version_prefix="/api/v",
    )
    api.blueprint(api_bluenprint)
