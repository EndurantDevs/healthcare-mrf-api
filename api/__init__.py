# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from sanic.blueprints import Blueprint

from api.endpoint.formulary import blueprint as v1_formulary
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
from db.connection import db


def init_api(api):
    db.init_app(api)
    api_bluenprint = Blueprint.group(
        [
            v1_healthcheck,
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
        ],
        version_prefix="/api/v",
    )
    api.blueprint(api_bluenprint)
