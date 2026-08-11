# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from sanic import response
from sanic.blueprints import Blueprint
from sanic.exceptions import SanicException

from api.control import blueprint as control_blueprint, control_error
from api.metrics import blueprint as metrics_blueprint
from api.endpoint.formulary import blueprint as v1_formulary
from api.endpoint.formulary_fhir import blueprint as v1_formulary_fhir
from api.endpoint.coverage import blueprint as v1_coverage
from api.endpoint.codes import blueprint as v1_codes
from api.endpoint.clinical import blueprint as v1_clinical
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
from api.ptg2_capacity_evidence import (
    CapacityEvidenceError,
    guard_isolated_capacity_process_request,
)
from api.provider_directory_profile_capacity_preflight import (
    register_profile_capacity_preflight_route,
)
from api.runtime_identity import add_runtime_identity_headers
from api.worker_memory import register_worker_memory_lifecycle
from db.connection import db

profile_capacity_blueprint = Blueprint("profile_capacity_control", url_prefix="/control")
register_profile_capacity_preflight_route(profile_capacity_blueprint)
profile_capacity_blueprint.exception(SanicException)(control_error)


def _capacity_process_request_guard(request):
    """Keep an isolated cold-evidence process unavailable to other routes."""

    try:
        guard_isolated_capacity_process_request(request)
    except CapacityEvidenceError:
        return response.json(
            {"error": "capacity_evidence_process_isolated"},
            status=503,
        )
    return None


def init_api(api):
    """Register public API blueprints on the Sanic application."""

    db.init_app(api)
    register_worker_memory_lifecycle(api)
    api.register_middleware(_capacity_process_request_guard, "request")
    api.register_middleware(add_runtime_identity_headers, "response")
    api.blueprint(control_blueprint)
    api.blueprint(profile_capacity_blueprint)
    api.blueprint(metrics_blueprint)
    api_bluenprint = Blueprint.group(
        [
            v1_healthcheck,
            v1_coverage,
            v1_plan,
            v1_formulary,
            v1_formulary_fhir,
            v1_codes,
            v1_clinical,
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
