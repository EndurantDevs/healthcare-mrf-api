# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Keep raw release selectors behind the internal HTTP listener."""

from sanic.exceptions import Forbidden

PLAN_RELEASE_READ_BOUNDARY_HEADER = "X-Plan-Release-Read-Boundary"


def require_internal_plan_release_read(request):
    """Trust only the marker overwritten by the protected internal listener."""
    if "plan_release_id" not in request.get_args(keep_blank_values=True):
        return None
    if request.headers.getall(PLAN_RELEASE_READ_BOUNDARY_HEADER, []) != ["internal"]:
        raise Forbidden("raw plan release selectors require the internal read listener")
    return None
