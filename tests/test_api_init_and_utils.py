# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import types

import pytest
import importlib
from shapely.geometry import Polygon

from api import init_api
from api.utils import square_poly


def test_init_api_registers_group(monkeypatch):
    calls_by_name = {}

    fake_db = types.SimpleNamespace(init_app=lambda app: calls_by_name.setdefault("init", True))
    api_module = importlib.import_module("api.__init__")
    monkeypatch.setattr(api_module, "db", fake_db)
    monkeypatch.setitem(init_api.__globals__, "db", fake_db)

    class FakeApp:
        def __init__(self):
            self.config = {}
            self.registered = None
            self.registered_middleware = []
            self.listeners = {}

        def listener(self, event):
            def decorator(func):
                self.listeners[event] = func
                return func
            return decorator

        def middleware(self, _phase):
            def decorator(func):
                return func
            return decorator

        def register_middleware(self, middleware, phase):
            self.registered_middleware.append((middleware, phase))

        def blueprint(self, group):
            self.registered = group

    app = FakeApp()
    init_api(app)

    assert calls_by_name["init"] is True
    assert app.registered is not None
    assert app.registered_middleware == [
        (init_api.__globals__["_capacity_process_request_guard"], "request")
    ]
    assert hasattr(app.registered, "blueprints")
    assert {bp.name for bp in app.registered.blueprints} == {
        "coverage",
        "clinical",
        "codes",
        "healthcheck",
        "plan",
        "formulary",
        "import",
        "issuer",
        "npi",
        "nucc",
        "geo",
        "pricing",
        "partd_formulary",
        "pharmacy_license",
        "reports",
        "site_intelligence",
    }


@pytest.mark.parametrize("distance", [0.1, 1, 5])
def test_square_poly(distance):
    polygon = square_poly(40.0, -74.0, distance=distance)
    assert isinstance(polygon, Polygon)
    minx, miny, maxx, maxy = polygon.bounds
    assert minx < maxx and miny < maxy
