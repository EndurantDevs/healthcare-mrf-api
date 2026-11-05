# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Use real request ownership while native transport fixtures own persistent pools."""


def attach_request_database_hooks(database, app):
    """Register production request cleanup without replacing fixture-owned pool lifetimes."""

    class RequestHooks:
        def listener(self, _name):
            return lambda callback: callback

        def middleware(self, kind):
            return app.middleware(kind)

    database.init_app(RequestHooks())
