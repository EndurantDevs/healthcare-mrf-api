# pylint: disable = unused-variable
import os
from gino.engine import GinoConnection as _Connection, GinoEngine as _Engine
from gino.strategies import GinoStrategy
from gino.api import Gino as _Gino, GinoExecutor as _Executor
from sqlalchemy.engine.url import URL
from sanic.exceptions import NotFound


class SanicModelMixin:
    @classmethod
    async def get_or_404(cls, *args, **kwargs):
        # noinspection PyUnresolvedReferences
        rv = await cls.get(*args, **kwargs)
        if rv is None:
            raise NotFound("{} is not found".format(cls.__name__))
        return rv


# noinspection PyClassHasNoInit
class GinoExecutor(_Executor):
    async def first_or_404(self, *args, **kwargs):
        rv = await self.first(*args, **kwargs)
        if rv is None:
            raise NotFound("No such data")
        return rv


# noinspection PyClassHasNoInit
class GinoConnection(_Connection):
    async def first_or_404(self, *args, **kwargs):
        rv = await self.first(*args, **kwargs)
        if rv is None:
            raise NotFound("No such data")
        return rv


# noinspection PyClassHasNoInit
class GinoEngine(_Engine):
    connection_cls = GinoConnection

    async def first_or_404(self, *args, **kwargs):
        rv = await self.first(*args, **kwargs)
        if rv is None:
            raise NotFound("No such data")
        return rv


class SanicStrategy(GinoStrategy):
    name = "sanic"
    engine_cls = GinoEngine


SanicStrategy()


async def init_db(db, loop):
    dsn = URL(
        drivername=os.environ.get('HLTHPRT_DB_DRIVER', 'asyncpg'),
        host=os.environ.get('HLTHPRT_DB_HOST', 'localhost'),
        port=os.environ.get('HLTHPRT_DB_PORT', 5432),
        username=os.environ.get('HLTHPRT_DB_USER', 'postgres'),
        password=os.environ.get('HLTHPRT_DB_PASSWORD', ''),
        database=os.environ.get('HLTHPRT_DB_DATABASE', 'postgres'),
    )
    await db.set_bind(
        dsn,
        echo=bool(os.environ.get('HLTHPRT_DB_ECHO', False)),
        min_size=int(os.environ.get('HLTHPRT_DB_POOL_MIN_SIZE', 5)),
        max_size=int(os.environ.get('HLTHPRT_DB_POOL_MAX_SIZE', 10)),
        ssl=bool(os.environ.get('HLTHPRT_DB_SSL', False)),
        loop=loop,
        **os.environ.get('HLTHPRT_DB_KWARGS', {}),
    )


class Gino(_Gino):
    """Support Sanic web server.
    By :meth:`init_app` GINO registers a few hooks on Sanic, so that GINO could
    use database configuration in Sanic ``config`` to initialize the bound
    engine.
    A lazy connection context is enabled by default for every request. You can
    change this default behavior by setting ``DB_USE_CONNECTION_FOR_REQUEST``
    config value to ``False``. By default, a database connection is borrowed on
    the first query, shared in the same execution context, and returned to the
    pool on response. If you need to release the connection early in the middle
    to do some long-running tasks, you can simply do this::
        await request['connection'].release(permanent=False)
    """

    model_base_classes = _Gino.model_base_classes + (SanicModelMixin,)
    query_executor = GinoExecutor

    def __init__(self, app=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if app is not None:
            self.init_app(app)

    def init_app(self, app):
        if app.config.setdefault("DB_USE_CONNECTION_FOR_REQUEST", True):

            @app.middleware("request")
            async def on_request(request):
                conn = await self.acquire(lazy=True)
                if hasattr(request, "ctx"):
                    request.ctx.connection = conn
                else:
                    request["connection"] = conn

            @app.middleware("response")
            async def on_response(request, _):
                if hasattr(request, "ctx"):
                    conn = getattr(request.ctx, "connection", None)
                else:
                    conn = request.pop("connection", None)
                if conn is not None:
                    await conn.release()

        @app.listener("after_server_start")
        async def before_server_start(_, loop):
            if app.config.get("DB_DSN"):
                dsn = app.config.DB_DSN
            else:
                await init_db(self, loop)

        @app.listener("before_server_stop")
        async def after_server_stop(_, loop):
            await self.pop_bind().close()

    async def first_or_404(self, *args, **kwargs):
        rv = await self.first(*args, **kwargs)
        if rv is None:
            raise NotFound("No such data")
        return rv

    async def set_bind(self, bind, loop=None, **kwargs):
        kwargs.setdefault("strategy", "sanic")
        return await super().set_bind(bind, loop=loop, **kwargs)

db = Gino()