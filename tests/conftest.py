# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import os

TEST_ENV_DEFAULTS = {
    "HLTHPRT_DB_DRIVER": "asyncpg",
    "HLTHPRT_DB_HOST": "127.0.0.1",
    "HLTHPRT_DB_PORT": "5432",
    "HLTHPRT_DB_USER": "postgres",
    "HLTHPRT_DB_PASSWORD": "",
    "HLTHPRT_DB_DATABASE": "healthporta",
    "HLTHPRT_DB_SCHEMA": "mrf",
    "HLTHPRT_DB_POOL_MIN_SIZE": "1",
    "HLTHPRT_DB_POOL_MAX_SIZE": "5",
    "HLTHPRT_DB_ECHO": "False",
    "HLTHPRT_REDIS_ADDRESS": "redis://127.0.0.1:6379",
}

for key, value in TEST_ENV_DEFAULTS.items():
    os.environ.setdefault(key, value)