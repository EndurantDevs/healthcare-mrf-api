# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Keep protected registry ownership independent of source-table ownership."""

import os
import re


def registry_schema():
    """Use a dedicated registry namespace when configured; preserve the existing default."""
    schema = os.getenv("HLTHPRT_NETWORK_REGISTRY_SCHEMA") or os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,62}", schema) is None:
        raise ValueError("Registry schema identifier is invalid")
    return schema
