# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Independent deployment policy for managed pricing control admission."""

import json
import os
import re
from dataclasses import dataclass
from pathlib import Path

from process.plan_release_storage_custody import storage_profile
from process.source_profile_result_pins import _policy_object, _role_policy

NAMESPACE_POLICY_PATH = Path("/etc/pricing-control/namespace-role-policy.json")
PREPARATION_POLICY_PATH = Path("/etc/pricing-control/preparation.json")
HISTORICAL_POLICY_PATH = Path("/etc/pricing-control/historical.json")
HISTORICAL_POLICY_CONTRACT = "plan-pricing-historical-control-v1"


@dataclass(frozen=True)
class PricingControlPolicy:
    owner_role: str
    publisher_role: str
    reader_roles: tuple[str, ...]
    control_reader_roles: tuple[str, ...]

    def storage_arguments(self):
        """Pass only the cross-checked explicit native custody profile."""
        return {
            "owner_role": self.owner_role,
            "publisher_role": self.publisher_role,
            "reader_roles": self.reader_roles,
            "control_reader_roles": self.control_reader_roles,
        }


def _read_policy_file(path):
    with path.open("rb") as stream:
        payload = stream.read(8193)
    if len(payload) > 8192:
        raise ValueError("pricing control policy is too large")
    return json.loads(payload, object_pairs_hook=_policy_object)


def pricing_control_policy(namespace_by_field, preparation_by_field):
    """Cross-check both existing documents without discovering any principal."""
    owner_role, reader_roles = _role_policy(namespace_by_field)
    if not isinstance(preparation_by_field, dict) or set(preparation_by_field) != {
        "owner_role",
        "publisher_role",
        "runtime_roles",
        "pin_role",
    }:
        raise ValueError("pricing preparation policy is invalid")
    control_roles = tuple(namespace_by_field.get("control_runtime_roles", ()))
    publisher_role = preparation_by_field["publisher_role"]
    configured_roles = [value for key, value in namespace_by_field.items() if key.endswith("_role")]
    configured_roles.extend(reader_roles)
    if (
        not control_roles
        or preparation_by_field["owner_role"] != owner_role
        or preparation_by_field["runtime_roles"] != list(reader_roles)
        or preparation_by_field["pin_role"] != namespace_by_field.get("catalog_pin_role")
        or type(publisher_role) is not str
        or re.fullmatch(r"[a-z_][a-z0-9_]{0,62}", publisher_role) is None
        or publisher_role in configured_roles
    ):
        raise ValueError("pricing control policy documents differ")
    storage_profile(owner_role, publisher_role, reader_roles, control_roles)
    return PricingControlPolicy(owner_role, publisher_role, reader_roles, control_roles)


def load_pricing_control_policy():
    """Only application-owned fixed read-only files can authorize the role tuple."""
    try:
        return pricing_control_policy(
            _read_policy_file(NAMESPACE_POLICY_PATH), _read_policy_file(PREPARATION_POLICY_PATH)
        )
    except OSError, TypeError, ValueError:
        raise ValueError("pricing control policy is unavailable or invalid") from None


def is_qualified_historical_revision(schema, revision):
    """Match one independently qualified scalar layout without granting managed authority."""
    try:
        declared = _read_policy_file(HISTORICAL_POLICY_PATH)
        if type(declared) is not dict or set(declared) != {"contract", "schema", "revision"}:
            raise ValueError("invalid historical policy")
        if (
            declared["contract"] != HISTORICAL_POLICY_CONTRACT
            or type(declared["schema"]) is not str
            or re.fullmatch(r"[a-z_][a-z0-9_]{0,62}", declared["schema"]) is None
            or type(declared["revision"]) is not str
            or re.fullmatch(r"[A-Za-z0-9_]{1,64}", declared["revision"]) is None
        ):
            raise ValueError("invalid historical policy")
        return declared["schema"] == schema and declared["revision"] == revision
    except FileNotFoundError:
        return False
    except OSError, TypeError, ValueError:
        raise ValueError("pricing historical policy is unavailable or invalid") from None


def pricing_control_schema():
    """Use an explicit deployment schema, never a request or inferred default."""
    value = os.getenv("HLTHPRT_PLAN_PRICING_CONTROL_SCHEMA", "")
    if re.fullmatch(r"[a-z_][a-z0-9_]{0,62}", value) is None:
        raise ValueError("pricing control schema is unavailable or invalid")
    return value
