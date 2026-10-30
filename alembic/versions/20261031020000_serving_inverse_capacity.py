# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Admit fresh standalone inverse authority in the existing immutable ledgers."""

import importlib.util
from pathlib import Path

from sqlalchemy import text

from alembic import op

revision = "20261031020000_serving_inverse_capacity"
down_revision = "20261031010000_profile_initial_reactivation"
branch_labels = None
depends_on = None
_PURPOSE = "serving_inverse"
_RECEIPT = "healthporta.provider-directory-serving-inverse-capacity-preflight.v1"
_REQUEST = "healthporta.provider-directory-serving-inverse-capacity-preflight-request.v1"
_LIMITS = "provider-directory-serving-inverse-limits.v1"
_GEOMETRY = "provider-directory-serving-inverse-capacity.v1"
_SCOPE = "provider-directory-serving-inverse-scope.v1"


def _previous():
    path = Path(__file__).with_name("20261031010000_profile_initial_reactivation.py")
    spec = importlib.util.spec_from_file_location("_inverse_capacity_previous", path)
    if spec is None or spec.loader is None:
        raise RuntimeError("serving_inverse_capacity_predecessor_unavailable")
    previous = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(previous)
    return previous, *previous._previous()


def _purpose_check():
    return "admission_purpose IN ('profile','cms_nonprofile','serving_nonprofile','serving_inverse')"


def _inverse_bindings():
    document = "receipt_json::jsonb"
    geometry = document + "->'capacity_geometry'"
    scope = geometry + "->'scope'"
    provenance = scope + "->'profile_provenance'"
    columns = (
        "selection_proof_id",
        "profile_input_digest",
        "control_generation",
        "profile_schema_version",
        "profile_strategy_version",
    )
    bindings = [f"{provenance}->'{name}'=to_jsonb({name})" for name in columns]
    for field, column in (
        ("receipt_sha256", "receipt_sha256"),
        ("request_nonce", "request_nonce"),
        ("request_sha256", "request_sha256"),
        ("control_plane_receipt_sha256", "control_plane_receipt_sha256"),
        ("capacity_limits_sha256", "limits_sha256"),
        ("capacity_geometry_hash", "capacity_geometry_hash"),
        ("quiescence_sha256", "quiescence_sha256"),
    ):
        bindings.append(f"{document}->>'{field}'={column}")
    bindings.extend(
        (
            f"{geometry}->>'limits_sha256'=limits_sha256",
            f"{geometry}->>'input_sha256'=serving_preflight_sha256",
            f"{scope}->>'input_sha256'=serving_preflight_sha256",
            f"{document}->'capacity_limits'->>'contract_id'=limits_contract_id",
            f"{document}->'recovery_execution_identity'->>'contract_id'='provider-directory-serving-inverse-execution.v1'",
            f"{document}->'recovery_execution_identity'->>'importer'='provider-directory-fhir'",
            f"{document}->'recovery_execution_identity'->>'operation'='recover'",
        )
    )
    bindings.extend(
        f"{document}->'recovery_execution_identity'->'{name}'={scope}->'{name}'"
        for name in ("generation", "published_event_sha256", "node_id")
    )
    return " AND ".join(bindings)


def _inverse_values_check(initial):
    """Retain native digest/time/use checks without pretending historical provenance is a new Profile."""
    common_prefix = "AND receipt_sha256 ~ "
    original = initial._ORIGINAL._values_check()
    if original.count(common_prefix) != 1:
        raise RuntimeError("serving_inverse_capacity_predecessor_changed")
    common = "receipt_sha256 ~ " + original.split(common_prefix, 1)[1]
    document = "receipt_json::jsonb"
    geometry = document + "->'capacity_geometry'"
    scope = geometry + "->'scope'"
    return f"""contract_id='{_RECEIPT}' AND request_contract_id='{_REQUEST}'
        AND limits_contract_id='{_LIMITS}' AND materialization_mode='{_PURPOSE}'
        AND {document}->>'contract_id'=contract_id AND {document}->>'request_contract_id'=request_contract_id
        AND jsonb_typeof({geometry})='object' AND jsonb_typeof({scope})='object'
        AND jsonb_typeof({scope}->'profile_provenance')='object'
        AND {geometry}->>'contract_id'='{_GEOMETRY}' AND {geometry}->>'admission_purpose'='{_PURPOSE}'
        AND {scope}->>'contract_id'='{_SCOPE}' AND {scope}->>'operation_sha256' ~ '^[0-9a-f]{{64}}$'
        AND {_inverse_bindings()} AND {common}"""


def _values_check(previous, ordinary, initial):
    return f"({previous._recovered_preflight_check(ordinary, initial)}) OR COALESCE(({_inverse_values_check(initial)}),false)"


def _locked(ordinary, initial):
    schema = initial._ORIGINAL._schema()
    tables = tuple(initial._qt(schema, name) for name in (ordinary._TABLE, ordinary._PREFLIGHT_TABLE))
    op.execute("SET LOCAL lock_timeout='5s'")
    op.execute("SELECT pg_advisory_xact_lock(hashtextextended('provider-directory-profile-capacity-preflight:v3',0))")
    op.execute("LOCK TABLE " + ",".join(tables) + " IN ACCESS EXCLUSIVE MODE")
    return tables


def _replace(previous, ordinary, initial, tables, *, inverse):
    consumption, preflight = tables
    old_values, new_values = (
        previous._recovered_preflight_check(ordinary, initial),
        _values_check(previous, ordinary, initial),
    )
    old_purpose, new_purpose = ordinary._purpose_check(ordinary=True), _purpose_check()
    before_values, after_values = (old_values, new_values) if inverse else (new_values, old_values)
    before_purpose, after_purpose = (old_purpose, new_purpose) if inverse else (new_purpose, old_purpose)
    ordinary._replace_check(consumption, ordinary._CHECK, before_purpose, after_purpose)
    ordinary._replace_check(preflight, ordinary._PREFLIGHT_CHECK, before_values, after_values)


def upgrade():
    """Replace only two validated CHECKs; every key, guard and row keeps its identity."""
    previous, ordinary, initial = _previous()
    _replace(previous, ordinary, initial, _locked(ordinary, initial), inverse=True)


def downgrade():
    """Never erase issued or consumed inverse authority, including historical receipts."""
    previous, ordinary, initial = _previous()
    tables = _locked(ordinary, initial)
    consumption, preflight = tables
    used = (
        op.get_bind()
        .execute(
            text(f"""
        SELECT EXISTS(SELECT 1 FROM {consumption} WHERE admission_purpose=:purpose)
        OR EXISTS(SELECT 1 FROM {preflight} WHERE contract_id=:receipt OR request_contract_id=:request
          OR limits_contract_id=:limits OR materialization_mode=:purpose
          OR receipt_json::jsonb->'capacity_geometry'->>'admission_purpose'=:purpose
          OR receipt_json::jsonb->'capacity_geometry'->>'contract_id'=:geometry)
        """),
            dict(purpose=_PURPOSE, receipt=_RECEIPT, request=_REQUEST, limits=_LIMITS, geometry=_GEOMETRY),
        )
        .scalar()
    )
    if used is not False:
        raise RuntimeError("serving_inverse_capacity_history_requires_retention")
    _replace(previous, ordinary, initial, tables, inverse=False)
