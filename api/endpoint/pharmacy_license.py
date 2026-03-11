# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import datetime
from typing import Any

from sanic import Blueprint, response
from sanic.exceptions import InvalidUsage, NotFound
from sqlalchemy import func, select, text

from db.models import (PharmacyLicenseImportRun, PharmacyLicenseRecord,
                       PharmacyLicenseRecordHistory,
                       PharmacyLicenseSnapshot,
                       PharmacyLicenseStateCoverage)

blueprint = Blueprint("pharmacy_license", url_prefix="/pharmacy-license", version=1)

MAX_HISTORY_LIMIT = 200


def _get_session(request):
    session = getattr(request.ctx, "sa_session", None)
    if session is None:
        raise RuntimeError("SQLAlchemy session not available on request context")
    return session


def _parse_npi(npi: str) -> int:
    try:
        parsed = int(str(npi).strip())
    except ValueError as exc:
        raise InvalidUsage("NPI must be numeric") from exc
    if parsed <= 0:
        raise InvalidUsage("NPI must be positive")
    return parsed


def _parse_date_param(value: str | None, param_name: str) -> datetime.date | None:
    if value in (None, "", "null"):
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return datetime.date.fromisoformat(text)
    except ValueError as exc:
        raise InvalidUsage(f"Parameter '{param_name}' must be a valid ISO date (YYYY-MM-DD)") from exc


def _parse_int_param(value: str | None, param_name: str) -> int | None:
    if value in (None, "", "null"):
        return None
    try:
        return int(str(value).strip())
    except ValueError as exc:
        raise InvalidUsage(f"Parameter '{param_name}' must be an integer") from exc


def _parse_bool_param(value: str | None, default: bool = False) -> bool:
    if value in (None, "", "null"):
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on", "y"}


async def _table_exists(session, table_name: str, schema: str = "mrf") -> bool:
    result = await session.execute(
        text("SELECT to_regclass(:name)"),
        {"name": f"{schema}.{table_name}"},
    )
    value = result.scalar()
    return bool(value)


@blueprint.get("/import/status")
async def get_pharmacy_license_import_status(request):
    session = _get_session(request)
    if not await _table_exists(session, PharmacyLicenseImportRun.__tablename__):
        raise NotFound("No pharmacy-license imports found")

    run_id = request.args.get("run_id")
    run_table = PharmacyLicenseImportRun.__table__
    snapshot_table = PharmacyLicenseSnapshot.__table__

    if run_id:
        run_stmt = select(run_table).where(run_table.c.run_id == run_id)
    else:
        run_stmt = select(run_table).order_by(run_table.c.started_at.desc().nullslast()).limit(1)

    run_row = (await session.execute(run_stmt)).first()
    if run_row is None:
        raise NotFound("No pharmacy-license imports found")

    run_data = dict(run_row._mapping)
    stats_row = (
        await session.execute(
            select(
                func.count().label("snapshot_count"),
                func.coalesce(func.sum(snapshot_table.c.row_count_parsed), 0).label("parsed_rows"),
                func.coalesce(func.sum(snapshot_table.c.row_count_matched), 0).label("matched_rows"),
                func.coalesce(func.sum(snapshot_table.c.row_count_dropped), 0).label("dropped_rows"),
                func.coalesce(func.sum(snapshot_table.c.row_count_inserted), 0).label("inserted_rows"),
            ).where(snapshot_table.c.run_id == run_data["run_id"])
        )
    ).first()
    stats = stats_row._mapping if stats_row else {}

    return response.json(
        {
            "run_id": run_data.get("run_id"),
            "import_id": run_data.get("import_id"),
            "status": run_data.get("status"),
            "started_at": run_data.get("started_at").isoformat() if run_data.get("started_at") else None,
            "finished_at": run_data.get("finished_at").isoformat() if run_data.get("finished_at") else None,
            "source_summary": run_data.get("source_summary") or {},
            "error_text": run_data.get("error_text"),
            "snapshots": {
                "count": int(stats.get("snapshot_count") or 0),
                "parsed_rows": int(stats.get("parsed_rows") or 0),
                "matched_rows": int(stats.get("matched_rows") or 0),
                "dropped_rows": int(stats.get("dropped_rows") or 0),
                "inserted_rows": int(stats.get("inserted_rows") or 0),
            },
        }
    )


@blueprint.get("/coverage")
async def get_pharmacy_license_coverage(request):
    session = _get_session(request)
    if not await _table_exists(session, PharmacyLicenseStateCoverage.__tablename__):
        return response.json(
            {
                "total_states": 0,
                "supported_states": 0,
                "unsupported_states": 0,
                "items": [],
            }
        )

    table = PharmacyLicenseStateCoverage.__table__
    rows = (await session.execute(select(table).order_by(table.c.state_code.asc()))).all()

    items: list[dict[str, Any]] = []
    supported = 0
    for row in rows:
        mapping = row._mapping
        is_supported = bool(mapping.get("supported"))
        if is_supported:
            supported += 1
        items.append(
            {
                "state_code": mapping.get("state_code"),
                "state_name": mapping.get("state_name"),
                "board_url": mapping.get("board_url"),
                "source_url": mapping.get("source_url"),
                "supported": is_supported,
                "unsupported_reason": mapping.get("unsupported_reason"),
                "status": mapping.get("status"),
                "last_attempted_at": mapping.get("last_attempted_at").isoformat()
                if mapping.get("last_attempted_at")
                else None,
                "last_success_at": mapping.get("last_success_at").isoformat()
                if mapping.get("last_success_at")
                else None,
                "last_run_id": mapping.get("last_run_id"),
                "records_parsed": int(mapping.get("records_parsed") or 0),
                "records_matched": int(mapping.get("records_matched") or 0),
                "records_dropped": int(mapping.get("records_dropped") or 0),
                "records_inserted": int(mapping.get("records_inserted") or 0),
            }
        )

    total_states = len(items)
    return response.json(
        {
            "total_states": total_states,
            "supported_states": supported,
            "unsupported_states": max(total_states - supported, 0),
            "items": items,
        }
    )


@blueprint.get("/pharmacies/<npi>")
async def get_pharmacy_license_by_npi(request, npi):
    session = _get_session(request)
    parsed_npi = _parse_npi(npi)
    as_of = _parse_date_param(request.args.get("as_of"), "as_of") or datetime.date.today()
    include_history = _parse_bool_param(request.args.get("include_history"), default=True)
    history_limit = _parse_int_param(request.args.get("history_limit"), "history_limit") or 50
    history_limit = max(1, min(history_limit, MAX_HISTORY_LIMIT))

    if not await _table_exists(session, PharmacyLicenseRecord.__tablename__):
        return response.json(
            {
                "npi": parsed_npi,
                "as_of": as_of.isoformat(),
                "summary": {
                    "total_licenses": 0,
                    "active_license_count": 0,
                    "active_states": [],
                    "disciplinary_flag_any": False,
                    "latest_verified_at": None,
                },
                "licenses": [],
                "history": [],
            }
        )

    table = PharmacyLicenseRecord.__table__
    rows = (
        await session.execute(
            select(table)
            .where(table.c.npi == parsed_npi)
            .order_by(table.c.state_code.asc(), table.c.license_number.asc())
        )
    ).all()

    licenses: list[dict[str, Any]] = []
    active_states: set[str] = set()
    active_license_count = 0
    disciplinary_flag_any = False
    latest_verified: datetime.datetime | None = None

    for row in rows:
        mapping = row._mapping
        exp_date = mapping.get("license_expiration_date")
        status = str(mapping.get("license_status") or "unknown").lower()
        is_active = status == "active" and (exp_date is None or exp_date >= as_of)
        if is_active:
            active_states.add(str(mapping.get("state_code") or "").upper())
            active_license_count += 1
        if bool(mapping.get("disciplinary_flag")):
            disciplinary_flag_any = True

        verified_at = mapping.get("last_verified_at")
        if verified_at and (latest_verified is None or verified_at > latest_verified):
            latest_verified = verified_at

        licenses.append(
            {
                "state_code": mapping.get("state_code"),
                "state_name": mapping.get("state_name"),
                "board_url": mapping.get("board_url"),
                "source_url": mapping.get("source_url"),
                "license_number": mapping.get("license_number"),
                "license_type": mapping.get("license_type"),
                "license_status": status,
                "source_status_raw": mapping.get("source_status_raw"),
                "license_issue_date": mapping.get("license_issue_date").isoformat()
                if mapping.get("license_issue_date")
                else None,
                "license_effective_date": mapping.get("license_effective_date").isoformat()
                if mapping.get("license_effective_date")
                else None,
                "license_expiration_date": exp_date.isoformat() if exp_date else None,
                "last_renewal_date": mapping.get("last_renewal_date").isoformat()
                if mapping.get("last_renewal_date")
                else None,
                "disciplinary_flag": bool(mapping.get("disciplinary_flag")),
                "disciplinary_summary": mapping.get("disciplinary_summary"),
                "disciplinary_action_date": mapping.get("disciplinary_action_date").isoformat()
                if mapping.get("disciplinary_action_date")
                else None,
                "entity_name": mapping.get("entity_name"),
                "dba_name": mapping.get("dba_name"),
                "address_line1": mapping.get("address_line1"),
                "address_line2": mapping.get("address_line2"),
                "city": mapping.get("city"),
                "state": mapping.get("state"),
                "zip_code": mapping.get("zip_code"),
                "phone_number": mapping.get("phone_number"),
                "source_record_id": mapping.get("source_record_id"),
                "last_snapshot_id": mapping.get("last_snapshot_id"),
                "first_seen_at": mapping.get("first_seen_at").isoformat() if mapping.get("first_seen_at") else None,
                "last_seen_at": mapping.get("last_seen_at").isoformat() if mapping.get("last_seen_at") else None,
                "last_verified_at": verified_at.isoformat() if verified_at else None,
                "active_as_of": is_active,
            }
        )

    history: list[dict[str, Any]] = []
    if include_history and await _table_exists(session, PharmacyLicenseRecordHistory.__tablename__):
        hist_table = PharmacyLicenseRecordHistory.__table__
        hist_rows = (
            await session.execute(
                select(hist_table)
                .where(hist_table.c.npi == parsed_npi)
                .order_by(hist_table.c.imported_at.desc().nullslast(), hist_table.c.history_id.desc())
                .limit(history_limit)
            )
        ).all()
        for row in hist_rows:
            mapping = row._mapping
            history.append(
                {
                    "snapshot_id": mapping.get("snapshot_id"),
                    "run_id": mapping.get("run_id"),
                    "state_code": mapping.get("state_code"),
                    "license_number": mapping.get("license_number"),
                    "license_status": mapping.get("license_status"),
                    "source_status_raw": mapping.get("source_status_raw"),
                    "disciplinary_flag": bool(mapping.get("disciplinary_flag")),
                    "disciplinary_summary": mapping.get("disciplinary_summary"),
                    "license_expiration_date": mapping.get("license_expiration_date").isoformat()
                    if mapping.get("license_expiration_date")
                    else None,
                    "imported_at": mapping.get("imported_at").isoformat() if mapping.get("imported_at") else None,
                }
            )

    return response.json(
        {
            "npi": parsed_npi,
            "as_of": as_of.isoformat(),
            "summary": {
                "total_licenses": len(licenses),
                "active_license_count": active_license_count,
                "active_states": sorted(state for state in active_states if state),
                "disciplinary_flag_any": disciplinary_flag_any,
                "latest_verified_at": latest_verified.isoformat() if latest_verified else None,
            },
            "licenses": licenses,
            "history": history,
        }
    )
