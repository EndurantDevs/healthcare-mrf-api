# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Inspect a failed native test window without changing its admission accounting."""

import asyncio
import json
import re
from decimal import Decimal
from functools import partial

from sqlalchemy import text


def wal_interval(admission, wal_before, wal_after):
    """Recover the original global insert-LSN interval from its observed counters."""
    origin = admission.initial_wal_lsn
    offset = admission.initial_wal_offset_bytes
    if (
        type(origin) is not str
        or re.fullmatch(r"[0-9A-Fa-f]{1,8}/[0-9A-Fa-f]{1,8}", origin) is None
        or any(type(value) is not int for value in (offset, wal_before, wal_after))
        or not 0 <= offset <= wal_before < wal_after
        or wal_after - wal_before > 16 * 1024 * 1024
    ):
        raise ValueError("invalid diagnostic WAL interval")
    origin_words = origin.split("/")
    first = (int(origin_words[0], 16) << 32) + int(origin_words[1], 16) + wal_before - offset
    last = first + wal_after - wal_before
    if not 0 <= first < last < 2**64:
        raise ValueError("diagnostic WAL interval outside native range")
    return {
        "start": f"{first >> 32:X}/{first & 0xFFFFFFFF:X}",
        "end": f"{last >> 32:X}/{last & 0xFFFFFFFF:X}",
    }


def failed_window_counter(error, admission, wal_before):
    """Read the original failed settlement's counter instead of sampling WAL again."""
    traceback = error.__traceback__
    while traceback is not None:
        frame = traceback.tb_frame
        if (
            frame.f_globals.get("__name__") == "process.provider_directory_profile_capacity_control_projection"
            and frame.f_code.co_name == "_settle_mutation_window"
            and frame.f_locals.get("admission") is admission
            and frame.f_locals.get("wal_before") == wal_before
        ):
            return frame.f_locals.get("wal_after")
        traceback = traceback.tb_next
    return None


async def failed_window_records(database, interval):
    """Aggregate every WAL record and block only inside the owned native test database."""
    async with asyncio.timeout(5), database.engine.begin() as connection:
        database_name = await connection.scalar(text("SELECT current_database()"))
        if re.fullmatch(r"hc_cms_admission_test_[0-9a-f]{32}", database_name) is None:
            raise ValueError("WAL inspection requires an owned native test database")
        # Install after the failed interval so extension WAL cannot enter its measurements.
        await connection.exec_driver_sql("CREATE EXTENSION IF NOT EXISTS pg_walinspect WITH SCHEMA public")
        record_totals = await connection.execute(
            text("""
                SELECT resource_manager, record_type, count(*) AS records,
                       sum(record_length)::bigint AS record_bytes,
                       sum(fpi_length)::bigint AS fpi_bytes
                FROM public.pg_get_wal_records_info(
                    CAST(CAST(:start AS text) AS pg_lsn), CAST(CAST(:end AS text) AS pg_lsn)
                )
                GROUP BY resource_manager, record_type
                ORDER BY resource_manager, record_type
                LIMIT 257
            """),
            interval,
        )
        block_totals = await connection.execute(
            text("""
                SELECT reltablespace, reldatabase, relfilenode, relforknumber,
                       count(*) AS block_references,
                       sum(block_fpi_length)::bigint AS fpi_bytes,
                       CASE WHEN reldatabase = 0 OR reldatabase = (
                           SELECT oid FROM pg_database WHERE datname = current_database()
                       ) THEN pg_filenode_relation(reltablespace, relfilenode)::text END AS relation
                FROM public.pg_get_wal_block_info(
                    CAST(CAST(:start AS text) AS pg_lsn), CAST(CAST(:end AS text) AS pg_lsn), false
                )
                GROUP BY reltablespace, reldatabase, relfilenode, relforknumber
                ORDER BY reltablespace, reldatabase, relfilenode, relforknumber
                LIMIT 513
            """),
            interval,
        )
        record_rows = record_totals.mappings().fetchmany(257)
        blocks = block_totals.mappings().fetchmany(513)
        if len(record_rows) > 256 or len(blocks) > 512:
            raise ValueError("WAL diagnostic summary exceeds its row limit")
        summary_by_field = {
            "interval": interval,
            "records": [dict(wal_record) for wal_record in record_rows],
            "blocks": [dict(wal_block) for wal_block in blocks],
        }
        if len(json.dumps(summary_by_field).encode()) > 65536:
            raise ValueError("WAL diagnostic summary exceeds its output limit")
        summary_by_field["record_links"] = await _record_links(connection, interval)
        return summary_by_field


async def _record_links(connection, interval):
    """Retain bounded metadata; native WAL decoder allocation is not governed by LIMIT."""
    if any(
        type(interval.get(name)) is not str
        or re.fullmatch(r"[0-9A-Fa-f]{1,8}/[0-9A-Fa-f]{1,8}", interval[name]) is None
        for name in ("start", "end")
    ):
        raise ValueError("invalid diagnostic WAL interval")
    words = [endpoint_lsn.split("/") for endpoint_lsn in (interval["start"], interval["end"])]
    span = (int(words[1][0], 16) << 32) + int(words[1][1], 16) - (int(words[0][0], 16) << 32) - int(words[0][1], 16)
    if not 0 < span <= 1024 * 1024:
        return {"status": "interval_limit_exceeded", "records": [], "blocks": []}
    fully_flushed = await connection.scalar(
        text("SELECT pg_catalog.pg_current_wal_flush_lsn() >= CAST(CAST(:end AS text) AS pg_lsn)"), interval
    )
    if fully_flushed is not True:
        return {"status": "end_not_flushed", "records": [], "blocks": []}
    linked_rows = []
    for fields, function, order, limit in (
        (
            "start_lsn::text,end_lsn::text,xid::text::bigint AS xid,"
            "left(resource_manager,32) AS resource_manager,left(record_type,32) AS record_type,"
            "record_length,main_data_length,fpi_length",
            "pg_get_wal_records_info",
            "start_lsn",
            256,
        ),
        (
            "start_lsn::text,end_lsn::text,xid::text::bigint AS xid,block_id,"
            "reltablespace,reldatabase,relfilenode,relforknumber,relblocknumber,"
            "block_data_length,block_fpi_length",
            "pg_get_wal_block_info",
            "start_lsn,block_id",
            512,
        ),
    ):
        arguments = ",false" if function == "pg_get_wal_block_info" else ""
        inspection_result = await connection.execute(
            text(
                f"SELECT {fields} FROM public.{function}("
                "CAST(CAST(:start AS text) AS pg_lsn),CAST(CAST(:end AS text) AS pg_lsn)"
                f"{arguments}) AS wal ORDER BY wal.{order.replace(',', ',wal.')} LIMIT {limit + 1}"
            ),
            interval,
        )
        mappings = inspection_result.mappings().fetchmany(limit + 1)
        if len(mappings) > limit:
            return {"status": "row_limit_exceeded", "records": [], "blocks": []}
        linked_rows.append([dict(wal_row) for wal_row in mappings])
    # start_lsn binds the two sets; records without blocks, including COMMIT, remain.
    return {
        "status": "complete",
        "records": linked_rows[0],
        "blocks": linked_rows[1],
        "client_row_limits": {"records": 256, "blocks": 512},
        "native_decoder_allocation_bounded_by_row_limits": False,
        "accounting_authority": False,
    }


def _owner_phase_metadata(fhir, measurement):
    """Detach validated native counters from one original measurement phase."""
    if not isinstance(measurement, fhir.BackendWalDiagnosticResult):
        return None
    wal_bytes = measurement.wal_bytes_delta
    if not (
        type(wal_bytes) is Decimal
        and wal_bytes.is_finite()
        and 0 <= wal_bytes <= 2**64 - 1
        and wal_bytes == wal_bytes.to_integral_value()
    ):
        raise ValueError("native byte counter unavailable")
    for count in (
        measurement.baseline.identity.pid,
        measurement.baseline.identity.database_oid,
        measurement.wal_records_delta,
        measurement.wal_fpi_delta,
        measurement.global_insert_lsn_span,
    ):
        if type(count) is not int or not 0 <= count <= 2**64 - 1:
            raise ValueError("native counter unavailable")
    for snapshot in (measurement.baseline, measurement.final):
        if (
            type(snapshot.insert_lsn) is not str
            or re.fullmatch(r"[0-9A-F]{1,8}/[0-9A-F]{1,8}", snapshot.insert_lsn) is None
        ):
            raise ValueError("native interval unavailable")
    return {
        "pid": measurement.baseline.identity.pid,
        "database_oid": measurement.baseline.identity.database_oid,
        "same_identity": measurement.baseline.identity == measurement.final.identity,
        "start_lsn": measurement.baseline.insert_lsn,
        "end_lsn": measurement.final.insert_lsn,
        "wal_records": measurement.wal_records_delta,
        "wal_fpi": measurement.wal_fpi_delta,
        "wal_bytes": int(wal_bytes),
        "global_insert_lsn_span": measurement.global_insert_lsn_span,
    }


def _retain_owner_metadata(
    fhir, entries, seen, family, outcome, capture=None, *, custody_verified=None, coordinate=None
):
    """Retain each original owner once within the fixed metadata bound."""
    if not isinstance(outcome, fhir.profile_owned_wal.OwnedWalTransaction):
        return
    if id(outcome) in seen:
        return
    if len(entries) == 32:
        raise ValueError("native owner diagnostic limit exceeded")
    seen.add(id(outcome))
    callback_by_field = None
    if type(capture) is dict and len(capture) <= 32:
        callback_by_field = {
            name: capture.get(name)
            for name in ("committed", "cleanup_complete", "accounting_authority", "reservation_refund")
            if type(capture.get(name)) is bool
        }
        callback_by_field["measurement_complete"] = capture.get("measurement_status") == "complete"
    entries.append(
        {
            "family": family,
            "current_custody_verified": custody_verified,
            "coordinate": coordinate if type(coordinate) is int or coordinate == "stage" else None,
            "commit_state": outcome.commit_state
            if outcome.commit_state in {"not_attempted", "attempted", "confirmed", "uncertain", "rolled_back"}
            else "unknown",
            "original_status": outcome.status
            if outcome.status
            in {
                "accounting_incomplete",
                "committed_measured",
                "committed_accounting_incomplete",
                "commit_uncertain_accounting_incomplete",
            }
            else "unknown",
            "cleanup_complete": outcome.cleanup_complete is True,
            "body": _owner_phase_metadata(fhir, outcome.measurement),
            "before_setup_after_restoration": _owner_phase_metadata(fhir, outcome.owner_measurement),
            "original_callback": callback_by_field,
            "whole_owner_complete": outcome.owner_measurement_complete is True,
        }
    )


def _retain_preflight_member(fhir, admission, current, wave, member, compact, family, retain):
    """Use the original reader capture; unavailable custody stays diagnostic."""
    capture = member.get("capture")
    coordinate = member.get("coordinate")
    is_current = (
        wave.get("status") == "complete"
        and type(capture) is dict
        and capture.get("native_outcome") is member.get("outcome")
        and (coordinate == "stage" or any(number == coordinate for number, _ in wave.get("coordinates", ())))
        and fhir._is_native_preflight_reader_complete(
            coordinate, member.get("task"), capture, admission, current, compact
        )
    )
    retain(family, member.get("outcome"), custody_verified=is_current, coordinate=coordinate)


def _retain_native_wave(fhir, admission, current, wave, compact, family, retain):
    """Keep only the current bounded original wave and its checked members."""
    if (
        wave.get("admission") is not admission
        or wave.get("window") is not current
        or current[1] != ("profile_stage" if compact else "evidence_stage")
    ):
        return
    members = wave.get("readers", wave.get("workers", []))
    coordinate_pairs = wave.get("coordinates", ())
    if type(members) is not list or len(members) > 32 or len(coordinate_pairs) > 32:
        raise ValueError("native member diagnostic limit exceeded")
    coordinates_by_key = dict(coordinate_pairs)
    for member in members:
        borrowed = member.get("owner")
        if borrowed is None:
            _retain_preflight_member(fhir, admission, current, wave, member, compact, family, retain)
            continue
        validator = fhir._is_current_compact_worker if compact else fhir._is_current_evidence_worker
        if (
            type(borrowed) is dict
            and member.get("coordinate") in coordinates_by_key
            and validator(
                borrowed,
                admission,
                current,
                member.get("task"),
                wave.get("build"),
                (member.get("coordinate"), coordinates_by_key[member.get("coordinate")]),
            )
        ):
            retain(
                family,
                borrowed.get("outcome"),
                borrowed.get("capture"),
                custody_verified=True,
                coordinate=member.get("coordinate"),
            )


def _retain_wave_owners(fhir, admission, current, tracker, retain):
    """Select evidence and compact owners without changing settlement authority."""
    for family, field, compact in (
        ("preflight", "owned_evidence_preflight_native_outcomes", False),
        ("payload", "owned_evidence_wave_native_outcomes", False),
        ("compact_preflight", "owned_compact_preflight_native_outcomes", True),
        ("compact_payload", "owned_compact_wave_native_outcomes", True),
    ):
        waves = getattr(tracker, field, [])
        if type(waves) is not list or len(waves) > 1:
            raise ValueError("native wave diagnostic limit exceeded")
        for wave in waves:
            _retain_native_wave(fhir, admission, current, wave, compact, family, retain)


def failed_window_owners(fhir, admission, owner, relation_name):
    """Detach only fixed fields from current original custody, never invoke a callback."""
    current = fhir._PROFILE_CAPACITY_MUTATION_WINDOW.get()
    if not (type(current) is tuple and len(current) == 2 and current[0] is owner and current[1] == relation_name):
        raise ValueError("original window custody unavailable")
    entries, seen = [], set()
    tracker = admission.wal_tracker
    retain = partial(_retain_owner_metadata, fhir, entries, seen)
    _retain_wave_owners(fhir, admission, current, tracker, retain)
    groups = getattr(tracker, "owned_control_transaction_groups", [])
    if type(groups) is not list or len(groups) > 128:
        raise ValueError("native control diagnostic limit exceeded")
    for group in groups:
        if (
            group.get("admission") is admission
            and group.get("window") is current
            and group.get("original_outcome") is group.get("outcome")
            and group.get("identity") is group.get("original_identity")
        ):
            retain("control", group.get("original_outcome"))
    return {
        "owners": entries,
        "owner_limit": 32,
        "parent_coverage_complete": False,
        "accounting_authority": False,
        "unknown_phases": [
            "checkout_and_first_sampler",
            "final_sampler_self_tail",
            "release_or_reset",
            "unterminated_maintenance",
            "uncovered_readers",
        ],
    }
