# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Loopback receiver with real transport/cursors and synthetic database rows."""

from __future__ import annotations

import asyncio
import dataclasses
import json
import sys
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from types import SimpleNamespace


def _guard(event, args):
    if event == "socket.connect":
        raise RuntimeError("receiver fixture cannot connect to services")
    if event == "socket.bind" and args[1][0] != "127.0.0.1":
        raise RuntimeError("receiver fixture must bind loopback")
    if event == "open" and isinstance(args[0], str):
        name = Path(args[0]).name
        if name == ".env" or name.startswith(".env."):
            raise RuntimeError("receiver fixture cannot read environment files")


sys.addaudithook(_guard)
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from api import custom_import_provider_geo as geo
from api import custom_import_read_http as transport
from process.custom_import import publication, read_core
from process.custom_import.definition import CustomImportDefinition, canonical_json, canonical_sha256
from tests.test_custom_import_provider_geo_sql import _geo_row
from tests.test_custom_import_provider_http import _Session

_document = (Path(__file__).parent / "custom_import/v1_valid.json").read_text()
_document = _document.replace('"service_code"', '"region"').replace('"amount"', '"score"')
_document = json.loads(_document.replace('"decimal"', '"integer"'))
_document["query"]["aliases"] = {"region_alias": "region", "score_alias": "score"}
_document["query"]["sortable_fields"] = ["score"]
_DEFINITION = CustomImportDefinition.from_mapping(_document)
_NPIS = ("1000000012", "1000000020", "1000000038")
_EVENTS = []
_FIXTURE_STATE = SimpleNamespace(
    request_event_by_name=None, cursor_time_base=datetime.now(timezone.utc), clock_offset=0
)
geo.MAX_CURSOR_TTL_SECONDS = 5


def _result(rows):
    scalars = [row[0] for row in rows]
    return SimpleNamespace(
        all=lambda: rows,
        one_or_none=lambda: rows[0] if rows else None,
        scalar_one_or_none=lambda: scalars[0] if scalars else None,
        scalars=lambda: SimpleNamespace(all=lambda: scalars, first=lambda: scalars[0] if scalars else None),
    )


class SyntheticSession(_Session):
    """Return persisted metadata and row shapes; leave read algorithms intact."""

    def __init__(self, target):
        super().__init__()
        self.target = target

    async def execute(self, statement, parameters=None):
        """Implement the session protocol with synthetic persisted result rows."""
        query_sql = str(statement)
        if "current_setting('statement_timeout')" in query_sql:
            return SimpleNamespace(one_or_none=lambda: SimpleNamespace(timeout_text="0", timeout_milliseconds=0))
        if query_sql == "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY":
            return _result([])
        if "SELECT COUNT(*) AS anchor_count" in query_sql:
            return SimpleNamespace(all=lambda: [SimpleNamespace(_mapping={"anchor_count": 1})])
        if "SELECT COUNT(*) AS total_count" in query_sql:
            return SimpleNamespace(all=lambda: [SimpleNamespace(_mapping={"total_count": len(_NPIS)})])
        if "page_geo AS MATERIALIZED" in query_sql:
            _FIXTURE_STATE.request_event_by_name["native_page"] = True
            anchor = parameters.get("__custom_import_geo_cursor_npi")
            start = 0 if anchor is None else _NPIS.index(str(anchor)) + 1
            rows = [
                _geo_row(int(npi), f"00000000-0000-4000-8000-{index + 1:012d}", 10.0)
                for index, npi in enumerate(_NPIS)
                if index >= start
            ]
            return SimpleNamespace(all=lambda: rows[: parameters["__custom_import_geo_page_limit"]])

        return self._persisted_result_rows(statement)

    def _persisted_result_rows(self, statement):
        """Return synthetic metadata or identity rows for the requested query."""
        column_names = tuple(column["name"] for column in statement.column_descriptions)
        if column_names == ("set_config",):
            return _result([(None,)])
        if column_names == ("dataset_id",):
            return _result([(11,)])
        if column_names == ("generation_id",):
            return _result([(self.target["generation_id"],)])
        if column_names == ("CustomImportPublicationEvent",):
            return _result([(self._publication_event_row(),)])
        if column_names == ("CustomImportDefinitionRevision", "CustomImportSchemaRevision"):
            definition = SimpleNamespace(
                contract_version="custom-import/v1",
                revision_number=1,
                canonical_definition=_DEFINITION.canonical,
                definition_sha256=bytes.fromhex(_DEFINITION.digest),
            )
            schema = SimpleNamespace(
                revision_number=1,
                canonical_schema=_DEFINITION.schema_canonical,
                schema_sha256=bytes.fromhex(_DEFINITION.schema_digest),
            )
            return _result([(definition, schema)])
        if column_names == ("CustomImportChildCollection",):
            return _result([(SimpleNamespace(collection_name="rates", collection_slot=1),)])
        if column_names == ("CustomImportSelectionProfile",):
            profile = read_core._profile_document(_DEFINITION.selection_profiles[0], "rates")
            profile_row = SimpleNamespace(
                profile_id="default",
                profile_slot=1,
                context_collection_slot=1,
                canonical_profile=canonical_json(profile),
                profile_sha256=bytes.fromhex(canonical_sha256(profile, domain="profile")),
            )
            return _result([(profile_row,)])
        if column_names == ("CustomImportField",):
            return _result(
                [
                    (
                        SimpleNamespace(
                            field_name=field.field_id,
                            field_slot=field.field_slot,
                            collection_slot=0 if field.collection is None else 1,
                            field_type=field.value_type,
                            is_nullable=field.nullable,
                            projection_slot=field.projection_slot or 0,
                        ),
                    )
                    for field in _DEFINITION.fields
                ]
            )
        return self._hydration_result_rows(statement, column_names)

    def _hydration_result_rows(self, statement, column_names):
        """Provide distinct persisted identities and field values for each NPI."""
        if column_names[-1:] == ("canonical_value",):
            _FIXTURE_STATE.request_event_by_name["hydration"] = True
            requested_npis = statement.compile().params["canonical_value_1"]
            return _result(
                [
                    (
                        SimpleNamespace(entity_binding_id=index + 1, context_key_sha256=bytes([index + 1]) * 32),
                        SimpleNamespace(root_record_id=index + 1, family_revision_id=101 + index, child_count=1),
                        SimpleNamespace(root_revision_id=201 + index),
                        SimpleNamespace(child_revision_id=301 + index),
                        npi,
                    )
                    for index, npi in enumerate(_NPIS)
                    if npi in requested_npis
                ]
            )
        if column_names == ("CustomImportFamilyChild", "CustomImportChildRevision"):
            return self._family_membership_rows(statement)
        if column_names == ("CustomImportRootScalar",):
            requested_ids = statement.compile().params["root_revision_id_1"]
            return _result(
                [
                    (
                        SimpleNamespace(
                            root_revision_id=201 + index,
                            field_slot=slot,
                            field_type="string",
                            value_state="value",
                            string_value=scalar_text_value,
                        ),
                    )
                    for index, npi in enumerate(_NPIS)
                    if 201 + index in requested_ids
                    for slot, scalar_text_value in ((1, npi), (2, "Synthetic Provider"))
                ]
            )
        if column_names == ("CustomImportChildScalar",):
            requested_ids = statement.compile().params["child_revision_id_1"]
            return _result(
                [
                    (
                        SimpleNamespace(
                            child_revision_id=301 + index,
                            field_slot=slot,
                            field_type=field_type,
                            value_state="value",
                            **scalar_value,
                        ),
                    )
                    for index, _npi in enumerate(_NPIS)
                    if 301 + index in requested_ids
                    for slot, field_type, scalar_value in (
                        (4, "string", {"string_value": "north"}),
                        (5, "integer", {"integer_value": 9}),
                    )
                ]
            )
        raise AssertionError(f"unexpected synthetic query columns: {column_names}")

    def _family_membership_rows(self, statement):
        """Keep synthetic full-family rows inside the exact requested page."""
        requested_families = statement.compile().params["param_1"]
        return _result(
            [
                (
                    SimpleNamespace(family_revision_id=101 + index, collection_slot=1),
                    SimpleNamespace(child_revision_id=301 + index),
                )
                for index, _npi in enumerate(_NPIS)
                if (101 + index, index + 1) in requested_families
            ]
        )

    def _publication_event_row(self):
        details = publication._PublicationEventDetails(
            dataset_id=11,
            definition_revision_id=self.target["definition_revision_id"],
            schema_revision_id=self.target["schema_revision_id"],
            execution_id=41,
            event_kind="activated",
            from_generation_id=None,
            to_generation_id=self.target["generation_id"],
            expected_pointer_version=0,
            committed_pointer_version=1,
        )
        canonical, digest = publication._event_document(details)
        return SimpleNamespace(
            **dataclasses.asdict(details),
            finality_contract=publication.FINALITY_EVENT_CONTRACT,
            canonical_event=canonical,
            event_sha256=digest,
        )


_verify_transport = transport._verify_provider_transport
_open_cursor = geo.open_geo_cursor
_issue_cursor = geo.issue_geo_cursor
_read_payload = geo._read_geo_payload


def _verify(**kwargs):
    verified = _verify_transport(**kwargs)
    _FIXTURE_STATE.request_event_by_name["transport_verified"] = True
    _FIXTURE_STATE.request_event_by_name["verified_scope"] = verified.scope
    return verified


def _open(token, **kwargs):
    _FIXTURE_STATE.request_event_by_name["cursor_attempt"] = True
    _FIXTURE_STATE.request_event_by_name["query_fingerprint"] = kwargs["query_fingerprint"]
    _FIXTURE_STATE.request_event_by_name["cursor_scope"] = kwargs["authorization_scope_sha256"]
    try:
        state = _open_cursor(token, **kwargs)
    except Exception as error:
        _FIXTURE_STATE.request_event_by_name["cursor_error"] = type(error).__name__
        raise
    _FIXTURE_STATE.request_event_by_name["opened"] = dataclasses.asdict(state)
    return state


def _issue(state, **kwargs):
    token = _issue_cursor(state, **kwargs)
    _FIXTURE_STATE.request_event_by_name["issued"] = dataclasses.asdict(state)
    return token


async def _read_geo_payload(request, session, parsed, verified, cursor_secret, _trusted_now):
    """Pass only deterministic cursor time to the unchanged reader."""

    cursor_time = (_FIXTURE_STATE.cursor_time_base + timedelta(seconds=_FIXTURE_STATE.clock_offset)).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    return await _read_payload(request, session, parsed, verified, cursor_secret, cursor_time)


transport._verify_provider_transport = _verify
geo.open_geo_cursor = _open
geo.issue_geo_cursor = _issue
geo._read_geo_payload = _read_geo_payload


class CustomImportGeoFixtureHandler(BaseHTTPRequestHandler):
    def log_message(self, *_args):
        return None

    def do_GET(self):
        if self.path != "/__fixture__/events":
            self.send_error(404)
            return
        self._reply(200, json.dumps(_EVENTS).encode())

    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        if not 0 < length <= 128 * 1024:
            self.send_error(400)
            return
        body = self.rfile.read(length)
        if self.path == "/__fixture__/clock":
            offset = json.loads(body)["offset_seconds"]
            assert offset in (0, 6)
            _FIXTURE_STATE.clock_offset = offset
            self._reply(200, b"{}")
            return
        _FIXTURE_STATE.request_event_by_name = {
            "transport_verified": False,
            "cursor_attempt": False,
            "native_page": False,
            "hydration": False,
        }
        session = SyntheticSession(json.loads(body)["target"])
        request = SimpleNamespace(
            body=body,
            headers=dict(self.headers),
            method="POST",
            path=self.path,
            query_string="",
            args={},
            ctx=SimpleNamespace(sa_session=session),
        )
        reply = asyncio.run(geo.serve_custom_import_provider_geo(request, session))
        _FIXTURE_STATE.request_event_by_name["status"] = reply.status
        _EVENTS.append(_FIXTURE_STATE.request_event_by_name)
        self._reply(reply.status, reply.body)

    def _reply(self, status, body):
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "private, no-store")
        self.end_headers()
        self.wfile.write(body)


if __name__ == "__main__":
    with HTTPServer(("127.0.0.1", 0), CustomImportGeoFixtureHandler) as server:
        print(f"http://127.0.0.1:{server.server_port}", flush=True)
        server.serve_forever()
