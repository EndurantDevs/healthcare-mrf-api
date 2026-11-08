# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Signed tuple admission happens inside the independently owned billing Reader."""

from types import SimpleNamespace

import pytest
from sqlalchemy import text

from api import billing_search_http as native_http
from api import custom_import_billing_http as billing
from api import custom_import_read_http as transport
from api import signed_request_reader
from db.connection import db
from tests import test_billing_search_endpoint_access as signature
from tests import test_custom_import_read_http as import_signature
from tests.custom_import_billing_support import signed_request
from tests.signed_request_reader_postgres_support import native_reader_witness
from tests.test_plan_release_read_admission import DIGEST, RELEASE, REVISION
from tests.test_plan_release_read_admission_postgres import native_admission


@pytest.mark.asyncio
async def test_signed_billing_tuple_is_checked_before_payload_in_genuine_reader(native_admission, monkeypatch):
    owner, schema, reader_role = native_admission
    monkeypatch.setenv("HLTHPRT_API_READER_ENABLED", "false")
    monkeypatch.setattr(signed_request_reader, "db", db)
    monkeypatch.setattr(native_http, "_transport_keyring", signature._keyring)
    monkeypatch.setattr(native_http, "_cursor_keyring", lambda: object())
    monkeypatch.setenv(transport.CUSTOM_IMPORT_READ_TRANSPORT_KEYRING_ENV, import_signature._keyring_document())
    monkeypatch.setattr(transport, "_trusted_now", lambda: signature.TRUSTED_NOW)
    payload_calls = []

    async def payload(session, *_arguments):
        payload_calls.append(await native_reader_witness(session, reader_role))
        assert await session.scalar(text(f"SELECT payload FROM {schema}.shared_payload")) == "old"
        return transport._canonical_json_bytes({"items": []})

    monkeypatch.setattr(billing, "_read_billing_payload", payload)
    await db.connect()
    async with db.session_factory() as ordinary:

        def request(revision=None):
            query_by_name = {"plan_release_id": RELEASE}
            if revision is not None:
                query_by_name.update(plan_release_serving_revision_id=revision, plan_release_binding_set_digest=DIGEST)
            result = signed_request(native_changes=query_by_name)
            result.ctx = SimpleNamespace(sa_session=ordinary, session=ordinary)
            return result

        stale = await billing.serve_custom_import_billing_search(request("hpserve_" + "2" * 26), ordinary)
        assert stale.status == 409 and payload_calls == []
        assert stale.headers["Cache-Control"] == "private, no-store"
        accepted = await billing.serve_custom_import_billing_search(request(REVISION), ordinary)
        assert accepted.status == 200 and len(payload_calls) == 1
        legacy = await billing.serve_custom_import_billing_search(request(), ordinary)
        assert legacy.status == 200 and len(payload_calls) == 2
        assert not ordinary.in_transaction()
    assert not db.has_reader_session()
    assert (
        await owner.fetchval(
            "SELECT count(*) FROM pg_stat_activity WHERE usename=$1 AND xact_start IS NOT NULL", reader_role
        )
        == 0
    )
