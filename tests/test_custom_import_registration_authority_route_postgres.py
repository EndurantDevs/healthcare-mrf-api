# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Mounted registration routes commit one graph and retain exact replay."""

from __future__ import annotations

import base64
import hashlib
import json
import secrets
import uuid
from datetime import UTC, datetime, timedelta

import httpx
import pytest
from sanic import Sanic
from sqlalchemy import func, select

from api.control_registration_authority import blueprint
from db.models.custom_import import CustomImportDataset, CustomImportRegistrationAuthority
from process.custom_import.definition import canonical_json
from process.custom_import.registration_authority import get_registration_authority
from process.custom_import.registration_authority_operator import register_authority
from tests.custom_import_postgres_support import isolated_publication_case
from tests.test_custom_import_registration_authority_operator import (
    _rewrite_authority,
    mounted_case,
)
from tests.test_custom_import_registration_authority_postgres import _registration


def _mounted_client(publication_case):
    """Bind one disposable database session to the mounted control blueprint."""

    app = Sanic(f"test_registration_authority_{uuid.uuid4().hex}")
    app.blueprint(blueprint)

    @app.middleware("request")
    async def bind_session(request):
        request.ctx.sa_session = publication_case.sessions()

    @app.middleware("response")
    async def close_session(request, _response):
        await request.ctx.sa_session.close()

    return app.asgi_client


@pytest.mark.asyncio
@pytest.mark.parametrize("connector", (None, "https", "s3", "upload"))
async def test_mounted_registration_authority_commits_and_replays_exact_graph(monkeypatch, connector):
    """Exercise mounted HTTP auth, store transaction, and committed readback."""

    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "synthetic-control-token")
    registration = _registration(connector=connector)
    authority_id = secrets.token_hex(32)
    token = secrets.token_bytes(32)
    bearer = base64.urlsafe_b64encode(token).decode("ascii").rstrip("=")
    path = f"/control/v1/custom-import/registration-authorities/{authority_id}"

    async with isolated_publication_case() as case:
        async with case.engine.begin() as connection:
            await connection.run_sync(CustomImportRegistrationAuthority.__table__.create)

        client = _mounted_client(case)
        broad_headers_by_name = {"Authorization": "Bearer synthetic-control-token"}
        capability_headers_by_name = {"Authorization": f"Bearer {bearer}"}
        mint_document_by_name = {
            "registration": registration,
            "expires_at": (datetime.now(UTC) + timedelta(minutes=3)).isoformat(),
            "token_sha256": hashlib.sha256(token).hexdigest(),
        }
        _, mint = await client.put(path, data=json.dumps(mint_document_by_name).encode(), headers=broad_headers_by_name)
        assert mint.status == 200
        assert mint.headers["Cache-Control"] == "no-store"
        assert mint.json["input_sha256"] == hashlib.sha256(canonical_json(registration).encode()).hexdigest()
        assert mint.json["result"] is None

        registration_body = json.dumps(registration).encode()
        _, broad_denied = await client.post(path + "/register", data=registration_body, headers=broad_headers_by_name)
        wrong_bearer = base64.urlsafe_b64encode(secrets.token_bytes(32)).decode("ascii").rstrip("=")
        _, wrong_denied = await client.post(
            path + "/register",
            data=registration_body,
            headers={"Authorization": f"Bearer {wrong_bearer}"},
        )
        assert broad_denied.status == wrong_denied.status == 403
        async with case.sessions() as session:
            assert await session.scalar(select(func.count()).select_from(CustomImportDataset)) == 0

        _, registered = await client.post(
            path + "/register", data=registration_body, headers=capability_headers_by_name
        )
        assert registered.status == 200
        assert registered.headers["Cache-Control"] == "no-store"
        assert registered.json["status"] == "registered"
        async with case.sessions() as session:
            committed = await get_registration_authority(session, authority_id)
            assert committed is not None and committed.result == registered.json
            assert await session.scalar(select(func.count()).select_from(CustomImportDataset)) == 1

        _, revoked = await client.post(path + "/revoke", data=b"{}", headers=broad_headers_by_name)
        assert revoked.status == 200 and revoked.json["revoked_at"] is not None
        _, replay = await client.post(path + "/register", data=registration_body, headers=capability_headers_by_name)
        assert replay.status == 200 and replay.json == registered.json
        async with case.sessions() as session:
            assert await session.scalar(select(func.count()).select_from(CustomImportDataset)) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("mounted_case", ("https", "s3", "upload"), indirect=True)
async def test_scoped_operator_recovers_committed_result_after_lost_ack(mounted_case, monkeypatch):
    """The unchanged scoped transport can recover a committed generic graph."""

    case = mounted_case
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "synthetic-control-token")
    authority_id = case.authority["authority_id"]
    path = f"/control/v1/custom-import/registration-authorities/{authority_id}"
    async with isolated_publication_case() as publication:
        async with publication.engine.begin() as connection:
            await connection.run_sync(CustomImportRegistrationAuthority.__table__.create)
        client = _mounted_client(publication)
        _, minted = await client.put(
            path,
            json={
                "registration": case.registration,
                "expires_at": case.authority["expires_at"],
                "token_sha256": case.authority["token_sha256"],
            },
            headers={"Authorization": "Bearer synthetic-control-token"},
        )
        assert minted.status == 200
        responses = []

        async def handler(request):
            assert request.content == canonical_json(case.registration).encode("utf-8")
            _, reply = await client.post(request.url.path, data=request.content, headers=dict(request.headers))
            assert reply.status == 200
            responses.append(reply.json)
            if len(responses) == 1:
                raise httpx.ReadError("synthetic lost acknowledgement")
            return httpx.Response(200, json=reply.json, headers={"Cache-Control": "no-store"})

        transport = httpx.MockTransport(handler)
        with pytest.raises(ValueError, match="^registration authority is unavailable$"):
            await register_authority(case.dataset_key, case.definition, case.binding, transport=transport)
        _, revoked = await client.post(
            path + "/revoke", data=b"{}", headers={"Authorization": "Bearer synthetic-control-token"}
        )
        assert revoked.status == 200
        _rewrite_authority(case, document={**case.authority, "expires_at": "2020-01-01T00:00:00Z"})
        rendered = await register_authority(case.dataset_key, case.definition, case.binding, transport=transport)
        assert json.loads(rendered) == responses[0] == responses[1]
        async with publication.sessions() as session:
            retained = await get_registration_authority(session, authority_id)
            assert retained.result == responses[0]
            assert await session.scalar(select(func.count()).select_from(CustomImportDataset)) == 1
