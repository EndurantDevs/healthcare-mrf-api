# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Source resolution and retained-impact preview each own their isolation level."""

from types import SimpleNamespace

import pytest

from api.endpoint import registry_publication as publication
from process import registry_source_binding_publication as source_batches


class PreviewSession:
    def __init__(self):
        self.transaction_number = 0
        self.isolation = None
        self.statements = []

    def begin(self):
        session = self

        class Transaction:
            async def __aenter__(self):
                session.transaction_number += 1
                session.isolation = "read committed"

            async def __aexit__(self, *_):
                session.isolation = None

        return Transaction()

    async def execute(self, statement):
        sql = str(statement)
        self.statements.append((self.transaction_number, sql))
        if "REPEATABLE READ" in sql:
            self.isolation = "repeatable read"


def _preview_inputs(session, configured):
    service = object() if configured else None
    request = SimpleNamespace(
        ctx=SimpleNamespace(sa_session=session),
        app=SimpleNamespace(ctx=SimpleNamespace(registry_ptg_scope_engine=service)),
    )
    compact_by_field = {
        "operation": "publish_source_binding_batch",
        "expected_serving_generation": 1,
        "selection_count": 178,
        "selection_sha256": "a" * 64,
    }
    return request, service, compact_by_field


@pytest.mark.asyncio
@pytest.mark.parametrize("configured", [False, True])
async def test_source_batch_preview_isolation_is_owned_by_each_transaction(monkeypatch, configured):
    """Both preview paths retain their own source snapshot and exact selected identity."""
    session = PreviewSession()
    request, service, compact_by_field = _preview_inputs(session, configured)
    document, actor, command = object(), object(), object()
    preview_calls = []

    async def driver(actual_session):
        assert actual_session is session
        return session

    async def resolve(actual_driver, actual_document, actual_actor, *, allow_unbound):
        assert (actual_driver, actual_document, actual_actor) == (session, document, actor)
        assert allow_unbound is True
        assert session.transaction_number == 1 and session.isolation == "repeatable read"
        return compact_by_field, command

    async def checked(actual_driver, actual_command, actual_actor, head, *, summary_only):
        assert (actual_driver, actual_command, actual_actor, head) == (session, command, actor, 1)
        assert summary_only is True
        assert session.transaction_number == 2 and session.isolation == "repeatable read"
        preview_calls.append("fallback")
        return {"selected_count": 178, "unresolved_count": 0}

    async def configured_preview(actual_request, actual_service, actual_command, actual_actor, head, *, summary_only):
        assert (actual_request, actual_service, actual_command, actual_actor, head) == (
            request,
            service,
            command,
            actor,
            1,
        )
        assert summary_only is True and session.isolation is None
        preview_calls.append("configured")
        return {"selected_count": 178, "unresolved_count": 0}

    monkeypatch.setattr(publication, "_driver", driver)
    monkeypatch.setattr(source_batches, "resolve_source_batch", resolve)
    monkeypatch.setattr(publication, "_checked_preview", checked)
    monkeypatch.setattr(publication, "_configured_preview", configured_preview)

    preview_response = await publication._source_batch_preview(request, document, actor)

    assert preview_response == {
        "selected_count": 178,
        "unresolved_count": 0,
        "operation": compact_by_field["operation"],
        "selection_count": 178,
        "selection_sha256": compact_by_field["selection_sha256"],
    }
    expected_statements = [(1, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")]
    if not configured:
        expected_statements.append((2, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
    assert session.statements == expected_statements
    assert preview_calls == ["configured" if configured else "fallback"]
