# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed bootstrap configuration and untrusted notification refusal controls."""

import hashlib
import json
from dataclasses import replace

import pytest

from process.network_registry_cms_source_attempt import RegistryCMSSourceAttempt
from process.registry_cms_source_notifications import (
    RegistryCMSSourceNotification,
    RegistryCMSSourceNotificationStore,
    decode_registry_cms_source_notification,
)


def _attempt():
    return RegistryCMSSourceAttempt("source-run", "source-run:" + "a" * 32, "2026-01-01T00:00:00+00:00")


@pytest.mark.parametrize(
    "changes",
    [
        {"owner_role": "publisher"},
        {"reader_roles": ["reader"]},
        {"reader_roles": ("reader", "reader")},
        {"reader_roles": ("protected_owner",)},
        {"source_attempt": None},
        {"source_attempt": {}},
        {"control_schema": "invalid-name"},
        {"publisher_role": "invalid role"},
    ],
)
def test_notification_store_refuses_unclosed_bootstrap_configuration(changes):
    store = RegistryCMSSourceNotificationStore("registry_control", "protected_owner", "publisher", _attempt())
    with pytest.raises(ValueError):
        replace(store, **changes)


@pytest.mark.parametrize(
    "encoded",
    [
        "{}",
        "[]",
        '{"contract":"x","contract":"y"}',
        '{"contract":"registry-cms-source-notification/v1","pair":{}}',
        "x" * (70 * 1024 + 1),
    ],
)
def test_untrusted_notification_requires_closed_pair_and_bounded_canonical_bytes(encoded):
    digest = hashlib.sha256(encoded.encode()).hexdigest()
    with pytest.raises(ValueError, match="^registry_cms_source_notification_invalid$"):
        decode_registry_cms_source_notification(encoded, digest)


def test_digest_mismatch_and_untyped_pair_refuse_without_native_calls():
    with pytest.raises(ValueError, match="^registry_cms_source_notification_invalid$"):
        decode_registry_cms_source_notification(json.dumps({}), "0" * 64)
    with pytest.raises(ValueError, match="^registry_cms_source_notification_invalid$"):
        RegistryCMSSourceNotification(_attempt(), {})


class _OperatorConnection:
    def __init__(self, permitted=True, transaction=True):
        self.permitted = permitted
        self.transaction = transaction
        self.queries = []

    def is_in_transaction(self):
        return self.transaction

    async def fetchval(self, query, *arguments):
        self.queries.append((query, arguments))
        return self.permitted


@pytest.mark.asyncio
async def test_operator_plan_only_returns_exact_configured_narrow_grants():
    from process.registry_cms_source_notifications import plan_registry_cms_source_notification_grants

    connection = _OperatorConnection()
    statements = await plan_registry_cms_source_notification_grants(
        connection,
        control_schema="registry_control",
        owner_role="protected_owner",
        publisher_role="publisher",
        reader_roles=("consumer",),
    )
    assert len(connection.queries) == 1 and len(statements) == 6
    assert statements[1] == 'ALTER TABLE "registry_control".registry_cms_source_notification OWNER TO "protected_owner"'
    assert statements[3] == 'GRANT USAGE ON SCHEMA "registry_control" TO "consumer","publisher"'
    assert (
        statements[4] == 'GRANT SELECT ON "registry_control".registry_cms_source_notification TO "consumer","publisher"'
    )
    assert statements[5] == (
        'GRANT INSERT ("source_schema","receipt_id","capture_id","publication_xid",'
        '"profile_generation_id","notification_sha256","notification_json") ON '
        '"registry_control".registry_cms_source_notification TO "publisher"'
    )
    assert all(
        token not in " ".join(statements) for token in ("CREATE ROLE", "ALTER ROLE", "created_at", "ArchiveReader")
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("permitted,transaction", [(False, True), (True, False)])
async def test_operator_plan_refuses_preflight_or_missing_operator_transaction(permitted, transaction):
    from process.registry_cms_source_notifications import plan_registry_cms_source_notification_grants

    connection = _OperatorConnection(permitted, transaction)
    with pytest.raises(ValueError, match="source_notification_invalid"):
        await plan_registry_cms_source_notification_grants(
            connection, control_schema="registry_control", owner_role="protected_owner", publisher_role="publisher"
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("writing", [True, False])
async def test_rc_preflight_is_publisher_only_and_never_widens_committed_reader(monkeypatch, writing):
    from process import registry_cms_source_notifications as notifications

    class Connection:
        def is_in_transaction(self):
            return True

        async def fetchval(self, query, *_arguments):
            return {
                "SHOW transaction_isolation": "read committed",
                "SHOW transaction_read_only": "off" if writing else "on",
                "SELECT current_user": "publisher",
            }.get(query, True)

    connection = Connection()

    async def driver(_session):
        return connection

    monkeypatch.setattr(notifications, "native_driver", driver)
    store = RegistryCMSSourceNotificationStore("registry_control", "protected_owner", "publisher", _attempt())
    if writing:
        assert await store._connection(object(), writing=True) is connection
    else:
        with pytest.raises(ValueError, match="source_notification_invalid"):
            await store._connection(object(), writing=False)
