# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Synthetic policy/pool parsing, route affinity and destination cooldown proof."""

import base64
import collections
import copy
import json
import ssl
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta

import aiohttp
import pytest
from yarl import URL

from process.ext import source_http_proxy as proxy

SCOPE = "https://directory.example/fhir"
OTHER_SCOPE = "https://other.example/fhir"
NOW = datetime(2035, 1, 1, tzinfo=UTC)


@pytest.fixture(autouse=True)
def isolated_environment(monkeypatch):
    monkeypatch.delenv(proxy.POLICY_ENV, raising=False)
    monkeypatch.delenv(proxy.POOL_ENV, raising=False)
    monkeypatch.setattr(proxy, "_utcnow", lambda: NOW)


def _rule(source_id="source-a", scope=SCOPE, enabled=True):
    return {"source_id": source_id, "endpoint_scope": scope, "use_proxy": enabled}


def _policy(rules=None, revision="policy-1"):
    return {"version": 1, "revision": revision, "sources": rules if rules is not None else [_rule()]}


def _pool(revision="pool-1"):
    return {
        "version": 1,
        "revision": revision,
        "proxies": [
            {"id": "route-" + suffix, "url": f"https://synthetic-user:synthetic-password@proxy.example:{port}"}
            for suffix, port in zip("abc", (8401, 8402, 8403), strict=True)
        ],
    }


def _configured(monkeypatch, rules=None):
    monkeypatch.setenv(proxy.POLICY_ENV, json.dumps(_policy(rules)))
    monkeypatch.setenv(proxy.POOL_ENV, json.dumps(_pool()))
    return proxy.SourceProxyConfig.from_environment()


def _bound(monkeypatch, rules=None):
    return _configured(monkeypatch, rules).bind("source-a", SCOPE)


def _at(seconds):
    return (NOW + timedelta(seconds=seconds)).isoformat().replace("+00:00", "Z")


def test_absent_and_disabled_sources_never_read_pool(monkeypatch):
    class PolicyOnlyEnvironment(dict):
        def get(self, key, default=None):
            assert key != proxy.POOL_ENV, "secret pool read"
            return super().get(key, default)

    policy_json = json.dumps(_policy([_rule(), _rule("disabled", enabled=False)]))
    monkeypatch.setattr(proxy.os, "environ", PolicyOnlyEnvironment({proxy.POLICY_ENV: policy_json}))
    config = proxy.SourceProxyConfig.from_environment()
    assert config.policy_revision == "policy-1" and config.use_proxy("source-a", SCOPE)
    assert not config.use_proxy("absent", "unconfigured-scope")
    assert config.bind("absent", "unconfigured-scope") is None
    assert config.bind("disabled", SCOPE) is None
    with pytest.raises(proxy.SourceProxyError, match="source_proxy_scope_mismatch"):
        config.use_proxy("source-a", OTHER_SCOPE)


def test_unconfigured_policy_is_direct_even_with_invalid_pool(monkeypatch):
    monkeypatch.setenv(proxy.POOL_ENV, "invalid secret pool")
    config = proxy.SourceProxyConfig.from_environment()
    assert config.policy_revision is None
    assert config.bind("source-a", SCOPE) is None


@pytest.mark.parametrize(
    "change",
    [
        lambda value: value.update(version=True),
        lambda value: value.update(revision=None),
        lambda value: value.update(sources={}),
        lambda value: value["sources"].append(_rule()),
        lambda value: value["sources"][0].update(use_proxy=1),
        lambda value: value["sources"][0].update(endpoint_scope=SCOPE + "?filter=partial"),
        lambda value: value["sources"][0].update(endpoint_scope="http://directory.example/fhir"),
        lambda value: value.update(unrecognized=True),
    ],
)
def test_policy_rejects_malformed_or_ambiguous_configuration(monkeypatch, change):
    policy_by_field = _policy()
    change(policy_by_field)
    monkeypatch.setenv(proxy.POLICY_ENV, json.dumps(policy_by_field))
    with pytest.raises(proxy.SourceProxyError, match="source_proxy_policy_invalid"):
        proxy.SourceProxyConfig.from_environment()


def test_policy_and_lazy_pool_are_frozen_for_run(monkeypatch):
    config = _configured(monkeypatch, [_rule(), _rule("source-b", OTHER_SCOPE)])
    monkeypatch.setenv(proxy.POLICY_ENV, json.dumps(_policy([_rule(enabled=False)], revision="policy-2")))
    assert config.use_proxy("source-a", SCOPE)
    monkeypatch.setenv(proxy.POOL_ENV, json.dumps(_pool(revision="pool-before-bind")))
    bound = config.bind("source-a", SCOPE)
    first = bound.select(SCOPE + "/Practitioner", "Practitioner")
    monkeypatch.setenv(proxy.POOL_ENV, "invalid replacement pool")
    assert config.bind("source-a", SCOPE) is bound
    other = config.bind("source-b", OTHER_SCOPE)
    assert bound.policy_revision == "policy-1" and other.pool_revision == "pool-before-bind"
    assert other.select(OTHER_SCOPE, "Organization").request_options["ssl"] is first.request_options["ssl"]


def test_enabled_source_missing_pool_fails_without_direct_fallback(monkeypatch):
    monkeypatch.setenv(proxy.POLICY_ENV, json.dumps(_policy()))
    config = proxy.SourceProxyConfig.from_environment()
    with pytest.raises(proxy.SourceProxyError, match="source_proxy_pool_invalid"):
        config.bind("source-a", SCOPE)


@pytest.mark.parametrize(
    "url",
    [
        "https://proxy.example:8401",
        "https://user:@proxy.example:8401",
        "socks5://user:secret@proxy.example:8401",
        "https://user:secret@proxy.example:0",
        "https://user:secret@proxy.example:65536",
        "https://user:secret@proxy.example:invalid",
        "https://user:secret@proxy.example:8401/path",
        "https://user:secret@proxy.example:8401?value=secret",
        "https://user:secret@proxy.example:8401#secret",
        "https://user:sec\nret@proxy.example:8401",
        "https://user:sec%0Aret@proxy.example:8401",
        "https://us%3Aer:secret@proxy.example:8401",
    ],
)
def test_pool_url_errors_are_sanitized(monkeypatch, url):
    config = _configured(monkeypatch)
    pool_by_field = _pool()
    pool_by_field["proxies"] = [{"id": "route-a", "url": url}]
    monkeypatch.setenv(proxy.POOL_ENV, json.dumps(pool_by_field))
    with pytest.raises(proxy.SourceProxyError) as failure:
        config.bind("source-a", SCOPE)
    assert str(failure.value) == "source_proxy_pool_invalid"
    assert "secret" not in repr(failure.value) and "proxy.example" not in repr(failure.value)


@pytest.mark.parametrize(
    "change",
    [
        lambda value: value.update(version=True),
        lambda value: value.update(proxies=[]),
        lambda value: value["proxies"].append(value["proxies"][0]),
        lambda value: value["proxies"][0].update(id="invalid id"),
    ],
)
def test_pool_rejects_invalid_shape(monkeypatch, change):
    config = _configured(monkeypatch)
    pool_by_field = _pool()
    change(pool_by_field)
    monkeypatch.setenv(proxy.POOL_ENV, json.dumps(pool_by_field))
    with pytest.raises(proxy.SourceProxyError, match="source_proxy_pool_invalid"):
        config.bind("source-a", SCOPE)


@pytest.mark.parametrize("route_count", [64, 65])
def test_pool_size_matches_durable_cooldown_limit(monkeypatch, route_count):
    config = _configured(monkeypatch)
    pool_by_field = _pool()
    pool_by_field["proxies"] = [
        {"id": f"route-{index}", "url": f"https://synthetic:secret@proxy.example:{10001 + index}"}
        for index in range(route_count)
    ]
    monkeypatch.setenv(proxy.POOL_ENV, json.dumps(pool_by_field))
    if route_count == 64:
        assert config.bind("source-a", SCOPE).pool_revision == "pool-1"
    else:
        monkeypatch.setattr(proxy, "_proxy_route", lambda _entry: pytest.fail("oversized pool parsed credentials"))
        with pytest.raises(proxy.SourceProxyError, match="source_proxy_pool_invalid"):
            config.bind("source-a", SCOPE)


@pytest.mark.parametrize("scheme", ["http", "https"])
def test_raw_authentication_and_repr_redaction(monkeypatch, scheme):
    login_text, credential_text = "synthetic+user", "p:a@ss/word?#% ü"
    proxy_url = URL.build(scheme=scheme, host="proxy.example", port=8401, user=login_text, password=credential_text)
    config = _configured(monkeypatch)
    pool_by_field = _pool()
    pool_by_field["proxies"] = [{"id": "route-a", "url": str(proxy_url)}]
    monkeypatch.setenv(proxy.POOL_ENV, json.dumps(pool_by_field))
    bound = config.bind("source-a", SCOPE)
    selected = bound.select(SCOPE, "Practitioner")
    options = selected.request_options
    auth = options["proxy_auth"]
    assert isinstance(auth, aiohttp.BasicAuth) and (auth.login, auth.password) == (login_text, credential_text)
    assert base64.b64decode(auth.encode().removeprefix("Basic ")).decode("utf-8") == login_text + ":" + credential_text
    assert options["proxy"] == f"{scheme}://proxy.example:8401"
    representations = repr((config, bound, selected, options, auth))
    assert (
        login_text not in representations and credential_text not in representations and "p%3Aa" not in representations
    )


def test_tls_context_is_verified_and_reused(monkeypatch):
    monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_TLS_VERIFY", "false")
    bound = _bound(monkeypatch)
    first = bound.select(SCOPE + "/Practitioner", "Practitioner").request_options["ssl"]
    second = bound.select(SCOPE + "/Organization", "Organization").request_options["ssl"]
    assert first is second and first.verify_mode == ssl.CERT_REQUIRED and first.check_hostname


def test_affinity_ignores_query_and_spreads_initial_cursor_keys(monkeypatch):
    bound = _bound(monkeypatch)
    first = bound.select(SCOPE + "/Practitioner?_count=1&cursor=one%2Ftwo", "Practitioner")
    second = bound.select(SCOPE + "/Practitioner?_count=50&cursor=next%2Bvalue", "Practitioner")
    assert first.proxy_id == second.proxy_id == "route-a"
    assert bound.select(SCOPE + "/Organization", "Organization").proxy_id == "route-b"
    assert bound.select(SCOPE + "/Location", "Location").proxy_id == "route-c"


def test_new_cursor_skips_a_blocked_initial_route(monkeypatch):
    bound = _bound(monkeypatch)
    bound.defer("route-a", None)
    assert bound.select(SCOPE + "/Practitioner", "Practitioner").proxy_id == "route-b"


def test_bind_and_selection_are_atomic_across_callers(monkeypatch):
    config = _configured(monkeypatch)
    with ThreadPoolExecutor(max_workers=8) as executor:
        bindings = list(executor.map(lambda _: config.bind("source-a", SCOPE), range(8)))
        selections = list(executor.map(lambda key: bindings[0].select(SCOPE, "cursor-" + str(key)), range(8)))
    assert all(bound is bindings[0] for bound in bindings)
    assert sorted(collections.Counter(selected.proxy_id for selected in selections).values()) == [2, 3, 3]
    assert len({id(selected.request_options["ssl"]) for selected in selections}) == 1


def test_cooldowns_fail_over_and_report_earliest_boundary(monkeypatch):
    bound = _bound(monkeypatch)
    assert bound.select(SCOPE, "cursor").proxy_id == "route-a"
    bound.defer("route-a", _at(20))
    assert bound.select(SCOPE, "cursor").proxy_id == "route-b"
    bound.defer("route-b", _at(10))
    bound.defer("route-c", None)
    with pytest.raises(proxy.SourceProxyUnavailable) as blocked:
        bound.select(SCOPE, "cursor")
    assert blocked.value.retry_not_before == _at(10)
    monkeypatch.setattr(proxy, "_utcnow", lambda: NOW + timedelta(seconds=11))
    assert bound.select(SCOPE, "cursor").proxy_id == "route-b"
    assert any(entry["retry_not_before"] == _at(10) for entry in bound.cooldown_snapshot()["cooldowns"])
    monkeypatch.setattr(proxy, "_utcnow", lambda: NOW + timedelta(seconds=30))
    assert bound.select(SCOPE, "cursor").proxy_id == "route-b"


def test_manual_blocks_and_longer_deferrals_are_preserved(monkeypatch):
    bound = _bound(monkeypatch)
    bound.defer("route-a", _at(20))
    bound.defer("route-a", _at(10))
    bound.defer("route-b", None)
    bound.defer("route-b", _at(30))
    bound.defer("route-c", None)
    snapshot = bound.cooldown_snapshot()
    assert snapshot["cooldowns"] == [
        {"proxy_id": "route-a", "retry_not_before": _at(20)},
        {"proxy_id": "route-b", "retry_not_before": None},
        {"proxy_id": "route-c", "retry_not_before": None},
    ]
    with pytest.raises(proxy.SourceProxyUnavailable) as blocked:
        bound.select(SCOPE, "cursor", exclude_ids=frozenset({"route-a"}))
    assert blocked.value.retry_not_before is None


def test_destination_cooldowns_do_not_disable_other_origins(monkeypatch):
    config = _configured(monkeypatch, [_rule(), _rule("source-b", SCOPE + "-other"), _rule("source-c", OTHER_SCOPE)])
    first = config.bind("source-a", SCOPE)
    same_origin = config.bind("source-b", SCOPE + "-other")
    other_origin = config.bind("source-c", OTHER_SCOPE)
    first.defer("route-a", None)
    only_first = frozenset({"route-b", "route-c"})
    with pytest.raises(proxy.SourceProxyUnavailable):
        same_origin.select(SCOPE + "-other", "cursor", only_first)
    assert other_origin.select(OTHER_SCOPE, "cursor", only_first).proxy_id == "route-a"
    assert other_origin.cooldown_snapshot()["cooldowns"] == []


def test_exclusions_never_trigger_direct_fallback(monkeypatch):
    bound = _bound(monkeypatch)
    first = bound.select(SCOPE, "cursor")
    assert bound.select(SCOPE, "cursor", frozenset({first.proxy_id})).proxy_id != first.proxy_id
    with pytest.raises(proxy.SourceProxyUnavailable) as blocked:
        bound.select(SCOPE, "cursor", frozenset({"route-a", "route-b", "route-c"}))
    assert blocked.value.retry_not_before is None
    with pytest.raises(proxy.SourceProxyError, match="source_proxy_exclusions_invalid"):
        bound.select(SCOPE, "cursor", frozenset({"unknown"}))


@pytest.mark.parametrize(
    "suffix",
    [
        "/../clinical",
        "/%2e%2e/clinical",
        "/%252fclinical",
        "/%2Fclinical",
        "/%5cclinical",
        "/\\clinical",
        "//clinical",
        "/%00invalid",
        "/%FF",
        "/%ZZ",
        "/Practitioner#",
        "/Practitioner\n",
    ],
)
def test_target_rejects_ambiguous_paths(monkeypatch, suffix):
    with pytest.raises(proxy.SourceProxyError, match="source_proxy_target_invalid"):
        _bound(monkeypatch).select(SCOPE + suffix, "cursor")


@pytest.mark.parametrize(
    "target",
    [
        "http://directory.example/fhir",
        "https://directory.example:8443/fhir",
        "https://directory.example/fhir-other",
        "https://other.example/fhir",
        "https://directory.example.evil/fhir",
        "https://user:secret@directory.example/fhir",
    ],
)
def test_target_requires_exact_origin_and_path_scope(monkeypatch, target):
    with pytest.raises(proxy.SourceProxyError, match="source_proxy_target_invalid"):
        _bound(monkeypatch).select(target, "cursor")


def test_explicit_nondefault_source_port_is_scoped(monkeypatch):
    scope = "https://directory.example:8443/fhir"
    bound = _configured(monkeypatch, [_rule(scope=scope)]).bind("source-a", scope)
    assert bound.select(scope + "/Practitioner?cursor=one%2Ftwo", "cursor").proxy_id == "route-a"
    with pytest.raises(proxy.SourceProxyError):
        bound.select(SCOPE, "cursor")


@pytest.mark.parametrize(
    "key", ["", "https://directory.example/fhir?cursor=value", "Practitioner?cursor=value", "bad\nkey"]
)
def test_cursor_identity_must_be_explicit_logical_key(monkeypatch, key):
    with pytest.raises(proxy.SourceProxyError, match="source_proxy_cursor_invalid"):
        _bound(monkeypatch).select(SCOPE, key)


def test_cooldown_snapshot_roundtrip_and_atomic_rejection(monkeypatch):
    first = _bound(monkeypatch)
    first.defer("route-a", _at(20))
    first.defer("route-b", None)
    snapshot = first.cooldown_snapshot()
    second = _bound(monkeypatch)
    second.restore_cooldowns(snapshot)
    assert second.cooldown_snapshot() == snapshot and second.select(SCOPE, "cursor").proxy_id == "route-c"
    assert "synthetic-password" not in json.dumps(snapshot) and "proxy.example" not in json.dumps(snapshot)
    changed_snapshot = copy.deepcopy(snapshot)
    changed_snapshot["cooldowns"].append({"proxy_id": "unknown", "retry_not_before": None})
    with pytest.raises(proxy.SourceProxyError):
        second.restore_cooldowns(changed_snapshot)
    assert second.cooldown_snapshot() == snapshot


@pytest.mark.parametrize("field,value", [("pool_revision", "different-pool"), ("origin", OTHER_SCOPE)])
def test_cooldown_restore_requires_same_pool_and_destination(monkeypatch, field, value):
    bound = _bound(monkeypatch)
    snapshot = bound.cooldown_snapshot()
    snapshot[field] = value
    with pytest.raises(proxy.SourceProxyError, match="source_proxy_cooldown_invalid"):
        bound.restore_cooldowns(snapshot)
    assert bound.cooldown_snapshot()["cooldowns"] == []


@pytest.mark.parametrize("value", ["2035-01-01T00:00:00", "invalid", 3])
def test_cooldown_rejects_unknown_time_formats(monkeypatch, value):
    bound = _bound(monkeypatch)
    with pytest.raises(proxy.SourceProxyError, match="source_proxy_cooldown_invalid"):
        bound.defer("route-a", value)
    assert bound.cooldown_snapshot()["cooldowns"] == []
