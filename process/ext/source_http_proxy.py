# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Run-scoped Source proxy policy, sticky selection and destination cooldowns."""

import json
import os
import re
import ssl
from dataclasses import dataclass, field
from datetime import UTC, datetime
from threading import RLock
from types import MappingProxyType
from urllib.parse import unquote

import aiohttp
from yarl import URL

POLICY_ENV = "HLTHPRT_SOURCE_HTTP_PROXY_POLICY_JSON"
POOL_ENV = "HLTHPRT_HTTP_PROXY_POOL_JSON"
_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,255}\Z")
_BAD_ESCAPE = re.compile(r"%(?![0-9A-Fa-f]{2})")


class SourceProxyError(ValueError):
    """Expose static error codes without target URLs or credentials."""


class SourceProxyUnavailable(SourceProxyError):
    """No allowed route is presently usable; the caller owns waiting/retry."""

    def __init__(self, retry_not_before: str | None):
        super().__init__("source_proxy_unavailable")
        self.retry_not_before = retry_not_before


class _ProxyAuth(aiohttp.BasicAuth):
    __slots__ = ()

    def __repr__(self):
        return "<source proxy authentication>"


@dataclass(frozen=True, slots=True)
class _SourceRule:
    endpoint_scope: str
    enabled: bool
    origin: tuple[str, str, int]
    base_path: str


@dataclass(frozen=True, slots=True)
class _ProxyRoute:
    proxy_id: str
    url: str = field(repr=False)
    auth: aiohttp.BasicAuth = field(repr=False)


@dataclass(frozen=True, slots=True)
class SourceProxySelection:
    proxy_id: str
    request_options: dict = field(repr=False)


def _require(condition, error):
    if not condition:
        raise SourceProxyError(error)


def _is_identifier(value):
    return type(value) is str and _IDENTIFIER.fullmatch(value) is not None


def _json_environment(name, error):
    try:
        payload = json.loads(os.environ.get(name, ""))
    except ValueError, TypeError:
        raise SourceProxyError(error) from None
    _require(type(payload) is dict, error)
    return payload


def _target_scope(url_text, *, base=False):
    """Inspect the original URL without rewriting cursor bytes or resolving DNS."""
    error = "source_proxy_target_invalid"
    _require(type(url_text) is str and bool(url_text), error)
    _require(not any(ord(char) <= 32 or ord(char) == 127 for char in url_text) and "#" not in url_text, error)
    try:
        parsed = URL(url_text, encoded=True)
        path = parsed.raw_path
        decoded_path = unquote(path, errors="strict")
        fully_decoded_path = unquote(decoded_path, errors="strict")
        origin = (parsed.scheme, parsed.raw_host.lower() if parsed.raw_host else "", parsed.port)
    except ValueError, TypeError, UnicodeError:
        raise SourceProxyError(error) from None
    _require(
        origin[0] == "https"
        and bool(origin[1])
        and origin[2] is not None
        and 1 <= origin[2] <= 65535
        and "%" not in origin[1]
        and parsed.user is None
        and parsed.password is None,
        error,
    )
    _require(not base or "?" not in url_text, error)
    _require(_BAD_ESCAPE.search(path) is None and path.count("/") == decoded_path.count("/"), error)
    _require("\\" not in decoded_path and "//" not in decoded_path, error)
    _require(not any(ord(char) < 32 or ord(char) == 127 for char in decoded_path), error)
    _require(not any(part in {".", ".."} for part in decoded_path.split("/")), error)
    _require(fully_decoded_path == decoded_path, error)
    return origin, decoded_path.rstrip("/") or "/"


def _policy_rules(payload):
    error = "source_proxy_policy_invalid"
    _require(set(payload) == {"version", "revision", "sources"}, error)
    _require(type(payload["version"]) is int and payload["version"] == 1 and _is_identifier(payload["revision"]), error)
    _require(type(payload["sources"]) is list, error)
    rules_by_source = {}
    for entry in payload["sources"]:
        _require(type(entry) is dict and set(entry) == {"source_id", "endpoint_scope", "use_proxy"}, error)
        _require(_is_identifier(entry["source_id"]) and entry["source_id"] not in rules_by_source, error)
        _require(type(entry["use_proxy"]) is bool, error)
        try:
            origin, path = _target_scope(entry["endpoint_scope"], base=True)
        except SourceProxyError, UnicodeError:
            raise SourceProxyError(error) from None
        rules_by_source[entry["source_id"]] = _SourceRule(entry["endpoint_scope"], entry["use_proxy"], origin, path)
    return MappingProxyType(rules_by_source)


def _proxy_route(entry):
    error = "source_proxy_pool_invalid"
    _require(type(entry) is dict and set(entry) == {"id", "url"} and _is_identifier(entry["id"]), error)
    raw_url = entry["url"]
    _require(type(raw_url) is str and not any(ord(char) <= 32 or ord(char) == 127 for char in raw_url), error)
    try:
        parsed = URL(raw_url)
        _require(
            parsed.scheme in {"http", "https"}
            and bool(parsed.host)
            and 1 <= parsed.port <= 65535
            and bool(parsed.user)
            and bool(parsed.password)
            and ":" not in parsed.user
            and parsed.path in {"", "/"}
            and "?" not in raw_url
            and "#" not in raw_url,
            error,
        )
        _require(not any(ord(char) < 32 or ord(char) == 127 for char in parsed.user + parsed.password), error)
        return _ProxyRoute(entry["id"], str(parsed.with_user(None)), _ProxyAuth(parsed.user, parsed.password, "utf-8"))
    except ValueError, TypeError, UnicodeError:
        raise SourceProxyError(error) from None


def _pool_routes(payload):
    error = "source_proxy_pool_invalid"
    _require(set(payload) == {"version", "revision", "proxies"}, error)
    _require(type(payload["version"]) is int and payload["version"] == 1 and _is_identifier(payload["revision"]), error)
    _require(type(payload["proxies"]) is list and 1 <= len(payload["proxies"]) <= 64, error)
    routes = tuple(_proxy_route(entry) for entry in payload["proxies"])
    _require(len({route.proxy_id for route in routes}) == len(routes), error)
    return routes


def _utcnow():
    return datetime.now(UTC)


def _boundary(value):
    if value is None:
        return None
    _require(type(value) is str, "source_proxy_cooldown_invalid")
    try:
        result = datetime.fromisoformat(value)
        _require(result.tzinfo is not None and result.utcoffset() is not None, "source_proxy_cooldown_invalid")
        return result.astimezone(UTC)
    except ValueError, TypeError, OverflowError:
        raise SourceProxyError("source_proxy_cooldown_invalid") from None


def _iso(value):
    return value.isoformat().replace("+00:00", "Z") if value is not None else None


def _origin_text(origin):
    return str(URL.build(scheme=origin[0], host=origin[1], port=origin[2]))


class SourceProxyConfig:
    """Freeze policy immediately and the secret pool upon its first enabled bind."""

    def __init__(self, revision, rules_by_source):
        self._revision = revision
        self._rules = rules_by_source
        self._pool_revision = None
        self._routes = ()
        self._ssl = None
        self._bindings_by_source = {}
        self._assignments_by_cursor = {}
        self._cooldowns_by_route = {}
        self._next_index = 0
        self._lock = RLock()

    def __repr__(self):
        return f"SourceProxyConfig(policy_revision={self._revision!r})"

    @property
    def policy_revision(self):
        """Return the frozen nonsecret policy revision, or None for direct defaults."""
        return self._revision

    @property
    def has_enabled_sources(self):
        """Report whether a worker must distinguish explicit Source HTTP scopes."""
        return any(rule.enabled for rule in self._rules.values())

    @classmethod
    def from_environment(cls):
        """Freeze trusted policy without accessing the secret route pool."""
        if not os.environ.get(POLICY_ENV, "").strip():
            return cls(None, MappingProxyType({}))
        payload = _json_environment(POLICY_ENV, "source_proxy_policy_invalid")
        return cls(payload["revision"] if "revision" in payload else None, _policy_rules(payload))

    def use_proxy(self, source_id, endpoint_scope):
        """Check exact Source scope without reading credentials; reject scope drift."""
        _require(_is_identifier(source_id), "source_proxy_source_invalid")
        rule = self._rules.get(source_id)
        if rule is None:
            return False
        _require(endpoint_scope == rule.endpoint_scope, "source_proxy_scope_mismatch")
        return rule.enabled

    def bind(self, source_id, endpoint_scope):
        """Lazily freeze the pool for an enabled Source, or return None for direct."""
        if not self.use_proxy(source_id, endpoint_scope):
            return None
        with self._lock:
            if self._pool_revision is None:
                payload = _json_environment(POOL_ENV, "source_proxy_pool_invalid")
                routes = _pool_routes(payload)
                verified_ssl = ssl.create_default_context()
                self._routes, self._ssl, self._pool_revision = routes, verified_ssl, payload["revision"]
            if source_id not in self._bindings_by_source:
                self._bindings_by_source[source_id] = BoundSourceProxy(self, source_id, self._rules[source_id])
            return self._bindings_by_source[source_id]

    def _is_available(self, origin, proxy_id, now):
        key = origin, proxy_id
        if key not in self._cooldowns_by_route:
            return True
        boundary = self._cooldowns_by_route[key]
        return boundary is not None and boundary <= now

    def _select(self, bound, url, cursor_key, exclude_ids):
        origin, path = _target_scope(url)
        rule = bound._rule
        _require(
            origin == rule.origin
            and (rule.base_path == "/" or path == rule.base_path or path.startswith(rule.base_path + "/")),
            "source_proxy_target_invalid",
        )
        _require(
            type(cursor_key) is str
            and bool(cursor_key)
            and len(cursor_key) <= 512
            and not any(char in cursor_key for char in "?#")
            and "://" not in cursor_key
            and not any(ord(char) < 32 or ord(char) == 127 for char in cursor_key),
            "source_proxy_cursor_invalid",
        )
        _require(type(exclude_ids) in {set, frozenset}, "source_proxy_exclusions_invalid")
        with self._lock:
            routes_by_id = {route.proxy_id: route for route in self._routes}
            _require(exclude_ids <= routes_by_id.keys(), "source_proxy_exclusions_invalid")
            now = _utcnow()
            eligible_ids = {
                proxy_id
                for proxy_id in routes_by_id
                if proxy_id not in exclude_ids and self._is_available(origin, proxy_id, now)
            }
            if not eligible_ids:
                boundaries = [
                    boundary
                    for (destination, proxy_id), boundary in self._cooldowns_by_route.items()
                    if destination == origin and proxy_id not in exclude_ids and boundary is not None and boundary > now
                ]
                raise SourceProxyUnavailable(_iso(min(boundaries)) if boundaries else None)
            key = bound._source_id, cursor_key
            proxy_id = self._assignments_by_cursor.get(key)
            if proxy_id not in eligible_ids:
                for offset in range(len(self._routes)):
                    index = (self._next_index + offset) % len(self._routes)
                    if self._routes[index].proxy_id in eligible_ids:
                        proxy_id = self._routes[index].proxy_id
                        self._next_index = (index + 1) % len(self._routes)
                        self._assignments_by_cursor[key] = proxy_id
                        break
            route = routes_by_id[proxy_id]
            return SourceProxySelection(proxy_id, {"proxy": route.url, "proxy_auth": route.auth, "ssl": self._ssl})

    def _defer(self, origin, proxy_id, boundary):
        _require(
            _is_identifier(proxy_id) and proxy_id in {route.proxy_id for route in self._routes},
            "source_proxy_route_invalid",
        )
        key = origin, proxy_id
        if key not in self._cooldowns_by_route:
            self._cooldowns_by_route[key] = boundary
        elif self._cooldowns_by_route[key] is not None:
            self._cooldowns_by_route[key] = (
                max(self._cooldowns_by_route[key], boundary) if boundary is not None else None
            )


class BoundSourceProxy:
    """Bind one exact Source scope to the run's frozen pool and shared TLS context."""

    def __init__(self, config, source_id, rule):
        self._config, self._source_id, self._rule = config, source_id, rule

    def __repr__(self):
        return f"BoundSourceProxy(source_id={self._source_id!r}, pool_revision={self.pool_revision!r})"

    @property
    def policy_revision(self):
        """Return this binding's frozen nonsecret policy revision."""
        return self._config._revision

    @property
    def pool_revision(self):
        """Return this binding's frozen nonsecret pool revision."""
        return self._config._pool_revision

    def select(self, url, cursor_key, exclude_ids=frozenset()):
        """Select a scoped route for one logical cursor without rewriting its URL."""
        return self._config._select(self, url, cursor_key, exclude_ids)

    def defer(self, proxy_id, retry_not_before):
        """Extend an origin-specific cooldown; None requires manual intervention."""
        boundary = _boundary(retry_not_before)
        with self._config._lock:
            self._config._defer(self._rule.origin, proxy_id, boundary)

    def cooldown_snapshot(self):
        """Copy nonsecret origin cooldowns for durable caller-owned diagnostics."""
        with self._config._lock:
            return {
                "version": 1,
                "pool_revision": self.pool_revision,
                "origin": _origin_text(self._rule.origin),
                "cooldowns": [
                    {"proxy_id": proxy_id, "retry_not_before": _iso(boundary)}
                    for (origin, proxy_id), boundary in sorted(self._config._cooldowns_by_route.items())
                    if origin == self._rule.origin
                ],
            }

    def restore_cooldowns(self, snapshot):
        """Atomically merge matching-pool origin cooldowns without shortening blocks."""
        error = "source_proxy_cooldown_invalid"
        _require(type(snapshot) is dict and set(snapshot) == {"version", "pool_revision", "origin", "cooldowns"}, error)
        _require(
            type(snapshot["version"]) is int
            and snapshot["version"] == 1
            and snapshot["pool_revision"] == self.pool_revision
            and snapshot["origin"] == _origin_text(self._rule.origin)
            and type(snapshot["cooldowns"]) is list,
            error,
        )
        boundaries_by_id = {}
        for entry in snapshot["cooldowns"]:
            _require(type(entry) is dict and set(entry) == {"proxy_id", "retry_not_before"}, error)
            _require(_is_identifier(entry["proxy_id"]) and entry["proxy_id"] not in boundaries_by_id, error)
            boundaries_by_id[entry["proxy_id"]] = _boundary(entry["retry_not_before"])
        with self._config._lock:
            _require(boundaries_by_id.keys() <= {route.proxy_id for route in self._config._routes}, error)
            for proxy_id, boundary in boundaries_by_id.items():
                self._config._defer(self._rule.origin, proxy_id, boundary)
