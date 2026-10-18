# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Check retained source candidates without admission or publication authority."""

import asyncio
import fcntl
import hashlib
import json
import os
import shutil
import stat
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5

import click

from process.registry_naic_source import MEMBERS, PARSER_VERSION, prepare_naic_source_archive
from process.registry_source_admission import RegistrySourceAdmissionError
from process.registry_source_editions import (
    RegistrySourceEditionDiscoveryError,
    RegistrySourceEditionPolicy,
    _read_catalog,
    discover_registry_source_edition,
    registry_source_catalog_spec,
    select_registry_source_edition,
)
from process.registry_source_fetch import (
    MAX_ARTIFACT_BYTES,
    PLAN_FINDER_MAX_ARTIFACT_BYTES,
    RegistrySourceFetchError,
    RegistrySourceFetchReceipt,
    RegistrySourceFetchResult,
    RegistrySourceFetchSpec,
    _verified_previous,
    fetch_registry_source,
)
from process.registry_source_preparation import RegistrySourcePreparationError, prepare_registry_source_input
from process.registry_source_schedule_intents import RegistrySourceRefreshScope, build_registry_source_schedule_intents
from process.uhc_flex_practitioner_async_safety import drain_operation

CONFIG_ENV = "HLTHPRT_REGISTRY_SOURCE_CHECK"
SCOPE_FIELDS = frozenset({"source_system", "source_id", "edition_policy", "reporting_year"})
_SOURCES = {("cms", "commercial-mlr"), ("cms", "plan-finder"), ("naic", "company-listings")}


def _invalid():
    raise ValueError("registry_source_check_config_invalid")


def _object(document, required, optional=frozenset()):
    if type(document) is not dict or not required <= document.keys() or document.keys() - required - optional:
        _invalid()
    return document


def _unique(pairs):
    document_by_name = {}
    for key, field_value in pairs:
        if key in document_by_name:
            _invalid()
        document_by_name[key] = field_value
    return document_by_name


def validate_registry_source_check_scope(document):
    """Accept logical selectors only; no location, credentials or runtime overrides."""
    _object(document, SCOPE_FIELDS)
    scope = RegistrySourceRefreshScope(**document)
    if (scope.source_system, scope.source_id) not in _SOURCES:
        _invalid()
    if scope.reporting_year is not None and not 2010 <= scope.reporting_year <= 2100:
        _invalid()
    return scope


@dataclass(frozen=True)
class RegistrySourceCheckConfig:
    cache_dir: Path
    scopes: tuple
    max_cache_bytes: int = 2 * 1024**3
    max_cache_files: int = 256
    naic: dict | None = None

    def __post_init__(self):
        if not isinstance(self.cache_dir, Path) or not self.cache_dir.is_absolute():
            _invalid()
        if type(self.scopes) is not tuple or not 1 <= len(self.scopes) <= 100:
            _invalid()
        for scope in self.scopes:
            if (
                type(scope) is not RegistrySourceRefreshScope
                or scope.available is not True
                or scope.last_checked_at is not None
            ):
                _invalid()
            validate_registry_source_check_scope(_scope(scope))
        if len({_key(scope) for scope in self.scopes}) != len(self.scopes):
            _invalid()
        if type(self.max_cache_bytes) is not int or not 32 * 1024**2 <= self.max_cache_bytes <= 8 * 1024**3:
            _invalid()
        if type(self.max_cache_files) is not int or not 32 <= self.max_cache_files <= 4096:
            _invalid()
        if self.naic is not None:
            _object(self.naic, {"edition_id", "source_url", "artifact_sha256", "layout_sha256"})
            _object(self.naic["layout_sha256"], set(MEMBERS))
            for digest in (self.naic["artifact_sha256"], *self.naic["layout_sha256"].values()):
                if (
                    type(digest) is not str
                    or len(digest) != 64
                    or any(char not in "0123456789abcdef" for char in digest)
                ):
                    _invalid()
            _naic_spec(self)


def _scope(scope):
    return {name: getattr(scope, name) for name in SCOPE_FIELDS}


def _key(scope):
    return hashlib.sha256(json.dumps(_scope(scope), sort_keys=True).encode()).hexdigest()


def _profile(config, scope):
    parser = (
        ("cms-header-v1" if scope.source_id == "commercial-mlr" else "cms-planfinder-hios-2026-08-06-v2")
        if scope.source_system == "cms"
        else PARSER_VERSION
    )
    profile_by_name = {
        "scope": _scope(scope),
        "parser_version": parser,
        "naic": config.naic if scope.source_system == "naic" else None,
    }
    return hashlib.sha256(json.dumps(profile_by_name, sort_keys=True).encode()).hexdigest()


def parse_registry_source_check_config(encoded):
    """Read a closed operator document before creating the bounded cache."""
    if type(encoded) is not str or not 0 < len(encoded.encode()) <= 16384:
        _invalid()
    document = _object(
        json.loads(encoded, object_pairs_hook=_unique),
        {"cache_dir", "scopes"},
        {"max_cache_bytes", "max_cache_files", "naic"},
    )
    if type(document["cache_dir"]) is not str or type(document["scopes"]) is not list:
        _invalid()
    return RegistrySourceCheckConfig(
        **{
            **document,
            "cache_dir": Path(document["cache_dir"]),
            "scopes": tuple(validate_registry_source_check_scope(scope) for scope in document["scopes"]),
        }
    )


def _naic_spec(config):
    return RegistrySourceFetchSpec(
        "naic",
        "company-listings",
        config.naic["edition_id"],
        config.naic["source_url"],
        PARSER_VERSION,
        2026,
        None,
        MAX_ARTIFACT_BYTES,
        config.naic["artifact_sha256"],
    )


@contextmanager
def _cache(config, scope):
    root = config.cache_dir
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    root_stat = root.lstat()
    if (
        not stat.S_ISDIR(root_stat.st_mode)
        or root_stat.st_uid != os.geteuid()
        or stat.S_IMODE(root_stat.st_mode) & 0o077
        or root.resolve(strict=True) != root
    ):
        _invalid()
    descriptor = os.open(root / ".check.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            yield None
            return
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            _invalid()
        files = []
        for path in root.rglob("*"):
            if len(files) >= config.max_cache_files:
                raise RuntimeError("registry_source_check_cache_full")
            files.append(path)
        if any(path.is_symlink() or not (path.is_file() or path.is_dir()) for path in files):
            _invalid()
        size = sum(path.stat().st_size for path in files if path.is_file())
        reservation = (
            PLAN_FINDER_MAX_ARTIFACT_BYTES if scope.source_id == "plan-finder" else MAX_ARTIFACT_BYTES
        ) + 64 * 1024**2
        if (
            len(files) + 20 > config.max_cache_files
            or size + reservation > config.max_cache_bytes
            or shutil.disk_usage(root).free < reservation
        ):
            raise RuntimeError("registry_source_check_cache_full")
        selected = root / _key(scope)
        selected.mkdir(exist_ok=True)
        yield selected
    finally:
        os.close(descriptor)


def _json_default(field_value):
    if isinstance(field_value, (date, datetime)):
        return field_value.isoformat()
    raise TypeError()


def _receipt(document):
    _object(document, {"spec", "artifact_sha256", "artifact_bytes", "retrieved_at", "etag", "last_modified"})
    spec_by_field = dict(document["spec"])
    if spec_by_field.get("published_at") is not None:
        spec_by_field["published_at"] = datetime.fromisoformat(spec_by_field["published_at"])
    return RegistrySourceFetchReceipt(
        **{
            **document,
            "spec": RegistrySourceFetchSpec(**spec_by_field),
            "retrieved_at": datetime.fromisoformat(document["retrieved_at"]),
        }
    )


def _load(root, scope):
    path = root / "prepared.json"
    if not path.exists():
        return None
    with path.open("rb") as stream:
        encoded = stream.read(65537)
    if len(encoded) > 65536:
        _invalid()
    state = _object(
        json.loads(encoded, object_pairs_hook=_unique),
        {"scope", "window", "profile", "checked_at", "catalog", "artifact", "candidate"},
    )
    if state["scope"] != _scope(scope):
        _invalid()
    return state


def _metadata(prepared, fetched):
    spec = fetched.receipt.spec
    metadata_by_name = dict(
        source_system=spec.source_system,
        source_id=spec.source_id,
        edition_id=spec.edition_id,
        reporting_year=spec.reporting_year,
        parser_version=spec.parser_version,
        artifact_sha256=fetched.receipt.artifact_sha256,
        artifact_bytes=fetched.receipt.artifact_bytes,
        published_at=None if spec.published_at is None else spec.published_at.isoformat(),
        retrieved_at=fetched.receipt.retrieved_at.isoformat(),
    )
    if spec.source_system == "naic":
        metadata_by_name.update(
            input_sha256=prepared.edition.input_sha256,
            header_sha256=prepared.header_sha256(),
            members=prepared.manifest(),
        )
    else:
        metadata_by_name.update(
            input_sha256=prepared.input_sha256,
            header_sha256=prepared.header_sha256,
            input_bytes=prepared.input_bytes,
            member_name=prepared.member_name,
            lookup_sha256=prepared.lookup_sha256,
            quarter=prepared.candidate.quarter,
            release=prepared.candidate.release,
            as_of_date=None if prepared.candidate.as_of_date is None else prepared.candidate.as_of_date.isoformat(),
        )
    return metadata_by_name


def _prepare(config, scope, root, fetched, candidate):
    if scope.source_system == "naic":
        prepared = prepare_naic_source_archive(
            fetched, root / "inputs", snapshot_id=uuid5(NAMESPACE_URL, fetched.receipt.artifact_sha256)
        )
        if {member.member_name: member.layout_pdf_sha256 for member in prepared.members} != config.naic[
            "layout_sha256"
        ]:
            raise RegistrySourcePreparationError()
    else:
        prepared = prepare_registry_source_input(candidate, fetched, root / "inputs")
    return _metadata(prepared, fetched)


def _cms_spec(scope, candidate):
    parser = "cms-header-v1" if scope.source_id == "commercial-mlr" else "cms-planfinder-hios-2026-08-06-v2"
    maximum = PLAN_FINDER_MAX_ARTIFACT_BYTES if scope.source_id == "plan-finder" else MAX_ARTIFACT_BYTES
    return RegistrySourceFetchSpec(
        "cms",
        scope.source_id,
        candidate.edition_id,
        candidate.source_url,
        parser,
        candidate.reporting_year,
        None,
        maximum,
    )


async def _retained(config, scope, root, state):
    artifact = _receipt(state["artifact"])
    await drain_operation(
        asyncio.to_thread(_verified_previous, artifact.spec, root, artifact), preserve_cancellation=True
    )
    fetched = RegistrySourceFetchResult(artifact, root / (artifact.artifact_sha256 + ".artifact"), True)
    if scope.source_system == "cms":
        catalog = _receipt(state["catalog"])
        if catalog.spec != registry_source_catalog_spec(scope.source_id):
            _invalid()
        result = RegistrySourceFetchResult(catalog, root / (catalog.artifact_sha256 + ".artifact"), True)
        candidate = select_registry_source_edition(
            await drain_operation(
                asyncio.to_thread(_read_catalog, scope.source_id, result), preserve_cancellation=True
            ),
            RegistrySourceEditionPolicy(scope.edition_policy, scope.reporting_year),
        )
    else:
        if artifact.spec != _naic_spec(config):
            _invalid()
        candidate = None
    if scope.source_system == "cms" and artifact.spec != _cms_spec(scope, candidate):
        _invalid()
    metadata = await drain_operation(
        asyncio.to_thread(_prepare, config, scope, root, fetched, candidate), preserve_cancellation=True
    )
    if metadata != state["candidate"]:
        _invalid()
    return metadata


async def _check(config, scope, root, window, state, checked_at):
    catalog = candidate = None
    if scope.source_system == "cms":
        discovery = await discover_registry_source_edition(
            scope.source_id,
            RegistrySourceEditionPolicy(scope.edition_policy, scope.reporting_year),
            root,
            previous=_receipt(state["catalog"]) if state else None,
        )
        candidate, catalog = discovery.candidate, discovery.catalog.receipt
        spec = _cms_spec(scope, candidate)
    else:
        if config.naic is None or scope.edition_policy != "fixed-year" or scope.reporting_year != 2026:
            return {"state": "review_required", "scope": _scope(scope), "window": window}
        spec = _naic_spec(config)
    prior = _receipt(state["artifact"]) if state else None
    if prior is not None and prior.spec != spec:
        prior = None
    fetched = await fetch_registry_source(spec, root, previous=prior)
    try:
        metadata = await drain_operation(
            asyncio.to_thread(_prepare, config, scope, root, fetched, candidate), preserve_cancellation=True
        )
    except RegistrySourcePreparationError, RegistrySourceAdmissionError:
        return {"state": "review_required", "scope": _scope(scope), "window": window}
    checkpoint_by_name = dict(
        scope=_scope(scope),
        window=window,
        checked_at=checked_at.isoformat(),
        profile=_profile(config, scope),
        catalog=asdict(catalog) if catalog else None,
        artifact=asdict(fetched.receipt),
        candidate=metadata,
    )
    temporary = root / ".prepared.tmp"
    try:
        temporary.write_text(json.dumps(checkpoint_by_name, default=_json_default, sort_keys=True))
        os.replace(temporary, root / "prepared.json")
    finally:
        temporary.unlink(missing_ok=True)
    return {"state": "prepared", "scope": _scope(scope), "window": window, "candidate": metadata}


async def run_registry_source_check(config, params, *, now=None):
    """Check one allowlisted scope; retained candidates never become serving authority."""
    if type(config) is not RegistrySourceCheckConfig:
        _invalid()
    scope = validate_registry_source_check_scope(params)
    if scope not in config.scopes:
        _invalid()
    instant = now or datetime.now(timezone.utc)
    batch = build_registry_source_schedule_intents([scope], instant)
    if not batch.intents:
        return {"state": "not_due", "scope": params}
    window = batch.intents[0].dedup_key
    with _cache(config, scope) as root:
        if root is None:
            return {"state": "busy", "scope": params}
        state = _load(root, scope)
        if state and state["profile"] != _profile(config, scope):
            state = None
        if state and state["window"] == window:
            checked_at = datetime.fromisoformat(state["checked_at"])
            prior_batch = build_registry_source_schedule_intents([scope], checked_at)
            if checked_at > instant or not prior_batch.intents or prior_batch.intents[0].dedup_key != window:
                _invalid()
            metadata = await _retained(config, scope, root, state)
            return {"state": "already_checked", "scope": params, "window": window, "candidate": metadata}
        try:
            return await _check(config, scope, root, window, state, instant)
        except asyncio.CancelledError:
            raise
        except RegistrySourceFetchError, RegistrySourceEditionDiscoveryError:
            return {"state": "unavailable", "scope": params, "window": window}


async def control_start(ctx, task):
    """Use existing control cancellation and terminal projection with logical selectors."""
    from process.control_cancel import ImportCancelledError, raise_if_cancelled
    from process.registry_cms_source_worker import _watch_control_cancel

    _object(task, SCOPE_FIELDS | {"run_id"})
    if (
        type(task["run_id"]) is not str
        or not 0 < len(task["run_id"]) <= 128
        or task["run_id"] != task["run_id"].strip()
    ):
        _invalid()
    params_by_name = {name: task[name] for name in SCOPE_FIELDS}
    config = parse_registry_source_check_config(os.environ.get(CONFIG_ENV))
    await raise_if_cancelled(ctx, task)
    work = asyncio.create_task(run_registry_source_check(config, params_by_name))
    watcher = asyncio.create_task(_watch_control_cancel(ctx, task))
    try:
        await asyncio.wait((work, watcher), return_when=asyncio.FIRST_COMPLETED)
        if work.done():
            return await work
        await watcher
    except asyncio.CancelledError, ImportCancelledError:
        raise
    except Exception:
        raise RuntimeError("registry_source_check_failed") from None
    finally:
        for owned in (work, watcher):
            if not owned.done():
                owned.cancel()
        await asyncio.gather(work, watcher, return_exceptions=True)


class _CheckCommand(click.Command):
    def parse_args(self, context, arguments):
        """Refuse unsupported CLI values without reflecting them into diagnostics."""
        if arguments and arguments != ["--help"]:
            print('{"state":"error","code":"invalid_arguments"}')
            context.exit(2)
        return super().parse_args(context, arguments)


@click.command(name="registry-source-check", cls=_CheckCommand)
def registry_source_check_command():
    """Check every explicitly configured scope without accepting source data."""
    try:
        config = parse_registry_source_check_config(os.environ.get(CONFIG_ENV))

        async def run():
            """Run the configured scopes in one serial batch."""
            return [await run_registry_source_check(config, _scope(scope)) for scope in config.scopes]

        result = asyncio.run(run())
    except KeyboardInterrupt, asyncio.CancelledError:
        raise click.exceptions.Exit(130) from None
    except Exception:
        print('{"state":"error","code":"registry_source_check_failed"}')
        raise click.exceptions.Exit(1) from None
    print(json.dumps(result, separators=(",", ":")))
