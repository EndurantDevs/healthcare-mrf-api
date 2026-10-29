# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Protected, attempt-bound evidence for the NPI-only witnessed alias profile."""

from __future__ import annotations

import asyncio
import datetime as dt
import hashlib
import json
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field

from sqlalchemy import text

from process.npi_isolated_execution import NPI_TABLES, NpiInputRelation

WITNESS_CONTRACT = "npi-address-alias-witness.v1"
MODEL_CONTRACT = "npi-result-family.postgres.alias.v1"
METADATA_KEY = "npi_address_witness"
_ISSUER = object()
_SHA = re.compile(r"[0-9a-f]{64}\Z", re.ASCII)
INPUTS = ("address_archive_v2", "address_alias_v1", "address_alias_state_v1")
SETS = ("raw", "strict", "effective", "aliases", "absent_strict", "canonical")


def _require(condition):
    if not condition:
        raise RuntimeError("NPI witnessed alias evidence differs")


def canonical_bytes(witness_data):
    """Encode portable evidence with deterministic JSON bytes."""
    return json.dumps(witness_data, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()


def observation_time(witness_data):
    """Keep the Publisher-issued UTC observation instant stable across replay."""
    if isinstance(witness_data, str):
        try:
            witness_data = dt.datetime.fromisoformat(witness_data)
        except ValueError:
            raise RuntimeError("NPI observation time differs") from None
    _require(
        type(witness_data) is dt.datetime
        and witness_data.tzinfo is not None
        and witness_data.utcoffset() == dt.timedelta(0)
    )
    return witness_data.astimezone(dt.timezone.utc)


@dataclass(frozen=True)
class NpiAddressWitness:
    """Portable data only; this object never grants publication authority."""

    encoded: bytes = field(repr=False)

    def as_dict(self):
        """Decode a detached portable evidence mapping."""
        return json.loads(self.encoded)

    @property
    def sha256(self):
        """Digest the exact deterministic witness encoding."""
        return hashlib.sha256(self.encoded).hexdigest()


def validate_npi_address_witness(witness_data):
    """Validate closed portable data without granting runtime authority."""
    if type(witness_data) is NpiAddressWitness:
        witness_data = witness_data.as_dict()
    fields = {
        "contract",
        "run_id",
        "attempt_id",
        "attempt_started_at",
        "observation_time",
        "input_receipt_sha256",
        "inputs",
        "alias_generation",
        "counts",
        "hashes",
        "identity_version",
        "ruleset_version",
        "formatter_version",
        "strict_observations",
    }
    _require(isinstance(witness_data, Mapping) and set(witness_data) == fields)
    _require(
        witness_data["contract"] == WITNESS_CONTRACT
        and type(witness_data["input_receipt_sha256"]) is str
        and _SHA.fullmatch(witness_data["input_receipt_sha256"])
    )
    _require(
        type(witness_data["run_id"]) is str and bool(witness_data["run_id"]) and "\0" not in witness_data["run_id"]
    )
    _require(
        type(witness_data["attempt_id"]) is str and witness_data["attempt_id"].startswith(witness_data["run_id"] + ":")
    )
    started, observed = (
        observation_time(witness_data["attempt_started_at"]),
        observation_time(witness_data["observation_time"]),
    )
    _require(observed >= started)
    _require(type(witness_data["alias_generation"]) is int and witness_data["alias_generation"] >= 0)
    _require(type(witness_data["strict_observations"]) is int and witness_data["strict_observations"] == 0)
    _require(isinstance(witness_data["inputs"], Mapping) and set(witness_data["inputs"]) == set(INPUTS))
    for name in INPUTS:
        relation = witness_data["inputs"][name]
        _require(
            isinstance(relation, Mapping)
            and set(relation) == {"schema_name", "table_name", "relation_oid", "relfilenode"}
        )
        parsed = NpiInputRelation(**relation)
        _require(parsed.table_name == name)
    _require(witness_data["inputs"][INPUTS[1]]["schema_name"] == witness_data["inputs"][INPUTS[2]]["schema_name"])
    _require(isinstance(witness_data["counts"], Mapping) and set(witness_data["counts"]) == set(SETS))
    _require(all(type(x) is int and x >= 0 for x in witness_data["counts"].values()))
    _require(isinstance(witness_data["hashes"], Mapping) and set(witness_data["hashes"]) == set(SETS))
    _require(all(type(x) is str and _SHA.fullmatch(x) for x in witness_data["hashes"].values()))
    versions = (witness_data["identity_version"], witness_data["ruleset_version"], witness_data["formatter_version"])
    _require(all(type(version) is int for version in versions) and versions == (2, 4, 2))
    encoded = canonical_bytes(dict(witness_data))
    _require(len(encoded) <= 12_288)
    return NpiAddressWitness(encoded)


@dataclass(frozen=True)
class NpiAliasWitnessAuthority:
    """One constructor-authenticated native transaction, never a JSON capability."""

    session: object = field(repr=False)
    transaction: object = field(repr=False)
    backend: tuple[int, int, int, int]
    task: object = field(repr=False)
    relation_oids: tuple[tuple[str, int], ...]
    witness: NpiAddressWitness
    verify_witness: Callable = field(repr=False, compare=False)
    _issuer: object = field(repr=False, compare=False)

    def __post_init__(self):
        _require(self._issuer is _ISSUER and type(self.witness) is NpiAddressWitness and callable(self.verify_witness))


async def authenticate_npi_alias_witness(session, witness, *, schema_name, verify_witness):
    """Bind only after the trusted Publisher revalidates its protected native receipt."""
    _require(session.in_transaction() and callable(verify_witness))
    witness = validate_npi_address_witness(witness)
    _require(await verify_witness(session, witness.as_dict()) is True)
    backend_ids = tuple(
        (
            await session.execute(
                text(
                    "SELECT pg_catalog.pg_backend_pid(),(SELECT oid FROM pg_catalog.pg_database WHERE datname=current_database()),"
                    "session_user::regrole::oid,current_user::regrole::oid"
                )
            )
        ).one()
    )
    _require(backend_ids[2] == backend_ids[3])
    pairs = await _output_relations(session, schema_name)
    authority = NpiAliasWitnessAuthority(
        session, session.get_transaction(), backend_ids, asyncio.current_task(), pairs, witness, verify_witness, _ISSUER
    )
    await require_npi_alias_authority(session, authority)
    return authority


async def _output_relations(session, schema):
    _require(type(schema) is str and re.fullmatch(r"[a-z_][a-z0-9_]{0,62}", schema, re.ASCII))
    pairs = []
    for name in (*NPI_TABLES, "npi_canonical_address"):
        oid = await session.scalar(
            text("SELECT pg_catalog.to_regclass(:relation)::oid"), {"relation": f'"{schema}"."{name}"'}
        )
        _require(type(oid) is int and oid > 0)
        pairs.append((name, oid))
    return tuple(pairs)


async def require_npi_alias_authority(session, authority, witness=None):
    """Reauthenticate the same task, principal, transaction and pinned native inputs."""
    _require(type(authority) is NpiAliasWitnessAuthority and authority._issuer is _ISSUER)
    _require(
        session is authority.session and session.in_transaction() and session.get_transaction() is authority.transaction
    )
    _require(asyncio.current_task() is authority.task)
    if witness is not None:
        _require(validate_npi_address_witness(witness) == authority.witness)
    backend_ids = tuple(
        (
            await session.execute(
                text(
                    "SELECT pg_catalog.pg_backend_pid(),(SELECT oid FROM pg_catalog.pg_database WHERE datname=current_database()),"
                    "session_user::regrole::oid,current_user::regrole::oid"
                )
            )
        ).one()
    )
    _require(
        backend_ids == authority.backend
        and await authority.verify_witness(session, authority.witness.as_dict()) is True
    )
    for witness_data in authority.witness.as_dict()["inputs"].values():
        relation = NpiInputRelation(**witness_data)
        matched = await session.scalar(
            text(
                "SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_class WHERE oid=pg_catalog.to_regclass(:relation) "
                "AND oid=:oid AND relfilenode=:filenode AND relkind='r' AND relpersistence='p' "
                "AND NOT relrowsecurity AND NOT relforcerowsecurity)"
            ),
            {"relation": relation.qualified, "oid": relation.relation_oid, "filenode": relation.relfilenode},
        )
        _require(matched is True)
    return authority.witness.as_dict()


async def raw_connection(session):
    """Borrow the active native connection without opening another transaction."""
    await session.execute(text("SELECT 1"))
    return (await (await session.connection()).get_raw_connection()).driver_connection


async def validate_npi_alias_contribution(session, schema, authority):
    """Rebuild the complete witnessed projection against closed heaps and pinned inputs."""
    from process.npi_isolated_addresses import verify_address_projection

    witness = await require_npi_alias_authority(session, authority)
    _require(await _output_relations(session, schema) == authority.relation_oids)
    await verify_address_projection(await raw_connection(session), schema=schema, witness=witness)
    from process.mrf_address_publication import _validate_redirect_closure

    await _validate_redirect_closure(session, f'"{schema}"."npi_canonical_address"')
    return witness


async def merge_npi_alias_contribution(session, stage, archive, authority, *, importer_id="npi"):
    """Apply the sealed effective observation, preserving unrelated global rows."""
    from db.models import AddressArchiveV2
    from process import mrf_address_publication as canonical

    _require(importer_id == "npi")
    schema = stage.split(".", 1)[0].strip('"')
    _require(stage == f'"{schema}"."npi_canonical_address"')
    await session.execute(text(f"LOCK TABLE {archive} IN SHARE ROW EXCLUSIVE MODE NOWAIT"))
    await canonical.require_canonical_publication_capability(session, archive)
    await canonical.require_canonical_publication_catalog(session, archive)
    witness = await validate_npi_alias_contribution(session, schema, authority)
    baseline = NpiInputRelation(**witness["inputs"]["address_archive_v2"]).qualified
    keys = f"SELECT address_key FROM {stage}"
    unchanged = await session.scalar(
        text(
            f"SELECT NOT EXISTS((SELECT * FROM {archive} WHERE address_key IN ({keys}) EXCEPT ALL "
            f"SELECT * FROM {baseline} WHERE address_key IN ({keys})) UNION ALL "
            f"(SELECT * FROM {baseline} WHERE address_key IN ({keys}) EXCEPT ALL "
            f"SELECT * FROM {archive} WHERE address_key IN ({keys})))"
        )
    )
    _require(unchanged is True)
    await canonical.validate_canonical_merge(session, stage, archive)
    columns = tuple(column.name for column in AddressArchiveV2.__table__.columns)
    await session.execute(
        text(
            f"INSERT INTO {archive}({','.join(columns)}) SELECT {','.join('c.' + x for x in columns)} FROM {stage} c "
            f"WHERE NOT EXISTS(SELECT 1 FROM {archive} incumbent WHERE incumbent.address_key=c.address_key)"
        )
    )
    effects = (*canonical.DISPLAY_COLUMNS, "source_bits", "strict_source_bits", "display_priority", "last_seen_at")
    left, right = ",".join("incumbent." + x for x in effects), ",".join("contribution." + x for x in effects)
    await session.execute(
        text(
            f"UPDATE {archive} incumbent SET {','.join(x + '=contribution.' + x for x in effects)} FROM {stage} contribution "
            f"WHERE incumbent.address_key=contribution.address_key AND ROW({left}) IS DISTINCT FROM ROW({right})"
        )
    )
    _require(
        await session.scalar(
            text(
                f"SELECT NOT EXISTS((SELECT * FROM {stage} EXCEPT ALL SELECT * FROM {archive} WHERE address_key IN ({keys})) "
                f"UNION ALL (SELECT * FROM {archive} WHERE address_key IN ({keys}) EXCEPT ALL SELECT * FROM {stage}))"
            )
        )
        is True
    )
    return await canonical.canonical_merge_receipt(session, archive, stage, 1, 0)


def npi_alias_archive_projection(contributions, archive, authority):
    """SQL data projection only; operational callers must authenticate the live cap."""
    from process.mrf_address_publication import canonical_archive_projection

    _require(type(authority) is NpiAliasWitnessAuthority and authority._issuer is _ISSUER)
    _require("npi" in contributions and set(contributions) <= {"npi", "mrf"})
    stage = contributions["npi"]
    base = (
        f"SELECT incumbent.* FROM {archive} incumbent WHERE NOT EXISTS(SELECT 1 FROM {stage} npi "
        f"WHERE npi.address_key=incumbent.address_key) UNION ALL SELECT * FROM {stage}"
    )
    if "mrf" in contributions:
        return canonical_archive_projection({"mrf": contributions["mrf"]}, "(" + base + ")")
    return base
