# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Register all observed SOURCE revision IDs through the existing owner boundary.

Homes are routing metadata, including initially valid revisions later eliminated
by admission. They do not assert that a record exists in a compact family block.
Retained IDs and complete candidate home verification remain separate work.
"""

from contextlib import closing
from dataclasses import dataclass
from itertools import zip_longest

from process.custom_import.build_source import _page_session, _source_cleanup_failure
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from process.custom_import.family_block_codec import FamilyBlockResourceLimit
from process.custom_import.family_block_records import _integer
from process.custom_import.family_block_store import _candidate, _control_schema, _execute
from process.custom_import.family_shard_admission import _source_entries, read_admission_occurrences
from process.custom_import.family_shard_allocate import SourceAllocationLimits
from process.custom_import.family_shard_common import SourceCommonContext
from process.custom_import.family_shard_events import _scope as _admission_scope
from process.custom_import.family_shard_facts import _sorted
from process.custom_import.family_shard_graph_control import _source_digest
from process.custom_import.family_shard_output import _pack_row
from process.custom_import.family_shard_plan import PlanWork, _cpu, _Scratch

_PHASES = {"loading": {"admission", "graph", "output"}, "closed": {"output", "verifying"}}
_MAXIMUM_NATIVE_IDS = 100_000
_ID_BYTES = 128

_HOME_OWNER = """
SELECT c.relkind='r' AND c.relpersistence='p'
    AND pg_catalog.pg_get_userbyid(c.relowner)=CURRENT_USER
    AND NOT EXISTS (
        SELECT 1 FROM pg_catalog.aclexplode(coalesce(c.relacl,pg_catalog.acldefault('r',c.relowner))) a
        WHERE a.grantee<>c.relowner
          AND a.privilege_type IN ('INSERT','UPDATE','DELETE','TRUNCATE','REFERENCES','TRIGGER','MAINTAIN'))
    AND NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_attribute x, LATERAL pg_catalog.aclexplode(x.attacl) a
        WHERE x.attrelid=c.oid AND x.attnum>0 AND NOT x.attisdropped AND a.grantee<>c.relowner
          AND a.privilege_type IN ('INSERT','UPDATE','REFERENCES')) AS owner_only
FROM pg_catalog.pg_class c WHERE c.oid=pg_catalog.to_regclass(:home)
"""

_APPEND = """
WITH requested AS MATERIALIZED (
    SELECT * FROM unnest(CAST(:kinds AS smallint[]),CAST(:ids AS bigint[])) r(revision_kind,revision_id)
), wanted AS MATERIALIZED (
    SELECT revision_kind,range_agg(int8range(revision_id,
        CASE WHEN revision_id=9223372036854775807 THEN NULL ELSE revision_id+1 END,'[)')) AS revision_ids
    FROM requested GROUP BY revision_kind
), matched AS MATERIALIZED (
    SELECT h.revision_kind,h.family_id,h.revision_ids*w.revision_ids AS revision_ids
    FROM {control}.custom_import_revision_home h JOIN wanted w
      ON w.revision_kind=1 AND h.revision_ids&&w.revision_ids WHERE h.revision_kind=1
    UNION ALL
    SELECT h.revision_kind,h.family_id,h.revision_ids*w.revision_ids
    FROM {control}.custom_import_revision_home h JOIN wanted w
      ON w.revision_kind=2 AND h.revision_ids&&w.revision_ids WHERE h.revision_kind=2
), conflicts AS MATERIALIZED (
    SELECT EXISTS(SELECT 1 FROM matched WHERE family_id<>:family_id)
        OR EXISTS(SELECT 1 FROM requested r JOIN {control}.custom_import_root_revision c
            ON c.root_revision_id=r.revision_id WHERE r.revision_kind=1)
        OR EXISTS(SELECT 1 FROM requested r JOIN {control}.custom_import_child_revision c
            ON c.child_revision_id=r.revision_id WHERE r.revision_kind=2) AS invalid
), missing AS MATERIALIZED (
    SELECT r.* FROM requested r WHERE NOT EXISTS(
        SELECT 1 FROM matched m WHERE m.revision_kind=r.revision_kind AND m.revision_ids@>r.revision_id)
), appended AS (
    INSERT INTO {control}.custom_import_revision_home(family_id,revision_kind,revision_ids)
    SELECT :family_id,revision_kind,range_agg(int8range(revision_id,
        CASE WHEN revision_id=9223372036854775807 THEN NULL ELSE revision_id+1 END,'[)'))
    FROM missing WHERE NOT (SELECT invalid FROM conflicts) GROUP BY revision_kind ORDER BY revision_kind
    RETURNING revision_kind,revision_ids
)
SELECT (SELECT invalid FROM conflicts),
    (SELECT count(*) FROM requested WHERE revision_kind=1),
    (SELECT count(*) FROM requested WHERE revision_kind=2),
    (SELECT count(*) FROM missing WHERE revision_kind=1),
    (SELECT count(*) FROM missing WHERE revision_kind=2),
    (SELECT count(*) FROM appended),
    (SELECT count(*) FROM missing m JOIN appended a
        ON a.revision_kind=m.revision_kind AND a.revision_ids@>m.revision_id)
"""


@dataclass(frozen=True)
class SourceHomeCounts:
    """SOURCE-only accounting; no retained coverage or candidate acceptance."""

    root_count: int
    child_count: int
    inserted_root_count: int
    inserted_child_count: int


def _matching_occurrence(original, admitted):
    if original is None or admitted is None:
        raise FamilyBlockUnavailable("SOURCE home admission occurrence coverage differs")
    actual = (
        admitted.coordinate,
        admitted.encoded_record,
        admitted.pack_id,
        admitted.root_record_id,
        admitted.revision_id,
        admitted.initial_rejection_id,
        admitted.initial_rejection_ordinal,
        admitted.collection_slot,
        admitted.landing,
    )
    expected = (
        original.coordinate,
        original.encoded_record,
        original.pack_id,
        original.root_record_id,
        original.revision_id,
        original.rejection_id,
        original.rejection_ordinal,
        original.collection_slot,
        original.landing,
    )
    if _pack_row(actual) != _pack_row(expected):
        raise FamilyBlockUnavailable("SOURCE home original allocation or admission differs")


def _initial_ids(work, common, admission):
    count = 0
    with (
        closing(_source_entries(common.source, common.request.definition, work.current)) as originals,
        closing(read_admission_occurrences(admission, check=work.current)) as occurrences,
    ):
        for original, admitted in zip_longest(originals, occurrences):
            work.current()
            _matching_occurrence(original, admitted)
            count += 1
            if original.revision_id is not None:
                yield 1 if original.collection_slot == 0 else 2, original.revision_id
    if count != common.source.source.counts[0]:
        raise FamilyBlockUnavailable("SOURCE home complete occurrence census differs")


def _stage_ids(work, common, admission):
    counts_by_kind, previous = {1: 0, 2: 0}, None
    path = work.path()
    with (
        path.open("xb") as staged,
        closing(_sorted(work, _initial_ids(work, common, admission), lambda pair: pair)) as pairs,
    ):
        for pair in pairs:
            work.current()
            if pair == previous:
                raise FamilyBlockUnavailable("SOURCE home actual revision ID repeats")
            previous = pair
            kind, identifier = pair
            _integer(identifier, "SOURCE home revision")
            counts_by_kind[kind] += 1
            work.frame(staged, pair, path)
    counts = counts_by_kind[1], counts_by_kind[2]
    if counts != common.source.source.counts[2:4]:
        raise FamilyBlockUnavailable("SOURCE home initial revision census differs")
    return path, counts


def _batches(work, path, limits):
    maximum_rows = min(limits.maximum_batch_records, _MAXIMUM_NATIVE_IDS)
    maximum_bytes = min(limits.maximum_batch_bytes, work.limits.memory_bytes)
    pending_ids, count = [], 0
    with closing(work.rows(path)) as pairs:
        for pair in pairs:
            if _ID_BYTES > maximum_bytes:
                raise FamilyBlockResourceLimit("SOURCE home ID exceeds the operational batch budget")
            if pending_ids and (len(pending_ids) == maximum_rows or (len(pending_ids) + 1) * _ID_BYTES > maximum_bytes):
                yield tuple(pending_ids)
                pending_ids = []
            pending_ids.append(pair)
            count += 1
        if pending_ids or count == 0:
            yield tuple(pending_ids)


def _build_state(common, build, descriptor):
    capture = dict(common.source.source.scope[0])["capture_bundle_id"]
    if (
        build is None
        or descriptor.phase not in _PHASES
        or build.phase not in _PHASES[descriptor.phase]
        or build.source_frozen_at is None
        or build.source_occurrence_count != common.source.source.counts[0]
        or build.capture_bundle_id != capture
        or build.verified_at is not None
    ):
        raise FamilyBlockUnavailable("SOURCE homes require frozen original SOURCE and an unfinished candidate")


async def _owned_home(session):
    quote, schema = await _control_schema(session)
    control = quote.quote_schema(schema)
    home = control + ".custom_import_revision_home"
    await _execute(session, f"LOCK TABLE {home} IN ROW EXCLUSIVE MODE")
    owner = (await _execute(session, _HOME_OWNER, {"home": home})).scalar_one_or_none()
    if owner is not True:
        raise FamilyBlockUnavailable("SOURCE home table owner or write ACL differs")
    return control


async def _append_batch(session, family_id, batch):
    control = await _owned_home(session)
    bind_by_name = {
        "family_id": family_id,
        "kinds": [kind for kind, _identifier in batch],
        "ids": [identifier for _kind, identifier in batch],
    }
    native_counts = tuple((await _execute(session, _APPEND.format(control=control), bind_by_name)).one())
    expected = (sum(kind == 1 for kind, _identifier in batch), sum(kind == 2 for kind, _identifier in batch))
    if len(native_counts) != 7 or native_counts[0] is not False:
        raise FamilyBlockUnavailable("SOURCE revision overlaps canonical or foreign home storage")
    for count in native_counts[1:]:
        _integer(count, "SOURCE native home count", zero=True)
    if (
        native_counts[1:3] != expected
        or any(inserted > count for inserted, count in zip(native_counts[3:5], expected, strict=True))
        or native_counts[5] != sum(count > 0 for count in native_counts[3:5])
        or native_counts[6] != sum(native_counts[3:5])
    ):
        raise FamilyBlockUnavailable("SOURCE native home batch coverage differs")
    return native_counts[3:5]


async def _register_batch(factory, common, build_id, batch, check):
    check()
    async with _page_session(factory, common.request, build_id) as (session, build):
        descriptor, _binding = await _candidate(
            session, common.request, common.source.snapshot_family_id, common.layout, set(_PHASES)
        )
        _build_state(common, build, descriptor)
        await _source_digest(session, common)
        check()
        inserted = await _append_batch(session, common.source.snapshot_family_id, batch)
        check()
        return inserted


async def register_source_homes(factory, common, build_id, admission, *, work, limits):
    """Append only missing observed SOURCE IDs after complete original EOF proof.

    Partial committed batches are safe to replay. Every retry must re-read the
    complete original artifacts and reacquire current authority, including empty
    input. The final source-plus-retained home comparison remains caller-owned.
    """
    if (
        type(common) is not SourceCommonContext
        or type(work) is not PlanWork
        or type(limits) is not SourceAllocationLimits
    ):
        raise ValueError("SOURCE homes require original owner inputs and explicit resource bounds")
    _integer(build_id, "SOURCE home build")
    _admission_scope(common.request, common.layout, common.source, admission)
    scratch, primary, home_counts = _Scratch(work), None, None
    try:
        path, counts = await _cpu(scratch, _stage_ids, scratch, common, admission)
        inserted_roots, inserted_children = 0, 0
        with closing(_batches(scratch, path, limits)) as batches:
            while (batch := await _cpu(scratch, next, batches, None)) is not None:
                inserted = await _register_batch(factory, common, build_id, batch, scratch.current)
                inserted_roots += inserted[0]
                inserted_children += inserted[1]
        home_counts = SourceHomeCounts(*counts, inserted_roots, inserted_children)
    except BaseException as error:
        primary = error
    try:
        scratch.close()
    except BaseException as error:
        primary = _source_cleanup_failure(primary, error)
    if primary is not None:
        raise primary
    return home_counts
