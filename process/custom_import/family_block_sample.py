# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Compare one deterministic fifth of compact families with admitted SOURCE records."""

import hashlib
from collections import Counter
from contextlib import closing
from itertools import chain, groupby, zip_longest
from operator import itemgetter

from sqlalchemy import LargeBinary, and_, func, literal, select

from db.models.custom_import import CustomImportChildRevision, CustomImportRootRevision
from db.models.custom_import_family_block import CustomImportFamilyBlockLayout
from db.models.custom_import_storage import CustomImportSnapshotFamily
from process.custom_import import build_graph_prepare_page as prepare
from process.custom_import.build_source import _prepare_statement
from process.custom_import.family_block_binding import FamilyBlockUnavailable, checked_binding
from process.custom_import.family_block_codec import FamilyBlockError
from process.custom_import.family_block_read import _directory, _membership, _stream_rows, verified_family
from process.custom_import.family_block_retained import _remaining_limits
from process.custom_import.family_block_storage_codec import verify_stored_family
from process.custom_import.runner_codec import digest_text
from process.custom_import.storage_layout import snapshot_models

_SAMPLE_PAGE = 128


def _family_page(binding, source_root, seed, after):
    family = binding.tables_by_role["family"]
    root = binding.tables_by_role["root_record"]
    priority = func.sha256(literal(seed, LargeBinary()).op("||")(root.c.logical_key_sha256))
    bucket = (func.get_byte(priority, 0) * 256 + func.get_byte(priority, 1)) % 5
    return (
        select(
            family,
            root.c.logical_key_sha256,
            source_root.pack_id.label("source_pack_id"),
            source_root.payload_sha256.label("source_payload_sha256"),
            source_root.source_ordinal.label("source_ordinal"),
        )
        .select_from(family)
        .join(root, root.c.root_record_id == family.c.root_record_id)
        .outerjoin(
            source_root,
            and_(
                source_root.root_revision_id == family.c.root_revision_id,
                source_root.root_record_id == family.c.root_record_id,
                source_root.dataset_id == root.c.dataset_id,
            ),
        )
        .where(family.c.family_revision_id > after, bucket == 0)
        .order_by(family.c.family_revision_id)
        .limit(_SAMPLE_PAGE)
    )


def _related_rows(binding, role, family_ids, *order):
    table, family = binding.tables_by_role[role], binding.tables_by_role["family"]
    root_id = family.c.root_record_id
    return (
        select(table, root_id)
        .join(family, family.c.family_revision_id == table.c.family_revision_id)
        .where(family.c.family_revision_id.in_(family_ids))
        .order_by(root_id, *(table.c[name] for name in order))
    )


def _expected_children(request, registry, build_id, candidate_models, root_ids):
    source, keys, _ = prepare._children_statement(
        request, registry, build_id, root_ids, candidate_models, phase="verifying"
    )
    child = candidate_models[CustomImportChildRevision]
    return source.with_only_columns(
        keys[0].label("root_record_id"),
        keys[1].label("collection"),
        child.child_revision_id.label("revision_id"),
        child.pack_id,
        child.source_ordinal,
        child.child_key_sha256.label("key_sha256"),
        child.payload_sha256,
        source.selected_columns.scope_valid,
        maintain_column_froms=True,
    ).order_by(*keys)


def _root_evidence(row):
    return (
        None,
        row["root_revision_id"],
        row["source_pack_id"],
        row["source_ordinal"],
        row["logical_key_sha256"],
        row["source_payload_sha256"],
        row["source_payload_sha256"] is not None
        and row["source_payload_sha256"] == row["root_payload_sha256"]
        and row["source_ordinal"] == row["root_source_ordinal"],
    )


def _children_evidence(rows):
    for row in rows:
        yield (
            row["collection"],
            row["revision_id"],
            row["pack_id"],
            row["source_ordinal"],
            row["key_sha256"],
            row["payload_sha256"],
            row["scope_valid"],
        )


def _verify_one(binding, family_row, fragments, runs, children, limits, check, counts):
    source_records = chain((_root_evidence(family_row),), _children_evidence(children))

    def observe(collection, record, parts):
        """Compare the decoded identity and canonical bytes to the admitted row."""
        key_hash, key_text, payload = parts
        if collection is None:
            key_hash = digest_text("root-key", key_text)
        digest = digest_text("child-payload" if collection is not None else "root-payload", payload)
        expected = next(source_records, None)
        actual = (collection, record.revision_id, record.pack_id, record.source_ordinal, key_hash, digest, True)
        if actual != expected:
            raise FamilyBlockUnavailable("sampled compact record differs from admitted SOURCE")
        counts[collection] += 1

    layout = binding.layout
    try:
        with verify_stored_family(
            layout.definition,
            layout.collection_slots,
            ((_directory(fragment), fragment["encoded_fragment"]) for fragment in fragments),
            (_membership(run) for run in runs),
            binding.manifest(family_row),
            limits=limits,
            check=check,
            observe=observe,
        ):
            if next(source_records, None) is not None:
                raise FamilyBlockUnavailable("sampled compact family omits an admitted record")
    except FamilyBlockError as error:
        raise FamilyBlockUnavailable("sampled compact family could not be decoded") from error


def _take_group(groups, root_id):
    """Advance one ordered cursor, leaving a missing group's successor untouched."""
    current, remaining = groups
    if current is None or current[0] > root_id:
        return
    if current[0] != root_id:
        raise FamilyBlockUnavailable("compact sample cursor contains an unexpected family")
    yield from current[1]
    groups[0] = next(remaining, None)


def _grouped(rows):
    groups = groupby(rows, key=itemgetter("root_record_id"))
    return [next(groups, None), groups]


def _sample_page(session, binding, page, child_statement, limits, check, counts):
    family_ids = tuple(row["family_revision_id"] for row in page)
    fragments = _related_rows(binding, "fragment", family_ids, "fragment_ordinal")
    runs = _related_rows(binding, "revision_run", family_ids, "collection_slot", "first_revision_id")
    with (
        closing(_stream_rows(session, fragments)) as fragment_rows,
        closing(_stream_rows(session, runs)) as run_rows,
        closing(_stream_rows(session, child_statement)) as source_rows,
    ):
        fragment_groups, run_groups, child_groups = map(_grouped, (fragment_rows, run_rows, source_rows))
        for row in sorted(page, key=itemgetter("root_record_id")):
            root_id = row["root_record_id"]
            _verify_one(
                binding,
                row,
                _take_group(fragment_groups, root_id),
                _take_group(run_groups, root_id),
                _take_group(child_groups, root_id),
                limits,
                check,
                counts,
            )


def _compare_retained_records(collections, copied, original, check, sampled_by_collection):
    for name in collections:
        actual, expected = copied.decoded_records(name), original.decoded_records(name)
        for new, old in zip_longest(actual, expected):
            if check is not None:
                check()
            if (
                new is None
                or old is None
                or (new[0].revision_id, new[1][3:], new[2]) != (old[0].revision_id, old[1][3:], old[2])
            ):
                raise FamilyBlockUnavailable("sampled retained record differs from its base")
            sampled_by_collection[name] += 1


async def _sample_retained(session, binding, base, family_row, limits, check, counts):
    family = base.tables_by_role["family"]
    await _prepare_statement(session)
    before = (
        (
            await session.execute(
                select(family).where(
                    family.c.family_revision_id == family_row["family_revision_id"],
                    family.c.root_record_id == family_row["root_record_id"],
                )
            )
        )
        .mappings()
        .one_or_none()
    )
    if before is None or before["family_sha256"] != family_row["family_sha256"]:
        raise FamilyBlockUnavailable("sampled retained family differs from its base")
    await _prepare_statement(session)
    with await verified_family(session, base, before, limits=limits) as original:
        await _prepare_statement(session)
        with await verified_family(
            session, binding, family_row, limits=_remaining_limits(limits, before["raw_bytes"])
        ) as copied:
            _compare_retained_records((None, *binding.layout.collection_slots), copied, original, check, counts)


async def _sample_base(session, request, registry):
    snapshot = CustomImportSnapshotFamily
    layout = CustomImportFamilyBlockLayout
    await _prepare_statement(session)
    family_id = await session.scalar(
        select(snapshot.family_id)
        .join(layout, snapshot.family_id == layout.family_id)
        .where(
            snapshot.dataset_id == request.dataset_id,
            snapshot.generation_id == request.expected_base_generation_id,
            layout.phase == "logged",
        )
    )
    if family_id is None:
        raise FamilyBlockUnavailable("sampled retained family has no compact base")
    return checked_binding(family_id, request.definition, registry.child_collection_slots)


async def audit_block_sample(session, binding, request, registry, build_id, seed, *, limits, check=None):
    """Read scoped SOURCE fingerprints and compact payloads in bounded family batches."""
    if not session.in_transaction() or type(seed) is not bytes or len(seed) != 32:
        raise FamilyBlockUnavailable("compact sample requires a frozen candidate and a 32-byte seed")
    candidate_models = snapshot_models(binding.family_id, schema_name=binding.tables_by_role["family"].schema)
    source_root = candidate_models[CustomImportRootRevision]
    base = None
    selection = hashlib.sha256(seed)
    sampled_by_collection = Counter()
    after = 0
    while True:
        if check is not None:
            check()
        await _prepare_statement(session)
        page = (await session.execute(_family_page(binding, source_root, seed, after))).mappings().all()
        if not page:
            return selection.hexdigest(), sampled_by_collection
        source_rows = page
        if request.expected_base_generation_id is not None:
            retained_rows = [family_row for family_row in page if family_row["retained"]]
            if retained_rows:
                base = base or await _sample_base(session, request, registry)
                for family_row in retained_rows:
                    await _sample_retained(session, binding, base, family_row, limits, check, sampled_by_collection)
                source_rows = [family_row for family_row in page if not family_row["retained"]]
        if source_rows:
            children = _expected_children(
                request,
                registry,
                build_id,
                candidate_models,
                tuple(family_row["root_record_id"] for family_row in source_rows),
            )
            await _prepare_statement(session)
            await session.run_sync(
                lambda sync: _sample_page(sync, binding, source_rows, children, limits, check, sampled_by_collection)
            )
        selection.update(b"".join(family_row["logical_key_sha256"] for family_row in page))
        after = page[-1]["family_revision_id"]
