# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Only original logical slots and exact checkpoint-referenced roots may retire."""

import os
import re
from dataclasses import astuple, dataclass, field
from pathlib import Path

from process.custom_import import family_shard_checkpoint as checkpoint
from process.custom_import import family_shard_output_checkpoint as output
from process.custom_import import family_shard_retained_checkpoint as retained
from process.custom_import import family_shard_retirement_files as inventory
from process.custom_import import family_shard_source_checkpoint as source
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from process.custom_import.family_shard_retirement_files import _directory

_RUN = r"run-[0-9]{9}\.mpk"
_INPUT = r"shard-(?:[0-9]+|invalid)-[0-9]{6}\.mpk"
_VECTOR = r"(?:vectors|metadata-family|metadata-query)-[0-9a-f]{32}\.mpk"
_WORK_FILES = {
    "output.mpk",
    "family-plan.mpk",
    "retained-input.mpk",
    "retained-destination.mpk",
    "admission.mpk",
    "events.mpk",
    "winners.mpk",
    "source-ids.mpk",
    "source-plan.mpk",
    "families.mpk",
    "query-keys.mpk",
    "owner-metadata.json",
    "joined.mpk",
}


class RetiredFilePolicy:
    """Closed directory names, writer file forms and recorded directory identities."""

    def __init__(self, root):
        self.root = Path(root)
        self.by_directory = {"": ({".owner-job.lock"}, ())}
        self.modes_by_file = {}

    def add(self, path, *, names=(), patterns=(), identity=None):
        """Admit an exact descendant root, checking any committed directory identity."""
        path = Path(path)
        if not path.is_absolute() or not path.is_relative_to(self.root) or path == self.root:
            raise FamilyBlockUnavailable("retirement artifact lies outside its original owner")
        relative = path.relative_to(self.root)
        if any(part in {".", ".."} for part in relative.parts):
            raise FamilyBlockUnavailable("retirement artifact path differs")
        if identity is not None and _directory(path)[: len(identity)] != identity:
            raise FamilyBlockUnavailable("retirement artifact directory changed")
        for ancestor in relative.parents:
            self.by_directory.setdefault("" if str(ancestor) == "." else str(ancestor), (set(), ()))
        value = self.by_directory.setdefault(str(relative), (set(), ()))
        value[0].update(names)
        self.by_directory[str(relative)] = (value[0], tuple(dict.fromkeys((*value[1], *patterns))))

    def work(self, path, identity=None):
        """Admit only known closed files and bounded run names in this exact slot."""
        self.add(path, names=_WORK_FILES, patterns=(_RUN,), identity=identity)

    def descriptor(self, artifact):
        """Keep a closed producer's root and native-recorded directory identity exact."""
        if type(artifact) is not tuple or len(artifact) < 3 or type(artifact[0]) is not str:
            raise FamilyBlockUnavailable("retirement closed descriptor differs")
        identity = _checkpoint_identity(artifact[1]) if type(artifact[1]) is tuple else artifact[1:3]
        self.work(artifact[0], identity)

    def names(self, relative, names):
        """Return only names admitted by this exact directory's writer contract."""
        rule = self.by_directory.get(relative)
        if rule is None:
            return set()
        fixed, patterns = rule
        return {
            name
            for name in names
            if name in fixed
            or (name if not relative else relative + "/" + name) in self.by_directory
            or any(re.fullmatch(pattern, name) is not None for pattern in patterns)
        }

    def is_directory(self, relative):
        """Distinguish admitted directories from names reserved for regular files."""
        return relative in self.by_directory

    def file_modes(self, relative):
        """Preserve original metadata writer modes only for its verified exact files."""
        parent = str(Path(relative).parent)
        if parent == str(Path("output") / output._REPLAY_SLOT) and parent in self.by_directory:
            return 0o400, 0o600, 0o644
        return self.modes_by_file.get(relative, (0o400, 0o600))


def _original_slots(policy, snapshot):
    root = policy.root
    policy.work(root / "base-keys")
    policy.add(root / "source")
    for slot in ("metadata", "ready-scratch", "tasks", "admission", "counts", "homes", "common"):
        policy.work(root / "source" / slot)
    policy.add(root / "source/input", names={"owner.json", "manifest.json"}, patterns=(_INPUT,))
    policy.add(root / "source/plan", names={"source-plan.mpk"})
    policy.work(root / "source/route")
    journal = root / "source/allocation-journal"
    policy.add(journal)
    policy.add(journal / "vectors", patterns=(_VECTOR,))
    policy.add(journal / "outputs")
    # Uncommitted pending or orphan checkpoint files do not become cleanup authority.
    names = {checkpoint._OWNER}
    for head, _record in snapshot.records():
        names.add(checkpoint._filename(head))
    policy.add(journal / "checkpoints", names=names)
    ready = root / "source/ready"
    policy.add(ready, names={source._DESCRIPTOR})
    policy.add(ready / "input", names={"owner.json", "manifest.json", "storage.json"}, patterns=(_INPUT,))
    for slot in ("plan", "allocations", "routed", "metadata", "tasks"):
        policy.work(ready / slot)


def _retained_slots(policy, cells):
    if cells is None:
        return
    if Path(cells[2]) != policy.root:
        raise FamilyBlockUnavailable("retirement retained owner differs")
    policy.add(cells[3][0], identity=_checkpoint_identity(cells[3][1]))
    artifacts = cells[5]
    for index in (0, 1, 2, 4):
        if artifacts[index] is not None:
            policy.descriptor(artifacts[index])


def _output_slots(policy, cells):
    if cells is None:
        return
    if Path(cells[7][0]) != policy.root:
        raise FamilyBlockUnavailable("retirement OUTPUT owner differs")
    policy.add(cells[3][0], identity=_checkpoint_identity(cells[3][1]))
    for descriptor in cells[4][:2]:
        policy.descriptor(descriptor)
    artifacts = cells[5]
    for index in (1, 2, 3):
        if artifacts[index] is not None:
            policy.descriptor(artifacts[index])
    if artifacts[1] is not None:
        output_root = Path(cells[3][0])
        if output_root != policy.root / "output" or Path(artifacts[1][0]) != output_root / "source-families":
            raise FamilyBlockUnavailable("retirement prepared SOURCE slot differs")
        policy.work(output_root / output._REPLAY_SLOT)
    if artifacts[5] is not None and artifacts[5][0]:
        policy.descriptor(artifacts[5][1])


def _checkpoint_identity(identity):
    # SOURCE-ready closed-file helpers use (dev, ino, mode, uid); the lifetime
    # root/journal custody uses (dev, ino, uid, mode). Keep both contracts exact.
    if type(identity) is not tuple or len(identity) != 4:
        raise FamilyBlockUnavailable("retirement checkpoint directory differs")
    return identity[0], identity[1], identity[3], identity[2]


@dataclass
class _ReadyCensus:
    files_by_key: dict = field(default_factory=dict)
    directories_by_role: dict = field(default_factory=dict)
    task_count: int = 0
    total_bytes: int = 0
    footer: tuple | None = None


def _ready_file(census, ready, frame, limits, check):
    _, role, name, expected, expected_sha = frame
    source._name(name)
    if (
        role not in census.directories_by_role
        or (role, name) in census.files_by_key
        or (source._hash_file(ready / role / name, check=check) != (expected, expected_sha))
    ):
        raise FamilyBlockUnavailable("retirement SOURCE-ready file changed")
    census.files_by_key[role, name] = expected[-1], expected_sha
    census.total_bytes += expected[-1]
    if census.total_bytes > limits.max_spool_bytes:
        raise FamilyBlockUnavailable("retirement SOURCE-ready exceeds its budget")


def _ready_frame(census, ready, frame, limits, check):
    source._tuple(frame)
    if census.footer is not None:
        raise FamilyBlockUnavailable("retirement SOURCE-ready has trailing frames")
    if len(frame) == 3 and frame[0] == "directory":
        _, role, identity = frame
        if (
            role not in source._ROLES
            or role in census.directories_by_role
            or source._directory(ready / role) != identity
        ):
            raise FamilyBlockUnavailable("retirement SOURCE-ready directory changed")
        census.directories_by_role[role] = identity
    elif len(frame) == 5 and frame[0] == "file":
        _ready_file(census, ready, frame, limits, check)
    elif len(frame) == 3 and frame[0] == "task" and census.task_count < 4096:
        census.task_count += 1
    elif len(frame) == 4 and frame[0] == "end":
        census.footer = frame
    else:
        raise FamilyBlockUnavailable("retirement SOURCE-ready frame differs")


def _ready_manifest(ready_record, root, limits, check):
    ready = Path(root) / "source/ready"
    stamp, digest = source._hash_file(ready / source._DESCRIPTOR, check=check)
    checkpoint_cells = source._checkpoint_payload(ready_record)
    if checkpoint_cells != (checkpoint_cells[0], str(ready), source._directory(ready), stamp) or (
        digest != ready_record.input_sha256 or dict(ready_record.commitments).get("descriptor_sha256") != digest
    ):
        raise FamilyBlockUnavailable("retirement SOURCE-ready descriptor changed")
    census = _ReadyCensus()
    with os.fdopen(os.open(ready / source._DESCRIPTOR, os.O_RDONLY | os.O_NOFOLLOW), "rb") as stream:
        frames = source._read_frames(stream, limits, check)
        header = source._tuple(next(frames, None), 10)
        if header[:4] != ("header", checkpoint_cells[0], str(ready), source._directory(ready)) or (
            header[9] != dict(ready_record.commitments).get("prepared_sha256")
        ):
            raise FamilyBlockUnavailable("retirement SOURCE-ready header changed")
        for frame in frames:
            _ready_frame(census, ready, frame, limits, check)
    if census.footer != ("end", len(census.files_by_key), census.task_count, census.total_bytes) or (
        tuple(census.directories_by_role) != source._ROLES
    ):
        raise FamilyBlockUnavailable("retirement SOURCE-ready EOF differs")
    for role in source._ROLES:
        if {entry.name for entry in (ready / role).iterdir()} != {
            name for kind, name in census.files_by_key if kind == role
        }:
            raise FamilyBlockUnavailable("retirement SOURCE-ready file census differs")
    if source._hash_file(ready / source._DESCRIPTOR, check=check) != (stamp, digest):
        raise FamilyBlockUnavailable("retirement SOURCE-ready changed during reading")
    return census.files_by_key


def _copied_originals(policy, files, check):
    parent = policy.root / "source/allocation-journal/outputs"
    for candidate in parent.iterdir():
        if re.fullmatch(r"(?:attempt|metadata)-[0-9a-f]{32}", candidate.name) is None:
            continue
        identity = _directory(candidate)
        if re.fullmatch(r"attempt-[0-9a-f]{32}", candidate.name):
            expected_fingerprints = (files["allocations", "source-ids.mpk"],)
            names = {"source-ids.mpk"}
        elif re.fullmatch(r"metadata-[0-9a-f]{32}", candidate.name):
            expected_fingerprints = tuple(files["metadata", name] for name in ("families.mpk", "query-keys.mpk"))
            names = {entry.name for entry in candidate.iterdir()}
            if len(names) != 2 or any(re.fullmatch(_RUN, name) is None for name in names):
                raise FamilyBlockUnavailable("retirement original metadata is incomplete")
        if {entry.name for entry in candidate.iterdir()} != names:
            raise FamilyBlockUnavailable("retirement original producer is incomplete")
        observed_fingerprints = []
        for name in names:
            directory = inventory._open_directory(candidate)
            try:
                stamp, digest, _content = inventory._read_file(
                    directory, name, allowed_modes=(0o400, 0o600, 0o644), check=check
                )
            finally:
                os.close(directory)
            observed_fingerprints.append((stamp[5], digest))
            policy.modes_by_file[str((candidate / name).relative_to(policy.root))] = (stamp[3] & 0o777,)
        if sorted(observed_fingerprints) != sorted(expected_fingerprints):
            raise FamilyBlockUnavailable("retirement original producer differs from its protected copy")
        policy.add(candidate, names=names, identity=identity)


def original_policy(snapshot, root, limits, work_mem_mb, check):
    """Validate the complete typed chain before admitting any directory for census."""
    prefix = output.OriginalPrefix(snapshot)
    profile = astuple(limits.facts), work_mem_mb, astuple(limits)
    ready, custody, outputs, common = source.source_owner_checkpoint(prefix, root, profile, check)
    policy = RetiredFilePolicy(root)
    _original_slots(policy, snapshot)
    manifest = _ready_manifest(ready, root, limits.facts, check)
    _copied_originals(policy, manifest, check)
    for original in outputs:
        policy.add(original.root, names={"joined.mpk"}, identity=original.directory)
        shard, audit_directory, audit_file, output_directory, output_file, _counts, _receipt = original.artifact
        audit = Path(original.root) / f"shard-{shard:04d}"
        policy.add(audit, names={audit_file[0]}, identity=audit_directory)
        policy.add(audit / "output", names={output_file[0]}, identity=output_directory)
    if common is not None:
        policy.add(common.root, names={"output.mpk"}, identity=common.directory)
    _retained_slots(policy, retained.latest(prefix, check))
    _output_slots(policy, output.latest(snapshot, check))
    return policy, custody
