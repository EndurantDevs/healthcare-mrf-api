# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded ordering of actual revision IDs observed during block encoding."""

import heapq
import struct
import tempfile
from contextlib import AbstractContextManager
from itertools import groupby

from process.custom_import.family_block_codec import FamilyBlockResourceLimit
from process.custom_import.family_block_directory import RevisionMembership, iter_revision_memberships

_ENTRY = struct.Struct("!QQQ")


class RevisionOrder(AbstractContextManager):
    """Keep normal families in memory and spill only fixed-width ID/ordinal triples."""

    def __init__(self, limits, child_count):
        self.limits = limits
        self.chunk_size = max(1, min(4096, limits.memory_bytes // 256))
        self.reserved_spool_bytes = 2 * _ENTRY.size * child_count if child_count > self.chunk_size else 0
        if self.reserved_spool_bytes >= limits.max_spool_bytes:
            raise FamilyBlockResourceLimit("revision ordering exceeds the family spool budget")
        self.pending = []
        self.runs_by_level = {}

    def add(self, slot, revision_id, ordinal):
        """Append one observed pair without guessing gaps or changing its ordinal."""
        if len(self.pending) == self.chunk_size:
            self._spill()
        self.pending.append((slot, revision_id, ordinal))

    def _write_run(self, triples):
        run = tempfile.TemporaryFile(dir=self.limits.spool_directory)
        try:
            for triple in triples:
                run.write(_ENTRY.pack(*triple))
        except BaseException:
            run.close()
            raise
        return run

    def _read_run(self, run):
        run.seek(0)
        # Leave room for every live merge iterator and the membership chunk.
        width = max(1, min(128, self.limits.memory_bytes // (256 * (2 + len(self.runs_by_level)))))
        while block := run.read(width * _ENTRY.size):
            yield from _ENTRY.iter_unpack(block)

    def _spill(self):
        """Carry sorted runs by level so the number of open files stays logarithmic."""
        self.pending.sort()
        run = self._write_run(self.pending)
        self.pending.clear()
        level = 0
        while level in self.runs_by_level:
            previous = self.runs_by_level.pop(level)
            with previous, run:
                run = self._write_run(heapq.merge(self._read_run(previous), self._read_run(run)))
            level += 1
        self.runs_by_level[level] = run

    def memberships(self, root_revision_id):
        """Read exactly the sorted identities in the storage directory's native order."""
        yield RevisionMembership(0, root_revision_id, root_revision_id, 1, 0, None)
        self.pending.sort()
        sources = (self._read_run(run) for run in self.runs_by_level.values())
        triples = heapq.merge(self.pending, *sources)
        for slot, group in groupby(triples, key=lambda entry: entry[0]):
            yield from iter_revision_memberships(
                slot, ((revision_id, ordinal) for _, revision_id, ordinal in group), chunk_records=self.chunk_size
            )

    def __exit__(self, *_exc):
        for run in self.runs_by_level.values():
            run.close()
