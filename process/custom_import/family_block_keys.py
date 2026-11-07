# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Hash-ordered internal query keys and the unchanged scoped collision guards.

The owner binds these rows to one immutable candidate dictionary. A key rank
replaces hash ordering only; its variant identifies exact text and is never a
semantic tie-break. Existing API cursor generation/query/authorization binding
is unchanged. Both scope guards must exhaust views of the SAME frozen source.
"""

from typing import NamedTuple

from process.custom_import.query_projection_pages import _checked_context


class KeyDictionaryRow(NamedTuple):
    """Native COPY cells; equal hashes share a rank even across text variants."""

    key_rank: int
    key_variant: int
    child_key_sha256: bytes
    canonical_child_key: str


def _key(value):
    if not isinstance(value, (tuple, list)) or len(value) != 2:
        raise ValueError("dictionary key requires a digest and exact text")
    # The existing projection validator owns the native hash and TEXT domain.
    checked = _checked_context((1, 0, *value), child=True, canonical=True)
    return checked[2], checked[3]


def iter_key_dictionary(ordered_keys):
    """Intern distinct (hash, exact text) pairs without retaining the dictionary.

    Input is ordered by binary hash then UTF-8 C text. IDs are Python integers;
    there is no smallint input ceiling. The owner uses adequate native integer
    columns and freezes the completed dictionary with its candidate snapshot.
    Global hash/text ambiguity is allowed; refusal belongs to the original
    entity/selected-value scope, checked separately below.
    """
    previous = None
    rank = variant = 0
    for value in ordered_keys:
        digest, canonical = _key(value)
        key = digest, canonical.encode("utf-8")
        if previous is not None and key < previous:
            raise ValueError("dictionary keys are not in hash/text order")
        if key == previous:
            continue
        if previous is None or digest != previous[0]:
            rank += 1
            variant = 1
        else:
            variant += 1
        yield KeyDictionaryRow(rank, variant, digest, canonical)
        previous = key


def _validate_scopes(rows, *, by_hash):
    previous = None
    count = 0
    for value in rows:
        entity, selected, digest, canonical = _checked_context(value, child=True, canonical=True)
        text = canonical.encode("utf-8")
        key = (entity, selected, digest, text) if by_hash else (entity, selected, text, digest)
        if previous is not None:
            if key < previous:
                raise ValueError("projection keys are not in the required scope order")
            if key[:3] == previous[:3] and key[3] != previous[3]:
                raise ValueError("projection child key digest collision has inconsistent complete keys")
        previous = key
        count += 1
    return count


def validate_key_hash_scopes(rows):
    """Reject multiple texts per hash in (entity, selected), retaining one row.

    Rows are ordered by entity, selected value, hash, then exact UTF-8 text. The
    returned count alone does not prove equivalence to another input view.
    """
    return _validate_scopes(rows, by_hash=True)


def validate_key_text_scopes(rows):
    """Reject multiple hashes per text in the SAME frozen source's scoped view.

    Rows are ordered by entity, selected value, exact UTF-8 text, then hash.
    Both directions are required, matching child_hash_collision_statement;
    callers must not replace this pass with a count-only comparison of views.
    """
    return _validate_scopes(rows, by_hash=False)
