# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Durably charge origin requests and retain a replayable registry of directory details."""

import zlib
from collections import Counter

from process.guardian_directory_normalization import merge_guardian_resources
from process.vsp_directory_contract import canonical_json
from process.vsp_directory_geometry import distance_miles
from process.vsp_directory_http import strict_json


class GuardianCapture:
    """Retain request outcomes, coverage gaps and normalized resources for one acquisition."""

    def __init__(self, connection, scope, budget, *, fresh):
        self.db = connection
        self.budget = budget
        self.coverage_invalidated = False
        if fresh:
            self.db.executescript("""
                BEGIN IMMEDIATE;
                CREATE TABLE edition (scope TEXT NOT NULL, attempts INTEGER NOT NULL);
                CREATE TABLE request (
                    kind TEXT NOT NULL, payload TEXT NOT NULL, status TEXT NOT NULL, body BLOB,
                    PRIMARY KEY (kind, payload)
                ) WITHOUT ROWID;
                CREATE INDEX pending_request ON request (kind, status, payload);
                CREATE TABLE attempts (kind TEXT PRIMARY KEY, amount INTEGER NOT NULL) WITHOUT ROWID;
                CREATE TABLE delegation (
                    request TEXT PRIMARY KEY, window TEXT NOT NULL, destination TEXT NOT NULL
                ) WITHOUT ROWID;
                CREATE TABLE facility_npi (payload TEXT PRIMARY KEY) WITHOUT ROWID;
                CREATE TABLE gap (
                    window TEXT PRIMARY KEY, scope TEXT NOT NULL, reason TEXT NOT NULL, reported INTEGER, keys TEXT, observed_span REAL
                ) WITHOUT ROWID;
                CREATE INDEX gap_scope ON gap(scope,window);
                CREATE TABLE observation (
                    scope TEXT NOT NULL, payload TEXT NOT NULL, lat REAL, lon REAL, office TEXT, dentist TEXT,
                    PRIMARY KEY (scope,payload)
                ) WITHOUT ROWID;
                CREATE INDEX observation_position ON observation (scope,lat);
                CREATE TABLE circle (
                    scope TEXT NOT NULL, lat REAL NOT NULL, lon REAL NOT NULL, radius REAL NOT NULL,
                    PRIMARY KEY (scope,lat,lon)
                ) WITHOUT ROWID;
                CREATE TABLE resource (
                    family TEXT NOT NULL, id TEXT NOT NULL, payload TEXT NOT NULL,
                    PRIMARY KEY (family, id)
                ) WITHOUT ROWID;
            """)
            with self.db:
                self.db.execute("INSERT INTO edition VALUES (?, 0)", (scope,))
        if self.db.execute("SELECT scope FROM edition").fetchall() != [(scope,)]:
            raise ValueError("guardian_retained_scope_changed")
        if not self.db.execute("PRAGMA user_version").fetchone()[0]:
            # Rebuild legacy geographic shortcuts from retained responses under the current rules.
            with self.db:
                self.db.execute("DELETE FROM circle")
                self.db.execute("PRAGMA user_version=1")

    def reserve(self, kind, key):
        """Charge before sending. A lost response must never be silently replayed."""
        with self.db:
            self.db.execute("INSERT OR IGNORE INTO request VALUES (?, ?, 'pending', NULL)", (kind, key))
            if not self.db.execute(
                "UPDATE request SET status='sent' WHERE kind=? AND payload=? AND status='pending'", (kind, key)
            ).rowcount:
                raise ValueError("guardian_request_outcome_unresolved")
            if not self.db.execute(
                "UPDATE edition SET attempts=attempts+1 WHERE attempts < ?", (self.budget,)
            ).rowcount:
                raise ValueError("guardian_request_budget_exhausted")
            self.db.execute(
                "INSERT INTO attempts VALUES (?,1) ON CONFLICT(kind) DO UPDATE SET amount=amount+1", (kind,)
            )

    def lookup(self, kind, key):
        """Serve retained results before admitting another origin request."""
        row = self.db.execute("SELECT status,body FROM request WHERE kind=? AND payload=?", (kind, key)).fetchone()
        if row is None or row[0] == "pending":
            return None
        if row[0] == "sent":
            raise ValueError("guardian_request_outcome_unresolved")
        return zlib.decompress(row[1])

    def retain(self, kind, key, raw):
        """Persist the received bytes before validating or projecting their contents."""
        with self.db:
            if not self.db.execute(
                "UPDATE request SET status='received',body=? WHERE kind=? AND payload=? AND status='sent'",
                (zlib.compress(raw, level=1), kind, key),
            ).rowcount:
                raise ValueError("guardian_request_outcome_unresolved")

    def release_limited(self, kind, key):
        """Only a confirmed, finitely deferred 429 permits repeating the same request."""
        with self.db:
            self.db.execute(
                "UPDATE request SET status='pending' WHERE kind=? AND payload=? AND status='sent'", (kind, key)
            )

    def enqueue_details(self, keys):
        """Deduplicate exact publisher detail keys across all search modes."""
        with self.db:
            self.db.executemany(
                "INSERT OR IGNORE INTO request VALUES ('detail',?,'pending',NULL)", ((canonical_json(k),) for k in keys)
            )

    def detail_batch(self, after, limit):
        """Use keyset order so retained progress needs no unbounded in-memory identifier set."""
        return self.db.execute(
            "SELECT payload FROM request WHERE kind='detail' AND payload>? AND status!='projected' "
            "ORDER BY payload LIMIT ?",
            (after, limit),
        ).fetchall()

    def projected_count(self):
        """Count acknowledged detail projections when reopening the capture."""
        return self.db.execute("SELECT count(*) FROM request WHERE kind='detail' AND status='projected'").fetchone()[0]

    def project(self, key, resources, *, unattributed_facility_npi=False):
        """Commit a whole detail and refuse contradictory data under one canonical identity."""
        with self.db:
            if unattributed_facility_npi:
                self.db.execute("INSERT OR IGNORE INTO facility_npi VALUES (?)", (key,))
            for resource in resources:
                self._merge_resource(resource)
            if not self.db.execute(
                "UPDATE request SET status='projected' WHERE kind='detail' AND payload=? AND status='received'", (key,)
            ).rowcount:
                raise ValueError("guardian_detail_not_received")

    def _merge_resource(self, resource):
        family, identifier, payload = resource["resourceType"], resource["id"], canonical_json(resource)
        if self.db.execute("INSERT OR IGNORE INTO resource VALUES (?,?,?)", (family, identifier, payload)).rowcount:
            return
        (prior,) = self.db.execute(
            "SELECT payload FROM resource WHERE family=? AND id=?", (family, identifier)
        ).fetchone()
        if prior != payload:
            merged = canonical_json(merge_guardian_resources(strict_json(prior), resource))
            if merged != prior:
                self.db.execute("UPDATE resource SET payload=? WHERE family=? AND id=?", (merged, family, identifier))

    def attempt_count(self):
        """Count charged requests including interrupted attempts and confirmed rate limits."""
        return self.db.execute("SELECT attempts FROM edition").fetchone()[0]

    def observe(self, scope, observations):
        """Exclude coordinate conflicts from cross-window coverage proofs."""
        with self.db:
            conflicts = self.db.executemany(
                "UPDATE observation SET lat=NULL,lon=NULL WHERE scope=? AND payload=? AND lat IS NOT NULL "
                "AND (lat IS NOT ? OR lon IS NOT ?)",
                ((scope, key, lat, lon) for key, lat, lon, *_ in observations),
            ).rowcount
            self.db.executemany(
                "INSERT OR IGNORE INTO observation VALUES (?,?,?,?,?,?)", ((scope, *row) for row in observations)
            )
            if conflicts:
                # A previously credited position may have closed a window earlier in this sweep.
                self.db.execute("DELETE FROM circle WHERE scope=?", (scope,))
                self.coverage_invalidated = True

    def has_locations_for_all_keys(self, scope, keys):
        """A reusable geographic proof needs unambiguous retained positions for its direct cards."""
        if not keys:
            return False
        missing = self.db.execute(
            "SELECT 1 FROM json_each(?) AS keys WHERE NOT EXISTS ("
            "SELECT 1 FROM observation WHERE scope=? AND payload=keys.value AND lat IS NOT NULL) LIMIT 1",
            (canonical_json(sorted(keys)), scope),
        ).fetchone()
        return missing is None

    def is_count_reconciled(self, scope, center, radius, keys, total):
        """Count only unambiguous same-selector identities safely inside this actual search circle."""
        lat, lon = center
        rows = self.db.execute(
            "SELECT payload,lat,lon FROM observation WHERE scope=? AND lat BETWEEN ? AND ?",
            (scope, lat - radius / 68, lat + radius / 68),
        )
        inside = len(keys)
        for key, row_lat, row_lon in rows:
            if key not in keys and distance_miles(lat, lon, row_lat, row_lon) * 1.01 + 0.1 < radius:
                inside += 1
                if inside > total:
                    return False
        return inside == total

    def boundary_names(self, scope, center, radius, keys, total):
        """Rank names for checking geometrically plausible boundary cards against the original search."""
        lat, lon = center
        rows = self.db.execute(
            "SELECT payload,lat,lon,office,dentist FROM observation WHERE scope=? AND lat BETWEEN ? AND ?",
            (scope, lat - radius / 68, lat + radius / 68),
        )
        candidates = Counter()
        nominal = len(keys)
        for key, row_lat, row_lon, office, dentist in rows:
            if key in keys:
                continue
            distance = distance_miles(lat, lon, row_lat, row_lon)
            if distance > radius:
                continue
            nominal += 1
            if distance * 1.01 + 0.1 >= radius:
                for field, name in (("officeName", office), ("dentistName", dentist)):
                    if isinstance(name, str) and 3 <= len(name) <= 200 and name.isprintable():
                        candidates[(field, name)] += 1
        if nominal < total:
            return ()
        return sorted(candidates, key=lambda item: (-candidates[item], item[0] != "officeName", item[1]))

    def add_circle(self, scope, center, radius):
        """Record the maximum independently reconciled radius at this center."""
        with self.db:
            self.db.execute(
                "INSERT INTO circle VALUES (?,?,?,?) ON CONFLICT(scope,lat,lon) DO UPDATE "
                "SET radius=MAX(radius,excluded.radius)",
                (scope, *center, radius),
            )

    def covers_tile(self, scope, center, extent):
        """A prior complete circle covers a new rectangle only if its farthest corner also fits."""
        lat, lon = center
        rows = self.db.execute(
            "SELECT lat,lon,radius FROM circle WHERE scope=? AND lat BETWEEN ? AND ? AND radius>=?",
            (scope, lat - 3, lat + 3, extent),
        )
        return any(distance_miles(lat, lon, a, b) + extent < radius for a, b, radius in rows)

    def clear_gap(self, window):
        """Remove a covered cell from the outstanding coverage ledger."""
        with self.db:
            self.db.execute("DELETE FROM gap WHERE window=?", (canonical_json(window),))

    def has_gaps(self):
        """Detect unresolved geographic or publisher redirect gaps."""
        return bool(self.db.execute("SELECT 1 FROM gap LIMIT 1").fetchone())

    def record_delegation(self, query, window, destination):
        """Keep the exact source search and its delegated destination without following the handoff."""
        with self.db:
            self.db.execute(
                "INSERT OR IGNORE INTO delegation VALUES (?,?,?)",
                (canonical_json(query), canonical_json(window), destination),
            )

    def record_gap(self, window, reason, count, keys, observed_span):
        """Store an unresolved cell and its direct publisher evidence."""
        window_key = canonical_json(window)
        with self.db:
            prior = self.db.execute("SELECT keys,reason,reported FROM gap WHERE window=?", (window_key,)).fetchone()
            if prior:
                if prior[2] != count:
                    raise ValueError("guardian_search_count_mismatch")
                keys = keys.union(strict_json(prior[0]))
                if prior[1] == "direct_only_result_cap":
                    reason = prior[1]
            self.db.execute(
                "INSERT OR REPLACE INTO gap VALUES (?,?,?,?,?,?)",
                (window_key, window[0], reason, count, canonical_json(sorted(keys)), observed_span),
            )

    def gap_scopes(self):
        """Provide independent selector scopes in bounded pages."""
        after = ""
        while rows := self.db.execute(
            "SELECT DISTINCT scope FROM gap WHERE scope>? ORDER BY scope LIMIT 32", (after,)
        ).fetchall():
            yield from (row[0] for row in rows)
            after = rows[-1][0]

    def gap_batches(self, scope):
        """Read one selector's capped cells without revisiting cleared rows."""
        after = ""
        while rows := self.db.execute(
            "SELECT window,reported,keys,observed_span,reason FROM gap WHERE scope=? AND window>? "
            "AND reason IN ('unresolved_result_cap','direct_only_result_cap') ORDER BY window LIMIT 32",
            (scope, after),
        ).fetchall():
            for window, count, keys, observed_span, reason in rows:
                yield strict_json(window), count, set(strict_json(keys)), observed_span, reason
            after = rows[-1][0]

    def progress(self):
        """Report the live attempt and coverage counters without exposing provider records."""
        return {
            "origin_requests_by_kind": dict(self.db.execute("SELECT kind,amount FROM attempts")),
            "coverage_gaps": self.db.execute("SELECT count(*) FROM gap").fetchone()[0],
            "delegated_windows": self.db.execute("SELECT count(*) FROM delegation").fetchone()[0],
        }

    def iter_resources(self):
        """Yield normalized resources in canonical family and ID order."""
        for (payload,) in self.db.execute("SELECT payload FROM resource ORDER BY family,id"):
            yield strict_json(payload)

    def summary(self, families):
        """Require complete coverage and account for all normalized resource families."""
        if self.db.execute("SELECT 1 FROM gap LIMIT 1").fetchone():
            raise ValueError("guardian_directory_coverage_incomplete")
        if self.db.execute("SELECT 1 FROM request WHERE status IN ('pending','sent') LIMIT 1").fetchone():
            raise ValueError("guardian_request_outcome_unresolved")
        if self.db.execute("SELECT 1 FROM request WHERE kind='detail' AND status!='projected' LIMIT 1").fetchone():
            raise ValueError("guardian_detail_coverage_incomplete")
        counts = dict.fromkeys(families, 0)
        counts.update(self.db.execute("SELECT family,count(*) FROM resource GROUP BY family"))
        return {
            **self.progress(),
            "delegated_destinations": dict(
                self.db.execute("SELECT destination,count(*) FROM delegation GROUP BY destination")
            ),
            "partner_membership_complete": False,
            "origin_requests": self.attempt_count(),
            "requests_by_kind": dict(self.db.execute("SELECT kind,count(*) FROM request GROUP BY kind")),
            "resource_counts": counts,
            "facility_details_with_unattributed_npi": self.db.execute("SELECT count(*) FROM facility_npi").fetchone()[
                0
            ],
        }
