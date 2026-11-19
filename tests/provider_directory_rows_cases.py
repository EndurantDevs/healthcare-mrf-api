# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Raw PractitionerRole NDJSON batches for the native row preparation differential tests."""

from __future__ import annotations

import ast
import copy
import functools
import json
import random
from pathlib import Path
from typing import Any

PLAN_NET = "http://hl7.org/fhir/us/davinci-pdex-plan-net/StructureDefinition"
NUMBER_MARK = "__number__"


PLAN_NET_ROLE_TEMPLATE = {
    "resourceType": "PractitionerRole",
    "id": "ROLE_ID",
    "meta": {
        "versionId": "7",
        "lastUpdated": "2026-03-04T05:06:07Z",
        "source": "https://User:pw@Directory.Example.test:0443/fhir;v=1/token/abc/x?q=1",
        "profile": [PLAN_NET + "/plannet-PractitionerRole", "urn:oid:1.2.3"],
        "security": [{"system": "http://terminology.example.test/sec", "code": "R", "userSelected": False}],
        "tag": {"code": "t1", "display": "Tag"},
    },
    "active": True,
    "identifier": [
        {"system": "http://hl7.org/fhir/sid/us-npi", "value": "1234567893"},
        {
            "type": {"text": "Role", "coding": [{"system": "urn:role", "code": "R"}]},
            "value": "ROLE_ID-NETWORK",
        },
        {
            "use": "official",
            "period": {"start": "2020-01-01"},
            "assigner": {"reference": "Organization/a", "display": "A"},
        },
    ],
    "practitioner": {"reference": "Practitioner/p1", "display": "Dr. Example"},
    "organization": {"reference": "Organization/o1"},
    "location": [{"reference": "Location/l1"}, {"reference": " Location/l2 "}, {"display": "no ref"}],
    "healthcareService": [{"reference": "HealthcareService/h1"}],
    "network": [{"reference": "Organization/NETWORK"}],
    "insurancePlan": {"reference": "InsurancePlan/ip1"},
    "endpoint": [{"reference": "Endpoint/e1"}],
    "specialty": [
        {"coding": [{"system": "http://nucc.org/provider-taxonomy", "code": "207Q00000X", "display": "Family"}]},
        {"text": "Free text"},
    ],
    "code": {"coding": [{"code": "doc"}, {"system": None, "code": None}], "text": "Doctor"},
    "telecom": [
        {"system": "phone", "value": "555-0100", "rank": 1},
        "skip",
        {"system": "email", "value": "a@example.test"},
    ],
    "availableTime": [{"daysOfWeek": ["mon"], "allDay": True}],
    "notAvailable": [{"description": "Holiday"}],
    "availabilityExceptions": "  Closed holidays  ",
    "period": {"start": "2021-01-01", "end": None},
    "extension": [
        {
            "url": PLAN_NET + "/newpatients",
            "extension": [
                {"url": "acceptingPatients", "valueCodeableConcept": {"coding": [{"code": "NewPt"}]}},
                {"url": "fromNetwork", "valueReference": {"reference": "Organization/NETWORK"}},
            ],
        },
        {
            "url": PLAN_NET + "/delivery-method/",
            "extension": [
                {"url": "type", "valueCodeableConcept": {"coding": [{"code": "Virtual"}]}},
                {"url": "virtualModalities", "valueCodeableConcept": {"coding": [{"code": "video"}]}},
            ],
        },
        {"url": "https://healthporta.com/fhir/provider-directory/telehealth", "valueBoolean": True},
        {"url": "https://healthporta.com/fhir/provider-directory/accepting-medicaid", "valueBoolean": False},
        {
            "url": "https://example.test/wrapper",
            "extension": [
                {
                    "url": PLAN_NET + "/network-reference|1.1",
                    "valueReference": {"reference": "Organization/nested"},
                },
                {"url": PLAN_NET + "/network-reference", "valueReference": {"reference": "Location/not-a-network"}},
            ],
        },
    ],
}


def _plan_net_role(role_id: str, network: str) -> dict[str, Any]:
    """A fully populated Plan-Net role; `network` varies only the per-network fields."""

    text = json.dumps(PLAN_NET_ROLE_TEMPLATE).replace("ROLE_ID", role_id).replace("NETWORK", network)
    return json.loads(text)


SYNTHETIC_ROLES = (
    _plan_net_role("role-1", "net-a"),
    _plan_net_role("role-2", "net-b"),
    {"resourceType": "PractitionerRole", "id": "minimal"},
    {"resourceType": "PractitionerRole"},
)


@functools.cache
def harvested_roles() -> tuple[str, ...]:
    """Every literal raw PractitionerRole resource written in this test suite, as JSON text."""

    found_texts: set[str] = set()
    for path in sorted(Path(__file__).resolve().parent.rglob("*.py")):
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"))
        except SyntaxError, UnicodeDecodeError:
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.Dict) and _is_role_literal(node):
                try:
                    found_texts.add(json.dumps(ast.literal_eval(node), sort_keys=True))
                except ValueError:
                    continue
    return tuple(sorted(found_texts))


def _is_role_literal(node: ast.Dict) -> bool:
    return any(
        isinstance(key, ast.Constant)
        and key.value == "resourceType"
        and isinstance(value, ast.Constant)
        and value.value == "PractitionerRole"
        for key, value in zip(node.keys, node.values)
    )


def base_roles() -> list[dict[str, Any]]:
    return [json.loads(text) for text in harvested_roles()] + [copy.deepcopy(role) for role in SYNTHETIC_ROLES]


ODD_TEXTS = (
    "",
    "   ",
    "\u3000padded\u2003",
    "\x1cfield\x1f",
    "\x85next",
    "caf\u00e9",
    "\U0001f600 emoji",
    "line\nbreak",
    'quote" and \\',
    "x" * 300,
    "\u212aelvin",
    "\x7fdel",
)
ODD_VALUES = (None, [], {}, 0, 1, -0, True, False, 12345678901234567890123, "1234567893")
NUMBER_NOTATIONS = ("1", "-0", "0", "10", "1.0", "1e2", "1E+2", "-1.5e-3", "123456789012345678901234567890", "0.1")
ODD_URLS = (
    "https://Example.test:8443/a/api_key/secret/b?x=1#f",
    "http://[::1]/fhir",
    "http://host:99999/x",
    "urn:uuid:1234",
    "relative/path",
    "https://example.test/\u212aey/v",
    "https://ex\u00e4mple.test/p",
    "//host/only",
    "\tleading-tab",
)
SCALAR_FIELDS = ("id", "availabilityExceptions", "active")
REFERENCE_FIELDS = (
    "practitioner",
    "organization",
    "location",
    "network",
    "insurancePlan",
    "endpoint",
    "healthcareService",
)
CODEABLE_FIELDS = ("specialty", "code")


def _numbered(rng: random.Random) -> str:
    return NUMBER_MARK + rng.choice(NUMBER_NOTATIONS)


def _mutate_identifier(rng: random.Random, resource: dict[str, Any]) -> None:
    identifier_by_field = {
        "system": rng.choice(["http://hl7.org/fhir/sid/us-npi", "urn:npi", "HTTP://HL7.ORG/FHIR/SID/US-NPI ", None]),
        "type": rng.choice([{}, {"text": "National Provider"}, {"coding": [{"display": "NPI"}]}, {"coding": "NPI"}]),
        "value": rng.choice(["123-456-7893", "1234567890", "1987654326", "\u0661\u0662", 1234567893, " ", None]),
    }
    identifiers = resource.get("identifier")
    if isinstance(identifiers, list):
        identifiers.insert(rng.randrange(len(identifiers) + 1), identifier_by_field)
    else:
        resource["identifier"] = [identifier_by_field]


def _mutate_extension(rng: random.Random, resource: dict[str, Any]) -> None:
    extension_by_field: dict[str, Any] = {
        "url": rng.choice([f"{PLAN_NET}/network-reference", f"{PLAN_NET}/newpatients", "https://other.example.test"]),
        "valueReference": {"reference": rng.choice(["Organization/n9", "https://example.test/Organization/n1", "x"])},
    }
    for _ in range(rng.randrange(1, 6)):
        extension_by_field = {"url": "https://example.test/wrap", "extension": [extension_by_field]}
    if rng.random() < 0.5:
        code = rng.choice(["nopt", "EXISTPTONLY", "no"])
        extension_by_field["extension"].append(
            {"url": "acceptingPatients", "valueCodeableConcept": {"coding": [{"code": code}]}}
        )
    current_extensions = resource.get("extension")
    resource["extension"] = (current_extensions if isinstance(current_extensions, list) else []) + [extension_by_field]


def _mutate_meta(rng: random.Random, resource: dict[str, Any]) -> None:
    resource["meta"] = {
        "lastUpdated": rng.choice(ODD_TEXTS),
        "versionId": rng.choice(ODD_VALUES),
        "source": rng.choice(ODD_URLS),
        "profile": rng.choice([rng.choice(ODD_URLS), [rng.choice(ODD_URLS), None], 5]),
        "tag": [{"system": rng.choice(ODD_URLS), "code": rng.choice(ODD_TEXTS)}],
    }


def _drop_field(rng: random.Random, resource: dict[str, Any]) -> None:
    if resource:
        resource.pop(rng.choice(sorted(resource)), None)


def _odd_value(rng: random.Random, resource: dict[str, Any]) -> None:
    resource[rng.choice(SCALAR_FIELDS + REFERENCE_FIELDS)] = rng.choice(ODD_VALUES)


def _odd_text(rng: random.Random, resource: dict[str, Any]) -> None:
    resource[rng.choice(SCALAR_FIELDS)] = rng.choice(ODD_TEXTS)


def _odd_reference(rng: random.Random, resource: dict[str, Any]) -> None:
    resource[rng.choice(REFERENCE_FIELDS)] = [{"reference": rng.choice(ODD_TEXTS + ODD_URLS)}]


def _odd_coding(rng: random.Random, resource: dict[str, Any]) -> None:
    coding_by_field = {
        "system": rng.choice(ODD_TEXTS),
        "code": rng.choice(ODD_VALUES),
        "display": rng.choice(ODD_TEXTS),
    }
    resource[rng.choice(CODEABLE_FIELDS)] = {"coding": [coding_by_field], "text": rng.choice(ODD_TEXTS)}


def _numbered_telecom(rng: random.Random, resource: dict[str, Any]) -> None:
    resource["telecom"] = [{"system": "phone", "rank": _numbered(rng), "value": rng.choice(ODD_TEXTS)}]


def _unknown_field(rng: random.Random, resource: dict[str, Any]) -> None:
    resource[f"unknown{rng.randrange(3)}"] = {"nested": [rng.choice(ODD_TEXTS), _numbered(rng), {"deep": None}]}


MUTATIONS = (
    _drop_field,
    _odd_value,
    _odd_text,
    _odd_reference,
    _odd_coding,
    _mutate_meta,
    _numbered_telecom,
    _unknown_field,
    _mutate_identifier,
    _mutate_extension,
)


def _encode(rng: random.Random, resource: Any) -> bytes:
    text = json.dumps(resource, ensure_ascii=rng.random() < 0.5, separators=rng.choice([(",", ":"), (", ", ": ")]))
    for notation in NUMBER_NOTATIONS:
        text = text.replace(json.dumps(NUMBER_MARK + notation), notation)
    return text.encode("utf-8")


LINE_HAZARDS = (
    b"",
    b"   ",
    b'{"resourceType": "PractitionerRole", "id": NaN}',
    b'{"resourceType": "PractitionerRole", "id": "a", "x": Infinity}',
    b'{"resourceType": "PractitionerRole", "id": "\\ud800"}',
    b'{"resourceType": "PractitionerRole", "id": "bad\xff"}',
    b'\xef\xbb\xbf{"resourceType": "PractitionerRole", "id": "bom"}',
    b'{"resourceType": "PractitionerRole", "id": "dup", "id": "last"}',
    b'{"resourceType": "PractitionerRole", "id": "trailing"} x',
    b"[1, 2]",
    b'{"resourceType": "Practitioner", "id": "other"}',
    b'{"resourceType": "PractitionerRole", "telecom": [{"rank": 1.5}]}',
    b'{"resourceType": "PractitionerRole", "id": 1e2}',
    b'{"resourceType": "PractitionerRole", "id": "01", "active": 01}',
    b'{"resourceType": "PractitionerRole", "identifier": [{"type": {"coding": 5}, "value": "1"}]}',
)


def generated_lines(rng: random.Random, bases: list[dict[str, Any]], count: int) -> list[bytes]:
    """Mutated raw lines, with network siblings and repeats so the in-batch identity rules run."""

    lines: list[bytes] = []
    while len(lines) < count:
        roll = rng.random()
        if roll < 0.005:
            lines.append(rng.choice(LINE_HAZARDS))
            continue
        resource = copy.deepcopy(rng.choice(bases))
        for _ in range(rng.choice([0, 1, 1, 2, 3])):
            rng.choice(MUTATIONS)(rng, resource)
        resource["resourceType"] = "PractitionerRole"
        if isinstance(resource.get("id"), str) and resource["id"].strip():
            # Distinct roles get distinct ids; repeats below are exact copies.
            resource["id"] = f"{len(lines)}-{resource['id']}"
        lines.append(_encode(rng, resource))
        if roll > 0.9:
            sibling = copy.deepcopy(resource)
            sibling["id"] = f"{resource.get('id')}-sibling-{len(lines)}"
            sibling["network"] = [{"reference": f"Organization/n{rng.randrange(4)}"}]
            lines.append(_encode(rng, sibling))
        elif roll > 0.85:
            lines.append(lines[-1])
    return lines


def generated_batches(seed: int, line_count: int) -> list[list[bytes]]:
    rng = random.Random(seed)
    lines = generated_lines(rng, base_roles(), line_count)
    batches: list[list[bytes]] = []
    start = 0
    while start < len(lines):
        size = rng.choice([1, 2, 5, 20, 50, 200])
        batches.append(lines[start : start + size])
        start += size
    return batches
