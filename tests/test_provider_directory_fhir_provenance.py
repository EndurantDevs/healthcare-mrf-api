# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import importlib
import json


importer = importlib.import_module("process.provider_directory_fhir")


def test_fhir_meta_and_resource_url_are_bounded_replay_safe_projections():
    fhir_resource_by_field = {
        "resourceType": "PractitionerRole",
        "id": "role-1",
        "meta": {
            "versionId": "7",
            "lastUpdated": "2026-07-13T10:00:00Z",
            "source": (
                "https://user:pass@payer.example/source/token/path-secret/"
                "PractitionerRole?token=secret#fragment"
            ),
            "profile": [
                "https://payer.example/Profile/provider-directory?api_key=secret"
            ],
            "security": [
                {
                    "system": "https://payer.example/security?token=secret",
                    "code": "public",
                    "display": "Public directory",
                    "credential": "must-not-persist",
                }
            ],
            "tag": [{"system": "urn:tag:system", "code": "directory"}],
            "extension": [{"url": "https://example.test?secret=1"}],
        },
    }
    _model, role_row = importer.parse_fhir_resource(
        "source-a",
        fhir_resource_by_field,
        resource_url=(
            "https://resource-user:resource-pass@payer.example/fhir/"
            "PractitionerRole/role-1?access_token=secret#history"
        ),
    )

    assert role_row["resource_url"] == (
        "https://payer.example/fhir/PractitionerRole/role-1"
    )
    assert role_row["fhir_meta"] == {
        "versionId": "7",
        "lastUpdated": "2026-07-13T10:00:00Z",
        "source": "https://payer.example/source/PractitionerRole",
        "profile": ["https://payer.example/Profile/provider-directory"],
        "security": [
            {
                "system": "https://payer.example/security",
                "code": "public",
                "display": "Public directory",
            }
        ],
        "tag": [{"system": "urn:tag:system", "code": "directory"}],
    }
    serialized_role_row = json.dumps(role_row, default=str)
    assert "secret" not in serialized_role_row
    assert "resource-pass" not in serialized_role_row
    assert "must-not-persist" not in serialized_role_row
    assert "path-secret" not in serialized_role_row


def test_fhir_meta_url_collections_and_values_are_strictly_bounded():
    oversized_url = "https://payer.example/" + ("x" * 70000)
    profiles = [
        f"https://payer.example/Profile/{index}/" + ("y" * 3000)
        for index in range(40)
    ]

    sanitized_meta = importer._sanitized_fhir_meta(
        {
            "source": oversized_url,
            "profile": profiles,
            "security": [
                {
                    "system": (
                        f"https://payer.example/security/{index}/"
                        + ("z" * 3000)
                    ),
                    "display": "d" * 3000,
                }
                for index in range(40)
            ],
        }
    )

    assert sanitized_meta is not None
    assert "source" not in sanitized_meta
    assert len(sanitized_meta["profile"]) == 32
    assert len(sanitized_meta["security"]) == 32
    assert max(map(len, sanitized_meta["profile"])) == (
        importer.FHIR_URL_IDENTITY_MAX_LENGTH
    )
    assert max(
        len(security_coding["system"])
        for security_coding in sanitized_meta["security"]
    ) == (
        importer.FHIR_URL_IDENTITY_MAX_LENGTH
    )
    assert max(
        len(security_coding["display"])
        for security_coding in sanitized_meta["security"]
    ) == 2048


def test_codings_preserve_each_codeable_concept_text():
    assert importer._codings(
        [
            {
                "coding": [
                    {
                        "system": "https://example.test/codes",
                        "code": "cardiology",
                        "display": "Cardiology",
                    }
                ],
                "text": "Heart specialist",
            },
            {"text": "Second text-only concept"},
        ]
    ) == [
        {
            "system": "https://example.test/codes",
            "code": "cardiology",
            "display": "Cardiology",
            "text": "Heart specialist",
        },
        {"text": "Second text-only concept"},
    ]
