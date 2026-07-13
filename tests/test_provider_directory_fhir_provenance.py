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
            "source": "https://user:pass@payer.example/source?token=secret#fragment",
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
        "source": "https://payer.example/source",
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
