# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Canonical provider-language values shared by profile composition and evidence."""

from __future__ import annotations

import re
import unicodedata
from collections.abc import Mapping
from typing import Any

BCP47_SYSTEM = "urn:ietf:bcp:47"

# This intentionally stays conservative. Ambiguous labels such as "Creole",
# "Chinese", "Other", and "Sign Language" remain readable text instead of being
# assigned a code that the source did not justify.
_LANGUAGE_BY_LABEL = {
    "afrikaans": ("af", "Afrikaans"),
    "albanian": ("sq", "Albanian"),
    "amharic": ("am", "Amharic"),
    "arabic": ("ar", "Arabic"),
    "armenian": ("hy", "Armenian"),
    "bengali": ("bn", "Bengali"),
    "bosnian": ("bs", "Bosnian"),
    "bulgarian": ("bg", "Bulgarian"),
    "burmese": ("my", "Burmese"),
    "cantonese": ("yue", "Cantonese"),
    "catalan": ("ca", "Catalan"),
    "croatian": ("hr", "Croatian"),
    "czech": ("cs", "Czech"),
    "danish": ("da", "Danish"),
    "dutch": ("nl", "Dutch"),
    "english": ("en", "English"),
    "estonian": ("et", "Estonian"),
    "farsi": ("fa", "Persian"),
    "filipino": ("fil", "Filipino"),
    "finnish": ("fi", "Finnish"),
    "french": ("fr", "French"),
    "georgian": ("ka", "Georgian"),
    "german": ("de", "German"),
    "greek": ("el", "Greek"),
    "gujarati": ("gu", "Gujarati"),
    "haitian": ("ht", "Haitian Creole"),
    "haitian creole": ("ht", "Haitian Creole"),
    "hebrew": ("he", "Hebrew"),
    "hindi": ("hi", "Hindi"),
    "hungarian": ("hu", "Hungarian"),
    "icelandic": ("is", "Icelandic"),
    "indonesian": ("id", "Indonesian"),
    "italian": ("it", "Italian"),
    "japanese": ("ja", "Japanese"),
    "kannada": ("kn", "Kannada"),
    "khmer": ("km", "Khmer"),
    "korean": ("ko", "Korean"),
    "lao": ("lo", "Lao"),
    "latvian": ("lv", "Latvian"),
    "lithuanian": ("lt", "Lithuanian"),
    "malay": ("ms", "Malay"),
    "malayalam": ("ml", "Malayalam"),
    "mandarin": ("cmn", "Mandarin Chinese"),
    "mandarin chinese": ("cmn", "Mandarin Chinese"),
    "marathi": ("mr", "Marathi"),
    "nepali": ("ne", "Nepali"),
    "norwegian": ("no", "Norwegian"),
    "panjabi": ("pa", "Punjabi"),
    "persian": ("fa", "Persian"),
    "polish": ("pl", "Polish"),
    "portuguese": ("pt", "Portuguese"),
    "punjabi": ("pa", "Punjabi"),
    "romanian": ("ro", "Romanian"),
    "rumanian": ("ro", "Romanian"),
    "russian": ("ru", "Russian"),
    "serbian": ("sr", "Serbian"),
    "sinhala": ("si", "Sinhala"),
    "slovak": ("sk", "Slovak"),
    "somali": ("so", "Somali"),
    "spanish": ("es", "Spanish"),
    "spanish castilian": ("es", "Spanish"),
    "swahili": ("sw", "Swahili"),
    "swedish": ("sv", "Swedish"),
    "tagalog": ("tl", "Tagalog"),
    "tagalog filipino": ("fil", "Filipino"),
    "tamil": ("ta", "Tamil"),
    "telegu": ("te", "Telugu"),
    "telugu": ("te", "Telugu"),
    "thai": ("th", "Thai"),
    "turkish": ("tr", "Turkish"),
    "ukrainian": ("uk", "Ukrainian"),
    "ukranian": ("uk", "Ukrainian"),
    "urdu": ("ur", "Urdu"),
    "vietnamese": ("vi", "Vietnamese"),
    "welsh": ("cy", "Welsh"),
    "xhosa": ("xh", "Xhosa"),
    "yiddish": ("yi", "Yiddish"),
    "zulu": ("zu", "Zulu"),
}

_CODE_ALIASES = {
    "alb": "sq",
    "ara": "ar",
    "arm": "hy",
    "ben": "bn",
    "chi": "zh",
    "cze": "cs",
    "dut": "nl",
    "eng": "en",
    "engl": "en",
    "fre": "fr",
    "fra": "fr",
    "fren": "fr",
    "ger": "de",
    "deu": "de",
    "germ": "de",
    "gre": "el",
    "ell": "el",
    "heb": "he",
    "hin": "hi",
    "ita": "it",
    "ital": "it",
    "jpn": "ja",
    "kor": "ko",
    "may": "ms",
    "msa": "ms",
    "per": "fa",
    "fas": "fa",
    "por": "pt",
    "port": "pt",
    "rum": "ro",
    "ron": "ro",
    "rus": "ru",
    "russ": "ru",
    "spa": "es",
    "span": "es",
    "tam": "ta",
    "tel": "te",
    "tha": "th",
    "tur": "tr",
    "ukr": "uk",
    "urd": "ur",
    "vie": "vi",
}
_UNKNOWN_CODE_KEYS = {
    "n a",
    "none",
    "not applicable",
    "not reported",
    "null",
    "und",
    "unk",
    "unknown",
}
_DISPLAY_BY_CODE = {
    code: display for code, display in _LANGUAGE_BY_LABEL.values()
}
_DISPLAY_BY_CODE.update(
    {
        "zh": "Chinese",
    }
)
_WHITESPACE = re.compile(r"\s+")
_NON_ALNUM = re.compile(r"[^a-z0-9]+")
_NUMERIC_ONLY_LABEL = re.compile(r"^[0-9\s.,;/:+\-]+$")
_BCP47_TAG = re.compile(
    r"^(?P<primary>[A-Za-z]{2,8})"
    r"(?P<extlang>(?:-[A-Za-z]{3}){0,3})"
    r"(?P<script>-[A-Za-z]{4})?"
    r"(?P<region>-(?:[A-Za-z]{2}|[0-9]{3}))?"
    r"(?P<variants>(?:-(?:[A-Za-z0-9]{5,8}|[0-9][A-Za-z0-9]{3}))*)"
    r"(?P<extensions>(?:-[0-9A-WY-Za-wy-z](?:-[A-Za-z0-9]{2,8})+)*)"
    r"(?P<private>-(?:x|X)(?:-[A-Za-z0-9]{1,8})+)?$"
)


def _text(value: Any) -> str:
    if value in (None, "", [], {}) or isinstance(value, (Mapping, list, tuple, set)):
        return ""
    return _WHITESPACE.sub(" ", str(value)).strip()


def _label_key(value: Any) -> str:
    text = unicodedata.normalize("NFKD", _text(value).casefold())
    without_marks = "".join(
        character for character in text if not unicodedata.combining(character)
    )
    return _NON_ALNUM.sub(" ", without_marks).strip()


def _readable_unmapped_label(value: str) -> str:
    label = _WHITESPACE.sub(" ", value).strip()
    if label.isupper():
        return label.title()
    return label


def _has_valid_subtag_relationships(match: re.Match[str]) -> bool:
    primary = match.group("primary")
    if match.group("extlang") and len(primary) > 3:
        return False
    variants = [
        part.casefold()
        for part in (match.group("variants") or "").split("-")
        if part
    ]
    if len(variants) != len(set(variants)):
        return False
    extension_tokens = [
        part.casefold()
        for part in (match.group("extensions") or "").split("-")
        if part
    ]
    singletons = [
        token
        for token in extension_tokens
        if len(token) == 1
    ]
    return len(singletons) == len(set(singletons))


def _code_from_candidate(code: str, system: str = "") -> str | None:
    normalized = code.strip().replace("_", "-")
    if _label_key(normalized) in _UNKNOWN_CODE_KEYS:
        return None
    label_match = _LANGUAGE_BY_LABEL.get(_label_key(normalized))
    if label_match:
        return label_match[0]
    alias = _CODE_ALIASES.get(normalized.casefold())
    if alias:
        return alias
    match = _BCP47_TAG.fullmatch(normalized)
    if match is None or not _has_valid_subtag_relationships(match):
        return None
    primary = match.group("primary").casefold()
    system_key = system.casefold()
    if (
        "bcp" not in system_key
        and "ietf" not in system_key
        and primary not in _DISPLAY_BY_CODE
    ):
        return None
    canonical_parts = [_CODE_ALIASES.get(primary, primary)]
    canonical_parts.extend(
        part.casefold()
        for part in match.group("extlang").split("-")
        if part
    )
    if match.group("script"):
        canonical_parts.append(match.group("script")[1:].title())
    if match.group("region"):
        canonical_parts.append(match.group("region")[1:].upper())
    canonical_parts.extend(
        part.casefold()
        for group_name in ("variants", "extensions", "private")
        for part in (match.group(group_name) or "").split("-")
        if part
    )
    return "-".join(canonical_parts)


def _coding_items(value: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    raw_codes = value.get("codes")
    if raw_codes is None:
        raw_codes = value.get("coding")
    if isinstance(raw_codes, Mapping):
        return [raw_codes]
    if isinstance(raw_codes, list):
        return [item for item in raw_codes if isinstance(item, Mapping)]
    if value.get("code") is not None:
        return [value]
    return []


def _labels(value: Any) -> list[str]:
    if isinstance(value, str):
        return [_text(value)] if _text(value) else []
    if not isinstance(value, Mapping):
        return []
    candidates: list[str] = []
    for field_name in ("language", "text", "display", "name", "label"):
        field_value = _text(value.get(field_name))
        if field_value:
            candidates.append(field_value)
    for coding in _coding_items(value):
        for field_name in ("display", "text"):
            field_value = _text(coding.get(field_name))
            if field_value:
                candidates.append(field_value)
    return list(dict.fromkeys(candidates))


def _matched_labels(labels: list[str]) -> list[tuple[str, str]]:
    return [
        _LANGUAGE_BY_LABEL[label_key]
        for label in labels
        if (label_key := _label_key(label)) in _LANGUAGE_BY_LABEL
    ]


def _matched_codes(raw_language: Any) -> list[str]:
    if not isinstance(raw_language, Mapping):
        return []
    matched_codes: list[str] = []
    for coding in _coding_items(raw_language):
        code = _code_from_candidate(
            _text(coding.get("code")),
            _text(coding.get("system")),
        )
        if code:
            matched_codes.append(code)
    return matched_codes


def _coded_language_by_field(
    code: str,
    labels: list[str],
    label_matches: list[tuple[str, str]],
    preferred: bool,
    has_multiple_matches: bool,
) -> dict[str, Any]:
    primary_code = code.split("-", 1)[0]
    display = _DISPLAY_BY_CODE.get(code) or _DISPLAY_BY_CODE.get(primary_code)
    if not display:
        display = _readable_unmapped_label(labels[0]) if labels else code
    language_by_field: dict[str, Any] = {
        "codes": [
            {
                "system": BCP47_SYSTEM,
                "code": code,
                "display": display,
            }
        ]
    }
    if preferred:
        language_by_field["preferred"] = True
    label_code = label_matches[0][0] if label_matches else None
    if has_multiple_matches:
        language_by_field["normalization_warning"] = (
            "multiple_source_language_codes"
        )
    elif label_code and label_code != primary_code:
        language_by_field["normalization_warning"] = (
            "source_code_display_mismatch"
        )
    return language_by_field


def normalize_language_value(
    raw_language: Any,
) -> tuple[tuple[str, str], dict[str, Any]] | None:
    """Return the provider-level language identity and canonical public value.

    Valid regional and script subtags are preserved. The unmodified source
    coding remains available through opt-in evidence.
    """
    labels = _labels(raw_language)
    label_matches = _matched_labels(labels)
    coded_matches = _matched_codes(raw_language)
    coded_identities = set(coded_matches)
    label_identities = {
        matched_code for matched_code, _display in label_matches
    }
    has_multiple_matches = (
        len(coded_identities) > 1
        or len(label_identities) > 1
    )
    code = coded_matches[0] if coded_matches else (
        label_matches[0][0] if label_matches else None
    )
    preferred = bool(
        isinstance(raw_language, Mapping)
        and raw_language.get("preferred") is True
    )
    if code:
        return (
            ("code", code),
            _coded_language_by_field(
                code,
                labels,
                label_matches,
                preferred,
                has_multiple_matches,
            ),
        )

    if not labels:
        return None
    if all(_label_key(label) in _UNKNOWN_CODE_KEYS for label in labels):
        return None
    if all(_NUMERIC_ONLY_LABEL.fullmatch(label) for label in labels):
        return None
    display = _readable_unmapped_label(labels[0])
    canonical_language_by_field = {"text": display}
    if preferred:
        canonical_language_by_field["preferred"] = True
    return ("text", _label_key(display)), canonical_language_by_field


def language_identity(raw_language: Any) -> tuple[str, str] | None:
    """Return only the stable semantic identity for a language value."""
    normalized = normalize_language_value(raw_language)
    return normalized[0] if normalized else None
