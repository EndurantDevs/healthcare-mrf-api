# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Deterministic display rendering for structured address components.

Renderers never select sources or use geocoder labels. Callers pass the
structured components chosen by canonicalization or materialization.
"""

from __future__ import annotations

import re
import unicodedata

from process.ext.address_format_constants import (
    ADDRESS_UNIT_PREFIX_DISPLAY,
    PUB28_STREET_SUFFIX_MAP,
    _AMBIGUOUS_UNIT_WORDS,
    _ASCII_WHITESPACE_RE,
    _BOX_HASH_RE,
    _CITY_PREFIX_DISPLAY,
    _COMPOUND_DIRECTION_RE,
    _DEFAULT_US_COUNTRIES,
    _DIRECTION_DISPLAY,
    _DISPLAY_SPACE_TRANSLATION,
    _display_street_suffix,
    _humanize_unit_value,
    _humanize_word,
    _PO_BOX_RE,
    _STREET_SUFFIX_DISPLAY,
    _TOKEN_RE,
    _TRUNCATION_SUFFIX,
    _UNIT_MARKER_SEPARATOR_RE,
    _UNIT_PREFIX_DISPLAY,
    _US_ZIP4_SEPARATED_RE,
    _US_ZIP9_RE,
)

ADDRESS_FORMAT_VERSION = 2
ADDRESS_FORMAT_SOURCE = "canonical_v2"
ADDRESS_FORMAT_FUNCTION = "addr_formatted_address_v2"
ADDRESS_FORMAT_MAX_LENGTH = 1024



def _clean_component(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = unicodedata.normalize("NFC", str(value)).translate(
        _DISPLAY_SPACE_TRANSLATION
    )
    cleaned = _ASCII_WHITESPACE_RE.sub(" ", normalized).strip()
    return cleaned or None


def _us_postal_code(value: str | None) -> str | None:
    if value is None:
        return None
    match = _US_ZIP9_RE.fullmatch(value) or _US_ZIP4_SEPARATED_RE.fullmatch(
        value
    )
    if match is None:
        return value
    return f"{match.group(1)}-{match.group(2)}"


def _bounded_display(value: str) -> str | None:
    if len(value) <= ADDRESS_FORMAT_MAX_LENGTH:
        return value
    bounded = value[:ADDRESS_FORMAT_MAX_LENGTH].rstrip(_TRUNCATION_SUFFIX)
    return bounded or None


def _display_second_line(
    line_one: str | None,
    line_two: str | None,
) -> str | None:
    """Suppress only an exact normalized unit already present in line one."""
    if line_one is None or line_two is None:
        return line_two
    if line_one == line_two:
        return None
    suffix_start = len(line_one) - len(line_two)
    if (
        suffix_start > 0
        and line_one.endswith(line_two)
        and line_one[suffix_start - 1] in " ,;"
    ):
        return None
    return line_two


def _display_second_line_v2(
    line_one: str | None,
    line_two: str | None,
    *,
    source_line_one: str | None = None,
    source_line_two: str | None = None,
) -> str | None:
    """Suppress a repeated humanized unit without trusting source casing."""
    if line_one is None or line_two is None:
        return line_two
    folded_one = line_one.lower()
    folded_two = line_two.lower()
    if folded_one == folded_two:
        return None
    suffix_start = len(line_one) - len(line_two)
    if (
        suffix_start > 0
        and line_one[suffix_start:].lower() == folded_two
        and line_one[suffix_start - 1] in " ,;"
    ):
        return None
    if source_line_one is not None and source_line_two is not None:
        from process.ext.address_canon import street_norm, unit_norm

        line_one_unit = unit_norm(source_line_one, None)
        if line_one_unit and line_one_unit == unit_norm(source_line_two, None):
            line_one_street = street_norm(source_line_one, None)
            if line_one_street and line_one_street == street_norm(
                source_line_two,
                None,
            ):
                return None
        if line_one_unit and line_one_unit == unit_norm(
            source_line_one,
            source_line_two,
        ):
            line_one_street = street_norm(source_line_one, None)
            if line_one_street and line_one_street == street_norm(
                source_line_one,
                source_line_two,
            ):
                return None
    return line_two


def _country_key(country: str | None) -> str | None:
    if country is None:
        return None
    return re.sub(r"[^A-Z]", "", country.upper()) or None


def _has_street_suffix_before_unit(address_line: str) -> bool:
    from process.ext.address_canon import street_suffix_token

    without_post_direction = re.sub(
        r"\s+(?:N|S|E|W|NE|NW|SE|SW)\.?$",
        "",
        address_line,
        flags=re.IGNORECASE,
    )
    return street_suffix_token(without_post_direction, None) is not None


def _display_tail_unit(
    address_line: str,
    *,
    is_line_one: bool,
) -> tuple[str | None, str] | None:
    """Split one valid canonical tail unit without treating street words as units."""
    from process.ext.address_canon import UNIT_TAIL_RE, _tail_unit

    parse_value = _UNIT_MARKER_SEPARATOR_RE.sub(
        lambda match: f"{match.group(1)}{match.group(2)} {match.group(3)}",
        address_line,
    )
    source_padded = f" {address_line} "
    padded = f" {parse_value} "
    tail = _tail_unit(padded)
    if tail is None:
        return None
    unit_key, start = tail
    source_tail = UNIT_TAIL_RE.search(padded, start)
    for prefix, display_prefix in _UNIT_PREFIX_DISPLAY:
        if unit_key == prefix or (
            unit_key.startswith(prefix) and len(unit_key) > len(prefix)
        ):
            raw_unit_value = unit_key[len(prefix):]
            if (
                source_tail is not None
                and source_tail.start() == start
                and source_tail.group(2) != "#"
                and source_tail.group(3)
                and not padded[source_tail.end(2):source_tail.start(3)]
                and raw_unit_value.isalpha()
            ):
                return None
            street_value = _clean_component(source_padded[:start])
            if street_value is not None:
                street_value = street_value.rstrip(" ,;:") or None
            if (
                is_line_one
                and source_tail is not None
                and source_tail.group(2).lower() in _AMBIGUOUS_UNIT_WORDS
                and source_tail.group(1) != ","
                and not _has_street_suffix_before_unit(street_value or "")
            ):
                return None
            source_unit_value = raw_unit_value
            if source_tail is not None and source_tail.group(3):
                source_unit_value = source_tail.group(3)
                if source_tail.group(4):
                    source_unit_value += f" {source_tail.group(4)}"
            unit_value = _humanize_unit_value(source_unit_value)
            unit_display = " ".join(
                component for component in (display_prefix, unit_value) if component
            )
            return street_value, unit_display
    return None


def _render_component_text(
    cleaned: str,
    *,
    component_kind: str,
    is_us_style: bool,
) -> str:
    """Humanize tokens after component-specific cleanup."""
    tokens = _TOKEN_RE.findall(cleaned)
    alpha_positions = [
        index for index, token in enumerate(tokens)
        if any(character.isalpha() for character in token)
    ]
    first_alpha = alpha_positions[0] if alpha_positions else None
    suffix_position = next(
        (
            index
            for index in reversed(alpha_positions)
            if tokens[index].upper() not in _DIRECTION_DISPLAY
            and len(tokens[index]) > 1
        ),
        None,
    )

    rendered_tokens: list[str] = []
    should_drop_suffix_period = False
    for index, token in enumerate(tokens):
        if not any(character.isalnum() for character in token):
            if should_drop_suffix_period:
                without_period = re.sub(r"^(\s*)\.", r"\1", token, count=1)
                should_drop_suffix_period = without_period == token and not token.strip()
                token = without_period
            rendered_tokens.append(token)
            continue
        upper_token = token.upper()
        rendered_token: str | None = None
        if index != suffix_position:
            should_drop_suffix_period = False
        if is_us_style and component_kind in {"line1", "line2"}:
            if index == suffix_position:
                rendered_token = _display_street_suffix(token)
                should_drop_suffix_period = rendered_token is not None
            if (
                rendered_token is None
                and upper_token in _DIRECTION_DISPLAY
                and (
                    index == first_alpha
                    or (
                        suffix_position is not None
                        and index > suffix_position
                    )
                )
            ):
                rendered_token = _DIRECTION_DISPLAY[upper_token]
                should_drop_suffix_period = True
        elif is_us_style and component_kind == "city" and index == first_alpha:
            rendered_token = _CITY_PREFIX_DISPLAY.get(upper_token)
            should_drop_suffix_period = rendered_token is not None
        rendered_tokens.append(rendered_token or _humanize_word(token))
    return "".join(rendered_tokens)


def _humanize_address_component(
    cleaned: str,
    *,
    is_line_one: bool,
    is_us_style: bool,
) -> str | None:
    """Humanize one address line and its canonical tail units."""
    if is_us_style:
        cleaned = _BOX_HASH_RE.sub(lambda match: f"BOX {match.group(1)}", cleaned)
    original_cleaned = cleaned
    unit_displays: list[str] = []
    while is_us_style:
        unit_tail = _display_tail_unit(cleaned, is_line_one=is_line_one)
        if unit_tail is None:
            break
        street_value, unit_display = unit_tail
        if street_value == cleaned:
            break
        unit_displays.insert(0, unit_display)
        cleaned = street_value
        if cleaned is None:
            break
    if not is_line_one:
        if cleaned is None and unit_displays:
            return ", ".join(unit_displays)
        cleaned = original_cleaned
        unit_displays.clear()
    if cleaned is None:
        return ", ".join(unit_displays) or None
    cleaned = _PO_BOX_RE.sub("PO Box ", cleaned).rstrip(" ,;:") or None
    if cleaned is None:
        return ", ".join(unit_displays) or None
    component_kind = "line1" if is_line_one else "line2"
    rendered = _render_component_text(
        cleaned,
        component_kind=component_kind,
        is_us_style=is_us_style,
    )
    if is_line_one and unit_displays:
        display_units = ", ".join(unit_displays)
        rendered = (
            f"{rendered.rstrip(' ,;:')}, {display_units}"
            if rendered
            else display_units
        )
    return rendered


def _humanize_component(
    component_value: str | None,
    *,
    component_kind: str,
    is_us_style: bool,
) -> str | None:
    """Humanize one structured component without importing display labels."""
    cleaned = _clean_component(component_value)
    if cleaned is None:
        return None
    cleaned = cleaned.rstrip(" ,;:") or None
    if cleaned is None:
        return None
    if is_us_style and component_kind in {"line1", "line2", "city"}:
        cleaned = _COMPOUND_DIRECTION_RE.sub(
            lambda match: f"{match.group(1)}{match.group(2)}".upper(),
            cleaned,
        )
    if component_kind in {"line1", "line2"}:
        return _humanize_address_component(
            cleaned,
            is_line_one=component_kind == "line1",
            is_us_style=is_us_style,
        )
    if component_kind == "postal":
        return cleaned.upper()
    if component_kind == "state" and (
        (is_us_style and cleaned.isalpha() and len(cleaned) == 2)
        or (cleaned.isalpha() and len(cleaned) <= 3)
    ):
        return cleaned.upper()
    return _render_component_text(
        cleaned,
        component_kind=component_kind,
        is_us_style=is_us_style,
    )


def _display_country(
    cleaned_country: str | None,
    *,
    is_us_style: bool,
) -> str | None:
    if is_us_style:
        return None
    if (
        cleaned_country is not None
        and cleaned_country.isalpha()
        and len(cleaned_country) <= 3
    ):
        return cleaned_country.upper()
    return _humanize_component(
        cleaned_country,
        component_kind="country",
        is_us_style=False,
    )


def render_formatted_address_v1(
    first_line: str | None,
    second_line: str | None,
    city_name: str | None,
    state_name: str | None,
    postal_code: str | None,
    country_code: str | None,
) -> str | None:
    """Render one stable display string from structured address components."""

    line_one = _clean_component(first_line)
    line_two = _display_second_line(
        line_one,
        _clean_component(second_line),
    )
    city = _clean_component(city_name)
    state = _clean_component(state_name)
    postal = _clean_component(postal_code)
    country = _clean_component(country_code)
    if country is not None:
        country = country.upper()
    if country in (None, "US"):
        postal = _us_postal_code(postal)

    state_postal = " ".join(
        component for component in (state, postal) if component
    ) or None
    locality = ", ".join(
        component for component in (city, state_postal) if component
    ) or None
    displayed_country = country if country not in (None, "US") else None
    rendered = ", ".join(
        component
        for component in (line_one, line_two, locality, displayed_country)
        if component
    )
    if not rendered:
        return None
    return _bounded_display(rendered)


def render_formatted_address_v2(
    first_line: str | None,
    second_line: str | None,
    city_name: str | None,
    state_name: str | None,
    postal_code: str | None,
    country_code: str | None,
) -> str | None:
    """Render a coherent, human-readable label from structured components."""

    cleaned_country = _clean_component(country_code)
    country_key = _country_key(cleaned_country)
    is_us_style = cleaned_country is None or country_key in _DEFAULT_US_COUNTRIES
    line_one = _humanize_component(
        first_line,
        component_kind="line1",
        is_us_style=is_us_style,
    )
    line_two = _display_second_line_v2(
        line_one,
        _humanize_component(
            second_line,
            component_kind="line2",
            is_us_style=is_us_style,
        ),
        source_line_one=first_line,
        source_line_two=second_line,
    )
    city = _humanize_component(
        city_name,
        component_kind="city",
        is_us_style=is_us_style,
    )
    state = _humanize_component(
        state_name,
        component_kind="state",
        is_us_style=is_us_style,
    )
    postal = _humanize_component(
        postal_code,
        component_kind="postal",
        is_us_style=is_us_style,
    )
    if is_us_style:
        postal = _us_postal_code(postal)

    state_postal = " ".join(
        component for component in (state, postal) if component
    ) or None
    locality = ", ".join(
        component for component in (city, state_postal) if component
    ) or None
    displayed_country = _display_country(cleaned_country, is_us_style=is_us_style)
    rendered = ", ".join(
        component
        for component in (line_one, line_two, locality, displayed_country)
        if component
    )
    return _bounded_display(rendered) if rendered else None
