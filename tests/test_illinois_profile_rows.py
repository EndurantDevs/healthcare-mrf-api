# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import hashlib
from datetime import datetime
from html import escape

import pytest

from api.provider_education import canonicalize_education_category
from process import illinois_profile_rows as rows

PREFIX = rows.PREFIX
IDENTITY = {"display_name": " Alex  Morgan Example MD ", "original_issue_date": "04/28/2001",
            "license_status": "ACTIVE", "expiration_date": "07/31/2023"}
SCHOOL = "Example University, School of Medicine, Example City, ZZ, 2001"
TRAINING = {"ProgramType": "Internship", "Specialty": "Example Pediatric Residency Program",
            "SchoolLocation": "Example Teaching Hospital, Example City, 2004"}


def span(identifier, raw):
    return f'<span id="{PREFIX}{identifier}">{escape(raw)}</span>'


def section_markup(number, title, entries, *, notice=""):
    heading = span(f'repSection{number}Header_ctl00_ctl00_SectionTitle', title)
    heading += "".join(span(f'repSection{number}Header_ctl00_ctl00_{field}',
                        notice if field == 'SectionPublicDisclaimer' else '')
                       for field in ['HeadingNote', 'Required', 'LastUpdatedText', 'SectionPublicDisclaimer'])
    tables = "".join('<table><tr><td>' + ''.join(span(f'repSection{number}_ctl{index:02d}_ctl00_{field}', raw)
                                              for field, raw in fields.items()) + '</td></tr></table>'
                     for index, fields in entries)
    return f'<div id="{PREFIX}divSection{number}"><table><tr><td>{heading}</td></tr></table>{tables}</div>'


def profile_html(*, identity=None, schools=None, trainings=None, notice=""):
    header = ''.join(span(f'repProfileHeader_ctl00_ctl00_{field}',
                         (identity or IDENTITY)[name]) for field, name in rows.IDENTITY_FIELDS.items())
    school_entries = [(0, {"SchoolLocation": SCHOOL})] if schools is None else schools
    training_entries = [(0, TRAINING)] if trainings is None else trainings
    return f'''<html><body><form id="aspnetForm" method="post" action="ProfileDetails.aspx?did=10&amp;cid=036">
      <div id="{PREFIX}repProfileHeader_ctl00_ctl00_divProfileHeader"><table><tr><td>{header}</td></tr></table></div>
      <a class="profile_tabs_selected" href="javascript:__doPostBack('ctl00$ctl00$MainContent$MainContentContainer$ProfileMenu','3')">Education</a>
      <div id="{PREFIX}divSection1"></div>
      {section_markup('2', 'Medical School', school_entries, notice=notice)}
      {section_markup('3', 'Post Graduate Education', training_entries)}
      </form></body></html>'''


def evidence_for(html):
    return {"run_id": "synthetic-run", "artifact_id": "synthetic-artifact", "row_number": 3,
            "source_url": "https://idfprapps.illinois.gov/Applications/ProfessionProfile/ProfileDetails.aspx",
            "downloaded_at": "2026-09-08T00:00:00Z", "content_sha256": hashlib.sha256(html.encode()).hexdigest()}


def test_original_identity_and_unattached_facts_are_retained():
    html = profile_html()
    source_record, facts = rows.parse_profile(html, evidence=evidence_for(html))
    assert source_record['raw_payload']['html'] == html
    assert source_record['raw_payload']['profile']['identity'] == IDENTITY
    assert source_record['normalized_payload']['profile_identity'] == IDENTITY
    assert source_record['license_number'] is source_record['matched_npi'] is None
    assert source_record['license_id'] is source_record['profession_code'] is None
    assert source_record['match_status'] == 'unmatched'
    assert source_record['match_evidence']['reason'] == 'license_bridge_required'
    assert len(facts) == 2
    assert facts[0]['value_json'] == {'graduation_year': 2001, 'reported_school_location': SCHOOL}
    assert facts[1]['value_json'] == {'completion_year': 2004, 'reported_school_location': TRAINING['SchoolLocation'],
                                     'program_type': 'Internship', 'program': TRAINING['Specialty']}
    assert [(fact['category'], fact['fact_type']) for fact in facts] == [
        ('education', 'education_history'), ('training', 'postgraduate_training')]
    for fact in facts:
        assert fact['npi'] is fact['published_at'] is fact['effective_start'] is fact['effective_end'] is None
        assert fact['assertion_type'] == 'self_reported'
        assert fact['verification_status'] == 'not_independently_verified'
        assert fact['source_json']['source_key'] == 'illinois-idfpr'
        assert fact['source_json']['schema_version'] == 'il-idfpr-profile/v1'
        assert fact['source_json']['source_record_id'] == source_record['record_id']
        assert fact['source_json']['content_sha256'] == evidence_for(html)['content_sha256']
        assert 'institution' not in fact['value_json']
    assert facts[0]['source_json']['raw_fields'] == {'SchoolLocation': SCHOOL}
    assert facts[1]['source_json']['raw_fields'] == TRAINING


def test_multiple_repeated_noncontiguous_rows_preserve_source_order():
    html = profile_html(schools=[(12, {'SchoolLocation': SCHOOL}), (2, {'SchoolLocation': SCHOOL})],
                        trainings=[(8, TRAINING), (0, {**TRAINING, 'ProgramType': 'Residency'})])
    source_record, facts = rows.parse_profile(html, evidence=evidence_for(html))
    assert len(facts) == 4 and len({fact['fact_id'] for fact in facts}) == 4
    assert [fact['source_json']['source_path'] for fact in facts] == [
        'repSection2_ctl12_ctl00', 'repSection2_ctl02_ctl00', 'repSection3_ctl08_ctl00', 'repSection3_ctl00_ctl00']
    assert facts[0]['value_json'] == facts[1]['value_json']
    assert source_record['raw_payload']['profile']['education'][0]['field_ids']['SchoolLocation'].endswith('_ctl12_ctl00_SchoolLocation')


def test_same_year_preserves_unresolved_education_assertions():
    schools = [SCHOOL, 'Another Medical School, Another City, ZZ, 2001']
    html = profile_html(schools=[(index, {'SchoolLocation': school}) for index, school in enumerate(schools)])
    _, facts = rows.parse_profile(html, evidence=evidence_for(html))
    education_facts = [fact for fact in facts if fact['fact_type'] == 'education_history']
    projected_facts = [{'type': fact['fact_type'], 'value': fact['value_json'],
                  'source_kinds': ['state_regulator'], 'source_record_ids': [fact['fact_id']],
                  'source_ids': [rows.SOURCE_KEY], 'public_default': True, 'sensitive': False}
                       for fact in education_facts]
    category_by_field = {'items': [*projected_facts, {'type': 'education_history', 'value': {'graduation_year': 2001},
                                     'source_kinds': ['cms_doctors'], 'source_record_ids': ['synthetic-cms'],
                                     'public_default': True, 'sensitive': False}]}
    canonicalize_education_category(category_by_field)
    assert len(category_by_field['items']) == 3
    assert len({item['logical_fact_key'] for item in category_by_field['items']}) == 3
    for original, composed in zip(projected_facts, category_by_field['items']):
        assert composed['value'] == original['value']
        assert composed['source_record_ids'] == original['source_record_ids']
        assert composed['assertions'][0]['value'] == original['value']
    assert all(not item.get('corroborated_fields') for item in category_by_field['items'])


@pytest.mark.parametrize(('reported', 'expected', 'flag'), [
    ('Example School, City, 2031', 2031, 'graduation_year_in_future'),
    ('Example School, City, 1800', 1800, None),
    ('Example School, City, 1799', None, 'graduation_year_before_1800'),
    ('Example School, City, 0000', None, 'graduation_year_before_1800'),
    ('Example School, City, Unknown', None, 'graduation_year_unresolved'),
    ('Example School 2001', None, 'graduation_year_unresolved'),
    ('Example School, City, ٢٠٠١', None, 'graduation_year_unresolved'),
    ('Example School, City, 2001?', None, 'graduation_year_unresolved'),
    ('Example School, City, 2001, note', None, 'graduation_year_unresolved'),
])
def test_only_strict_terminal_reported_year_is_normalized(reported, expected, flag):
    html = profile_html(schools=[(0, {'SchoolLocation': reported})])
    _, facts = rows.parse_profile(html, evidence=evidence_for(html))
    assert facts[0]['value_json'].get('graduation_year') == expected
    assert facts[0]['source_json']['raw_fields']['SchoolLocation'] == reported
    assert 'institution_boundary_unresolved' in facts[0]['source_json']['quality_flags']
    if flag:
        assert flag in facts[0]['source_json']['quality_flags']
    assert 'graduation_year' not in facts[1]['value_json']


def test_source_type_is_not_overridden_by_program_text_or_license_dates():
    identity_by_field = {**IDENTITY, 'original_issue_date': 'Unknown', 'expiration_date': '07/31/1999'}
    html = profile_html(identity=identity_by_field, trainings=[(0, {**TRAINING, 'SchoolLocation': 'Example Hospital, City, 2030'})])
    source_record, facts = rows.parse_profile(html, evidence=evidence_for(html))
    assert source_record['normalized_payload']['profile_identity'] == identity_by_field
    assert facts[1]['value_json']['program_type'] == 'Internship'
    assert facts[1]['value_json']['completion_year'] == 2030
    assert 'completion_year_in_future' in facts[1]['source_json']['quality_flags']
    assert not any(key in fact['value_json'] for fact in facts for key in ['experience', 'student', 'completed_degree'])


def test_raw_nested_markup_spacing_and_evidence_are_independent():
    html = profile_html().replace(escape(SCHOOL), '  Example <b>University</b>,<br> City, 2001 ')
    evidence = evidence_for(html)
    evidence['downloaded_at'] = datetime.fromisoformat(evidence['downloaded_at'])
    source_record, facts = rows.parse_profile(html, evidence=evidence)
    assert facts[0]['source_json']['raw_fields']['SchoolLocation'] == '  Example University,\n City, 2001 '
    assert facts[0]['value_json']['reported_school_location'] == 'Example University, City, 2001'
    assert facts[0]['source_json']['downloaded_at'].endswith('+00:00')
    source_record['raw_payload']['profile']['education'][0]['fields']['SchoolLocation'] = 'changed'
    source_record['normalized_payload']['quality_flags'].append('record-only')
    facts[0]['source_json']['quality_flags'].append('fact-only')
    evidence['run_id'] = 'changed'
    assert facts[0]['source_json']['raw_fields']['SchoolLocation'].startswith('  Example University')
    assert 'record-only' not in facts[0]['source_json']['quality_flags']
    assert 'fact-only' not in facts[1]['source_json']['quality_flags']
    assert facts[0]['source_json']['run_id'] == 'synthetic-run'


def test_source_notices_hold_assertions_with_original_content():
    html = profile_html(notice='This section is not currently available.')
    source_record, facts = rows.parse_profile(html, evidence=evidence_for(html))
    assert facts == []
    assert source_record['normalized_payload']['visibility'] == 'held_source_notice'
    assert source_record['raw_payload']['profile']['education'][0]['fields']['SchoolLocation'] == SCHOOL


@pytest.mark.parametrize(('missing_field', 'reported_field', 'value_field', 'flag'), [
    ('ProgramType', 'Specialty', 'program', 'program_type_unreported'),
    ('Specialty', 'ProgramType', 'program_type', 'specialty_unreported'),
])
def test_reported_training_survives_one_absent_description(missing_field, reported_field, value_field, flag):
    reported_by_field = {field: value for field, value in TRAINING.items() if field != missing_field}
    html = profile_html(trainings=[(8, reported_by_field)])
    record, facts = rows.parse_profile(html, evidence=evidence_for(html))
    fact = facts[1]
    assert record['raw_payload']['html'] == html
    assert fact['source_json']['raw_fields'] == reported_by_field
    assert missing_field not in fact['source_json']['field_ids']
    assert fact['value_json'] == {'completion_year': 2004, 'reported_school_location': TRAINING['SchoolLocation'],
                                 value_field: TRAINING[reported_field]}
    assert flag in fact['source_json']['quality_flags']
    for missing in reported_by_field:
        incomplete = profile_html(trainings=[(8, {key: value for key, value in reported_by_field.items() if key != missing})])
        with pytest.raises(ValueError, match='incomplete_repeater_row'):
            rows.parse_profile(incomplete, evidence=evidence_for(incomplete))
    marker = span('repSection3_ctl08_ctl00_SchoolLocation', TRAINING['SchoolLocation'])
    hidden = html.replace(marker, span('repSection3_ctl08_ctl00_' + missing_field, 'Hidden value').replace('<span ', '<span hidden ') + marker)
    with pytest.raises(ValueError, match='hidden_or_active_section'):
        rows.parse_profile(hidden, evidence=evidence_for(hidden))


def test_legacy_navigation_width_has_narrow_exception():
    original = profile_html()
    opening = '<table class="profile_tabs_selected profile_tabs" cellpadding="0" cellspacing="0" border="0" width="100%" width="95"><tr><td>'
    html = original.replace('<a class="profile_tabs_selected"', opening + '<a class="profile_tabs_selected"', 1)
    html = html.replace('</a>', '</a></td></tr></table>', 1)
    assert rows.extract_profile(html) == rows.extract_profile(original)
    for changed in (html.replace('width="95"', 'width="96"'), html.replace('cellpadding="0"', 'cellpadding="0" cellpadding="1"')):
        with pytest.raises(ValueError, match='duplicate_attribute'):
            rows.parse_profile(changed, evidence=evidence_for(changed))
    nested = original.replace(f'<div id="{PREFIX}divSection2">', f'<div id="{PREFIX}divSection2">' + opening)
    with pytest.raises(ValueError, match='duplicate_attribute'):
        rows.parse_profile(nested, evidence=evidence_for(nested))


@pytest.mark.parametrize(('old', 'new'), [
    ('profile_tabs_selected', 'profile_tabs'),
    ('Medical School', 'Other School'),
    ('</html>', ''),
    ('</html>', '</html><div>extra</div>'),
    ('</html>', '</html>extra'),
    ('<form id="aspnetForm"', '<form id="aspnetForm" hidden'),
    ('<body>', '<body style="display:none">'),
    ('method="post"', 'method="get"'),
    ('action="ProfileDetails.aspx', 'action="https://example.test/ProfileDetails.aspx'),
    ('<span id=', '<span id="duplicate" id='),
    ('_ctl00_ctl00_FormattedContactName', '_ctl00_ctl00_UnknownName'),
    ('_ctl00_ctl00_ProgramType', '_ctl00_ctl00_UnknownField'),
    ('_ctl00_ctl00_SchoolLocation', '_ctl00_ctl00_ChangedLocation'),
    ('_ctl00_ctl00_FirstEffectiveDate', '_ctl02_ctl00_FormattedContactName'),
    (f'id="{PREFIX}divSection2"', f'id="{PREFIX}divSection2" aria-hidden="true"'),
    (f'id="{PREFIX}divSection3"', f'id="{PREFIX}divSection2"'),
])
def test_unrecognized_or_malformed_pages_fail(old, new):
    html = profile_html().replace(old, new)
    with pytest.raises(ValueError, match='illinois_profile_'):
        rows.extract_profile(html)


@pytest.mark.parametrize('content', [
    '<p>Service temporarily unavailable</p>', '<span>Changed unrecognized layout</span>',
    '<iframe src="https://example.test"></iframe>', '<script>alert(1)</script>',
])
def test_intact_section_envelope_does_not_hide_error_content(content):
    html = profile_html().replace(f'<div id="{PREFIX}divSection2">', f'<div id="{PREFIX}divSection2">{content}')
    with pytest.raises(ValueError, match='illinois_profile_'):
        rows.extract_profile(html)


def test_duplicate_missing_mixed_and_split_rows_fail():
    duplicate = profile_html(schools=[(0, {'SchoolLocation': SCHOOL}), (0, {'SchoolLocation': SCHOOL})])
    missing = profile_html(trainings=[(0, {'SchoolLocation': SCHOOL})])
    empty = profile_html(schools=[])
    blank = profile_html(schools=[(0, {'SchoolLocation': ' '})])
    for html in (duplicate, missing, empty, blank):
        with pytest.raises(ValueError, match='illinois_profile_'):
            rows.extract_profile(html)
    html = profile_html()
    marker = span('repSection3_ctl00_ctl00_Specialty', TRAINING['Specialty'])
    split = html.replace(marker, '</td></tr></table><table><tr><td>' + marker)
    with pytest.raises(ValueError, match='split_repeater_row'):
        rows.extract_profile(split)


@pytest.mark.parametrize(('field', 'value'), [
    ('content_sha256', 'a' * 64), ('row_number', True), ('row_number', 0), ('run_id', ''),
    ('artifact_id', None), ('source_url', 7), ('downloaded_at', 'invalid'), ('downloaded_at', '2026-01-01'),
])
def test_invalid_provenance_cannot_create_facts(field, value):
    html = profile_html()
    with pytest.raises(ValueError, match='illinois_profile_evidence_'):
        rows.parse_profile(html, evidence={**evidence_for(html), field: value})


def test_input_and_field_limits_fail_closed(monkeypatch):
    with pytest.raises(ValueError, match='html_input_limit'):
        rows.extract_profile('x' * (rows.MAX_HTML_BYTES + 1))
    with pytest.raises(ValueError, match='html_input_limit'):
        rows.extract_profile(None)
    with pytest.raises(ValueError, match='incomplete_profile'):
        rows.extract_profile('<html><body>Service unavailable</body>')
    monkeypatch.setattr(rows, 'MAX_FIELDS', 3)
    with pytest.raises(ValueError, match='field_count_limit'):
        rows.extract_profile(profile_html())


@pytest.mark.parametrize('markup', ['<td hidden>', '<td style="display: none!important">', '<td aria-hidden="true">'])
def test_hidden_header_ancestors_cannot_supply_visible_identity(markup):
    html = profile_html().replace('<table><tr><td>', '<table><tr>' + markup, 1)
    with pytest.raises(ValueError, match='hidden_or_active_section'):
        rows.extract_profile(html)


def test_selected_tab_must_have_observed_education_target():
    html = profile_html().replace("ProfileMenu','3'", "ProfileMenu','7'")
    with pytest.raises(ValueError, match='education_tab_not_selected'):
        rows.extract_profile(html)


@pytest.mark.parametrize('url', ['https://example.test/ProfileDetails.aspx',
                                 'https://secret@example.test/ProfileDetails.aspx',
                                 'https://idfprapps.illinois.gov/Applications/ProfessionProfile/Default.aspx'])
def test_provenance_rejects_unrelated_source_routes(url):
    html = profile_html()
    with pytest.raises(ValueError, match='evidence_url_invalid'):
        rows.parse_profile(html, evidence={**evidence_for(html), 'source_url': url})


def test_legacy_navigation_and_inert_footer_spans_are_allowed():
    html = profile_html().replace('</form>', '<span>Public profile footer</span></form>')
    html = html.replace('Medical School</span>', 'Medical School</span><a href="#TopOfProfile">Top of Profile</a>')
    source_record, facts = rows.parse_profile(html, evidence=evidence_for(html))
    assert source_record['normalized_payload']['visibility'] == 'public'
    assert len(facts) == 2


def test_training_without_reported_location_keeps_program_only():
    html = profile_html(trainings=[(0, {**TRAINING, 'SchoolLocation': ''})])
    _, facts = rows.parse_profile(html, evidence=evidence_for(html))
    assert facts[1]['value_json'] == {'program_type': 'Internship', 'program': TRAINING['Specialty']}
    assert facts[1]['source_json']['raw_fields']['SchoolLocation'] == ''
    assert 'completion_year_unresolved' in facts[1]['source_json']['quality_flags']


@pytest.mark.parametrize('markup', ['<div>Changed layout</div>', '<table><tr><td>Changed layout</td></tr></table>'])
def test_block_layout_inside_field_is_unrecognized(markup):
    html = profile_html().replace(escape(SCHOOL), markup)
    with pytest.raises(ValueError, match='unexpected_field_markup'):
        rows.extract_profile(html)


def test_hidden_education_navigation_cannot_prove_selected_view():
    html = profile_html().replace('<a class="profile_tabs_selected"', '<a hidden class="profile_tabs_selected"')
    with pytest.raises(ValueError, match='hidden_container'):
        rows.extract_profile(html)


@pytest.mark.parametrize('container', ['section hidden', 'table style="display:none"', 'span aria-hidden="true"'])
def test_hidden_outer_containers_cannot_publish_fields(container):
    html = profile_html().replace('<body>', '<body><' + container + '>')
    html = html.replace('</body>', '</' + container.split()[0] + '></body>')
    with pytest.raises(ValueError, match='hidden_container'):
        rows.parse_profile(html, evidence=evidence_for(html))


def test_form_state_preserves_public_fields():
    html = profile_html().replace('<div id=', '<input type="hidden" name="__VIEWSTATE" value="synthetic"><div id=', 1)
    _, facts = rows.parse_profile(html, evidence=evidence_for(html))
    assert len(facts) == 2


def test_mixed_header_identity_fails():
    html = profile_html().replace('_ctl00_ctl00_FirstEffectiveDate', '_ctl01_ctl00_FirstEffectiveDate')
    with pytest.raises(ValueError, match='unrecognized_header_field'):
        rows.extract_profile(html)
    html = profile_html()
    marker = span('repProfileHeader_ctl00_ctl00_LicenseStatus', IDENTITY['license_status'])
    html = html.replace(marker, '</td></tr></table><table><tr><td>' + marker)
    with pytest.raises(ValueError, match='mixed_header_tables'):
        rows.extract_profile(html)


def test_nested_header_preserves_identity():
    html = profile_html()
    marker = span('repProfileHeader_ctl00_ctl00_FormattedContactName', IDENTITY['display_name'])
    html = html.replace(marker, marker + '<table><tr><td>')
    html = html.replace('</td></tr></table></div>', '</td></tr></table></td></tr></table></div>', 1)
    assert rows.extract_profile(html)['identity'] == IDENTITY
