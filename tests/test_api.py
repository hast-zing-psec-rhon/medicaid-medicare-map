from __future__ import annotations

from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def test_healthz() -> None:
    resp = client.get('/healthz')
    assert resp.status_code == 200
    assert resp.json()['ok'] is True


def test_meta_years() -> None:
    resp = client.get('/api/v1/meta/years')
    assert resp.status_code == 200
    payload = resp.json()
    assert payload['years']
    assert len(payload['years']) >= 10
    assert isinstance(payload['years'][0], int)


def test_states_endpoint() -> None:
    years = client.get('/api/v1/meta/years').json()['years']
    year = years[-1]
    resp = client.get('/api/v1/states', params={'fiscal_year': year, 'metric': 'medicaid_total'})
    assert resp.status_code == 200
    rows = resp.json()
    assert len(rows) >= 51
    assert 'state_code' in rows[0]

    comp = client.get(
        '/api/v1/states',
        params={
            'fiscal_year': year,
            'metric': 'private_total',
            'payer_scope': 'comprehensive',
            'taxonomy_view': 'funding_source',
        },
    )
    assert comp.status_code == 200
    comp_rows = comp.json()
    assert len(comp_rows) >= 51
    assert 'private_total' in comp_rows[0]


def test_state_summary_and_facilities() -> None:
    years = client.get('/api/v1/meta/years').json()['years']
    year = years[-1]

    summary = client.get(f'/api/v1/states/CA/summary', params={'fiscal_year': year})
    assert summary.status_code == 200
    assert summary.json()['state_code'] == 'CA'

    facilities = client.get(
        f'/api/v1/states/CA/facilities',
        params={'fiscal_year': year, 'ownership': 'all', 'sort': 'medicaid_revenue'},
    )
    assert facilities.status_code == 200
    rows = facilities.json()
    assert len(rows) > 0
    assert 'facility_id' in rows[0]


def test_scenario_zero_cut_identity() -> None:
    years = client.get('/api/v1/meta/years').json()['years']
    year = years[-1]

    resp = client.post(
        '/api/v1/scenarios/run',
        json={
            'fiscal_year': year,
            'medicare_cut_pct': 0,
            'federal_medicaid_cut_pct': 0,
            'state_medicaid_cut_pct': 0,
            'private_cut_pct': 0,
            'state_code': 'TX',
        },
    )
    assert resp.status_code == 200
    payload = resp.json()
    assert payload['revenue_at_risk_abs'] == 0
    assert payload['revenue_at_risk_pct'] == 0


def test_scenario_private_cut_has_effect() -> None:
    years = client.get('/api/v1/meta/years').json()['years']
    year = years[-1]
    resp = client.post(
        '/api/v1/scenarios/run',
        json={
            'fiscal_year': year,
            'medicare_cut_pct': 0,
            'federal_medicaid_cut_pct': 0,
            'state_medicaid_cut_pct': 0,
            'private_cut_pct': 15,
            'payer_scope': 'comprehensive',
            'state_code': 'CA',
        },
    )
    assert resp.status_code == 200
    payload = resp.json()
    assert payload['revenue_at_risk_abs'] >= 0
    assert payload['baseline_total_revenue'] >= payload['shocked_total_revenue']


def test_export_csv() -> None:
    years = client.get('/api/v1/meta/years').json()['years']
    year = years[-1]

    resp = client.get('/api/v1/exports/facilities.csv', params={'fiscal_year': year, 'state_code': 'FL'})
    assert resp.status_code == 200
    assert 'text/csv' in resp.headers['content-type']
    assert 'facility_id' in resp.text.splitlines()[0]


def test_trend_endpoints() -> None:
    national = client.get('/api/v1/trends/national', params={'payer_scope': 'comprehensive'})
    assert national.status_code == 200
    national_rows = national.json()
    assert len(national_rows) >= 10
    assert national_rows[0]['fiscal_year'] < national_rows[-1]['fiscal_year']

    state = client.get('/api/v1/trends/states/CA', params={'payer_scope': 'comprehensive'})
    assert state.status_code == 200
    state_rows = state.json()
    assert len(state_rows) >= 10

    year = client.get('/api/v1/meta/years').json()['years'][-1]
    chains = client.get(
        '/api/v1/chains',
        params={'fiscal_year': year, 'state_code': 'CA', 'payer_scope': 'comprehensive'},
    )
    assert chains.status_code == 200
    assert chains.json()
    chain_name = chains.json()[0]['chain_name']
    chain = client.get(
        '/api/v1/trends/chains',
        params={'chain_name': chain_name, 'state_code': 'CA', 'payer_scope': 'comprehensive'},
    )
    assert chain.status_code == 200
    assert len(chain.json()) >= 1

    facilities = client.get('/api/v1/states/CA/facilities', params={'fiscal_year': year})
    assert facilities.status_code == 200
    assert facilities.json()
    facility_id = facilities.json()[0]['facility_id']
    fac_trend = client.get(f'/api/v1/trends/facilities/{facility_id}')
    assert fac_trend.status_code == 200
    assert len(fac_trend.json()) >= 1


def test_root_serves_dashboard_shell() -> None:
    resp = client.get('/')
    assert resp.status_code == 200
    assert "National KPIs" in resp.text
    assert "Scenario Builder" in resp.text


def test_private_insurer_endpoints() -> None:
    years = client.get('/api/v1/meta/years').json()['years']
    year = years[-1]

    state_insurers = client.get(
        '/api/v1/states/CA/insurers',
        params={'fiscal_year': year, 'basis': 'covered_lives', 'top_n': 10},
    )
    assert state_insurers.status_code == 200
    assert isinstance(state_insurers.json(), list)

    chains = client.get('/api/v1/chains', params={'fiscal_year': year, 'state_code': 'CA'})
    assert chains.status_code == 200
    assert chains.json()
    chain_name = next((c['chain_name'] for c in chains.json() if '/' not in c['chain_name']), chains.json()[0]['chain_name'])
    chain_exposure = client.get(
        '/api/v1/exposures/chains/insurers',
        params={'fiscal_year': year, 'basis': 'covered_lives', 'chain_name': chain_name},
    )
    assert chain_exposure.status_code == 200
    assert 'insurers' in chain_exposure.json()


def test_emma_link_endpoint_exists() -> None:
    years = client.get('/api/v1/meta/years').json()['years']
    year = years[-1]
    facilities = client.get('/api/v1/states/CA/facilities', params={'fiscal_year': year})
    assert facilities.status_code == 200
    nfp_rows = [r for r in facilities.json() if r.get('ownership_group') == 'not_for_profit']
    assert nfp_rows
    facility_id = nfp_rows[0]['facility_id']

    resp = client.get(f'/api/v1/facilities/{facility_id}/emma-link', params={'fiscal_year': year})
    assert resp.status_code == 200
    payload = resp.json()
    assert payload['facility_id'] == facility_id
    assert 'emma_mapping_status' in payload
    assert 'emma_issuer_url' in payload
    assert 'emma_resolved_url' in payload
    assert 'emma_link_type' in payload
    assert 'emma_fallback_status' in payload


def test_emma_link_include_fallback_uses_client_when_unmapped(monkeypatch) -> None:
    years = client.get('/api/v1/meta/years').json()['years']
    year = years[-1]
    facility_id = 'TEST_NFP_001'

    monkeypatch.setattr(
        'app.main.get_facility_emma_link',
        lambda _store, facility_id, year: {
            'facility_id': facility_id,
            'facility_name': 'Test NFP Hospital',
            'state_code': 'CA',
            'ownership_group': 'not_for_profit',
            'emma_mapping_status': 'unmapped',
            'emma_mapping_method': 'none',
            'emma_match_score': 0.0,
            'emma_issuer_id': '',
            'emma_issuer_name': '',
            'emma_issuer_url': '',
            'emma_primary_url': '',
            'emma_resolved_url': '',
            'emma_link_type': 'none',
            'emma_fallback_status': 'not_requested',
            'emma_fallback_type': 'none',
            'emma_fallback_url': '',
            'emma_fallback_cusip_query': '',
            'emma_fallback_cusip9': '',
            'emma_fallback_issue_id': '',
            'emma_fallback_issue_desc': '',
            'emma_fallback_issuer_name': '',
            'emma_fallback_match_basis': '',
            'emma_fallback_outstanding_filter_applied': False,
            'emma_fallback_error': '',
        },
    )

    class _StubEmmaClient:
        def find_emma_fallback_link(self, facility_name: str, state_code: str, candidate_cusips: set[str] | None = None, max_issue_rows: int = 8):
            return {
                'emma_fallback_status': 'found',
                'emma_fallback_type': 'cusip',
                'emma_fallback_url': 'https://emma.msrb.org/QuickSearch/Transfer?quickSearchText=123456AA1',
                'emma_fallback_cusip_query': '123456AA1',
                'emma_fallback_cusip9': '123456AA1',
                'emma_fallback_issue_id': 'ES999999',
                'emma_fallback_issue_desc': 'Test issue',
                'emma_fallback_issuer_name': 'Test issuer',
                'emma_fallback_match_basis': 'unit_test_stub',
                'emma_fallback_outstanding_filter_applied': True,
                'emma_fallback_error': '',
            }

    monkeypatch.setattr(app.state, 'emma_client', _StubEmmaClient(), raising=False)
    resp = client.get(
        f'/api/v1/facilities/{facility_id}/emma-link',
        params={'fiscal_year': year, 'include_fallback': True, 'portfolio_id': 'default'},
    )
    assert resp.status_code == 200
    payload = resp.json()
    assert payload['emma_fallback_status'] == 'found'
    assert payload['emma_fallback_type'] == 'cusip'
    assert payload['emma_fallback_cusip_query'] == '123456AA1'
    assert payload['emma_resolved_url'] == payload['emma_fallback_url']


def test_emma_link_include_fallback_applies_to_government_facility(monkeypatch) -> None:
    years = client.get('/api/v1/meta/years').json()['years']
    year = years[-1]
    facility_id = 'TEST_GOV_001'

    monkeypatch.setattr(
        'app.main.get_facility_emma_link',
        lambda _store, facility_id, year: {
            'facility_id': facility_id,
            'facility_name': 'Test Government Hospital',
            'state_code': 'NY',
            'ownership_group': 'government',
            'emma_mapping_status': 'not_applicable',
            'emma_mapping_method': 'none',
            'emma_match_score': 0.0,
            'emma_issuer_id': '',
            'emma_issuer_name': '',
            'emma_issuer_url': '',
            'emma_primary_url': '',
            'emma_resolved_url': '',
            'emma_link_type': 'none',
            'emma_fallback_status': 'not_requested',
            'emma_fallback_type': 'none',
            'emma_fallback_url': '',
            'emma_fallback_search_term': '',
            'emma_fallback_cusip_query': '',
            'emma_fallback_cusip9': '',
            'emma_fallback_issue_id': '',
            'emma_fallback_issue_desc': '',
            'emma_fallback_issuer_name': '',
            'emma_fallback_match_basis': '',
            'emma_fallback_outstanding_filter_applied': False,
            'emma_fallback_error': '',
        },
    )

    class _StubEmmaClient:
        def find_emma_fallback_link(self, facility_name: str, state_code: str, candidate_cusips: set[str] | None = None, max_issue_rows: int = 8):
            return {
                'emma_fallback_status': 'found',
                'emma_fallback_type': 'cusip',
                'emma_fallback_url': 'https://emma.msrb.org/QuickSearch/Transfer?quickSearchText=64990GYA4',
                'emma_fallback_search_term': facility_name,
                'emma_fallback_cusip_query': '64990GYA4',
                'emma_fallback_cusip9': '64990GYA4',
                'emma_fallback_issue_id': 'ER397062',
                'emma_fallback_issue_desc': 'NYU LANGONE HOSPITALS OBLIGATED GROUP REVENUE BONDS, SERIES 2020A',
                'emma_fallback_issuer_name': 'DORMITORY AUTHORITY - STATE OF NEW YORK',
                'emma_fallback_match_basis': 'unit_test_stub_government',
                'emma_fallback_outstanding_filter_applied': True,
                'emma_fallback_error': '',
            }

    monkeypatch.setattr(app.state, 'emma_client', _StubEmmaClient(), raising=False)
    resp = client.get(
        f'/api/v1/facilities/{facility_id}/emma-link',
        params={'fiscal_year': year, 'include_fallback': True, 'portfolio_id': 'default'},
    )
    assert resp.status_code == 200
    payload = resp.json()
    assert payload['ownership_group'] == 'government'
    assert payload['emma_fallback_status'] == 'found'
    assert payload['emma_link_type'] == 'cusip'
    assert payload['emma_resolved_url'] == 'https://emma.msrb.org/QuickSearch/Transfer?quickSearchText=64990GYA4'


def test_portfolio_holdings_summary_endpoint() -> None:
    resp = client.get('/api/v1/portfolio/holdings/summary')
    assert resp.status_code == 200
    payload = resp.json()
    assert 'portfolio_count' in payload
    assert 'holding_count' in payload
    assert 'unique_cusip_count' in payload


def test_portfolio_linkage_unmapped_or_empty_holdings_graceful() -> None:
    years = client.get('/api/v1/meta/years').json()['years']
    year = years[-1]

    facilities = client.get('/api/v1/states/CA/facilities', params={'fiscal_year': year})
    assert facilities.status_code == 200
    facility_id = facilities.json()[0]['facility_id']

    # First validate unmapped/empty branch behavior without relying on external network.
    resp = client.get(
        f'/api/v1/facilities/{facility_id}/portfolio-linkage',
        params={'fiscal_year': year, 'portfolio_id': 'default'},
    )
    assert resp.status_code == 200
    payload = resp.json()
    assert payload['facility_id'] == facility_id
    assert 'owned_securities' in payload and isinstance(payload['owned_securities'], list)
    assert 'related_documents' in payload and isinstance(payload['related_documents'], list)
    assert payload['scrape_status'] in {'skipped', 'ok', 'partial', 'error'}
