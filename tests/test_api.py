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
    assert isinstance(payload['years'][0], int)


def test_states_endpoint() -> None:
    years = client.get('/api/v1/meta/years').json()['years']
    year = years[-1]
    resp = client.get('/api/v1/states', params={'fiscal_year': year, 'metric': 'medicaid_total'})
    assert resp.status_code == 200
    rows = resp.json()
    assert len(rows) >= 51
    assert 'state_code' in rows[0]


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
            'state_code': 'TX',
        },
    )
    assert resp.status_code == 200
    payload = resp.json()
    assert payload['revenue_at_risk_abs'] == 0
    assert payload['revenue_at_risk_pct'] == 0


def test_export_csv() -> None:
    years = client.get('/api/v1/meta/years').json()['years']
    year = years[-1]

    resp = client.get('/api/v1/exports/facilities.csv', params={'fiscal_year': year, 'state_code': 'FL'})
    assert resp.status_code == 200
    assert 'text/csv' in resp.headers['content-type']
    assert 'facility_id' in resp.text.splitlines()[0]
