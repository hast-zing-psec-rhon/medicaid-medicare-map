const appState = {
  year: null,
  metric: 'medicaid_total',
  selectedState: null,
  states: [],
  metadata: {},
};

const $ = (id) => document.getElementById(id);

const fmtUsd = (v) =>
  new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: 'USD',
    maximumFractionDigits: 0,
  }).format(Number(v || 0));

const fmtPct = (v) => `${(Number(v || 0) * 100).toFixed(1)}%`;

async function api(path, opts = {}) {
  const res = await fetch(path, opts);
  if (!res.ok) {
    throw new Error(`API error ${res.status} for ${path}`);
  }
  return res.json();
}

function renderMetricCards(el, items) {
  el.innerHTML = '';
  items.forEach(({ label, value }) => {
    const div = document.createElement('div');
    div.className = 'metric';
    div.innerHTML = `<div class="label">${label}</div><div class="value">${value}</div>`;
    el.appendChild(div);
  });
}

function setupSliders() {
  const pairs = [
    ['medicareCut', 'medicareCutVal'],
    ['fedCut', 'fedCutVal'],
    ['stateCut', 'stateCutVal'],
  ];

  pairs.forEach(([inputId, outId]) => {
    const input = $(inputId);
    const out = $(outId);
    out.textContent = `${input.value}%`;
    input.addEventListener('input', () => {
      out.textContent = `${input.value}%`;
    });
  });
}

async function init() {
  setupSliders();

  const meta = await api('/api/v1/meta/years');
  appState.metadata = meta.metadata || {};
  const years = meta.years || [];

  const yearSelect = $('yearSelect');
  years.reverse().forEach((yr) => {
    const opt = document.createElement('option');
    opt.value = yr;
    opt.textContent = yr;
    yearSelect.appendChild(opt);
  });

  appState.year = Number(yearSelect.value || years[0]);
  $('sourceNote').textContent = `Selected FY: ${appState.year}. Medicare values are modeled estimates (see metadata).`;

  yearSelect.addEventListener('change', async (e) => {
    appState.year = Number(e.target.value);
    appState.selectedState = null;
    await refreshStates();
    clearStatePanel();
  });

  $('metricSelect').addEventListener('change', async (e) => {
    appState.metric = e.target.value;
    await refreshStates();
  });

  $('refreshBtn').addEventListener('click', refreshStates);
  $('ownershipFilter').addEventListener('change', loadFacilitiesForSelectedState);
  $('facilitySort').addEventListener('change', loadFacilitiesForSelectedState);
  $('groupByChain').addEventListener('change', loadFacilitiesForSelectedState);

  $('runScenarioBtn').addEventListener('click', runScenario);
  $('exportBtn').addEventListener('click', exportCsv);

  await refreshStates();
}

function clearStatePanel() {
  $('selectedStateLabel').textContent = '(none)';
  $('stateSummary').innerHTML = '';
  $('facilityTable').querySelector('tbody').innerHTML = '';
}

async function refreshStates() {
  const rows = await api(`/api/v1/states?fiscal_year=${appState.year}&metric=${appState.metric}`);
  appState.states = rows;
  renderStateTable(rows);
  renderMap(rows);
}

function renderMap(rows) {
  const z = rows.map((r) => Number(r[appState.metric] || 0));
  const data = [
    {
      type: 'choropleth',
      locationmode: 'USA-states',
      locations: rows.map((r) => r.state_code),
      z,
      text: rows.map((r) => `${r.state_code}<br>Medicaid: ${fmtUsd(r.medicaid_total)}<br>Medicare: ${fmtUsd(r.medicare_total)}`),
      hovertemplate: '%{text}<extra></extra>',
      colorscale: 'Blues',
      marker: { line: { color: 'white', width: 1 } },
      colorbar: { title: appState.metric },
    },
  ];

  const layout = {
    geo: {
      scope: 'usa',
      projection: { type: 'albers usa' },
      bgcolor: 'rgba(0,0,0,0)',
    },
    margin: { l: 0, r: 0, t: 0, b: 0 },
  };

  Plotly.react('stateMap', data, layout, { displayModeBar: false, responsive: true });

  const mapEl = $('stateMap');
  if (typeof mapEl.on === 'function' && !mapEl.dataset.boundClick) {
    mapEl.on('plotly_click', async (evt) => {
      const pt = evt.points?.[0];
      if (!pt) return;
      const code = pt.location;
      await selectState(code);
    });
    mapEl.dataset.boundClick = '1';
  }
}

function renderStateTable(rows) {
  const tbody = $('stateTable').querySelector('tbody');
  tbody.innerHTML = '';

  rows.forEach((r) => {
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td><button data-state="${r.state_code}" class="linkish">${r.state_code}</button></td>
      <td>${fmtUsd(r.medicaid_total)}</td>
      <td>${fmtUsd(r.medicare_total)}</td>
      <td>${fmtUsd(r.federal_medicaid_total)}</td>
      <td>${fmtUsd(r.state_medicaid_total)}</td>
      <td>${r.facility_count}</td>
    `;
    tbody.appendChild(tr);
  });

  tbody.querySelectorAll('button[data-state]').forEach((btn) => {
    btn.addEventListener('click', async () => {
      await selectState(btn.dataset.state);
    });
  });
}

async function selectState(stateCode) {
  appState.selectedState = stateCode;
  $('selectedStateLabel').textContent = stateCode;

  const summary = await api(`/api/v1/states/${stateCode}/summary?fiscal_year=${appState.year}`);

  renderMetricCards($('stateSummary'), [
    { label: 'Medicaid total', value: fmtUsd(summary.medicaid_total) },
    { label: 'Medicare total', value: fmtUsd(summary.medicare_total) },
    { label: 'Federal Medicaid est.', value: fmtUsd(summary.federal_medicaid_total) },
    { label: 'State Medicaid est.', value: fmtUsd(summary.state_medicaid_total) },
    { label: 'Public dependency', value: fmtPct(summary.public_dependency) },
    { label: 'Facility count', value: summary.facility_count },
  ]);

  await loadFacilitiesForSelectedState();
}

async function loadFacilitiesForSelectedState() {
  if (!appState.selectedState) return;

  const ownership = $('ownershipFilter').value;
  const sort = $('facilitySort').value;
  const groupByChain = $('groupByChain').checked;

  const params = new URLSearchParams({
    fiscal_year: appState.year,
    ownership,
    sort,
    descending: 'true',
    group_by_chain: String(groupByChain),
  });

  const rows = await api(`/api/v1/states/${appState.selectedState}/facilities?${params.toString()}`);

  const tbody = $('facilityTable').querySelector('tbody');
  tbody.innerHTML = '';

  rows.slice(0, 500).forEach((r) => {
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td>${r.facility_name}</td>
      <td>${r.ownership_group}</td>
      <td>${r.chain_name}</td>
      <td>${fmtUsd(r.total_revenue)}</td>
      <td>${fmtUsd(r.medicare_revenue)}</td>
      <td>${fmtUsd(r.medicaid_revenue)}</td>
      <td>${fmtUsd(r.federal_medicaid_revenue)}</td>
      <td>${fmtUsd(r.state_medicaid_revenue)}</td>
      <td>${fmtPct(r.public_dependency)}</td>
    `;
    tbody.appendChild(tr);
  });
}

async function runScenario() {
  const body = {
    fiscal_year: appState.year,
    medicare_cut_pct: Number($('medicareCut').value),
    federal_medicaid_cut_pct: Number($('fedCut').value),
    state_medicaid_cut_pct: Number($('stateCut').value),
    state_code: appState.selectedState || null,
  };

  const result = await api('/api/v1/scenarios/run', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });

  renderMetricCards($('scenarioSummary'), [
    { label: 'Baseline revenue', value: fmtUsd(result.baseline_total_revenue) },
    { label: 'Shocked revenue', value: fmtUsd(result.shocked_total_revenue) },
    { label: 'Revenue at risk', value: fmtUsd(result.revenue_at_risk_abs) },
    { label: 'Risk % of baseline', value: fmtPct(result.revenue_at_risk_pct) },
    { label: 'Scope', value: result.scope_state_code || 'National' },
  ]);

  const tbody = $('scenarioTable').querySelector('tbody');
  tbody.innerHTML = '';

  result.top_impacted_facilities.forEach((r) => {
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td>${r.facility_name}</td>
      <td>${r.state_code}</td>
      <td>${fmtUsd(r.baseline_total_revenue)}</td>
      <td>${fmtUsd(r.shocked_total_revenue)}</td>
      <td>${fmtUsd(r.revenue_at_risk_abs)}</td>
      <td>${fmtPct(r.revenue_at_risk_pct)}</td>
    `;
    tbody.appendChild(tr);
  });
}

function exportCsv() {
  if (!appState.selectedState) {
    alert('Select a state first to export facilities.');
    return;
  }

  const ownership = $('ownershipFilter').value;
  const params = new URLSearchParams({
    fiscal_year: appState.year,
    state_code: appState.selectedState,
    ownership,
  });

  window.open(`/api/v1/exports/facilities.csv?${params.toString()}`, '_blank');
}

init().catch((err) => {
  console.error(err);
  alert(`Initialization failed: ${err.message}`);
});
