const $ = (id) => document.getElementById(id);

const THEME_KEY = 'mm_theme';
const PRESET_KEY = 'mm_scenario_presets';
const APP_CONFIG = window.__APP_CONFIG__ || {};
const API_BASE_URL = String(APP_CONFIG.API_BASE_URL || '').replace(/\/+$/, '');

const appState = {
  year: null,
  metric: 'medicaid_total',
  payerScope: 'public_only',
  taxonomyView: 'funding_source',
  marketShareBasis: 'covered_lives',
  view: 'national',
  selectedState: null,
  selectedFacilityId: null,
  selectedFacilityMenuRow: null,
  metadata: {},
  states: [],
  stateSummary: null,
  facilities: [],
  facilitiesUngrouped: [],
  selectedTrendChain: null,
  trendMode: 'levels',
  trendRows: {
    national: [],
    state: [],
    chain: [],
    facility: [],
    facilityTitle: '',
  },
  filterHistory: [],
  selectedTableIndex: -1,
  cache: new Map(),
  loading: false,
  theme: localStorage.getItem(THEME_KEY) || 'dark',
  portfolioId: 'default',
  scenarioInsurers: [],
  scenarioInsurerOverrides: {},
  filters: {
    search: '',
    ownership: 'all',
    groupByChain: false,
    sort: 'medicaid_revenue',
    density: 'default',
    analysisView: 'chain',
  },
};

const OWNERSHIP_BADGE_CLASS = {
  for_profit: 'fp',
  not_for_profit: 'nfp',
  government: 'gov',
  unknown: 'unknown',
  mixed: 'unknown',
};

const OWNERSHIP_SHORT_LABEL = {
  for_profit: 'FP',
  not_for_profit: 'NFP',
  government: 'GOV',
  unknown: 'UNK',
  mixed: 'MIX',
};

const CONFIDENCE_BADGE_CLASS = {
  A: 'conf-a',
  B: 'conf-b',
  C: 'conf-c',
  U: 'conf-u',
};

const STATE_NAME = {
  AL: 'Alabama', AK: 'Alaska', AZ: 'Arizona', AR: 'Arkansas', CA: 'California', CO: 'Colorado', CT: 'Connecticut',
  DE: 'Delaware', DC: 'District of Columbia', FL: 'Florida', GA: 'Georgia', HI: 'Hawaii', ID: 'Idaho', IL: 'Illinois',
  IN: 'Indiana', IA: 'Iowa', KS: 'Kansas', KY: 'Kentucky', LA: 'Louisiana', ME: 'Maine', MD: 'Maryland', MA: 'Massachusetts',
  MI: 'Michigan', MN: 'Minnesota', MS: 'Mississippi', MO: 'Missouri', MT: 'Montana', NE: 'Nebraska', NV: 'Nevada',
  NH: 'New Hampshire', NJ: 'New Jersey', NM: 'New Mexico', NY: 'New York', NC: 'North Carolina', ND: 'North Dakota',
  OH: 'Ohio', OK: 'Oklahoma', OR: 'Oregon', PA: 'Pennsylvania', RI: 'Rhode Island', SC: 'South Carolina', SD: 'South Dakota',
  TN: 'Tennessee', TX: 'Texas', UT: 'Utah', VT: 'Vermont', VA: 'Virginia', WA: 'Washington', WV: 'West Virginia',
  WI: 'Wisconsin', WY: 'Wyoming',
};

const fmtUsdFull = (value) =>
  new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 0 }).format(Number(value || 0));

const fmtPct = (value, digits = 1) => `${(Number(value || 0) * 100).toFixed(digits)}%`;
const escapeHtml = (v) =>
  String(v ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/\"/g, '&quot;')
    .replace(/'/g, '&#39;');

function confidenceBadge(tier) {
  const t = String(tier || 'U').toUpperCase();
  const cls = CONFIDENCE_BADGE_CLASS[t] || 'conf-u';
  return `<span class="badge ${cls}" title="Private-insurer data confidence tier">${escapeHtml(t)}</span>`;
}

function normalizeConfidenceTier(value) {
  const raw = String(value || '').trim().toUpperCase();
  if (['A', 'B', 'C', 'U'].includes(raw)) return raw;
  if (raw.startsWith('HIGH')) return 'A';
  if (raw.startsWith('MED')) return 'B';
  if (raw.startsWith('LOW')) return 'C';
  return 'U';
}

function formatSmartCurrency(value) {
  const n = Number(value || 0);
  const abs = Math.abs(n);
  const trimDecimals = (raw) => (raw.includes('.') ? raw.replace(/\.?0+$/, '') : raw);
  if (abs >= 1e9) return `$${trimDecimals((n / 1e9).toFixed(abs >= 1e11 ? 0 : abs >= 1e10 ? 1 : 2))}B`;
  if (abs >= 1e6) return `$${trimDecimals((n / 1e6).toFixed(abs >= 1e8 ? 0 : abs >= 1e7 ? 1 : 2))}M`;
  if (abs >= 1e3) return `$${trimDecimals((n / 1e3).toFixed(abs >= 1e5 ? 0 : abs >= 1e4 ? 1 : 2))}K`;
  return fmtUsdFull(n);
}

function niceTickStep(span, targetTicks = 4) {
  if (!Number.isFinite(span) || span <= 0) return 1;
  const rough = span / Math.max(1, targetTicks);
  const magnitude = 10 ** Math.floor(Math.log10(rough));
  const multipliers = [1, 2, 2.5, 5, 10];
  const m = multipliers.find((v) => rough <= v * magnitude) || 10;
  return m * magnitude;
}

function buildCurrencyAxisTicks(values, { includeZero = true, targetTicks = 4 } = {}) {
  const nums = values
    .map((v) => Number(v))
    .filter((v) => Number.isFinite(v));

  if (!nums.length) return {};

  let min = Math.min(...nums);
  let max = Math.max(...nums);
  if (includeZero) {
    min = Math.min(min, 0);
    max = Math.max(max, 0);
  }

  if (Math.abs(max - min) < 1e-9) {
    const single = min || 0;
    return {
      tickmode: 'array',
      tickvals: [single],
      ticktext: [formatSmartCurrency(single)],
    };
  }

  const step = niceTickStep(max - min, targetTicks);
  const start = Math.floor(min / step) * step;
  const end = Math.ceil(max / step) * step;
  const tickvals = [];
  const maxTicks = 12;
  for (let v = start; v <= end + (step * 0.5); v += step) {
    tickvals.push(Number(v.toFixed(8)));
    if (tickvals.length >= maxTicks) break;
  }

  return {
    tickmode: 'array',
    tickvals,
    ticktext: tickvals.map((v) => formatSmartCurrency(v)),
  };
}

function buildPaddedCurrencyBarAxis(values, { includeZero = true, targetTicks = 5, padRatio = 0.18 } = {}) {
  const nums = values
    .map((v) => Number(v))
    .filter((v) => Number.isFinite(v));

  if (!nums.length) return buildCurrencyAxisTicks([0], { includeZero: true, targetTicks });

  let min = Math.min(...nums);
  let max = Math.max(...nums);
  if (includeZero) {
    min = Math.min(min, 0);
    max = Math.max(max, 0);
  }

  const span = Math.max(Math.abs(max - min), Math.abs(max), Math.abs(min), 1);
  const pad = span * Math.max(0, padRatio);

  const paddedMin = min < 0 ? min - pad : min;
  const paddedMax = max > 0 ? max + pad : max;

  return {
    ...buildCurrencyAxisTicks([paddedMin, paddedMax], { includeZero: false, targetTicks }),
    range: [paddedMin, paddedMax],
  };
}

function percentile(values, p) {
  if (!values.length) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  const idx = (sorted.length - 1) * p;
  const low = Math.floor(idx);
  const high = Math.ceil(idx);
  if (low === high) return sorted[low];
  return sorted[low] + (sorted[high] - sorted[low]) * (idx - low);
}

function setTheme(theme) {
  appState.theme = theme;
  document.documentElement.setAttribute('data-theme', theme);
  localStorage.setItem(THEME_KEY, theme);
}

function status(msg, type = 'info', retryFn = null) {
  const banner = $('statusBanner');
  if (!msg) {
    banner.classList.add('hidden');
    banner.innerHTML = '';
    return;
  }

  let html = `<span>${msg}</span>`;
  if (retryFn) {
    html += ` <button id="retryBannerBtn" class="btn ghost">Retry</button>`;
  }
  banner.innerHTML = html;
  banner.classList.remove('hidden');
  banner.style.borderColor = type === 'error' ? 'var(--negative)' : 'var(--warning)';
  banner.style.color = type === 'error' ? 'var(--negative)' : 'var(--warning)';

  if (retryFn) {
    $('retryBannerBtn').onclick = retryFn;
  }
}

function setLoading(isLoading) {
  appState.loading = isLoading;
  document.body.style.cursor = isLoading ? 'progress' : 'default';
}

function apiUrl(path) {
  const normalized = String(path || '');
  if (/^https?:\/\//i.test(normalized)) return normalized;
  if (!API_BASE_URL) return normalized;
  if (!normalized.startsWith('/')) return `${API_BASE_URL}/${normalized}`;
  return `${API_BASE_URL}${normalized}`;
}

async function api(path, options = {}, retryFn = null) {
  const key = `${options.method || 'GET'}::${path}::${options.body || ''}`;
  try {
    const res = await fetch(apiUrl(path), options);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();
    appState.cache.set(key, data);
    status('');
    return data;
  } catch (err) {
    if (appState.cache.has(key)) {
      status(`Using cached data after API error (${err.message}).`, 'error', retryFn);
      return appState.cache.get(key);
    }
    status(`Unable to load data (${err.message}).`, 'error', retryFn);
    throw err;
  }
}

function renderKpiCards(el, items) {
  el.innerHTML = '';
  items.forEach((item) => {
    const card = document.createElement('article');
    card.className = 'kpi-card';
    card.innerHTML = `
      <div class="kpi-label">${item.label}</div>
      <div class="kpi-value">${item.value}</div>
      ${item.sub ? `<div class="kpi-sub">${item.sub}</div>` : ''}
    `;
    el.appendChild(card);
  });
}

function quantileBins(values, classes = 7) {
  const bins = [];
  for (let i = 0; i <= classes; i += 1) {
    bins.push(percentile(values, i / classes));
  }
  return bins;
}

function bucketize(value, bins) {
  for (let i = 1; i < bins.length; i += 1) {
    if (value <= bins[i]) return i - 1;
  }
  return bins.length - 2;
}

function parseUrlState() {
  const params = new URLSearchParams(window.location.search);
  const view = params.get('view');
  const state = params.get('state');
  const ownership = params.get('ownership');
  const sort = params.get('sort');
  const search = params.get('search');
  const group = params.get('group');
  const density = params.get('density');
  const metric = params.get('metric');
  const trendMode = params.get('trend_mode');
  const payerScope = params.get('payer_scope');
  const taxonomyView = params.get('taxonomy_view');
  const marketShareBasis = params.get('market_share_basis');

  if (view === 'scenarios' || view === 'national') appState.view = view;
  if (state) appState.selectedState = state.toUpperCase();
  if (ownership) appState.filters.ownership = ownership;
  if (sort) appState.filters.sort = sort;
  if (search) appState.filters.search = search;
  if (group) appState.filters.groupByChain = group === 'true';
  if (density) appState.filters.density = density;
  if (metric) appState.metric = metric;
  if (trendMode && ['levels', 'yoy_abs', 'yoy_pct'].includes(trendMode)) appState.trendMode = trendMode;
  if (payerScope && ['public_only', 'comprehensive'].includes(payerScope)) appState.payerScope = payerScope;
  if (taxonomyView && ['funding_source', 'carrier_ownership'].includes(taxonomyView)) appState.taxonomyView = taxonomyView;
  if (marketShareBasis && ['covered_lives', 'premium', 'claims'].includes(marketShareBasis)) appState.marketShareBasis = marketShareBasis;
}

function syncUrl() {
  const params = new URLSearchParams();
  params.set('view', appState.view);
  params.set('metric', appState.metric);
  params.set('payer_scope', appState.payerScope);
  params.set('taxonomy_view', appState.taxonomyView);
  params.set('market_share_basis', appState.marketShareBasis);
  if (appState.selectedState) params.set('state', appState.selectedState);
  if (appState.filters.ownership !== 'all') params.set('ownership', appState.filters.ownership);
  if (appState.filters.sort !== 'medicaid_revenue') params.set('sort', appState.filters.sort);
  if (appState.filters.search) params.set('search', appState.filters.search);
  if (appState.filters.groupByChain) params.set('group', 'true');
  if (appState.filters.density !== 'default') params.set('density', appState.filters.density);
  if (appState.trendMode !== 'levels') params.set('trend_mode', appState.trendMode);
  const next = `${window.location.pathname}?${params.toString()}`;
  history.replaceState({}, '', next);
}

function pushFilterHistory() {
  appState.filterHistory.push(JSON.stringify(appState.filters));
  if (appState.filterHistory.length > 10) appState.filterHistory.shift();
}

function applyFiltersToControls() {
  $('metricSelect').value = appState.metric;
  $('payerScopeSelect').value = appState.payerScope;
  $('taxonomyViewSelect').value = appState.taxonomyView;
  $('marketShareBasisSelect').value = appState.marketShareBasis;
  $('ownershipFilter').value = appState.filters.ownership;
  $('facilitySort').value = appState.filters.sort;
  $('facilitySearch').value = appState.filters.search;
  $('groupByChain').checked = appState.filters.groupByChain;
  $('densityMode').value = appState.filters.density;
  $('trendModeSelect').value = appState.trendMode;
  $('facilityTableWrap').classList.remove('compact', 'default', 'comfortable');
  $('facilityTableWrap').classList.add(appState.filters.density);
  syncMetricOptions();
}

function withPayerScope(params = new URLSearchParams()) {
  params.set('payer_scope', appState.payerScope);
  params.set('taxonomy_view', appState.taxonomyView);
  return params;
}

function syncMetricOptions() {
  const metricSelect = $('metricSelect');
  const privateMetrics = ['private_total', 'private_dependency', 'comprehensive_total'];
  [...metricSelect.options].forEach((option) => {
    if (privateMetrics.includes(option.value)) {
      option.disabled = appState.payerScope !== 'comprehensive';
    }
  });

  if (appState.payerScope !== 'comprehensive' && privateMetrics.includes(appState.metric)) {
    appState.metric = 'public_total';
  }
  metricSelect.value = appState.metric;
}

function setView(view) {
  appState.view = view;
  $('viewNational').classList.toggle('hidden', view !== 'national');
  $('viewScenarios').classList.toggle('hidden', view !== 'scenarios');
  $('tabNational').classList.toggle('active', view === 'national');
  $('tabScenarios').classList.toggle('active', view === 'scenarios');
  updateBreadcrumb();
  syncUrl();
}

function updateBreadcrumb(extra = null) {
  const crumbs = ['National Overview'];
  if (appState.selectedState) crumbs.push(`${STATE_NAME[appState.selectedState] || appState.selectedState} (${appState.selectedState})`);
  if (extra) crumbs.push(extra);
  if (appState.view === 'scenarios') crumbs.length = 0;
  if (appState.view === 'scenarios') crumbs.push('Scenarios');
  $('breadcrumb').textContent = crumbs.join(' > ');
}

function renderTopRiskTable(rows) {
  const top = [...rows].sort((a, b) => b.public_dependency - a.public_dependency).slice(0, 5);
  const tbody = $('topRiskTable').querySelector('tbody');
  tbody.innerHTML = top
    .map(
      (r) => `
      <tr>
        <td><button class="linkish" data-state="${r.state_code}">${r.state_code}</button></td>
        <td>${fmtPct(r.public_dependency)}</td>
        <td>${r.facility_count}</td>
        <td>${formatSmartCurrency(r.public_total)}</td>
      </tr>
    `,
    )
    .join('');

  tbody.querySelectorAll('[data-state]').forEach((btn) => {
    btn.addEventListener('click', () => selectState(btn.dataset.state));
  });
}

function renderNationalKpis(rows) {
  const medicaid = rows.reduce((s, r) => s + Number(r.medicaid_total || 0), 0);
  const medicare = rows.reduce((s, r) => s + Number(r.medicare_total || 0), 0);
  const publicTotal = medicaid + medicare;
  const privateTotal = rows.reduce((s, r) => s + Number(r.private_total || 0), 0);
  const comprehensiveTotal = publicTotal + privateTotal;
  const totalRevenue = rows.reduce((s, r) => s + Number(r.total_revenue || 0), 0);
  const avgDep = totalRevenue > 0 ? publicTotal / totalRevenue : 0;
  const privateDep = totalRevenue > 0 ? privateTotal / totalRevenue : 0;
  const comprehensiveDep = totalRevenue > 0 ? comprehensiveTotal / totalRevenue : 0;
  const facilityCount = rows.reduce((s, r) => s + Number(r.facility_count || 0), 0);

  const sortedPublic = [...rows].sort((a, b) => b.public_total - a.public_total);
  const top10Share = sortedPublic.slice(0, 10).reduce((s, r) => s + Number(r.public_total || 0), 0) / Math.max(publicTotal, 1);

  const fmapVals = rows
    .map((r) => Number(r.federal_medicaid_total) / Math.max(Number(r.medicaid_total || 0), 1))
    .filter((n) => Number.isFinite(n) && n >= 0 && n <= 1);
  const fmapMin = fmapVals.length ? Math.min(...fmapVals) : 0;
  const fmapMax = fmapVals.length ? Math.max(...fmapVals) : 0;

  const highRisk = [...rows].sort((a, b) => Number(b.public_dependency || 0) - Number(a.public_dependency || 0))[0];

  const items = [
    { label: 'Total Medicaid', value: formatSmartCurrency(medicaid), sub: 'CMS cost report net revenue' },
    { label: 'Total Medicare', value: formatSmartCurrency(medicare), sub: 'Modeled estimate' },
    { label: 'Total Public', value: formatSmartCurrency(publicTotal), sub: 'Medicare + Medicaid' },
    { label: 'Avg Public Dependency', value: fmtPct(avgDep), sub: 'Weighted by total revenue' },
    { label: 'Total Facilities', value: new Intl.NumberFormat('en-US').format(facilityCount), sub: 'Medicare-certified hospitals' },
    { label: 'Top-10 Concentration', value: fmtPct(top10Share), sub: 'Share of public dollars' },
    { label: 'FMAP Range', value: `${fmtPct(fmapMin)} — ${fmtPct(fmapMax)}`, sub: 'Federal Medicaid share' },
    {
      label: 'Highest Risk State',
      value: highRisk ? `${highRisk.state_code} (${fmtPct(highRisk.public_dependency)})` : 'N/A',
      sub: highRisk ? formatSmartCurrency(highRisk.public_total) : '',
    },
  ];

  if (appState.payerScope === 'comprehensive') {
    items.splice(3, 0, { label: 'Total Private', value: formatSmartCurrency(privateTotal), sub: 'Modeled private insurance' });
    items.splice(4, 0, { label: 'Comprehensive Total', value: formatSmartCurrency(comprehensiveTotal), sub: 'Public + private' });
    items.splice(5, 0, { label: 'Private Dependency', value: fmtPct(privateDep), sub: `${appState.taxonomyView === 'carrier_ownership' ? 'Carrier ownership view' : 'Funding source view'}` });
    items.splice(6, 0, { label: 'Comprehensive Dependency', value: fmtPct(comprehensiveDep), sub: 'Comprehensive / total revenue' });
  }

  renderKpiCards($('nationalKpis'), items);
}

function renderStateTable(rows) {
  const sorted = [...rows].sort((a, b) => Number(b[appState.metric] || 0) - Number(a[appState.metric] || 0));
  const tbody = $('stateTable').querySelector('tbody');
  tbody.innerHTML = sorted
    .map(
      (r, idx) => `
      <tr>
        <td>${idx + 1}</td>
        <td><button class="linkish" data-state="${r.state_code}">${r.state_code}</button></td>
        <td>${formatSmartCurrency(r.medicaid_total)}</td>
        <td>${formatSmartCurrency(r.medicare_total)}</td>
        <td>${formatSmartCurrency(r.federal_medicaid_total)}</td>
        <td>${formatSmartCurrency(r.state_medicaid_total)}</td>
        <td>${appState.payerScope === 'comprehensive' ? formatSmartCurrency(r.private_total) : '—'}</td>
        <td>${fmtPct(r.public_dependency)}</td>
        <td>${appState.payerScope === 'comprehensive' ? fmtPct(r.private_dependency) : '—'}</td>
        <td>${r.facility_count}</td>
      </tr>
    `,
    )
    .join('');

  tbody.querySelectorAll('[data-state]').forEach((btn) => {
    btn.addEventListener('click', () => selectState(btn.dataset.state));
  });
}

function renderStateMap(rows) {
  const values = rows.map((r) => Number(r[appState.metric] || 0));
  const bins = quantileBins(values, 7);
  const z = values.map((v) => bucketize(v, bins));

  const rankMap = [...rows]
    .sort((a, b) => Number(b[appState.metric] || 0) - Number(a[appState.metric] || 0))
    .reduce((acc, row, idx) => {
      acc[row.state_code] = idx + 1;
      return acc;
    }, {});

  const colors = ['#1E3A5F', '#1E4D7A', '#2563EB', '#3B82F6', '#60A5FA', '#93C5FD', '#BFDBFE'];

  const data = [
    {
      type: 'choropleth',
      locationmode: 'USA-states',
      locations: rows.map((r) => r.state_code),
      z,
      zmin: 0,
      zmax: 6,
      colorscale: colors.map((c, i) => [i / 6, c]),
      marker: {
        line: {
          color: rows.map((r) => (r.state_code === appState.selectedState ? '#FFFFFF' : '#7A8599')),
          width: rows.map((r) => (r.state_code === appState.selectedState ? 2.2 : 0.8)),
        },
      },
      hovertemplate:
        '%{location}<br>' +
        `${appState.metric}: %{customdata[0]}<br>` +
        'National Rank: %{customdata[1]}<br>' +
        'Facilities: %{customdata[2]}<extra></extra>',
      customdata: rows.map((r) => [formatSmartCurrency(r[appState.metric]), rankMap[r.state_code], r.facility_count]),
      showscale: false,
      opacity: rows.map((r) => (appState.selectedState && r.state_code !== appState.selectedState ? 0.4 : 1)),
    },
  ];

  const layout = {
    geo: {
      scope: 'usa',
      projection: { type: 'albers usa' },
      bgcolor: 'rgba(0,0,0,0)',
      showland: true,
      landcolor: 'rgba(0,0,0,0)',
      showlakes: false,
      showframe: false,
    },
    paper_bgcolor: 'rgba(0,0,0,0)',
    plot_bgcolor: 'rgba(0,0,0,0)',
    margin: { l: 0, r: 0, t: 0, b: 0 },
  };

  Plotly.react('stateMap', data, layout, { displayModeBar: false, responsive: true, staticPlot: false });

  const mapEl = $('stateMap');
  if (typeof mapEl.on === 'function' && !mapEl.dataset.boundMapClick) {
    mapEl.on('plotly_click', (evt) => {
      const loc = evt.points?.[0]?.location;
      if (loc) selectState(loc);
    });
    mapEl.dataset.boundMapClick = '1';
  }
}

function normalizeTrendRows(rows) {
  return [...rows]
    .map((r) => {
      const medicaid = Number(r.medicaid_total ?? r.medicaid_revenue ?? 0);
      const medicare = Number(r.medicare_total ?? r.medicare_revenue ?? 0);
      const totalRevenue = Number(r.total_revenue ?? 0);
      const publicTotal = Number(r.public_total ?? (medicaid + medicare));
      const dependency = Number(
        r.public_dependency ?? (totalRevenue > 0 ? publicTotal / totalRevenue : 0),
      );
      return {
        fiscal_year: Number(r.fiscal_year),
        medicaid_total: medicaid,
        medicare_total: medicare,
        public_total: publicTotal,
        public_dependency: dependency,
      };
    })
    .filter((r) => Number.isFinite(r.fiscal_year))
    .sort((a, b) => a.fiscal_year - b.fiscal_year);
}

function deriveTrendSeries(trend, mode) {
  if (mode === 'levels') {
    return {
      x: trend.map((r) => r.fiscal_year),
      medicaid: trend.map((r) => r.medicaid_total),
      medicare: trend.map((r) => r.medicare_total),
      publicTotal: trend.map((r) => r.public_total),
      dependency: trend.map((r) => Number(r.public_dependency || 0) * 100),
      amountAxisTitle: '$',
      amountTickFormat: null,
      depAxisTitle: '%',
      depTickSuffix: '%',
      useCurrencyHover: true,
      hoverDep: '%{y:.1f}%',
      modeLabel: 'Levels',
    };
  }

  const x = [];
  const medicaid = [];
  const medicare = [];
  const publicTotal = [];
  const dependency = [];

  for (let i = 1; i < trend.length; i += 1) {
    const prev = trend[i - 1];
    const curr = trend[i];
    x.push(curr.fiscal_year);

    if (mode === 'yoy_abs') {
      medicaid.push(curr.medicaid_total - prev.medicaid_total);
      medicare.push(curr.medicare_total - prev.medicare_total);
      publicTotal.push(curr.public_total - prev.public_total);
      dependency.push((curr.public_dependency - prev.public_dependency) * 100);
    } else {
      const pct = (c, p) => (Math.abs(p) > 1e-9 ? ((c / p) - 1) * 100 : null);
      medicaid.push(pct(curr.medicaid_total, prev.medicaid_total));
      medicare.push(pct(curr.medicare_total, prev.medicare_total));
      publicTotal.push(pct(curr.public_total, prev.public_total));
      dependency.push(pct(curr.public_dependency, prev.public_dependency));
    }
  }

  if (mode === 'yoy_abs') {
    return {
      x,
      medicaid,
      medicare,
      publicTotal,
      dependency,
      amountAxisTitle: '$ Δ',
      amountTickFormat: null,
      depAxisTitle: 'pp Δ',
      depTickSuffix: 'pp',
      useCurrencyHover: true,
      hoverDep: '%{y:.2f}pp',
      modeLabel: 'YoY Δ ($ / pp)',
    };
  }

  return {
    x,
    medicaid,
    medicare,
    publicTotal,
    dependency,
    amountAxisTitle: 'YoY %',
    amountTickFormat: '.1f',
    depAxisTitle: 'YoY %',
    depTickSuffix: '%',
    useCurrencyHover: false,
    hoverAmount: '%{y:.2f}%',
    hoverDep: '%{y:.2f}%',
    modeLabel: 'YoY Δ (%)',
  };
}

function renderTrendChart(elId, rows, title = '') {
  const trend = normalizeTrendRows(rows);
  if (!trend.length || (appState.trendMode !== 'levels' && trend.length < 2)) {
    $(elId).innerHTML = '<div class="chip">No historical data available.</div>';
    return;
  }
  const series = deriveTrendSeries(trend, appState.trendMode);
  const chartTitle = title ? `${title} · ${series.modeLabel}` : series.modeLabel;
  const amountAxisConfig = series.useCurrencyHover
    ? buildCurrencyAxisTicks([...series.medicaid, ...series.medicare, ...series.publicTotal], { includeZero: true, targetTicks: 5 })
    : { tickformat: series.amountTickFormat, ticksuffix: appState.trendMode === 'yoy_pct' ? '%' : '' };

  Plotly.react(
    elId,
    [
      {
        type: 'scatter',
        mode: 'lines+markers',
        name: 'Medicaid',
        x: series.x,
        y: series.medicaid,
        customdata: series.medicaid.map((v) => formatSmartCurrency(v)),
        line: { color: '#2dd881', width: 2 },
        marker: { size: 6 },
        hovertemplate: series.useCurrencyHover
          ? 'FY %{x}<br>Medicaid: %{customdata}<extra></extra>'
          : `FY %{x}<br>Medicaid: ${series.hoverAmount}<extra></extra>`,
      },
      {
        type: 'scatter',
        mode: 'lines+markers',
        name: 'Medicare',
        x: series.x,
        y: series.medicare,
        customdata: series.medicare.map((v) => formatSmartCurrency(v)),
        line: { color: '#3b82f6', width: 2 },
        marker: { size: 6 },
        hovertemplate: series.useCurrencyHover
          ? 'FY %{x}<br>Medicare: %{customdata}<extra></extra>'
          : `FY %{x}<br>Medicare: ${series.hoverAmount}<extra></extra>`,
      },
      {
        type: 'scatter',
        mode: 'lines+markers',
        name: 'Public total',
        x: series.x,
        y: series.publicTotal,
        customdata: series.publicTotal.map((v) => formatSmartCurrency(v)),
        line: { color: '#8b5cf6', width: 2.5 },
        marker: { size: 7 },
        hovertemplate: series.useCurrencyHover
          ? 'FY %{x}<br>Public total: %{customdata}<extra></extra>'
          : `FY %{x}<br>Public total: ${series.hoverAmount}<extra></extra>`,
      },
      {
        type: 'scatter',
        mode: 'lines+markers',
        name: 'Public dependency',
        x: series.x,
        y: series.dependency,
        yaxis: 'y2',
        line: { color: '#f59e0b', width: 2, dash: 'dot' },
        marker: { size: 5 },
        hovertemplate: `FY %{x}<br>Public dependency: ${series.hoverDep}<extra></extra>`,
      },
    ],
    {
      margin: { l: 45, r: 45, t: 24, b: 30 },
      title: { text: chartTitle, x: 0, xanchor: 'left', font: { size: 12, color: '#8b95a8' } },
      paper_bgcolor: 'rgba(0,0,0,0)',
      plot_bgcolor: 'rgba(0,0,0,0)',
      xaxis: { tickmode: 'array', tickvals: series.x, gridcolor: 'rgba(122,133,153,0.2)' },
      yaxis: {
        title: series.amountAxisTitle,
        ...amountAxisConfig,
        gridcolor: 'rgba(122,133,153,0.2)',
      },
      yaxis2: {
        title: series.depAxisTitle,
        overlaying: 'y',
        side: 'right',
        ticksuffix: series.depTickSuffix,
      },
      legend: { orientation: 'h', y: -0.25 },
    },
    { displayModeBar: false, responsive: true },
  );
}

function rerenderAllTrendCharts() {
  renderTrendChart('nationalTrendChart', appState.trendRows.national, 'National trajectory');
  if (appState.selectedState) {
    renderTrendChart('stateTrendChart', appState.trendRows.state, `${appState.selectedState} historical trajectory`);
    if (appState.selectedTrendChain) {
      renderTrendChart('chainTrendChart', appState.trendRows.chain, `${appState.selectedTrendChain} (within ${appState.selectedState})`);
    }
  }
  if (!$('facilityDetailPanel').classList.contains('hidden') && appState.trendRows.facility.length) {
    renderTrendChart('detailTrendChart', appState.trendRows.facility, appState.trendRows.facilityTitle || 'Facility trajectory');
  }
}

async function refreshNational() {
  setLoading(true);
  try {
    const stateParams = withPayerScope(new URLSearchParams({
      fiscal_year: appState.year,
      metric: appState.metric,
    }));
    const rows = await api(`/api/v1/states?${stateParams.toString()}`, {}, refreshNational);
    appState.states = rows;
    renderNationalKpis(rows);
    renderTopRiskTable(rows);
    renderStateTable(rows);
    renderStateMap(rows);
    const trendParams = withPayerScope(new URLSearchParams());
    const nationalTrend = await api(`/api/v1/trends/national?${trendParams.toString()}`, {}, refreshNational);
    appState.trendRows.national = nationalTrend;
    renderTrendChart('nationalTrendChart', nationalTrend, 'National trajectory');
    await populateScenarioScope(rows);
    if (appState.selectedState) await selectState(appState.selectedState, false);
  } finally {
    setLoading(false);
  }
}

function filterFacilitiesBySearch(rows) {
  const term = appState.filters.search.trim().toLowerCase();
  if (!term) return rows;
  return rows.filter((r) => {
    const facility = String(r.facility_name || '').toLowerCase();
    const chain = String(r.chain_name || '').toLowerCase();
    return facility.includes(term) || chain.includes(term);
  });
}

function renderFilterChips() {
  const chips = [];
  if (appState.filters.search) chips.push(`Search: ${appState.filters.search}`);
  if (appState.filters.ownership !== 'all') chips.push(`Ownership: ${appState.filters.ownership}`);
  if (appState.filters.groupByChain) chips.push('Grouped by chain');
  if (appState.filters.sort !== 'medicaid_revenue') chips.push(`Sort: ${appState.filters.sort}`);
  if (appState.filters.density !== 'default') chips.push(`Density: ${appState.filters.density}`);
  $('activeFilterChips').innerHTML = chips.map((chip) => `<span class="chip">${chip}</span>`).join('');
}

function closeFacilityContextMenu() {
  const menu = $('facilityContextMenu');
  menu.classList.add('hidden');
  appState.selectedFacilityMenuRow = null;
}

function openFacilityContextMenu(anchorEl, row) {
  appState.selectedFacilityMenuRow = row;
  const menu = $('facilityContextMenu');
  const rect = anchorEl.getBoundingClientRect();
  menu.style.top = `${Math.min(window.innerHeight - 180, rect.bottom + 6)}px`;
  menu.style.left = `${Math.min(window.innerWidth - 250, rect.left)}px`;
  menu.classList.remove('hidden');

  const hasIssuerLink = row.emma_mapping_status === 'mapped' && Boolean(row.emma_issuer_url);
  const canLookupFallback = !String(row.facility_id || '').startsWith('CHAIN::');
  $('ctxOpenEmma').disabled = !(hasIssuerLink || canLookupFallback);
  $('ctxOpenEmma').textContent = hasIssuerLink
    ? 'Open EMMA Issuer Profile'
    : 'Find EMMA Link (CUSIP/Issue)';
  $('ctxViewOwned').disabled = String(row.facility_id || '').startsWith('CHAIN::');
  $('ctxViewOwned').textContent = 'View Owned Securities';
}

async function runFacilityMenuAction(action) {
  const row = appState.selectedFacilityMenuRow;
  if (!row) return;
  closeFacilityContextMenu();

  if (action === 'detail') {
    openFacilityDetail(row.facility_id);
    return;
  }

  if (action === 'emma') {
    if (row.emma_mapping_status === 'mapped' && row.emma_issuer_url) {
      window.open(row.emma_issuer_url, '_blank', 'noopener');
    } else {
      try {
        const mapping = await api(
          `/api/v1/facilities/${encodeURIComponent(row.facility_id)}/emma-link?fiscal_year=${appState.year}&include_fallback=true&portfolio_id=${encodeURIComponent(appState.portfolioId)}`,
          {},
          () => runFacilityMenuAction(action),
        );
        if (mapping?.emma_resolved_url) {
          window.open(mapping.emma_resolved_url, '_blank', 'noopener');
        } else {
          status('No EMMA issuer or active CUSIP fallback was found for this facility.', 'info');
        }
      } catch (err) {
        status('Unable to resolve EMMA fallback link right now.', 'warn');
      }
    }
    return;
  }

  if (action === 'owned') {
    openFacilityDetail(row.facility_id, { focusOwned: true });
  }
}

function renderFacilityTable(rows) {
  const tbody = $('facilityTable').querySelector('tbody');
  appState.selectedTableIndex = -1;

  if (!rows.length) {
    tbody.innerHTML = `<tr><td colspan="10">No facilities match current filters.</td></tr>`;
    return;
  }

  const medicaidDeps = rows.map((r) => Number(r.medicaid_dependency || 0));
  const dep90 = percentile(medicaidDeps, 0.9);

  tbody.innerHTML = rows
    .map((r, idx) => {
      const own = r.ownership_group || 'unknown';
      const ownClass = OWNERSHIP_BADGE_CLASS[own] || 'unknown';
      const ownLabel = OWNERSHIP_SHORT_LABEL[own] || String(own).toUpperCase();
      const publicRiskClass = Number(r.public_dependency || 0) > 0.7 ? 'public-risk-high' : '';
      const topDecileClass = Number(r.medicaid_dependency || 0) >= dep90 ? 'dep-top-decile' : '';
      const chainConfidenceClass = String(r.chain_confidence || '').toLowerCase().includes('low') ? 'chain-confidence-low' : '';
      const btnHtml = String(r.facility_id).startsWith('CHAIN::')
        ? `${escapeHtml(r.facility_name)}`
        : `<button class="facility-name-btn" data-facility-menu="${idx}">${escapeHtml(r.facility_name)}</button>`;

      return `
      <tr data-row-index="${idx}">
        <td class="pinned-left">${btnHtml}</td>
        <td><span class="badge ${ownClass}">${ownLabel}</span></td>
        <td class="${chainConfidenceClass}">${r.chain_name || '—'}</td>
        <td>${formatSmartCurrency(r.total_revenue)}</td>
        <td>${formatSmartCurrency(r.medicare_revenue)}</td>
        <td class="${topDecileClass}">${formatSmartCurrency(r.medicaid_revenue)}</td>
        <td>${formatSmartCurrency(r.federal_medicaid_revenue)}</td>
        <td>${formatSmartCurrency(r.state_medicaid_revenue)}</td>
        <td>${appState.payerScope === 'comprehensive' ? formatSmartCurrency(r.private_revenue || 0) : '—'}</td>
        <td class="pinned-right ${publicRiskClass}">${fmtPct(r.public_dependency)}</td>
      </tr>
    `;
    })
    .join('');

  tbody.querySelectorAll('[data-facility-menu]').forEach((btn) => {
    const rowIdx = Number(btn.dataset.facilityMenu);
    btn.addEventListener('click', (evt) => {
      evt.stopPropagation();
      const row = appState.tableRows?.[rowIdx];
      if (!row) return;
      openFacilityContextMenu(evt.currentTarget, row);
    });
    btn.addEventListener('keydown', (evt) => {
      if (evt.key === 'Enter' || evt.key === ' ') {
        evt.preventDefault();
        evt.stopPropagation();
        const row = appState.tableRows?.[rowIdx];
        if (!row) return;
        openFacilityContextMenu(evt.currentTarget, row);
      }
    });
  });

  appState.tableRows = rows;
}

function selectTableRow(index) {
  const rows = $('facilityTable').querySelectorAll('tbody tr');
  rows.forEach((r) => r.classList.remove('row-selected'));
  const target = rows[index];
  if (!target) return;
  target.classList.add('row-selected');
  target.scrollIntoView({ block: 'nearest' });
  appState.selectedTableIndex = index;
}

function renderChainChart(facilities) {
  const byChain = {};
  facilities.forEach((f) => {
    const name = f.chain_name || 'Unmapped / Independent';
    if (!byChain[name]) byChain[name] = { medicaid: 0, total: 0 };
    byChain[name].medicaid += Number(f.medicaid_revenue || 0);
    byChain[name].total += Number(f.total_revenue || 0);
  });
  const rows = Object.entries(byChain)
    .map(([chain, values]) => ({ chain, ...values }))
    .sort((a, b) => b.medicaid - a.medicaid)
    .slice(0, 10)
    .reverse();
  const axisTicks = buildPaddedCurrencyBarAxis(rows.map((r) => r.medicaid), { includeZero: true, targetTicks: 5, padRatio: 0.2 });

  Plotly.react(
    'chainChart',
    [
      {
        type: 'bar',
        orientation: 'h',
        y: rows.map((r) => r.chain),
        x: rows.map((r) => r.medicaid),
        text: rows.map((r) => `${formatSmartCurrency(r.medicaid)}`),
        textposition: 'outside',
        cliponaxis: false,
        marker: { color: '#3b82f6' },
        hovertemplate: '%{y}<br>Medicaid: %{text}<extra></extra>',
      },
    ],
    {
      margin: { l: 120, r: 85, t: 10, b: 30 },
      paper_bgcolor: 'rgba(0,0,0,0)',
      plot_bgcolor: 'rgba(0,0,0,0)',
      xaxis: { gridcolor: 'rgba(122,133,153,0.2)', ...axisTicks },
      yaxis: { automargin: true },
    },
    { displayModeBar: false, responsive: true },
  );
}

function renderConcentrationBubbleChart(facilities) {
  const sizes = facilities.map((f) => Math.max(8, Math.min(34, Number(f.total_revenue || 0) / 500000000)));
  const yAxisTicks = buildCurrencyAxisTicks(facilities.map((f) => Number(f.total_revenue || 0)), { includeZero: true, targetTicks: 5 });
  const colorMap = {
    government: '#3b82f6',
    not_for_profit: '#2dd881',
    for_profit: '#ea580c',
    unknown: '#6b7280',
  };

  Plotly.react(
    'bubbleChart',
    [
      {
        type: 'scatter',
        mode: 'markers',
        x: facilities.map((f) => Number(f.medicaid_dependency || 0) * 100),
        y: facilities.map((f) => Number(f.total_revenue || 0)),
        marker: {
          size: sizes,
          color: facilities.map((f) => colorMap[f.ownership_group] || '#6b7280'),
          opacity: 0.75,
          line: { color: '#1f2937', width: 1 },
        },
        text: facilities.map((f) => f.facility_name),
        customdata: facilities.map((f) => [f.facility_id, f.state_code, formatSmartCurrency(f.total_revenue)]),
        hovertemplate:
          '%{text}<br>Medicaid dep: %{x:.1f}%<br>Total revenue: %{customdata[2]}<extra></extra>',
      },
    ],
    {
      margin: { l: 55, r: 10, t: 8, b: 45 },
      paper_bgcolor: 'rgba(0,0,0,0)',
      plot_bgcolor: 'rgba(0,0,0,0)',
      xaxis: { title: 'Medicaid Dependency (%)', gridcolor: 'rgba(122,133,153,0.2)' },
      yaxis: { title: 'Total Revenue ($)', ...yAxisTicks, gridcolor: 'rgba(122,133,153,0.2)' },
    },
    { displayModeBar: false, responsive: true },
  );

  const bubbleEl = $('bubbleChart');
  if (typeof bubbleEl.on === 'function' && !bubbleEl.dataset.boundBubbleClick) {
    bubbleEl.on('plotly_click', (evt) => {
      const id = evt.points?.[0]?.customdata?.[0];
      if (id && !String(id).startsWith('CHAIN::')) openFacilityDetail(id);
    });
    bubbleEl.dataset.boundBubbleClick = '1';
  }
}

function renderHistogram(facilities) {
  Plotly.react(
    'histogramChart',
    [
      {
        type: 'histogram',
        x: facilities.map((f) => Number(f.public_dependency || 0) * 100),
        marker: { color: '#8b5cf6' },
        nbinsx: 16,
        hovertemplate: 'Public dependency bin: %{x:.1f}%<br>Count: %{y}<extra></extra>',
      },
    ],
    {
      margin: { l: 45, r: 10, t: 8, b: 35 },
      paper_bgcolor: 'rgba(0,0,0,0)',
      plot_bgcolor: 'rgba(0,0,0,0)',
      xaxis: { title: 'Public Dependency (%)', gridcolor: 'rgba(122,133,153,0.2)' },
      yaxis: { title: 'Facilities', gridcolor: 'rgba(122,133,153,0.2)' },
    },
    { displayModeBar: false, responsive: true },
  );
}

function renderStateInsurerTable(rows = []) {
  const tbody = $('stateInsurerTable').querySelector('tbody');
  if (!rows.length) {
    tbody.innerHTML = '<tr><td colspan="6">No insurer market-share data available for this state/year.</td></tr>';
    $('stateInsurerConfidenceLegend').innerHTML = '';
    return;
  }

  tbody.innerHTML = rows
    .map((r, idx) => `
      <tr>
        <td>${idx + 1}</td>
        <td>${escapeHtml(r.insurer_name || 'Unknown')}</td>
        <td>${fmtPct(r.market_share || 0, 2)}</td>
        <td>${formatSmartCurrency(r.premium || 0)}</td>
        <td>${new Intl.NumberFormat('en-US').format(Number(r.covered_lives || 0))}</td>
        <td>${confidenceBadge(normalizeConfidenceTier(r.confidence_tier || 'B'))}</td>
      </tr>
    `)
    .join('');

  const sourceYear = rows[0]?.source_fiscal_year ? `Source FY ${rows[0].source_fiscal_year}` : 'Source FY N/A';
  $('stateInsurerMeta').textContent = `${sourceYear} · Basis: ${appState.marketShareBasis.replace('_', ' ')}`;
  $('stateInsurerConfidenceLegend').innerHTML = `
    ${confidenceBadge('A')} Direct/high-confidence
    ${confidenceBadge('B')} Public-source mapped
    ${confidenceBadge('C')} Modeled/long-tail
  `;
}

async function loadStateInsurerPanel() {
  if (!appState.selectedState || appState.payerScope !== 'comprehensive') {
    $('stateInsurerPanel').classList.add('hidden');
    return;
  }
  $('stateInsurerPanel').classList.remove('hidden');
  const params = new URLSearchParams({
    fiscal_year: appState.year,
    basis: appState.marketShareBasis,
    top_n: '15',
  });
  const rows = await api(`/api/v1/states/${appState.selectedState}/insurers?${params.toString()}`, {}, loadStateInsurerPanel);
  renderStateInsurerTable(rows);
}

async function populateTrendChains() {
  if (!appState.selectedState) {
    $('trendChainSelect').innerHTML = '<option value="">Select chain</option>';
    $('chainTrendChart').innerHTML = '';
    return;
  }

  const chains = await api(
    `/api/v1/chains?${withPayerScope(new URLSearchParams({
      fiscal_year: appState.year,
      state_code: appState.selectedState,
    })).toString()}`,
    {},
    populateTrendChains,
  );
  const select = $('trendChainSelect');
  const current = appState.selectedTrendChain;
  select.innerHTML = '<option value="">Select chain</option>';
  chains.slice(0, 100).forEach((row) => {
    select.insertAdjacentHTML('beforeend', `<option value="${row.chain_name}">${row.chain_name}</option>`);
  });

  const available = new Set(chains.map((c) => c.chain_name));
  if (current && available.has(current)) {
    select.value = current;
  } else {
    appState.selectedTrendChain = chains[0]?.chain_name || null;
    select.value = appState.selectedTrendChain || '';
  }
}

async function loadSelectedChainTrend() {
  const chain = $('trendChainSelect').value || null;
  appState.selectedTrendChain = chain;
  if (!chain || !appState.selectedState) {
    $('chainTrendChart').innerHTML = '';
    return;
  }
  const rows = await api(
    `/api/v1/trends/chains?${withPayerScope(new URLSearchParams({
      chain_name: chain,
      state_code: appState.selectedState,
    })).toString()}`,
    {},
    loadSelectedChainTrend,
  );
  appState.trendRows.chain = rows;
  renderTrendChart('chainTrendChart', rows, `${chain} (within ${appState.selectedState})`);
}

async function loadStateData() {
  if (!appState.selectedState) return;

  setLoading(true);
  try {
    const state = appState.selectedState;
    const summary = await api(
      `/api/v1/states/${state}/summary?${withPayerScope(new URLSearchParams({ fiscal_year: appState.year })).toString()}`,
      {},
      loadStateData,
    );
    appState.stateSummary = summary;
    $('selectedStateLabel').textContent = `${STATE_NAME[state] || state} (${state})`;

    const stateCards = [
      { label: 'Medicaid', value: formatSmartCurrency(summary.medicaid_total), sub: 'Reported net revenue' },
      { label: 'Medicare', value: formatSmartCurrency(summary.medicare_total), sub: 'Modeled estimate' },
      { label: 'Federal Medicaid', value: formatSmartCurrency(summary.federal_medicaid_total), sub: 'FMAP allocation' },
      { label: 'State Medicaid', value: formatSmartCurrency(summary.state_medicaid_total), sub: 'Residual share' },
      { label: 'Public Dependency', value: fmtPct(summary.public_dependency), sub: 'Public / total revenue' },
      { label: 'Facilities', value: new Intl.NumberFormat('en-US').format(summary.facility_count), sub: 'Medicare-certified' },
    ];
    if (appState.payerScope === 'comprehensive') {
      stateCards.splice(4, 0, { label: 'Private', value: formatSmartCurrency(summary.private_total), sub: appState.taxonomyView === 'carrier_ownership' ? 'Carrier ownership view' : 'Funding-source private' });
      stateCards.splice(5, 0, { label: 'Comprehensive', value: formatSmartCurrency(summary.comprehensive_total), sub: 'Public + private' });
      stateCards.splice(6, 0, { label: 'Private Dependency', value: fmtPct(summary.private_dependency), sub: 'Private / total revenue' });
    }
    renderKpiCards($('stateSummary'), stateCards);

    const paramsUngrouped = new URLSearchParams({
      fiscal_year: appState.year,
      ownership: appState.filters.ownership,
      sort: appState.filters.sort,
      descending: 'true',
      group_by_chain: 'false',
    });
    withPayerScope(paramsUngrouped);

    const facilitiesUngrouped = await api(
      `/api/v1/states/${state}/facilities?${paramsUngrouped.toString()}`,
      {},
      loadStateData,
    );

    appState.facilitiesUngrouped = facilitiesUngrouped;

    const paramsTable = new URLSearchParams({
      fiscal_year: appState.year,
      ownership: appState.filters.ownership,
      sort: appState.filters.sort,
      descending: 'true',
      group_by_chain: String(appState.filters.groupByChain),
    });
    withPayerScope(paramsTable);

    const rows = await api(`/api/v1/states/${state}/facilities?${paramsTable.toString()}`, {}, loadStateData);
    appState.facilities = filterFacilitiesBySearch(rows);

    renderFilterChips();
    renderFacilityTable(appState.facilities);
    renderChainChart(appState.facilitiesUngrouped);
    renderConcentrationBubbleChart(appState.facilitiesUngrouped);
    renderHistogram(appState.facilitiesUngrouped);
    const stateTrend = await api(
      `/api/v1/trends/states/${state}?${withPayerScope(new URLSearchParams()).toString()}`,
      {},
      loadStateData,
    );
    appState.trendRows.state = stateTrend;
    renderTrendChart('stateTrendChart', stateTrend, `${state} historical trajectory`);
    await loadStateInsurerPanel();
    await populateTrendChains();
    await loadSelectedChainTrend();

    toggleAnalysisView(appState.filters.analysisView);
    updateBreadcrumb();
  } finally {
    setLoading(false);
  }
}

function toggleAnalysisView(viewName) {
  appState.filters.analysisView = viewName;
  $('analysisViewSelect').value = viewName;
  $('chainChart').classList.toggle('hidden', viewName !== 'chain');
  $('bubbleChart').classList.toggle('hidden', viewName !== 'concentration');
}

async function selectState(stateCode, makeVisible = true) {
  appState.selectedState = stateCode.toUpperCase();
  updateBreadcrumb();
  syncUrl();
  await loadStateData();
  if (makeVisible) {
    $('stateDrilldownPanel').scrollIntoView({ behavior: 'smooth', block: 'start' });
  }
}

function clearStateSelection() {
  appState.selectedState = null;
  appState.stateSummary = null;
  appState.selectedTrendChain = null;
  $('selectedStateLabel').textContent = '(none)';
  $('stateSummary').innerHTML = '';
  $('facilityTable').querySelector('tbody').innerHTML = '';
  $('chainChart').innerHTML = '';
  $('bubbleChart').innerHTML = '';
  $('histogramChart').innerHTML = '';
  $('stateTrendChart').innerHTML = '';
  $('chainTrendChart').innerHTML = '';
  $('stateInsurerTable').querySelector('tbody').innerHTML = '';
  $('stateInsurerMeta').textContent = '';
  $('stateInsurerConfidenceLegend').innerHTML = '';
  $('stateInsurerPanel').classList.add('hidden');
  appState.trendRows.state = [];
  appState.trendRows.chain = [];
  $('trendChainSelect').innerHTML = '<option value="">Select chain</option>';
  updateBreadcrumb();
  syncUrl();
}

function renderDetailLinkage(mapping, linkage) {
  const mapStatus = mapping?.emma_mapping_status || 'unmapped';
  const resolvedUrl = mapping?.emma_resolved_url || mapping?.emma_issuer_url || '';
  const linkType = mapping?.emma_link_type || (resolvedUrl ? 'issuer' : 'none');
  const isResolved = Boolean(resolvedUrl);
  const mapLabel = mapStatus.replace(/_/g, ' ');
  const score = Number(mapping?.emma_match_score || 0);
  const scoreText = score > 0 ? `score ${score.toFixed(2)}` : 'score n/a';
  const fallbackStatus = mapping?.emma_fallback_status || 'not_requested';
  const fallbackType = mapping?.emma_fallback_type || 'none';
  const fallbackBasis = mapping?.emma_fallback_match_basis || '';
  const fallbackCusip = mapping?.emma_fallback_cusip_query || mapping?.emma_fallback_cusip9 || '';

  $('detailEmmaMapping').innerHTML = `
    <span class="chip">Mapping: ${mapLabel}</span>
    <span class="chip">Method: ${mapping?.emma_mapping_method || 'none'}</span>
    <span class="chip">${scoreText}</span>
    <span class="chip">Link type: ${escapeHtml(linkType)}</span>
    ${
      isResolved
        ? `<a class="chip" href="${resolvedUrl}" target="_blank" rel="noopener">${linkType === 'issuer' ? 'EMMA issuer profile' : 'Open EMMA fallback link'}</a>`
        : '<span class="chip">EMMA link unavailable</span>'
    }
    ${fallbackStatus !== 'not_requested' ? `<span class="chip">Fallback: ${escapeHtml(fallbackStatus)}</span>` : ''}
    ${fallbackType !== 'none' ? `<span class="chip">Fallback type: ${escapeHtml(fallbackType)}</span>` : ''}
    ${fallbackCusip ? `<span class="chip">CUSIP: ${escapeHtml(fallbackCusip)}</span>` : ''}
    ${fallbackBasis ? `<span class="chip">Basis: ${escapeHtml(fallbackBasis)}</span>` : ''}
    <span class="chip">Cache: ${linkage?.cache_status || 'n/a'}</span>
  `;

  const ownedRows = linkage?.owned_securities || [];
  $('detailOwnedTable').querySelector('tbody').innerHTML = ownedRows.length
    ? ownedRows
      .map(
        (r) => `
      <tr>
        <td>${escapeHtml(r.cusip9 || '')}</td>
        <td>${r.security_url ? `<a href=\"${escapeHtml(r.security_url)}\" target=\"_blank\" rel=\"noopener\">${escapeHtml(r.issue_description || 'Security')}</a>` : escapeHtml(r.issue_description || '—')}</td>
        <td>${escapeHtml(r.security_status || 'matched')}</td>
      </tr>
    `,
      )
      .join('')
    : `<tr><td colspan="3">No owned securities matched for this issuer.</td></tr>`;

  const docRows = linkage?.related_documents || [];
  $('detailDocsTable').querySelector('tbody').innerHTML = docRows.length
    ? docRows
      .map(
        (d) => `
      <tr>
        <td>${escapeHtml(d.document_type || 'Document')}</td>
        <td>${d.document_url ? `<a href=\"${escapeHtml(d.document_url)}\" target=\"_blank\" rel=\"noopener\">${escapeHtml(d.title || 'Open')}</a>` : escapeHtml(d.title || '—')}</td>
        <td>${escapeHtml(d.related_cusip9 || '')}</td>
      </tr>
    `,
      )
      .join('')
    : `<tr><td colspan="3">No related disclosure documents found.</td></tr>`;
}

async function loadDetailLinkage(facilityId, { forceRefresh = false } = {}) {
  const emma = await api(
    `/api/v1/facilities/${encodeURIComponent(facilityId)}/emma-link?fiscal_year=${appState.year}&include_fallback=true&portfolio_id=${encodeURIComponent(appState.portfolioId)}`,
    {},
    () => loadDetailLinkage(facilityId, { forceRefresh }),
  );
  const linkage = await api(
    `/api/v1/facilities/${encodeURIComponent(facilityId)}/portfolio-linkage?fiscal_year=${appState.year}&portfolio_id=${encodeURIComponent(appState.portfolioId)}&force_refresh=${forceRefresh}`,
    {},
    () => loadDetailLinkage(facilityId, { forceRefresh }),
  );
  renderDetailLinkage(emma, linkage);
}

function renderPayorMixChart(detail) {
  const sources = [
    { label: 'Medicaid', value: Number(detail.medicaid_revenue || 0), color: '#2dd881' },
    { label: 'Medicare', value: Number(detail.medicare_revenue || 0), color: '#3b82f6' },
    { label: 'Private Insurance', value: Number(detail.private_revenue || 0), color: '#a855f7' },
  ].filter((s) => s.value > 0);

  const chartEl = $('detailPayorMixChart');
  if (!sources.length) {
    chartEl.innerHTML = '<div class="chip">No payor mix data available.</div>';
    return;
  }

  const totalRevenue = Number(detail.total_revenue || 0);
  Plotly.react(
    'detailPayorMixChart',
    [
      {
        type: 'pie',
        labels: sources.map((s) => s.label),
        values: sources.map((s) => s.value),
        marker: { colors: sources.map((s) => s.color) },
        sort: false,
        textinfo: 'label+percent',
        customdata: sources.map((s) => [
          formatSmartCurrency(s.value),
          totalRevenue > 0 ? fmtPct(s.value / totalRevenue, 2) : 'n/a',
        ]),
        hovertemplate: '%{label}<br>%{customdata[0]}<br>%{percent} of payor mix<br>%{customdata[1]} of total revenue<extra></extra>',
      },
    ],
    {
      margin: { l: 10, r: 10, t: 8, b: 8 },
      showlegend: true,
      paper_bgcolor: 'rgba(0,0,0,0)',
      plot_bgcolor: 'rgba(0,0,0,0)',
      legend: { orientation: 'h', y: -0.1 },
    },
    { displayModeBar: false, responsive: true },
  );
}

async function openFacilityDetail(facilityId, opts = {}) {
  appState.selectedFacilityId = facilityId;
  const detail = await api(`/api/v1/facilities/${facilityId}?fiscal_year=${appState.year}`, {}, () => openFacilityDetail(facilityId, opts));
  const trendRows = await api(`/api/v1/trends/facilities/${encodeURIComponent(facilityId)}`, {}, () => openFacilityDetail(facilityId, opts));
  const insurerExposure = await api(
    `/api/v1/facilities/${encodeURIComponent(facilityId)}/insurer-exposure?fiscal_year=${appState.year}&basis=${appState.marketShareBasis}&top_n=10`,
    {},
    () => openFacilityDetail(facilityId, opts),
  );
  appState.trendRows.facility = trendRows;
  appState.trendRows.facilityTitle = `${detail.facility_name} trajectory`;

  $('facilityDetailPanel').classList.remove('hidden');
  $('detailTitle').textContent = detail.facility_name;
  $('detailMeta').textContent = `${detail.city || ''}, ${detail.state_code} · CCN: ${detail.facility_id} · ${detail.ownership_type}`;

  $('detailBadges').innerHTML = `
    <span class="badge warning">Modeled Medicare: ${detail.medicare_method}</span>
    <span class="badge info">FMAP-based split</span>
    <span class="badge neutral">Chain confidence: ${detail.chain_confidence}</span>
    <span class="badge neutral">Private method: ${detail.private_data_method || 'modeled'}</span>
    ${confidenceBadge(normalizeConfidenceTier(detail.private_data_confidence))}
  `;

  const detailCards = [
    { label: 'Total revenue', value: formatSmartCurrency(detail.total_revenue) },
    { label: 'Public dependency', value: fmtPct(detail.public_dependency, 2) },
    { label: 'Medicaid dependency', value: fmtPct(detail.medicaid_dependency, 2) },
  ];
  if (appState.payerScope === 'comprehensive') {
    detailCards.push({ label: 'Private revenue', value: formatSmartCurrency(detail.private_revenue || 0) });
    detailCards.push({ label: 'Private dependency', value: fmtPct(detail.private_dependency || 0, 2) });
  }
  renderKpiCards($('detailKpis'), detailCards);

  await loadDetailLinkage(facilityId);
  $('detailRefreshLinkageBtn').onclick = () => loadDetailLinkage(facilityId, { forceRefresh: true });
  const revenueComponents = [
    Number(detail.medicare_revenue || 0),
    Number(detail.federal_medicaid_revenue || 0),
    Number(detail.state_medicaid_revenue || 0),
    Number(detail.private_revenue || 0),
    Number(detail.uninsured_other_revenue || detail.other_revenue || 0),
  ];
  const revenueBreakdownAxis = buildPaddedCurrencyBarAxis(
    [0, revenueComponents.reduce((sum, value) => sum + Math.max(0, value), 0)],
    { includeZero: true, targetTicks: 5, padRatio: 0.16 },
  );

  Plotly.react(
    'detailBreakdownChart',
    [
      {
        type: 'bar',
        orientation: 'h',
        y: ['Revenue'],
        x: [detail.medicare_revenue],
        name: 'Medicare',
        marker: { color: '#3b82f6' },
      },
      {
        type: 'bar',
        orientation: 'h',
        y: ['Revenue'],
        x: [detail.federal_medicaid_revenue],
        name: 'Fed Medicaid',
        marker: { color: '#2dd881' },
      },
      {
        type: 'bar',
        orientation: 'h',
        y: ['Revenue'],
        x: [detail.state_medicaid_revenue],
        name: 'State Medicaid',
        marker: { color: '#06b6d4' },
      },
      {
        type: 'bar',
        orientation: 'h',
        y: ['Revenue'],
        x: [detail.private_revenue || 0],
        name: 'Private',
        marker: { color: '#a855f7' },
      },
      {
        type: 'bar',
        orientation: 'h',
        y: ['Revenue'],
        x: [detail.uninsured_other_revenue || detail.other_revenue],
        name: 'Uninsured / Other',
        marker: { color: '#6b7280' },
      },
    ],
    {
      barmode: 'stack',
      margin: { l: 10, r: 16, t: 8, b: 42 },
      showlegend: true,
      paper_bgcolor: 'rgba(0,0,0,0)',
      plot_bgcolor: 'rgba(0,0,0,0)',
      xaxis: {
        ...revenueBreakdownAxis,
        tickangle: 0,
        automargin: true,
        gridcolor: 'rgba(122,133,153,0.2)',
      },
      yaxis: { showticklabels: false },
    },
    { displayModeBar: false, responsive: true },
  );
  renderPayorMixChart(detail);

  renderTrendChart('detailTrendChart', trendRows, appState.trendRows.facilityTitle);

  $('detailTable').innerHTML = `
    <tr><td>Total Revenue</td><td>${fmtUsdFull(detail.total_revenue)}</td></tr>
    <tr><td>Medicare Revenue</td><td>${fmtUsdFull(detail.medicare_revenue)}</td></tr>
    <tr><td>Medicaid Revenue</td><td>${fmtUsdFull(detail.medicaid_revenue)}</td></tr>
    <tr><td>Federal Medicaid</td><td>${fmtUsdFull(detail.federal_medicaid_revenue)}</td></tr>
    <tr><td>State Medicaid</td><td>${fmtUsdFull(detail.state_medicaid_revenue)}</td></tr>
    <tr><td>Private Revenue</td><td>${fmtUsdFull(detail.private_revenue || 0)}</td></tr>
    <tr><td>Uninsured/Other</td><td>${fmtUsdFull(detail.uninsured_other_revenue || detail.other_revenue || 0)}</td></tr>
    <tr><td>Other Revenue</td><td>${fmtUsdFull(detail.other_revenue)}</td></tr>
  `;

  const topInsurers = (insurerExposure?.insurers || []).slice(0, 3);
  if (topInsurers.length) {
    $('detailTable').insertAdjacentHTML(
      'beforeend',
      topInsurers
        .map((r) => `<tr><td>Top Insurer: ${escapeHtml(r.insurer_name || 'Unknown')}</td><td>${fmtUsdFull(r.estimated_revenue || 0)} (${fmtPct(r.exposure_pct || 0, 2)})</td></tr>`)
        .join(''),
    );
  }

  $('detailScenarioBtn').onclick = () => {
    setView('scenarios');
    $('scenarioStateSelect').value = detail.state_code;
    populateScenarioChains().then(async () => {
      $('scenarioChainSelect').value = detail.chain_name || '';
      await loadScenarioInsurerOverrides();
      status(`Scenario scope prefilled for ${detail.facility_name}.`, 'info');
    });
  };

  if (opts.focusOwned) {
    $('detailOwnedTable').scrollIntoView({ behavior: 'smooth', block: 'nearest' });
  }

  updateBreadcrumb(detail.facility_name);
}

function closeFacilityDetail() {
  $('facilityDetailPanel').classList.add('hidden');
  appState.selectedFacilityId = null;
  appState.trendRows.facility = [];
  appState.trendRows.facilityTitle = '';
  $('detailEmmaMapping').innerHTML = '';
  $('detailPayorMixChart').innerHTML = '';
  $('detailOwnedTable').querySelector('tbody').innerHTML = '';
  $('detailDocsTable').querySelector('tbody').innerHTML = '';
  updateBreadcrumb();
}

async function populateScenarioScope(stateRows = appState.states) {
  const stateSelect = $('scenarioStateSelect');
  stateSelect.innerHTML = '<option value="">All states</option>';
  [...stateRows]
    .sort((a, b) => a.state_code.localeCompare(b.state_code))
    .forEach((r) => {
      stateSelect.insertAdjacentHTML('beforeend', `<option value="${r.state_code}">${r.state_code} — ${STATE_NAME[r.state_code] || ''}</option>`);
    });

  await populateScenarioChains();
}

async function populateScenarioChains() {
  const selectedState = $('scenarioStateSelect').value;
  const params = withPayerScope(new URLSearchParams({ fiscal_year: appState.year }));
  if (selectedState) params.set('state_code', selectedState);
  const path = `/api/v1/chains?${params.toString()}`;
  const chains = await api(path, {}, populateScenarioChains);
  const chainSelect = $('scenarioChainSelect');
  const current = chainSelect.value;
  chainSelect.innerHTML = '<option value="">All chains</option>';
  chains.slice(0, 200).forEach((c) => {
    chainSelect.insertAdjacentHTML('beforeend', `<option value="${c.chain_name}">${c.chain_name}</option>`);
  });
  if ([...chainSelect.options].some((o) => o.value === current)) chainSelect.value = current;

  await loadScenarioInsurerOverrides();
}

function renderScenarioInsurerOverrides(rows = []) {
  const panel = $('scenarioInsurerOverrides');
  const hint = $('scenarioInsurerOverrideHint');
  const tbody = $('scenarioInsurerTable').querySelector('tbody');

  if (appState.payerScope !== 'comprehensive') {
    panel.classList.add('hidden');
    appState.scenarioInsurers = [];
    appState.scenarioInsurerOverrides = {};
    return;
  }

  panel.classList.remove('hidden');

  if (!rows.length) {
    tbody.innerHTML = '<tr><td colspan="3">Select a state with insurer market data to configure insurer-specific cuts.</td></tr>';
    hint.textContent = 'Overrides are applied as additional private cuts weighted by insurer market share.';
    appState.scenarioInsurers = [];
    appState.scenarioInsurerOverrides = {};
    return;
  }

  appState.scenarioInsurers = rows;
  const freshOverrides = {};
  rows.forEach((row) => {
    const key = String(row.insurer_id || '');
    if (!key) return;
    const previous = Number(appState.scenarioInsurerOverrides[key] ?? 0);
    freshOverrides[key] = Number.isFinite(previous) ? Math.max(0, Math.min(previous, 40)) : 0;
  });
  appState.scenarioInsurerOverrides = freshOverrides;

  tbody.innerHTML = rows
    .map((row) => {
      const insurerId = String(row.insurer_id || '');
      const value = Number(appState.scenarioInsurerOverrides[insurerId] || 0);
      return `
        <tr>
          <td>${escapeHtml(row.insurer_name || 'Unknown')} ${confidenceBadge(normalizeConfidenceTier(row.confidence_tier || 'B'))}</td>
          <td>${fmtPct(row.market_share || 0, 2)}</td>
          <td><input class="insurer-cut-input" type="number" min="0" max="40" step="1" data-insurer-cut="${escapeHtml(insurerId)}" value="${value}" />%</td>
        </tr>
      `;
    })
    .join('');

  const sourceYear = rows[0]?.source_fiscal_year ? `Source FY ${rows[0].source_fiscal_year}` : 'Source FY N/A';
  hint.textContent = `${sourceYear}. Overrides apply on top of aggregate private cut in the selected state scope.`;

  tbody.querySelectorAll('[data-insurer-cut]').forEach((input) => {
    input.addEventListener('input', (evt) => {
      const insurerId = evt.target.dataset.insurerCut;
      const pct = Math.max(0, Math.min(40, Number(evt.target.value || 0)));
      evt.target.value = String(pct);
      appState.scenarioInsurerOverrides[insurerId] = pct;
    });
  });
}

async function loadScenarioInsurerOverrides() {
  const state = $('scenarioStateSelect').value;
  if (appState.payerScope !== 'comprehensive' || !state) {
    renderScenarioInsurerOverrides([]);
    return;
  }
  const params = new URLSearchParams({
    fiscal_year: appState.year,
    basis: appState.marketShareBasis,
    top_n: '8',
  });
  const rows = await api(`/api/v1/states/${state}/insurers?${params.toString()}`, {}, loadScenarioInsurerOverrides);
  renderScenarioInsurerOverrides(rows);
}

function renderTornado(result) {
  const rows = [...result.top_impacted_facilities].slice(0, 20).reverse();
  const xValues = rows.map((r) => -Math.abs(Number(r.revenue_at_risk_abs || 0)));
  const axisTicks = buildPaddedCurrencyBarAxis(xValues, { includeZero: true, targetTicks: 5, padRatio: 0.22 });
  Plotly.react(
    'tornadoChart',
    [
      {
        type: 'bar',
        orientation: 'h',
        y: rows.map((r) => r.facility_name),
        x: xValues,
        marker: { color: '#e85d5d' },
        text: rows.map((r) => `${formatSmartCurrency(r.revenue_at_risk_abs)} (${fmtPct(r.revenue_at_risk_pct)})`),
        textposition: 'outside',
        cliponaxis: false,
        hovertemplate: '%{y}<br>Revenue at risk: %{text}<extra></extra>',
      },
    ],
    {
      margin: { l: 160, r: 110, t: 10, b: 30 },
      paper_bgcolor: 'rgba(0,0,0,0)',
      plot_bgcolor: 'rgba(0,0,0,0)',
      xaxis: {
        title: 'Loss magnitude',
        ...axisTicks,
        gridcolor: 'rgba(122,133,153,0.2)',
        zeroline: true,
        zerolinecolor: '#9ca3af',
      },
      yaxis: { automargin: true },
    },
    { displayModeBar: false, responsive: true },
  );
}

function renderScenarioTable(result) {
  const tbody = $('scenarioTable').querySelector('tbody');
  tbody.innerHTML = result.top_impacted_facilities
    .map(
      (r) => `
      <tr>
        <td>${r.facility_name}</td>
        <td>${r.state_code}</td>
        <td>${formatSmartCurrency(r.baseline_total_revenue)}</td>
        <td>${formatSmartCurrency(r.shocked_total_revenue)}</td>
        <td>${formatSmartCurrency(r.revenue_at_risk_abs)}</td>
        <td>${fmtPct(r.revenue_at_risk_pct)}</td>
      </tr>
    `,
    )
    .join('');
}

async function runScenario() {
  const insurerOverrides = Object.fromEntries(
    Object.entries(appState.scenarioInsurerOverrides || {})
      .map(([k, v]) => [k, Number(v || 0)])
      .filter(([, v]) => v > 0),
  );

  const payload = {
    fiscal_year: appState.year,
    medicare_cut_pct: Number($('medicareCut').value),
    federal_medicaid_cut_pct: Number($('fedCut').value),
    state_medicaid_cut_pct: Number($('stateCut').value),
    private_cut_pct: Number($('privateCut').value),
    payer_scope: appState.payerScope,
    taxonomy_view: appState.taxonomyView,
    market_share_basis: appState.marketShareBasis,
    insurer_cut_overrides: insurerOverrides,
    state_code: $('scenarioStateSelect').value || null,
    chain_name: $('scenarioChainSelect').value || null,
  };

  const result = await api('/api/v1/scenarios/run', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  }, runScenario);

  const scenarioCards = [
    { label: 'Baseline Revenue', value: formatSmartCurrency(result.baseline_total_revenue) },
    { label: 'Shocked Revenue', value: formatSmartCurrency(result.shocked_total_revenue) },
    { label: 'Revenue At Risk', value: formatSmartCurrency(result.revenue_at_risk_abs) },
    { label: 'Risk %', value: fmtPct(result.revenue_at_risk_pct, 2) },
    { label: 'Payer Scope', value: appState.payerScope === 'comprehensive' ? 'Comprehensive' : 'Public only', sub: appState.taxonomyView.replace('_', ' ') },
    { label: 'Scope', value: result.scope_state_code || 'National', sub: result.scope_chain_name || 'All chains' },
  ];
  const overrideCount = Object.values(insurerOverrides).filter((v) => Number(v) > 0).length;
  if (overrideCount > 0) {
    scenarioCards.push({ label: 'Insurer Overrides', value: String(overrideCount), sub: 'Insurers with custom cuts' });
  }
  renderKpiCards($('scenarioSummary'), scenarioCards);

  renderTornado(result);
  renderScenarioTable(result);
  status(`Scenario complete. ${result.top_impacted_facilities.length} facilities shown in impact ranking.`, 'info');
}

function resetScenarioControls() {
  $('medicareCut').value = '0';
  $('fedCut').value = '0';
  $('stateCut').value = '0';
  $('privateCut').value = '0';
  $('medicareCutVal').textContent = '0%';
  $('fedCutVal').textContent = '0%';
  $('stateCutVal').textContent = '0%';
  $('privateCutVal').textContent = '0%';
  $('scenarioStateSelect').value = '';
  $('scenarioChainSelect').value = '';
  appState.scenarioInsurerOverrides = {};
  renderScenarioInsurerOverrides([]);
}

function loadScenarioPresets() {
  const presets = JSON.parse(localStorage.getItem(PRESET_KEY) || '{}');
  const select = $('scenarioPresetSelect');
  select.innerHTML = '<option value="">Select preset</option>';
  Object.keys(presets)
    .sort()
    .forEach((name) => {
      select.insertAdjacentHTML('beforeend', `<option value="${name}">${name}</option>`);
    });
}

function saveScenarioPreset() {
  const name = window.prompt('Preset name:');
  if (!name) return;
  const presets = JSON.parse(localStorage.getItem(PRESET_KEY) || '{}');
  presets[name] = {
    medicareCut: $('medicareCut').value,
    fedCut: $('fedCut').value,
    stateCut: $('stateCut').value,
    privateCut: $('privateCut').value,
    payerScope: appState.payerScope,
    taxonomyView: appState.taxonomyView,
    marketShareBasis: appState.marketShareBasis,
    insurerOverrides: appState.scenarioInsurerOverrides,
    state: $('scenarioStateSelect').value,
    chain: $('scenarioChainSelect').value,
  };
  localStorage.setItem(PRESET_KEY, JSON.stringify(presets));
  loadScenarioPresets();
  $('scenarioPresetSelect').value = name;
  status(`Saved preset: ${name}`, 'info');
}

function applyPreset(name) {
  if (!name) return;
  const presets = JSON.parse(localStorage.getItem(PRESET_KEY) || '{}');
  const p = presets[name];
  if (!p) return;
  $('medicareCut').value = p.medicareCut;
  $('fedCut').value = p.fedCut;
  $('stateCut').value = p.stateCut;
  $('privateCut').value = p.privateCut ?? '0';
  $('medicareCutVal').textContent = `${p.medicareCut}%`;
  $('fedCutVal').textContent = `${p.fedCut}%`;
  $('stateCutVal').textContent = `${p.stateCut}%`;
  $('privateCutVal').textContent = `${p.privateCut ?? 0}%`;
  appState.payerScope = p.payerScope || appState.payerScope;
  appState.taxonomyView = p.taxonomyView || appState.taxonomyView;
  appState.marketShareBasis = p.marketShareBasis || appState.marketShareBasis;
  appState.scenarioInsurerOverrides = p.insurerOverrides || {};
  applyFiltersToControls();
  $('scenarioStateSelect').value = p.state || '';
  populateScenarioChains().then(async () => {
    $('scenarioChainSelect').value = p.chain || '';
    await loadScenarioInsurerOverrides();
  });
}

function attachSliderLabel(sliderId, labelId) {
  const slider = $(sliderId);
  const label = $(labelId);
  label.textContent = `${slider.value}%`;
  slider.addEventListener('input', () => {
    label.textContent = `${slider.value}%`;
  });
}

function exportFacilitiesCsv() {
  if (!appState.selectedState) {
    alert('Select a state first.');
    return;
  }

  const confirmed = window.confirm(`Export filtered facilities for ${appState.selectedState}?`);
  if (!confirmed) return;

  const params = new URLSearchParams({
    fiscal_year: appState.year,
    state_code: appState.selectedState,
    ownership: appState.filters.ownership,
    payer_scope: appState.payerScope,
    taxonomy_view: appState.taxonomyView,
  });
  window.open(apiUrl(`/api/v1/exports/facilities.csv?${params.toString()}`), '_blank');
}

function isInputFocused() {
  const a = document.activeElement;
  if (!a) return false;
  const tag = a.tagName?.toLowerCase();
  return tag === 'input' || tag === 'textarea' || tag === 'select' || a.isContentEditable;
}

function setupKeyboardShortcuts() {
  document.addEventListener('keydown', (e) => {
    if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === 'k') {
      e.preventDefault();
      if (appState.selectedState) {
        $('facilitySearch').focus();
      } else {
        $('yearSelect').focus();
      }
      return;
    }

    if (e.key === 'Escape') {
      if (!$('facilityContextMenu').classList.contains('hidden')) {
        closeFacilityContextMenu();
        return;
      }
      if (!$('facilityDetailPanel').classList.contains('hidden')) {
        closeFacilityDetail();
        return;
      }
      if (appState.selectedState) {
        clearStateSelection();
      }
      return;
    }

    if (isInputFocused()) return;

    if (e.key === '1') {
      setView('national');
      return;
    }
    if (e.key === '2') {
      setView('scenarios');
      return;
    }
    if (e.key === '/') {
      e.preventDefault();
      $('facilitySearch').focus();
      return;
    }
    if (e.key.toLowerCase() === 'e') {
      exportFacilitiesCsv();
      return;
    }
    if (e.key === '?') {
      alert('Shortcuts: 1 National, 2 Scenarios, / Search, j/k rows, Enter open row, e Export, Esc close panel/back.');
      return;
    }

    if (e.key.toLowerCase() === 'j') {
      const next = Math.min((appState.selectedTableIndex < 0 ? -1 : appState.selectedTableIndex) + 1, appState.tableRows.length - 1);
      selectTableRow(next);
      return;
    }

    if (e.key.toLowerCase() === 'k') {
      const next = Math.max((appState.selectedTableIndex < 0 ? 1 : appState.selectedTableIndex) - 1, 0);
      selectTableRow(next);
      return;
    }

    if (e.key === 'Enter' && appState.selectedTableIndex >= 0) {
      const row = appState.tableRows[appState.selectedTableIndex];
      if (row && !String(row.facility_id).startsWith('CHAIN::')) openFacilityDetail(row.facility_id);
      return;
    }

    if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === 'z') {
      const previous = appState.filterHistory.pop();
      if (!previous) return;
      appState.filters = JSON.parse(previous);
      applyFiltersToControls();
      if (appState.selectedState) loadStateData();
      syncUrl();
    }
  });
}

function bindEvents() {
  $('tabNational').addEventListener('click', () => setView('national'));
  $('tabScenarios').addEventListener('click', () => setView('scenarios'));

  $('themeToggle').addEventListener('click', () => {
    setTheme(appState.theme === 'dark' ? 'light' : 'dark');
    refreshNational();
  });

  $('yearSelect').addEventListener('change', async (e) => {
    appState.year = Number(e.target.value);
    renderVintage();
    syncUrl();
    await refreshNational();
  });

  $('metricSelect').addEventListener('change', async (e) => {
    appState.metric = e.target.value;
    syncUrl();
    await refreshNational();
  });

  $('payerScopeSelect').addEventListener('change', async (e) => {
    appState.payerScope = e.target.value;
    syncMetricOptions();
    syncUrl();
    await refreshNational();
    await loadScenarioInsurerOverrides();
  });

  $('taxonomyViewSelect').addEventListener('change', async (e) => {
    appState.taxonomyView = e.target.value;
    syncUrl();
    await refreshNational();
    await loadScenarioInsurerOverrides();
  });

  $('marketShareBasisSelect').addEventListener('change', async (e) => {
    appState.marketShareBasis = e.target.value;
    syncUrl();
    if (appState.selectedState && appState.payerScope === 'comprehensive') {
      await loadStateInsurerPanel();
    }
    await loadScenarioInsurerOverrides();
  });

  $('trendModeSelect').addEventListener('change', (e) => {
    appState.trendMode = e.target.value;
    syncUrl();
    rerenderAllTrendCharts();
  });

  $('analysisViewSelect').addEventListener('change', (e) => toggleAnalysisView(e.target.value));

  const onFilterChange = async (updater) => {
    pushFilterHistory();
    updater();
    applyFiltersToControls();
    syncUrl();
    if (appState.selectedState) await loadStateData();
  };

  $('facilitySearch').addEventListener('input', () => onFilterChange(() => {
    appState.filters.search = $('facilitySearch').value;
  }));

  $('ownershipFilter').addEventListener('change', () => onFilterChange(() => {
    appState.filters.ownership = $('ownershipFilter').value;
  }));

  $('groupByChain').addEventListener('change', () => onFilterChange(() => {
    appState.filters.groupByChain = $('groupByChain').checked;
  }));

  $('facilitySort').addEventListener('change', () => onFilterChange(() => {
    appState.filters.sort = $('facilitySort').value;
  }));

  $('densityMode').addEventListener('change', () => onFilterChange(() => {
    appState.filters.density = $('densityMode').value;
  }));

  $('clearFiltersBtn').addEventListener('click', async () => {
    pushFilterHistory();
    appState.filters.search = '';
    appState.filters.ownership = 'all';
    appState.filters.groupByChain = false;
    appState.filters.sort = 'medicaid_revenue';
    appState.filters.density = 'default';
    applyFiltersToControls();
    syncUrl();
    if (appState.selectedState) await loadStateData();
  });

  $('exportBtn').addEventListener('click', exportFacilitiesCsv);
  $('closeDetailBtn').addEventListener('click', closeFacilityDetail);
  $('ctxOpenDetail').addEventListener('click', () => runFacilityMenuAction('detail'));
  $('ctxOpenEmma').addEventListener('click', () => runFacilityMenuAction('emma'));
  $('ctxViewOwned').addEventListener('click', () => runFacilityMenuAction('owned'));
  document.addEventListener('click', (evt) => {
    const menu = $('facilityContextMenu');
    if (menu.classList.contains('hidden')) return;
    if (!menu.contains(evt.target)) {
      closeFacilityContextMenu();
    }
  });
  window.addEventListener('resize', closeFacilityContextMenu);
  window.addEventListener('scroll', closeFacilityContextMenu, { passive: true });

  attachSliderLabel('medicareCut', 'medicareCutVal');
  attachSliderLabel('fedCut', 'fedCutVal');
  attachSliderLabel('stateCut', 'stateCutVal');
  attachSliderLabel('privateCut', 'privateCutVal');

  $('runScenarioBtn').addEventListener('click', runScenario);
  $('resetScenarioBtn').addEventListener('click', resetScenarioControls);
  $('savePresetBtn').addEventListener('click', saveScenarioPreset);
  $('scenarioPresetSelect').addEventListener('change', (e) => applyPreset(e.target.value));
  $('scenarioStateSelect').addEventListener('change', populateScenarioChains);
  $('trendChainSelect').addEventListener('change', loadSelectedChainTrend);

  setupKeyboardShortcuts();
}

function renderVintage() {
  const src = appState.metadata?.source_stats || {};
  const year = appState.year || src.selected_fiscal_year || 'N/A';
  const generated = appState.metadata?.generated_at_utc || '';
  const releaseYears = Array.isArray(src.release_years_loaded) ? src.release_years_loaded : [];
  const rangeText = releaseYears.length
    ? `${Math.min(...releaseYears)}-${Math.max(...releaseYears)} releases`
    : 'single release';

  $('dataVintageBadge').textContent = `FY ${year} · Final`;
  $('sourceNote').textContent = `Data as of FY ${year} (CMS Cost Reports, ${rangeText}). FMAP table: FY 2026. Medicare is modeled. ETL run: ${generated || 'N/A'}.`;
}

async function init() {
  parseUrlState();
  setTheme(appState.theme);
  bindEvents();
  applyFiltersToControls();
  toggleAnalysisView(appState.filters.analysisView);
  loadScenarioPresets();

  const meta = await api('/api/v1/meta/years', {}, init);
  appState.metadata = meta.metadata || {};
  const years = [...(meta.years || [])].sort((a, b) => b - a);
  const yearSelect = $('yearSelect');
  yearSelect.innerHTML = years.map((y) => `<option value="${y}">${y}</option>`).join('');

  appState.year = appState.year || Number(years[0]);
  yearSelect.value = String(appState.year);
  $('metricSelect').value = appState.metric;

  renderVintage();
  setView(appState.view);
  syncUrl();

  await refreshNational();

  if (appState.selectedState) {
    await selectState(appState.selectedState, false);
  }
}

init().catch((err) => {
  console.error(err);
  status(`Initialization failed: ${err.message}`, 'error', init);
});
