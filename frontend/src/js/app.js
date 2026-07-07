import { AOIS, APP_CONFIG, METRICS, PRODUCTS } from "./config.js";
import { createOceanApi } from "./api/client.js?v=0.4.6";
import { state, setState, subscribe } from "./state.js";
import { fillSelect, renderMetricTabs } from "./components/controls.js";
import { renderPipelineKpis } from "./components/kpiCards.js";
import { renderComponentBars } from "./components/bars.js?v=0.4.6";
import { createOceanMap } from "./components/leafletMap.js";
import { drawTrend } from "./components/trendCanvas.js";

const api = createOceanApi();
let oceanMap;
const byId = (id) => document.getElementById(id);
const els = {
  date: byId("dateInput"),
  aoi: byId("aoiSelect"),
  product: byId("productSelect"),
  metric: byId("metricSelect"),
  tabs: byId("metricTabs"),
  map: byId("oceanMap"),
  title: byId("mapTitle"),
  subtitle: byId("mapSubtitle"),
  insight: byId("mapInsight"),
  statusTitle: byId("dataStatusTitle"),
  statusText: byId("dataStatusText"),
  source: byId("sourceMode"),
  bars: byId("componentBars"),
  trend: byId("trendCanvas"),
  kpis: byId("pipelineKpis"),
};

const metricsFor = (product) => METRICS.filter((item) => item.productId === product);
const selectedMetric = () => METRICS.find((item) => item.id === state.metric) ?? METRICS[0];
const selectedAoi = () => AOIS.find((item) => item.id === state.aoi) ?? AOIS[0];

function queryFilters(includeDate = true) {
  const filters = {
    aoi: state.aoi,
    product: state.product,
    metric: state.metric,
    resolution: state.aoi === "northwest_pacific" ? 16 : 4,
  };
  if (includeDate) filters.date = state.date;
  return filters;
}

function chooseAvailableDate(dates, preferred) {
  if (!dates.length) return preferred;
  if (dates.includes(preferred)) return preferred;
  const earlier = dates.filter((date) => date <= preferred).at(-1);
  return earlier ?? dates.at(-1);
}

const levelText = (score) => {
  if (score == null || Number.isNaN(Number(score))) return "無資料";
  if (score >= 80) return "非常多";
  if (score >= 60) return "多";
  if (score >= 40) return "中等";
  if (score >= 20) return "少";
  return "非常少";
};

async function loadData() {
  setState({ loading: true, error: null });
  const filters = queryFilters(true);
  try {
    const [grid, summary, trend] = await Promise.all([
      api.getDailyGrid(filters),
      api.getSummary(filters),
      api.getTrend(filters),
    ]);
    setState({ grid: grid.grid, summary, trend: trend.points, loading: false });
  } catch (error) {
    setState({
      grid: [],
      summary: null,
      trend: [],
      error,
      loading: false,
    });
  }
}

async function syncAvailabilityAndLoad() {
  setState({ loading: true, error: null });
  try {
    const availability = await api.getAvailability(queryFilters(false));
    const dates = availability.dates ?? [];
    if (!dates.length) throw new Error("目前選擇的 AOI / 產品 / 指標沒有可用日期");
    const nextDate = chooseAvailableDate(dates, state.date);
    els.date.min = dates[0];
    els.date.max = dates.at(-1);
    els.date.value = nextDate;
    setState({ availableDates: dates, date: nextDate });
    await loadData();
  } catch (error) {
    setState({
      availableDates: [],
      grid: [],
      summary: null,
      trend: [],
      error,
      loading: false,
    });
  }
}

function refreshMetrics() {
  const metrics = metricsFor(state.product);
  if (!metrics.some((item) => item.id === state.metric)) state.metric = metrics[0].id;
  fillSelect(els.metric, metrics, state.metric);
  renderMetricTabs({
    tabs: els.tabs,
    metrics,
    selected: state.metric,
    onChange: (metric) => {
      setState({ metric });
      loadData();
    },
  });
}

function render(current) {
  const metric = selectedMetric();
  const aoi = selectedAoi();
  const resolution = current.grid[0]?.resolution_km
    ?? (current.aoi === "northwest_pacific" ? 16 : 4);
  els.title.textContent = metric.label;
  els.subtitle.textContent = `${current.date}｜${aoi.label}｜約 ${resolution} km`;
  els.source.textContent = APP_CONFIG.dataSource === "api" ? "ICEBERG API" : "MOCK";
  els.insight.textContent = current.metric === "fishing_hours"
    ? "亮色區域代表同日、同海域內漁船活動相對較密集。"
    : "亮色區域代表此指標在同日、同海域內相對較高。";

  if (current.loading) {
    els.statusTitle.textContent = "載入中";
    els.statusText.textContent = "正在查詢相對分數網格。";
  } else if (current.error) {
    els.statusTitle.textContent = "查詢失敗";
    els.statusText.textContent = current.error.message;
  } else if (current.summary) {
    els.statusTitle.textContent = "相對分布已載入";
    els.statusText.textContent = "色彩僅表示同日、同海域內的相對多寡。";
    renderComponentBars(els.bars, [
      { label: "整體分布", value: current.summary.average, text: levelText(current.summary.average) },
      { label: "最高區域", value: current.summary.maximum, text: levelText(current.summary.maximum) },
      {
        label: "資料覆蓋",
        value: current.summary.nasa_coverage * 100,
        text: levelText(current.summary.nasa_coverage * 100),
      },
    ]);
  }

  oceanMap.render({
    grid: current.grid,
    metricMeta: metric,
    mapConfig: aoi,
  });
  drawTrend(els.trend, current.trend, current.date);
}

function init() {
  oceanMap = createOceanMap(els.map);
  els.date.min = APP_CONFIG.dateRange.min;
  els.date.max = APP_CONFIG.dateRange.max;
  els.date.value = state.date;
  fillSelect(els.aoi, AOIS, state.aoi);
  fillSelect(els.product, PRODUCTS, state.product);
  refreshMetrics();

  els.date.onchange = (event) => {
    const nextDate = chooseAvailableDate(state.availableDates, event.target.value);
    els.date.value = nextDate;
    setState({ date: nextDate });
    loadData();
  };
  els.aoi.onchange = (event) => {
    setState({ aoi: event.target.value });
    syncAvailabilityAndLoad();
  };
  els.product.onchange = (event) => {
    setState({ product: event.target.value });
    refreshMetrics();
    syncAvailabilityAndLoad();
  };
  els.metric.onchange = (event) => {
    setState({ metric: event.target.value });
    refreshMetrics();
    syncAvailabilityAndLoad();
  };
  window.onresize = () => {
    oceanMap.invalidateSize();
    render(state);
  };

  renderPipelineKpis(els.kpis);
  subscribe(render);
  syncAvailabilityAndLoad();
}

init();
