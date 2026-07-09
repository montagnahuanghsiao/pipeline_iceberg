import { AOIS, METRICS } from "../config.js";

function noise(row, col, day, salt = 0) {
  const value = Math.sin(row * 12.9898 + col * 78.233 + day * 37.719 + salt) * 43758.5453;
  return value - Math.floor(value);
}
function makeRawValue(metric, nx, ny, row, col, day) {
  const n = noise(row, col, day, metric.length);
  const current = Math.exp(-((nx - 0.72) ** 2) / 0.025) * Math.exp(-((ny - 0.52) ** 2) / 0.16);
  if (metric === "fishing_hours") return n < 0.08 ? 0 : n * 35 + current * 65;
  if (metric === "sea_temperature") return Math.min(100, ny * 65 + n * 35);
  if (metric === "chlor_a") return Math.min(100, current * 75 + n * 25);
  if (metric === "poc") return Math.min(100, current * 68 + n * 32);
  if (metric === "nflh") return Math.min(100, current * 72 + n * 28);
  return Math.min(100, 20 + current * 60 + n * 20);
}
function buildGrid({ date, aoi, product, metric, resolution = 4 }) {
  const area = AOIS.find((item) => item.id === aoi) ?? AOIS[0];
  const day = Number(date.slice(-2));
  const rowMin = Math.floor((90 - area.maxLat) * area.gridScale);
  const rowMax = Math.floor((90 - area.minLat) * area.gridScale) - 1;
  const colMin = Math.floor((area.minLon + 180) * area.gridScale);
  const colMax = Math.floor((area.maxLon + 180) * area.gridScale) - 1;
  const step = Math.max(1, resolution / 4);
  const grid = [];
  for (let row = rowMin; row <= rowMax; row += step) {
    for (let col = colMin; col <= colMax; col += step) {
      const nx = (col - colMin) / Math.max(1, colMax - colMin);
      const ny = (row - rowMin) / Math.max(1, rowMax - rowMin);
      const rawValue = makeRawValue(metric, nx, ny, row, col, day);
      grid.push({
        date,
        product,
        metric,
        grid_id: `nasa4km_r${String(row).padStart(4, "0")}_c${String(col).padStart(4, "0")}`,
        grid_row: row,
        grid_col: col,
        rawValue,
        data_coverage: Number((0.68 + noise(col, row, day) * 0.3).toFixed(3)),
      });
    }
  }
  const rankable = grid
    .filter((cell) => metric !== "fishing_hours" || cell.rawValue > 0)
    .sort((left, right) => left.rawValue - right.rawValue);
  const scores = new Map(
    rankable.map((cell, index) => [
      cell.grid_id,
      rankable.length === 1 ? 100 : (index / (rankable.length - 1)) * 100,
    ]),
  );
  return grid.map(({ rawValue, ...cell }) => {
    const score = metric === "fishing_hours" && rawValue <= 0 ? 0 : scores.get(cell.grid_id);
    const relativeScore = Number(score.toFixed(3));
    return {
      ...cell,
      resolution_km: resolution,
      relative_score: relativeScore,
      display_level: relativeScore >= 80 ? "very_high"
        : relativeScore >= 60 ? "high"
          : relativeScore >= 40 ? "medium"
            : relativeScore >= 20 ? "low"
              : "very_low",
      value_source: "observed",
      value: relativeScore,
    };
  });
}
export const mockOceanApi = {
  async getAvailability(filters) {
    return {
      ...filters,
      dates: Array.from({ length: 31 }, (_, index) => `2024-12-${String(index + 1).padStart(2, "0")}`),
      partitions: [],
    };
  },
  async getDailyGrid(filters) { return { ...filters, source: "mock", grid: buildGrid(filters) }; },
  async getSummary(filters) {
    const grid = buildGrid(filters); const values = grid.map((cell) => cell.value);
    const high = (threshold) => grid.filter((cell) => cell.value >= threshold).length / grid.length;
    const fishingHours = grid.reduce((sum, cell) => sum + (filters.metric === "fishing_hours" ? cell.value : makeRawValue("fishing_hours", 0.5, 0.5, cell.grid_row, cell.grid_col, Number(filters.date.slice(-2)))), 0);
    return {
      ...filters,
      partition: `event_date=${filters.date}/aoi_id=${filters.aoi}`,
      data_coverage: grid.reduce((sum, cell) => sum + cell.data_coverage, 0) / grid.length,
      nasa_coverage: grid.reduce((sum, cell) => sum + cell.data_coverage, 0) / grid.length,
      cells: grid.length,
      average: values.reduce((a, b) => a + b, 0) / values.length,
      chlor_a_avg: 1.7 + noise(1, 2, Number(filters.date.slice(-2))) * 2.2,
      sea_temperature_avg: 23 + noise(2, 3, Number(filters.date.slice(-2))) * 5,
      ocean_productivity_avg: 2.6 + noise(3, 4, Number(filters.date.slice(-2))) * 1.1,
      sustainability_pressure_avg: 8 + noise(4, 5, Number(filters.date.slice(-2))) * 9,
      sustainability_pressure_p90: 20 + noise(5, 6, Number(filters.date.slice(-2))) * 22,
      fishing_hours_total: fishingHours,
      active_cell_ratio: 0.34,
      high_activity_cell_ratio: high(80),
      high_productivity_cell_ratio: 0.2 + noise(6, 7, Number(filters.date.slice(-2))) * 0.18,
      high_pressure_cell_ratio: 0.12 + noise(7, 8, Number(filters.date.slice(-2))) * 0.16,
      share_of_all_fishing_hours: filters.aoi === "taiwan" ? 0.64 : 0.36,
      fishing_hours_7d_avg: fishingHours * 0.92,
      sustainability_pressure_7d_avg: 11.5,
      components: [
        { label: "高生產力格", value: 28, text: "28%" },
        { label: "高捕魚活動", value: 18, text: "18%" },
        { label: "高永續壓力", value: 14, text: "14%" },
      ],
    };
  },
  async getStatusDistribution(filters) {
    return {
      ...filters,
      classes: [
        { status_class: "high_productivity_low_fishing", cell_ratio: 0.26, cell_count: 260, fishing_hours_total: 180 },
        { status_class: "high_productivity_high_fishing", cell_ratio: 0.14, cell_count: 140, fishing_hours_total: 620 },
        { status_class: "low_productivity_high_fishing", cell_ratio: 0.09, cell_count: 90, fishing_hours_total: 410 },
        { status_class: "low_productivity_low_fishing", cell_ratio: 0.51, cell_count: 510, fishing_hours_total: 90 },
      ],
    };
  },
  async getTrend(filters) {
    return { ...filters, points: Array.from({ length: 31 }, (_, index) => ({ date: `2024-12-${String(index + 1).padStart(2, "0")}`, value: 48 + Math.sin(index / 3) * 17 + noise(index, 1, 4) * 12 })) };
  },
};
