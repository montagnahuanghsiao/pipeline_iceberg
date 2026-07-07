import { APP_CONFIG } from "../config.js";

function buildApiUrl(path) {
  return new URL(`${APP_CONFIG.apiBaseUrl}${path}`, window.location.origin);
}

async function request(path, params) {
  const url = buildApiUrl(path);
  Object.entries(params).forEach(([key, value]) => url.searchParams.set(key, value));
  const response = await fetch(url, { headers: { accept: "application/json" } });
  if (!response.ok) throw new Error(`API ${response.status}: ${await response.text()}`);
  return response.json();
}
export const httpOceanApi = {
  getAvailability: (filters) => request("/availability", filters),
  getDailyGrid: (filters) => request("/gold/daily-grid", filters),
  getSummary: (filters) => request("/gold/summary", filters),
  getTrend: ({ date: _date, ...filters }) => request("/gold/trend", filters),
};
