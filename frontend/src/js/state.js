import { APP_CONFIG } from "./config.js";
const listeners = new Set();
export const state = {
  date: APP_CONFIG.defaultDate, aoi: APP_CONFIG.defaultAoi,
  product: APP_CONFIG.defaultProduct, metric: APP_CONFIG.defaultMetric,
  availableDates: [],
  grid: [], summary: null, trend: [], loading: false, error: null,
};
export function setState(patch) { Object.assign(state, patch); listeners.forEach((listener) => listener(state)); }
export function subscribe(listener) { listeners.add(listener); return () => listeners.delete(listener); }
