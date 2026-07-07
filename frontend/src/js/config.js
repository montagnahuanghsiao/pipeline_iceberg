const runtimeConfig = window.OCEAN_CONFIG ?? {};

export const APP_CONFIG = {
  dataSource: runtimeConfig.dataSource ?? "mock",
  apiBaseUrl: runtimeConfig.apiBaseUrl ?? "http://localhost:8000/api/v1",
  defaultDate: "2024-12-12",
  defaultAoi: "taiwan",
  defaultProduct: "SST",
  defaultMetric: "sea_temperature",
  dateRange: { min: "2024-12-01", max: "2024-12-31" },
};

export const AOIS = [
  { id: "taiwan", label: "台灣周邊", minLat: 20, maxLat: 27, minLon: 118, maxLon: 124, gridScale: 24 },
  { id: "northwest_pacific", label: "西北太平洋", minLat: 15, maxLat: 35, minLon: 105, maxLon: 135, gridScale: 24 },
];

export const METRICS = [
  { productId: "CHL", id: "chlor_a", label: "葉綠素相對高低", shortLabel: "CHL", unit: "", domain: [0, 100], higherIsBetter: true },
  { productId: "SST", id: "sea_temperature", label: "海溫相對高低", shortLabel: "海溫", unit: "", domain: [0, 100], higherIsBetter: true },
  { productId: "PRODUCTIVITY", id: "ocean_productivity_score", label: "海洋生產力相對高低", shortLabel: "生產力", unit: "", domain: [0, 100], higherIsBetter: true },
  { productId: "SUSTAINABILITY", id: "sustainability_pressure", label: "永續壓力相對高低", shortLabel: "永續壓力", unit: "", domain: [0, 100], higherIsBetter: true },
  { productId: "GFW", id: "fishing_hours", label: "捕魚時數相對高低", shortLabel: "捕魚時數", unit: "", domain: [0, 100], higherIsBetter: true },
];

export const PRODUCTS = [
  { id: "CHL", label: "葉綠素濃度" },
  { id: "SST", label: "海溫" },
  { id: "PRODUCTIVITY", label: "海洋生產力分數" },
  { id: "SUSTAINABILITY", label: "永續壓力" },
  { id: "GFW", label: "捕魚時數熱力圖" },
];
