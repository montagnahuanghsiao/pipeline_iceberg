import { APP_CONFIG } from "../config.js";
import { httpOceanApi } from "./httpOceanApi.js";
import { mockOceanApi } from "./mockOceanApi.js?v=0.4.5";

export function createOceanApi() {
  if (APP_CONFIG.dataSource === "api") return httpOceanApi;
  return mockOceanApi;
}
