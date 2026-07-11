export function renderPipelineKpis(container) {
  // 固定規模展示：依 Gold 4 km 基礎格網與 2020–2024 共 1,827 天計算。
  // 這些值不呼叫 API、不使用動畫，避免因目前選取日期而改變。
  const cards = [
    ["01 / 台灣周邊", "24,192 格", "每日完整格網"],
    ["02 / 西北太平洋", "345,600 格", "每日完整格網"],
    ["03 / 五年累積", "約 6.8 億格", "2020–2024 合計"],
  ];
  container.innerHTML = cards.map(([label, value, unit]) => `
    <article class="kpi-card">
      <div class="kpi-card__label"><span>${label}</span><span>${unit}</span></div>
      <div class="kpi-card__value">${value}</div>
    </article>
  `).join("");
}
