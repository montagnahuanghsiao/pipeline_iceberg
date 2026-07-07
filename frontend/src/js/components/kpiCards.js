export function renderPipelineKpis(container) {
  const cards = [
    ["01 / Bronze", "9.2B", "rows / year"],
    ["02 / Silver AOI", "345K", "max cells / day"],
    ["03 / Gold grid", "24,192", "rows / date"],
    ["04 / Runtime", "Spark", "Silver & Gold"],
  ];
  container.innerHTML = cards.map(([label, value, unit]) => `
    <article class="kpi-card">
      <div class="kpi-card__label"><span>${label}</span><span>${unit}</span></div>
      <div class="kpi-card__value">${value}</div>
    </article>
  `).join("");
}
