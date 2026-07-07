export function fillSelect(select, items, selected) {
  select.innerHTML = items.map((item) => `<option value="${item.id}" ${item.id === selected ? "selected" : ""}>${item.label}</option>`).join("");
}
export function renderMetricTabs({ tabs, metrics, selected, onChange }) {
  tabs.innerHTML = metrics.map((metric) => `<button class="segmented__item ${metric.id === selected ? "is-active" : ""}" data-metric="${metric.id}">${metric.shortLabel}</button>`).join("");
  tabs.onclick = (event) => { const button = event.target.closest("[data-metric]"); if (button) onChange(button.dataset.metric); };
}
