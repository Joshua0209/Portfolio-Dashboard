// KpiCard — label + value + optional delta with tone class.
// Phase 8 Cycle 56.

import { EM_DASH, int, pct, tone, twd, usd } from "../lib/format";

export type KpiKind = "twd" | "usd" | "pct" | "int";

export interface KpiCardProps {
  label: string;
  value: number | null | undefined;
  kind: KpiKind;
  delta?: number | null;
  decimals?: number;
}

const formatByKind = (
  v: unknown,
  kind: KpiKind,
  decimals?: number,
): string => {
  if (v === null || v === undefined || v === "") return EM_DASH;
  switch (kind) {
    case "twd":
      return twd(v, decimals ?? 0);
    case "usd":
      return usd(v, decimals ?? 2);
    case "pct":
      return pct(v, decimals ?? 2);
    case "int":
      return int(v);
  }
};

export const renderKpiCard = (root: HTMLElement, props: KpiCardProps): void => {
  while (root.firstChild) root.removeChild(root.firstChild);
  root.classList.add("kpi-card");

  const labelEl = document.createElement("div");
  labelEl.className = "kpi-label";
  labelEl.textContent = props.label;
  root.appendChild(labelEl);

  const valueEl = document.createElement("div");
  valueEl.className = "kpi-value";
  valueEl.textContent = formatByKind(props.value, props.kind, props.decimals);
  root.appendChild(valueEl);

  if (props.delta !== undefined && props.delta !== null) {
    const deltaEl = document.createElement("div");
    deltaEl.className = `kpi-delta ${tone(props.delta)}`;
    deltaEl.textContent = pct(props.delta, props.decimals ?? 2);
    root.appendChild(deltaEl);
  }
};
