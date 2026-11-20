/* 采集审计台 SPA — vanilla JS, polls every 2s. */
"use strict";

// ---------- status meta: icon + label + chip class (never color alone) ----------
const STATUS = {
  ok:          { cls: "s-good", icon: "✓", label: "正常" },
  done:        { cls: "s-good", icon: "✓", label: "完成" },
  active:      { cls: "s-good", icon: "●", label: "当前版本" },
  parsed:      { cls: "s-good", icon: "✓", label: "已解析" },
  warn:        { cls: "s-warn", icon: "⚠", label: "警告" },
  error:       { cls: "s-crit", icon: "✕", label: "错误" },
  failed:      { cls: "s-crit", icon: "✕", label: "失败" },
  conflicted:  { cls: "s-crit", icon: "⚡", label: "冲突" },
  running:     { cls: "s-run",  icon: "▶", label: "运行中" },
  parsing:     { cls: "s-run",  icon: "▶", label: "解析中" },
  queued:      { cls: "s-mute", icon: "◷", label: "排队中" },
  pending:     { cls: "s-mute", icon: "◷", label: "待处理" },
  received:    { cls: "s-mute", icon: "◷", label: "已接收" },
  cancelled:   { cls: "s-mute", icon: "⊗", label: "已取消" },
  superseded:  { cls: "s-mute", icon: "↺", label: "已被取代" },
};
const sm = s => STATUS[s] || { cls: "s-mute", icon: "?", label: s };

function chip(status, label) {
  const m = sm(status);
  const el = document.createElement("span");
  el.className = `chip ${m.cls}`;
  el.dataset.icon = m.icon;
  el.textContent = label || m.label;
  return el;
}

// ---------- tiny dom helpers (textContent only: labels are untrusted) ----------
function el(tag, cls, text) {
  const e = document.createElement(tag);
  if (cls) e.className = cls;
  if (text !== undefined) e.textContent = text;
  return e;
}
function pre(obj) {
  const p = el("pre", "json");
  p.textContent = JSON.stringify(obj, null, 2);
  return p;
}
function clear(node) { while (node.firstChild) node.removeChild(node.firstChild); }

async function api(path, opts) {
  const r = await fetch(path, opts);
  const body = await r.json().catch(() => ({}));
  if (!r.ok) throw Object.assign(new Error(body.error || r.statusText), { status: r.status, body });
  return body;
}
const post = (path, body) => api(path, {
  method: "POST", headers: { "Content-Type": "application/json" },
  body: JSON.stringify(body || {}),
});

// ---------- state ----------
const state = {
  sources: [], batches: [], batchId: null, lineage: null,
  tab: "graph", sourceFilter: "", recVersion: "", recStatus: "",
  showSuperseded: true, selectedNode: null, corrDetailId: null,
  entDetailId: null, recDetailId: null,
  _cache: {},   // per-pane JSON fingerprint to avoid flicker
};

function fingerprint(key, obj) {
  const s = JSON.stringify(obj);
  if (state._cache[key] === s) return false;
  state._cache[key] = s;
  return true;
}

// ---------- overview tiles ----------
async function renderOverview() {
  const o = await api("/api/overview");
  if (!fingerprint("overview", o)) return;
  const tiles = document.getElementById("tiles");
  clear(tiles);
  const defs = [
    ["来源", o.sources], ["批次", o.batches], ["解析记录", o.records],
    ["活跃实体", o.entities],
    ["进行中纠错", o.corrections_open, o.corrections_open > 0],
    ["冲突纠错", o.corrections_conflicted, o.corrections_conflicted > 0],
    ["排队任务", o.tasks_queued], ["运行任务", o.tasks_running],
  ];
  for (const [k, v, alert] of defs) {
    const t = el("div", "tile" + (alert ? " alert" : ""));
    t.append(el("div", "v", String(v)), el("div", "k", k));
    tiles.append(t);
  }
}

// ---------- sidebar ----------
async function renderSidebar() {
  const [sources, batches] = await Promise.all([api("/api/sources"), api("/api/batches")]);
  state.sources = sources; state.batches = batches;
  const sel = document.getElementById("source-filter");
  if (fingerprint("sources", sources)) {
    const cur = sel.value;
    clear(sel);
    sel.append(new Option("全部来源", ""));
    for (const s of sources) sel.append(new Option(`${s.name} (${s.kind})`, s.id));
    sel.value = cur;
  }
  const list = document.getElementById("batch-list");
  if (!fingerprint("batches", batches) && state.batchId) return;
  clear(list);
  const filtered = batches.filter(b => !state.sourceFilter || b.source_id == state.sourceFilter);
  if (!state.batchId && filtered.length) state.batchId = filtered[0].id;
  for (const b of filtered) {
    const li = el("li", b.id === state.batchId ? "sel" : "");
    const head = el("div", "bl-label");
    head.append(el("span", null, b.label), chip(b.status));
    const sub = el("div", "bl-sub");
    sub.append(el("span", null, b.source_name), el("span", null, `v${b.current_version}`));
    if (b.open_corrections) sub.append(chip("running", `纠错中×${b.open_corrections}`));
    if (b.conflicted_corrections) sub.append(chip("conflicted", `冲突×${b.conflicted_corrections}`));
    li.append(head, sub);
    li.onclick = () => { state.batchId = b.id; state.selectedNode = null; refresh(true); };
    list.append(li);
  }
}

// ---------- batch header ----------
async function renderBatchHead() {
  const host = document.getElementById("batch-head");
  if (!state.batchId) { clear(host); return; }
  const b = await api(`/api/batches/${state.batchId}`);
  if (!fingerprint("batchhead", b)) return;
  clear(host);
  host.append(el("h2", null, b.label));
  host.append(chip(b.status));
  host.append(el("span", "muted", `来源: ${b.source_name} · 当前版本 v${b.current_version} · ${b.versions.length} 个版本`));
  for (const v of b.versions) {
    host.append(chip(v.state === "active" ? "active" : "superseded",
      `v${v.version} · 记录${v.records} · 实体${v.entities}`));
  }
}

// ---------- dependency graph ----------
const LAYERS = ["来源", "批次", "版本", "原始文件", "解析记录", "下游实体"];
const NODE_STATUS_COLOR = {
  good: "var(--status-good)", warn: "var(--status-warning)",
  crit: "var(--status-critical)", run: "var(--accent)", mute: "var(--text-muted)",
};

function buildGraph(lineage) {
  const nodes = [], edges = [];
  const push = (id, layer, label, sub, status, shape, ref) =>
    nodes.push({ id, layer, label, sub, status, shape, ref });
  const src = lineage.source, bat = lineage.batch;
  push("src", 0, src.name, src.kind, "run", "source", { type: "source", id: src.id });
  push("bat", 1, bat.label, `当前 v${bat.current_version}`, "run", "batch", { type: "batch", id: bat.id });
  edges.push(["src", "bat"]);
  for (const v of lineage.versions) {
    const vid = `v${v.id}`;
    const vstat = v.state === "active" ? "good" : "mute";
    push(vid, 2, `v${v.version}`, v.origin === "correction" ? `纠错#${v.correction_id}` : "采集",
         vstat, "version", { type: "version", id: v.id });
    edges.push(["bat", vid, v.state === "superseded"]);
    for (const f of v.files) {
      const fid = `f${f.id}`;
      push(fid, 3, f.filename, `${f.size_bytes}B`, "run", "file", { type: "file", id: f.id });
      edges.push([vid, fid, v.state === "superseded"]);
    }
    for (const r of v.records) {
      const rid = `r${r.id}`;
      const rst = r.status === "ok" ? "good" : r.status === "warn" ? "warn" : "crit";
      push(rid, 4, r.record_key, r.status, rst, "record", { type: "record", id: r.id });
      edges.push([`f${r.raw_file_id}`, rid, v.state === "superseded"]);
    }
    for (const e of v.entities) {
      const eid = `e${e.id}`;
      push(eid, 5, e.entity_key, e.entity_type, e.state === "active" ? "good" : "mute",
           "entity", { type: "entity", id: e.id });
      edges.push([e.parse_record_id ? `r${e.parse_record_id}` : vid, eid,
                  e.state === "superseded"]);
    }
  }
  return { nodes, edges };
}

function shapePath(shape, s) {
  switch (shape) {
    case "source":   // cylinder-ish
      return `M ${-s} ${-s * 0.55} h ${2 * s} v ${1.1 * s} h ${-2 * s} z`;
    case "batch":
      return `M ${-s} ${-s * 0.7} h ${2 * s} v ${1.4 * s} h ${-2 * s} z`;
    case "version":  // diamond
      return `M 0 ${-s} L ${s} 0 L 0 ${s} L ${-s} 0 Z`;
    case "file":
      return `M ${-s * 0.8} ${-s * 0.6} h ${1.6 * s} v ${1.2 * s} h ${-1.6 * s} z`;
    case "entity":   // hexagon
      return `M ${-s} 0 L ${-s / 2} ${-s * 0.87} L ${s / 2} ${-s * 0.87} L ${s} 0 L ${s / 2} ${s * 0.87} L ${-s / 2} ${s * 0.87} Z`;
    default:         // record: circle handled separately
      return null;
  }
}

function renderGraph() {
  const svg = document.getElementById("graph");
  const tip = document.getElementById("graph-tip");
  clear(svg);
  if (!state.lineage) return;
  const { nodes, edges } = buildGraph(state.lineage);

  // layered layout
  const colW = 165, padX = 80, padT = 34, padB = 20;
  const byLayer = Array.from({ length: 6 }, () => []);
  nodes.forEach(n => byLayer[n.layer].push(n));
  const maxRows = Math.max(...byLayer.map(l => l.length), 1);
  const rowH = 46, H = padT + maxRows * rowH + padB;
  byLayer.forEach((layer, i) => {
    const off = padT + (maxRows - layer.length) * rowH / 2 + rowH / 2;
    layer.forEach((n, j) => { n.x = padX + i * colW; n.y = off + j * rowH; });
  });
  const W = padX * 2 + 5 * colW;
  svg.setAttribute("viewBox", `0 0 ${W} ${H}`);
  svg.setAttribute("width", W); svg.setAttribute("height", H);

  const NS = "http://www.w3.org/2000/svg";
  const mk = (tag, attrs) => {
    const e = document.createElementNS(NS, tag);
    for (const k in attrs) e.setAttribute(k, attrs[k]);
    return e;
  };
  // layer labels
  LAYERS.forEach((name, i) => {
    const t = mk("text", { x: padX + i * colW, y: 18, "text-anchor": "middle", class: "layer-label" });
    t.textContent = name;
    svg.append(t);
  });
  const byId = Object.fromEntries(nodes.map(n => [n.id, n]));
  for (const [a, b, faded] of edges) {
    if (!byId[a] || !byId[b]) continue;
    const A = byId[a], B = byId[b];
    const mx = (A.x + B.x) / 2;
    svg.append(mk("path", {
      class: "edge" + (faded ? " superseded" : ""),
      d: `M ${A.x} ${A.y} C ${mx} ${A.y}, ${mx} ${B.y}, ${B.x} ${B.y}`,
    }));
  }
  for (const n of nodes) {
    const g = mk("g", {
      class: "node" + (n.status === "mute" ? " superseded" : "")
        + (state.selectedNode === n.id ? " sel" : ""),
      transform: `translate(${n.x},${n.y})`, tabindex: "0", role: "button",
    });
    const color = NODE_STATUS_COLOR[n.status];
    const big = n.layer <= 1;
    const s = big ? 26 : n.shape === "record" ? 9 : 15;
    if (n.shape === "record") {
      g.append(mk("circle", { class: "shape", r: s, style: `stroke:${color}` }));
    } else {
      g.append(mk("path", { class: "shape", d: shapePath(n.shape, s), style: `stroke:${color}` }));
    }
    // status icon inside the mark
    const icon = mk("text", { "text-anchor": "middle", dy: "0.32em", "font-size": big ? 13 : 9 });
    icon.textContent = { good: "✓", warn: "⚠", crit: "✕", run: "▶", mute: "↺" }[n.status];
    icon.style.fill = color;
    g.append(icon);
    const label = mk("text", { y: s + 13, "text-anchor": "middle" });
    label.textContent = n.label.length > 16 ? n.label.slice(0, 15) + "…" : n.label;
    const sub = mk("text", { y: s + 25, "text-anchor": "middle", class: "sub" });
    sub.textContent = n.sub || "";
    g.append(label, sub);
    // generous transparent hit target (>=24px)
    g.append(mk("circle", { class: "hit", r: Math.max(24, s + 8) }));
    const show = () => {
      clear(tip);
      tip.append(el("div", "tt-title", n.label));
      tip.append(el("div", "tt-row", `类型: ${LAYERS[n.layer]} · ${n.sub}`));
      tip.append(el("div", "tt-row", `状态: ${sm(n.status === "good" ? "ok" : n.status === "mute" ? "superseded" : n.status === "warn" ? "warn" : n.status === "crit" ? "error" : "running").label}`));
      tip.hidden = false;
      const wrap = svg.parentElement.getBoundingClientRect();
      tip.style.left = Math.min(n.x + 20, wrap.width - 180) + "px";
      tip.style.top = Math.max(n.y - 10, 8) + "px";
    };
    g.addEventListener("pointerenter", show);
    g.addEventListener("focus", show);
    g.addEventListener("pointerleave", () => { tip.hidden = true; });
    g.addEventListener("blur", () => { tip.hidden = true; });
    g.addEventListener("click", () => { state.selectedNode = n.id; renderGraph(); showNodeDetail(n); });
    g.addEventListener("keydown", ev => { if (ev.key === "Enter") { state.selectedNode = n.id; renderGraph(); showNodeDetail(n); } });
    svg.append(g);
  }
  // legend
  const lg = document.getElementById("graph-legend");
  if (!lg.childElementCount) {
    const items = [
      ["good", "正常/当前"], ["warn", "警告"], ["crit", "错误"],
      ["run", "流转中"], ["mute", "已取代(原结论保留)"],
    ];
    for (const [k, label] of items) {
      const s = el("span", "lg");
      const sw = el("span", "sw");
      sw.style.borderColor = NODE_STATUS_COLOR[k];
      s.append(sw, el("span", null, label));
      lg.append(s);
    }
  }
}

async function showNodeDetail(n) {
  const pane = document.getElementById("node-detail");
  clear(pane);
  pane.append(el("p", "muted", "加载中…"));
  try {
    if (n.ref.type === "record") {
      const r = await api(`/api/records/${n.ref.id}`);
      renderRecordDetail(pane, r);
    } else if (n.ref.type === "entity") {
      const e = await api(`/api/entities/${n.ref.id}`);
      renderEntityDetail(pane, e);
    } else {
      clear(pane);
      pane.append(el("h3", null, `${LAYERS[n.layer]} · ${n.label}`));
      const dl = el("dl", "kv");
      for (const [k, v] of [["类型", LAYERS[n.layer]], ["标识", n.label], ["说明", n.sub || "-"]]) {
        dl.append(el("dt", null, k), el("dd", null, String(v)));
      }
      pane.append(dl);
    }
  } catch (e) {
    clear(pane);
    pane.append(el("p", null, `加载失败: ${e.message}`));
  }
}

function provChain(host, parts) {
  const row = el("div", "prov-chain");
  parts.forEach((p, i) => {
    if (i) row.append(el("span", "arrow", "→"));
    row.append(el("span", "node-pill", p));
  });
  host.append(row);
}

function renderRecordDetail(pane, r) {
  clear(pane);
  pane.append(el("h3", null, `解析记录 ${r.record_key}`));
  provChain(pane, [
    `来源 ${r.provenance.source.name}`, `批次 ${r.provenance.batch.label}`,
    `v${r.provenance.version.version}${r.provenance.version.state === "superseded" ? "(已取代)" : ""}`,
    `文件 ${r.provenance.file.filename}`, `记录 ${r.record_key}`,
  ]);
  const dl = el("dl", "kv");
  dl.append(el("dt", null, "状态"), el("dd", null, ""));
  dl.lastChild.append(chip(r.status));
  if (r.error_msg) { dl.append(el("dt", null, "错误信息"), el("dd", null, r.error_msg)); }
  dl.append(el("dt", null, "文件校验"), el("dd", null, r.provenance.file.checksum));
  pane.append(dl, el("h3", null, "记录内容"), pre(r.payload));
  if (r.entities.length) {
    pane.append(el("h3", null, `下游实体 (${r.entities.length})`));
    const t = el("table");
    t.append(thead(["ID", "类型", "键", "状态"]));
    const tb = el("tbody");
    for (const e of r.entities) {
      const tr = el("tr", e.state === "superseded" ? "row-superseded" : "");
      const c = el("td"); c.append(chip(e.state));
      tr.append(el("td", null, `#${e.id}`), el("td", null, e.entity_type),
                el("td", null, e.entity_key), c);
      tb.append(tr);
    }
    t.append(tb); pane.append(t);
  }
}

function renderEntityDetail(pane, e) {
  clear(pane);
  pane.append(el("h3", null, `实体 ${e.entity_key}`));
  const parts = [`来源 ${e.provenance.source.name}`, `批次 ${e.provenance.batch.label}`,
                 `v${e.provenance.version.version}${e.provenance.version.state === "superseded" ? "(已取代,原结论)" : ""}`];
  if (e.parse_record) parts.push(`记录 ${e.parse_record.record_key}`);
  parts.push(`实体 ${e.entity_key}`);
  provChain(pane, parts);
  const dl = el("dl", "kv");
  dl.append(el("dt", null, "类型"), el("dd", null, e.entity_type),
            el("dt", null, "状态"), el("dd", null, ""));
  dl.lastChild.append(chip(e.state, e.state === "active" ? "生效中" : "已被取代(原结论保留)"));
  pane.append(dl, el("h3", null, "实体内容"), pre(e.payload));
  if (e.parse_record) {
    pane.append(el("h3", null, "来源记录内容"), pre(e.parse_record.payload));
  }
}

// ---------- records tab ----------
function renderRecords() {
  const table = document.getElementById("records-table");
  clear(table);
  if (!state.lineage) return;
  const vf = document.getElementById("rec-version-filter");
  const curV = vf.value;
  clear(vf); vf.append(new Option("全部", ""));
  for (const v of state.lineage.versions)
    vf.append(new Option(`v${v.version}${v.state === "superseded" ? " (已取代)" : ""}`, v.id));
  vf.value = curV;
  table.append(thead(["ID", "版本", "记录键", "状态", "错误信息", "文件"]));
  const tb = el("tbody");
  for (const v of state.lineage.versions) {
    if (state.recVersion && v.id != state.recVersion) continue;
    const fileById = Object.fromEntries(v.files.map(f => [f.id, f.filename]));
    for (const r of v.records) {
      if (state.recStatus && r.status !== state.recStatus) continue;
      const tr = el("tr", "clickable" + (r.status === "error" ? " row-error" : "")
        + (v.state === "superseded" ? " row-superseded" : ""));
      const cStatus = el("td"); cStatus.append(chip(r.status));
      tr.append(el("td", null, `#${r.id}`), el("td", null, `v${v.version}`),
                el("td", null, r.record_key), cStatus,
                el("td", null, r.error_msg || ""), el("td", null, fileById[r.raw_file_id] || ""));
      tr.onclick = async () => {
        const r2 = await api(`/api/records/${r.id}`);
        renderRecordDetail(document.getElementById("record-detail"), r2);
      };
      tb.append(tr);
    }
  }
  table.append(tb);
}

// ---------- entities tab ----------
function renderEntities() {
  const table = document.getElementById("entities-table");
  clear(table);
  if (!state.lineage) return;
  table.append(thead(["ID", "版本", "类型", "实体键", "状态", "来源记录"]));
  const tb = el("tbody");
  for (const v of state.lineage.versions) {
    for (const e of v.entities) {
      if (!state.showSuperseded && e.state === "superseded") continue;
      const tr = el("tr", "clickable" + (e.state === "superseded" ? " row-superseded" : ""));
      const cState = el("td"); cState.append(chip(e.state, e.state === "active" ? "生效中" : "原结论"));
      tr.append(el("td", null, `#${e.id}`), el("td", null, `v${v.version}`),
                el("td", null, e.entity_type), el("td", null, e.entity_key), cState,
                el("td", null, e.parse_record_id ? `记录#${e.parse_record_id}` : "(聚合)"));
      tr.onclick = async () => {
        const e2 = await api(`/api/entities/${e.id}`);
        renderEntityDetail(document.getElementById("entity-detail"), e2);
      };
      tb.append(tr);
    }
  }
  table.append(tb);
}

// ---------- corrections tab ----------
function corrItemRow(item) {
  const row = el("div", "corr-item");
  const sel = el("select");
  for (const [v, l] of [["fix", "修正"], ["drop", "剔除"], ["add", "补录"]])
    sel.append(new Option(l, v));
  sel.value = (item && item.action) || "fix";
  const key = el("input", "k"); key.type = "text"; key.placeholder = "record_key";
  key.value = (item && item.record_key) || "";
  const patch = el("input", "p"); patch.type = "text";
  patch.placeholder = 'patch JSON, 例 {"payload":{"amount":150},"status":"ok"}';
  patch.value = item && item.patch ? JSON.stringify(item.patch) : "";
  const rm = el("button", "btn small danger", "✕");
  rm.onclick = () => row.remove();
  const sync = () => { patch.disabled = sel.value === "drop"; if (sel.value === "drop") patch.value = ""; };
  sel.onchange = sync; sync();
  row.append(sel, key, patch, rm);
  return row;
}

async function submitCorrection() {
  const items = [];
  for (const row of document.querySelectorAll("#corr-items .corr-item")) {
    const [sel, key, patch] = row.querySelectorAll("select,input.k,input.p");
    if (!key.value.trim()) continue;
    let p = {};
    if (sel.value !== "drop" && patch.value.trim()) {
      try { p = JSON.parse(patch.value); }
      catch { alert(`记录 ${key.value} 的 patch 不是合法 JSON`); return; }
    }
    items.push({ action: sel.value, record_key: key.value.trim(), patch: p });
  }
  if (!items.length) { alert("至少一条纠错项"); return; }
  const reason = document.getElementById("corr-reason").value.trim();
  try {
    const r = await post(`/api/batches/${state.batchId}/corrections`, { reason, items });
    document.getElementById("corr-reason").value = "";
    clear(document.getElementById("corr-items"));
    document.getElementById("corr-items").append(corrItemRow());
    state.corrDetailId = r.correction_id;
    refresh(true);
  } catch (e) { alert(`提交失败: ${e.message}`); }
}

async function renderCorrections() {
  const table = document.getElementById("corrections-table");
  const list = await api(`/api/corrections?batch_id=${state.batchId}`);
  const b = state.batches.find(x => x.id === state.batchId);
  document.getElementById("corr-base-note").textContent =
    b ? `(基于当前版本 v${b.current_version})` : "";
  clear(table);
  table.append(thead(["ID", "基于版本", "状态", "原因", "发起", "时间", "操作"]));
  const tb = el("tbody");
  for (const c of list) {
    const tr = el("tr", "clickable");
    const cSt = el("td"); cSt.append(chip(c.status));
    const cAct = el("td");
    if (["pending", "running"].includes(c.status)) {
      const btn = el("button", "btn small danger", "取消");
      btn.onclick = ev => { ev.stopPropagation(); post(`/api/corrections/${c.id}/cancel`).then(() => refresh(true)); };
      cAct.append(btn);
    }
    const info = el("button", "btn small", "详情");
    info.onclick = ev => { ev.stopPropagation(); state.corrDetailId = c.id; renderCorrectionDetail(); };
    cAct.append(info);
    tr.append(el("td", null, `#${c.id}`),
              el("td", null, `v${c.base_version}${c.result_version ? " → v" + c.result_version : ""}`),
              cSt, el("td", null, c.reason || "-"), el("td", null, c.created_by),
              el("td", null, (c.created_at || "").slice(5, 19)), cAct);
    tr.onclick = () => { state.corrDetailId = c.id; renderCorrectionDetail(); };
    tb.append(tr);
  }
  table.append(tb);
  if (state.corrDetailId) renderCorrectionDetail();
}

async function renderCorrectionDetail() {
  const pane = document.getElementById("correction-detail");
  const c = await api(`/api/corrections/${state.corrDetailId}`);
  clear(pane);
  const h = el("h3", null, `纠错 #${c.id} · 批次 ${c.batch_label} · `);
  h.append(chip(c.status));
  pane.append(h);
  if (c.status === "conflicted" && c.conflict_reason) {
    const box = el("div", "conflict-box");
    box.append(el("b", null, "冲突原因: "), document.createTextNode(c.conflict_reason));
    pane.append(box);
  }
  const dl = el("dl", "kv");
  dl.append(el("dt", null, "基于版本"), el("dd", null, `v${c.base_version}`),
            el("dt", null, "生成版本"), el("dd", null, c.result_version ? `v${c.result_version}` : "-"),
            el("dt", null, "发起人"), el("dd", null, c.created_by),
            el("dt", null, "完成时间"), el("dd", null, c.finished_at || "-"));
  pane.append(dl);
  pane.append(el("h3", null, "纠错项"));
  const t = el("table");
  t.append(thead(["动作", "记录键", "Patch"]));
  const tb = el("tbody");
  for (const it of c.items) {
    const tr = el("tr");
    tr.append(el("td", null, { fix: "修正", drop: "剔除", add: "补录" }[it.action]),
              el("td", null, it.record_key),
              el("td", null, Object.keys(it.patch).length ? JSON.stringify(it.patch) : "-"));
    tb.append(tr);
  }
  t.append(tb); pane.append(t);
  if (c.impact && c.impact.new_version) {
    const box = el("div", "impact-box");
    box.append(el("b", null, "影响范围: "));
    box.append(document.createTextNode(
      `影响记录键 [${(c.impact.affected_record_keys || []).join(", ") || "无"}] · ` +
      `复制记录 ${c.impact.records_copied} · 修正 ${c.impact.records_fixed} · ` +
      `剔除 ${c.impact.records_dropped} · 补录 ${c.impact.records_added} · ` +
      `原结论实体保留 ${c.impact.entities_superseded} 条`));
    pane.append(box);
  }
  if (c.rerun_tasks && c.rerun_tasks.length) {
    pane.append(el("h3", null, "下游重跑结果"));
    const rt = el("table");
    rt.append(thead(["任务", "类型", "状态", "结果", "耗时"]));
    const rb = el("tbody");
    for (const t2 of c.rerun_tasks) {
      const tr = el("tr");
      const cs = el("td"); cs.append(chip(t2.status));
      const dur = t2.started_at && t2.finished_at
        ? `${((new Date(t2.finished_at) - new Date(t2.started_at)) / 1000).toFixed(1)}s` : "-";
      tr.append(el("td", null, `#${t2.id}`), el("td", null, t2.task_type), cs,
                el("td", null, JSON.stringify(t2.result)), el("td", "num", dur));
      rb.append(tr);
    }
    rt.append(rb); pane.append(rt);
  }
}

// ---------- tasks tab ----------
async function renderTasks() {
  const table = document.getElementById("tasks-table");
  const list = await api(`/api/tasks?batch_id=${state.batchId}`);
  if (!fingerprint("tasks", list)) return;
  clear(table);
  table.append(thead(["ID", "类型", "版本", "状态", "依赖", "取代", "结果", "开始", "结束", "操作"]));
  const tb = el("tbody");
  for (const t of list) {
    const tr = el("tr");
    const cs = el("td"); cs.append(chip(t.status));
    const ca = el("td");
    if (["queued", "running"].includes(t.status)) {
      const btn = el("button", "btn small danger", t.status === "queued" ? "取消" : "请求取消");
      btn.onclick = () => post(`/api/tasks/${t.id}/cancel`).then(() => refresh(true));
      ca.append(btn);
    }
    tr.append(el("td", null, `#${t.id}`), el("td", null, t.task_type),
              el("td", null, t.version ? `v${t.version}` : "-"), cs,
              el("td", null, (t.depends_on || []).map(d => "#" + d).join(",") || "-"),
              el("td", null, t.supersedes ? `#${t.supersedes}` : "-"),
              el("td", null, JSON.stringify(t.result).slice(0, 80)),
              el("td", null, (t.started_at || "-").slice(11, 19)),
              el("td", null, (t.finished_at || "-").slice(11, 19)), ca);
    tb.append(tr);
  }
  table.append(tb);
}

// ---------- events tab ----------
async function renderEvents() {
  const table = document.getElementById("events-table");
  const list = await api("/api/events?limit=80");
  if (!fingerprint("events", list)) return;
  clear(table);
  table.append(thead(["时间", "类别", "事件"]));
  const tb = el("tbody");
  for (const e of list) {
    const tr = el("tr");
    tr.append(el("td", null, e.ts.slice(5, 23)), el("td", null, e.kind),
              el("td", null, e.message));
    tb.append(tr);
  }
  table.append(tb);
}

function thead(cols) {
  const t = el("thead"), tr = el("tr");
  for (const c of cols) tr.append(el("th", null, c));
  t.append(tr);
  return t;
}

// ---------- refresh orchestration ----------
async function refreshLineage() {
  if (!state.batchId) return;
  const lin = await api(`/api/batches/${state.batchId}/lineage`);
  if (fingerprint("lineage", lin)) {
    state.lineage = lin;
    renderGraph();
    renderRecords();
    renderEntities();
  } else {
    state.lineage = lin;
  }
}

async function refresh(force) {
  if (force) state._cache = {};
  try {
    await renderOverview();
    await renderSidebar();
    await renderBatchHead();
    await refreshLineage();
    if (state.tab === "corrections") await renderCorrections();
    if (state.tab === "tasks") await renderTasks();
    if (state.tab === "events") await renderEvents();
  } catch (e) { /* transient during reseed */ }
}

// ---------- wiring ----------
document.querySelectorAll(".tab").forEach(btn => {
  btn.onclick = () => {
    document.querySelectorAll(".tab").forEach(b => b.classList.toggle("active", b === btn));
    document.querySelectorAll(".tabpane").forEach(p =>
      p.classList.toggle("active", p.id === `tab-${btn.dataset.tab}`));
    state.tab = btn.dataset.tab;
    refresh(true);
  };
});
document.getElementById("source-filter").onchange = ev => {
  state.sourceFilter = ev.target.value; state.batchId = null; refresh(true);
};
document.getElementById("rec-version-filter").onchange = ev => {
  state.recVersion = ev.target.value; renderRecords();
};
document.getElementById("rec-status-filter").onchange = ev => {
  state.recStatus = ev.target.value; renderRecords();
};
document.getElementById("ent-show-superseded").onchange = ev => {
  state.showSuperseded = ev.target.checked; renderEntities();
};
document.getElementById("corr-add-item").onclick = () =>
  document.getElementById("corr-items").append(corrItemRow());
document.getElementById("corr-submit").onclick = submitCorrection;
document.getElementById("btn-reseed").onclick = async () => {
  if (!confirm("重置数据库并重新装载演示数据?")) return;
  await post("/api/seed");
  state.batchId = null; state.corrDetailId = null;
  refresh(true);
};
document.getElementById("btn-theme").onclick = () => {
  const cur = document.documentElement.dataset.theme;
  const next = cur === "dark" ? "light" : "dark";
  document.documentElement.dataset.theme = next;
  localStorage.setItem("theme", next);
};
document.documentElement.dataset.theme = localStorage.getItem("theme") || "";
document.getElementById("corr-items").append(corrItemRow());

refresh(true);
setInterval(refresh, 2000);
