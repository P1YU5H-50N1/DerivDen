import marimo as mo


def build_hft_dashboard(
    payouts_df,
    synth_worker,
    deriv_listner,
    forecast_fetch_interval: int = 600,
):
    css = """
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600&display=swap');
.terminal{
  --t-bg:#0a0c0f;--t-bg1:#0f1218;--t-bg2:#151a22;--t-bg3:#1c2330;
  --t-border:rgba(56,180,120,.18);--t-border2:rgba(56,180,120,.35);
  --t-green:#38b478;--t-green-dim:rgba(56,180,120,.55);--t-green-glow:rgba(56,180,120,.08);
  --t-amber:#e8a030;--t-red:#e05050;
  --t-text:#c8d4c0;--t-text-dim:#5e6e60;--t-text-bright:#e8f0e0;
  --t-mono:'IBM Plex Mono',monospace;
  background:var(--t-bg);font-family:var(--t-mono);color:var(--t-text);
  height:100vh;display:flex;flex-direction:column;overflow:hidden;
}
.terminal *{box-sizing:border-box;margin:0;padding:0}
.terminal .topbar{display:flex;align-items:center;justify-content:space-between;padding:7px 16px;border-bottom:1px solid var(--t-border);background:var(--t-bg1);flex-shrink:0}
.terminal .logo{display:flex;align-items:center;gap:8px;font-size:11px;font-weight:600;letter-spacing:.12em;color:var(--t-green);text-transform:uppercase}
.terminal .logo-dot{width:7px;height:7px;border-radius:50%;background:var(--t-green);animation:t-pulse 1.8s ease-in-out infinite}
@keyframes t-pulse{0%,100%{opacity:1}50%{opacity:.3}}
.terminal .status-pill{font-size:9px;letter-spacing:.1em;padding:2px 8px;border-radius:2px;text-transform:uppercase}
.terminal .status-live{background:rgba(56,180,120,.15);color:var(--t-green);border:1px solid var(--t-border2)}
.terminal .status-ws{background:rgba(232,160,48,.12);color:var(--t-amber);border:1px solid rgba(232,160,48,.3)}
.terminal .stat-ribbon{display:flex;align-items:center;border-bottom:1px solid var(--t-border);background:var(--t-bg1);flex-shrink:0;overflow-x:auto}
.terminal .stat-chip{display:flex;align-items:center;gap:7px;padding:5px 13px;border-right:1px solid var(--t-border);white-space:nowrap}
.terminal .stat-chip:last-child{border-right:none;margin-left:auto}
.terminal .chip-label{font-size:9px;letter-spacing:.12em;text-transform:uppercase;color:var(--t-text-dim)}
.terminal .chip-val{font-size:13px;font-weight:600;font-variant-numeric:tabular-nums;color:var(--t-text-bright)}
.terminal .chip-val.pos{color:var(--t-green)}
.terminal .chip-val.neg{color:var(--t-red)}
.terminal .body{display:flex;flex:1;min-height:0}
.terminal .chain-panel{display:flex;flex-direction:column;flex:1;min-width:0;border-right:1px solid var(--t-border)}
.terminal .panel-header{display:flex;align-items:center;justify-content:space-between;padding:6px 14px;border-bottom:1px solid var(--t-border);background:var(--t-bg1);font-size:9px;letter-spacing:.12em;text-transform:uppercase;color:var(--t-green-dim);flex-shrink:0}
.terminal .panel-tag{font-size:9px;color:var(--t-text-dim);background:var(--t-bg2);border:1px solid var(--t-border);padding:1px 7px;border-radius:1px;letter-spacing:.08em}
.terminal .chain-scroll{flex:1;overflow:auto;min-height:0}
.terminal table.data-table{width:100%;border-collapse:collapse;font-size:11px}
.terminal .data-table thead th{position:sticky;top:0;z-index:2;padding:5px 10px;text-align:right;font-size:9px;letter-spacing:.1em;text-transform:uppercase;color:var(--t-text-dim);border-bottom:1px solid var(--t-border);background:var(--t-bg2);font-weight:500}
.terminal .data-table thead th:first-child{text-align:left}
.terminal .data-table tbody tr{border-bottom:1px solid rgba(56,180,120,.07);transition:background .12s}
.terminal .data-table tbody tr:hover{background:var(--t-green-glow)}
.terminal .data-table tbody td{padding:5px 10px;text-align:right;font-variant-numeric:tabular-nums;color:var(--t-text)}
.terminal .data-table tbody td:first-child{text-align:left;color:var(--t-text-dim)}
.terminal .val-pos{color:var(--t-green)}
.terminal .val-neg{color:var(--t-red)}
.terminal .val-warn{color:var(--t-amber)}
.terminal .val-muted{color:var(--t-text-dim);font-size:10px}
.terminal .val-bright{color:var(--t-text-bright)}
.terminal .edge-bar-wrap{display:flex;align-items:center;gap:5px;justify-content:flex-end}
.terminal .edge-bar{width:40px;height:3px;border-radius:1px;background:var(--t-bg3);overflow:hidden}
.terminal .edge-bar-fill{height:100%;border-radius:1px}
.terminal .fill-pos{background:var(--t-green)}
.terminal .fill-neg{background:var(--t-red)}
.terminal .fill-warn{background:var(--t-amber)}
.terminal .side-panel{width:250px;flex-shrink:0;display:flex;flex-direction:column;overflow-y:auto;background:var(--t-bg)}
.terminal .mini-card{padding:10px 14px;border-bottom:1px solid var(--t-border)}
.terminal .mini-label{font-size:9px;letter-spacing:.12em;text-transform:uppercase;color:var(--t-text-dim);margin-bottom:5px}
.terminal .ws-log{font-size:10px;line-height:1.55;color:var(--t-text-dim);font-family:var(--t-mono)}
.terminal .ws-log-entry{display:flex;gap:8px}
.terminal .ws-ts{color:var(--t-green-dim);min-width:56px;flex-shrink:0}
.terminal .ws-type-proposal{color:var(--t-green)}
.terminal .ws-type-error{color:var(--t-red)}
.terminal .ws-type-info{color:var(--t-amber)}
.terminal .bottom-bar{display:flex;align-items:center;gap:10px;padding:5px 16px;border-top:1px solid var(--t-border);background:var(--t-bg1);font-size:9px;color:var(--t-text-dim);letter-spacing:.08em;flex-shrink:0}
.terminal .bottom-sep{color:var(--t-border2)}
.terminal .refresh-ring{width:11px;height:11px;border-radius:50%;border:1.5px solid var(--t-border);border-top-color:var(--t-green);animation:t-spin 1.2s linear infinite;display:inline-block;vertical-align:middle;margin-right:3px}
@keyframes t-spin{to{transform:rotate(360deg)}}
</style>
"""

    import json
    import pandas as pd
    from datetime import datetime

    def ts_now():
        return datetime.now().strftime("%H:%M:%S")

    df_snapshot = payouts_df.copy()
    spot_val = getattr(synth_worker, "current_spot", None) or "—"
    msg_count = getattr(deriv_listner, "_message_counter", 0)
    ws_log_raw = list(getattr(deriv_listner, "ws_msg_history", []))
    is_running = getattr(deriv_listner, "is_running", False)
    ws_status_cls = "status-live" if is_running else "status-ws"
    ws_status_lbl = "WS · LIVE" if is_running else "WS · CONNECTING"
    uly = getattr(deriv_listner, "uly_symbol", "frxXAUUSD")
    now_str = datetime.now().strftime("%d %b %Y  %H:%M:%S IST")

    log_html = ""
    for raw in reversed(ws_log_raw[-30:]):
        try:
            m = json.loads(raw)
            mtype = m.get("msg_type", "msg")
            log_html += f'<div class="ws-log-entry"><span class="ws-ts">{ts_now()}</span><span class="ws-type-proposal">{mtype}</span></div>'
        except Exception:
            log_html += f'<div class="ws-log-entry"><span class="ws-ts">{ts_now()}</span><span class="ws-type-info">raw</span></div>'
    if not log_html:
        log_html = '<div class="ws-log-entry"><span class="ws-ts">--:--:--</span><span class="ws-type-info">waiting...</span></div>'

    def edge_cell(val):
        try:
            v = float(val)
            fill = "fill-pos" if v >= 5 else ("fill-warn" if v >= 0 else "fill-neg")
            cls = "val-pos" if v >= 5 else ("val-warn" if v >= 0 else "val-neg")
            pct = min(100, abs(v) * 4)
            return (
                f'<td><div class="edge-bar-wrap">'
                f'<span class="{cls}">{v:.1f}%</span>'
                f'<div class="edge-bar"><div class="edge-bar-fill {fill}" style="width:{pct:.0f}%"></div></div>'
                f"</div></td>"
            )
        except Exception:
            return '<td class="val-muted">—</td>'

    def short_ts(val):
        s = str(val) if val is not None else ""
        return s[-8:] if s not in ("", "—", "nan", "None") else "—"

    def safe_float(row, col):
        v = row.get(col)
        try:
            return f"{float(v):.2f}" if pd.notna(v) and v != "" else "—"
        except Exception:
            return "—"

    table_rows = ""
    call_edges, put_edges = [], []
    pos_edges = neg_edges = 0
    best_call = best_call_key = best_put = None

    for expiry, row in df_snapshot.sort_index().iterrows():
        ce_mkt = safe_float(row, "CE-payout")
        pe_mkt = safe_float(row, "PE-payout")
        c_fair = safe_float(row, "c_fair_payout")
        p_fair = safe_float(row, "p_fair_payout")

        def _edge_val(col):
            v = row[col] if col in row.index else None
            if v is None or (isinstance(v, float) and pd.isna(v)):
                return None
            try:
                return float(v)
            except Exception:
                return None

        c_edge_v = _edge_val("c_edge_pct")
        p_edge_v = _edge_val("p_edge_pct")
        c_edge = c_edge_v if c_edge_v is not None else ""
        p_edge = p_edge_v if p_edge_v is not None else ""
        ce_ts = short_ts(row.get("CE_spot_ts"))
        pe_ts = short_ts(row.get("PE_spot_ts"))
        exp_s = str(expiry)[11:19] if len(str(expiry)) > 11 else str(expiry)

        if c_edge_v is not None:
            call_edges.append(c_edge_v)
            pos_edges += 1 if c_edge_v >= 0 else 0
            neg_edges += 1 if c_edge_v < 0 else 0
            if best_call is None or c_edge_v > best_call:
                best_call, best_call_key = c_edge_v, exp_s

        if p_edge_v is not None:
            put_edges.append(p_edge_v)
            pos_edges += 1 if p_edge_v >= 0 else 0
            neg_edges += 1 if p_edge_v < 0 else 0
            if best_put is None or p_edge_v > best_put:
                best_put = p_edge_v

        c_fair_cls = "val-pos" if c_fair != "—" else "val-muted"
        p_fair_cls = "val-pos" if p_fair != "—" else "val-muted"

        table_rows += (
            "<tr>"
            f'<td style="font-size:10px;">{exp_s}</td>'
            f'<td class="val-bright">{ce_mkt}</td>'
            f'<td class="{c_fair_cls}">{c_fair}</td>'
            f"{edge_cell(c_edge)}"
            f'<td class="val-muted">{ce_ts}</td>'
            f'<td class="val-bright" style="border-left:1px solid var(--t-border)">{pe_mkt}</td>'
            f'<td class="{p_fair_cls}">{p_fair}</td>'
            f"{edge_cell(p_edge)}"
            f'<td class="val-muted">{pe_ts}</td>'
            "</tr>"
        )

    if not table_rows:
        table_rows = (
            '<tr><td colspan="9" style="text-align:center;padding:32px;'
            'color:var(--t-text-dim);font-size:11px;letter-spacing:.1em;">'
            "AWAITING DATA STREAM...</td></tr>"
        )

    n_rows = len(df_snapshot)

    def avg(lst):
        return sum(lst) / len(lst) if lst else None

    avg_c = avg(call_edges)
    avg_p = avg(put_edges)
    avg_call_str = f"{avg_c:.1f}%" if avg_c is not None else "—"
    avg_put_str = f"{avg_p:.1f}%" if avg_p is not None else "—"
    avg_call_cls = "pos" if avg_c is not None and avg_c >= 0 else "neg"
    avg_put_cls = "pos" if avg_p is not None and avg_p >= 0 else "neg"
    avg_call_pct = min(100, abs(avg_c) * 4) if avg_c is not None else 0
    avg_put_pct = min(100, abs(avg_p) * 4) if avg_p is not None else 0

    best_call_str = f"{best_call:.1f}%" if best_call is not None else "—"
    best_put_str = f"{best_put:.1f}%" if best_put is not None else "—"
    best_call_cls = "pos" if best_call is not None and best_call >= 0 else "neg"
    best_put_cls = "pos" if best_put is not None and best_put >= 0 else "neg"

    html = f"""
{css}
<div class="terminal">

  <div class="topbar">
    <div class="logo">
      <div class="logo-dot"></div>
      {uly} · BINARY EDGE MONITOR
    </div>
    <div style="font-size:10px;color:var(--t-text-dim);letter-spacing:.08em;">{now_str}</div>
    <div style="display:flex;gap:10px;align-items:center;">
      <span class="status-pill status-live">SYNTH · LIVE</span>
      <span class="status-pill {ws_status_cls}">{ws_status_lbl}</span>
    </div>
  </div>

  <div class="stat-ribbon">
    <div class="stat-chip">
      <span class="chip-label">Spot</span>
      <span class="chip-val">{spot_val}</span>
    </div>
    <div class="stat-chip">
      <span class="chip-label">Best Call</span>
      <span class="chip-val {best_call_cls}">{best_call_str}</span>
    </div>
    <div class="stat-chip">
      <span class="chip-label">Best Put</span>
      <span class="chip-val {best_put_cls}">{best_put_str}</span>
    </div>
    <div class="stat-chip">
      <span class="chip-label">+Edge</span>
      <span class="chip-val pos">{pos_edges}</span>
    </div>
    <div class="stat-chip">
      <span class="chip-label">-Edge</span>
      <span class="chip-val neg">{neg_edges}</span>
    </div>
    <div class="stat-chip">
      <span class="chip-label">Rows</span>
      <span class="chip-val">{n_rows}</span>
    </div>
    <div class="stat-chip">
      <span class="chip-label">Msgs</span>
      <span class="chip-val">{msg_count}</span>
    </div>
    <div class="stat-chip">
      <span class="chip-label"><span class="refresh-ring"></span>{forecast_fetch_interval}s</span>
    </div>
  </div>

  <div class="body">

    <div class="chain-panel">
      <div class="panel-header">
        Option Chain · Rise/Fall · {uly}
        <span class="panel-tag">STAKE $10 · USD</span>
      </div>
      <div class="chain-scroll">
        <table class="data-table">
          <thead>
            <tr>
              <th>Expiry</th>
              <th>CE Mkt</th>
              <th>CE Fair</th>
              <th>Call Edge</th>
              <th>CE ts</th>
              <th style="border-left:1px solid var(--t-border)">PE Mkt</th>
              <th>PE Fair</th>
              <th>Put Edge</th>
              <th>PE ts</th>
            </tr>
          </thead>
          <tbody>{table_rows}</tbody>
        </table>
      </div>
    </div>

    <div class="side-panel">
      <div class="panel-header" style="border-right:none;">
        WS Log <span class="panel-tag">{msg_count}</span>
      </div>
      <div class="mini-card" style="max-height:200px;overflow-y:auto;">
        <div class="ws-log">{log_html}</div>
      </div>

      <div class="panel-header" style="border-right:none;">Edge Summary</div>
      <div class="mini-card">
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:6px;margin-bottom:10px;">
          <div>
            <div class="mini-label">Positive</div>
            <div style="font-size:20px;font-weight:600;color:var(--t-green);">{pos_edges}</div>
          </div>
          <div>
            <div class="mini-label">Negative</div>
            <div style="font-size:20px;font-weight:600;color:var(--t-red);">{neg_edges}</div>
          </div>
        </div>
        <div class="mini-label">Avg Call Edge</div>
        <div class="edge-bar-wrap" style="justify-content:flex-start;margin-bottom:8px;">
          <span style="font-size:12px;font-weight:500;" class="chip-val {avg_call_cls}">{avg_call_str}</span>
          <div class="edge-bar" style="width:80px;"><div class="edge-bar-fill fill-pos" style="width:{avg_call_pct:.0f}%"></div></div>
        </div>
        <div class="mini-label">Avg Put Edge</div>
        <div class="edge-bar-wrap" style="justify-content:flex-start;">
          <span style="font-size:12px;font-weight:500;" class="chip-val {avg_put_cls}">{avg_put_str}</span>
          <div class="edge-bar" style="width:80px;"><div class="edge-bar-fill fill-pos" style="width:{avg_put_pct:.0f}%"></div></div>
        </div>
      </div>
    </div>

  </div>

  <div class="bottom-bar">
    <span>{now_str}</span>
    <span class="bottom-sep">|</span>
    <span>DERIV WS · BINARY OPTIONS</span>
    <span class="bottom-sep">|</span>
    <span>SYNTH AI · XAU · 1H</span>
  </div>

</div>
"""
    return mo.Html(html)
