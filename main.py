"""
main.py — Pipeline Orchestration
=================================
The top-level entry point that wires together:
  ingestion   → transformations   → output sinks
                        ↕
                    rag_layer (served via FastAPI)

Run:
  python main.py

The Pathway engine starts in streaming mode.  A FastAPI server runs on
a background thread at http://localhost:8000 for interactive queries.
Open http://localhost:8000/docs for the auto-generated Swagger UI.
"""

import os
import json
import time
import asyncio
import threading
import uvicorn
from contextlib import asynccontextmanager
from typing import Optional

import pathway as pw
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from ingestion        import get_budget_stream
from transformations  import transform_budget
from rag_layer        import (
    build_document_store,
    build_rag_answerer,
    query_budget_ai,
    update_live_context,
)

# ─────────────────────────────────────────────────────────────────────────────
# Output directories
# ─────────────────────────────────────────────────────────────────────────────
os.makedirs("output", exist_ok=True)
os.makedirs("data",   exist_ok=True)

# ─────────────────────────────────────────────────────────────────────────────
# Global state — populated by Pathway output callbacks
# ─────────────────────────────────────────────────────────────────────────────
_state = {
    "spike_alerts":        [],
    "contractor_flags":    [],
    "state_sector_agg":    [],
    "last_updated":        None,
}
_state_lock = threading.Lock()

_rag_answerer = None   # set after Pathway pipeline starts


# ─────────────────────────────────────────────────────────────────────────────
# 1. Build & wire Pathway pipeline
# ─────────────────────────────────────────────────────────────────────────────

def build_pipeline():
    """
    Assemble the full Pathway streaming pipeline and register output sinks.
    This function configures the graph; pw.run() starts execution.
    """
    # ── Ingestion ─────────────────────────────────────────────────────────
    budget_stream = get_budget_stream()

    # ── Transformations ───────────────────────────────────────────────────
    results = transform_budget(budget_stream)

    state_sector_agg  = results["state_sector_agg"]
    spike_alerts      = results["spike_alerts"]
    contractor_flags  = results["contractor_flags"]

    # ── Output sinks — write to JSONL so FastAPI can serve them ───────────

    # 1. State-sector rolling aggregations
    pw.io.jsonlines.write(
        state_sector_agg.select(
            pw.this.state,
            pw.this.sector,
            pw.this.total_allocation,
            pw.this.event_count,
            pw.this.avg_allocation,
        ),
        "output/state_sector_agg.jsonl",
    )

    # 2. Spike alerts
    pw.io.jsonlines.write(
        spike_alerts.select(
            pw.this.state,
            pw.this.sector,
            pw.this.allocation,
            pw.this.contractor,
            pw.this.timestamp,
            pw.this.sector_avg,
            pw.this.spike_ratio,
            pw.this.alert_reason,
        ),
        "output/spike_alerts.jsonl",
    )

    # 3. Contractor anomaly flags
    pw.io.jsonlines.write(
        contractor_flags.select(
            pw.this.contractor,
            pw.this.total_spend,
            pw.this.payment_count,
            pw.this.alert_reason,
        ),
        "output/contractor_flags.jsonl",
    )


# ─────────────────────────────────────────────────────────────────────────────
# 2. Output file polling — keep in-memory state fresh for FastAPI
# ─────────────────────────────────────────────────────────────────────────────

def _read_jsonl(path: str) -> list[dict]:
    """Read all records from a JSONL file; return empty list if missing."""
    if not os.path.exists(path):
        return []
    records = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass
    except Exception:
        pass
    # Deduplicate by converting to set of frozensets (simple approach)
    seen, unique = set(), []
    for r in records:
        key = json.dumps(r, sort_keys=True)
        if key not in seen:
            seen.add(key)
            unique.append(r)
    return unique


def _poll_outputs():
    """Background thread: refresh in-memory state every 2 s."""
    while True:
        time.sleep(2)
        spikes      = _read_jsonl("output/spike_alerts.jsonl")
        contractors = _read_jsonl("output/contractor_flags.jsonl")
        agg         = _read_jsonl("output/state_sector_agg.jsonl")

        # Build summary dict for RAG context enrichment
        summary = {}
        for row in agg:
            key = f"{row.get('state','?')} / {row.get('sector','?')}"
            summary[key] = {
                "total_allocation": row.get("total_allocation"),
                "event_count":      row.get("event_count"),
                "avg_allocation":   row.get("avg_allocation"),
            }

        with _state_lock:
            _state["spike_alerts"]     = spikes
            _state["contractor_flags"] = contractors
            _state["state_sector_agg"] = agg
            _state["last_updated"]     = time.time()

        update_live_context(spikes, contractors, summary)


# ─────────────────────────────────────────────────────────────────────────────
# 3. Pathway runner — separate thread (pw.run blocks)
# ─────────────────────────────────────────────────────────────────────────────

def _run_pathway():
    """Run the Pathway streaming engine in a daemon thread."""
    build_pipeline()
    pw.run(monitoring_level=pw.MonitoringLevel.NONE)


# ─────────────────────────────────────────────────────────────────────────────
# 4. FastAPI application
# ─────────────────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Start Pathway pipeline
    pw_thread = threading.Thread(target=_run_pathway, daemon=True)
    pw_thread.start()

    # Give Pathway a moment to boot
    await asyncio.sleep(3)

    # Start RAG (can be built without blocking Pathway)
    global _rag_answerer
    try:
        store         = build_document_store()
        _rag_answerer = build_rag_answerer(store)
    except Exception as e:
        print(f"[RAG] Warning: {e} — RAG queries will be unavailable.")
        _rag_answerer = None

    # Start output file poller
    poll_thread = threading.Thread(target=_poll_outputs, daemon=True)
    poll_thread.start()

    yield   # Application runs


app = FastAPI(
    title="🌿 Green Bharat — Real-Time Budget Auditor",
    description=(
        "Pathway-powered real-time streaming system for monitoring "
        "government budget allocations, detecting anomalies, and "
        "answering natural-language audit queries via LLM RAG."
    ),
    version="1.0.0",
    lifespan=lifespan,
)


# ── Pydantic models ──────────────────────────────────────────────────────────

class QueryRequest(BaseModel):
    question: str

class QueryResponse(BaseModel):
    question: str
    answer: str
    timestamp: float


# ── Endpoints ────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse, tags=["Dashboard"])
async def dashboard():
    """Real-time HTML dashboard (refreshes every 3 seconds)."""
    with _state_lock:
        spikes      = _state["spike_alerts"][-10:]
        contractors = _state["contractor_flags"]
        agg         = _state["state_sector_agg"]
        updated     = _state["last_updated"]

    updated_str = (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(updated))
        if updated else "Initializing…"
    )

    def spike_rows():
        if not spikes:
            return "<tr><td colspan='6' style='text-align:center;color:#aaa'>No spikes detected yet — streaming…</td></tr>"
        return "".join(
            f"<tr>"
            f"<td>{s.get('state','')}</td>"
            f"<td>{s.get('sector','')}</td>"
            f"<td>₹{s.get('allocation',0):,.0f} Cr</td>"
            f"<td>{s.get('contractor','')}</td>"
            f"<td>{s.get('spike_ratio',0):.1f}×</td>"
            f"<td style='color:#ff6b6b'>{s.get('alert_reason','')}</td>"
            f"</tr>"
            for s in spikes
        )

    def contractor_rows():
        if not contractors:
            return "<tr><td colspan='4' style='text-align:center;color:#aaa'>No contractor flags yet</td></tr>"
        return "".join(
            f"<tr>"
            f"<td>{c.get('contractor','')}</td>"
            f"<td>₹{c.get('total_spend',0):,.0f} Cr</td>"
            f"<td>{c.get('payment_count',0)}</td>"
            f"<td style='color:#ff9f43'>{c.get('alert_reason','')}</td>"
            f"</tr>"
            for c in contractors
        )

    def agg_rows():
        top = sorted(agg, key=lambda x: x.get("total_allocation", 0), reverse=True)[:15]
        if not top:
            return "<tr><td colspan='5' style='text-align:center;color:#aaa'>Aggregating…</td></tr>"
        return "".join(
            f"<tr>"
            f"<td>{r.get('state','')}</td>"
            f"<td>{r.get('sector','')}</td>"
            f"<td>₹{r.get('total_allocation',0):,.0f} Cr</td>"
            f"<td>{r.get('event_count',0)}</td>"
            f"<td>₹{r.get('avg_allocation',0):,.0f} Cr</td>"
            f"</tr>"
            for r in top
        )

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta http-equiv="refresh" content="3">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>🌿 Green Bharat — Budget Auditor</title>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600;700;800&display=swap" rel="stylesheet">
  <style>
    *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
    :root {{
      --bg:      #0a0f1e;
      --surface: #111827;
      --card:    #1a2235;
      --border:  #2a3a52;
      --accent:  #10b981;
      --danger:  #ef4444;
      --warn:    #f59e0b;
      --text:    #e2e8f0;
      --muted:   #64748b;
    }}
    body {{
      font-family: 'Inter', sans-serif;
      background: var(--bg);
      color: var(--text);
      min-height: 100vh;
      padding: 0 0 3rem;
    }}
    header {{
      background: linear-gradient(135deg,#0d2137 0%,#0f3d2a 100%);
      border-bottom: 1px solid var(--border);
      padding: 1.4rem 2rem;
      display: flex;
      align-items: center;
      justify-content: space-between;
      position: sticky; top: 0; z-index: 100;
    }}
    header h1 {{ font-size: 1.5rem; font-weight: 800; color: var(--accent); }}
    header p  {{ font-size: 0.75rem; color: var(--muted); }}
    .badge {{
      display: inline-block; padding: .2rem .7rem;
      border-radius: 9999px; font-size: .7rem; font-weight: 700;
      background: rgba(16,185,129,.15); color: var(--accent);
      border: 1px solid rgba(16,185,129,.3);
      animation: pulse 2s infinite;
    }}
    @keyframes pulse {{ 0%,100%{{opacity:1}} 50%{{opacity:.5}} }}
    .container {{ max-width: 1400px; margin: 2rem auto; padding: 0 1.5rem; }}
    .stats {{ display: grid; grid-template-columns: repeat(auto-fit,minmax(200px,1fr)); gap: 1rem; margin-bottom: 2rem; }}
    .stat-card {{
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: .75rem;
      padding: 1.25rem 1.5rem;
      position: relative; overflow: hidden;
    }}
    .stat-card::before {{
      content: '';
      position: absolute; top: 0; left: 0; right: 0; height: 3px;
      background: var(--accent);
    }}
    .stat-card.danger::before {{ background: var(--danger); }}
    .stat-card.warn::before  {{ background: var(--warn); }}
    .stat-card h3 {{ font-size: .75rem; color: var(--muted); text-transform: uppercase; letter-spacing: .05em; }}
    .stat-card .val {{ font-size: 2rem; font-weight: 700; margin-top: .4rem; }}
    .stat-card.danger .val {{ color: var(--danger); }}
    .stat-card.warn   .val {{ color: var(--warn); }}
    .section-title {{ font-size: 1.1rem; font-weight: 700; margin-bottom: 1rem; display: flex; align-items: center; gap: .5rem; }}
    .section-title span {{ font-size: 1.3rem; }}
    table {{
      width: 100%; border-collapse: collapse;
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: .75rem; overflow: hidden;
      margin-bottom: 2rem;
    }}
    thead {{ background: rgba(16,185,129,.08); }}
    th {{ padding: .8rem 1rem; text-align: left; font-size: .7rem; text-transform: uppercase; letter-spacing: .08em; color: var(--muted); }}
    td {{ padding: .8rem 1rem; font-size: .85rem; border-top: 1px solid rgba(255,255,255,.04); }}
    tr:hover td {{ background: rgba(255,255,255,.03); }}
    .query-section {{
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: .75rem; padding: 1.5rem;
      margin-bottom: 2rem;
    }}
    .query-section p {{ font-size: .85rem; color: var(--muted); margin-bottom: 1rem; }}
    .query-chips {{ display: flex; flex-wrap: wrap; gap: .5rem; }}
    .chip {{
      background: rgba(16,185,129,.1);
      border: 1px solid rgba(16,185,129,.2);
      border-radius: .5rem; padding: .4rem .9rem;
      font-size: .78rem; color: var(--accent); cursor: pointer;
      transition: background .2s;
    }}
    .chip:hover {{ background: rgba(16,185,129,.2); }}
    footer {{ text-align: center; color: var(--muted); font-size: .75rem; margin-top: 2rem; }}
  </style>
</head>
<body>
<header>
  <div>
    <h1>🌿 Green Bharat — Real-Time Budget Auditor</h1>
    <p>Powered by Pathway Streaming · Last updated: {updated_str}</p>
  </div>
  <span class="badge">● LIVE STREAM</span>
</header>

<div class="container">
  <!-- Stats cards -->
  <div class="stats">
    <div class="stat-card">
      <h3>Allocation Events</h3>
      <div class="val">{len(agg)}</div>
    </div>
    <div class="stat-card danger">
      <h3>Spike Alerts</h3>
      <div class="val">{len(spikes)}</div>
    </div>
    <div class="stat-card warn">
      <h3>Flagged Contractors</h3>
      <div class="val">{len(contractors)}</div>
    </div>
    <div class="stat-card">
      <h3>States Monitored</h3>
      <div class="val">{len(set(r.get('state','') for r in agg))}</div>
    </div>
  </div>

  <!-- AI Query Suggestions -->
  <div class="section-title"><span>🤖</span> AI Auditor Queries — <a href="/docs#/AI%20Query" style="color:var(--accent);font-size:.85rem;text-decoration:none">POST /query</a></div>
  <div class="query-section">
    <p>Ask the AI auditor anything about the live budget data:</p>
    <div class="query-chips">
      <span class="chip">Why did Tamil Nadu electricity allocation increase?</span>
      <span class="chip">Which contractor shows abnormal patterns?</span>
      <span class="chip">Summarize this week's budget changes</span>
      <span class="chip">Is AquaWorks India compliant?</span>
      <span class="chip">What are the Renewable Energy benchmarks?</span>
    </div>
  </div>

  <!-- Spike Alerts -->
  <div class="section-title"><span>⚠️</span> Spike Alerts (Recent 10)</div>
  <table>
    <thead><tr>
      <th>State</th><th>Sector</th><th>Allocation</th><th>Contractor</th><th>Spike Ratio</th><th>Alert</th>
    </tr></thead>
    <tbody>{spike_rows()}</tbody>
  </table>

  <!-- Contractor Flags -->
  <div class="section-title"><span>🚨</span> Flagged Contractors</div>
  <table>
    <thead><tr>
      <th>Contractor</th><th>Total Spend</th><th>Payments</th><th>Alert</th>
    </tr></thead>
    <tbody>{contractor_rows()}</tbody>
  </table>

  <!-- State-Sector Aggregation -->
  <div class="section-title"><span>📊</span> Rolling Aggregations (Top 15 by Allocation)</div>
  <table>
    <thead><tr>
      <th>State</th><th>Sector</th><th>Total Allocation</th><th>Events</th><th>Avg per Event</th>
    </tr></thead>
    <tbody>{agg_rows()}</tbody>
  </table>
</div>

<footer>
  🌿 Green Bharat Initiative · Hack For Green Bharat · Pathway Streaming · 2026
</footer>
</body>
</html>"""
    return HTMLResponse(content=html)


@app.get("/api/status", tags=["Status"])
async def status():
    """System health and stream statistics."""
    with _state_lock:
        return {
            "status":           "streaming",
            "last_updated":     _state["last_updated"],
            "spike_count":      len(_state["spike_alerts"]),
            "contractor_flags": len(_state["contractor_flags"]),
            "agg_rows":         len(_state["state_sector_agg"]),
            "rag_ready":        _rag_answerer is not None,
        }


@app.get("/api/spikes", tags=["Anomalies"])
async def get_spikes(limit: int = 20):
    """Return the most recent spike alerts."""
    with _state_lock:
        return {"data": _state["spike_alerts"][-limit:], "count": len(_state["spike_alerts"])}


@app.get("/api/contractors", tags=["Anomalies"])
async def get_contractor_flags():
    """Return all flagged contractors."""
    with _state_lock:
        return {"data": _state["contractor_flags"], "count": len(_state["contractor_flags"])}


@app.get("/api/aggregations", tags=["Data"])
async def get_aggregations(state: Optional[str] = None, sector: Optional[str] = None):
    """Return rolling state-sector aggregations, with optional filters."""
    with _state_lock:
        data = _state["state_sector_agg"]
    if state:
        data = [r for r in data if r.get("state", "").lower() == state.lower()]
    if sector:
        data = [r for r in data if r.get("sector", "").lower() == sector.lower()]
    return {"data": data, "count": len(data)}


@app.post("/query", response_model=QueryResponse, tags=["AI Query"])
async def ai_query(request: QueryRequest):
    """
    Ask the AI auditor a natural-language question.

    Examples:
    - "Why did Tamil Nadu electricity allocation increase?"
    - "Which contractor shows abnormal patterns?"
    - "Summarize this week's budget changes."
    """
    if not _rag_answerer:
        raise HTTPException(
            status_code=503,
            detail="RAG engine not ready — check OPENAI_API_KEY env variable.",
        )
    try:
        answer = query_budget_ai(request.question, _rag_answerer)
        return QueryResponse(
            question=request.question,
            answer=answer,
            timestamp=time.time(),
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─────────────────────────────────────────────────────────────────────────────
# 5. Entry point
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("━" * 60)
    print("  🌿 Green Bharat — Real-Time Budget Auditor")
    print("  Pathway Streaming + LLM RAG")
    print("━" * 60)
    print("  Dashboard → http://localhost:8000")
    print("  API Docs  → http://localhost:8000/docs")
    print("━" * 60)

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        log_level="info",
    )
