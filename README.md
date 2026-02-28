# 🌿 Green Bharat — Real-Time Government Budget Monitoring & AI Auditor

> **Hack For Green Bharat Hackathon — Pathway Track**  
> Real-time streaming anomaly detection + LLM RAG for government budget transparency.

---

## 🏗️ Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                   ingestion.py                               │
│  Background thread ──► JSONL file ──► pw.io.jsonlines.read   │
│  (generates events every 1.5s)        (streaming connector)  │
└────────────────────────┬─────────────────────────────────────┘
                         │  pw.Table (live stream)
                         ▼
┌──────────────────────────────────────────────────────────────┐
│                 transformations.py                           │
│  • groupby(state, sector) → rolling totals + avg             │
│  • join(sector_avg) → spike detection (4× threshold)         │
│  • groupby(contractor) → cumulative spend flag (₹5000 Cr)    │
└────────┬──────────────────────┬───────────────────────────────┘
         │                      │
         ▼                      ▼
   output/*.jsonl          rag_layer.py
   (Pathway sinks)         • Policy docs → DocumentStore
         │                 • OpenAI embedder + GPT-4o-mini
         ▼                 • query_budget_ai(question)
┌──────────────────────────────────────────────────────────────┐
│                      main.py                                 │
│  FastAPI  ──► /          (live HTML dashboard, auto-refresh) │
│           ──► /api/spikes, /api/contractors, /api/aggregations│
│           ──► POST /query  (LLM RAG natural-language audit)  │
└──────────────────────────────────────────────────────────────┘
```

---

## 🚀 Quickstart

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Set your OpenAI API key (for LLM RAG)

```bash
cp .env.example .env
# Edit .env — add your OPENAI_API_KEY
```

### 3. Run the system

```bash
python main.py
```

Open **http://localhost:8000** — the dashboard auto-refreshes every 3 s showing live data.

### 4. Query the AI Auditor

```bash
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{"question": "Which contractor shows abnormal patterns?"}'
```

---

## 🐳 Docker

```bash
# Build + run
docker-compose up --build

# Dashboard
open http://localhost:8000
```

---

## 📁 Project Structure

```
green-bharat-pathway/
├── ingestion.py        ← Pathway streaming connectors (demo artificial stream)
├── transformations.py  ← Incremental aggregations + anomaly detection
├── rag_layer.py        ← Pathway LLM xPack RAG (DocumentStore + GPT)
├── main.py             ← Pipeline orchestration + FastAPI server
├── requirements.txt
├── Dockerfile
├── docker-compose.yml
├── .env.example
└── data/
    ├── budget_stream.jsonl   ← Live event stream (auto-generated)
    └── policy_docs/          ← Compliance documents (auto-created)
        ├── budget_compliance.txt
        ├── sector_benchmarks.txt
        ├── contractor_watchlist.txt
        └── green_bharat_mandate.txt
```

---

## 🔍 Key Pathway Concepts Used

| Concept | Where Used |
|---|---|
| `pw.io.jsonlines.read` (streaming mode) | `ingestion.py` — live event ingestion |
| `pw.Table.groupby().reduce()` | `transformations.py` — incremental rolling aggregation |
| `pw.Table.join()` | `transformations.py` — spike detection via sector avg join |
| `pw.Table.filter()` | `transformations.py` — anomaly filtering |
| `pw.io.jsonlines.write` | `main.py` — streaming output sinks |
| `DocumentStore` + `OpenAIEmbedder` | `rag_layer.py` — LLM xPack RAG |
| `BaseRAGQuestionAnswerer` | `rag_layer.py` — natural-language Q&A |
| `pw.apply()` | `transformations.py` — UDF for alert reason strings |

---

## 🤖 Sample AI Queries

- *"Why did Tamil Nadu electricity allocation increase?"*
- *"Which contractor shows abnormal patterns?"*
- *"Summarize this week's budget changes."*
- *"Is AquaWorks India compliant with policy guidelines?"*
- *"What are the Renewable Energy sector benchmarks?"*

---

## ⚡ Streaming Guarantees

- ✅ All data processed in **streaming mode** — no batch re-scans
- ✅ New events appear in dashboard within **~2–3 seconds**
- ✅ Aggregations **auto-update** without restart
- ✅ RAG context **enriched with live anomaly data** per query
- ✅ Policy documents can be added to `data/policy_docs/` at runtime

---

*Built for the Hack For Green Bharat Hackathon · Pathway Track · 2026*
