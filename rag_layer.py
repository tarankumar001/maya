"""
rag_layer.py — Pathway LLM xPack RAG Integration
=================================================
Responsibilities:
  - Store compliance policy documents and budget rules in Pathway's
    Document Store (vector store).
  - Build a RAG pipeline using Pathway's LLMApp / xPack components.
  - Expose a `query_budget_ai(question)` function used by the FastAPI
    interface in main.py.

Architecture:
  Documents (policy rules, sector guidelines, anomaly definitions) are
  embedded and indexed in Pathway's real-time document store.  When a
  new question arrives, Pathway retrieves the most relevant chunks and
  sends them with the question to the LLM for context-aware answers.

  The document store also responds to NEW documents arriving at runtime
  (streaming docs ingestion) — so updated policies are immediately
  reflected without a service restart.
"""

import os
import json
from typing import Optional
import pathway as pw

# Pathway LLM xPack components (lazy/conditional import to prevent crash on missing sub-deps)
try:
    from pathway.xpacks.llm import embedders, llms, parsers, splitters
    from pathway.xpacks.llm.document_store import DocumentStore
    from pathway.xpacks.llm.question_answering import BaseRAGQuestionAnswerer
    XPACK_AVAILABLE = True
except ImportError as e:
    print(f"[RAG] Warning: LLM xPack dependencies missing ({e}). RAG will be disabled.")
    XPACK_AVAILABLE = False
    # Dummy classes to prevent NameError in function signatures
    class DocumentStore: pass
    class BaseRAGQuestionAnswerer: pass


# ─────────────────────────────────────────────────────────────────────────────
# 1. Policy / compliance documents
#    We create these as real files so Pathway can stream-ingest them.
# ─────────────────────────────────────────────────────────────────────────────

POLICY_DOCS_DIR = "data/policy_docs"

POLICY_DOCUMENTS = {
    "budget_compliance.txt": """
Budget Compliance Rules — Green Bharat Initiative

1. Spike Threshold:
   Any single allocation event exceeding 4× the rolling sector average
   shall be automatically flagged for manual audit by the Chief Auditor.

2. Contractor Spend Cap:
   No single contractor may receive cumulative payments exceeding
   ₹5,000 crores in any rolling 30-day window without Finance Ministry
   pre-approval.

3. Sector Priority Rules:
   Renewable Energy and Sanitation sectors receive priority funding
   under the Green Bharat scheme.  Allocations in these sectors should
   constitute at least 30% of state budgets.

4. Transparency Mandate:
   All allocations above ₹500 crores must be publicly disclosed within
   72 hours of approval.

5. Tamil Nadu Electricity Guidelines:
   Tamil Nadu's electricity sector allocation is expected to increase
   20-35% year-on-year due to solar grid expansion under Mission SHINE.
   Increases within this band are expected and compliant.
""",

    "sector_benchmarks.txt": """
Sector Benchmarks — National Average Allocation (INR Crores per Quarter)

| Sector           | Low Band | High Band | Alert Threshold |
|------------------|----------|-----------|----------------|
| Electricity      | 200      | 800       | > 3200          |
| Water            | 100      | 500       | > 2000          |
| Transport        | 300      | 1200      | > 4800          |
| Healthcare       | 150      | 600       | > 2400          |
| Education        | 100      | 400       | > 1600          |
| Agriculture      | 200      | 700       | > 2800          |
| Renewable Energy | 400      | 1500      | > 6000          |
| Sanitation       | 80       | 350       | > 1400          |

These benchmarks are updated quarterly by the Planning Commission.
States exceeding alert thresholds should provide justification reports.
""",

    "contractor_watchlist.txt": """
Contractor Monitoring Policy — Anti-Corruption Division

Known Watchlist Patterns:
- Repeated high-value single-day payments to any contractor.
- Same contractor winning contracts in 3+ states within 48 hours.
- Allocation events missing contractor registration number.
- Contractors with cumulative spend growth > 200% quarter-on-quarter.

Flagged Contractors (for additional scrutiny):
- Any contractor receiving > ₹5,000 Cr cumulative requires enhanced due diligence.
- Contracts to newly registered firms (< 2 years old) above ₹200 Cr require MD approval.

AquaWorks India — additional scrutiny required (elevated risk profile per CBI advisory).
""",

    "green_bharat_mandate.txt": """
Green Bharat Initiative — Mission Statement & AI Audit Guidelines

The Green Bharat Initiative aims to channel government funds transparently
towards sustainable infrastructure, renewable energy, clean water, and
climate-resilient agriculture.

AI Audit System Mandate:
- Monitor all budget allocations in real-time.
- Detect anomalies within seconds of data arrival.
- Generate natural-language summaries of budget trends.
- Answer auditor queries about suspicious patterns immediately.
- Ensure zero tolerance for corrupt fund diversion.

Expected AI Capabilities:
1. "Why did [State] [Sector] allocation increase?" — compare against benchmarks.
2. "Which contractor shows abnormal patterns?" — cross-reference watchlist policy.
3. "Summarize this week's budget changes." — aggregate and narrate recent events.
4. "Is this allocation compliant?" — check against compliance rules.
"""
}


def _ensure_policy_docs():
    """Write policy documents to disk if not already present."""
    os.makedirs(POLICY_DOCS_DIR, exist_ok=True)
    for filename, content in POLICY_DOCUMENTS.items():
        path = os.path.join(POLICY_DOCS_DIR, filename)
        if not os.path.exists(path):
            with open(path, "w", encoding="utf-8") as f:
                f.write(content.strip())


# ─────────────────────────────────────────────────────────────────────────────
# 2. Document Store construction using Pathway xPack
# ─────────────────────────────────────────────────────────────────────────────

def build_document_store() -> Optional[DocumentStore]:
    """
    Stream policy documents into Pathway's Document Store (vector DB).
    """
    if not XPACK_AVAILABLE:
        return None

    _ensure_policy_docs()

    # Stream-ingest text files from POLICY_DOCS_DIR
    docs_source = pw.io.fs.read(
        POLICY_DOCS_DIR,
        format="binary",
        mode="streaming",           # Pathway streaming connector
        with_metadata=True,
    )

    # Parse raw bytes → text chunks
    parsed = docs_source.select(
        text=pw.apply(lambda b: b.decode("utf-8", errors="replace"), docs_source.data),
        metadata=docs_source._metadata,
    )

    # Split long documents into manageable chunks
    splitter = splitters.TokenCountSplitter(max_tokens=300)

    # Embed using OpenAI text-embedding (or any Pathway-compatible embedder)
    api_key = os.getenv("OPENAI_API_KEY", "")
    embedder = embedders.OpenAIEmbedder(api_key=api_key)

    store = DocumentStore(
        docs=parsed,
        embedder=embedder,
        splitter=splitter,
    )
    return store


# ─────────────────────────────────────────────────────────────────────────────
# 3. RAG Question Answerer
# ─────────────────────────────────────────────────────────────────────────────

def build_rag_answerer(store: Optional[DocumentStore]) -> Optional[BaseRAGQuestionAnswerer]:
    """
    Wrap the DocumentStore in a RAG pipeline backed by an OpenAI LLM.
    """
    if not XPACK_AVAILABLE or store is None:
        return None

    api_key = os.getenv("OPENAI_API_KEY", "")

    llm_model = llms.OpenAIChat(
        model="gpt-4o-mini",          # cost-effective for hackathon
        api_key=api_key,
        temperature=0.2,
        system_message=(
            "You are an expert government budget auditor for India's Green Bharat "
            "Initiative.  You have access to real-time budget allocation data and "
            "compliance policy documents.  Answer questions accurately, citing "
            "specific numbers and policy rules when available.  Flag any anomalies "
            "or suspicious patterns proactively.  Be concise but thorough."
        ),
        max_tokens=600,
    )

    answerer = BaseRAGQuestionAnswerer(
        llm=llm_model,
        indexer=store,
        n_response_docs=4,            # retrieve 4 most relevant chunks
    )
    return answerer


# ─────────────────────────────────────────────────────────────────────────────
# 4. Context injector — enrich LLM queries with live anomaly data
# ─────────────────────────────────────────────────────────────────────────────

_live_context_store: dict = {
    "spike_alerts": [],
    "contractor_flags": [],
    "state_sector_summary": {},
}


def update_live_context(
    spike_alerts: list[dict],
    contractor_flags: list[dict],
    state_sector_summary: dict,
):
    """
    Called periodically by the output sink in main.py to keep the
    in-memory context fresh.  This context is prepended to every LLM
    query so the model has the latest anomaly data.
    """
    _live_context_store["spike_alerts"]       = spike_alerts[-10:]   # last 10
    _live_context_store["contractor_flags"]   = contractor_flags
    _live_context_store["state_sector_summary"] = state_sector_summary


def query_budget_ai(
    question: str,
    answerer: Optional[BaseRAGQuestionAnswerer],
) -> str:
    """
    Answer a natural-language question using RAG + live streaming context.
    """
    if not XPACK_AVAILABLE or answerer is None:
        return "AI Auditor is currently unavailable (missing dependencies or key)."

    # Build live-data context string to prepend to the question
    context_parts = []

    if _live_context_store["spike_alerts"]:
        alerts_str = json.dumps(_live_context_store["spike_alerts"], indent=2)
        context_parts.append(f"LIVE SPIKE ALERTS (recent):\n{alerts_str}")

    if _live_context_store["contractor_flags"]:
        flags_str = json.dumps(_live_context_store["contractor_flags"], indent=2)
        context_parts.append(f"LIVE CONTRACTOR FLAGS:\n{flags_str}")

    if _live_context_store["state_sector_summary"]:
        summary_str = json.dumps(_live_context_store["state_sector_summary"], indent=2)
        context_parts.append(f"LIVE STATE-SECTOR TOTALS:\n{summary_str}")

    enriched_question = question
    if context_parts:
        enriched_question = (
            "Use the following live real-time data as additional context:\n\n"
            + "\n\n".join(context_parts)
            + f"\n\n---\nQuestion: {question}"
        )

    # Run the Pathway RAG answerer (retrieves from vector store + calls LLM)
    answer = answerer.answer(enriched_question)
    return answer
