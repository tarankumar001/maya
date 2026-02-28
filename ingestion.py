"""
ingestion.py — Real-Time Government Budget Monitoring System
============================================================
Pathway streaming ingestion module.

Responsibilities:
  - Simulate live government budget allocation events using Pathway's
    demo connector (artificial stream generator), so no static dataset
    is ever loaded in batch.
  - Define the schema for every incoming budget event.
  - Export a single function `get_budget_stream()` that returns a
    live Pathway Table ready for downstream transformations.

Architecture note:
  Pathway's `pw.demo.range_stream` / `pw.io.csv` connectors are
  inherently streaming.  New rows appear continuously; downstream
  operators react incrementally without re-scanning old data.
"""

import random
import time
import threading
import json
import os
import pathway as pw

# ─────────────────────────────────────────────────────────────────────────────
# 1. Schema definition
#    Every budget allocation event must conform to this schema.
# ─────────────────────────────────────────────────────────────────────────────

class BudgetEventSchema(pw.Schema):
    state: str          # e.g. "Tamil Nadu", "Maharashtra"
    sector: str         # e.g. "Electricity", "Water", "Transport"
    allocation: float   # INR in crores
    contractor: str     # contractor name
    timestamp: int      # Unix epoch seconds


# ─────────────────────────────────────────────────────────────────────────────
# 2. Data generation constants
# ─────────────────────────────────────────────────────────────────────────────

STATES = [
    "Tamil Nadu", "Maharashtra", "Karnataka", "Uttar Pradesh",
    "Gujarat", "Rajasthan", "West Bengal", "Andhra Pradesh",
    "Telangana", "Kerala"
]

SECTORS = [
    "Electricity", "Water", "Transport", "Healthcare",
    "Education", "Agriculture", "Renewable Energy", "Sanitation"
]

CONTRACTORS = [
    "GreenInfra Ltd", "BharatBuild Corp", "EcoPower Solutions",
    "National Constructions", "SunTech Projects",
    "AquaWorks India", "RoadMaster Pvt Ltd", "SmartGrid Co",
    "HydroForce Inc", "TerraBuild Associates"
]

# Base allocation ranges per sector (crores INR)
BASE_ALLOCATIONS = {
    "Electricity":       (200, 800),
    "Water":             (100, 500),
    "Transport":         (300, 1200),
    "Healthcare":        (150, 600),
    "Education":         (100, 400),
    "Agriculture":       (200, 700),
    "Renewable Energy":  (400, 1500),
    "Sanitation":        (80,  350),
}

# Probability that a generated event is intentionally anomalous
ANOMALY_PROBABILITY = 0.08


def _generate_event() -> dict:
    """Generate a single realistic (or occasionally anomalous) budget event."""
    state = random.choice(STATES)
    sector = random.choice(SECTORS)
    contractor = random.choice(CONTRACTORS)

    lo, hi = BASE_ALLOCATIONS.get(sector, (100, 500))

    if random.random() < ANOMALY_PROBABILITY:
        # Spike — 5–15× the normal upper bound
        allocation = random.uniform(hi * 5, hi * 15)
    else:
        allocation = random.uniform(lo, hi)

    return {
        "state":       state,
        "sector":      sector,
        "allocation":  round(allocation, 2),
        "contractor":  contractor,
        "timestamp":   int(time.time()),
    }


# ─────────────────────────────────────────────────────────────────────────────
# 3. Streaming JSONL writer
#    Pathway's `pw.io.jsonlines` connector tails a file in real-time,
#    so we write events to a JSONL file from a background thread and
#    let Pathway's connector stream them in incrementally.
# ─────────────────────────────────────────────────────────────────────────────

STREAM_FILE = "data/budget_stream.jsonl"
_writer_started = False


def _start_stream_writer(interval_seconds: float = 1.5):
    """
    Background thread: continuously appends new budget events to
    STREAM_FILE.  Pathway's file connector detects the appended lines
    and emits them as streaming rows — no batch reloading ever occurs.
    """
    global _writer_started
    if _writer_started:
        return
    _writer_started = True

    os.makedirs("data", exist_ok=True)
    # Seed the file with a few initial rows so the pipeline starts immediately
    with open(STREAM_FILE, "w") as f:
        for _ in range(5):
            f.write(json.dumps(_generate_event()) + "\n")

    def _write_loop():
        while True:
            time.sleep(interval_seconds)
            event = _generate_event()
            with open(STREAM_FILE, "a") as f:
                f.write(json.dumps(event) + "\n")

    t = threading.Thread(target=_write_loop, daemon=True)
    t.start()


# ─────────────────────────────────────────────────────────────────────────────
# 4. Public API — get_budget_stream()
# ─────────────────────────────────────────────────────────────────────────────

def get_budget_stream() -> pw.Table:
    """
    Start the background event writer and return a live Pathway Table
    that streams budget allocation events in real-time.

    Returns
    -------
    pw.Table  — schema: BudgetEventSchema
        A streaming table whose rows accumulate as new events arrive.
        All downstream operators (in transformations.py) react
        incrementally to each new row.
    """
    _start_stream_writer(interval_seconds=1.5)

    # pw.io.jsonlines tails STREAM_FILE; every new line becomes a new
    # streaming row — this is Pathway's native streaming connector.
    budget_table = pw.io.jsonlines.read(
        STREAM_FILE,
        schema=BudgetEventSchema,
        mode="streaming",          # Pathway key: never batch-reload
    )

    return budget_table
