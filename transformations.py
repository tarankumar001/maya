"""
transformations.py — Streaming Aggregations & Anomaly Detection
===============================================================
All operations here are INCREMENTAL — Pathway re-evaluates only the
rows that change when a new event arrives.  There is zero batch
re-scanning of historical data.

Modules exported:
  transform_budget(table)  → returns a dict of result tables:
    {
      "state_sector_agg"  : rolling totals + event counts per (state, sector),
      "spike_alerts"      : events exceeding the dynamic spike threshold,
      "contractor_flags"  : contractors with unusually high cumulative spend,
    }
"""

import pathway as pw


# ─────────────────────────────────────────────────────────────────────────────
# Tunable thresholds
# ─────────────────────────────────────────────────────────────────────────────

# A single event is flagged as a "spike" if its allocation exceeds this
# multiple of the sector-wide per-event average.
SPIKE_MULTIPLIER: float = 4.0

# A contractor is flagged if their CUMULATIVE spend exceeds this threshold
# (INR crores).
CONTRACTOR_ALERT_CRORES: float = 5_000.0


# ─────────────────────────────────────────────────────────────────────────────
# 1. Rolling aggregations — (state × sector) window
#    Pathway keeps a continuously updated, auto-incrementing result table.
# ─────────────────────────────────────────────────────────────────────────────

def _state_sector_aggregations(table: pw.Table) -> pw.Table:
    """
    Group by (state, sector) and compute running totals.

    Columns returned:
      state, sector, total_allocation, event_count, avg_allocation
    """
    agg = (
        table
        .groupby(table.state, table.sector)
        .reduce(
            state=pw.reducers.any(table.state),
            sector=pw.reducers.any(table.sector),
            total_allocation=pw.reducers.sum(table.allocation),
            event_count=pw.reducers.count(),
        )
    )

    # Derived: streaming average — recomputed each time a row changes
    agg = agg.select(
        *pw.this,
        avg_allocation=agg.total_allocation / agg.event_count,
    )
    return agg


# ─────────────────────────────────────────────────────────────────────────────
# 2. Spike detection — join events against their sector averages
#    Uses an incremental join: whenever the sector average changes
#    (new events arrive), old comparisons are automatically updated.
# ─────────────────────────────────────────────────────────────────────────────

def _spike_detection(
    table: pw.Table,
    state_sector_agg: pw.Table,
) -> pw.Table:
    """
    Flag individual events whose allocation > SPIKE_MULTIPLIER × sector avg.

    Steps:
      1. Compute sector-level (not state-level) average allocation.
      2. Join every raw event against the sector average.
      3. Filter rows where allocation is anomalously high.
    """
    # Sector-level average (aggregate across all states for this sector)
    sector_avg = (
        table
        .groupby(table.sector)
        .reduce(
            sector=pw.reducers.any(table.sector),
            sector_avg=pw.reducers.sum(table.allocation) / pw.reducers.count(),
        )
    )

    # Incremental join: each event row acquires its sector's rolling average
    joined = table.join(sector_avg, table.sector == sector_avg.sector).select(
        state=table.state,
        sector=table.sector,
        allocation=table.allocation,
        contractor=table.contractor,
        timestamp=table.timestamp,
        sector_avg=sector_avg.sector_avg,
    )

    # Filter — only suspicious rows survive
    spikes = joined.filter(
        joined.allocation > SPIKE_MULTIPLIER * joined.sector_avg
    )

    spikes = spikes.select(
        *pw.this,
        spike_ratio=spikes.allocation / spikes.sector_avg,
        alert_reason=pw.apply(
            lambda state, sector, ratio: (
                f"⚠ SPIKE: {state} / {sector} — "
                f"{ratio:.1f}× above sector average"
            ),
            spikes.state, spikes.sector, spikes.allocation / spikes.sector_avg,
        ),
    )
    return spikes


# ─────────────────────────────────────────────────────────────────────────────
# 3. Contractor anomaly flagging
#    Running total per contractor; flag those exceeding threshold.
# ─────────────────────────────────────────────────────────────────────────────

def _contractor_anomalies(table: pw.Table) -> pw.Table:
    """
    Aggregate cumulative spend per contractor.
    Flag contractors whose total spend exceeds CONTRACTOR_ALERT_CRORES.

    Columns returned:
      contractor, total_spend, payment_count, alert_reason
    """
    contractor_agg = (
        table
        .groupby(table.contractor)
        .reduce(
            contractor=pw.reducers.any(table.contractor),
            total_spend=pw.reducers.sum(table.allocation),
            payment_count=pw.reducers.count(),
        )
    )

    flagged = contractor_agg.filter(
        contractor_agg.total_spend > CONTRACTOR_ALERT_CRORES
    )

    flagged = flagged.select(
        *pw.this,
        alert_reason=pw.apply(
            lambda c, spend, count: (
                f"🚨 CONTRACTOR FLAG: {c} — ₹{spend:,.0f} Cr "
                f"across {count} payments"
            ),
            flagged.contractor, flagged.total_spend, flagged.payment_count,
        ),
    )
    return flagged


# ─────────────────────────────────────────────────────────────────────────────
# 4. Public API
# ─────────────────────────────────────────────────────────────────────────────

def transform_budget(table: pw.Table) -> dict[str, pw.Table]:
    """
    Run all streaming transformations on the raw budget event table.

    Parameters
    ----------
    table : pw.Table
        The live streaming table from ingestion.get_budget_stream().

    Returns
    -------
    dict with three live pw.Table objects:
      "state_sector_agg"  — rolling aggregations
      "spike_alerts"      — individual spike events
      "contractor_flags"  — high-spend contractor alerts
    """
    state_sector_agg = _state_sector_aggregations(table)
    spike_alerts     = _spike_detection(table, state_sector_agg)
    contractor_flags = _contractor_anomalies(table)

    return {
        "state_sector_agg": state_sector_agg,
        "spike_alerts":     spike_alerts,
        "contractor_flags": contractor_flags,
    }
