# api/latency.py
from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, Any, List

# Vercel maps api/latency.py -> /api/latency, so define route at "/"
app = FastAPI()

# Allow POST from any origin
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["POST"],
    allow_headers=["*"],
)

# Load telemetry at cold start from data/q-vercel-latency.json
DATA_PATH = Path(__file__).parent.parent / "data" / "q-vercel-latency.json"
try:
    # Try NDJSON / line-delimited first
    TELEMETRY_DF = pd.read_json(DATA_PATH, lines=True)
except ValueError:
    try:
        TELEMETRY_DF = pd.read_json(DATA_PATH)
    except Exception:
        TELEMETRY_DF = pd.DataFrame()

def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    """Rename common columns to standard names: region, latency_ms, uptime."""
    rename = {}
    cols = {c.lower(): c for c in df.columns}
    if "region" in cols:
        rename[cols["region"]] = "region"
    if "latency_ms" in cols:
        rename[cols["latency_ms"]] = "latency_ms"
    elif "latency" in cols:
        rename[cols["latency"]] = "latency_ms"
    elif "ping" in cols:
        rename[cols["ping"]] = "latency_ms"
    if "uptime" in cols:
        rename[cols["uptime"]] = "uptime"
    elif "up" in cols:
        rename[cols["up"]] = "uptime"
    return df.rename(columns=rename)

@app.post("/")
async def latency_endpoint(request: Request) -> Dict[str, Any]:
    """
    Expects: {"regions": [...], "threshold_ms": 180}
    Returns per-region metrics.
    """
    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    regions = payload.get("regions")
    threshold = payload.get("threshold_ms")

    if not isinstance(regions, list) or not isinstance(threshold, (int, float)):
        raise HTTPException(
            status_code=400,
            detail='Body must be {"regions":[...], "threshold_ms":180}'
        )

    if TELEMETRY_DF.empty:
        raise HTTPException(status_code=500, detail="Telemetry data not found on server. Place data/q-vercel-latency.json in repo.")

    df = normalize_df(TELEMETRY_DF.copy())

    # make sure required column exists
    if "region" not in df.columns or "latency_ms" not in df.columns:
        raise HTTPException(
            status_code=500,
            detail="Telemetry data must include 'region' and 'latency' (or 'latency_ms') fields."
        )

    out: Dict[str, Any] = {}
    for region in regions:
        # case-insensitive match
        sub = df[df["region"].astype(str).str.lower() == str(region).lower()]
        if sub.empty:
            out[region] = {"avg_latency": None, "p95_latency": None, "avg_uptime": None, "breaches": 0}
            continue

        lat = pd.to_numeric(sub["latency_ms"], errors="coerce").dropna()
        if lat.empty:
            avg_latency = None
            p95 = None
            breaches = 0
        else:
            avg_latency = float(lat.mean())
            p95 = float(np.percentile(lat, 95))
            breaches = int((lat > float(threshold)).sum())

        # uptime handling: normalize percents >1 to 0-1 range
        if "uptime" in sub.columns:
            up = pd.to_numeric(sub["uptime"], errors="coerce").dropna()
            if not up.empty and (up > 1).any():
                up = up / 100.0
            avg_uptime = float(up.mean()) if not up.empty else None
        else:
            avg_uptime = None

        out[region] = {
            "avg_latency": round(avg_latency, 3) if avg_latency is not None else None,
            "p95_latency": round(p95, 3) if p95 is not None else None,
            "avg_uptime": round(avg_uptime, 6) if avg_uptime is not None else None,
            "breaches": breaches,
        }

    return out
