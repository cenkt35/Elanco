import os
import json
import time
import hashlib
import logging
from datetime import datetime, date, timedelta
from typing import List, Optional, Dict, Any
from collections import defaultdict, Counter

import httpx
from fastapi import FastAPI, Query, HTTPException, Path
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("elanco-tick-backend")

UPSTREAM_BASE_URL = "https://dev-task.elancoapps.com/data"
CACHE_TTL_SECONDS = 600
LOCAL_SNAPSHOT_FILE = "sightings_cache.json"

# Robust Fallback Mock Data so the API always works seamlessly for your video demo
FALLBACK_MOCK_DATA = [
    {
        "id": "tick-lon-001",
        "date": "2025-08-14T17:30:00",
        "location": "London",
        "species": "Southern rodent tick",
        "latinName": "Ixodes acuminatus"
    },
    {
        "id": "tick-man-002",
        "date": "2025-08-20T10:15:00",
        "location": "Manchester",
        "species": "Castor bean tick",
        "latinName": "Ixodes ricinus"
    },
    {
        "id": "tick-edi-003",
        "date": "2025-08-22T14:00:00",
        "location": "Edinburgh",
        "species": "Hedgehog tick",
        "latinName": "Ixodes hexagonus"
    },
    {
        "id": "tick-lon-004",
        "date": "2025-09-01T09:45:00",
        "location": "London",
        "species": "Castor bean tick",
        "latinName": "Ixodes ricinus"
    },
    {
        "id": "tick-birm-005",
        "date": "2025-09-05T16:20:00",
        "location": "Birmingham",
        "species": "Ornate dog tick",
        "latinName": "Dermacentor reticulatus"
    }
]

app = FastAPI(
    title="Elanco UK Tick Sightings Analytics & Reporting API",
    description="Backend MVP for consuming, deduplicating, and analysing UK tick sighting data from Elanco API.",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

class NormalizedSighting(BaseModel):
    id: str
    date: str
    timestamp: str
    location: str
    species: str
    latinName: str
    is_complete: bool
    missing_fields: List[str] = []

class DataPipelineSummary(BaseModel):
    total_raw_records: int
    valid_unique_records: int
    duplicates_removed: int
    incomplete_records_cleaned: int
    incomplete_records_dropped: int
    last_synced_utc: str
    upstream_status: str

SPECIES_TO_LATIN = {
    "southern rodent tick": "Ixodes acuminatus",
    "castor bean tick": "Ixodes ricinus",
    "sheep tick": "Ixodes ricinus",
    "hedgehog tick": "Ixodes hexagonus",
    "ornate dog tick": "Dermacentor reticulatus",
}
LATIN_TO_SPECIES = {v.lower(): k.title() for k, v in SPECIES_TO_LATIN.items()}

class SightingRepository:
    def __init__(self):
        self.records: List[NormalizedSighting] = []
        self.by_id: Dict[str, NormalizedSighting] = {}
        self.last_fetched: float = 0.0
        self.pipeline_stats: Dict[str, Any] = {
            "total_raw_records": 0,
            "valid_unique_records": 0,
            "duplicates_removed": 0,
            "incomplete_records_cleaned": 0,
            "incomplete_records_dropped": 0,
            "last_synced_utc": "Never",
            "upstream_status": "uninitialized",
        }

    def _parse_datetime(self, raw_val: Any) -> Optional[tuple[str, str]]:
        if not raw_val or not str(raw_val).strip():
            return None
        raw_str = str(raw_val).strip()
        try:
            dt = datetime.fromisoformat(raw_str.replace("Z", "+00:00"))
            return dt.strftime("%Y-%m-%d"), dt.strftime("%Y-%m-%dT%H:%M:%S")
        except ValueError:
            pass
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
            try:
                dt = datetime.strptime(raw_str[:19], fmt)
                return dt.strftime("%Y-%m-%d"), dt.strftime("%Y-%m-%dT%H:%M:%S")
            except ValueError:
                continue
        return None

    def _normalize_record(self, raw: Dict[str, Any]) -> Optional[NormalizedSighting]:
        if not isinstance(raw, dict):
            return None
        missing_fields = []
        parsed_dt = self._parse_datetime(raw.get("date"))
        if not parsed_dt:
            return None
        norm_date, norm_timestamp = parsed_dt

        raw_loc = raw.get("location") or raw.get("city")
        if not raw_loc or not str(raw_loc).strip():
            missing_fields.append("location")
            norm_loc = "Unknown Location"
        else:
            norm_loc = str(raw_loc).strip().title()

        raw_species = raw.get("species")
        raw_latin = raw.get("latinName") or raw.get("latin_name")
        norm_species = str(raw_species).strip() if raw_species and str(raw_species).strip() else ""
        norm_latin = str(raw_latin).strip() if raw_latin and str(raw_latin).strip() else ""

        if not norm_species:
            missing_fields.append("species")
            norm_species = LATIN_TO_SPECIES.get(norm_latin.lower(), "Unidentified Tick")
        if not norm_latin:
            missing_fields.append("latinName")
            norm_latin = SPECIES_TO_LATIN.get(norm_species.lower(), "Ixodes spp.")

        raw_id = raw.get("id")
        if not raw_id or not str(raw_id).strip():
            missing_fields.append("id")
            composite = f"{norm_timestamp}|{norm_loc}|{norm_species}|{norm_latin}"
            norm_id = hashlib.md5(composite.encode("utf-8")).hexdigest()[:20]
        else:
            norm_id = str(raw_id).strip()

        return NormalizedSighting(
            id=norm_id,
            date=norm_date,
            timestamp=norm_timestamp,
            location=norm_loc,
            species=norm_species,
            latinName=norm_latin,
            is_complete=(len(missing_fields) == 0),
            missing_fields=missing_fields,
        )

    def ingest_raw_data(self, raw_list: List[Dict[str, Any]], status: str = "healthy"):
        seen_ids = set()
        seen_signatures = set()
        cleaned_records: List[NormalizedSighting] = []
        duplicates = 0
        incomplete_cleaned = 0
        incomplete_dropped = 0

        for raw in raw_list:
            norm = self._normalize_record(raw)
            if norm is None:
                incomplete_dropped += 1
                continue
            signature = f"{norm.timestamp}|{norm.location.lower()}|{norm.latinName.lower()}"
            if norm.id in seen_ids or signature in seen_signatures:
                duplicates += 1
                continue
            seen_ids.add(norm.id)
            seen_signatures.add(signature)
            if not norm.is_complete:
                incomplete_cleaned += 1
            cleaned_records.append(norm)

        cleaned_records.sort(key=lambda r: r.timestamp, reverse=True)
        self.records = cleaned_records
        self.by_id = {r.id: r for r in cleaned_records}
        self.last_fetched = time.time()
        self.pipeline_stats = {
            "total_raw_records": len(raw_list),
            "valid_unique_records": len(cleaned_records),
            "duplicates_removed": duplicates,
            "incomplete_records_cleaned": incomplete_cleaned,
            "incomplete_records_dropped": incomplete_dropped,
            "last_synced_utc": datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "upstream_status": status,
        }

    def ensure_fresh_data(self, force_refresh: bool = False):
        now = time.time()
        if not force_refresh and self.records and (now - self.last_fetched < CACHE_TTL_SECONDS):
            return

        url = f"{UPSTREAM_BASE_URL}/tick-sightings"
        last_error = None

        try:
            with httpx.Client(timeout=4.0, follow_redirects=True) as client:
                resp = client.get(url)
                if resp.status_code == 200:
                    raw_list = resp.json()
                    if isinstance(raw_list, dict):
                        raw_list = raw_list.get("data") or raw_list.get("sightings") or []
                    self.ingest_raw_data(raw_list, status="healthy_live_upstream")
                    return
        except Exception as exc:
            last_error = str(exc)

        # Fallback to local mock data so the app always functions seamlessly
        logger.warning(f"Upstream API unreachable ({last_error}). Using robust fallback mock data.")
        self.ingest_raw_data(FALLBACK_MOCK_DATA, status=f"fallback_mock_active (upstream error: {last_error})")

repo = SightingRepository()

@app.get("/")
def root():
    return {"service": "Elanco UK Tick Sightings Backend MVP", "docs": "/docs"}

@app.get("/api/pipeline/status", response_model=DataPipelineSummary)
def get_pipeline_status(refresh: bool = Query(False)):
    repo.ensure_fresh_data(force_refresh=refresh)
    return repo.pipeline_stats

@app.get("/api/sightings")
def search_and_filter_sightings(
    location: Optional[str] = Query(None),
    species: Optional[str] = Query(None),
    start_date: Optional[date] = Query(None),
    end_date: Optional[date] = Query(None),
    complete_only: bool = Query(False),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    if start_date and end_date and start_date > end_date:
        raise HTTPException(status_code=400, detail={"error": "InvalidDateRange"})
    repo.ensure_fresh_data()
    results = repo.records

    if location:
        loc_q = location.strip().lower()
        results = [r for r in results if loc_q in r.location.lower()]
    if species:
        sp_q = species.strip().lower()
        results = [r for r in results if sp_q in r.species.lower() or sp_q in r.latinName.lower()]
    if start_date:
        results = [r for r in results if r.date >= start_date.strftime("%Y-%m-%d")]
    if end_date:
        results = [r for r in results if r.date <= end_date.strftime("%Y-%m-%d")]
    if complete_only:
        results = [r for r in results if r.is_complete]

    total_matches = len(results)
    paginated = results[offset : offset + limit]
    return {"total_matches": total_matches, "results": paginated}

@app.get("/api/reports/regions")
def report_sightings_per_region():
    repo.ensure_fresh_data()
    records = repo.records
    total_count = len(records)
    by_region: Dict[str, List] = defaultdict(list)
    for r in records:
        by_region[r.location].append(r)

    region_reports = []
    for region, items in by_region.items():
        sp_counts = Counter(f"{i.species} ({i.latinName})" for i in items)
        region_reports.append({
            "location": region,
            "sightings_count": len(items),
            "percentage_of_total": round((len(items) / total_count) * 100, 2) if total_count else 0.0,
            "dominant_species": sp_counts.most_common(1)[0][0] if sp_counts else "None",
        })
    region_reports.sort(key=lambda x: x["sightings_count"], reverse=True)
    return {"total_sightings_analysed": total_count, "regions": region_reports}

@app.get("/api/reports/trends")
def report_trends_over_time(interval: str = Query("monthly", pattern="^(weekly|monthly)$")):
    repo.ensure_fresh_data()
    records = repo.records
    buckets = defaultdict(list)
    for r in records:
        dt = datetime.strptime(r.date, "%Y-%m-%d").date()
        key = r.date[:7] if interval == "monthly" else f"{dt.isocalendar()[0]}-W{dt.isocalendar()[1]:02d}"
        buckets[key].append(r)

    timeline = []
    prev_count = None
    for key in sorted(buckets.keys()):
        count = len(buckets[key])
        growth_pct = round(((count - prev_count) / prev_count) * 100, 1) if prev_count and prev_count > 0 else None
        timeline.append({"period": key, "sightings_count": count, "period_over_period_change_pct": growth_pct})
        prev_count = count
    return {"interval": interval, "trends": timeline}

@app.get("/api/insights/risk-forecast")
def get_ai_ml_risk_insights():
    repo.ensure_fresh_data()
    records = repo.records
    if not records:
        return {"regional_forecasts": []}
    by_region = defaultdict(list)
    for r in records:
        by_region[r.location].append(r)
    forecasts = []
    for region, items in by_region.items():
        score = min(len(items) * 20.0, 100.0)
        forecasts.append({"location": region, "outbreak_risk_score": score, "total_sightings": len(items)})
    forecasts.sort(key=lambda x: x["outbreak_risk_score"], reverse=True)
    return {"regional_forecasts": forecasts}