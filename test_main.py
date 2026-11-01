from fastapi.testclient import TestClient
from main import app, repo

client = TestClient(app)

SAMPLE_RAW_DATA = [
    {
        "id": "Kq3x9ZfV2mTn8aL0pYwB",
        "date": "2025-08-14T17:30:00",
        "location": "London",
        "species": "Southern rodent tick",
        "latinName": "Ixodes acuminatus",
    },
    {
        "id": "Kq3x9ZfV2mTn8aL0pYwB",
        "date": "2025-08-14T17:30:00",
        "location": "London",
        "species": "Southern rodent tick",
        "latinName": "Ixodes acuminatus",
    },
    {
        "date": "2025-08-20T10:15:00",
        "location": "Manchester",
        "species": "Castor bean tick",
    },
    {
        "id": "corrupt-001",
        "location": "Edinburgh",
        "species": "Hedgehog tick",
    },
    {
        "id": "sight-003",
        "date": "2025-07-10T09:00:00",
        "location": "London",
        "species": "Castor bean tick",
        "latinName": "Ixodes ricinus",
    },
]

def setup_module():
    repo.ingest_raw_data(SAMPLE_RAW_DATA, status="unit_test_mock")

def test_pipeline_deduplication_and_imputation():
    resp = client.get("/api/pipeline/status")
    assert resp.status_code == 200
    stats = resp.json()
    assert stats["total_raw_records"] == 5
    assert stats["duplicates_removed"] == 1
    assert stats["incomplete_records_dropped"] == 1
    assert stats["incomplete_records_cleaned"] == 1
    assert stats["valid_unique_records"] == 3

def test_search_filter_by_location_and_date():
    resp = client.get("/api/sightings?location=London&start_date=2025-08-01&end_date=2025-08-31")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total_matches"] == 1
    assert data["results"][0]["id"] == "Kq3x9ZfV2mTn8aL0pYwB"

def test_invalid_date_range_error_handling():
    resp = client.get("/api/sightings?start_date=2025-09-01&end_date=2025-08-01")
    assert resp.status_code == 400
    assert resp.json()["detail"]["error"] == "InvalidDateRange"

def test_regional_reporting():
    resp = client.get("/api/reports/regions")
    assert resp.status_code == 200
    data = resp.json()
    assert "total_sightings_analysed" in data

def test_trends_reporting():
    resp = client.get("/api/reports/trends?interval=monthly")
    assert resp.status_code == 200
    data = resp.json()
    assert "interval" in data

def test_ai_ml_risk_insights():
    resp = client.get("/api/insights/risk-forecast")
    assert resp.status_code == 200
    data = resp.json()
    assert "regional_forecasts" in data