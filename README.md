# Elanco Tick Sightings - Backend API (Placement Task)

Hey, this is my backend solution for the Elanco Software Engineer placement taskj. I built a lightweight Python API using FastAPI that pulls data from Elanco's API, cleans it up, handles errors, and runs some analytics/risk scoring on it.

---

## What I Built & How It Works

### 1. Data Handling & Rate Limit Resilience (Handling 429 & 500 Errors)
While checking out the Elanco API, I noticed it can throw `429 Too Many Requests` (Rate Limit) and `500 Server Errors`. To make sure my app doesn't just crash when that happens, I set up a few things:
* **In-Memory + Disk Caching:** Once data is fetched, it gets cached in memory and saved to a local JSON file (`sightings_cache.json`). If the Elanco API goes down or rate-limits me while testing, it falls back to the cache seamlessly.
* **Automatic Retries:** If a transient server error happens, it retries automatically with a short backoff.

### 2. Deduplication & Data Cleaning
Real-world data is messy, so the ingestion pipeline fixes a few things automatically:
* **Deduplication:** It drops exact duplicate IDs and also checks a composite signature (`timestamp + location + latinName`) to catch duplicate crowd-sourced entries submitted under different IDs.
* **Incomplete Data Imputation:** If records are missing a species or Latin name, it cross-imputes them using a built-in UK tick taxonomy dictionary. Corrupt records missing a valid date are safely dropped so they don't break time-series reporting.

### 3. API Endpoints Included
* `GET /api/pipeline/status` – Shows stats on raw records, duplicates removed, and data cleaning metrics.
* `GET /api/sightings` – Search and filter sightings by location (city), species/Latin name, and date range (`start_date`, `end_date`), with pagination (`limit`, `offset`).
* `GET /api/sightings/{id}` – Look up a single sighting by its ID.
* `GET /api/reports/regions` – Aggregates sightings per UK city, shows percentage share of total sightings, and identifies the dominant tick species.
* `GET /api/reports/trends` – Weekly or monthly trend analysis with period-over-period growth percentages.
* `GET /api/insights/risk-forecast` – **(Extension Task)** A custom risk-scoring engine (0–100) using Z-score volume anomalies, 30-day velocity, and high-risk disease vector prevalence (*Ixodes ricinus*, *Dermacentor reticulatus*) to flag high-risk UK areas.

---

## How to Run It Locally

1. **Install dependencies:**
   ```bash
   pip install fastapi uvicorn httpx pytest

## Future Improvements (If I Had More Time)

If I had more time to expand this backend service, I would implement the following enhancements:
1. **Automated Testing & CI/CD Pipeline:** Set up GitHub Actions to automatically run `pytest` on every commit and push.
2. **Database Persistence:** Replace the in-memory repository structure with a lightweight persistent database like SQLite or PostgreSQL using SQLAlchemy.
3. **Advanced Caching & Rate Limiting:** Implement Redis caching to further optimize response times for heavy regional reporting queries.
4. **Enhanced ML Forecasting:** Upgrade the risk-forecast extension from statistical scoring to a proper time-series forecasting model (such as ARIMA or Prophet) for seasonal outbreak predictions.
