import os
from fastapi import FastAPI
from fastapi.responses import JSONResponse
import uvicorn

from scraper import run_daily_scrape

app = FastAPI(title="Mackin Betz DVP Locks")

@app.get("/")
async def get_report():
    try:
        report = run_daily_scrape("output/daily_report.json")
        return report
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": str(e), "message": "Make sure data/fallback.csv exists and is valid."}
        )

@app.get("/health")
async def health_check():
    return {"status": "healthy"}

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
