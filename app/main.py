import psycopg2
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

import shared.config as config
from shared.models import OHLCVResponseRecord

app = FastAPI(
    title="Vectoro OHLCV API",
    description="API for serving historical OHLCV data from QuestDB.",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET"],
    allow_headers=["*"],
)

# pgwire connection
QUESTDB_CONN_STR = (
    f"user=admin password=quest host={config.QUESTDB_HOST} port=8812 dbname=qdb"
)

@app.get("/")
def read_root():
    return {"status": "ok", "message": "Welcome to the OHLCV Data API!"}


@app.get(
    "/ohlcv/{ticker}",
    response_model=list[OHLCVResponseRecord],
    summary="Get OHLCV Data for a Single Ticker",
    tags=["OHLCV"],
)
def get_ohlcv_for_ticker(ticker: str):
    """
    Retrieves the full OHLCV history for a given stock ticker.
    """
    query = f"""
        SELECT "Date", "Open", "High", "Low", "Close", "Volume"
        FROM "{config.QUESTDB_TABLE_NAME}"
        WHERE "Ticker" = %s 
        ORDER BY "Date";
    """
    
    results = []
    try:
        with psycopg2.connect(QUESTDB_CONN_STR) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, (ticker.upper(),))
                colnames = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                for row in rows:
                    results.append(dict(zip(colnames, row)))

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database query failed: {e}")

    if not results:
        raise HTTPException(status_code=404, detail=f"Ticker '{ticker}' not found.")

    return results