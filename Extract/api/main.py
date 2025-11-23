from fastapi import FastAPI
from pydantic import BaseModel
from typing import List, Any
from producer.producer import send_to_kafka # import module producer

app = FastAPI()

class Payload(BaseModel):
    symbol: str
    page: int
    headers: List[str]
    data: List[List[Any]]

@app.post("/push")
def push_data(payload: Payload):
    print(f"📥 Nhận dữ liệu: {payload.symbol} | page {payload.page}")

    # --- Chuyển list of lists -> list of dict ---
    formatted_data = [
        {h: v for h, v in zip(payload.headers, row)} for row in payload.data
    ]

    kafka_msg = {
        "symbol": payload.symbol,
        "page": payload.page,
        "data": formatted_data
    }

    send_to_kafka(payload.symbol, kafka_msg)

    return {
        "status": "ok",
        "symbol": payload.symbol,
        "page": payload.page,
        "rows_sent": len(formatted_data)
    }
