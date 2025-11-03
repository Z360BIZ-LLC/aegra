"""
Example webhook receiver for testing Aegra webhook notifications.

Run this in one terminal:
    python test_webhook_receiver.py

Then run test_agent.py in another terminal to trigger the webhook.
"""

import asyncio
import json
from datetime import datetime
from pathlib import Path
from fastapi import FastAPI, Request
import uvicorn

app = FastAPI()

# Webhook logs directory (mounted as volume)
WEBHOOK_DIR = Path("/app/webhook_logs")


@app.post("/webhook/langgraph")
async def receive_webhook(request: Request):
    """Receive webhook notifications from Aegra"""
    payload = await request.json()

    # Save to file with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_id = payload.get('run_id', 'unknown')[:8]  # First 8 chars of run_id
    filename = f"webhook_{timestamp}_{run_id}.json"
    filepath = WEBHOOK_DIR / filename

    with open(filepath, 'w') as f:
        json.dump(payload, f, indent=2)

    print(payload)
    print("\n" + "=" * 60)
    print("WEBHOOK RECEIVED!")
    print("=" * 60)
    print(f"Run ID: {payload.get('run_id')}")
    print(f"Thread ID: {payload.get('thread_id')}")
    print(f"Status: {payload.get('status')}")
    print(f"Timestamp: {payload.get('webhook_sent_at')}")
    print(f"Saved to: {filepath}")

    if "values" in payload and payload.get('values'):
        print(f"\nValues (Output):")
        print(json.dumps(payload.get('values'), indent=2))


    if "error" in payload:
        print(f"\nError: {payload.get('error')}")

    print("=" * 60 + "\n")

    return {"status": "received", "saved_to": str(filepath)}


@app.get("/health")
async def health():
    return {"status": "ok"}


if __name__ == "__main__":
    print("Starting webhook receiver on http://localhost:8002")
    print("Waiting for webhooks from Aegra...\n")
    uvicorn.run(app, host="0.0.0.0", port=8002)
