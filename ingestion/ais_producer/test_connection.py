"""Manual check that aisstream.io accepts your API key.

Connects the same way the producer does (same URL, default TLS certificate verification),
subscribes to a small box over the Port of Rotterdam and prints the first message received.

    cd ingestion/ais_producer && python test_connection.py

It needs AIS_API_KEY in the environment or in .env. The key itself is never printed.
"""
import asyncio
import json
import sys

import websockets

from config import Config


async def check() -> int:
    if not Config.AIS_API_KEY:
        print("AIS_API_KEY is not set")
        return 2
    print("AIS_API_KEY is set")

    try:
        async with websockets.connect(Config.AIS_WS_URL, open_timeout=20) as ws:
            print("Connected successfully")
            await ws.send(json.dumps({
                "APIKey": Config.AIS_API_KEY,
                "BoundingBoxes": [[[51.0, 3.0], [52.0, 5.0]]],
                "FilterMessageTypes": ["PositionReport"],
            }))
            print("Subscription sent, waiting for a message...")
            message = await asyncio.wait_for(ws.recv(), timeout=20)
            print(f"First message: {message[:300]}")
    except Exception as error:
        print(f"Error: {type(error).__name__}: {error}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(check()))
