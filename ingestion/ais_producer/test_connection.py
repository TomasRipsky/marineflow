import asyncio
import json
import os
import ssl
from dotenv import load_dotenv
import websockets

load_dotenv()

API_KEY = os.getenv("AIS_API_KEY", "")

async def test():
    print(f"API key prefix: {API_KEY[:8]}")

    # Create SSL context that handles schannel renegotiation
    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    ssl_context.options |= ssl.OP_NO_SSLv2
    ssl_context.options |= ssl.OP_NO_SSLv3

    try:
        async with websockets.connect(
            "wss://stream.aisstream.io/v0/stream",
            ssl=ssl_context,
            open_timeout=20,
        ) as ws:
            print("Connected successfully")
            await ws.send(json.dumps({
                "APIKey": API_KEY,
                "BoundingBoxes": [[[51.0, 3.0], [52.0, 5.0]]],
                "FilterMessageTypes": ["PositionReport"]
            }))
            print("Subscription sent, waiting for messages...")
            msg = await asyncio.wait_for(ws.recv(), timeout=20)
            print(f"Message: {msg[:300]}")
    except Exception as e:
        print(f"Error: {type(e).__name__}: {e}")

asyncio.run(test())