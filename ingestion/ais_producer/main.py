# =============================================================================
# MARINEFLOW — AIS Producer Entry Point
# ingestion/ais_producer/main.py
#
# Connects to aisstream.io via WebSocket, receives AIS messages in real time,
# parses them and publishes to the corresponding Pub/Sub topic.
#
# Usage:
#   python main.py
# =============================================================================

import asyncio
import json
import signal
import sys

import structlog
import websockets
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    RetryError,
)

from config import Config
from parser import parse_message
from producer import PubSubPublisher

# =============================================================================
# Logging setup — structured JSON logs for GCP Cloud Logging compatibility
# =============================================================================
structlog.configure(
    processors=[
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.JSONRenderer()
        if Config.LOG_FORMAT == "json"
        else structlog.dev.ConsoleRenderer(),
    ],
    logger_factory=structlog.PrintLoggerFactory(),
)

logger = structlog.get_logger(__name__)


# =============================================================================
# WebSocket connection and message loop
# =============================================================================

async def subscribe(ws) -> None:
    """Send the subscription message to aisstream.io after connecting."""
    subscription = {
        "APIKey": Config.AIS_API_KEY,
        "BoundingBoxes": Config.AIS_BOUNDING_BOXES,
        "FilterMessageTypes": Config.AIS_MESSAGE_TYPES,
    }
    await ws.send(json.dumps(subscription))
    logger.info(
        "ais_subscribed",
        message_types=Config.AIS_MESSAGE_TYPES,
        bounding_boxes=len(Config.AIS_BOUNDING_BOXES),
    )


async def process_message(raw_message: str, publisher: PubSubPublisher) -> None:
    """
    Parse a single raw WebSocket message and publish it to Pub/Sub.
    Invalid or unhandled messages are silently skipped.
    """
    try:
        raw = json.loads(raw_message)
    except json.JSONDecodeError:
        logger.warning("invalid_json_message")
        return

    payload, topic_key = parse_message(raw)

    if payload is None or topic_key is None:
        # Message type not handled (e.g. BaseStationReport) — skip silently
        return

    publisher.publish(payload, topic_key)


@retry(
    stop=stop_after_attempt(10),
    wait=wait_exponential(multiplier=2, min=2, max=60),
)
async def connect_and_stream(publisher: PubSubPublisher) -> None:
    """
    Main streaming loop — connects to aisstream.io and processes messages.

    Automatically retries with exponential backoff on disconnection.
    After 10 failed attempts, raises RetryError and the process exits.
    """
    logger.info("connecting_to_aisstream", url=Config.AIS_WS_URL)

    async with websockets.connect(
        Config.AIS_WS_URL,
        open_timeout=30,
        ping_interval=20,
        ping_timeout=10,
        close_timeout=10,
        max_size=2**23,
    ) as ws:
        await subscribe(ws)
        logger.info("streaming_started")

        msg_count = 0
        async for raw_message in ws:
            await process_message(raw_message, publisher)
            msg_count += 1

            # Log stats every 1000 messages
            if msg_count % 1000 == 0:
                stats = publisher.get_stats()
                logger.info(
                    "producer_heartbeat",
                    messages_received=msg_count,
                    **stats,
                )


# =============================================================================
# Graceful shutdown
# =============================================================================

shutdown_event = asyncio.Event()


def handle_shutdown(sig, frame) -> None:
    """Handle SIGINT/SIGTERM for graceful shutdown."""
    logger.info("shutdown_signal_received", signal=sig)
    shutdown_event.set()


# =============================================================================
# Main entry point
# =============================================================================

async def main() -> None:
    # Validate config before doing anything
    try:
        Config.validate()
    except ValueError as e:
        logger.error("config_validation_failed", error=str(e))
        sys.exit(1)

    logger.info(
        "ais_producer_starting",
        project=Config.GCP_PROJECT_ID,
        source=Config.MESSAGE_SOURCE,
    )

    publisher = PubSubPublisher()

    # Register shutdown handlers
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    try:
        # Run the streaming loop — retries automatically on disconnection
        stream_task = asyncio.create_task(connect_and_stream(publisher))
        shutdown_task = asyncio.create_task(shutdown_event.wait())

        # Wait for either the stream to fail or a shutdown signal
        done, pending = await asyncio.wait(
            [stream_task, shutdown_task],
            return_when=asyncio.FIRST_COMPLETED,
        )

        # Cancel any remaining tasks
        for task in pending:
            task.cancel()

    except RetryError:
        logger.error("max_retries_exceeded_exiting")
    except Exception as e:
        logger.error("unexpected_error", error=str(e))
    finally:
        publisher.shutdown()
        logger.info("ais_producer_stopped")


if __name__ == "__main__":
    asyncio.run(main())