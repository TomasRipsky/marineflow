# =============================================================================
# MARINEFLOW — Pub/Sub Publisher
# ingestion/ais_producer/producer.py
#
# Handles all communication with Google Cloud Pub/Sub.
# Uses batching for efficiency and routes messages to the correct topic.
# =============================================================================

import json
from typing import Optional

import structlog
from google.cloud import pubsub_v1
from google.api_core.exceptions import GoogleAPICallError
from tenacity import retry, stop_after_attempt, wait_exponential

from config import Config

logger = structlog.get_logger(__name__)


class PubSubPublisher:
    """
    Publishes normalized AIS messages to Google Cloud Pub/Sub.

    Uses BatchSettings for efficient publishing — messages are buffered
    and sent in batches rather than one by one, reducing API calls significantly.
    """

    def __init__(self) -> None:
        # Batch settings — tune these to balance latency vs. throughput
        batch_settings = pubsub_v1.types.BatchSettings(
            max_messages=Config.PUBSUB_BATCH_MAX_MESSAGES,
            max_latency=Config.PUBSUB_BATCH_MAX_LATENCY,
            max_bytes=1024 * 1024,  # 1MB max batch size
        )

        self._client = pubsub_v1.PublisherClient(batch_settings=batch_settings)
        self._project_id = Config.GCP_PROJECT_ID

        # Pre-build topic paths to avoid recomputing them on every message
        self._topics = {
            "positions": self._topic_path(Config.TOPIC_POSITIONS),
            "metadata": self._topic_path(Config.TOPIC_METADATA),
            "dlq": self._topic_path(Config.TOPIC_DLQ),
        }

        # In-memory counters for monitoring
        self._published_count = 0
        self._error_count = 0

        logger.info(
            "pubsub_publisher_initialized",
            project=self._project_id,
            topics=list(self._topics.keys()),
        )

    def _topic_path(self, topic_name: str) -> str:
        """Build the full Pub/Sub topic resource path."""
        return self._client.topic_path(self._project_id, topic_name)

    def publish(self, data: dict, topic_key: str) -> None:
        """
        Publish a single message to a Pub/Sub topic.

        Args:
            data: Dict to serialize as JSON and publish.
            topic_key: One of 'positions', 'metadata', 'dlq'.
        """
        topic_path = self._topics.get(topic_key)
        if not topic_path:
            logger.error("unknown_topic_key", topic_key=topic_key)
            return

        try:
            payload = json.dumps(data, default=str).encode("utf-8")

            # Attach attributes for server-side filtering if needed
            attributes = {
                "source": Config.MESSAGE_SOURCE,
                "message_type": data.get("message_type", "unknown"),
            }

            future = self._client.publish(topic_path, payload, **attributes)
            future.add_done_callback(self._on_publish_done)
            self._published_count += 1

        except Exception as e:
            self._error_count += 1
            logger.error(
                "publish_error",
                topic=topic_key,
                error=str(e),
                mmsi=data.get("mmsi"),
            )
            self._send_to_dlq(data, str(e))

    def _on_publish_done(self, future) -> None:
        """Callback executed when a publish future resolves."""
        try:
            future.result()
        except Exception as e:
            self._error_count += 1
            logger.error("publish_future_error", error=str(e))

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
    )
    def _send_to_dlq(self, original_data: dict, error_message: str) -> None:
        """
        Send a failed message to the Dead Letter Queue topic.
        Retried up to 3 times with exponential backoff.
        """
        dlq_payload = {
            "original_data": original_data,
            "error": error_message,
            "source": Config.MESSAGE_SOURCE,
        }
        payload = json.dumps(dlq_payload, default=str).encode("utf-8")
        self._client.publish(self._topics["dlq"], payload)

    def get_stats(self) -> dict:
        """Return current publishing statistics."""
        return {
            "published": self._published_count,
            "errors": self._error_count,
        }

    def shutdown(self) -> None:
        """Flush all pending messages and close the client."""
        self._client.stop()
        logger.info(
            "pubsub_publisher_shutdown",
            total_published=self._published_count,
            total_errors=self._error_count,
        )