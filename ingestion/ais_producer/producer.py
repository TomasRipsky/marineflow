# =============================================================================
# MARINEFLOW — Kafka Publisher
# ingestion/ais_producer/producer.py
#
# Handles all communication with the local Kafka broker.
# Uses librdkafka's internal batching (linger.ms / batch.num.messages) and
# keys each message by MMSI so all messages for a given vessel land on the
# same partition — preserves per-vessel ordering downstream in Spark.
# =============================================================================

import json

import structlog
from confluent_kafka import Producer, KafkaException
from tenacity import retry, stop_after_attempt, wait_exponential

from config import Config

logger = structlog.get_logger(__name__)


class KafkaPublisher:
    """
    Publishes normalized AIS messages to the local Kafka broker.

    librdkafka batches internally per linger.ms/batch.num.messages — no manual
    batching needed on the Python side. Delivery reports are handled async via
    callback and drained with periodic non-blocking poll(0) calls.
    """

    def __init__(self) -> None:
        producer_config = {
            "bootstrap.servers": Config.KAFKA_BOOTSTRAP_SERVERS,
            "linger.ms": int(Config.KAFKA_BATCH_MAX_LATENCY * 1000),
            "batch.num.messages": Config.KAFKA_BATCH_MAX_MESSAGES,
            "compression.type": "snappy",
            "acks": "1",  # leader ack only — enough for this use case, faster than "all"
            "retries": 3,
            "retry.backoff.ms": 500,
        }
        self._producer = Producer(producer_config)

        self._topics = {
            "positions": Config.TOPIC_POSITIONS,
            "metadata": Config.TOPIC_METADATA,
            "dlq": Config.TOPIC_DLQ,
        }

        # In-memory counters for monitoring
        # Per-topic counters, e.g. {"positions": 0, "metadata": 0, "dlq": 0}
        self._published_count = {k: 0 for k in self._topics}
        self._error_count = 0

        logger.info(
            "kafka_publisher_initialized",
            bootstrap_servers=Config.KAFKA_BOOTSTRAP_SERVERS,
            topics=list(self._topics.values()),
        )

    def publish(self, data: dict, topic_key: str) -> None:
        """
        Publish a single message to a Kafka topic.

        Args:
            data: Dict to serialize as JSON and publish.
            topic_key: One of 'positions', 'metadata', 'dlq'.
        """
        topic = self._topics.get(topic_key)
        if not topic:
            logger.error("unknown_topic_key", topic_key=topic_key)
            return

        try:
            payload = json.dumps(data, default=str).encode("utf-8")
            mmsi = data.get("mmsi")
            key = str(mmsi).encode("utf-8") if mmsi is not None else None

            headers = [
                ("source", Config.MESSAGE_SOURCE.encode("utf-8")),
                ("message_type", str(data.get("MessageType", "unknown")).encode("utf-8")),
            ]

            self._producer.produce(
                topic=topic,
                key=key,
                value=payload,
                headers=headers,
                callback=self._on_delivery,
            )
            # Non-blocking — serves any pending delivery callbacks without
            # waiting for the broker. Without this, callbacks only fire on
            # the next produce()/flush(), which can lag under low throughput.
            self._producer.poll(0)
            self._published_count[topic_key] += 1

        except BufferError:
            # Local librdkafka queue is full — broker is slower than the
            # producer. Block briefly to drain instead of dropping the message.
            logger.warning("kafka_queue_full_backpressure", topic=topic_key)
            self._producer.poll(1.0)
        except Exception as e:
            self._error_count += 1
            logger.error(
                "publish_error",
                topic=topic_key,
                error=str(e),
                mmsi=data.get("mmsi"),
            )
            self._send_to_dlq(data, str(e))

    def _on_delivery(self, err, msg) -> None:
        """Delivery report callback — fired async by poll()/flush()."""
        if err is not None:
            self._error_count += 1
            logger.error("delivery_failed", error=str(err), topic=msg.topic())

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
        try:
            self._producer.produce(topic=self._topics["dlq"], value=payload)
            self._producer.poll(0)
        except KafkaException as e:
            logger.error("dlq_publish_failed", error=str(e))
            raise

    def get_stats(self) -> dict:
        """Return current publishing statistics, broken down by topic."""
        return {
            "published": dict(self._published_count),
            "published_total": sum(self._published_count.values()),
            "errors": self._error_count,
        }

    def shutdown(self) -> None:
        """Flush all pending messages (blocks until delivered or timeout) and close."""
        remaining = self._producer.flush(timeout=10)
        if remaining > 0:
            logger.warning("kafka_shutdown_messages_not_flushed", remaining=remaining)
        logger.info(
            "kafka_publisher_shutdown",
            published_positions=self._published_count["positions"],
            published_metadata=self._published_count["metadata"],
            published_dlq=self._published_count["dlq"],
            total_published=sum(self._published_count.values()),
            total_errors=self._error_count,
        )
