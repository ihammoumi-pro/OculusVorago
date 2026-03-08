"""
Kafka loader plugin for OculusVorago.

Publishes mapped payloads to the ``oculus.ingestion.raw`` Kafka topic
(or whichever topic is configured in the :class:`~vorago.core.config_models.MappingConfig`).

Batching strategy:
    Records are accumulated in memory until *flush_every* records have
    been queued, at which point ``producer.flush()`` is called.  A final
    flush is always performed at the end of the iterator.

Dependencies:
    ``confluent-kafka`` must be installed::

        pip install confluent-kafka
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterator
from typing import Any

from vorago.core.interfaces import ILoader

logger = logging.getLogger(__name__)


class KafkaLoader(ILoader):
    """
    Confluent-Kafka producer loader.

    Args:
        bootstrap_servers: Comma-separated list of Kafka broker addresses.
        topic:             Target Kafka topic.
        flush_every:       Flush the producer after this many records to
                           control throughput vs. latency.  Default 1 000.
        producer_config:   Extra ``confluent_kafka.Producer`` configuration
                           options merged on top of the defaults.
    """

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topic: str = "oculus.ingestion.raw",
        flush_every: int = 1_000,
        producer_config: dict[str, Any] | None = None,
    ) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.flush_every = flush_every
        self._extra_config: dict[str, Any] = producer_config or {}
        self._producer = None  # Lazy-initialised in load()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def load(self, records: Iterator[dict[str, Any]]) -> None:
        """
        Consume *records* and publish each one to Kafka as a JSON message.

        Args:
            records: Iterator of mapped payload dicts.
        """
        producer = self._get_producer()
        published = 0
        errors = 0

        for record in records:
            try:
                payload_bytes = json.dumps(record, default=str).encode("utf-8")
                producer.produce(
                    topic=self.topic,
                    value=payload_bytes,
                    on_delivery=self._delivery_callback,
                )
                published += 1

                if published % self.flush_every == 0:
                    queued = producer.flush(timeout=30)
                    logger.info(
                        "KafkaLoader: flushed batch — %d published so far "
                        "(%d still queued)",
                        published,
                        queued,
                    )

            except Exception as exc:  # noqa: BLE001
                errors += 1
                logger.error("KafkaLoader: failed to produce record — %s", exc)

        # Final flush to drain any remaining messages.
        remaining = producer.flush(timeout=60)
        if remaining > 0:
            logger.warning(
                "KafkaLoader: %d messages still in queue after final flush", remaining
            )

        logger.info(
            "KafkaLoader: finished — %d records published, %d errors", published, errors
        )

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _get_producer(self) -> Any:
        """Lazy-initialise and return the confluent_kafka Producer."""
        if self._producer is not None:
            return self._producer
        try:
            from confluent_kafka import Producer  # type: ignore[import-untyped]
        except ImportError as exc:
            raise ImportError(
                "confluent-kafka is required for KafkaLoader.  "
                "Install it with: pip install confluent-kafka"
            ) from exc

        config = {
            "bootstrap.servers": self.bootstrap_servers,
            "linger.ms": 5,
            "batch.num.messages": self.flush_every,
            "compression.type": "lz4",
            **self._extra_config,
        }
        self._producer = Producer(config)
        logger.info(
            "KafkaLoader: producer initialised (brokers=%s, topic=%s)",
            self.bootstrap_servers,
            self.topic,
        )
        return self._producer

    @staticmethod
    def _delivery_callback(err: Any, msg: Any) -> None:
        """Called by confluent-kafka for each message after delivery attempt."""
        if err:
            logger.error(
                "KafkaLoader: delivery failed for message on topic '%s' — %s",
                msg.topic() if msg else "unknown",
                err,
            )
        else:
            logger.debug(
                "KafkaLoader: delivered to %s [partition %d] @ offset %d",
                msg.topic(),
                msg.partition(),
                msg.offset(),
            )
