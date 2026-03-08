"""
Core abstract interfaces for OculusVorago.

All Extractors, Mappers, and Loaders must implement these contracts.
This keeps the engine fully decoupled from any concrete technology.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterator
from typing import Any

from vorago.core.config_models import MappingConfig


class IExtractor(ABC):
    """
    Plugin interface for data sources.

    Implementations must yield records one at a time so that arbitrarily
    large datasets (e.g. 50 GB CSV files) can be processed without loading
    the entire dataset into memory.
    """

    @abstractmethod
    def extract(self, source_uri: str) -> Iterator[dict[str, Any]]:
        """
        Open *source_uri* and yield one record (dict) at a time.

        Args:
            source_uri: A path, URL, or connection string that identifies
                        the data source. The meaning is implementation-specific.

        Yields:
            A single raw data record represented as a flat dictionary.
        """


class IMapper(ABC):
    """
    Plugin interface for record transformation.

    A single raw record may be transformed into multiple output payloads
    (e.g. one PERSON entity and one COMPANY entity with an edge between them).
    """

    @abstractmethod
    def map_record(
        self, record: dict[str, Any], config: MappingConfig
    ) -> list[dict[str, Any]]:
        """
        Transform a raw record into one or more OculusOntologia payloads.

        Args:
            record: A raw dictionary yielded by an IExtractor.
            config: The MappingConfig that describes how fields should be mapped.

        Returns:
            A list of Kafka-ready payload dictionaries, each with the keys:
            ``source_system``, ``entity_type``, ``classification``,
            ``properties``.
        """


class ILoader(ABC):
    """
    Plugin interface for data sinks.

    Implementations receive an iterator of already-mapped payloads and
    write them to some destination (Kafka, a file, a database, etc.).
    """

    @abstractmethod
    def load(self, records: Iterator[dict[str, Any]]) -> None:
        """
        Consume *records* and write them to the target sink.

        Args:
            records: An iterator of mapped payload dictionaries.
        """
