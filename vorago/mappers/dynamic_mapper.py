"""
Config-driven dynamic mapper for OculusVorago.

Applies a :class:`~vorago.core.config_models.MappingConfig` to a raw
record and produces one or more Kafka-ready payload dictionaries.

Output payload structure (matches OculusOntologia Kafka consumer):

.. code-block:: json

    {
        "source_system": "ICIJ_Offshore_Leaks",
        "entity_type":   "PERSON",
        "classification": "UNCLASSIFIED",
        "properties": {
            "first_name": "Alice",
            "dob": "1980-01-15"
        }
    }
"""

from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any

from vorago.core.config_models import EntityMapping, LinkMapping, MappingConfig
from vorago.core.interfaces import IMapper

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Supported cast helpers
# ---------------------------------------------------------------------------

_CAST_REGISTRY: dict[str, Any] = {
    "str": str,
    "int": int,
    "float": float,
    "bool": lambda v: str(v).strip().lower() in {"1", "true", "yes", "y"},
    "date": lambda v: _parse_date(v),
    "datetime": lambda v: _parse_datetime(v),
}


def _parse_date(value: Any) -> str:
    """Return an ISO-8601 date string (YYYY-MM-DD) or raise ValueError."""
    if isinstance(value, (date, datetime)):
        return value.strftime("%Y-%m-%d")
    raw = str(value).strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%d-%m-%Y", "%Y%m%d"):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    raise ValueError(f"Cannot parse '{value}' as a date")


def _parse_datetime(value: Any) -> str:
    """Return an ISO-8601 datetime string or raise ValueError."""
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day).isoformat()
    raw = str(value).strip()
    for fmt in (
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d",
        "%d/%m/%Y %H:%M:%S",
        "%m/%d/%Y %H:%M:%S",
    ):
        try:
            return datetime.strptime(raw, fmt).isoformat()
        except ValueError:
            continue
    raise ValueError(f"Cannot parse '{value}' as a datetime")


def _cast_value(value: Any, cast_as: str | None) -> Any:
    """Cast *value* to the type named by *cast_as*.  Returns raw value if no cast."""
    if cast_as is None or cast_as not in _CAST_REGISTRY:
        return value
    caster = _CAST_REGISTRY[cast_as]
    return caster(value)


# ---------------------------------------------------------------------------
# Mapper implementation
# ---------------------------------------------------------------------------


class DynamicMapper(IMapper):
    """
    Configuration-driven mapper.

    No dataset-specific logic lives here.  All field mappings and type
    casts are driven entirely by the :class:`~vorago.core.config_models.MappingConfig`
    passed at call time.
    """

    def map_record(
        self, record: dict[str, Any], config: MappingConfig
    ) -> list[dict[str, Any]]:
        """
        Transform a raw record into a list of Kafka-ready payloads.

        Entity payloads are generated first, then link payloads (if any).

        Args:
            record:  A raw row dict from an IExtractor.
            config:  The active MappingConfig.

        Returns:
            A (possibly empty) list of payload dicts.
        """
        payloads: list[dict[str, Any]] = []

        for entity_mapping in config.entities:
            payload = self._build_entity_payload(record, entity_mapping, config)
            if payload is not None:
                payloads.append(payload)

        if config.links:
            for link_mapping in config.links:
                payload = self._build_link_payload(record, link_mapping, config)
                if payload is not None:
                    payloads.append(payload)

        return payloads

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _build_entity_payload(
        self,
        record: dict[str, Any],
        mapping: EntityMapping,
        config: MappingConfig,
    ) -> dict[str, Any] | None:
        """Build one entity payload dict from a raw record and an EntityMapping."""
        properties: dict[str, Any] = {}
        for target_prop, prop_mapping in mapping.property_mappings.items():
            raw_value = record.get(prop_mapping.source_field)
            if raw_value is None or str(raw_value).strip() == "":
                # Omit null / empty properties rather than sending null to the
                # downstream system.
                continue
            try:
                properties[target_prop] = _cast_value(raw_value, prop_mapping.cast_as)
            except (ValueError, TypeError) as exc:
                logger.warning(
                    "DynamicMapper: cast failed for property '%s' "
                    "(source_field='%s', value=%r, cast_as=%s) — %s",
                    target_prop,
                    prop_mapping.source_field,
                    raw_value,
                    prop_mapping.cast_as,
                    exc,
                )
                # Keep the raw string value rather than dropping the field
                properties[target_prop] = str(raw_value)

        return {
            "source_system": config.source_system_name,
            "entity_type": mapping.target_entity_type,
            "classification": mapping.classification_override or config.default_classification,
            "properties": properties,
        }

    def _build_link_payload(
        self,
        record: dict[str, Any],
        mapping: LinkMapping,
        config: MappingConfig,
    ) -> dict[str, Any] | None:
        """Build one link payload dict from a raw record and a LinkMapping."""
        properties: dict[str, Any] = {}
        for target_prop, prop_mapping in mapping.property_mappings.items():
            raw_value = record.get(prop_mapping.source_field)
            if raw_value is None or str(raw_value).strip() == "":
                continue
            try:
                properties[target_prop] = _cast_value(raw_value, prop_mapping.cast_as)
            except (ValueError, TypeError) as exc:
                logger.warning(
                    "DynamicMapper: link property cast failed '%s' — %s",
                    target_prop,
                    exc,
                )
                properties[target_prop] = str(raw_value)

        return {
            "source_system": config.source_system_name,
            "entity_type": mapping.relationship_type,
            "classification": mapping.classification_override or config.default_classification,
            "properties": {
                **properties,
                "_source_entity_type": mapping.source_entity_type,
                "_target_entity_type": mapping.target_entity_type,
            },
        }
