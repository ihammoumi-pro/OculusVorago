"""
Unit tests for MappingConfig (Pydantic model validation).
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from vorago.core.config_models import (
    EntityMapping,
    LinkMapping,
    MappingConfig,
    PropertyMapping,
)


class TestMappingConfigValidation:
    def test_minimal_valid_config(self) -> None:
        config = MappingConfig(source_system_name="SYS")
        assert config.source_system_name == "SYS"
        assert config.default_classification == "UNCLASSIFIED"
        assert config.kafka_topic == "oculus.ingestion.raw"
        assert config.entities == []
        assert config.links is None

    def test_missing_source_system_raises(self) -> None:
        with pytest.raises(ValidationError):
            MappingConfig()  # type: ignore[call-arg]

    def test_entity_mapping_with_properties(self) -> None:
        config = MappingConfig(
            source_system_name="X",
            entities=[
                EntityMapping(
                    target_entity_type="PERSON",
                    property_mappings={
                        "name": PropertyMapping(source_field="full_name"),
                    },
                )
            ],
        )
        assert len(config.entities) == 1
        assert config.entities[0].target_entity_type == "PERSON"
        assert config.entities[0].property_mappings["name"].source_field == "full_name"

    def test_property_mapping_cast_as_optional(self) -> None:
        pm = PropertyMapping(source_field="col")
        assert pm.cast_as is None

    def test_link_mapping_validation(self) -> None:
        link = LinkMapping(
            source_entity_type="PERSON",
            target_entity_type="COMPANY",
            relationship_type="OFFICER_OF",
        )
        assert link.source_entity_type == "PERSON"
        assert link.relationship_type == "OFFICER_OF"
        assert link.property_mappings == {}

    def test_custom_kafka_topic(self) -> None:
        config = MappingConfig(
            source_system_name="SYS",
            kafka_topic="custom.topic",
        )
        assert config.kafka_topic == "custom.topic"

    def test_classification_override_on_entity(self) -> None:
        entity = EntityMapping(
            target_entity_type="T",
            classification_override="SECRET",
        )
        assert entity.classification_override == "SECRET"

    def test_model_validate_from_dict(self) -> None:
        raw = {
            "source_system_name": "FROM_DICT",
            "default_classification": "CONFIDENTIAL",
            "entities": [
                {
                    "target_entity_type": "ORG",
                    "property_mappings": {
                        "org_name": {"source_field": "name", "cast_as": "str"}
                    },
                }
            ],
        }
        config = MappingConfig.model_validate(raw)
        assert config.source_system_name == "FROM_DICT"
        assert config.default_classification == "CONFIDENTIAL"
        assert config.entities[0].target_entity_type == "ORG"
