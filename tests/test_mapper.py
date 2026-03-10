"""
Unit tests for DynamicMapper.
"""

from __future__ import annotations

import pytest

from vorago.core.config_models import (
    EntityMapping,
    LinkMapping,
    MappingConfig,
    PropertyMapping,
)
from vorago.mappers.dynamic_mapper import DynamicMapper, _cast_value, _parse_date, _parse_datetime

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def simple_config() -> MappingConfig:
    return MappingConfig(
        source_system_name="TEST_SYSTEM",
        default_classification="UNCLASSIFIED",
        entities=[
            EntityMapping(
                target_entity_type="PERSON",
                property_mappings={
                    "full_name": PropertyMapping(source_field="name", cast_as="str"),
                    "age": PropertyMapping(source_field="age_raw", cast_as="int"),
                    "dob": PropertyMapping(source_field="birth_date", cast_as="date"),
                },
            )
        ],
    )


@pytest.fixture()
def config_with_link() -> MappingConfig:
    return MappingConfig(
        source_system_name="TEST_SYSTEM",
        default_classification="UNCLASSIFIED",
        entities=[
            EntityMapping(
                target_entity_type="PERSON",
                property_mappings={
                    "name": PropertyMapping(source_field="officer_name"),
                },
            ),
            EntityMapping(
                target_entity_type="COMPANY",
                property_mappings={
                    "name": PropertyMapping(source_field="company_name"),
                },
            ),
        ],
        links=[
            LinkMapping(
                source_entity_type="PERSON",
                target_entity_type="COMPANY",
                relationship_type="OFFICER_OF",
                property_mappings={
                    "since": PropertyMapping(source_field="start_date", cast_as="date"),
                },
            )
        ],
    )


@pytest.fixture()
def mapper() -> DynamicMapper:
    return DynamicMapper()


# ---------------------------------------------------------------------------
# _cast_value helpers
# ---------------------------------------------------------------------------


class TestCastValue:
    def test_no_cast_returns_raw(self) -> None:
        assert _cast_value("hello", None) == "hello"

    def test_str_cast(self) -> None:
        assert _cast_value(123, "str") == "123"

    def test_int_cast(self) -> None:
        assert _cast_value("42", "int") == 42

    def test_float_cast(self) -> None:
        assert _cast_value("3.14", "float") == pytest.approx(3.14)

    def test_bool_true_values(self) -> None:
        for v in ("1", "true", "True", "TRUE", "yes", "Yes", "y", "Y"):
            assert _cast_value(v, "bool") is True

    def test_bool_false_values(self) -> None:
        for v in ("0", "false", "no", "n", ""):
            assert _cast_value(v, "bool") is False

    def test_date_iso(self) -> None:
        assert _cast_value("1990-05-20", "date") == "1990-05-20"

    def test_date_slash_format(self) -> None:
        assert _cast_value("20/05/1990", "date") == "1990-05-20"

    def test_datetime_cast(self) -> None:
        assert _cast_value("2023-01-15T10:30:00", "datetime") == "2023-01-15T10:30:00"

    def test_unknown_cast_type_returns_raw(self) -> None:
        assert _cast_value("foo", "unknown_type") == "foo"


class TestParseDatetime:
    def test_iso_format(self) -> None:
        assert _parse_datetime("2023-01-15T10:30:00") == "2023-01-15T10:30:00"

    def test_space_separated(self) -> None:
        assert _parse_datetime("2023-01-15 10:30:00") == "2023-01-15T10:30:00"

    def test_date_only(self) -> None:
        assert _parse_datetime("2023-01-15") == "2023-01-15T00:00:00"

    def test_invalid_raises(self) -> None:
        with pytest.raises(ValueError):
            _parse_datetime("not-a-date")


class TestParseDate:
    def test_various_formats(self) -> None:
        assert _parse_date("1990-05-20") == "1990-05-20"
        assert _parse_date("20/05/1990") == "1990-05-20"
        assert _parse_date("05/20/1990") == "1990-05-20"

    def test_invalid_raises(self) -> None:
        with pytest.raises(ValueError):
            _parse_date("not-a-date")


# ---------------------------------------------------------------------------
# DynamicMapper.map_record
# ---------------------------------------------------------------------------


class TestDynamicMapperEntityMapping:
    def test_basic_mapping(self, mapper: DynamicMapper, simple_config: MappingConfig) -> None:
        record = {"name": "Alice", "age_raw": "30", "birth_date": "1993-06-15"}
        results = mapper.map_record(record, simple_config)
        assert len(results) == 1
        p = results[0]
        assert p["source_system"] == "TEST_SYSTEM"
        assert p["entity_type"] == "PERSON"
        assert p["classification"] == "UNCLASSIFIED"
        assert p["properties"]["full_name"] == "Alice"
        assert p["properties"]["age"] == 30
        assert p["properties"]["dob"] == "1993-06-15"

    def test_missing_optional_field_omitted(
        self, mapper: DynamicMapper, simple_config: MappingConfig
    ) -> None:
        record = {"name": "Bob"}  # age_raw and birth_date are absent
        results = mapper.map_record(record, simple_config)
        props = results[0]["properties"]
        assert "full_name" in props
        assert "age" not in props
        assert "dob" not in props

    def test_empty_string_field_omitted(
        self, mapper: DynamicMapper, simple_config: MappingConfig
    ) -> None:
        record = {"name": "Carol", "age_raw": "  ", "birth_date": ""}
        results = mapper.map_record(record, simple_config)
        props = results[0]["properties"]
        assert "age" not in props
        assert "dob" not in props

    def test_cast_failure_keeps_raw_string(
        self, mapper: DynamicMapper, simple_config: MappingConfig
    ) -> None:
        record = {"name": "Dave", "age_raw": "not-a-number"}
        results = mapper.map_record(record, simple_config)
        assert results[0]["properties"]["age"] == "not-a-number"

    def test_classification_override(self, mapper: DynamicMapper) -> None:
        config = MappingConfig(
            source_system_name="SYS",
            default_classification="UNCLASSIFIED",
            entities=[
                EntityMapping(
                    target_entity_type="SECRET_ENTITY",
                    classification_override="SECRET",
                    property_mappings={
                        "id": PropertyMapping(source_field="id"),
                    },
                )
            ],
        )
        results = mapper.map_record({"id": "X1"}, config)
        assert results[0]["classification"] == "SECRET"

    def test_no_entities_returns_empty(self, mapper: DynamicMapper) -> None:
        config = MappingConfig(source_system_name="EMPTY", entities=[])
        assert mapper.map_record({"foo": "bar"}, config) == []


class TestClassificationResolution:
    """Tests for the dynamic classification_column resolution logic."""

    def test_classification_column_used_when_present(self, mapper: DynamicMapper) -> None:
        config = MappingConfig(
            source_system_name="SYS",
            default_classification="UNCLASSIFIED",
            entities=[
                EntityMapping(
                    target_entity_type="DOC",
                    classification_column="doc_clearance",
                    property_mappings={"id": PropertyMapping(source_field="id")},
                )
            ],
        )
        record = {"id": "X1", "doc_clearance": "SECRET"}
        results = mapper.map_record(record, config)
        assert results[0]["classification"] == "SECRET"

    def test_classification_column_falls_back_to_override(self, mapper: DynamicMapper) -> None:
        config = MappingConfig(
            source_system_name="SYS",
            default_classification="UNCLASSIFIED",
            entities=[
                EntityMapping(
                    target_entity_type="DOC",
                    classification_column="doc_clearance",
                    classification_override="CONFIDENTIAL",
                    property_mappings={"id": PropertyMapping(source_field="id")},
                )
            ],
        )
        # Column absent from record → use override
        record = {"id": "X1"}
        results = mapper.map_record(record, config)
        assert results[0]["classification"] == "CONFIDENTIAL"

    def test_classification_column_falls_back_to_default_when_empty(
        self, mapper: DynamicMapper
    ) -> None:
        config = MappingConfig(
            source_system_name="SYS",
            default_classification="UNCLASSIFIED",
            entities=[
                EntityMapping(
                    target_entity_type="DOC",
                    classification_column="doc_clearance",
                    property_mappings={"id": PropertyMapping(source_field="id")},
                )
            ],
        )
        # Column present but blank → fall back to default_classification
        record = {"id": "X1", "doc_clearance": "  "}
        results = mapper.map_record(record, config)
        assert results[0]["classification"] == "UNCLASSIFIED"

    def test_classification_column_takes_priority_over_override(
        self, mapper: DynamicMapper
    ) -> None:
        config = MappingConfig(
            source_system_name="SYS",
            default_classification="UNCLASSIFIED",
            entities=[
                EntityMapping(
                    target_entity_type="DOC",
                    classification_column="doc_clearance",
                    classification_override="CONFIDENTIAL",
                    property_mappings={"id": PropertyMapping(source_field="id")},
                )
            ],
        )
        # Column present and non-empty → column wins over override
        record = {"id": "X1", "doc_clearance": "TS/SCI"}
        results = mapper.map_record(record, config)
        assert results[0]["classification"] == "TS/SCI"

    def test_link_classification_column_used(self, mapper: DynamicMapper) -> None:
        config = MappingConfig(
            source_system_name="SYS",
            default_classification="UNCLASSIFIED",
            entities=[
                EntityMapping(
                    target_entity_type="PERSON",
                    property_mappings={"name": PropertyMapping(source_field="officer_name")},
                ),
                EntityMapping(
                    target_entity_type="COMPANY",
                    property_mappings={"name": PropertyMapping(source_field="company_name")},
                ),
            ],
            links=[
                LinkMapping(
                    source_entity_type="PERSON",
                    target_entity_type="COMPANY",
                    relationship_type="OFFICER_OF",
                    classification_column="link_clearance",
                )
            ],
        )
        record = {"officer_name": "Alice", "company_name": "Acme", "link_clearance": "SECRET"}
        results = mapper.map_record(record, config)
        link = results[2]
        assert link["classification"] == "SECRET"

    def test_link_classification_column_falls_back_to_default(
        self, mapper: DynamicMapper
    ) -> None:
        config = MappingConfig(
            source_system_name="SYS",
            default_classification="UNCLASSIFIED",
            entities=[
                EntityMapping(
                    target_entity_type="PERSON",
                    property_mappings={"name": PropertyMapping(source_field="officer_name")},
                ),
                EntityMapping(
                    target_entity_type="COMPANY",
                    property_mappings={"name": PropertyMapping(source_field="company_name")},
                ),
            ],
            links=[
                LinkMapping(
                    source_entity_type="PERSON",
                    target_entity_type="COMPANY",
                    relationship_type="OFFICER_OF",
                    classification_column="link_clearance",
                )
            ],
        )
        # Column absent → fall back to default_classification
        record = {"officer_name": "Alice", "company_name": "Acme"}
        results = mapper.map_record(record, config)
        link = results[2]
        assert link["classification"] == "UNCLASSIFIED"


class TestDynamicMapperLinkMapping:
    def test_link_payload_generated(
        self, mapper: DynamicMapper, config_with_link: MappingConfig
    ) -> None:
        record = {
            "officer_name": "Eve",
            "company_name": "Acme Corp",
            "start_date": "2010-01-01",
        }
        results = mapper.map_record(record, config_with_link)
        # 2 entities + 1 link
        assert len(results) == 3
        link = results[2]
        assert link["entity_type"] == "OFFICER_OF"
        assert link["properties"]["_source_entity_type"] == "PERSON"
        assert link["properties"]["_target_entity_type"] == "COMPANY"
        assert link["properties"]["since"] == "2010-01-01"

    def test_link_missing_date_property_omitted(
        self, mapper: DynamicMapper, config_with_link: MappingConfig
    ) -> None:
        record = {"officer_name": "Frank", "company_name": "Globex"}
        results = mapper.map_record(record, config_with_link)
        link = results[2]
        assert "since" not in link["properties"]
