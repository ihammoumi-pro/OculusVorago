"""
Pydantic configuration models for OculusVorago.

Analysts write a YAML or JSON file that matches this schema to describe
how raw incoming fields should be mapped to OculusOntologia entities and
links — without writing any Python code.
"""

from __future__ import annotations

from pydantic import BaseModel, Field


class PropertyMapping(BaseModel):
    """
    Maps a single target property name to a source column name.

    Optionally specifies a data type so the mapper can cast the value
    before sending it downstream.
    """

    source_field: str = Field(
        ...,
        description="The exact column / key name in the raw record.",
    )
    cast_as: str | None = Field(
        default=None,
        description=(
            "Optional type hint for casting.  Supported values: "
            "'str', 'int', 'float', 'bool', 'date', 'datetime'."
        ),
    )


class EntityMapping(BaseModel):
    """
    Describes how to construct one entity payload from a raw record.
    """

    target_entity_type: str = Field(
        ...,
        description="The entity type expected by OculusOntologia (e.g. 'PERSON').",
    )
    property_mappings: dict[str, PropertyMapping] = Field(
        default_factory=dict,
        description=(
            "Maps target property name → PropertyMapping.  "
            "E.g. {'first_name': {'source_field': 'fname', 'cast_as': 'str'}}."
        ),
    )
    classification_override: str | None = Field(
        default=None,
        description=(
            "Override the top-level default_classification for this entity."
        ),
    )


class LinkMapping(BaseModel):
    """
    Describes how to construct a relationship payload from a raw record.

    The relationship connects two entities that were also generated from
    the **same** raw record during the same mapping pass.
    """

    source_entity_type: str = Field(
        ...,
        description="The entity_type of the *source* end of the relationship.",
    )
    target_entity_type: str = Field(
        ...,
        description="The entity_type of the *target* end of the relationship.",
    )
    relationship_type: str = Field(
        ...,
        description="The relationship label (e.g. 'OFFICER_OF').",
    )
    property_mappings: dict[str, PropertyMapping] = Field(
        default_factory=dict,
        description="Optional edge properties mapped from the raw record.",
    )
    classification_override: str | None = Field(
        default=None,
        description=(
            "Override the top-level default_classification for this link."
        ),
    )


class MappingConfig(BaseModel):
    """
    Top-level configuration for a single ETL mapping job.

    One YAML/JSON file per dataset (e.g. ``configs/icij_mapping.yaml``).
    """

    source_system_name: str = Field(
        ...,
        description=(
            "Human-readable name that identifies the data source "
            "(e.g. 'ICIJ_Offshore_Leaks')."
        ),
    )
    default_classification: str = Field(
        default="UNCLASSIFIED",
        description=(
            "The ABAC classification label applied to all payloads unless "
            "overridden at the entity/link level."
        ),
    )
    kafka_topic: str = Field(
        default="oculus.ingestion.raw",
        description="Kafka topic where payloads will be published.",
    )
    entities: list[EntityMapping] = Field(
        default_factory=list,
        description="Ordered list of entity mappings to apply per raw record.",
    )
    links: list[LinkMapping] | None = Field(
        default=None,
        description=(
            "Optional list of link mappings to apply per raw record after "
            "all entity payloads have been generated."
        ),
    )
