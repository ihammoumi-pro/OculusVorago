"""
OculusVorago — Command Line Interface

Usage examples::

    # Run a pipeline using the CSV extractor and Kafka loader
    python main.py run-pipeline \\
        --extractor csv \\
        --source ./data/icij_officers.csv \\
        --config ./configs/icij_mapping.yaml \\
        --loader kafka

    # Dry-run: print payloads to stdout instead of publishing to Kafka
    python main.py run-pipeline \\
        --extractor csv \\
        --source ./data/sample.csv \\
        --config ./configs/icij_mapping.yaml \\
        --loader stdout

Supported --extractor values:  csv
Supported --loader values:     kafka, stdout
"""

from __future__ import annotations

import json
import logging
import sys
from collections.abc import Iterator
from typing import Any

import typer
import yaml

from vorago.core.config_models import MappingConfig
from vorago.core.interfaces import ILoader
from vorago.engine.pipeline import Pipeline
from vorago.extractors.csv_extractor import CsvExtractor
from vorago.loaders.kafka_loader import KafkaLoader

app = typer.Typer(
    name="vorago",
    help="OculusVorago — configuration-driven ETL pipeline engine.",
    add_completion=False,
)


# ---------------------------------------------------------------------------
# Stdout loader (useful for debugging / dry-runs without a Kafka broker)
# ---------------------------------------------------------------------------


class StdoutLoader(ILoader):
    """Prints each mapped payload as a JSON line to stdout.  No Kafka required."""

    def load(self, records: Iterator[dict[str, Any]]) -> None:
        count = 0
        for record in records:
            print(json.dumps(record, default=str))
            count += 1
        logging.getLogger(__name__).info("StdoutLoader: wrote %d records", count)


# ---------------------------------------------------------------------------
# CLI command
# ---------------------------------------------------------------------------


@app.command("run-pipeline")
def run_pipeline(
    extractor: str = typer.Option(
        ...,
        "--extractor",
        "-e",
        help="Extractor plugin to use.  Supported: 'csv'.",
    ),
    source: str = typer.Option(
        ...,
        "--source",
        "-s",
        help="Source URI to pass to the extractor (file path, URL, etc.).",
    ),
    config_path: str = typer.Option(
        ...,
        "--config",
        "-c",
        help="Path to a YAML or JSON MappingConfig file.",
    ),
    loader: str = typer.Option(
        "kafka",
        "--loader",
        "-l",
        help="Loader plugin to use.  Supported: 'kafka', 'stdout'.",
    ),
    kafka_brokers: str = typer.Option(
        "localhost:9092",
        "--kafka-brokers",
        help="Comma-separated Kafka bootstrap servers (used when --loader kafka).",
    ),
    log_level: str = typer.Option(
        "INFO",
        "--log-level",
        help="Python logging level (DEBUG, INFO, WARNING, ERROR).",
    ),
) -> None:
    """Run an ETL pipeline end-to-end."""
    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        stream=sys.stderr,
    )

    # --- Load config ---
    mapping_config = _load_config(config_path)

    # --- Build extractor ---
    if extractor.lower() == "csv":
        extractor_instance = CsvExtractor()
    else:
        typer.echo(f"Unknown extractor: '{extractor}'.  Supported: csv", err=True)
        raise typer.Exit(code=1)

    # --- Build loader ---
    if loader.lower() == "kafka":
        loader_instance: ILoader = KafkaLoader(
            bootstrap_servers=kafka_brokers,
            topic=mapping_config.kafka_topic,
        )
    elif loader.lower() == "stdout":
        loader_instance = StdoutLoader()
    else:
        typer.echo(
            f"Unknown loader: '{loader}'.  Supported: kafka, stdout", err=True
        )
        raise typer.Exit(code=1)

    # --- Run pipeline ---
    pipeline = Pipeline(
        extractor=extractor_instance,
        config=mapping_config,
        loader=loader_instance,
    )
    summary = pipeline.run(source_uri=source)

    typer.echo(json.dumps(summary, indent=2))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _load_config(config_path: str) -> MappingConfig:
    """Load a MappingConfig from a YAML or JSON file."""
    try:
        with open(config_path, encoding="utf-8") as fh:
            if config_path.lower().endswith((".yaml", ".yml")):
                raw = yaml.safe_load(fh)
            else:
                raw = json.load(fh)
        return MappingConfig.model_validate(raw)
    except FileNotFoundError:
        typer.echo(f"Config file not found: '{config_path}'", err=True)
        raise typer.Exit(code=1) from None
    except Exception as exc:  # noqa: BLE001
        typer.echo(f"Failed to load config '{config_path}': {exc}", err=True)
        raise typer.Exit(code=1) from exc


if __name__ == "__main__":
    app()
