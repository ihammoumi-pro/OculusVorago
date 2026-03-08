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

    # REST API (paginated) to Kafka
    python main.py run-pipeline \\
        --extractor api \\
        --source https://api.example.com/records \\
        --config ./configs/mapping.yaml \\
        --api-records-key data \\
        --api-pagination next_url \\
        --api-bearer-token MY_TOKEN

    # SQL database to stdout (dry-run)
    python main.py run-pipeline \\
        --extractor sql \\
        --source "postgresql+psycopg2://user:pass@localhost/mydb" \\
        --config ./configs/mapping.yaml \\
        --sql-query "SELECT * FROM my_table" \\
        --loader stdout

    # PDF text extraction to stdout
    python main.py run-pipeline \\
        --extractor pdf \\
        --source ./documents/report.pdf \\
        --config ./configs/mapping.yaml \\
        --loader stdout

    # OCR scanned document to stdout
    python main.py run-pipeline \\
        --extractor ocr \\
        --source ./scans/document.tiff \\
        --config ./configs/mapping.yaml \\
        --loader stdout

Supported --extractor values:  csv, api, sql, pdf, ocr
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
from vorago.extractors.api_extractor import APIExtractor
from vorago.extractors.csv_extractor import CsvExtractor
from vorago.extractors.ocr_extractor import OCRExtractor
from vorago.extractors.pdf_extractor import PDFExtractor
from vorago.extractors.sql_extractor import SQLExtractor
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
        help="Extractor plugin to use.  Supported: 'csv', 'api', 'sql', 'pdf', 'ocr'.",
    ),
    source: str = typer.Option(
        ...,
        "--source",
        "-s",
        help="Source URI to pass to the extractor (file path, URL, connection string, etc.).",
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
    # --- API extractor options ---
    api_records_key: str = typer.Option(
        None,
        "--api-records-key",
        help="[api] JSON key whose value is the records array in each page response.",
    ),
    api_pagination: str = typer.Option(
        "next_url",
        "--api-pagination",
        help="[api] Pagination style: 'next_url', 'offset', or 'cursor'.",
    ),
    api_page_size: int = typer.Option(
        100,
        "--api-page-size",
        help="[api] Records per page (offset pagination only).",
    ),
    api_bearer_token: str = typer.Option(
        None,
        "--api-bearer-token",
        help="[api] Bearer token for Authorization header.",
    ),
    api_header: list[str] | None = typer.Option(  # noqa: B008
        None,
        "--api-header",
        help="[api] Extra HTTP header in 'Key:Value' format.  Repeatable.",
    ),
    # --- SQL extractor options ---
    sql_query: str = typer.Option(
        None,
        "--sql-query",
        help="[sql] Raw SQL SELECT statement to execute.",
    ),
    sql_table: str = typer.Option(
        None,
        "--sql-table",
        help="[sql] Table name for a full SELECT * scan (ignored when --sql-query is set).",
    ),
    sql_chunk_size: int = typer.Option(
        1000,
        "--sql-chunk-size",
        help="[sql] Server-side cursor chunk size.",
    ),
    # --- OCR extractor options ---
    ocr_lang: str = typer.Option(
        "eng",
        "--ocr-lang",
        help="[ocr] Tesseract language code(s), e.g. 'eng', 'fra+eng'.",
    ),
    ocr_dpi: int = typer.Option(
        200,
        "--ocr-dpi",
        help="[ocr] DPI for PDF-to-image conversion.",
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
    extractor_key = extractor.lower()
    if extractor_key == "csv":
        extractor_instance = CsvExtractor()
    elif extractor_key == "api":
        extra_headers: dict[str, str] = {}
        for header_str in (api_header or []):
            if ":" in header_str:
                k, _, v = header_str.partition(":")
                extra_headers[k.strip()] = v.strip()
        extractor_instance = APIExtractor(
            records_key=api_records_key or None,
            pagination_style=api_pagination,
            page_size=api_page_size,
            bearer_token=api_bearer_token or None,
            headers=extra_headers or None,
        )
    elif extractor_key == "sql":
        if not sql_query and not sql_table:
            typer.echo(
                "The 'sql' extractor requires --sql-query or --sql-table.", err=True
            )
            raise typer.Exit(code=1)
        extractor_instance = SQLExtractor(
            query=sql_query or None,
            table=sql_table or None,
            chunk_size=sql_chunk_size,
        )
    elif extractor_key == "pdf":
        extractor_instance = PDFExtractor()
    elif extractor_key == "ocr":
        extractor_instance = OCRExtractor(lang=ocr_lang, dpi=ocr_dpi)
    else:
        typer.echo(
            f"Unknown extractor: '{extractor}'.  Supported: csv, api, sql, pdf, ocr",
            err=True,
        )
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
