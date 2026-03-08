# OculusVorago

> **OculusVorago** is a highly generic, scalable, memory-efficient ETL pipeline engine purpose-built for feeding structured intelligence payloads into the **OculusOntologia** Kafka topic graph.

[![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-blue)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

---

## Overview

OculusVorago ingests data from heterogeneous sources—CSV files, REST APIs, relational databases, PDF documents, and scanned images—applies a configuration-driven entity/link mapping, and publishes the resulting payloads to a Kafka topic in the format expected by **OculusOntologia**.

Key design goals:

| Goal | How it is achieved |
|---|---|
| **Memory efficiency** | Every extractor is a generator; only one record is ever in memory at a time |
| **Zero dead-code coupling** | Extractors, mappers, and loaders are pluggable via abstract interfaces |
| **Operational resilience** | Failed rows are routed to the Dead Letter Queue (DLQ); the pipeline never crashes |
| **Dataset agnosticism** | All field mapping and entity/link logic lives in a YAML config file |

---

## Architecture

### Plugin / Strategy Pattern

```
┌─────────────────────────────────────────────────────────┐
│                     Pipeline.run()                      │
│                                                         │
│  IExtractor.extract()   IMapper.map_record()   ILoader.load()  │
│         │                      │                     │         │
│   CsvExtractor            DynamicMapper          KafkaLoader  │
│   APIExtractor                                  StdoutLoader  │
│   SQLExtractor                                               │
│   PDFExtractor                                               │
│   OCRExtractor                                               │
└─────────────────────────────────────────────────────────┘
```

Three abstract interfaces (`vorago/core/interfaces.py`) define the contract:

- **`IExtractor`** — opens a data source and `yield`s one record (dict) at a time
- **`IMapper`** — transforms a raw record into one or more typed entity/link payloads
- **`ILoader`** — consumes the stream of payloads and writes them to a destination

Each interface is independently swappable; adding a new data source requires only implementing `IExtractor`.

### Streaming Generator Chain

```
source_uri
   │
   ▼
IExtractor.extract()        ← generator, yields 1 raw record at a time
   │
   ▼
IMapper.map_record()        ← transforms 1 raw record → N payloads
   │
   ▼
ILoader.load()              ← consumes and publishes N payloads
```

At no point is more than a single record buffered in Python memory, making it safe to run against 50 GB CSV files or paginated APIs returning millions of records.

### Dead Letter Queue (DLQ) Resilience

Any exception raised inside the mapper for a particular row is caught by `Pipeline`, logged as a warning, and that row is counted as `rows_failed` in the summary.  The pipeline continues with the next record so a single malformed row cannot halt a multi-million-row job.

---

## Features

| Feature | Details |
|---|---|
| **CSV Extractor** | Streaming, configurable delimiter/encoding, BOM-safe |
| **API Extractor** | Pagination: `next_url`, `offset`, `cursor`; Bearer + header auth; retry/back-off |
| **SQL Extractor** | SQLAlchemy (any dialect); server-side cursor via `yield_per`; raw query or table scan |
| **PDF Extractor** | PyMuPDF; page-by-page text + document metadata; encrypted PDF support |
| **OCR Extractor** | pytesseract + Pillow; scanned images and PDF-to-image via pdf2image |
| **Dynamic Mapper** | Config-driven Pydantic mapping; type casts (`str`, `int`, `float`, `bool`, `date`, `datetime`) |
| **Kafka Loader** | Confluent-Kafka; batched flush; LZ4 compression; delivery callbacks |
| **Stdout Loader** | JSONL to stdout for dry-runs (no Kafka required) |

---

## Prerequisites & Installation

### System Dependencies

| Dependency | Required by | Install |
|---|---|---|
| Python ≥ 3.11 | all | [python.org](https://www.python.org/downloads/) |
| Tesseract OCR | `OCRExtractor` | `sudo apt install tesseract-ocr` (Ubuntu) / `brew install tesseract` (macOS) |
| Poppler | `OCRExtractor` (PDF→image) | `sudo apt install poppler-utils` (Ubuntu) / `brew install poppler` (macOS) |

### Python Installation

```bash
# 1. Clone the repository
git clone https://github.com/ihammoumi-pro/OculusVorago.git
cd OculusVorago

# 2. Create and activate a virtual environment
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate

# 3. Install runtime + dev dependencies
pip install -e ".[dev]"

# 4. Verify the installation
pytest          # should show 109 passing tests
ruff check .    # should show 0 errors
```

---

## Configuration

Pipelines are driven by a YAML (or JSON) **MappingConfig** file.  Here is an annotated snippet based on the ICIJ Offshore Leaks dataset:

```yaml
# configs/icij_mapping.yaml

# Human-readable identifier embedded in every Kafka payload
source_system_name: ICIJ_Offshore_Leaks

# Default ABAC classification label applied to every record
default_classification: UNCLASSIFIED

# Target Kafka topic
kafka_topic: oculus.ingestion.raw

entities:
  # ---------- PERSON entity ----------
  - target_entity_type: PERSON
    property_mappings:
      # target_property: { source_field: <csv_column>, cast_as: <type> }
      node_id:       { source_field: node_id,       cast_as: str  }
      full_name:     { source_field: name,           cast_as: str  }
      country_codes: { source_field: country_codes,  cast_as: str  }

  # ---------- COMPANY entity ----------
  - target_entity_type: COMPANY
    property_mappings:
      node_id:             { source_field: entity_id,                    cast_as: str  }
      name:                { source_field: entity_name,                  cast_as: str  }
      incorporation_date:  { source_field: entity_incorporation_date,    cast_as: date }

links:
  # ---------- PERSON → COMPANY relationship ----------
  - source_entity_type: PERSON
    target_entity_type: COMPANY
    relationship_type:  OFFICER_OF
    property_mappings:
      relationship_label: { source_field: relationship_type, cast_as: str }
```

Supported `cast_as` values: `str`, `int`, `float`, `bool`, `date`, `datetime`.

---

## Usage

All commands are run via the Typer CLI (`main.py`).

### CSV → Kafka

```bash
python main.py run-pipeline \
  --extractor csv \
  --source ./data/icij_officers.csv \
  --config ./configs/icij_mapping.yaml \
  --loader kafka \
  --kafka-brokers localhost:9092
```

### CSV → stdout (dry-run)

```bash
python main.py run-pipeline \
  --extractor csv \
  --source ./data/sample.csv \
  --config ./configs/icij_mapping.yaml \
  --loader stdout
```

### REST API → Kafka

```bash
python main.py run-pipeline \
  --extractor api \
  --source https://api.example.com/v1/records \
  --config ./configs/mapping.yaml \
  --api-records-key data \
  --api-pagination next_url \
  --api-bearer-token "$MY_API_TOKEN" \
  --loader kafka
```

Additional API options:

| Option | Default | Description |
|---|---|---|
| `--api-records-key` | _(none)_ | JSON key holding the records array |
| `--api-pagination` | `next_url` | `next_url`, `offset`, or `cursor` |
| `--api-page-size` | `100` | Records per page (offset pagination) |
| `--api-bearer-token` | _(none)_ | Bearer token for `Authorization` header |
| `--api-header` | _(none)_ | Extra header as `Key:Value` (repeatable) |

### SQL Database → stdout

```bash
python main.py run-pipeline \
  --extractor sql \
  --source "postgresql+psycopg2://user:pass@localhost:5432/mydb" \
  --config ./configs/mapping.yaml \
  --sql-query "SELECT * FROM officers WHERE active = true" \
  --loader stdout
```

Or scan an entire table:

```bash
python main.py run-pipeline \
  --extractor sql \
  --source "sqlite:///./data/local.db" \
  --config ./configs/mapping.yaml \
  --sql-table officers \
  --loader stdout
```

Additional SQL options:

| Option | Default | Description |
|---|---|---|
| `--sql-query` | _(none)_ | Raw SQL SELECT statement |
| `--sql-table` | _(none)_ | Table name for `SELECT *` scan |
| `--sql-chunk-size` | `1000` | Server-side cursor chunk size |

### PDF Text Extraction → stdout

```bash
python main.py run-pipeline \
  --extractor pdf \
  --source ./documents/annual_report.pdf \
  --config ./configs/mapping.yaml \
  --loader stdout
```

### OCR Scanned Document → Kafka

```bash
python main.py run-pipeline \
  --extractor ocr \
  --source ./scans/classified_memo.tiff \
  --config ./configs/mapping.yaml \
  --ocr-lang eng \
  --ocr-dpi 300 \
  --loader kafka
```

Additional OCR options:

| Option | Default | Description |
|---|---|---|
| `--ocr-lang` | `eng` | Tesseract language code(s), e.g. `fra+eng` |
| `--ocr-dpi` | `200` | DPI for PDF-to-image conversion |

---

## Development

```bash
# Run all tests
pytest

# Run a specific test module
pytest tests/test_api_extractor.py -v

# Check code style
ruff check .

# Auto-fix style issues
ruff check --fix .

# Test coverage report
pytest --cov=vorago --cov-report=html
```

---

## Project Structure

```
OculusVorago/
├── main.py                          # Typer CLI entry point
├── pyproject.toml                   # Project metadata & tool config
├── requirements.txt                 # Pinned dependencies
├── configs/
│   └── icij_mapping.yaml            # Example ICIJ mapping config
├── vorago/
│   ├── core/
│   │   ├── interfaces.py            # IExtractor, IMapper, ILoader ABCs
│   │   └── config_models.py         # Pydantic MappingConfig models
│   ├── engine/
│   │   └── pipeline.py              # Orchestrator + DLQ logic
│   ├── extractors/
│   │   ├── csv_extractor.py         # CSV streaming extractor
│   │   ├── api_extractor.py         # REST API extractor (paginated)
│   │   ├── sql_extractor.py         # SQLAlchemy database extractor
│   │   ├── pdf_extractor.py         # PyMuPDF PDF text extractor
│   │   └── ocr_extractor.py         # pytesseract OCR extractor
│   ├── mappers/
│   │   └── dynamic_mapper.py        # Config-driven entity/link mapper
│   └── loaders/
│       └── kafka_loader.py          # Confluent-Kafka publisher
└── tests/
    ├── test_pipeline.py
    ├── test_config_models.py
    ├── test_extractor.py
    ├── test_mapper.py
    ├── test_api_extractor.py
    ├── test_sql_extractor.py
    ├── test_pdf_extractor.py
    └── test_ocr_extractor.py
```

---

## License

MIT © ihammoumi-pro
