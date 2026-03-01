# wdi-pipeline

Manifest-driven batch pipeline that fetches World Development Indicators (WDI)
from the World Bank API, filters them with SQL, and exports to CSV or Parquet.

Built as a production-style Python prototype — connector-agnostic core, DuckDB
for in-process SQL, streaming page ingestion, and no pandas dependency.

---

## Repository layout

```
api-bulk-downloader/
├── wdi_pipeline/                  # installable package (v2)
│   ├── cli.py                     #   argparse entry point
│   ├── runner.py                  #   job loop (dry-run / probe / full)
│   ├── manifest.py                #   YAML loader + validation
│   ├── sql_template.py            #   {{key}} → SQL literal renderer
│   ├── exporter.py                #   DuckDB COPY → CSV / Parquet
│   ├── summary.py                 #   per-job JSON summary
│   ├── tui.py                     #   Textual TUI dashboard
│   ├── logging_setup.py
│   ├── exceptions.py
│   └── connectors/
│       ├── protocol.py            #   DiscoveryResult
│       └── worldbank_indicator.py #   JSON paging + Session DI
├── pipelines/                     # one subdir per pipeline (= one job = one output file)
│   ├── gdp_jpn/
│   │   ├── manifest.yaml          #   job definition + output_root
│   │   ├── queries/timeseries.sql #   SQL template
│   │   └── schemas/timeseries.yaml#   column definitions (name + DuckDB type)
│   └── population_latam/
│       ├── manifest.yaml
│       ├── queries/timeseries.sql
│       └── schemas/timeseries.yaml
├── tests/                         # 36 unit tests
├── archive/
│   └── api_bulk_downloader_v1/    # v1 reference (ZIP/stream approach)
└── pyproject.toml
```

---

## Installation

```bash
pip install -e .
```

This installs the `wdi-pipeline` command and all dependencies
(`requests`, `urllib3`, `duckdb`, `pyyaml`, `python-dotenv`, `textual`).

---

## Usage

Set defaults in `.env` (optional):

```dotenv
WDI_MANIFEST=pipelines/gdp_jpn/manifest.yaml   # for `run`
WDI_PIPELINE_DIR=pipelines/                     # for `run-all`
```

### `run` — single pipeline

```bash
# Run with .env defaults (no flags needed)
wdi-pipeline run

# Override manifest or output root at runtime
wdi-pipeline run --manifest pipelines/gdp_jpn/manifest.yaml
wdi-pipeline run --output-root tmp/

# Validate manifest structure — no network calls
wdi-pipeline run --dry-run

# Discover column schema only — no data fetched
wdi-pipeline run --probe

# Run a single named job
wdi-pipeline run --only gdp_jpn

# Verbose logging
wdi-pipeline run --log-level DEBUG
```

### `run-all` — all pipelines under a directory

```bash
# Run every pipeline found under pipelines/
wdi-pipeline run-all --pipeline-dir pipelines/

# Dry-run all pipelines (uses WDI_PIPELINE_DIR from .env)
wdi-pipeline run-all --dry-run

# Override output root for all pipelines (flat, no subdirs)
wdi-pipeline run-all --pipeline-dir pipelines/ --output-root tmp/

# Skip the preflight path-collision check
wdi-pipeline run-all --pipeline-dir pipelines/ --allow-overwrite
```

`run-all` performs a preflight check before executing: if two pipelines would write to
the same path (same `export.filename` or same `job.job_id`), it exits with an error.
Use `--allow-overwrite` to disable the check (last write wins).

### `list` — show pipeline configuration

```bash
# List all jobs under pipelines/
wdi-pipeline list --pipeline-dir pipelines/

# Uses WDI_PIPELINE_DIR from .env
wdi-pipeline list
```

Displays a table of all jobs sorted by enabled status then `indicator_code`:

```
Enabled    indicator_code    filename              output dir    column names
---------  ----------------  --------------------  ------------  -----------------------------------------------------------------------
true       NY.GDP.MKTP.CD    gdp_jpn.csv           outputs       country_code, country_name, indicator_code, indicator_name, year, value
true       SP.POP.TOTL       population_latam.csv  outputs       country_code, country_name, indicator_code, indicator_name, year, value
```

Jobs with `enabled: false` appear at the bottom of the table.

### `gui` — interactive TUI dashboard

```bash
# Launch the TUI (uses WDI_PIPELINE_DIR from .env)
wdi-pipeline gui

# Specify the pipeline directory explicitly
wdi-pipeline gui --pipeline-dir pipelines/
```

Displays a live table of all jobs. Key bindings:

| Key | Action |
|-----|--------|
| `↑` / `↓` | Move cursor |
| `q` | Quit |

Buttons:

| Button | Action |
|--------|--------|
| **Toggle Enabled** | Flip `enabled` for the selected job and save to `manifest.yaml` |
| **Edit** | Open a modal to edit `indicator_code`, `country_code`, `filename`, `format`, `sql.params`, and `enabled`; Save writes back to `manifest.yaml` |

The table order stays fixed during the session. Rows are re-sorted (enabled
descending → `indicator_code` ascending) the next time the app is launched.

Resolution order:

| Setting | Priority 1 | Priority 2 | Priority 3 |
|---|---|---|---|
| manifest path | `--manifest` | `WDI_MANIFEST` | error |
| pipeline dir | `--pipeline-dir` | `WDI_PIPELINE_DIR` | error |
| output root | `--output-root` | manifest `defaults.output_root` | — |

| Mode | `discover()` | `materialize()` | SQL / export |
|------|:---:|:---:|:---:|
| normal | ✅ | ✅ | ✅ |
| `--dry-run` | — | — | — |
| `--probe` | ✅ | — | — |

---

## Manifest

Each pipeline lives in its own subdirectory and contains exactly one job.
Example — `pipelines/gdp_jpn/manifest.yaml`:

```yaml
defaults:
  output_root: outputs/      # base directory for all exports
  export_format: parquet     # default format (csv or parquet) — Excel (.xlsx) is not supported

jobs:
  - job_id: gdp_jpn
    enabled: true
    connector_params:
      indicator_code: NY.GDP.MKTP.CD
      country_code: JPN        # ISO 3166-1 alpha-3, or "all"
    sql:
      file: queries/timeseries.sql
      params:
        min_year: "2000"         # injected as SQL literal via {{min_year}}
    export:
      filename: gdp_jpn          # output: outputs/gdp_jpn.parquet
    schema:
      file: schemas/timeseries.yaml  # column definitions
```

`connector_params` are passed as keyword arguments to the connector constructor.
`sql.params` replace `{{key}}` placeholders in the SQL file.
`schema.file` points to a YAML file that defines the dataset columns and their DuckDB types.

---

## Schema files

Column definitions are declared in separate YAML files under `schemas/`:

```yaml
# schemas/timeseries.yaml
columns:
  - name: country_code
    type: VARCHAR
  - name: year
    type: INTEGER
  - name: value
    type: DOUBLE
  # ...
```

Each entry requires `name` (column name) and `type` (DuckDB type string).
The schema is loaded at manifest-parse time and used to:

- build the `CREATE TABLE dataset (...)` DDL in `materialize()`
- report available columns in `discover()`

---

## SQL templates

SQL files under `queries/` may contain `{{key}}` placeholders:

```sql
SELECT country_code, country_name, indicator_code, indicator_name, year, value
FROM dataset
WHERE year >= {{min_year}}
ORDER BY country_code, year;
```

`{{key}}` is replaced with a typed SQL literal before execution:
integers and floats are embedded bare; other strings are single-quoted
with `'` escaped. Parameters come from `sql.params` in the manifest
(operator-controlled config, not user input).

The materialized API data is always available as a DuckDB table named `dataset`.

---

## Architecture

### Connector interface

Connectors are duck-typed — any class implementing these two methods works:

```python
def discover(self, job: JobConfig) -> DiscoveryResult: ...
def materialize(self, job: JobConfig, conn: duckdb.DuckDBPyConnection) -> None: ...
```

`runner.py` calls both methods for every enabled job.
`discover()` is always a no-network call — it reads column names from `job.schema`.
`materialize()` builds the `CREATE TABLE` DDL dynamically from `job.schema` and
streams API pages directly into a per-job DuckDB connection — no full dataset is
held in memory.

### Per-job isolation

Each job gets a fresh `duckdb.connect()` that is closed after export.
Failure in one job logs an error and continues to the next.

### Retry

`WorldBankIndicatorConnector` uses `urllib3.Retry` with
`backoff_factor=1.0` on HTTP 429, 500, 502, 503, 504 (up to 3 attempts).

---

## Output

After a successful run, `outputs/` contains:

```
outputs/
├── gdp_jpn.parquet
├── gdp_jpn_summary.json
├── population_latam.csv
└── population_latam_summary.json
```

Each `_summary.json` records job id, status, start/end time,
duration, row count, export path, discovery columns, and any error message.

---

## Testing

```bash
pytest tests/ -v
```

36 unit tests. HTTP is never called in tests — `WorldBankIndicatorConnector`
accepts an injected `session` argument, and tests pass a `FakeSession`
that returns pre-defined page payloads.

---

## Further reading

- [DESIGN.md](DESIGN.md) — 設計判断とその理由
- [docs/connectors.md](docs/connectors.md) — コネクター設定リファレンス

---

## Archive

`archive/api_bulk_downloader_v1/` is the original v1 implementation:
a streaming HTTP downloader that fetched ZIP archives and counted CSV rows.
It is kept for reference only and is not installed by `pyproject.toml`.


## Data flow per job

```mermaid
flowchart LR
    CLI["<i>Entry Point</i><br><br><u><b><big>cli.py</b></u></big><br>main()<br><br>-----<br><br>run all の場合は<br>ここでループする"]
    MAN["<u><b><big>manifest.py</big></u></b><br>load_manifest()<br><br>-----<br><br>job.schema を作る"]
    RUN["<u><b><big>runner.py</big></u></b><br>run_pipeline()<br><br>-----<br><br>1 manifest の <br>job を実行する"]

    %% inputs (auxiliary)
    subgraph PRE ["<u><b>pipelines/*/</b></u><br>（事前設定）"]
        MF[/"<u><b><big>manifest.yaml</big></u></b><br>job定義<br><br>1 job = 1 CSV出力"/]
        SCH[/"<u><b><big>schemas/timeseries.yaml</big></u></b><br>列定義 (name · type)"/]
        SQL_FILE[/"<u><b><big>queries/timeseries.sql</big></u></b><br>SQL テンプレ"/]
    end

    SKIP(["⏭ skipped"])

    subgraph CONN ["<big><big><big><big><u><b>connectors/*.py</b></u>"]
        DISC["<big><b>discover()</b></big><br><br>-----<br><br>job.schema から列名取得"]
        MAT["<big><b>materialize()</b></big><br><br>-----<br><br>テーブル作成"]
    end

    PROBED(["⏭ probe 完了<br>列名確認のみ"])

    WB[/"<u><b><big>World Bank Indicator API</big></u></b><br>JSON ページング · urllib3.Retry"/]

    DS[("（メモリ上）<br><u><b><big>TABLE dataset</big></u></b><br>-----<br><br>（例）<br>country_code<br>country_name<br>indicator_code<br>indicator_name<br>year<br>value")]

    RENDER["<u><b><big>sql_template.py</big></u></b><br>render()<br><br>-----<br><br>{{key}} → SQL リテラル"]

    EXPORT["<u><b><big>exporter.py</big></u></b><br>export()<br><br>-----<br><br>SQL実行・ファイル生成"]

    SUM["<u><b><big>summary.py</big></u></b><br>write()<br><br>-----<br><br>ジョブ結果を JSON 出力"]

    OUT_DATA[/"<u><b><big>outputs/</big></u></b><br>*.csv / *.parquet"/]
    OUT_SUM[/"<u><b><big>outputs/</big></u></b><br>*_summary.json"/]

    %% main flow
    CLI --> MAN
    CLI --> RUN
    MAN -.->|"ManifestConfig"| RUN
    MF -.->|"load"| MAN
    MAN -.->|"schema.file"| SCH
    MAN -.->|"sql.file"| SQL_FILE

    RUN -->|"--dry-run"| SKIP
    RUN -->|"enabled: true の job"| DISC

    DISC -->|"--probe"| PROBED
    DISC -->|"full run"| MAT

    MAT -.->|"duckdb.connect()<br>（in-memory）"| DS
    MAT <-->|"GET /v2/country/{cc}/indicator/{ic}?page=N<br>← [{meta}, [{records}]]"| WB
    MAT -->|"INSERT rows<br>(page by page)"| DS

    DS -->|"data source (TABLE dataset)"| EXPORT
    SQL_FILE -.->|"template"| RENDER
    RENDER -->|"sql string"| EXPORT
    EXPORT --> OUT_DATA
    EXPORT --> SUM
    SKIP --> SUM
    PROBED --> SUM
    SUM --> OUT_SUM

    classDef entry fill:#e0f7fa,stroke:#0097a7,color:#000
    classDef file  fill:#e8f0fe,stroke:#4a7fcb,color:#000
    classDef ext   fill:#fff3e0,stroke:#e6a817,color:#000
    classDef db    fill:#e8f5e9,stroke:#43a047,color:#000
    classDef term  fill:#f3e5f5,stroke:#8e24aa,color:#000
    classDef out   fill:#fce4ec,stroke:#e91e63,color:#000

    class CLI entry
    class MF,SCH,SQL_FILE file
    class WB ext
    class DS db
    class SKIP,PROBED term
    class OUT_DATA,OUT_SUM out
```