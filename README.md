# API Bulk Downloader

A production-style Python prototype for safely downloading large datasets via API.

## Project Structure

```
api_bulk_downloader/
├── core/
│   ├── config.py       # Persistent output-dir config (load/save ~/.config/.../config.json)
│   ├── downloader.py   # Streaming downloader with retry/backoff (connector-agnostic)
│   ├── file_utils.py   # Chunk writes, ZIP extraction, primary CSV selection, row counting
│   └── logger.py       # Logging setup and DownloadMetrics dataclass
├── connectors/
│   └── worldbank.py    # World Bank Indicators API connector
├── main.py             # CLI entry point
tests/
└── test_file_utils.py  # Unit tests for file_utils (choose_primary_csv)
```

## Quick Start

```bash
pip install -r requirements.txt

# Download World Bank GDP data (default, saves to ./downloads)
python -m api_bulk_downloader.main

# Persist an output directory (written to ~/.config/api_bulk_downloader/config.json)
python -m api_bulk_downloader.main --set-dest /data/worldbank

# Specify a different indicator and output directory
python -m api_bulk_downloader.main --indicator SP.POP.TOTL --dest data/

# Full options
python -m api_bulk_downloader.main --help
```

## Architecture

### Separation of Concerns

| Layer | Responsibility |
|-------|---------------|
| `core/config.py` | Persist and load output directory (`~/.config/.../config.json`) |
| `core/downloader.py` | HTTP streaming, retry logic, orchestration — no API knowledge |
| `core/file_utils.py` | File I/O: chunked writes, ZIP extraction, primary CSV selection, row counting |
| `core/logger.py` | Logging config, `DownloadMetrics` (start/end/duration/bytes/rows) |
| `connectors/*` | API-specific URL building and authentication headers |

### Connector Protocol

Each connector exposes only two properties:

```python
@property
def download_url(self) -> str: ...      # full URL to the resource
@property
def request_headers(self) -> dict: ...  # HTTP headers (auth, Accept, etc.)
```

`BulkDownloader` depends on this protocol, not on concrete connector classes,
making it trivial to swap in the Salesforce connector later without touching
any core code.

### Retry Strategy

`BulkDownloader` uses `urllib3.Retry` with exponential backoff:

```
wait = backoff_factor × 2^(attempt − 1)
```

Retries on HTTP 429, 500, 502, 503, 504.

### Metrics Logged

| Field | Description |
|-------|-------------|
| `start_time` | Unix timestamp when download began |
| `end_time` | Unix timestamp when download finished |
| `duration_seconds` | Wall-clock duration |
| `bytes_downloaded` | Raw bytes written to disk |
| `row_count` | CSV data rows (header excluded); `n/a` for non-CSV |

### Data Flow

```mermaid
flowchart TD
    CLI["main.py\n(argparse)"]

    subgraph 設定解決
        CFG_FILE["~/.config/.../config.json"]
        CFG["core/config.py\nload_dest / save_dest"]
    end

    subgraph Connector
        WBC["WorldBankConnector\n(単一指標)"]
        WDIC["WorldBankWDIConnector\n(WDI全量)"]
    end

    subgraph Core
        DL["BulkDownloader\n(downloader.py)"]
        SESSION["requests.Session\n+ urllib3.Retry\n(429/5xx リトライ)"]
    end

    subgraph ファイル操作
        STREAM["stream_to_file()\n8KB チャンク書き込み"]
        ISZIP{is_zip?}
        UNZIP["extract_zip()"]
        COUNT["count_csv_rows()"]
    end

    subgraph 出力 dest_dir
        ZIP["*.zip (optional)"]
        CSV["*.csv (optional)"]
    end

    METRICS["DownloadMetrics\n(bytes / rows / duration)"]
    REMOTE["Remote API\n(HTTPS)"]

    CLI -->|"--set-dest"| CFG
    CFG <-->|読み書き| CFG_FILE
    CFG -->|"load_dest()"| CLI

    CLI -->|"dest_dir\nconnector\nchunk_size\nmax_retries"| DL

    CLI --> WBC
    CLI --> WDIC
    WBC -->|"download_url\nrequest_headers"| DL
    WDIC -->|"download_url\nrequest_headers"| DL

    DL --> SESSION
    SESSION -->|"GET stream=True"| REMOTE
    REMOTE -->|"chunked response"| SESSION
    SESSION --> STREAM
    STREAM -->|"bytes_downloaded"| DL
    STREAM --> ISZIP

    ISZIP -->|Yes| ZIP
    ZIP --> UNZIP
    UNZIP --> CSV
    ISZIP -->|No| CSV

    CSV -->|"count_rows?"| COUNT
    COUNT -->|"row_count"| DL

    DL --> METRICS
    METRICS -->|"log.info"| CLI

    style CFG_FILE fill:#f5f0e8,stroke:#b8a070
    style REMOTE fill:#e8f0fe,stroke:#4a7fcb
    style METRICS fill:#e8f5e9,stroke:#4caf50
    style ISZIP fill:#fff8e1,stroke:#f9a825
```
