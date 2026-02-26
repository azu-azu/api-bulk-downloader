# API Bulk Downloader

A production-style Python prototype for safely downloading large datasets via API.

## Project Structure

```
api_bulk_downloader/
├── core/
│   ├── downloader.py   # Streaming downloader with retry/backoff (connector-agnostic)
│   ├── file_utils.py   # Chunk writes, ZIP extraction, CSV row counting
│   └── logger.py       # Logging setup and DownloadMetrics dataclass
├── connectors/
│   ├── worldbank.py    # World Bank Indicators API connector
│   └── salesforce.py   # Salesforce Bulk API 2.0 (placeholder)
└── main.py             # CLI entry point
```

## Quick Start

```bash
pip install -r requirements.txt

# Download World Bank GDP data (default)
python -m api_bulk_downloader.main

# Specify a different indicator and output directory
python -m api_bulk_downloader.main --indicator SP.POP.TOTL --dest data/

# Full options
python -m api_bulk_downloader.main --help
```

## Architecture

### Separation of Concerns

| Layer | Responsibility |
|-------|---------------|
| `core/downloader.py` | HTTP streaming, retry logic, orchestration — no API knowledge |
| `core/file_utils.py` | File I/O: chunked writes, ZIP extraction, CSV counting |
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
        FU["file_utils.py"]
    end

    subgraph ファイル操作
        STREAM["stream_to_file()\n8KB チャンク書き込み"]
        UNZIP["extract_zip()"]
        COUNT["count_csv_rows()"]
    end

    subgraph 出力 dest_dir
        ZIP["*.zip"]
        CSV["*.csv\n(展開済み)"]
    end

    METRICS["DownloadMetrics\n(bytes / rows / duration)"]
    REMOTE["World Bank API\n(HTTPS)"]

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
    STREAM --> ZIP

    DL -->|"is_zip?"| UNZIP
    UNZIP --> CSV

    DL -->|"count_rows?"| COUNT
    COUNT -->|"row_count"| DL

    DL --> METRICS
    METRICS -->|"log.info"| CLI

    style CFG_FILE fill:#f5f0e8,stroke:#b8a070
    style REMOTE fill:#e8f0fe,stroke:#4a7fcb
    style METRICS fill:#e8f5e9,stroke:#4caf50
```
