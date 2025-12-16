# Municipal Annual Reports Downloader

Automated pipeline to discover, download, and organize municipal financial statements from Canadian municipalities.

## Overview

1. **URL Discovery** - Uses Google SERP API + local LLM to find official municipal finance pages
2. **Report Download** - Crawls discovered pages to find and download PDF reports
3. **Data Extraction** - (Planned) Extract structured data from PDFs via API

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env  # Add GOOGLE_SERP_API_KEY

# Install Ollama for LLM: https://ollama.ai
ollama pull mistral:7b-instruct
ollama serve
```

## Data Files

### Input Files

| File | Description |
|------|-------------|
| `input_municipalities.csv` | Municipality data from Statistics Canada (CSD ID, name, status, province) |
| `input_municipal_status_codes.csv` | Lookup: municipal status code → name (e.g., CY → City) |
| `input_province_codes.csv` | Lookup: province ID → name (e.g., 59 → British Columbia) |

### Output Files

| File | Description |
|------|-------------|
| `output_municipality_urls.csv` | Discovered URLs for each municipality's finance page |
| `output_master_records.csv` | Record of each downloaded PDF (source, URL, path, year) |
| `output_download_status.csv` | Status tracking per municipality (success/fail, years found) |
| `lake/` | Downloaded PDFs: `lake/<province_id>/<csd_id>/financial_statement_YYYY.pdf` |

## Usage

### Step 1: Find URLs

```bash
python -m src.find_urls --limit 5
python -m src.find_urls --province 59  # British Columbia only
```

### Step 2: Download Reports

```bash
python -m src.download_reports --limit 5
python -m src.download_reports --csd 5915022  # Vancouver only
python -m src.download_reports --retry-failed --limit 999
```

## Directory Structure

```
lake/
└── 59/                    # Province ID (British Columbia)
    ├── 5915022/           # CSD ID (Vancouver)
    │   └── financial_statement_2024.pdf
    └── BC_RD_01/          # BC Regional District (Alberni-Clayoquot)
        └── financial_statement_2023.pdf
```

## Project Structure

```
municipal_annual_reports/
├── input_municipalities.csv        # StatsCan municipality data
├── input_municipal_status_codes.csv
├── input_province_codes.csv
├── output_municipality_urls.csv    # Discovered URLs
├── output_master_records.csv       # Download records
├── output_download_status.csv      # Status tracking
├── src/
│   ├── municipality_helpers.py     # Lookup helpers
│   ├── find_urls.py                # URL discovery
│   └── download_reports.py         # Report downloading
└── lake/                           # Downloaded PDFs
```

## Status Tracking

`output_download_status.csv` columns:

| Column | Description |
|--------|-------------|
| `census_subdivision_id` | StatsCan CSD ID |
| `province_id` | Province ID |
| `municipality_name` | Name |
| `type` | District, City, etc. |
| `status` | OK or FAIL |
| `downloaded` | PDF count on disk |
| `years` | Unique years covered |
| `needs_reparse` | YES if failed or <5 years |
