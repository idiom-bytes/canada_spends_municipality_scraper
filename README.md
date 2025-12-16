# Canada Spends - Municipality Scraper

An end-to-end solution for finding municipality URLs, scraping them into the expected folder structure, and uploading to the Canada Spends API.

**GitHub Repo**: https://github.com/idiom-bytes/canada_spends_municipality_scraper

## Quick Start

### Step 1: Find Municipality URLs

Setup a local LLM using [Ollama](https://ollama.ai) and get a SERP API key from https://serpapi.com/ (free tier gives 250 searches/month).

```bash
# Install dependencies
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Setup Ollama
ollama pull mistral:7b-instruct
ollama serve

# Add API key to .env
cp .env.example .env
# Edit .env and add: GOOGLE_SERP_API_KEY=your_key_here

# Run URL discovery
python -m src.find_urls --limit 5
python -m src.find_urls --province 59  # British Columbia only
```

This uses `input_municipalities.csv` to do a Google search, then the LLM selects the most likely link to save into `output_municipality_urls.csv`.

### Step 2: Review URLs

Review the URLs inside `output_municipality_urls.csv` and make sure they point to the most-likely URL where the files exist. The download logic supports most HTML + FTP sites where all files are listed together.

### Step 3: Download Reports

```bash
python -m src.download_reports --limit 5
python -m src.download_reports --csd 5915022  # Vancouver only
python -m src.download_reports --retry-failed --limit 999
```

All data is saved to: `lake/<province_id>/<municipality_CSD>/financial_statement_YYYY.pdf`

### Step 4: Verify Downloads

Review the downloaded data for "Statement of Operations" or "Financial Statement" where Revenue + Expenses are listed. This verifies that the system has downloaded the correct data.

### Step 5: Upload to Canada Spends

Get your API key by registering at https://hub.buildcanada.com/

```bash
./upload_financial_statements.sh lake <your_build_canada_api_key>
```

Successful uploads are cached in `output_uploaded_records.csv` to avoid re-uploading.

### Step 6: Contribute Back

Help make it easier for others to get data! Push a PR to the repo by updating `output_municipality_urls.csv` with the latest URLs per municipality.

## Data Files

### Input Files

| File | Description |
|------|-------------|
| `input_municipalities.csv` | Municipality data from Statistics Canada (CSD ID, name, status, province) |
| `input_municipal_status_codes.csv` | Lookup: municipal status code -> name (e.g., CY -> City) |
| `input_province_codes.csv` | Lookup: province ID -> name (e.g., 59 -> British Columbia) |

### Output Files

| File | Description |
|------|-------------|
| `output_municipality_urls.csv` | Discovered URLs for each municipality's finance page |
| `output_master_records.csv` | Record of each downloaded PDF (source, URL, path, year) |
| `output_download_status.csv` | Status tracking per municipality (success/fail, years found) |
| `output_uploaded_records.csv` | Upload tracking (province, CSD, year, status) |
| `lake/` | Downloaded PDFs |

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
canada_spends_municipality_scraper/
├── input_municipalities.csv
├── input_municipal_status_codes.csv
├── input_province_codes.csv
├── output_municipality_urls.csv
├── output_master_records.csv
├── output_download_status.csv
├── output_uploaded_records.csv
├── upload_financial_statements.sh
├── src/
│   ├── municipality_helpers.py
│   ├── find_urls.py
│   └── download_reports.py
└── lake/
```
