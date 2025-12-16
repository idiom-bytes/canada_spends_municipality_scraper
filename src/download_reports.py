"""
Download annual reports from discovered URLs.

Input: output_municipality_urls.csv
Output: lake/<province_id>/<csd_id>/financial_statement_YYYY.pdf
        output_master_records.csv, output_download_status.csv

Features:
- Priority system: Annual Report > Financial Statement > SOFI
- Handles civicweb.net document centers
- Traverses subdirectories for additional documents

Usage:
    python -m src.download_reports --limit 5
    python -m src.download_reports --csd 5915022  # Vancouver only
"""

import argparse
import asyncio
import csv
import re
import sys
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin, unquote, urlparse

import httpx
from bs4 import BeautifulSoup

from .municipality_helpers import get_municipality_by_csd

# Current year - annual reports for this year can't exist yet
CURRENT_YEAR = datetime.now().year

BASE_DIR = Path(__file__).parent.parent
URLS_CSV = BASE_DIR / "output_municipality_urls.csv"
MASTER_CSV = BASE_DIR / "output_master_records.csv"
STATUS_CSV = BASE_DIR / "output_download_status.csv"
LAKE_DIR = BASE_DIR / "lake"

# Document type priorities (lower = better)
# Draft versions add 10 to priority (e.g., draft annual_report = 11)
DOCUMENT_PRIORITY = {
    'annual_report': 1,
    'financial_statement': 2,
    'sofi': 3,
    'other': 4,
}

DRAFT_PENALTY = 10  # Added to priority for draft documents


def is_draft_document(text: str, url: str) -> bool:
    """Check if document is a draft version."""
    combined = (text.lower() + " " + url.lower()).replace("_", " ").replace("-", " ")
    return 'draft' in combined


def is_civicweb_site(url: str) -> bool:
    """Check if URL is a civicweb.net document center."""
    return 'civicweb.net/filepro/documents' in url


async def fetch_civicweb_page(url: str) -> list[dict]:
    """
    Fetch a civicweb.net document center page.
    These use data-title attributes and /document/{id} links.
    """
    try:
        async with httpx.AsyncClient(follow_redirects=True, timeout=30.0) as client:
            response = await client.get(url)
            response.raise_for_status()

        soup = BeautifulSoup(response.text, "lxml")
        base_url = f"{urlparse(url).scheme}://{urlparse(url).netloc}"

        links = []
        seen_ids = set()

        # Find all elements with data-type="document" and data-title
        for elem in soup.find_all(attrs={"data-type": "document"}):
            doc_id = elem.get("data-id")
            title = elem.get("data-title", "")

            if not doc_id or doc_id in seen_ids:
                continue
            seen_ids.add(doc_id)

            # The PDF URL is /document/{id}
            doc_url = f"{base_url}/document/{doc_id}"

            links.append({
                "url": doc_url,
                "text": title,
                "is_pdf": True,  # civicweb /document/ endpoints serve PDFs
            })

        # Also look for direct /document/ links as fallback
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "/document/" in href and "filepro" not in href:
                # Extract document ID
                match = re.search(r'/document/(\d+)', href)
                if match:
                    doc_id = match.group(1)
                    if doc_id in seen_ids:
                        continue
                    seen_ids.add(doc_id)

                    doc_url = f"{base_url}/document/{doc_id}"
                    text = a.get("title") or a.get_text(strip=True)[:200]

                    links.append({
                        "url": doc_url,
                        "text": text,
                        "is_pdf": True,
                    })

        # Check for subdirectories (folders)
        for elem in soup.find_all(attrs={"data-type": "folder"}):
            folder_id = elem.get("data-id")
            folder_title = elem.get("data-title", "")

            if not folder_id:
                continue

            # Check if this folder might contain financial documents
            folder_lower = folder_title.lower()
            if any(kw in folder_lower for kw in ['report', 'finance', 'financial', 'annual', 'statement', 'sofi']):
                folder_url = f"{base_url}/filepro/documents/{folder_id}/"
                links.append({
                    "url": folder_url,
                    "text": folder_title,
                    "is_pdf": False,
                    "is_folder": True,
                })

        return links
    except Exception as e:
        print(f"    Fetch error (civicweb): {e}")
        return []


def looks_like_document_link(text: str, url: str) -> bool:
    """
    Check if a link looks like it points to a downloadable document.
    Matches URLs ending in .pdf OR links with text indicating a document.
    """
    url_lower = url.lower()
    text_lower = text.lower()

    # Explicit PDF extension
    if url_lower.endswith(".pdf"):
        return True

    # Common document URL patterns (like /media/123, /document/456, /files/)
    doc_url_patterns = ['/media/', '/document/', '/files/', '/download/', '/assets/']
    if any(p in url_lower for p in doc_url_patterns):
        # Check if link text suggests it's a report/statement
        doc_text_keywords = ['annual report', 'financial statement', 'sofi', 'view', 'download', 'report']
        if any(kw in text_lower for kw in doc_text_keywords):
            return True

    return False


async def fetch_page(url: str) -> list[dict]:
    """Fetch a webpage and extract links."""
    # Handle civicweb.net sites specially
    if is_civicweb_site(url):
        return await fetch_civicweb_page(url)

    try:
        async with httpx.AsyncClient(follow_redirects=True, timeout=30.0) as client:
            response = await client.get(url)
            response.raise_for_status()

        soup = BeautifulSoup(response.text, "lxml")

        links = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            text = a.get_text(strip=True)[:200]
            full_url = urljoin(url, href)

            # Check if this looks like a document link
            is_pdf = looks_like_document_link(text, full_url)

            links.append({
                "url": full_url,
                "text": text,
                "is_pdf": is_pdf,
            })

        return links
    except Exception as e:
        print(f"    Fetch error: {e}")
        return []


def extract_year(text: str) -> int | None:
    """
    Extract year from text. Handles various formats:
    - "Year Ended December 31, 2023" - preferred format
    - 2000-2029 (single year)
    - 2023-2024 fiscal year (returns end year)
    - FY2023, FY 2023

    If multiple years found, prefers the most recent year that's not the current year.
    """
    if not text:
        return None

    # First, check for "Year Ended ... YYYY" format (most accurate for annual reports)
    year_ended_match = re.search(r'year\s+ended[^0-9]*(20[0-2]\d)', text, re.IGNORECASE)
    if year_ended_match:
        return int(year_ended_match.group(1))

    # Try fiscal year range (take the end year)
    # Matches: 2023-2024, 2023/2024, 2023-24, 2023/24
    # Must be followed by word boundary or end to avoid matching dates like 2022-05-15
    fiscal_match = re.search(r'(20[0-2]\d)[-/](20[0-2]\d)\b', text)
    if fiscal_match:
        return int(fiscal_match.group(2))

    # Also try short form: 2023-24, 2023/24 (but not dates like 2022-05)
    # Only match if second part is >= 10 to avoid month numbers
    short_fiscal = re.search(r'20[0-2]\d[-/]([1-2]\d)\b', text)
    if short_fiscal:
        return 2000 + int(short_fiscal.group(1))

    # Find ALL years in the text (2000-2029)
    all_years = [int(m) for m in re.findall(r'20[0-2]\d', text)]
    if not all_years:
        return None

    # Filter out current year if there are alternatives
    valid_years = [y for y in all_years if y < CURRENT_YEAR]
    if valid_years:
        return max(valid_years)  # Return most recent valid year

    # If only current year found, return it (will be filtered later)
    return max(all_years)


def classify_document_type(text: str, url: str) -> str:
    """
    Classify document type based on text/URL.
    Returns: 'annual_report', 'financial_statement', 'sofi', or 'other'
    """
    combined = (text.lower() + " " + url.lower()).replace("_", " ").replace("-", " ")

    # Check for Annual Report
    if 'annual report' in combined:
        return 'annual_report'

    # Check for Financial Statement
    if any(kw in combined for kw in ['financial statement', 'audited financial', 'consolidated financial']):
        return 'financial_statement'

    # Check for SOFI (Statement of Financial Information)
    if 'sofi' in combined or 'statement of financial information' in combined:
        return 'sofi'

    return 'other'


def is_annual_report(text: str, url: str) -> bool:
    """
    Check if a PDF is an actual annual report (not budget/projection).

    Returns True for annual reports, False for budgets/projections.
    """
    text_lower = text.lower()
    url_lower = url.lower()
    # Normalize underscores/hyphens to spaces for matching
    combined = (text_lower + " " + url_lower).replace("_", " ").replace("-", " ")

    # Exclude: budgets, projections, future plans, quarterly reports
    # Note: 'draft' removed - we handle drafts via priority system (fallback if no final)
    exclude_keywords = [
        'budget', 'projection', 'forecast', 'plan', 'proposed',
        'preliminary', 'bylaw', 'tax rate', 'levy', 'quarterly',
    ]
    for kw in exclude_keywords:
        if kw in combined:
            return False

    # Include: annual reports, financial statements, audited reports, SOFI
    include_keywords = [
        'annual report', 'annual financial', 'financial statement',
        'audited', 'consolidated financial', 'year end', 'sofi',
    ]
    for kw in include_keywords:
        if kw in combined:
            return True

    # If it just says "annual" without "report" be more careful
    if 'annual' in combined and 'report' not in combined:
        return False

    # Generic "financial report" without more context - include it
    if 'financial report' in combined:
        return True

    return False


def extract_filename_from_content_disposition(header: str) -> str | None:
    """Extract filename from Content-Disposition header."""
    if not header:
        return None

    # Try to extract filename from header like: inline; filename="Annual Report 2024.pdf"
    match = re.search(r'filename[*]?=["\']?([^"\';\n]+)["\']?', header)
    if match:
        return unquote(match.group(1).strip())
    return None


async def download_pdf(url: str, save_path: Path) -> dict:
    """
    Download a PDF file.
    Returns dict with success status and optional metadata (original_filename).
    """
    try:
        async with httpx.AsyncClient(follow_redirects=True, timeout=120.0) as client:
            response = await client.get(url)
            response.raise_for_status()

            # Verify it's actually a PDF
            content_type = response.headers.get("content-type", "")
            if "pdf" not in content_type.lower() and not url.lower().endswith(".pdf"):
                print(f"    Not a PDF: {content_type}")
                return {"success": False}

            # Extract original filename from Content-Disposition header
            content_disposition = response.headers.get("content-disposition", "")
            original_filename = extract_filename_from_content_disposition(content_disposition)

            save_path.parent.mkdir(parents=True, exist_ok=True)
            save_path.write_bytes(response.content)
            return {
                "success": True,
                "original_filename": original_filename,
            }
    except Exception as e:
        print(f"    Download error: {e}")
        return {"success": False}


def record_download(
    census_subdivision_id: str,
    municipality: str,
    province_id: str,
    province: str,
    municipality_type: str,
    source_url: str,
    document_url: str,
    document_path: str,
    year: int | None = None,
):
    """Record a download in the master CSV."""
    file_exists = MASTER_CSV.exists()

    with open(MASTER_CSV, "a", newline="") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow([
                "census_subdivision_id", "municipality", "province_id", "province",
                "type", "year", "source_page_url", "document_url", "document_path"
            ])
        writer.writerow([
            census_subdivision_id, municipality, province_id, province,
            municipality_type, year or "", source_url, document_url, document_path
        ])


def load_status_csv() -> dict[tuple, dict]:
    """Load existing status CSV into a dict keyed by (census_subdivision_id, type)."""
    if not STATUS_CSV.exists():
        return {}

    status = {}
    with open(STATUS_CSV, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Use census_subdivision_id if available, fallback to municipality_name for old format
            csd_id = row.get("census_subdivision_id", row.get("municipality_name", ""))
            key = (csd_id, row["type"])
            status[key] = row
    return status


def count_files_on_disk(province_id: str, census_subdivision_id: str) -> int:
    """Count actual PDF files in the municipality's lake folder."""
    muni_dir = LAKE_DIR / province_id / census_subdivision_id
    if not muni_dir.exists():
        return 0
    return len(list(muni_dir.glob("*.pdf")))


def record_status(
    census_subdivision_id: str,
    municipality: str,
    province_id: str,
    province: str,
    municipality_type: str,
    page_url: str,
    status: str,
    downloaded: int,  # ignored - we count files on disk instead
    found: int,
    years: int,
    notes: str = "",
):
    """
    Record/update municipality status in the status CSV.
    This updates existing entries or appends new ones.
    The 'downloaded' field is always set to actual file count on disk.
    """
    # Load existing status
    all_status = load_status_csv()

    # Count actual files on disk (not session downloads)
    actual_downloaded = count_files_on_disk(province_id, census_subdivision_id)

    # Determine if needs reparse
    needs_reparse = "YES" if (status == "FAIL" or years < 5) else "NO"
    if not notes and years < 5 and status == "OK":
        notes = "Low year count"

    # Update or add entry - use census_subdivision_id as unique key
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    all_status[(census_subdivision_id, municipality_type)] = {
        "census_subdivision_id": census_subdivision_id,
        "municipality_name": municipality,
        "type": municipality_type,
        "province_id": province_id,
        "province": province,
        "page_url": page_url,
        "status": status,
        "downloaded": actual_downloaded,
        "found": found,
        "years": years,
        "needs_reparse": needs_reparse,
        "notes": notes,
        "last_updated": timestamp,
    }

    # Write back entire CSV (to update existing entries)
    fieldnames = [
        "census_subdivision_id", "municipality_name", "type", "province_id", "province", "status",
        "downloaded", "found", "years", "needs_reparse", "notes", "last_updated", "page_url"
    ]
    with open(STATUS_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for entry in all_status.values():
            writer.writerow(entry)


def load_municipality_urls() -> list[dict]:
    """Load URLs from CSV."""
    if not URLS_CSV.exists():
        return []

    urls = []
    with open(URLS_CSV, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            urls.append(row)
    return urls


def select_best_document_per_year(documents: list[dict]) -> dict[int, dict]:
    """
    Given a list of documents, select the best one per year based on priority.
    Priority: annual_report > financial_statement > sofi > other
    Draft documents get lower priority (DRAFT_PENALTY added) so we prefer final versions,
    but will fallback to drafts if no final version exists.
    """
    by_year: dict[int, dict] = {}

    for doc in documents:
        year = doc.get("year")
        if not year:
            continue

        # Skip current year - annual reports can't exist yet
        if year >= CURRENT_YEAR:
            continue

        doc_type = doc.get("doc_type", "other")
        is_draft = doc.get("is_draft", False)

        # Calculate priority - lower is better
        priority = DOCUMENT_PRIORITY.get(doc_type, 4)
        if is_draft:
            priority += DRAFT_PENALTY  # Drafts get lower priority

        if year not in by_year:
            by_year[year] = {**doc, "priority": priority}
        else:
            # Compare priorities - lower is better
            existing_priority = by_year[year].get("priority", 4)
            if priority < existing_priority:
                by_year[year] = {**doc, "priority": priority}

    return by_year


async def process_municipality(entry: dict, max_downloads: int = 50) -> dict:
    """
    Crawl a municipality's financial page and download annual reports.

    Features:
    - Traverses subdirectories to find more documents
    - Implements priority system for multiple files per year
    - Renames files to financial_statement_yyyy.pdf format
    - Directory structure: lake/<province_id>/<census_subdivision_id>/
    """
    # Extract fields - support both old and new CSV formats
    csd_id = entry.get("census_subdivision_id", "")
    name = entry.get("municipality_name", entry.get("name", ""))
    mtype = entry.get("type", "")
    province_id = entry.get("province_id", "")
    province = entry.get("province", "")
    url = entry.get("page_url", "").strip()

    # If we have csd_id but missing other fields, look them up
    if csd_id and (not province_id or not province):
        muni = get_municipality_by_csd(csd_id)
        if muni:
            province_id = province_id or muni.province_id
            province = province or muni.province_name
            name = name or muni.name
            mtype = mtype or muni.municipal_status_name

    print(f"\n  Crawling: {url[:70]}...")

    # Collect all links, including from subdirectories
    all_links = []
    urls_to_crawl = [url]
    crawled_urls = set()

    while urls_to_crawl:
        current_url = urls_to_crawl.pop(0)
        if current_url in crawled_urls:
            continue
        crawled_urls.add(current_url)

        links = await fetch_page(current_url)
        print(f"  Found {len(links)} links at {current_url[:50]}...")

        for link in links:
            if link.get("is_folder"):
                # Add subdirectory to crawl queue (limit depth)
                if len(crawled_urls) < 5:  # Max 5 directories
                    urls_to_crawl.append(link["url"])
            else:
                all_links.append(link)

    print(f"  Total links collected: {len(all_links)}")

    # Filter for annual report PDFs
    annual_pdfs = []
    seen_urls = set()

    for link in all_links:
        if not link.get("is_pdf"):
            continue
        if link["url"] in seen_urls:
            continue

        if is_annual_report(link["text"], link["url"]):
            # Extract year from text first (report year), URL as fallback (often upload date)
            year = extract_year(link["text"]) or extract_year(link["url"])
            doc_type = classify_document_type(link["text"], link["url"])
            is_draft = is_draft_document(link["text"], link["url"])

            annual_pdfs.append({
                **link,
                "year": year,
                "doc_type": doc_type,
                "is_draft": is_draft,
            })
            seen_urls.add(link["url"])

    print(f"  Annual report PDFs: {len(annual_pdfs)}")

    if not annual_pdfs:
        return {"success": False, "downloads": 0, "found": 0, "years": 0, "message": "No annual reports found"}

    # Select best document per year
    best_by_year = select_best_document_per_year(annual_pdfs)
    print(f"  Years with documents: {sorted(best_by_year.keys())}")

    # Show what we found
    for year in sorted(best_by_year.keys(), reverse=True)[:5]:
        doc = best_by_year[year]
        draft_marker = " [DRAFT]" if doc.get("is_draft") else ""
        print(f"    [{year}] ({doc['doc_type']}{draft_marker}) {doc['text'][:40]}")

    # Download - rename to financial_statement_yyyy.pdf
    # Directory structure: lake/<province_id>/<census_subdivision_id>/
    save_dir = LAKE_DIR / province_id / csd_id
    downloads = 0

    for year in sorted(best_by_year.keys(), reverse=True)[:max_downloads]:
        doc = best_by_year[year]

        # Standard filename format
        filename = f"financial_statement_{year}.pdf"
        filepath = save_dir / filename

        if filepath.exists():
            print(f"    Skip (exists): {filename}")
            continue

        print(f"    Downloading [{year}]: {doc['text'][:40]}...")
        result = await download_pdf(doc["url"], filepath)

        if result.get("success"):
            downloads += 1

            # If we got original filename from headers, try to extract year from it too
            original_filename = result.get("original_filename")
            if original_filename and not year:
                extracted_year = extract_year(original_filename)
                if extracted_year:
                    # Rename to proper format
                    new_filepath = save_dir / f"financial_statement_{extracted_year}.pdf"
                    if not new_filepath.exists():
                        filepath.rename(new_filepath)
                        filepath = new_filepath
                        year = extracted_year

            record_download(
                census_subdivision_id=csd_id,
                municipality=name,
                province_id=province_id,
                province=province,
                municipality_type=mtype,
                source_url=url,
                document_url=doc["url"],
                document_path=str(filepath.relative_to(BASE_DIR)),
                year=year,
            )

    # Also download documents without year (up to 5)
    no_year_docs = [d for d in annual_pdfs if not d.get("year")]
    for i, doc in enumerate(no_year_docs[:5]):
        filename = f"financial_statement_unknown_{i+1}.pdf"
        filepath = save_dir / filename

        if filepath.exists():
            continue

        print(f"    Downloading [????]: {doc['text'][:40]}...")
        result = await download_pdf(doc["url"], filepath)

        if result.get("success"):
            downloads += 1

            # Try to get year from original filename
            original_filename = result.get("original_filename")
            year = None
            if original_filename:
                year = extract_year(original_filename)
                if year:
                    new_filepath = save_dir / f"financial_statement_{year}.pdf"
                    if not new_filepath.exists():
                        filepath.rename(new_filepath)
                        filepath = new_filepath

            record_download(
                census_subdivision_id=csd_id,
                municipality=name,
                province_id=province_id,
                province=province,
                municipality_type=mtype,
                source_url=url,
                document_url=doc["url"],
                document_path=str(filepath.relative_to(BASE_DIR)),
                year=year,
            )

    return {
        "success": downloads > 0 or len(annual_pdfs) > 0,
        "downloads": downloads,
        "found": len(annual_pdfs),
        "years": len(best_by_year),
        "message": f"Downloaded {downloads}, found {len(annual_pdfs)} total for {len(best_by_year)} years",
    }


async def main(
    limit: int = 5,
    municipality: str | None = None,
    csd_id: str | None = None,
    retry_failed: bool = False,
    retry_incomplete: bool = False,
):
    """Download annual reports from discovered URLs."""
    print("=" * 60)
    print("Downloading Municipal Annual Reports")
    print("=" * 60)

    entries = load_municipality_urls()
    print(f"URLs available: {len(entries)}")

    if not entries:
        print(f"\nNo URLs found. Run first:")
        print("  python -m src.find_urls --limit 10")
        return

    # Filter by census subdivision ID if specified
    if csd_id:
        entries = [e for e in entries if e.get("census_subdivision_id", "") == csd_id]
        print(f"Filtered to {len(entries)} entries matching CSD '{csd_id}'")

    # Filter by municipality name if specified
    if municipality:
        entries = [e for e in entries if municipality.lower() in e.get("municipality_name", "").lower()]
        print(f"Filtered to {len(entries)} entries matching '{municipality}'")

    # Filter by status if retry options specified
    if retry_failed or retry_incomplete:
        existing_status = load_status_csv()
        filtered_entries = []
        for entry in entries:
            # Use census_subdivision_id as key, fallback to municipality_name
            csd = entry.get("census_subdivision_id", entry.get("municipality_name", ""))
            mtype = entry.get("type", "")
            status = existing_status.get((csd, mtype), {})

            if retry_failed and status.get("status") == "FAIL":
                filtered_entries.append(entry)
            elif retry_incomplete and status.get("needs_reparse") == "YES":
                filtered_entries.append(entry)
            elif not status:
                # Include entries not yet processed
                filtered_entries.append(entry)

        entries = filtered_entries
        filter_type = "failed" if retry_failed else "incomplete/needs reparse"
        print(f"Filtered to {len(entries)} {filter_type} entries")

    if not entries:
        print(f"No municipalities to process")
        return

    results = []

    for i, entry in enumerate(entries[:limit]):
        csd = entry.get("census_subdivision_id", "")
        name = entry.get("municipality_name", "")
        mtype = entry.get("type", "")
        province_id = entry.get("province_id", "")
        province = entry.get("province", "")

        print(f"\n[{i+1}/{min(limit, len(entries))}] {name} ({mtype}) - CSD: {csd}")

        result = await process_municipality(entry, max_downloads=50)
        results.append({"municipality": name, "csd_id": csd, **result})

        # Record status after each municipality
        status_str = "OK" if result["success"] else "FAIL"
        notes = "" if result["success"] else result.get("message", "")
        record_status(
            census_subdivision_id=csd,
            municipality=name,
            province_id=province_id,
            province=province,
            municipality_type=mtype,
            page_url=entry.get("page_url", ""),
            status=status_str,
            downloaded=result["downloads"],
            found=result["found"],
            years=result["years"],
            notes=notes,
        )

    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    for r in results:
        status = "OK" if r["success"] else "FAIL"
        csd_info = f" ({r['csd_id']})" if r.get('csd_id') else ""
        print(f"  [{status}] {r['municipality']}{csd_info}: {r['message']}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download annual reports from URLs")
    parser.add_argument("--limit", type=int, default=5, help="Max municipalities to process")
    parser.add_argument("--municipality", "-m", type=str, help="Filter by municipality name")
    parser.add_argument("--csd", type=str, help="Filter by census subdivision ID (e.g., 5915022 for Vancouver)")
    parser.add_argument("--retry-failed", action="store_true", help="Only retry previously failed municipalities")
    parser.add_argument("--retry-incomplete", action="store_true", help="Retry municipalities marked as needs_reparse")
    args = parser.parse_args()

    sys.stdout.reconfigure(line_buffering=True)
    asyncio.run(main(
        limit=args.limit,
        municipality=args.municipality,
        csd_id=args.csd,
        retry_failed=args.retry_failed,
        retry_incomplete=args.retry_incomplete,
    ))
