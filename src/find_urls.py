"""
Find official municipal financial report page URLs.

Uses SerpAPI for Google search + LLM to pick the best URL.

Data source: input_municipalities.csv
Output: output_municipality_urls.csv

Usage:
    python -m src.find_urls --limit 5
    python -m src.find_urls --province 59  # British Columbia only
"""

import argparse
import asyncio
import csv
import os
import re
import sys
from pathlib import Path

import httpx
from dotenv import load_dotenv
from pydantic_ai import Agent
from pydantic_ai.models.openai import OpenAIChatModel
from pydantic_ai.providers.openai import OpenAIProvider

from .municipality_helpers import get_all_municipalities, get_municipalities_by_province, Municipality

load_dotenv()

SERP_API_KEY = os.getenv('GOOGLE_SERP_API_KEY')


async def google_search(query: str, num_results: int = 5) -> list[dict]:
    """
    Search using SerpAPI (real Google results).

    Returns list of dicts with 'title', 'link', 'snippet' keys.
    """
    if not SERP_API_KEY:
        raise ValueError("GOOGLE_SERP_API_KEY must be set in .env")

    url = 'https://serpapi.com/search'
    params = {
        'api_key': SERP_API_KEY,
        'engine': 'google',
        'q': query,
        'num': min(num_results, 10),
        'gl': 'ca',  # Country: Canada
        'hl': 'en',  # Language: English
    }

    async with httpx.AsyncClient() as client:
        response = await client.get(url, params=params, timeout=30)
        response.raise_for_status()

    data = response.json()
    results = data.get('organic_results', [])

    return [
        {
            'title': item.get('title', ''),
            'link': item.get('link', ''),
            'snippet': item.get('snippet', ''),
        }
        for item in results[:num_results]
    ]

BASE_DIR = Path(__file__).parent.parent
URLS_CSV = BASE_DIR / "output_municipality_urls.csv"

# LLM for picking best URL
ollama_model = OpenAIChatModel(
    model_name="mistral:7b-instruct",
    provider=OpenAIProvider(base_url="http://localhost:11434/v1", api_key="ollama"),
)

url_picker = Agent(
    ollama_model,
    system_prompt="""Pick the official municipal financial reports page from search results.

Prefer:
- Official .ca government sites with municipality name in domain
- Pages listing reports (not individual PDF documents)
- URLs with /finance/ or /reports/ in path

Avoid: news sites, charities, LinkedIn, Wikipedia.

Reply with ONLY the result number (0-4).""",
)


def is_pdf_url(url: str) -> bool:
    """Check if URL points to a PDF file."""
    return url.lower().endswith(".pdf")


def load_existing_urls() -> set[str]:
    """Load already-found census subdivision IDs to skip."""
    if not URLS_CSV.exists():
        return set()

    found = set()
    with open(URLS_CSV, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Use census_subdivision_id as unique key (names can be duplicated)
            csd_id = row.get("census_subdivision_id", "")
            if csd_id:
                found.add(csd_id)
            else:
                # Fallback for old format files
                found.add(row.get("municipality_name", ""))
    return found


def save_url(municipality: Municipality, url: str, query: str):
    """Append a found URL to the CSV."""
    file_exists = URLS_CSV.exists()

    with open(URLS_CSV, "a", newline="") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow([
                "census_subdivision_id", "municipality_name", "type",
                "province_id", "province", "search_terms", "page_url"
            ])
        writer.writerow([
            municipality.census_subdivision_id,
            municipality.name,
            municipality.municipal_status_name,
            municipality.province_id,
            municipality.province_name,
            query,
            url,
        ])


async def pick_best_url(municipality: str, results: list[dict]) -> tuple[int, str]:
    """
    Use LLM to pick the best URL from search results.
    Filters out PDFs before presenting to LLM.

    Returns (index, url).
    """
    # Filter out PDFs - we want listing pages, not documents
    filtered = [(i, r) for i, r in enumerate(results) if not is_pdf_url(r['link'])]

    # If all results are PDFs, use original list
    if not filtered:
        filtered = list(enumerate(results))

    results_text = "\n".join([
        f"{i}. {r['title']}\n   {r['link']}"
        for i, r in filtered
    ])

    prompt = f"""Municipality: {municipality}

{results_text}

Which is the official financial reports page? Reply with the number."""

    try:
        result = await url_picker.run(prompt)
        response = result.output.strip()

        match = re.search(r'\d', response)
        if match:
            idx = int(match.group())
            if 0 <= idx < len(results):
                return idx, results[idx]['link']

        print(f"    LLM response unclear: '{response}', using index 0")
        return filtered[0][0], results[filtered[0][0]]['link']

    except Exception as e:
        print(f"    LLM error: {e}, using index 0")
        return filtered[0][0], results[filtered[0][0]]['link']


async def find_url_for_municipality(municipality: Municipality) -> tuple[str | None, str]:
    """
    Search for the official financial reports page for a municipality.

    Returns (url, query) or (None, query) if not found.
    """
    # Use the Municipality's built-in SERP query generation
    query = municipality.get_serp_query(suffix="Annual Reports")
    print(f"\n  Query: {query}")

    try:
        results = await google_search(query, num_results=5)
    except Exception as e:
        print(f"  Search error: {e}")
        return None, query

    if not results:
        print("  No results found")
        return None, query

    print(f"  Results:")
    for i, r in enumerate(results):
        pdf_marker = " [PDF]" if is_pdf_url(r['link']) else ""
        print(f"    [{i}]{pdf_marker} {r['title'][:50]}")
        print(f"        {r['link'][:70]}")

    print(f"\n  Asking LLM to pick best URL...")
    idx, url = await pick_best_url(municipality.name, results)
    print(f"  LLM chose: [{idx}] {url[:70]}")

    return url, query


async def main(limit: int = 5, skip_existing: bool = True, province_id: str | None = None):
    """Find URLs for municipalities."""
    print("=" * 60)
    print("Finding Municipal Financial Report URLs")
    print("=" * 60)

    # Load municipalities using helpers
    if province_id:
        municipalities = get_municipalities_by_province(province_id)
        print(f"Province filter: {province_id}")
    else:
        municipalities = get_all_municipalities()
    print(f"Total municipalities: {len(municipalities)}")

    existing = load_existing_urls() if skip_existing else set()
    print(f"Already found: {len(existing)}")

    # Filter by census_subdivision_id (unique identifier)
    to_process = [m for m in municipalities if m.census_subdivision_id not in existing]
    print(f"To process: {len(to_process)}")
    print(f"Limit: {limit}")

    found = 0
    not_found = []

    for m in to_process[:limit]:
        print(f"\n[{found + 1}/{limit}] {m.name} ({m.municipal_status_name}) - CSD: {m.census_subdivision_id}")

        url, query = await find_url_for_municipality(m)

        if url:
            save_url(m, url, query)
            found += 1
        else:
            not_found.append(f"{m.name} ({m.census_subdivision_id})")

    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"URLs found: {found}")
    if not_found:
        print(f"Not found: {', '.join(not_found)}")
    print(f"\nSaved to: {URLS_CSV}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Find municipal financial report URLs")
    parser.add_argument("--limit", type=int, default=5, help="Max municipalities to process")
    parser.add_argument("--no-skip", action="store_true", help="Re-process existing entries")
    parser.add_argument("--province", "-p", type=str, help="Filter by province ID (e.g., 59 for BC, 35 for ON)")
    args = parser.parse_args()

    sys.stdout.reconfigure(line_buffering=True)
    asyncio.run(main(limit=args.limit, skip_existing=not args.no_skip, province_id=args.province))
