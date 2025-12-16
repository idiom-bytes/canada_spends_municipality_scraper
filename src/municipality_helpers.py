"""
Helper functions for looking up municipality data from CSV files.

Data sources:
- input_municipalities.csv: Main municipality data (region/CSD ID, name, municipal_status, PR_UID)
- input_municipal_status_codes.csv: Maps municipal_status code -> full name
- input_province_codes.csv: Maps PR_UID -> province name
"""

import csv
import os
from typing import Dict, List, Optional
from dataclasses import dataclass

# Get the project root directory (parent of src/)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# CSV file paths
MUNICIPALITIES_CSV = os.path.join(BASE_DIR, 'input_municipalities.csv')
MUNICIPAL_STATUS_CODES_CSV = os.path.join(BASE_DIR, 'input_municipal_status_codes.csv')
PROVINCE_CODES_CSV = os.path.join(BASE_DIR, 'input_province_codes.csv')


@dataclass
class Municipality:
    """Represents a municipality with all lookup data resolved."""
    census_subdivision_id: str  # 'region' column - used for directory structure
    name: str
    municipal_status_code: str
    municipal_status_name: str  # looked up from status codes
    province_id: str  # PR_UID
    province_name: str  # looked up from province codes
    population: int

    def get_serp_query(self, suffix: str = "SOFI") -> str:
        """Generate a SERP search query string."""
        return f"{self.name} {self.municipal_status_name} {self.province_name} {suffix}"

    def get_download_dir(self, base_dir: str) -> str:
        """Get the download directory path for this municipality's files."""
        return os.path.join(base_dir, self.census_subdivision_id)


class MunicipalityLookup:
    """Helper class for looking up municipality data."""

    def __init__(self):
        self._municipal_status_codes: Optional[Dict[str, str]] = None
        self._province_codes: Optional[Dict[str, str]] = None
        self._municipalities: Optional[List[Dict]] = None
        self._municipalities_by_csd: Optional[Dict[str, Dict]] = None
        self._municipalities_by_name: Optional[Dict[str, List[Dict]]] = None

    @property
    def municipal_status_codes(self) -> Dict[str, str]:
        """Load and cache municipal status code -> name mapping."""
        if self._municipal_status_codes is None:
            self._municipal_status_codes = {}
            with open(MUNICIPAL_STATUS_CODES_CSV, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    code = row['code'].strip()
                    name = row['name'].strip()
                    if code:
                        self._municipal_status_codes[code] = name
        return self._municipal_status_codes

    @property
    def province_codes(self) -> Dict[str, str]:
        """Load and cache province ID -> name mapping."""
        if self._province_codes is None:
            self._province_codes = {}
            with open(PROVINCE_CODES_CSV, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    province_id = row['id'].strip()
                    province_name = row['province'].strip()
                    if province_id:
                        self._province_codes[province_id] = province_name
        return self._province_codes

    @property
    def municipalities(self) -> List[Dict]:
        """Load and cache all municipalities."""
        if self._municipalities is None:
            self._municipalities = []
            with open(MUNICIPALITIES_CSV, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    self._municipalities.append(row)
        return self._municipalities

    @property
    def municipalities_by_csd(self) -> Dict[str, Dict]:
        """Index municipalities by census subdivision ID (region)."""
        if self._municipalities_by_csd is None:
            self._municipalities_by_csd = {}
            for muni in self.municipalities:
                csd_id = muni.get('region', '').strip()
                if csd_id:
                    self._municipalities_by_csd[csd_id] = muni
        return self._municipalities_by_csd

    @property
    def municipalities_by_name(self) -> Dict[str, List[Dict]]:
        """Index municipalities by name (can have duplicates across provinces)."""
        if self._municipalities_by_name is None:
            self._municipalities_by_name = {}
            for muni in self.municipalities:
                name = muni.get('name', '').strip()
                if name:
                    if name not in self._municipalities_by_name:
                        self._municipalities_by_name[name] = []
                    self._municipalities_by_name[name].append(muni)
        return self._municipalities_by_name

    def get_municipal_status_name(self, code: str) -> str:
        """Look up the full name for a municipal status code."""
        return self.municipal_status_codes.get(code, code)

    def get_province_name(self, province_id: str) -> str:
        """Look up the province name for a PR_UID."""
        return self.province_codes.get(province_id, province_id)

    def get_municipality_by_csd(self, census_subdivision_id: str) -> Optional[Municipality]:
        """Get a Municipality object by census subdivision ID."""
        raw = self.municipalities_by_csd.get(census_subdivision_id)
        if raw is None:
            return None
        return self._raw_to_municipality(raw)

    def get_municipalities_by_name(self, name: str) -> List[Municipality]:
        """Get all municipalities matching a name."""
        raw_list = self.municipalities_by_name.get(name, [])
        return [self._raw_to_municipality(raw) for raw in raw_list]

    def get_all_municipalities(self) -> List[Municipality]:
        """Get all municipalities as Municipality objects."""
        return [self._raw_to_municipality(raw) for raw in self.municipalities]

    def get_municipalities_by_province(self, province_id: str) -> List[Municipality]:
        """Get all municipalities in a given province."""
        return [
            self._raw_to_municipality(raw)
            for raw in self.municipalities
            if raw.get('PR_UID', '').strip() == province_id
        ]

    def _raw_to_municipality(self, raw: Dict) -> Municipality:
        """Convert a raw CSV row to a Municipality object."""
        status_code = raw.get('municipal_status', '').strip()
        province_id = raw.get('PR_UID', '').strip()

        pop_str = raw.get('pop', '0').strip()
        try:
            population = int(pop_str)
        except ValueError:
            population = 0

        return Municipality(
            census_subdivision_id=raw.get('region', '').strip(),
            name=raw.get('name', '').strip(),
            municipal_status_code=status_code,
            municipal_status_name=self.get_municipal_status_name(status_code),
            province_id=province_id,
            province_name=self.get_province_name(province_id),
            population=population,
        )


# Singleton instance for convenience
_lookup_instance: Optional[MunicipalityLookup] = None

def get_lookup() -> MunicipalityLookup:
    """Get the singleton MunicipalityLookup instance."""
    global _lookup_instance
    if _lookup_instance is None:
        _lookup_instance = MunicipalityLookup()
    return _lookup_instance


# Convenience functions that use the singleton
def get_municipal_status_name(code: str) -> str:
    """Look up the full name for a municipal status code."""
    return get_lookup().get_municipal_status_name(code)


def get_province_name(province_id: str) -> str:
    """Look up the province name for a PR_UID."""
    return get_lookup().get_province_name(province_id)


def get_municipality_by_csd(census_subdivision_id: str) -> Optional[Municipality]:
    """Get a Municipality by census subdivision ID."""
    return get_lookup().get_municipality_by_csd(census_subdivision_id)


def get_all_municipalities() -> List[Municipality]:
    """Get all municipalities."""
    return get_lookup().get_all_municipalities()


def get_municipalities_by_province(province_id: str) -> List[Municipality]:
    """Get all municipalities in a province."""
    return get_lookup().get_municipalities_by_province(province_id)


def build_serp_query(name: str, municipal_status_code: str, province_id: str, suffix: str = "SOFI") -> str:
    """Build a SERP query string from individual components."""
    lookup = get_lookup()
    status_name = lookup.get_municipal_status_name(municipal_status_code)
    province_name = lookup.get_province_name(province_id)
    return f"{name} {status_name} {province_name} {suffix}"


if __name__ == '__main__':
    # Test the helpers
    lookup = get_lookup()

    print("=== Municipal Status Codes ===")
    for code, name in list(lookup.municipal_status_codes.items())[:5]:
        print(f"  {code} -> {name}")
    print(f"  ... ({len(lookup.municipal_status_codes)} total)")

    print("\n=== Province Codes ===")
    for pid, name in lookup.province_codes.items():
        print(f"  {pid} -> {name}")

    print("\n=== Sample Municipalities ===")
    for muni in get_all_municipalities()[:5]:
        print(f"  {muni.census_subdivision_id}: {muni.name}")
        print(f"    Type: {muni.municipal_status_name}")
        print(f"    Province: {muni.province_name}")
        print(f"    SERP: {muni.get_serp_query()}")
        print()
