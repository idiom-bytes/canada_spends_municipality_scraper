#!/bin/bash

# Upload Financial Statements Script
# Usage: ./upload_financial_statements.sh <directory> <api_key> [api_url]
#
# Directory structure: /path/to/lake/<province_id>/<census_subdivision_id>/<filename_with_year>.pdf
# Example: /data/lake/59/5901006/financial_statement_2023.pdf
#
# Tracks uploads in output_uploaded_records.csv to avoid re-uploading

set -e

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Upload tracking file (in same directory as script)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
UPLOAD_RECORDS="${SCRIPT_DIR}/output_uploaded_records.csv"

# Function to print colored output
print_error() {
    echo -e "${RED}ERROR: $1${NC}" >&2
}

print_success() {
    echo -e "${GREEN}SUCCESS: $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}WARNING: $1${NC}"
}

print_info() {
    echo -e "${BLUE}INFO: $1${NC}"
}

# Initialize upload records CSV if it doesn't exist
init_upload_records() {
    if [ ! -f "$UPLOAD_RECORDS" ]; then
        echo "province_id,census_subdivision_id,year,file_path,uploaded_at,status" > "$UPLOAD_RECORDS"
        print_info "Created upload tracking file: $UPLOAD_RECORDS"
    fi
}

# Check if file was already uploaded (by province_id + csd + year)
is_already_uploaded() {
    local province_id="$1"
    local census_subdivision_id="$2"
    local year="$3"

    if [ ! -f "$UPLOAD_RECORDS" ]; then
        return 1  # Not uploaded (no records file)
    fi

    # Check if this combination exists with status=success
    if grep -q "^${province_id},${census_subdivision_id},${year},.*,success$" "$UPLOAD_RECORDS" 2>/dev/null; then
        return 0  # Already uploaded
    fi

    return 1  # Not uploaded
}

# Record an upload
record_upload() {
    local province_id="$1"
    local census_subdivision_id="$2"
    local year="$3"
    local file_path="$4"
    local status="$5"

    local uploaded_at
    uploaded_at=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

    echo "${province_id},${census_subdivision_id},${year},${file_path},${uploaded_at},${status}" >> "$UPLOAD_RECORDS"
}

# Function to extract year from filename
extract_year() {
    local filename="$1"
    local max_year=0

    while [[ $filename =~ (19|20)[0-9]{2} ]]; do
        local year="${BASH_REMATCH[0]}"
        if [ "$year" -gt "$max_year" ]; then
            max_year="$year"
        fi
        filename="${filename/${BASH_REMATCH[0]}/}"
    done

    if [ "$max_year" -gt 0 ]; then
        echo "$max_year"
        return 0
    else
        return 1
    fi
}

# Function to upload a single file
# Returns: 0=success, 1=failure, 2=already_exists_on_server
upload_file() {
    local province_id="$1"
    local census_subdivision_id="$2"
    local year="$3"
    local file_path="$4"
    local api_key="$5"
    local api_url="$6"

    local endpoint="${api_url}/api/v1/bodies/${census_subdivision_id}/${year}"

    print_info "Uploading: CSD=${census_subdivision_id}, Year=${year}"

    # Make the API call
    response=$(curl -s -w "\n%{http_code}" -X POST \
        "$endpoint" \
        -H "Authorization: Bearer ${api_key}" \
        -F "document=@${file_path}")

    # Extract HTTP status code (last line)
    http_code=$(echo "$response" | tail -n1)
    # Extract response body (all but last line)
    response_body=$(echo "$response" | sed '$d')

    case $http_code in
        201)
            print_success "Uploaded successfully"
            record_upload "$province_id" "$census_subdivision_id" "$year" "$file_path" "success"
            return 0
            ;;
        400)
            print_error "Bad request"
            echo "$response_body" | jq -r '.error + ": " + .details' 2>/dev/null || echo "$response_body"
            record_upload "$province_id" "$census_subdivision_id" "$year" "$file_path" "failed"
            return 1
            ;;
        401)
            print_error "Unauthorized - Invalid API key"
            return 1
            ;;
        404)
            print_error "Body not found for CSD ${census_subdivision_id}"
            record_upload "$province_id" "$census_subdivision_id" "$year" "$file_path" "failed"
            return 1
            ;;
        409)
            print_warning "Already exists on server"
            record_upload "$province_id" "$census_subdivision_id" "$year" "$file_path" "success"
            return 2
            ;;
        *)
            print_error "Unexpected error (HTTP ${http_code})"
            record_upload "$province_id" "$census_subdivision_id" "$year" "$file_path" "failed"
            return 1
            ;;
    esac
}

# Main script

# Check arguments
if [ $# -lt 2 ]; then
    echo "Usage: $0 <directory> <api_key> [api_url]"
    echo ""
    echo "Arguments:"
    echo "  directory  - Path to directory containing financial statements"
    echo "               Structure: lake/<province_id>/<census_subdivision_id>/<filename>.pdf"
    echo "  api_key    - Your Build Canada Hub API token"
    echo "  api_url    - (Optional) API base URL (default: https://hub.buildcanada.com)"
    echo ""
    echo "Example:"
    echo "  $0 lake/59 bch_abc123..."
    echo "  $0 lake bch_abc123... http://localhost:3000"
    echo ""
    echo "Uploads are tracked in: output_uploaded_records.csv"
    exit 1
fi

DIRECTORY="$1"
API_KEY="$2"
API_URL="${3:-https://hub.buildcanada.com}"

# Validate directory exists
if [ ! -d "$DIRECTORY" ]; then
    print_error "Directory does not exist: $DIRECTORY"
    exit 1
fi

# Validate API key format
if [[ ! $API_KEY =~ ^bch_ ]]; then
    print_warning "API key does not start with 'bch_' - are you sure this is correct?"
fi

# Check for required commands
for cmd in curl jq; do
    if ! command -v $cmd &> /dev/null; then
        print_error "Required command '$cmd' not found. Please install it."
        exit 1
    fi
done

# Initialize upload records
init_upload_records

print_info "Starting upload process..."
print_info "Directory: $DIRECTORY"
print_info "API URL: $API_URL"
print_info "Tracking file: $UPLOAD_RECORDS"
echo ""

# Counters
total_files=0
successful_uploads=0
already_uploaded=0
failed_uploads=0
skipped_files=0

# Find all PDF files in subdirectories
while IFS= read -r -d '' pdf_file; do
    ((total_files++)) || true

    # Extract path components
    # Expected: .../lake/<province_id>/<census_subdivision_id>/file.pdf
    dir_path=$(dirname "$pdf_file")
    census_subdivision_id=$(basename "$dir_path")
    province_id=$(basename "$(dirname "$dir_path")")
    filename=$(basename "$pdf_file")

    # Validate province_id looks like a number (10-62 for Canadian provinces)
    if ! [[ "$province_id" =~ ^[0-9]+$ ]]; then
        # Try going one more level up (in case structure is different)
        province_id=$(basename "$(dirname "$(dirname "$dir_path")")")
        if ! [[ "$province_id" =~ ^[0-9]+$ ]]; then
            province_id="unknown"
        fi
    fi

    # Extract year from filename
    if ! year=$(extract_year "$filename"); then
        print_warning "Could not extract year from filename: $filename (skipping)"
        ((skipped_files++)) || true
        continue
    fi

    # Check if already uploaded
    if is_already_uploaded "$province_id" "$census_subdivision_id" "$year"; then
        print_info "[$total_files] Already uploaded: $pdf_file (skipping)"
        ((already_uploaded++)) || true
        continue
    fi

    echo ""
    echo "[$total_files] Processing: $pdf_file"
    echo "    Province: $province_id, CSD: $census_subdivision_id, Year: $year"

    upload_result=0
    upload_file "$province_id" "$census_subdivision_id" "$year" "$pdf_file" "$API_KEY" "$API_URL" || upload_result=$?

    case $upload_result in
        0)
            ((successful_uploads++)) || true
            ;;
        2)
            # Already exists on server (409) - count as success
            ((successful_uploads++)) || true
            ;;
        *)
            ((failed_uploads++)) || true
            ;;
    esac

done < <(find "$DIRECTORY" -type f -name "*.pdf" -print0)

# Print summary
echo ""
echo "================================================"
echo "                   SUMMARY                      "
echo "================================================"
echo "Total files found:      $total_files"
echo "Already uploaded:       $already_uploaded"
echo "Successful uploads:     $successful_uploads"
echo "Failed uploads:         $failed_uploads"
echo "Skipped (no year):      $skipped_files"
echo "================================================"
echo "Upload records: $UPLOAD_RECORDS"

if [ $failed_uploads -gt 0 ]; then
    exit 1
else
    exit 0
fi
