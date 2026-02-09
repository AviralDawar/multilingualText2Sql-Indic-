"""
Script to split India Employment Data into normalized tables.

Input: total_data.csv (single denormalized file with 71 columns)
Output: 7 normalized CSV files + sample JSON files + DDL.csv

Tables:
1. LOCATIONS - Geographic hierarchy
2. TIME_PERIOD - Time dimension
3. EMPLOYMENT_FACT - Core employment statistics
4. EMPLOYMENT_BY_INDUSTRY - Industry sector breakdown
5. EMPLOYMENT_BY_OWNERSHIP - Ownership type breakdown
6. EMPLOYMENT_BY_DEMOGRAPHICS - Demographics breakdown (gender, caste, religion)
7. EMPLOYMENT_BY_FIRM_CHARACTERISTICS - Firm operation and finance characteristics
"""

import csv
import json
import argparse
import sys
import re
from pathlib import Path
from collections import OrderedDict

# Add parent directory to path to import generate_ddl
sys.path.insert(0, str(Path(__file__).parent.parent))
from generate_ddl import generate_ddl_csv


# Table descriptions for DDL.csv
TABLE_DESCRIPTIONS = {
    'LOCATIONS': 'Geographic hierarchy dimension table containing country state district sub-district and village/town information',
    'TIME_PERIOD': 'Time dimension table with year information',
    'EMPLOYMENT_FACT': 'Central fact table with core employment statistics including gender-wise hired/not-hired counts and structure type',
    'EMPLOYMENT_BY_INDUSTRY': 'Employment breakdown by 23 industry/activity sectors including agriculture livestock manufacturing services etc',
    'EMPLOYMENT_BY_OWNERSHIP': 'Employment breakdown by firm ownership type including govt PSU private proprietary partnership company SHG cooperative etc',
    'EMPLOYMENT_BY_DEMOGRAPHICS': 'Employment breakdown by owner demographics including gender caste (SC ST OBC) and religion categories',
    'EMPLOYMENT_BY_FIRM_CHARACTERISTICS': 'Employment breakdown by firm operational characteristics including nature of operation (perennial seasonal casual) and major source of finance'
}


# Column mappings: original column index (0-based) -> new column name

LOCATION_COLUMNS = {
    0: 'COUNTRY',
    1: 'STATE',
    2: 'DISTRICT',
    3: 'SUB_DISTRICT',
    4: 'VILLAGE_TOWN'
}

TIME_COLUMNS = {
    5: 'YEAR_DESC'
}

EMPLOYMENT_FACT_COLUMNS = {
    6: 'SECTOR_TYPE',
    7: 'MALE_HIRED',
    8: 'MALE_NOT_HIRED',
    9: 'FEMALE_HIRED',
    10: 'FEMALE_NOT_HIRED',
    11: 'TOTAL_EMPLOYED',
    12: 'EMPLOYED_RESIDENTIAL_COMMERCIAL',
    13: 'EMPLOYED_COMMERCIAL'
}

INDUSTRY_COLUMNS = {
    14: 'AGRICULTURE_OTHER',
    15: 'LIVESTOCK',
    16: 'FORESTRY_LOGGING',
    17: 'AQUACULTURE',
    18: 'MINING_QUARRYING',
    19: 'MANUFACTURING',
    20: 'ELECTRICITY_GAS',
    21: 'WATER_WASTE',
    22: 'CONSTRUCTION',
    23: 'WHOLESALE_MOTOR',
    24: 'WHOLESALE_OTHER',
    25: 'RETAIL_TRADE',
    26: 'TRANSPORT_STORAGE',
    27: 'ACCOMMODATION_FOOD',
    28: 'INFO_COMMUNICATION',
    29: 'FINANCE_INSURANCE',
    30: 'REAL_ESTATE',
    31: 'PROFESSIONAL_SCIENTIFIC',
    32: 'ADMIN_SUPPORT',
    33: 'EDUCATION',
    34: 'HEALTH_SOCIAL',
    35: 'ARTS_ENTERTAINMENT',
    36: 'OTHER_ACTIVITIES'
}

OWNERSHIP_COLUMNS = {
    37: 'HANDLOOM_HANDICRAFT',
    38: 'NON_HANDLOOM',
    39: 'GOVT_PSU',
    40: 'PRIVATE_PROPRIETARY',
    41: 'PRIVATE_PARTNERSHIP',
    42: 'PRIVATE_COMPANY',
    43: 'PRIVATE_SHG',
    44: 'PRIVATE_COOPERATIVE',
    45: 'PRIVATE_NONPROFIT',
    46: 'PRIVATE_OTHER'
}

DEMOGRAPHICS_COLUMNS = {
    47: 'OWNER_MALE',
    48: 'OWNER_FEMALE',
    49: 'OWNER_OTHER',
    50: 'CASTE_SC',
    51: 'CASTE_ST',
    52: 'CASTE_OBC',
    53: 'CASTE_OTHER',
    54: 'RELIGION_HINDU',
    55: 'RELIGION_ISLAM',
    56: 'RELIGION_CHRISTIAN',
    57: 'RELIGION_SIKH',
    58: 'RELIGION_BUDDHIST',
    59: 'RELIGION_ZOROASTRIAN',
    60: 'RELIGION_JAIN',
    61: 'RELIGION_OTHER'
}

FIRM_CHARACTERISTICS_COLUMNS = {
    62: 'OPERATION_PERENNIAL',
    63: 'OPERATION_SEASONAL',
    64: 'OPERATION_CASUAL',
    65: 'FINANCE_SELF',
    66: 'FINANCE_GOVT',
    67: 'FINANCE_INSTITUTIONS',
    68: 'FINANCE_MONEYLENDERS',
    69: 'FINANCE_SHG',
    70: 'FINANCE_DONATIONS'
}


def is_valid_value(value: str) -> bool:
    """Check if a value is valid (not empty, not null, not NA)."""
    if value is None:
        return False
    value = str(value).strip()
    if value == '' or value.lower() in ('null', 'na', 'n/a', 'none', '-'):
        return False
    return True


def is_row_complete(row: list, column_indices: list) -> bool:
    """Check if all required columns in a row have valid values."""
    for idx in column_indices:
        if idx >= len(row) or not is_valid_value(row[idx]):
            return False
    return True


def extract_columns(row: list, column_mapping: dict) -> OrderedDict:
    """Extract and rename columns from a row based on mapping."""
    result = OrderedDict()
    for idx, new_name in column_mapping.items():
        if idx < len(row):
            result[new_name] = row[idx].strip() if row[idx] else ''
        else:
            result[new_name] = ''
    return result


def generate_location_key(row: list) -> str:
    """Generate a unique key for a location based on geographic hierarchy."""
    parts = [
        row[0].strip() if len(row) > 0 else '',  # Country
        row[1].strip() if len(row) > 1 else '',  # State
        row[2].strip() if len(row) > 2 else '',  # District
        row[3].strip() if len(row) > 3 else '',  # Sub-District
        row[4].strip() if len(row) > 4 else ''   # Village/Town
    ]
    return '|'.join(parts)


def extract_year(year_desc: str) -> int:
    """Extract numeric year from year description string."""
    # Pattern: "Calendar Year (Jan - Dec), 2013"
    match = re.search(r'(\d{4})', year_desc)
    if match:
        return int(match.group(1))
    return 0


def write_csv(filepath: Path, rows: list, headers: list):
    """Write rows to a CSV file."""
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)
    print(f"  Written: {filepath.name} ({len(rows)} rows)")


def write_sample_json(filepath: Path, rows: list, headers: list, sample_size: int = 5):
    """Write sample rows as JSON for reference."""
    samples = []
    for row in rows[:sample_size]:
        sample = OrderedDict()
        for i, header in enumerate(headers):
            sample[header] = row[i] if i < len(row) else None
        samples.append(sample)

    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(samples, f, indent=2)
    print(f"  Sample JSON: {filepath.name} ({len(samples)} rows)")


def split_employment_data(input_file: Path, output_dir: Path, sample_size: int = 5):
    """
    Split the employment data into normalized tables.

    Args:
        input_file: Path to total_data.csv
        output_dir: Directory to write output files
        sample_size: Number of sample rows for JSON files
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    data_dir = output_dir / 'data'
    data_dir.mkdir(exist_ok=True)

    # Data structures to hold table data
    locations = OrderedDict()  # key -> (location_id, location_data)
    time_periods = OrderedDict()  # year_desc -> (time_id, time_data)

    employment_fact_rows = []
    industry_rows = []
    ownership_rows = []
    demographics_rows = []
    firm_characteristics_rows = []

    # Statistics
    total_rows = 0
    skipped_rows = 0
    location_id_counter = 1
    time_id_counter = 1
    employment_id_counter = 1

    # Required columns for validation (location + time + sector + at least total employed)
    required_columns = list(LOCATION_COLUMNS.keys()) + list(TIME_COLUMNS.keys()) + [6, 11]

    print(f"Reading: {input_file}")
    print(f"Validating required columns per row...")

    with open(input_file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        headers = next(reader)  # Skip header

        for row in reader:
            total_rows += 1

            # Validate row completeness for required columns
            if not is_row_complete(row, required_columns):
                skipped_rows += 1
                continue

            # Process Location
            loc_key = generate_location_key(row)
            if loc_key not in locations:
                loc_data = extract_columns(row, LOCATION_COLUMNS)
                locations[loc_key] = (location_id_counter, loc_data)
                location_id_counter += 1

            location_id = locations[loc_key][0]

            # Process Time Period
            time_data = extract_columns(row, TIME_COLUMNS)
            year_desc = time_data['YEAR_DESC']
            if year_desc not in time_periods:
                year = extract_year(year_desc)
                time_periods[year_desc] = (time_id_counter, {'YEAR_DESC': year_desc, 'YEAR': year})
                time_id_counter += 1

            time_id = time_periods[year_desc][0]

            employment_id = employment_id_counter
            employment_id_counter += 1

            # Process Employment Fact table
            fact_data = extract_columns(row, EMPLOYMENT_FACT_COLUMNS)
            employment_fact_rows.append(
                [employment_id, location_id, time_id] + list(fact_data.values())
            )

            # Process Industry table
            industry_data = extract_columns(row, INDUSTRY_COLUMNS)
            industry_rows.append([employment_id] + list(industry_data.values()))

            # Process Ownership table
            ownership_data = extract_columns(row, OWNERSHIP_COLUMNS)
            ownership_rows.append([employment_id] + list(ownership_data.values()))

            # Process Demographics table
            demographics_data = extract_columns(row, DEMOGRAPHICS_COLUMNS)
            demographics_rows.append([employment_id] + list(demographics_data.values()))

            # Process Firm Characteristics table
            firm_data = extract_columns(row, FIRM_CHARACTERISTICS_COLUMNS)
            firm_characteristics_rows.append([employment_id] + list(firm_data.values()))

            if total_rows % 100000 == 0:
                print(f"  Processed {total_rows} rows...")

    print(f"\nProcessing complete!")
    print(f"  Total rows read: {total_rows}")
    print(f"  Rows skipped (missing data): {skipped_rows}")
    print(f"  Valid rows processed: {total_rows - skipped_rows}")
    print(f"  Unique locations: {len(locations)}")
    print(f"  Unique time periods: {len(time_periods)}")

    # Prepare location rows
    location_rows = []
    for loc_key, (loc_id, loc_data) in locations.items():
        location_rows.append([loc_id] + list(loc_data.values()))

    # Prepare time period rows
    time_period_rows = []
    for year_desc, (time_id, time_data) in time_periods.items():
        time_period_rows.append([time_id, time_data['YEAR_DESC'], time_data['YEAR']])

    # Define headers for each table
    location_headers = ['LOCATION_ID'] + list(LOCATION_COLUMNS.values())
    time_period_headers = ['TIME_ID', 'YEAR_DESC', 'YEAR']
    employment_fact_headers = ['EMPLOYMENT_ID', 'LOCATION_ID', 'TIME_ID'] + list(EMPLOYMENT_FACT_COLUMNS.values())
    industry_headers = ['EMPLOYMENT_ID'] + list(INDUSTRY_COLUMNS.values())
    ownership_headers = ['EMPLOYMENT_ID'] + list(OWNERSHIP_COLUMNS.values())
    demographics_headers = ['EMPLOYMENT_ID'] + list(DEMOGRAPHICS_COLUMNS.values())
    firm_characteristics_headers = ['EMPLOYMENT_ID'] + list(FIRM_CHARACTERISTICS_COLUMNS.values())

    print(f"\nWriting output files to: {output_dir}")

    # Write CSV files to data/ subdirectory
    write_csv(data_dir / 'LOCATIONS.csv', location_rows, location_headers)
    write_csv(data_dir / 'TIME_PERIOD.csv', time_period_rows, time_period_headers)
    write_csv(data_dir / 'EMPLOYMENT_FACT.csv', employment_fact_rows, employment_fact_headers)
    write_csv(data_dir / 'EMPLOYMENT_BY_INDUSTRY.csv', industry_rows, industry_headers)
    write_csv(data_dir / 'EMPLOYMENT_BY_OWNERSHIP.csv', ownership_rows, ownership_headers)
    write_csv(data_dir / 'EMPLOYMENT_BY_DEMOGRAPHICS.csv', demographics_rows, demographics_headers)
    write_csv(data_dir / 'EMPLOYMENT_BY_FIRM_CHARACTERISTICS.csv', firm_characteristics_rows, firm_characteristics_headers)

    # Write sample JSON files to schema directory
    print(f"\nWriting sample JSON files...")
    write_sample_json(output_dir / 'LOCATIONS.json', location_rows, location_headers, sample_size)
    write_sample_json(output_dir / 'TIME_PERIOD.json', time_period_rows, time_period_headers, sample_size)
    write_sample_json(output_dir / 'EMPLOYMENT_FACT.json', employment_fact_rows, employment_fact_headers, sample_size)
    write_sample_json(output_dir / 'EMPLOYMENT_BY_INDUSTRY.json', industry_rows, industry_headers, sample_size)
    write_sample_json(output_dir / 'EMPLOYMENT_BY_OWNERSHIP.json', ownership_rows, ownership_headers, sample_size)
    write_sample_json(output_dir / 'EMPLOYMENT_BY_DEMOGRAPHICS.json', demographics_rows, demographics_headers, sample_size)
    write_sample_json(output_dir / 'EMPLOYMENT_BY_FIRM_CHARACTERISTICS.json', firm_characteristics_rows, firm_characteristics_headers, sample_size)

    # Write DDL.csv file with table definitions (using generate_ddl module)
    print(f"\nWriting DDL.csv file...")
    generate_ddl_csv(output_dir, descriptions=TABLE_DESCRIPTIONS)

    print(f"\nDone! All files written to {output_dir}")

    return {
        'total_rows': total_rows,
        'skipped_rows': skipped_rows,
        'valid_rows': total_rows - skipped_rows,
        'unique_locations': len(locations),
        'unique_time_periods': len(time_periods)
    }


def main():
    parser = argparse.ArgumentParser(description='Split India Employment data into normalized tables')
    parser.add_argument('--input', type=str,
                        default='../../databases/INDIA_EMPLOYMENT_DATA/INDIA_EMPLOYMENT_DATA/data/total_data.csv',
                        help='Path to input CSV file')
    parser.add_argument('--output', type=str,
                        default='../../databases/INDIA_EMPLOYMENT_DATA/INDIA_EMPLOYMENT_DATA',
                        help='Output directory for split tables')
    parser.add_argument('--sample-size', type=int, default=5,
                        help='Number of sample rows for JSON files')

    args = parser.parse_args()

    input_file = Path(args.input)
    output_dir = Path(args.output)

    if not input_file.exists():
        print(f"Error: Input file not found: {input_file}")
        return

    split_employment_data(input_file, output_dir, args.sample_size)


if __name__ == "__main__":
    main()