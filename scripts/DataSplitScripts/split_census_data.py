"""
Script to split India Population Census data into normalized tables.

Input: total_data.csv (single denormalized file with 63 columns)
Output: 7 normalized CSV files + sample JSON files + DDL.csv

Tables:
1. LOCATIONS - Geographic hierarchy
2. CENSUS_POPULATION - Basic demographics
3. CENSUS_CASTE - SC/ST population
4. CENSUS_LITERACY - Education statistics
5. CENSUS_WORKERS_SUMMARY - Employment overview
6. CENSUS_MAIN_WORKERS_DETAIL - Main worker categories
7. CENSUS_MARGINAL_WORKERS_DETAIL - Marginal worker categories
"""

import csv
import json
import argparse
import sys
from pathlib import Path
from collections import OrderedDict

# Add parent directory to path to import generate_ddl
sys.path.insert(0, str(Path(__file__).parent.parent))
from generate_ddl import generate_ddl_csv


# Table descriptions for DDL.csv
TABLE_DESCRIPTIONS = {
    'LOCATIONS': 'Geographic hierarchy dimension table containing country state district sub-district and village information',
    'CENSUS_POPULATION': 'Basic population demographics including households total population and age 0-6 population by gender',
    'CENSUS_CASTE': 'Scheduled Caste and Scheduled Tribe population statistics by gender',
    'CENSUS_LITERACY': 'Literacy statistics including literate and illiterate population counts by gender',
    'CENSUS_WORKERS_SUMMARY': 'Employment overview with working population main workers marginal workers and non-workers by gender',
    'CENSUS_MAIN_WORKERS_DETAIL': 'Detailed breakdown of main workers by occupation category including cultivators agricultural labourers household industry and other workers',
    'CENSUS_MARGINAL_WORKERS_DETAIL': 'Detailed breakdown of marginal workers by occupation category including cultivators agricultural labourers household industry and other workers'
}


# Column mappings: original column index (0-based) -> new column name
LOCATION_COLUMNS = {
    0: 'COUNTRY',
    1: 'STATE',
    2: 'DISTRICT',
    3: 'SUB_DISTRICT',
    4: 'VILLAGE_ULB',
    7: 'LOCATION_LEVEL'
}

POPULATION_COLUMNS = {
    5: 'CENSUS_YEAR',
    6: 'RESIDENCE_TYPE',
    8: 'HOUSEHOLDS',
    9: 'POPULATION_TOTAL',
    10: 'POPULATION_MALE',
    11: 'POPULATION_FEMALE',
    12: 'POPULATION_0_6_TOTAL',
    13: 'POPULATION_0_6_MALE',
    14: 'POPULATION_0_6_FEMALE'
}

CASTE_COLUMNS = {
    15: 'SC_POPULATION_TOTAL',
    16: 'SC_POPULATION_MALE',
    17: 'SC_POPULATION_FEMALE',
    18: 'ST_POPULATION_TOTAL',
    19: 'ST_POPULATION_MALE',
    20: 'ST_POPULATION_FEMALE'
}

LITERACY_COLUMNS = {
    21: 'LITERATE_TOTAL',
    22: 'LITERATE_MALE',
    23: 'LITERATE_FEMALE',
    24: 'ILLITERATE_TOTAL',
    25: 'ILLITERATE_MALE',
    26: 'ILLITERATE_FEMALE'
}

WORKERS_SUMMARY_COLUMNS = {
    27: 'WORKING_TOTAL',
    28: 'WORKING_MALE',
    29: 'WORKING_FEMALE',
    30: 'MAIN_WORKERS_TOTAL',
    31: 'MAIN_WORKERS_MALE',
    32: 'MAIN_WORKERS_FEMALE',
    45: 'MARGINAL_WORKERS_TOTAL',
    46: 'MARGINAL_WORKERS_MALE',
    47: 'MARGINAL_WORKERS_FEMALE',
    60: 'NON_WORKERS_TOTAL',
    61: 'NON_WORKERS_MALE',
    62: 'NON_WORKERS_FEMALE'
}

MAIN_WORKERS_DETAIL_COLUMNS = {
    33: 'CULTIVATORS_TOTAL',
    34: 'CULTIVATORS_MALE',
    35: 'CULTIVATORS_FEMALE',
    36: 'AGRI_LABOURERS_TOTAL',
    37: 'AGRI_LABOURERS_MALE',
    38: 'AGRI_LABOURERS_FEMALE',
    39: 'HOUSEHOLD_INDUSTRY_TOTAL',
    40: 'HOUSEHOLD_INDUSTRY_MALE',
    41: 'HOUSEHOLD_INDUSTRY_FEMALE',
    42: 'OTHER_WORKERS_TOTAL',
    43: 'OTHER_WORKERS_MALE',
    44: 'OTHER_WORKERS_FEMALE'
}

MARGINAL_WORKERS_DETAIL_COLUMNS = {
    48: 'CULTIVATORS_TOTAL',
    49: 'CULTIVATORS_MALE',
    50: 'CULTIVATORS_FEMALE',
    51: 'AGRI_LABOURERS_TOTAL',
    52: 'AGRI_LABOURERS_MALE',
    53: 'AGRI_LABOURERS_FEMALE',
    54: 'HOUSEHOLD_INDUSTRY_TOTAL',
    55: 'HOUSEHOLD_INDUSTRY_MALE',
    56: 'HOUSEHOLD_INDUSTRY_FEMALE',
    57: 'OTHER_WORKERS_TOTAL',
    58: 'OTHER_WORKERS_MALE',
    59: 'OTHER_WORKERS_FEMALE'
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
        row[4].strip() if len(row) > 4 else '',  # Village/ULB
        row[7].strip() if len(row) > 7 else ''   # Location Level
    ]
    return '|'.join(parts)


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




def split_census_data(input_file: Path, output_dir: Path, sample_size: int = 5):
    """
    Split the census data into normalized tables.

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
    population_rows = []
    caste_rows = []
    literacy_rows = []
    workers_summary_rows = []
    main_workers_rows = []
    marginal_workers_rows = []

    # Statistics
    total_rows = 0
    skipped_rows = 0
    location_id_counter = 1
    census_id_counter = 1

    # All column indices we need to validate
    all_columns = set()
    all_columns.update(LOCATION_COLUMNS.keys())
    all_columns.update(POPULATION_COLUMNS.keys())
    all_columns.update(CASTE_COLUMNS.keys())
    all_columns.update(LITERACY_COLUMNS.keys())
    all_columns.update(WORKERS_SUMMARY_COLUMNS.keys())
    all_columns.update(MAIN_WORKERS_DETAIL_COLUMNS.keys())
    all_columns.update(MARGINAL_WORKERS_DETAIL_COLUMNS.keys())

    print(f"Reading: {input_file}")
    print(f"Validating {len(all_columns)} columns per row...")

    with open(input_file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        headers = next(reader)  # Skip header

        for row in reader:
            total_rows += 1

            # Validate row completeness
            if not is_row_complete(row, list(all_columns)):
                skipped_rows += 1
                continue

            # Process Location
            loc_key = generate_location_key(row)
            if loc_key not in locations:
                loc_data = extract_columns(row, LOCATION_COLUMNS)
                locations[loc_key] = (location_id_counter, loc_data)
                location_id_counter += 1

            location_id = locations[loc_key][0]
            census_id = census_id_counter
            census_id_counter += 1

            # Process Population table
            pop_data = extract_columns(row, POPULATION_COLUMNS)
            population_rows.append([census_id, location_id] + list(pop_data.values()))

            # Process Caste table
            caste_data = extract_columns(row, CASTE_COLUMNS)
            caste_rows.append([census_id] + list(caste_data.values()))

            # Process Literacy table
            literacy_data = extract_columns(row, LITERACY_COLUMNS)
            literacy_rows.append([census_id] + list(literacy_data.values()))

            # Process Workers Summary table
            workers_data = extract_columns(row, WORKERS_SUMMARY_COLUMNS)
            workers_summary_rows.append([census_id] + list(workers_data.values()))

            # Process Main Workers Detail table
            main_workers_data = extract_columns(row, MAIN_WORKERS_DETAIL_COLUMNS)
            main_workers_rows.append([census_id] + list(main_workers_data.values()))

            # Process Marginal Workers Detail table
            marginal_data = extract_columns(row, MARGINAL_WORKERS_DETAIL_COLUMNS)
            marginal_workers_rows.append([census_id] + list(marginal_data.values()))

            if total_rows % 100000 == 0:
                print(f"  Processed {total_rows} rows...")

    print(f"\nProcessing complete!")
    print(f"  Total rows read: {total_rows}")
    print(f"  Rows skipped (missing data): {skipped_rows}")
    print(f"  Valid rows processed: {total_rows - skipped_rows}")
    print(f"  Unique locations: {len(locations)}")

    # Prepare location rows
    location_rows = []
    for loc_key, (loc_id, loc_data) in locations.items():
        location_rows.append([loc_id] + list(loc_data.values()))

    # Define headers for each table
    location_headers = ['LOCATION_ID'] + list(LOCATION_COLUMNS.values())
    population_headers = ['CENSUS_ID', 'LOCATION_ID'] + list(POPULATION_COLUMNS.values())
    caste_headers = ['CENSUS_ID'] + list(CASTE_COLUMNS.values())
    literacy_headers = ['CENSUS_ID'] + list(LITERACY_COLUMNS.values())
    workers_summary_headers = ['CENSUS_ID'] + list(WORKERS_SUMMARY_COLUMNS.values())
    main_workers_headers = ['CENSUS_ID'] + list(MAIN_WORKERS_DETAIL_COLUMNS.values())
    marginal_workers_headers = ['CENSUS_ID'] + list(MARGINAL_WORKERS_DETAIL_COLUMNS.values())

    print(f"\nWriting output files to: {output_dir}")

    # Write CSV files to data/ subdirectory
    write_csv(data_dir / 'LOCATIONS.csv', location_rows, location_headers)
    write_csv(data_dir / 'CENSUS_POPULATION.csv', population_rows, population_headers)
    write_csv(data_dir / 'CENSUS_CASTE.csv', caste_rows, caste_headers)
    write_csv(data_dir / 'CENSUS_LITERACY.csv', literacy_rows, literacy_headers)
    write_csv(data_dir / 'CENSUS_WORKERS_SUMMARY.csv', workers_summary_rows, workers_summary_headers)
    write_csv(data_dir / 'CENSUS_MAIN_WORKERS_DETAIL.csv', main_workers_rows, main_workers_headers)
    write_csv(data_dir / 'CENSUS_MARGINAL_WORKERS_DETAIL.csv', marginal_workers_rows, marginal_workers_headers)

    # Write sample JSON files to schema directory
    print(f"\nWriting sample JSON files...")
    write_sample_json(output_dir / 'LOCATIONS.json', location_rows, location_headers, sample_size)
    write_sample_json(output_dir / 'CENSUS_POPULATION.json', population_rows, population_headers, sample_size)
    write_sample_json(output_dir / 'CENSUS_CASTE.json', caste_rows, caste_headers, sample_size)
    write_sample_json(output_dir / 'CENSUS_LITERACY.json', literacy_rows, literacy_headers, sample_size)
    write_sample_json(output_dir / 'CENSUS_WORKERS_SUMMARY.json', workers_summary_rows, workers_summary_headers, sample_size)
    write_sample_json(output_dir / 'CENSUS_MAIN_WORKERS_DETAIL.json', main_workers_rows, main_workers_headers, sample_size)
    write_sample_json(output_dir / 'CENSUS_MARGINAL_WORKERS_DETAIL.json', marginal_workers_rows, marginal_workers_headers, sample_size)

    # Write DDL.csv file with table definitions (using generate_ddl module)
    print(f"\nWriting DDL.csv file...")
    generate_ddl_csv(output_dir, descriptions=TABLE_DESCRIPTIONS)

    print(f"\nDone! All files written to {output_dir}")

    return {
        'total_rows': total_rows,
        'skipped_rows': skipped_rows,
        'valid_rows': total_rows - skipped_rows,
        'unique_locations': len(locations)
    }


def main():
    parser = argparse.ArgumentParser(description='Split India Census data into normalized tables')
    parser.add_argument('--input', type=str,
                        default='../../databases/INDIA_POPULATION_CENSUS/INDIA_POPULATION_CENSUS/data/total_data.csv',
                        help='Path to input CSV file')
    parser.add_argument('--output', type=str,
                        default='../../databases/INDIA_POPULATION_CENSUS/INDIA_POPULATION_CENSUS',
                        help='Output directory for split tables')
    parser.add_argument('--sample-size', type=int, default=5,
                        help='Number of sample rows for JSON files')

    args = parser.parse_args()

    input_file = Path(args.input)
    output_dir = Path(args.output)

    if not input_file.exists():
        print(f"Error: Input file not found: {input_file}")
        return

    split_census_data(input_file, output_dir, args.sample_size)


if __name__ == "__main__":
    main()