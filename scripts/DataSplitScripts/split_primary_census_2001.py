"""
Script to split India Primary Population Census 2001 data into normalized tables.

Input: total_data.csv (single denormalized file with 56 columns)
Output: 9 normalized CSV files + sample JSON files + DDL.csv

Tables (Dimension Tables):
1. DIM_GEOGRAPHY - Consolidated geographic hierarchy (Country, State, District, Sub-District, Location)
2. DIM_RESIDENCE_TYPE - Rural/Urban classification
3. DIM_CENSUS_YEAR - Census year reference

Tables (Fact Tables - Domain Specific):
4. FACT_POPULATION_CORE - Basic demographics (households, population, age 0-6)
5. FACT_SOCIAL_CATEGORY - SC/ST population statistics
6. FACT_LITERACY - Literacy statistics
7. FACT_EMPLOYMENT_SUMMARY - Overall employment (workers, non-workers)
8. FACT_MAIN_WORKERS - Main worker occupation breakdown
9. FACT_MARGINAL_WORKERS - Marginal worker occupation breakdown
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
    'DIM_GEOGRAPHY': 'Consolidated geographic hierarchy dimension containing country state district sub-district and village/town location information',
    'DIM_RESIDENCE_TYPE': 'Residence type dimension classifying locations as Rural or Urban',
    'DIM_CENSUS_YEAR': 'Census year dimension storing year metadata',
    'FACT_POPULATION_CORE': 'Core population demographics including households total population and age 0-6 population by gender',
    'FACT_SOCIAL_CATEGORY': 'Scheduled Caste and Scheduled Tribe population statistics by gender',
    'FACT_LITERACY': 'Literacy statistics including literate and illiterate population counts by gender',
    'FACT_EMPLOYMENT_SUMMARY': 'Employment overview with total working population and non-workers by gender',
    'FACT_MAIN_WORKERS': 'Detailed breakdown of main workers by occupation including cultivators agricultural labourers household industry and other workers',
    'FACT_MARGINAL_WORKERS': 'Detailed breakdown of marginal workers by occupation including cultivators agricultural labourers household industry and other workers'
}


# Column mappings: original column index (0-based) -> new column name
# CSV Structure (63 columns, indices 0-62):
# 0: Country, 1: State, 2: District, 3: Sub-District, 4: Ulb_Rlb_Village
# 5: Year, 6: Residence Type, 7: Location Level
# 8: Households, 9: Population, 10: Male Pop, 11: Female Pop
# 12-14: Population 0-6 (Total, Male, Female)
# 15-17: SC Population (Total, Male, Female)
# 18-20: ST Population (Total, Male, Female)
# 21-23: Literate (Total, Male, Female)
# 24-26: Illiterate (Total, Male, Female)
# 27-29: Working Population (Total, Male, Female)
# 30-32: Main Workers (Total, Male, Female)
# 33-35: Main Workers Cultivators (Total, Male, Female)
# 36-38: Main Workers Agri Labourers (Total, Male, Female)
# 39-41: Main Workers Household Industry (Total, Male, Female)
# 42-44: Main Workers Other (Total, Male, Female)
# 45-47: Marginal Workers (Total, Male, Female)
# 48-50: Marginal Workers Cultivators (Total, Male, Female)
# 51-53: Marginal Workers Agri Labourers (Total, Male, Female)
# 54-56: Marginal Workers Household Industry (Total, Male, Female)
# 57-59: Marginal Workers Other (Total, Male, Female)
# 60-62: Non Workers (Total, Male, Female)

GEOGRAPHY_COLUMNS = {
    0: 'COUNTRY_NAME',
    1: 'STATE_NAME',
    2: 'DISTRICT_NAME',
    3: 'SUB_DISTRICT_NAME',
    4: 'LOCATION_NAME',
    7: 'LOCATION_LEVEL'
}

RESIDENCE_TYPE_COLUMN = 6  # "Residence Type" column
CENSUS_YEAR_COLUMN = 5     # "Year" column

# Fact table column mappings (0-based indices from CSV)
POPULATION_CORE_COLUMNS = {
    8: 'HOUSEHOLDS',
    9: 'TOTAL_POPULATION',
    10: 'MALE_POPULATION',
    11: 'FEMALE_POPULATION',
    12: 'POPULATION_0_TO_6',
    13: 'MALE_POPULATION_0_TO_6',
    14: 'FEMALE_POPULATION_0_TO_6'
}

SOCIAL_CATEGORY_COLUMNS = {
    15: 'SC_POPULATION',
    16: 'SC_MALE_POPULATION',
    17: 'SC_FEMALE_POPULATION',
    18: 'ST_POPULATION',
    19: 'ST_MALE_POPULATION',
    20: 'ST_FEMALE_POPULATION'
}

LITERACY_COLUMNS = {
    21: 'LITERATE_POPULATION',
    22: 'LITERATE_MALE',
    23: 'LITERATE_FEMALE',
    24: 'ILLITERATE_POPULATION',
    25: 'ILLITERATE_MALE',
    26: 'ILLITERATE_FEMALE'
}

# Employment summary: Working population (27-29) + Non-workers (60-62)
EMPLOYMENT_SUMMARY_COLUMNS = {
    27: 'WORKING_POPULATION',
    28: 'WORKING_MALE',
    29: 'WORKING_FEMALE',
    60: 'NON_WORKERS',
    61: 'NON_WORKERS_MALE',
    62: 'NON_WORKERS_FEMALE'
}

# Main workers: Total (30-32) + by occupation (33-44)
MAIN_WORKERS_COLUMNS = {
    30: 'MAIN_WORKERS_TOTAL',
    31: 'MAIN_WORKERS_MALE',
    32: 'MAIN_WORKERS_FEMALE',
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

# Marginal workers: Total (45-47) + by occupation (48-59)
MARGINAL_WORKERS_COLUMNS = {
    45: 'MARGINAL_WORKERS_TOTAL',
    46: 'MARGINAL_WORKERS_MALE',
    47: 'MARGINAL_WORKERS_FEMALE',
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


def generate_geography_key(row: list) -> str:
    """Generate a unique key for a geography based on full hierarchy."""
    parts = [
        row[0].strip() if len(row) > 0 else '',   # Country
        row[1].strip() if len(row) > 1 else '',   # State
        row[2].strip() if len(row) > 2 else '',   # District
        row[3].strip() if len(row) > 3 else '',   # Sub-District
        row[4].strip() if len(row) > 4 else '',   # Location Name
        row[7].strip() if len(row) > 7 else ''    # Location Level
    ]
    return '|'.join(parts)


def clean_year_value(year_str: str) -> str:
    """Extract year from strings like 'Calendar Year (Jan - Dec), 2001'."""
    if '2001' in year_str:
        return '2001'
    # Try to extract any 4-digit year
    import re
    match = re.search(r'\b(19|20)\d{2}\b', year_str)
    if match:
        return match.group(0)
    return year_str.strip()


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


def split_primary_census_data(input_file: Path, output_dir: Path, sample_size: int = 5):
    """
    Split the primary census data into normalized tables.

    Args:
        input_file: Path to total_data.csv
        output_dir: Directory to write output files
        sample_size: Number of sample rows for JSON files
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    data_dir = output_dir / 'data'
    data_dir.mkdir(exist_ok=True)

    # Dimension table data structures
    geographies = OrderedDict()      # key -> (geography_id, geography_data)
    residence_types = OrderedDict()  # name -> residence_type_id
    census_years = OrderedDict()     # year_value -> (census_year_id, year_description)

    # Fact table data
    population_core_rows = []
    social_category_rows = []
    literacy_rows = []
    employment_summary_rows = []
    main_workers_rows = []
    marginal_workers_rows = []

    # Counters
    total_rows = 0
    skipped_rows = 0
    geography_id_counter = 1
    residence_type_id_counter = 1
    census_year_id_counter = 1
    fact_id_counter = 1

    # All column indices we need to validate
    all_columns = set()
    all_columns.update(GEOGRAPHY_COLUMNS.keys())
    all_columns.add(RESIDENCE_TYPE_COLUMN)
    all_columns.add(CENSUS_YEAR_COLUMN)
    all_columns.update(POPULATION_CORE_COLUMNS.keys())
    all_columns.update(SOCIAL_CATEGORY_COLUMNS.keys())
    all_columns.update(LITERACY_COLUMNS.keys())
    all_columns.update(EMPLOYMENT_SUMMARY_COLUMNS.keys())
    all_columns.update(MAIN_WORKERS_COLUMNS.keys())
    all_columns.update(MARGINAL_WORKERS_COLUMNS.keys())

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

            # Process DIM_GEOGRAPHY
            geo_key = generate_geography_key(row)
            if geo_key not in geographies:
                geo_data = extract_columns(row, GEOGRAPHY_COLUMNS)
                geographies[geo_key] = (geography_id_counter, geo_data)
                geography_id_counter += 1

            geography_id = geographies[geo_key][0]

            # Process DIM_RESIDENCE_TYPE
            residence_type_name = row[RESIDENCE_TYPE_COLUMN].strip()
            if residence_type_name not in residence_types:
                residence_types[residence_type_name] = residence_type_id_counter
                residence_type_id_counter += 1

            residence_type_id = residence_types[residence_type_name]

            # Process DIM_CENSUS_YEAR
            year_raw = row[CENSUS_YEAR_COLUMN].strip()
            year_value = clean_year_value(year_raw)
            if year_value not in census_years:
                census_years[year_value] = (census_year_id_counter, year_raw)
                census_year_id_counter += 1

            census_year_id = census_years[year_value][0]

            # Common foreign keys for all fact tables
            fact_id = fact_id_counter
            fact_id_counter += 1

            # Process FACT_POPULATION_CORE
            pop_data = extract_columns(row, POPULATION_CORE_COLUMNS)
            population_core_rows.append(
                [fact_id, geography_id, residence_type_id, census_year_id] + list(pop_data.values())
            )

            # Process FACT_SOCIAL_CATEGORY
            social_data = extract_columns(row, SOCIAL_CATEGORY_COLUMNS)
            social_category_rows.append(
                [fact_id, geography_id, residence_type_id, census_year_id] + list(social_data.values())
            )

            # Process FACT_LITERACY
            literacy_data = extract_columns(row, LITERACY_COLUMNS)
            literacy_rows.append(
                [fact_id, geography_id, residence_type_id, census_year_id] + list(literacy_data.values())
            )

            # Process FACT_EMPLOYMENT_SUMMARY
            employment_data = extract_columns(row, EMPLOYMENT_SUMMARY_COLUMNS)
            employment_summary_rows.append(
                [fact_id, geography_id, residence_type_id, census_year_id] + list(employment_data.values())
            )

            # Process FACT_MAIN_WORKERS
            main_workers_data = extract_columns(row, MAIN_WORKERS_COLUMNS)
            main_workers_rows.append(
                [fact_id, geography_id, residence_type_id, census_year_id] + list(main_workers_data.values())
            )

            # Process FACT_MARGINAL_WORKERS
            marginal_data = extract_columns(row, MARGINAL_WORKERS_COLUMNS)
            marginal_workers_rows.append(
                [fact_id, geography_id, residence_type_id, census_year_id] + list(marginal_data.values())
            )

            if total_rows % 100000 == 0:
                print(f"  Processed {total_rows} rows...")

    print(f"\nProcessing complete!")
    print(f"  Total rows read: {total_rows}")
    print(f"  Rows skipped (missing data): {skipped_rows}")
    print(f"  Valid rows processed: {total_rows - skipped_rows}")
    print(f"  Unique geographies: {len(geographies)}")
    print(f"  Unique residence types: {len(residence_types)}")
    print(f"  Unique census years: {len(census_years)}")

    # Prepare dimension table rows
    geography_rows = []
    for geo_key, (geo_id, geo_data) in geographies.items():
        geography_rows.append([geo_id] + list(geo_data.values()))

    residence_type_rows = []
    for name, rt_id in residence_types.items():
        residence_type_rows.append([rt_id, name])

    census_year_rows = []
    for year_val, (cy_id, year_desc) in census_years.items():
        census_year_rows.append([cy_id, year_val, year_desc])

    # Define headers for each table
    geography_headers = ['GEOGRAPHY_ID'] + list(GEOGRAPHY_COLUMNS.values())
    residence_type_headers = ['RESIDENCE_TYPE_ID', 'RESIDENCE_TYPE_NAME']
    census_year_headers = ['CENSUS_YEAR_ID', 'YEAR_VALUE', 'YEAR_DESCRIPTION']

    fact_key_headers = ['FACT_ID', 'GEOGRAPHY_ID', 'RESIDENCE_TYPE_ID', 'CENSUS_YEAR_ID']
    population_core_headers = fact_key_headers + list(POPULATION_CORE_COLUMNS.values())
    social_category_headers = fact_key_headers + list(SOCIAL_CATEGORY_COLUMNS.values())
    literacy_headers = fact_key_headers + list(LITERACY_COLUMNS.values())
    employment_summary_headers = fact_key_headers + list(EMPLOYMENT_SUMMARY_COLUMNS.values())
    main_workers_headers = fact_key_headers + list(MAIN_WORKERS_COLUMNS.values())
    marginal_workers_headers = fact_key_headers + list(MARGINAL_WORKERS_COLUMNS.values())

    print(f"\nWriting output files to: {output_dir}")

    # Write CSV files to data/ subdirectory
    # Dimension tables
    write_csv(data_dir / 'DIM_GEOGRAPHY.csv', geography_rows, geography_headers)
    write_csv(data_dir / 'DIM_RESIDENCE_TYPE.csv', residence_type_rows, residence_type_headers)
    write_csv(data_dir / 'DIM_CENSUS_YEAR.csv', census_year_rows, census_year_headers)

    # Fact tables
    write_csv(data_dir / 'FACT_POPULATION_CORE.csv', population_core_rows, population_core_headers)
    write_csv(data_dir / 'FACT_SOCIAL_CATEGORY.csv', social_category_rows, social_category_headers)
    write_csv(data_dir / 'FACT_LITERACY.csv', literacy_rows, literacy_headers)
    write_csv(data_dir / 'FACT_EMPLOYMENT_SUMMARY.csv', employment_summary_rows, employment_summary_headers)
    write_csv(data_dir / 'FACT_MAIN_WORKERS.csv', main_workers_rows, main_workers_headers)
    write_csv(data_dir / 'FACT_MARGINAL_WORKERS.csv', marginal_workers_rows, marginal_workers_headers)

    # Write sample JSON files
    print(f"\nWriting sample JSON files...")
    write_sample_json(output_dir / 'DIM_GEOGRAPHY.json', geography_rows, geography_headers, sample_size)
    write_sample_json(output_dir / 'DIM_RESIDENCE_TYPE.json', residence_type_rows, residence_type_headers, sample_size)
    write_sample_json(output_dir / 'DIM_CENSUS_YEAR.json', census_year_rows, census_year_headers, sample_size)
    write_sample_json(output_dir / 'FACT_POPULATION_CORE.json', population_core_rows, population_core_headers, sample_size)
    write_sample_json(output_dir / 'FACT_SOCIAL_CATEGORY.json', social_category_rows, social_category_headers, sample_size)
    write_sample_json(output_dir / 'FACT_LITERACY.json', literacy_rows, literacy_headers, sample_size)
    write_sample_json(output_dir / 'FACT_EMPLOYMENT_SUMMARY.json', employment_summary_rows, employment_summary_headers, sample_size)
    write_sample_json(output_dir / 'FACT_MAIN_WORKERS.json', main_workers_rows, main_workers_headers, sample_size)
    write_sample_json(output_dir / 'FACT_MARGINAL_WORKERS.json', marginal_workers_rows, marginal_workers_headers, sample_size)

    # Write DDL.csv file
    print(f"\nWriting DDL.csv file...")
    generate_ddl_csv(output_dir, descriptions=TABLE_DESCRIPTIONS)

    print(f"\nDone! All files written to {output_dir}")

    return {
        'total_rows': total_rows,
        'skipped_rows': skipped_rows,
        'valid_rows': total_rows - skipped_rows,
        'unique_geographies': len(geographies),
        'unique_residence_types': len(residence_types),
        'unique_census_years': len(census_years)
    }


def main():
    parser = argparse.ArgumentParser(
        description='Split India Primary Population Census 2001 data into normalized tables'
    )
    parser.add_argument(
        '--input', type=str,
        default='../../databases/INDIA_PRIMARY_POPULATION_CENSUS_2001/INDIA_PRIMARY_POPULATION_CENSUS_2001/data/total_data.csv',
        help='Path to input CSV file'
    )
    parser.add_argument(
        '--output', type=str,
        default='../../databases/INDIA_PRIMARY_POPULATION_CENSUS_2001/INDIA_PRIMARY_POPULATION_CENSUS_2001',
        help='Output directory for split tables'
    )
    parser.add_argument(
        '--sample-size', type=int, default=5,
        help='Number of sample rows for JSON files'
    )

    args = parser.parse_args()

    input_file = Path(args.input)
    output_dir = Path(args.output)

    # Handle relative paths from script directory
    if not input_file.is_absolute():
        input_file = Path(__file__).parent / input_file
    if not output_dir.is_absolute():
        output_dir = Path(__file__).parent / output_dir

    if not input_file.exists():
        print(f"Error: Input file not found: {input_file}")
        return

    split_primary_census_data(input_file, output_dir, args.sample_size)


if __name__ == "__main__":
    main()
