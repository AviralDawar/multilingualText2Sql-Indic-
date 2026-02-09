"""
Script to split India Capital Expenditure data into normalized tables.

Input: total_data.csv (single denormalized file with 126 columns)
Output: 8 normalized CSV files + sample JSON files + DDL.csv

Tables:
1. DIM_GEOGRAPHY - State/UT geographic dimension with regional classification
2. DIM_TIME - Fiscal year time dimension
3. DIM_ESTIMATE_TYPE - Budget estimate type dimension (BE/RE/Accounts)
4. DIM_EXPENDITURE_CATEGORY - Hierarchical expenditure categories
5. FACT_CAPITAL_EXPENDITURE - Main aggregate capital expenditure
6. FACT_SECTOR_EXPENDITURE - Detailed sector-wise expenditure (EAV pattern)
7. FACT_DEBT_TRANSACTIONS - Debt discharge and loan repayments
8. FACT_FISCAL_BALANCE - Public accounts and fiscal surplus/deficit
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
    'DIM_GEOGRAPHY': 'Geographic dimension table containing state and union territory information with regional classification zones and special category status',
    'DIM_TIME': 'Fiscal year time dimension with decade grouping GST era and COVID period markers',
    'DIM_ESTIMATE_TYPE': 'Budget estimate type dimension containing Budget Estimates Revised Estimates and Accounts classifications',
    'DIM_EXPENDITURE_CATEGORY': 'Hierarchical expenditure category dimension with parent-child relationships for social and economic services',
    'FACT_CAPITAL_EXPENDITURE': 'Main fact table containing aggregate capital expenditure metrics including total disbursements capital outlay and loans by state year and estimate type',
    'FACT_SECTOR_EXPENDITURE': 'Detailed sector-wise expenditure fact table using EAV pattern linking to expenditure categories',
    'FACT_DEBT_TRANSACTIONS': 'Debt discharge and loan repayment fact table tracking internal debt discharge and repayments to central government',
    'FACT_FISCAL_BALANCE': 'Fiscal balance fact table containing public account transactions surplus deficit metrics and cash balance positions'
}


# State code mapping
STATE_CODES = {
    'Andhra Pradesh': 'AP',
    'Arunachal Pradesh': 'AR',
    'Assam': 'AS',
    'Bihar': 'BR',
    'Chhattisgarh': 'CG',
    'Delhi': 'DL',
    'Goa': 'GA',
    'Gujarat': 'GJ',
    'Haryana': 'HR',
    'Himachal Pradesh': 'HP',
    'Jammu and Kashmir': 'JK',
    'Jharkhand': 'JH',
    'Karnataka': 'KA',
    'Kerala': 'KL',
    'Ladakh': 'LA',
    'Madhya Pradesh': 'MP',
    'Maharashtra': 'MH',
    'Manipur': 'MN',
    'Meghalaya': 'ML',
    'Mizoram': 'MZ',
    'Nagaland': 'NL',
    'Odisha': 'OD',
    'Puducherry': 'PY',
    'Punjab': 'PB',
    'Rajasthan': 'RJ',
    'Sikkim': 'SK',
    'Tamil Nadu': 'TN',
    'Telangana': 'TS',
    'Tripura': 'TR',
    'Uttar Pradesh': 'UP',
    'Uttarakhand': 'UK',
    'West Bengal': 'WB',
    'Unknown States Of India': 'XX'
}

# Region mapping
STATE_REGIONS = {
    'AP': ('South', 'Southern Zonal Council', 'State', False, 'Large'),
    'AR': ('Northeast', 'North Eastern Council', 'State', True, 'Small'),
    'AS': ('Northeast', 'North Eastern Council', 'State', True, 'Medium'),
    'BR': ('East', 'Eastern Zonal Council', 'State', True, 'Large'),
    'CG': ('Central', 'Central Zonal Council', 'State', False, 'Medium'),
    'DL': ('North', 'Northern Zonal Council', 'UT', False, 'Medium'),
    'GA': ('West', 'Western Zonal Council', 'State', False, 'Small'),
    'GJ': ('West', 'Western Zonal Council', 'State', False, 'Large'),
    'HR': ('North', 'Northern Zonal Council', 'State', False, 'Medium'),
    'HP': ('North', 'Northern Zonal Council', 'State', True, 'Small'),
    'JK': ('North', 'Northern Zonal Council', 'UT', True, 'Medium'),
    'JH': ('East', 'Eastern Zonal Council', 'State', True, 'Medium'),
    'KA': ('South', 'Southern Zonal Council', 'State', False, 'Large'),
    'KL': ('South', 'Southern Zonal Council', 'State', False, 'Medium'),
    'LA': ('North', 'Northern Zonal Council', 'UT', True, 'Small'),
    'MP': ('Central', 'Central Zonal Council', 'State', False, 'Large'),
    'MH': ('West', 'Western Zonal Council', 'State', False, 'Large'),
    'MN': ('Northeast', 'North Eastern Council', 'State', True, 'Small'),
    'ML': ('Northeast', 'North Eastern Council', 'State', True, 'Small'),
    'MZ': ('Northeast', 'North Eastern Council', 'State', True, 'Small'),
    'NL': ('Northeast', 'North Eastern Council', 'State', True, 'Small'),
    'OD': ('East', 'Eastern Zonal Council', 'State', True, 'Large'),
    'PY': ('South', 'Southern Zonal Council', 'UT', False, 'Small'),
    'PB': ('North', 'Northern Zonal Council', 'State', False, 'Medium'),
    'RJ': ('West', 'Northern Zonal Council', 'State', False, 'Large'),
    'SK': ('Northeast', 'North Eastern Council', 'State', True, 'Small'),
    'TN': ('South', 'Southern Zonal Council', 'State', False, 'Large'),
    'TS': ('South', 'Southern Zonal Council', 'State', False, 'Medium'),
    'TR': ('Northeast', 'North Eastern Council', 'State', True, 'Small'),
    'UP': ('North', 'Central Zonal Council', 'State', False, 'Large'),
    'UK': ('North', 'Central Zonal Council', 'State', True, 'Small'),
    'WB': ('East', 'Eastern Zonal Council', 'State', False, 'Large'),
    'XX': ('Unknown', '', 'Unknown', False, '')
}

# Estimate type mapping
ESTIMATE_TYPE_MAP = {
    'Budget Estimates': ('BE', False, 'Initial budget allocations presented before the fiscal year'),
    'Revised Estimates': ('RE', False, 'Mid-year revised estimates based on actual performance'),
    'Accounts': ('ACT', True, 'Actual expenditure recorded in audited accounts')
}

# Five year plan mapping by fiscal year start
FIVE_YEAR_PLANS = {
    2002: '10th Plan', 2003: '10th Plan', 2004: '10th Plan', 2005: '10th Plan', 2006: '10th Plan',
    2007: '11th Plan', 2008: '11th Plan', 2009: '11th Plan', 2010: '11th Plan', 2011: '11th Plan',
    2012: '12th Plan', 2013: '12th Plan', 2014: '12th Plan', 2015: '12th Plan', 2016: '12th Plan',
    2017: 'NITI Aayog', 2018: 'NITI Aayog', 2019: 'NITI Aayog', 2020: 'NITI Aayog',
    2021: 'NITI Aayog', 2022: 'NITI Aayog', 2023: 'NITI Aayog', 2024: 'NITI Aayog'
}


# Expenditure category hierarchy (category_code, name, parent_code, level, type, is_dev, service_type, csv_col)
EXPENDITURE_CATEGORIES = [
    # Level 1: Major categories
    ('CAP_OUT', 'Total Capital Outlay', None, 1, 'Capital Outlay', None, None, 7),
    ('DEV', 'Development Under Capital Outlay', None, 1, 'Capital Outlay', True, None, 8),
    ('NON_DEV', 'Non-Development General Services', None, 1, 'Capital Outlay', False, 'General', 50),

    # Level 2: Social and Economic Services
    ('SOC_SVC', 'Social Services Under Development', 'DEV', 2, 'Capital Outlay', True, 'Social', 9),
    ('ECO_SVC', 'Economic Services Under Development', 'DEV', 2, 'Capital Outlay', True, 'Economic', 19),

    # Level 3: Social Services breakdown
    ('EDU_SPORTS', 'Education Sports Art And Culture', 'SOC_SVC', 3, 'Capital Outlay', True, 'Social', 10),
    ('MED_HEALTH', 'Medical And Public Health', 'SOC_SVC', 3, 'Capital Outlay', True, 'Social', 11),
    ('FAMILY_WELF', 'Family Welfare', 'SOC_SVC', 3, 'Capital Outlay', True, 'Social', 12),
    ('WATER_SANIT', 'Water Supply And Sanitation', 'SOC_SVC', 3, 'Capital Outlay', True, 'Social', 13),
    ('HOUSING', 'Housing', 'SOC_SVC', 3, 'Capital Outlay', True, 'Social', 14),
    ('URBAN_DEV', 'Urban Development', 'SOC_SVC', 3, 'Capital Outlay', True, 'Social', 15),
    ('SC_ST_OBC', 'Welfare Of SC ST And OBC', 'SOC_SVC', 3, 'Capital Outlay', True, 'Social', 16),
    ('SOC_SEC_WELF', 'Social Security And Welfare', 'SOC_SVC', 3, 'Capital Outlay', True, 'Social', 17),
    ('OTH_SOC_SVC', 'Other Social Services', 'SOC_SVC', 3, 'Capital Outlay', True, 'Social', 18),

    # Level 3: Economic Services breakdown
    ('AGRI_ALLIED', 'Agriculture And Allied Activities', 'ECO_SVC', 3, 'Capital Outlay', True, 'Economic', 20),
    ('RURAL_DEV', 'Rural Development', 'ECO_SVC', 3, 'Capital Outlay', True, 'Economic', 32),
    ('SPECIAL_AREA', 'Special Area Programmes', 'ECO_SVC', 3, 'Capital Outlay', True, 'Economic', 33),
    ('IRRIGATION', 'Major And Medium Irrigation And Flood Control', 'ECO_SVC', 3, 'Capital Outlay', True, 'Economic', 35),
    ('ENERGY', 'Energy', 'ECO_SVC', 3, 'Capital Outlay', True, 'Economic', 36),
    ('IND_MINERALS', 'Industry And Minerals', 'ECO_SVC', 3, 'Capital Outlay', True, 'Economic', 37),
    ('TRANSPORT', 'Transport', 'ECO_SVC', 3, 'Capital Outlay', True, 'Economic', 42),
    ('COMMUNICATIONS', 'Communications', 'ECO_SVC', 3, 'Capital Outlay', True, 'Economic', 45),
    ('SCI_TECH', 'Science Technology And Environment', 'ECO_SVC', 3, 'Capital Outlay', True, 'Economic', 46),
    ('GEN_ECO_SVC', 'General Economic Services', 'ECO_SVC', 3, 'Capital Outlay', True, 'Economic', 47),

    # Level 4: Agriculture sub-categories
    ('CROP_HUSB', 'Crop Husbandry', 'AGRI_ALLIED', 4, 'Capital Outlay', True, 'Economic', 21),
    ('SOIL_WATER', 'Soil And Water Conservation', 'AGRI_ALLIED', 4, 'Capital Outlay', True, 'Economic', 22),
    ('ANIMAL_HUSB', 'Animal Husbandry', 'AGRI_ALLIED', 4, 'Capital Outlay', True, 'Economic', 23),
    ('DAIRY_DEV', 'Dairy Development', 'AGRI_ALLIED', 4, 'Capital Outlay', True, 'Economic', 24),
    ('FISHERIES', 'Fisheries', 'AGRI_ALLIED', 4, 'Capital Outlay', True, 'Economic', 25),
    ('FORESTRY', 'Forestry And Wild Life', 'AGRI_ALLIED', 4, 'Capital Outlay', True, 'Economic', 26),
    ('PLANTATIONS', 'Plantations', 'AGRI_ALLIED', 4, 'Capital Outlay', True, 'Economic', 27),
    ('FOOD_STORAGE', 'Food Storage And Warehousing', 'AGRI_ALLIED', 4, 'Capital Outlay', True, 'Economic', 28),
    ('AGRI_RES_EDU', 'Agricultural Research And Education', 'AGRI_ALLIED', 4, 'Capital Outlay', True, 'Economic', 29),
    ('COOPERATION', 'Co-Operation', 'AGRI_ALLIED', 4, 'Capital Outlay', True, 'Economic', 30),
    ('OTH_AGRI', 'Others Under Agriculture', 'AGRI_ALLIED', 4, 'Capital Outlay', True, 'Economic', 31),

    # Level 4: Special Area sub-categories
    ('HILL_AREAS', 'Hill Areas Under Special Area Programmes', 'SPECIAL_AREA', 4, 'Capital Outlay', True, 'Economic', 34),

    # Level 4: Industry sub-categories
    ('VILLAGE_IND', 'Village And Small Industries', 'IND_MINERALS', 4, 'Capital Outlay', True, 'Economic', 38),
    ('IRON_STEEL', 'Iron And Steel Industries', 'IND_MINERALS', 4, 'Capital Outlay', True, 'Economic', 39),
    ('NON_FERROUS', 'Non-Ferrous Mining And Metallurgical', 'IND_MINERALS', 4, 'Capital Outlay', True, 'Economic', 40),
    ('OTH_IND', 'Others Industries And Minerals', 'IND_MINERALS', 4, 'Capital Outlay', True, 'Economic', 41),

    # Level 4: Transport sub-categories
    ('ROADS_BRIDGES', 'Roads And Bridges', 'TRANSPORT', 4, 'Capital Outlay', True, 'Economic', 43),
    ('OTH_TRANSPORT', 'Other Transport Services', 'TRANSPORT', 4, 'Capital Outlay', True, 'Economic', 44),

    # Level 4: General Economic sub-categories
    ('TOURISM', 'Tourism', 'GEN_ECO_SVC', 4, 'Capital Outlay', True, 'Economic', 48),
    ('OTH_GEN_ECO', 'Other General Economic Services', 'GEN_ECO_SVC', 4, 'Capital Outlay', True, 'Economic', 49),
]


# Column mappings for fact tables (0-indexed)
CAPITAL_EXP_COLUMNS = {
    5: 'TOTAL_CAPITAL_DISBURSEMENTS',           # Col 5
    6: 'CAPITAL_DISBURSEMENTS_EXCL_PUBLIC_ACCTS',  # Col 6
    7: 'TOTAL_CAPITAL_OUTLAY',                   # Col 7
    8: 'DEVELOPMENT_CAPITAL_OUTLAY',             # Col 8
    9: 'SOCIAL_SERVICES_DEVELOPMENT',            # Col 9
    19: 'ECONOMIC_SERVICES_DEVELOPMENT',         # Col 19
    50: 'NON_DEVELOPMENT_SERVICES',              # Col 50
    72: 'LOANS_ADVANCES_BY_STATE',               # Col 72
    73: 'DEVELOPMENT_PURPOSE_LOANS',             # Col 73
    93: 'NON_DEVELOPMENT_PURPOSE_LOANS',         # Col 93
    96: 'INTER_STATE_SETTLEMENT',                # Col 96
}

DEBT_COLUMNS = {
    51: 'TOTAL_INTERNAL_DEBT_DISCHARGE',
    52: 'MARKET_LOANS_DISCHARGE',
    53: 'LIC_LOANS_DISCHARGE',
    54: 'SBI_BANK_LOANS_DISCHARGE',
    55: 'NABARD_LOANS_DISCHARGE',
    56: 'NCDC_LOANS_DISCHARGE',
    57: 'WMA_RBI_DISCHARGE',
    58: 'NSSF_SECURITIES_DISCHARGE',
    59: 'OTHER_INTERNAL_DEBT_DISCHARGE',
    60: 'LAND_COMPENSATION_BONDS',
    61: 'TOTAL_REPAYMENT_TO_CENTRE',
    62: 'STATE_PLAN_SCHEMES_REPAYMENT',
    63: 'CALAMITY_ADVANCE_REPAYMENT',
    64: 'CENTRAL_PLAN_REPAYMENT',
    65: 'CENTRALLY_SPONSORED_REPAYMENT',
    66: 'NON_PLAN_REPAYMENT',
    67: 'CALAMITY_RELIEF_REPAYMENT',
    68: 'OTHER_NON_PLAN_REPAYMENT',
    69: 'WMA_CENTRE_REPAYMENT',
    70: 'SPECIAL_SCHEMES_REPAYMENT',
    71: 'OTHER_CENTRE_REPAYMENT',
}

FISCAL_BALANCE_COLUMNS = {
    97: 'CONTINGENCY_FUND',
    98: 'STATE_PROVIDENT_FUNDS_TOTAL',
    99: 'STATE_PROVIDENT_FUNDS',
    100: 'OTHER_PROVIDENT_FUNDS',
    101: 'RESERVE_FUNDS_TOTAL',
    102: 'DEPRECIATION_RESERVE',
    103: 'SINKING_FUNDS',
    104: 'FAMINE_RELIEF_FUND',
    105: 'OTHER_RESERVE_FUNDS',
    106: 'DEPOSITS_ADVANCES_TOTAL',
    107: 'CIVIL_DEPOSITS',
    108: 'LOCAL_FUNDS_DEPOSITS',
    109: 'CIVIL_ADVANCES',
    110: 'OTHER_DEPOSITS_ADVANCES',
    111: 'SUSPENSE_MISC_TOTAL',
    112: 'SUSPENSE',
    113: 'CASH_BALANCE_INVESTMENT_ACCT',
    114: 'DEPOSITS_WITH_RBI',
    115: 'OTHER_SUSPENSE_MISC',
    116: 'APPROPRIATION_CONTINGENCY',
    117: 'REMITTANCES',
    118: 'REVENUE_SURPLUS_DEFICIT',
    119: 'CAPITAL_SURPLUS_DEFICIT',
    120: 'OVERALL_SURPLUS_DEFICIT',
    121: 'FINANCING_SURPLUS_DEFICIT',
    122: 'CASH_BALANCE_CHANGE',
    123: 'OPENING_CASH_BALANCE',
    124: 'CLOSING_CASH_BALANCE',
    125: 'CASH_INVESTMENT_NET_CHANGE',
    126: 'WMA_OVERDRAFT_NET_CHANGE',
}


def is_valid_value(value: str) -> bool:
    """Check if a value is valid (not empty, not null, not NA)."""
    if value is None:
        return False
    value = str(value).strip()
    if value == '' or value.lower() in ('null', 'na', 'n/a', 'none', '-'):
        return False
    return True


def clean_numeric(value: str, max_decimals: int = 2) -> str:
    """Clean numeric value, return empty string if invalid.

    Args:
        value: The value to clean
        max_decimals: Maximum decimal places to keep (default 2)
    """
    if not is_valid_value(value):
        return ''
    try:
        # Remove any commas and whitespace
        cleaned = str(value).strip().replace(',', '')
        # Validate it's a number
        num = float(cleaned)

        # Round to max_decimals places to avoid very long decimals
        # (e.g., J&K/Ladakh data has 20+ decimal places)
        rounded = round(num, max_decimals)

        # Format: remove unnecessary trailing zeros and decimal point
        if rounded == int(rounded):
            return str(int(rounded))
        else:
            return f'{rounded:.{max_decimals}f}'.rstrip('0').rstrip('.')
    except (ValueError, TypeError):
        return ''


def extract_year_from_string(year_str: str) -> int:
    """Extract fiscal year start from string like 'Financial Year (Apr - Mar), 2024'."""
    match = re.search(r'(\d{4})', year_str)
    if match:
        return int(match.group(1))
    return 0


def extract_columns(row: list, column_mapping: dict, clean_numbers: bool = True) -> OrderedDict:
    """Extract and rename columns from a row based on mapping."""
    result = OrderedDict()
    for idx, new_name in column_mapping.items():
        if idx < len(row):
            value = row[idx].strip() if row[idx] else ''
            if clean_numbers:
                value = clean_numeric(value) if value else ''
            result[new_name] = value
        else:
            result[new_name] = ''
    return result


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



def split_capital_expenditure_data(input_file: Path, output_dir: Path, sample_size: int = 5):
    """
    Split the capital expenditure data into normalized tables.

    Args:
        input_file: Path to total_data.csv
        output_dir: Directory to write output files
        sample_size: Number of sample rows for JSON files
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    data_dir = output_dir / 'data'
    data_dir.mkdir(exist_ok=True)

    # Data structures for dimension tables
    geographies = OrderedDict()  # state_name -> (geography_id, row_data)
    time_periods = OrderedDict()  # fiscal_year_start -> (time_id, row_data)
    estimate_types = OrderedDict()  # estimate_type_name -> (estimate_type_id, row_data)
    categories = OrderedDict()  # category_code -> (category_id, row_data)

    # Data structures for fact tables
    capital_exp_rows = []
    sector_exp_rows = []
    debt_txn_rows = []
    fiscal_balance_rows = []

    # ID counters
    geography_id_counter = 1
    time_id_counter = 1
    estimate_type_id_counter = 1
    category_id_counter = 1
    expenditure_id_counter = 1
    sector_exp_id_counter = 1
    debt_txn_id_counter = 1
    fiscal_balance_id_counter = 1

    # Pre-populate expenditure categories dimension
    category_code_to_id = {}
    for cat_tuple in EXPENDITURE_CATEGORIES:
        code, name, parent_code, level, cat_type, is_dev, svc_type, csv_col = cat_tuple
        parent_id = category_code_to_id.get(parent_code, '') if parent_code else ''
        is_dev_str = '1' if is_dev else ('0' if is_dev is not None else '')
        row_data = [category_id_counter, code, name, parent_id, level, cat_type, is_dev_str, svc_type or '', csv_col]
        categories[code] = (category_id_counter, row_data)
        category_code_to_id[code] = category_id_counter
        category_id_counter += 1

    # Statistics
    total_rows = 0
    skipped_rows = 0

    print(f"Reading: {input_file}")

    with open(input_file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        headers = next(reader)  # Skip header

        for row in reader:
            total_rows += 1

            # Skip rows with missing key columns
            if len(row) < 5:
                skipped_rows += 1
                continue

            state_name = row[1].strip() if len(row) > 1 else ''
            year_str = row[2].strip() if len(row) > 2 else ''
            estimate_type_name = row[3].strip() if len(row) > 3 else ''

            # Skip header-like rows or invalid rows
            if state_name == 'State' or not state_name or not year_str:
                skipped_rows += 1
                continue

            # Process Geography dimension
            if state_name not in geographies:
                state_code = STATE_CODES.get(state_name, 'XX')
                region_info = STATE_REGIONS.get(state_code, ('Unknown', '', 'Unknown', False, ''))
                region, zone, state_type, is_special, pop_tier = region_info
                is_special_str = '1' if is_special else '0'
                geo_row = [geography_id_counter, 'IND', 'India', state_code, state_name,
                           region, zone, state_type, is_special_str, pop_tier]
                geographies[state_name] = (geography_id_counter, geo_row)
                geography_id_counter += 1
            geography_id = geographies[state_name][0]

            # Process Time dimension
            fiscal_year_start = extract_year_from_string(year_str)
            if fiscal_year_start not in time_periods:
                fiscal_year_end = fiscal_year_start + 1
                fiscal_year_label = f'FY{fiscal_year_start}-{str(fiscal_year_end)[-2:]}'
                decade = f'{(fiscal_year_start // 10) * 10}s'
                is_pre_gst = '1' if fiscal_year_start < 2017 else '0'
                is_covid = '1' if fiscal_year_start in (2020, 2021) else '0'
                five_year_plan = FIVE_YEAR_PLANS.get(fiscal_year_start, 'NITI Aayog')
                time_row = [time_id_counter, year_str, fiscal_year_start, fiscal_year_end,
                            fiscal_year_label, decade, is_pre_gst, is_covid, five_year_plan]
                time_periods[fiscal_year_start] = (time_id_counter, time_row)
                time_id_counter += 1
            time_id = time_periods[fiscal_year_start][0]

            # Process Estimate Type dimension
            if estimate_type_name not in estimate_types:
                est_info = ESTIMATE_TYPE_MAP.get(estimate_type_name, ('UNK', False, ''))
                est_code, is_actual, description = est_info
                is_actual_str = '1' if is_actual else '0'
                est_row = [estimate_type_id_counter, est_code, estimate_type_name, is_actual_str, description]
                estimate_types[estimate_type_name] = (estimate_type_id_counter, est_row)
                estimate_type_id_counter += 1
            estimate_type_id = estimate_types[estimate_type_name][0]

            # Process Fact: Capital Expenditure (aggregate metrics)
            cap_exp_data = extract_columns(row, CAPITAL_EXP_COLUMNS)
            cap_exp_row = [expenditure_id_counter, geography_id, time_id, estimate_type_id] + list(cap_exp_data.values())
            capital_exp_rows.append(cap_exp_row)

            # Process Fact: Sector Expenditure (EAV pattern for sector details)
            for cat_tuple in EXPENDITURE_CATEGORIES:
                code, name, parent_code, level, cat_type, is_dev, svc_type, csv_col = cat_tuple
                if csv_col is not None and csv_col < len(row):
                    amount = clean_numeric(row[csv_col])
                    if amount:  # Only add non-empty values
                        cat_id = category_code_to_id[code]
                        sector_row = [sector_exp_id_counter, geography_id, time_id, estimate_type_id, cat_id, amount]
                        sector_exp_rows.append(sector_row)
                        sector_exp_id_counter += 1

            # Process Fact: Debt Transactions
            debt_data = extract_columns(row, DEBT_COLUMNS)
            # Only add if at least some debt data exists
            if any(v for v in debt_data.values()):
                debt_row = [debt_txn_id_counter, geography_id, time_id, estimate_type_id] + list(debt_data.values())
                debt_txn_rows.append(debt_row)
                debt_txn_id_counter += 1

            # Process Fact: Fiscal Balance
            fiscal_data = extract_columns(row, FISCAL_BALANCE_COLUMNS)
            # Only add if at least some fiscal data exists
            if any(v for v in fiscal_data.values()):
                fiscal_row = [fiscal_balance_id_counter, geography_id, time_id, estimate_type_id] + list(fiscal_data.values())
                fiscal_balance_rows.append(fiscal_row)
                fiscal_balance_id_counter += 1

            expenditure_id_counter += 1

            if total_rows % 500 == 0:
                print(f"  Processed {total_rows} rows...")

    print(f"\nProcessing complete!")
    print(f"  Total rows read: {total_rows}")
    print(f"  Rows skipped: {skipped_rows}")
    print(f"  Valid rows processed: {total_rows - skipped_rows}")
    print(f"  Unique geographies: {len(geographies)}")
    print(f"  Unique time periods: {len(time_periods)}")
    print(f"  Unique estimate types: {len(estimate_types)}")
    print(f"  Expenditure categories: {len(categories)}")

    # Prepare dimension rows
    geography_rows = [row_data for _, row_data in geographies.values()]
    time_rows = [row_data for _, row_data in time_periods.values()]
    estimate_type_rows = [row_data for _, row_data in estimate_types.values()]
    category_rows = [row_data for _, row_data in categories.values()]

    # Define headers
    geography_headers = ['GEOGRAPHY_ID', 'COUNTRY_CODE', 'COUNTRY_NAME', 'STATE_CODE', 'STATE_NAME',
                         'REGION', 'ZONE', 'STATE_TYPE', 'IS_SPECIAL_CATEGORY', 'POPULATION_TIER']
    time_headers = ['TIME_ID', 'FINANCIAL_YEAR', 'FISCAL_YEAR_START', 'FISCAL_YEAR_END',
                    'FISCAL_YEAR_LABEL', 'DECADE', 'IS_PRE_GST', 'IS_COVID_PERIOD', 'FIVE_YEAR_PLAN']
    estimate_type_headers = ['ESTIMATE_TYPE_ID', 'ESTIMATE_TYPE_CODE', 'ESTIMATE_TYPE_NAME',
                             'IS_ACTUAL', 'DESCRIPTION']
    category_headers = ['CATEGORY_ID', 'CATEGORY_CODE', 'CATEGORY_NAME', 'PARENT_CATEGORY_ID',
                        'CATEGORY_LEVEL', 'CATEGORY_TYPE', 'IS_DEVELOPMENT', 'SERVICE_TYPE', 'CSV_COLUMN_INDEX']

    capital_exp_headers = ['EXPENDITURE_ID', 'GEOGRAPHY_ID', 'TIME_ID', 'ESTIMATE_TYPE_ID'] + \
                          list(CAPITAL_EXP_COLUMNS.values())
    sector_exp_headers = ['SECTOR_EXP_ID', 'GEOGRAPHY_ID', 'TIME_ID', 'ESTIMATE_TYPE_ID',
                          'CATEGORY_ID', 'AMOUNT_LAKHS']
    debt_headers = ['DEBT_TXN_ID', 'GEOGRAPHY_ID', 'TIME_ID', 'ESTIMATE_TYPE_ID'] + \
                   list(DEBT_COLUMNS.values())
    fiscal_headers = ['BALANCE_ID', 'GEOGRAPHY_ID', 'TIME_ID', 'ESTIMATE_TYPE_ID'] + \
                     list(FISCAL_BALANCE_COLUMNS.values())

    print(f"\nWriting output files to: {output_dir}")

    # Write CSV files to data/ subdirectory
    write_csv(data_dir / 'DIM_GEOGRAPHY.csv', geography_rows, geography_headers)
    write_csv(data_dir / 'DIM_TIME.csv', time_rows, time_headers)
    write_csv(data_dir / 'DIM_ESTIMATE_TYPE.csv', estimate_type_rows, estimate_type_headers)
    write_csv(data_dir / 'DIM_EXPENDITURE_CATEGORY.csv', category_rows, category_headers)
    write_csv(data_dir / 'FACT_CAPITAL_EXPENDITURE.csv', capital_exp_rows, capital_exp_headers)
    write_csv(data_dir / 'FACT_SECTOR_EXPENDITURE.csv', sector_exp_rows, sector_exp_headers)
    write_csv(data_dir / 'FACT_DEBT_TRANSACTIONS.csv', debt_txn_rows, debt_headers)
    write_csv(data_dir / 'FACT_FISCAL_BALANCE.csv', fiscal_balance_rows, fiscal_headers)

    # Write sample JSON files to schema directory
    print(f"\nWriting sample JSON files...")
    write_sample_json(output_dir / 'DIM_GEOGRAPHY.json', geography_rows, geography_headers, sample_size)
    write_sample_json(output_dir / 'DIM_TIME.json', time_rows, time_headers, sample_size)
    write_sample_json(output_dir / 'DIM_ESTIMATE_TYPE.json', estimate_type_rows, estimate_type_headers, sample_size)
    write_sample_json(output_dir / 'DIM_EXPENDITURE_CATEGORY.json', category_rows, category_headers, sample_size)
    write_sample_json(output_dir / 'FACT_CAPITAL_EXPENDITURE.json', capital_exp_rows, capital_exp_headers, sample_size)
    write_sample_json(output_dir / 'FACT_SECTOR_EXPENDITURE.json', sector_exp_rows, sector_exp_headers, sample_size)
    write_sample_json(output_dir / 'FACT_DEBT_TRANSACTIONS.json', debt_txn_rows, debt_headers, sample_size)
    write_sample_json(output_dir / 'FACT_FISCAL_BALANCE.json', fiscal_balance_rows, fiscal_headers, sample_size)

    # Write DDL.csv file with table definitions
    print(f"\nWriting DDL.csv file...")
    generate_ddl_csv(output_dir, descriptions=TABLE_DESCRIPTIONS)

    print(f"\nDone! All files written to {output_dir}")

    return {
        'total_rows': total_rows,
        'skipped_rows': skipped_rows,
        'valid_rows': total_rows - skipped_rows,
        'unique_geographies': len(geographies),
        'unique_time_periods': len(time_periods),
        'unique_estimate_types': len(estimate_types),
        'expenditure_categories': len(categories),
        'capital_exp_records': len(capital_exp_rows),
        'sector_exp_records': len(sector_exp_rows),
        'debt_txn_records': len(debt_txn_rows),
        'fiscal_balance_records': len(fiscal_balance_rows)
    }


def main():
    parser = argparse.ArgumentParser(description='Split India Capital Expenditure data into normalized tables')
    parser.add_argument('--input', type=str,
                        default='../../databases/INDIA_CAPITAL_EXPENDITURE/INDIA_CAPITAL_EXPENDITURE/data/total_data.csv',
                        help='Path to input CSV file')
    parser.add_argument('--output', type=str,
                        default='../../databases/INDIA_CAPITAL_EXPENDITURE/INDIA_CAPITAL_EXPENDITURE',
                        help='Output directory for split tables')
    parser.add_argument('--sample-size', type=int, default=5,
                        help='Number of sample rows for JSON files')

    args = parser.parse_args()

    input_file = Path(args.input)
    output_dir = Path(args.output)

    # Handle relative paths
    if not input_file.is_absolute():
        input_file = Path(__file__).parent / input_file
    if not output_dir.is_absolute():
        output_dir = Path(__file__).parent / output_dir

    if not input_file.exists():
        print(f"Error: Input file not found: {input_file}")
        return

    stats = split_capital_expenditure_data(input_file, output_dir, args.sample_size)

    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    for key, value in stats.items():
        print(f"  {key}: {value}")


if __name__ == "__main__":
    main()
