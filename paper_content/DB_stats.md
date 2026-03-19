# Basic Scale & Diversity Stats (Created DBs)

Source of truth: all `DDL.csv` files under `databases/*/*/DDL.csv` (20 databases total).

## 1) High-level Scale

| Metric | Value |
|---|---:|
| Total Databases | 20 |
| Total Tables | 237 |
| Total Columns | 1,465 |
| Average Tables per DB | 11.85 |
| Average Columns per Table | 6.18 |

Comparison note:
- Spider 1.0 average tables/DB is ~5.
- BIRD average tables/DB is ~10.
- IndicDB (11.85) is higher than both, indicating **increased schema complexity** via broader multi-table structures.

## 2) Schema Width Signal (for Schema Linking)

| Metric | Value |
|---|---:|
| Max columns in any single table | 24 |
| Tables with 30+ columns | 0 |

Interpretation:
- In this current normalized DB release, complexity is driven more by **number of related tables** than by very wide single tables.

## 3) Domain Coverage

Domain assignment was done by dataset semantics in DB names (Agriculture, Education, Census, etc.).

| Domain | # DBs | % of DBs | # Tables | # Columns |
|---|---:|---:|---:|---:|
| Household & Social Surveys | 6 | 30.0% | 72 | 370 |
| Census & Demography | 4 | 20.0% | 39 | 312 |
| Education | 3 | 15.0% | 39 | 203 |
| Health & Public Health | 3 | 15.0% | 39 | 229 |
| Economy & Employment | 2 | 10.0% | 26 | 200 |
| Agriculture | 1 | 5.0% | 12 | 100 |
| Transport & Safety | 1 | 5.0% | 10 | 51 |

## 4) Sunburst Chart Data (Ready to Plot)

Use this directly in Plotly (`names`, `parents`, `values`) or convert to your plotting format.

```csv
name,parent,value,metric
IndicDB,,20,db_count
Agriculture,IndicDB,1,db_count
INDIA_ICRISAT_District_Level_Agricultural_Data,Agriculture,12,table_count
Census & Demography,IndicDB,4,db_count
INDIA_POPULATION_CENSUS,Census & Demography,7,table_count
INDIA_PRIMARY_POPULATION_CENSUS_1991,Census & Demography,11,table_count
INDIA_PRIMARY_POPULATION_CENSUS_2001,Census & Demography,9,table_count
INDIA_Village_Amenities_Directory_2001,Census & Demography,12,table_count
Economy & Employment,IndicDB,2,db_count
INDIA_EMPLOYMENT_DATA,Economy & Employment,7,table_count
INDIA_Economic_Census_Firms,Economy & Employment,19,table_count
Education,IndicDB,3,db_count
INDIA_SCHOOL_INFRASTRUCTURE,Education,15,table_count
INDIA_UDISE_Right_To_Education_RTE_and_School_Management_data,Education,13,table_count
INDIA_UDISE_SCHOOL_PROFILES,Education,11,table_count
Health & Public Health,IndicDB,3,db_count
INDIA_HMIS_Sub_District_Report_Facility_wise,Health & Public Health,9,table_count
INDIA_HMIS_Sub_District_Report_Rural_Urban,Health & Public Health,18,table_count
INDIA_NWMP_Water_Quality_Data,Health & Public Health,12,table_count
Household & Social Surveys,IndicDB,6,db_count
INDIA_IHDS_2005_HOUSEHOLD_SURVEY,Household & Social Surveys,13,table_count
INDIA_IHDS_2005_INDIVIDUAL_SURVEY,Household & Social Surveys,9,table_count
INDIA_IHDS_2011_ELIGIBLE_WOMAN_SURVEY,Household & Social Surveys,7,table_count
INDIA_IHDS_2011_HOUSEHOLD_SURVEY,Household & Social Surveys,9,table_count
INDIA_IHDS_2011_INDIVIDUAL_SURVEY,Household & Social Surveys,16,table_count
INDIA_IHDS_2011_TRACKING_SURVEY,Household & Social Surveys,18,table_count
Transport & Safety,IndicDB,1,db_count
INDIA_ROAD_ACCIDENTS_DATASET_2001,Transport & Safety,10,table_count
```

## Reproducibility Note

All numbers were computed from table DDL definitions, where column counts exclude table-level constraints (`FOREIGN KEY`, `CONSTRAINT`, `UNIQUE`, etc.) and include declared table columns (including key columns).

## 5) Row Count (Live PostgreSQL)

Computed via live `COUNT(*)` queries on PostgreSQL (`indicdb`) on March 19, 2026.
Expected datasets: 20. Loaded schemas found: 18.
Missing in Postgres at query time: `india_primary_population_census_2001`, `india_school_infrastructure`.

| Metric | Value |
|---|---:|
| Total rows across loaded DB schemas | 7,686,001 |
| Loaded schemas counted | 18 |
| Average rows per loaded schema | 427,000.06 |

Paper-ready line:
- IndicDB currently contains **7.69 million records** across the **18 currently loaded PostgreSQL schemas**.

## 6) Foreign Key Density

FK relationships were counted from live PostgreSQL metadata (`information_schema.table_constraints`, `constraint_type='FOREIGN KEY'`).

| Metric | Value |
|---|---:|
| Total FK relationships | 242 |
| Average FK relationships per loaded schema | 13.44 |
| Average FK relationships per table | 1.126 |

Per-DB FK counts:

| Database | FK count | FK per table |
|---|---:|---:|
| INDIA_EMPLOYMENT_DATA (`india_employment`) | 0 | 0.00 |
| INDIA_Economic_Census_Firms | 42 | 2.21 |
| INDIA_HMIS_Sub_District_Report_Facility_wise | 14 | 1.56 |
| INDIA_HMIS_Sub_District_Report_Rural_Urban | 28 | 1.56 |
| INDIA_ICRISAT_District_Level_Agricultural_Data | 20 | 1.67 |
| INDIA_IHDS_2005_HOUSEHOLD_SURVEY | 0 | 0.00 |
| INDIA_IHDS_2005_INDIVIDUAL_SURVEY | 0 | 0.00 |
| INDIA_IHDS_2011_ELIGIBLE_WOMAN_SURVEY | 0 | 0.00 |
| INDIA_IHDS_2011_HOUSEHOLD_SURVEY | 0 | 0.00 |
| INDIA_IHDS_2011_INDIVIDUAL_SURVEY | 0 | 0.00 |
| INDIA_IHDS_2011_TRACKING_SURVEY | 21 | 1.17 |
| INDIA_NWMP_Water_Quality_Data | 18 | 1.50 |
| INDIA_POPULATION_CENSUS (`india_census_2001`) | 0 | 0.00 |
| INDIA_PRIMARY_POPULATION_CENSUS_1991 | 11 | 1.00 |
| INDIA_PRIMARY_POPULATION_CENSUS_2001 | Not loaded | Not loaded |
| INDIA_ROAD_ACCIDENTS_DATASET_2001 | 9 | 0.90 |
| INDIA_SCHOOL_INFRASTRUCTURE | Not loaded | Not loaded |
| INDIA_UDISE_Right_To_Education_RTE_and_School_Management_data | 19 | 1.46 |
| INDIA_UDISE_SCHOOL_PROFILES | 0 | 0.00 |
| INDIA_Village_Amenities_Directory_2001 | 11 | 0.92 |

Interpretation:
- The dataset is not a collection of isolated tables: many DBs are strongly relational (e.g., up to 42 FK links in a single DB).

## 7) Unique Values (Cardinality for Indic Entity Linking, Live PostgreSQL)

To quantify entity-mapping difficulty, we measured unique values in administrative name/code/id columns (state/district/sub-district/village/block/location/geography variants) directly from loaded PostgreSQL tables.

| Metric | Unique values |
|---|---:|
| Administrative columns scanned | 136 |
| Unique administrative values (global union) | 980,408 |

Examples of highest-cardinality columns:

| Column | Unique values |
|---|---:|
| `india_census_2001.dim_geography.GEOGRAPHY_ID` | 569,193 |
| `india_census_2001.dim_geography.LOCATION_NAME` | 405,908 |
| `india_ihds_2011_tracking_survey.fact_migration_geography.migration_geography_id` | 30,000 |
| `india_economic_census_firms.dim_location.location_id` | 29,960 |

Paper-ready line:
- IndicDB includes very high-cardinality administrative entities (up to 569k distinct geography IDs), creating strong value-grounding demands for multilingual text-to-SQL (e.g., mapping Indic-language mentions to English-encoded database values).
