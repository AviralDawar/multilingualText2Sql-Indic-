# DATABASE PURPOSE
- Stores census data for geographic locations across hierarchical levels (country, state, district, sub-district, locality).
- Captures demographic, housing, social, literacy, and workforce statistics.
- Organizes data by census year and location, linking all metrics to a normalized geography hierarchy.

# TABLE GRANULARITY
- **DIM_GEOGRAPHY_HIERARCHY**: One row per unique geographic entity. Entity table. Primary Key: `HIERARCHY_ID`.
- **FACT_HOUSING**: One row per geographic location per census year. Snapshot table. Primary Key: `HOUSING_ID`.
- **FACT_POPULATION_CORE**: One row per geographic location per census year. Snapshot table. Primary Key: `POPULATION_CORE_ID`.
- **FACT_POPULATION_AGE_COHORT**: One row per geographic location per census year. Snapshot table. Primary Key: `POPULATION_AGE_ID`.
- **FACT_POPULATION_SOCIAL_CATEGORY**: One row per geographic location per census year. Snapshot table. Primary Key: `POPULATION_SOCIAL_ID`.
- **FACT_POPULATION_LITERACY**: One row per geographic location per census year. Snapshot table. Primary Key: `POPULATION_LITERACY_ID`.
- **FACT_WORKFORCE_STATUS**: One row per geographic location per census year. Snapshot table. Primary Key: `WORKFORCE_STATUS_ID`.
- **FACT_WORKFORCE_DETAIL**: One row per geographic location per census year. Snapshot table. Primary Key: `WORKFORCE_DETAIL_ID`.
- **FACT_WORKERS_PRIMARY_SECTOR**: One row per geographic location per census year. Snapshot table. Primary Key: `WORKERS_PRIMARY_ID`.
- **FACT_WORKERS_SECONDARY_SECTOR**: One row per geographic location per census year. Snapshot table. Primary Key: `WORKERS_SECONDARY_ID`.
- **FACT_WORKERS_TERTIARY_SECTOR**: One row per geographic location per census year. Snapshot table. Primary Key: `WORKERS_TERTIARY_ID`.

# RELATIONSHIP STRUCTURE
- **Foreign Key (1:N)**: `DIM_GEOGRAPHY_HIERARCHY.HIERARCHY_ID` ← `FACT_*.HIERARCHY_ID`. One geography record can be associated with many fact records across years and tables.
- **Self-Referencing Foreign Key (1:N Hierarchy)**: `DIM_GEOGRAPHY_HIERARCHY.PARENT_ID` → `DIM_GEOGRAPHY_HIERARCHY.HIERARCHY_ID`. A location (child) has one parent; a parent can have many child locations.
- **Fact-to-Fact Correlation Path**: Any two fact tables can be joined on `(HIERARCHY_ID, CENSUS_YEAR)` to correlate metrics for the same location and year.

# DOMAIN CLASSIFICATIONS & FUNCTIONS
- **`LOCATION_TYPE` (DIM_GEOGRAPHY_HIERARCHY)**: Categorical. Defines the administrative level of the row. Values are not guaranteed but may correspond to hierarchical levels (e.g., 'District', 'Village').
- **Denormalized Geography Columns (`COUNTRY`, `STATE`, `DISTRICT`, `SUB_DISTRICT`)**: Redundant strings for direct filtering and grouping, derivable from the `PARENT_ID` hierarchy.
- **`CENSUS_YEAR` (All Fact Tables)**: Integer representing the year of the census snapshot.
- **All `INT` columns in Fact Tables**: Represent counts. No inherent units beyond "number of".
- **Gender Prefixes (`MALE_`, `FEMALE_`)**: Columns prefixed with these terms segment counts by gender.
- **Sector & Worker Classifications**: Column names define categories (e.g., `CULTIVATORS`, `AGRICULTURAL_LABOURERS`). These represent formal census classifications.
- **Primary Keys**: Each table has a single-column primary key (`*_ID`). These enforce uniqueness at the row level.
- **Foreign Key Nullability**: `DIM_GEOGRAPHY_HIERARCHY.PARENT_ID` is nullable; NULL indicates a root node (e.g., a country). `FACT_*.HIERARCHY_ID` is assumed non-NULL for a valid join.
- **Metric Nullability**: Metric columns (all `INT` columns in fact tables) are assumed nullable. NULL indicates the metric was not collected, reported, or is not applicable for that location/year.

# SET DEFINITIONS
- **Geographic Locations** = { g | g.HIERARCHY_ID ∈ DIM_GEOGRAPHY_HIERARCHY.HIERARCHY_ID }
- **Root Geographic Entities** = { g | g ∈ Geographic Locations AND g.PARENT_ID IS NULL }
- **Leaf Geographic Entities** = { g | g ∈ Geographic Locations AND g.HIERARCHY_ID ∉ (SELECT DISTINCT PARENT_ID FROM DIM_GEOGRAPHY_HIERARCHY WHERE PARENT_ID IS NOT NULL) }
- **Location-Census Year Pairs** = { (h, y) | ∃ a fact table F such that F.HIERARCHY_ID = h AND F.CENSUS_YEAR = y }

# DERIVED METRICS
*(Note: Metrics combining columns from multiple fact tables require joining those tables on `(HIERARCHY_ID, CENSUS_YEAR)`.)*
- **Sex Ratio (Males per 1000 Females)**: (TOTAL_MALE_POPULATION / TOTAL_FEMALE_POPULATION) * 1000. Granularity: per location per year. Denominator safety: exclude rows where TOTAL_FEMALE_POPULATION is 0 or NULL.
- **Child Population (0-6)**: MALE_POPULATION_0_TO_6_YEARS + FEMALE_POPULATION_0_TO_6_YEARS. Granularity: per location per year.
- **Total Scheduled Caste Population**: MALE_SCHEDULED_CASTE_POPULATION + FEMALE_SCHEDULED_CASTE_POPULATION. Granularity: per location per year.
- **Total Scheduled Tribe Population**: MALE_SCHEDULED_TRIBE_POPULATION + FEMALE_SCHEDULED_TRIBE_POPULATION. Granularity: per location per year.
- **Total Illiterate Population**: MALE_ILLITERATE_POPULATION + FEMALE_ILLITERATE_POPULATION. Granularity: per location per year.
- **Literacy Rate**: 1 - (Total Illiterate Population / TOTAL_POPULATION). Granularity: per location per year. Denominator safety: exclude rows where TOTAL_POPULATION is 0 or NULL.
- **Total Working Population**: MALE_WORKING_POPULATION + FEMALE_WORKING_POPULATION. Granularity: per location per year.
- **Workforce Participation Rate**: Total Working Population / TOTAL_POPULATION. Granularity: per location per year. Denominator safety: exclude rows where TOTAL_POPULATION is 0 or NULL.
- **Total Main Workers in Primary Sector**: Sum of all `MALE_MAIN_WORKERS_*` and `FEMALE_MAIN_WORKERS_*` columns in `FACT_WORKERS_PRIMARY_SECTOR`. Granularity: per location per year.
- **Total Main Workers in Secondary Sector**: Sum of all `MALE_MAIN_WORKERS_MANUFACTURING_*`, `FEMALE_MAIN_WORKERS_MANUFACTURING_*`, `MALE_CONSTRUCTION_WORKERS`, and `FEMALE_CONSTRUCTION_WORKERS` columns in `FACT_WORKERS_SECONDARY_SECTOR`. Granularity: per location per year.
- **Total Main Workers in Tertiary Sector**: Sum of all `MALE_WORKERS_*` and `FEMALE_WORKERS_*` columns in `FACT_WORKERS_TERTIARY_SECTOR`. Granularity: per location per year.
- **Total Marginal Workers**: MALE_MARGINAL_WORKERS + FEMALE_MARGINAL_WORKERS. Granularity: per location per year.
- **Total Non-Workers**: MALE_NON_WORKERS + FEMALE_NON_WORKERS. Granularity: per location per year.

# GROUP CONSTRUCTION LOGIC
- **By Location Type**: Group = { g | g ∈ Geographic Locations AND g.LOCATION_TYPE = `value` } for a specific `value` in the `LOCATION_TYPE` column. NULL `LOCATION_TYPE` values form a separate group.
- **By Census Year**: Group = { f | f.HIERARCHY_ID = `h` AND f.CENSUS_YEAR = `y` } for a specific location `h` and year `y` from any fact table `f`.
- **By Denormalized Geography**: Group = { g | g ∈ Geographic Locations AND g.`COL` = `value` } where `COL` is one of `COUNTRY`, `STATE`, `DISTRICT`, or `SUB_DISTRICT`.
- **By Presence of Data**: For a specific metric column `X`, groups can be: { r | r.X IS NOT NULL } and { r | r.X IS NULL }.