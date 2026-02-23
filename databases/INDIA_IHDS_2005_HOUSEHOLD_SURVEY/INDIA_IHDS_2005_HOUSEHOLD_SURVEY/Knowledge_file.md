# DATABASE OVERVIEW
- Database Name: household_survey_data_warehouse
- SQL Dialect: PostgreSQL
- Description: A dimensional data warehouse modeling socio-economic and consumption data from household surveys. It captures household composition, demographics, education, health, livelihood, infrastructure, and detailed consumption expenditure across food, non-food, and utility categories. The schema is organized around the household as the central entity, with conformed dimensions for geography, social attributes, and time allocation.

==================================================
# TABLES

## TABLE: DIM_HOUSEHOLD
Type: DIMENSION
Primary Key: HOUSEHOLD_ID

Description:
Core entity table representing a household unit. Contains basic household composition identifiers and counts of workers, including those involved in animal care. Links to a farm decision maker.

Columns:
- HOUSEHOLD_ID | BIGINT | IDENTIFIER | Surrogate primary key for the household
- HOUSEHOLD_PERSON_CASE_ID | VARCHAR(50) | IDENTIFIER | Original household identifier from source data
- HOUSEHOLD_NUMBER_OF_WORKERS | INT | MEASURE | Count of workers in the household
- HOUSEHOLD_NUMBER_OF_ANIMAL_CARE_WORKERS | INT | MEASURE | Count of workers primarily engaged in animal care
- FARM_DECISION_MAKER_ID | VARCHAR(50) | ATTRIBUTE | Identifier for the primary agricultural decision-maker in the household

Foreign Keys:
- NONE

Natural Language Synonyms:
- Household master
- Family unit record
- Primary household data
- Household core details
- Household roster

--------------------------------------------------

## TABLE: DIM_GEOGRAPHY_HIERARCHY
Type: DIMENSION
Primary Key: GEOGRAPHY_ID

Description:
Pure geographic hierarchy table containing administrative and census-based location codes. Does not include infrastructure details.

Columns:
- GEOGRAPHY_ID | BIGINT | IDENTIFIER | Surrogate primary key for the geographic location
- STATE | VARCHAR(100) | ATTRIBUTE | Name or code of the state
- DISTRICT_CODE | VARCHAR(50) | ATTRIBUTE | Administrative district code
- PSU_VILLAGE_OR_NEIGHBORHOOD_CODE | VARCHAR(50) | ATTRIBUTE | Primary Sampling Unit (PSU) code, which could be a village or neighborhood identifier
- CENSUS_2001_URBAN_RURAL | VARCHAR(20) | CATEGORY | Urban/Rural classification based on the 2001 census

Foreign Keys:
- NONE

Natural Language Synonyms:
- Location hierarchy
- Administrative geography
- Survey geography
- Region master
- Geographic codes

--------------------------------------------------

## TABLE: DIM_VILLAGE_INFRASTRUCTURE
Type: DIMENSION
Primary Key: INFRASTRUCTURE_ID

Description:
Describes village-level infrastructure and service availability, linked to a geographic location. Captures the presence of schools, health facilities, and drinking water sources.

Columns:
- INFRASTRUCTURE_ID | BIGINT | IDENTIFIER | Surrogate primary key for the infrastructure record
- GEOGRAPHY_ID | BIGINT | IDENTIFIER | Foreign key to the geographic location (DIM_GEOGRAPHY_HIERARCHY)
- VILLAGE_SCHOOL_AVAILABLE_INDICATOR | BOOLEAN | CATEGORY | Flag indicating if a school is available in the village
- VILLAGE_HEALTH_FACILITY_AVAILABLE_INDICATOR | BOOLEAN | CATEGORY | Flag indicating if a health facility is available in the village
- VILLAGE_DRINKING_WATER_SOURCE_TYPE | VARCHAR(50) | CATEGORY | Type of primary drinking water source (e.g., tap, well, handpump)
- VILLAGE_DRINKING_WATER_AVAILABLE_INDICATOR | BOOLEAN | CATEGORY | Flag indicating if drinking water is generally available

Foreign Keys:
- GEOGRAPHY_ID → DIM_GEOGRAPHY_HIERARCHY.GEOGRAPHY_ID

Natural Language Synonyms:
- Village amenities
- Local infrastructure
- Public service availability
- Village facilities
- Service access points

--------------------------------------------------

## TABLE: DIM_HOUSEHOLD_SOCIAL
Type: DIMENSION
Primary Key: SOCIAL_ID

Description:
Captures social, cultural, and wealth attributes of a household, including caste/religion classification, social network type, and an asset-based wealth index.

Columns:
- SOCIAL_ID | BIGINT | IDENTIFIER | Surrogate primary key for the social record
- HOUSEHOLD_ID | BIGINT | IDENTIFIER | Foreign key to the household (DIM_HOUSEHOLD)
- CASTE_RELIGION_GROUP_8_CATEGORY | VARCHAR(50) | CATEGORY | Social classification into one of eight caste/religion groups
- SOCIAL_NETWORK_CONTACT_2_TYPE | VARCHAR(50) | CATEGORY | Type of a key social network contact
- HOUSEHOLD_ASSETS_INDEX | DOUBLE PRECISION | MEASURE | Composite index representing household wealth based on asset ownership

Foreign Keys:
- HOUSEHOLD_ID → DIM_HOUSEHOLD.HOUSEHOLD_ID

Natural Language Synonyms:
- Social profile
- Household demographics
- Cultural attributes
- Wealth and social group
- Socio-cultural dimension

--------------------------------------------------

## TABLE: DIM_HOUSEHOLD_MARITAL
Type: DIMENSION
Primary Key: MARITAL_ID

Description:
Records marital and union-related dates for households, specifically the date of marriage and the date of gauna (a post-marriage cohabitation ritual in some cultures).

Columns:
- MARITAL_ID | BIGINT | IDENTIFIER | Surrogate primary key for the marital record
- HOUSEHOLD_ID | BIGINT | IDENTIFIER | Foreign key to the household (DIM_HOUSEHOLD)
- DATE_OF_MARRIAGE | DATE | DATE | Date of marriage
- DATE_OF_GAUNA | DATE | DATE | Date of gauna ceremony

Foreign Keys:
- HOUSEHOLD_ID → DIM_HOUSEHOLD.HOUSEHOLD_ID

Natural Language Synonyms:
- Marriage details
- Union timeline
- Marital events
- Household marriage data
- Wedding dates

--------------------------------------------------

## TABLE: DIM_EDUCATION_ADULT
Type: DIMENSION
Primary Key: ADULT_EDUCATION_ID

Description:
Contains education and literacy metrics for adults (21+) within the household, including the highest education level, literacy status of women, and the household head's education.

Columns:
- ADULT_EDUCATION_ID | BIGINT | IDENTIFIER | Surrogate primary key for the adult education record
- HOUSEHOLD_ID | BIGINT | IDENTIFIER | Foreign key to the household (DIM_HOUSEHOLD)
- HIGHEST_EDUCATION_LEVEL_AMONG_ADULTS_21_PLUS | VARCHAR(50) | CATEGORY | Highest educational attainment among adults aged 21 and above
- WOMAN_LITERACY_STATUS | BOOLEAN | CATEGORY | Literacy status of a woman in the household (likely the primary respondent or spouse)
- HOUSEHOLD_HEAD_EDUCATION_LEVEL | VARCHAR(50) | CATEGORY | Educational level of the household head
- WOMAN_EVER_ATTENDED_SCHOOL | BOOLEAN | CATEGORY | Indicator if a woman in the household ever attended school

Foreign Keys:
- HOUSEHOLD_ID → DIM_HOUSEHOLD.HOUSEHOLD_ID

Natural Language Synonyms:
- Adult learning metrics
- Literacy and education
- Household education profile
- Adult schooling
- Education attainment

--------------------------------------------------

## TABLE: DIM_EDUCATION_CHILD
Type: DIMENSION
Primary Key: CHILD_EDUCATION_ID

Description:
Captures education status for children in the household, including school enrollment and current grade level.

Columns:
- CHILD_EDUCATION_ID | BIGINT | IDENTIFIER | Surrogate primary key for the child education record
- HOUSEHOLD_ID | BIGINT | IDENTIFIER | Foreign key to the household (DIM_HOUSEHOLD)
- CHILD_SCHOOL_ENROLLMENT_STATUS | VARCHAR(50) | CATEGORY | Current enrollment status of a child (e.g., enrolled, dropped out)
- CHILD_CURRENT_GRADE_LEVEL | VARCHAR(50) | CATEGORY | Current grade/standard the child is attending

Foreign Keys:
- HOUSEHOLD_ID → DIM_HOUSEHOLD.HOUSEHOLD_ID

Natural Language Synonyms:
- Child schooling
- Education enrollment
- Student status
- School participation
- Child academic record

--------------------------------------------------

## TABLE: DIM_HEALTH_CHILD
Type: DIMENSION
Primary Key: CHILD_HEALTH_ID

Description:
Stores child health and immunization indicators, including vaccine records and a household-level health insurance indicator.

Columns:
- CHILD_HEALTH_ID | BIGINT | IDENTIFIER | Surrogate primary key for the child health record
- HOUSEHOLD_ID | BIGINT | IDENTIFIER | Foreign key to the household (DIM_HOUSEHOLD)
- CHILD_IMMUNIZATION_CARD_AVAILABLE | BOOLEAN | CATEGORY | Availability of an immunization card for a child
- LAST_BIRTH_CHILD_IMMUNIZED_BCG | BOOLEAN | CATEGORY | Indicator if the last born child received the BCG vaccine
- LAST_BIRTH_CHILD_IMMUNIZED_DPT1 | BOOLEAN | CATEGORY | Indicator if the last born child received the first dose of DPT vaccine
- HOUSEHOLD_HAS_HEALTH_INSURANCE_INDICATOR | BOOLEAN | CATEGORY | Indicator if any member of the household has health insurance

Foreign Keys:
- HOUSEHOLD_ID → DIM_HOUSEHOLD.HOUSEHOLD_ID

Natural Language Synonyms:
- Child vaccination
- Immunization status
- Pediatric health
- Health coverage
- Vaccine records

--------------------------------------------------

## TABLE: DIM_LIVELIHOOD_INCOME
Type: DIMENSION
Primary Key: INCOME_ID

Description:
Documents household income sources, total income, outstanding loans, and details of a primary business activity.

Columns:
- INCOME_ID | BIGINT | IDENTIFIER | Surrogate primary key for the income record
- HOUSEHOLD_ID | BIGINT | IDENTIFIER | Foreign key to the household (DIM_HOUSEHOLD)
- HOUSEHOLD_TOTAL_INCOME | DOUBLE PRECISION | MEASURE | Total household income in Rupees
- LIVESTOCK_INCOME_RS | DOUBLE PRECISION | MEASURE | Income derived from livestock in Rupees
- HOUSEHOLD_TOTAL_OUTSTANDING_LOANS_RS | DOUBLE PRECISION | MEASURE | Total value of outstanding loans in Rupees
- BUSINESS_1_ACTIVITY_SUBTYPE | VARCHAR(100) | CATEGORY | Sub-category description of the primary business activity

Foreign Keys:
- HOUSEHOLD_ID → DIM_HOUSEHOLD.HOUSEHOLD_ID

Natural Language Synonyms:
- Income sources
- Livelihood details
- Financial status
- Earnings and debt
- Economic activities

--------------------------------------------------

## TABLE: DIM_HOUSEHOLD_CHORES
Type: DIMENSION
Primary Key: CHORES_ID

Description:
Measures time allocation for basic needs collection activities, specifically fuel collection (by women and children) and water collection.

Columns:
- CHORES_ID | BIGINT | IDENTIFIER | Surrogate primary key for the chores record
- HOUSEHOLD_ID | BIGINT | IDENTIFIER | Foreign key to the household (DIM_HOUSEHOLD)
- FUEL_COLLECTION_TIME_WOMEN_HOURS | DOUBLE PRECISION | MEASURE | Time spent by women collecting fuel, in hours
- FUEL_COLLECTION_TIME_CHILDREN_HOURS | DOUBLE PRECISION | MEASURE | Time spent by children collecting fuel, in hours
- WATER_COLLECTION_TIME_MINUTES | DOUBLE PRECISION | MEASURE | Time spent collecting water, in minutes

Foreign Keys:
- HOUSEHOLD_ID → DIM_HOUSEHOLD.HOUSEHOLD_ID

Natural Language Synonyms:
- Time use data
- Collection chores
- Domestic work time
- Fuel and water collection
- Unpaid labor hours

--------------------------------------------------

## TABLE: FACT_HOUSEHOLD_CONSUMPTION
Type: FACT
Primary Key: CONSUMPTION_ID

Description:
Core fact table for household consumption expenditure, including per capita, total, and non-food expenditure metrics. Linked to household and geography.

Columns:
- CONSUMPTION_ID | BIGINT | IDENTIFIER | Surrogate primary key for the consumption fact
- HOUSEHOLD_ID | BIGINT | IDENTIFIER | Foreign key to the household (DIM_HOUSEHOLD)
- GEOGRAPHY_ID | BIGINT | IDENTIFIER | Foreign key to the geographic location (DIM_GEOGRAPHY_HIERARCHY)
- MONTHLY_CONSUMPTION_PER_CAPITA | DOUBLE PRECISION | MEASURE | Average monthly consumption expenditure per person
- MONTHLY_TOTAL_CONSUMPTION_EXPENDITURE_RS | DOUBLE PRECISION | MEASURE | Total monthly consumption expenditure in Rupees
- MONTHLY_NON_FOOD_EXPENDITURE_RS | DOUBLE PRECISION | MEASURE | Monthly expenditure on non-food items in Rupees

Foreign Keys:
- HOUSEHOLD_ID → DIM_HOUSEHOLD.HOUSEHOLD_ID
- GEOGRAPHY_ID → DIM_GEOGRAPHY_HIERARCHY.GEOGRAPHY_ID

Natural Language Synonyms:
- Spending facts
- Consumption expenditure
- Household spending
- Expenditure metrics
- Consumption aggregate

--------------------------------------------------

## TABLE: FACT_UTILITY_CONSUMPTION
Type: FACT
Primary Key: UTILITY_ID

Description:
Fact table for household utility expenditure, specifically electricity bills, and includes the survey design weight for statistical representativeness.

Columns:
- UTILITY_ID | BIGINT | IDENTIFIER | Surrogate primary key for the utility fact
- HOUSEHOLD_ID | BIGINT | IDENTIFIER | Foreign key to the household (DIM_HOUSEHOLD)
- MONTHLY_ELECTRICITY_BILL_RS | DOUBLE PRECISION | MEASURE | Monthly electricity bill amount in Rupees
- SURVEY_DESIGN_WEIGHT | DOUBLE PRECISION | MEASURE | Statistical weight for the household to ensure sample representativeness

Foreign Keys:
- HOUSEHOLD_ID → DIM_HOUSEHOLD.HOUSEHOLD_ID

Natural Language Synonyms:
- Utility bills
- Electricity spending
- Survey weighted facts
- Energy expenditure
- Utility costs

--------------------------------------------------

## TABLE: FACT_FOOD_CONSUMPTION
Type: FACT
Primary Key: FOOD_CONSUMPTION_ID

Description:
Detailed fact table for expenditure on specific food items, including staples like rice, wheat, sugar, pulses, milk, vegetables, and edible oils.

Columns:
- FOOD_CONSUMPTION_ID | BIGINT | IDENTIFIER | Surrogate primary key for the food consumption fact
- HOUSEHOLD_ID | BIGINT | IDENTIFIER | Foreign key to the household (DIM_HOUSEHOLD)
- CONSUMPTION_RICE_EXPENDITURE_RS | DOUBLE PRECISION | MEASURE | Expenditure on rice in Rupees
- CONSUMPTION_WHEAT_EXPENDITURE_RS | DOUBLE PRECISION | MEASURE | Expenditure on wheat in Rupees
- CONSUMPTION_SUGAR_EXPENDITURE_RS | DOUBLE PRECISION | MEASURE | Expenditure on sugar in Rupees
- CONSUMPTION_PULSES_EXPENDITURE_RS | DOUBLE PRECISION | MEASURE | Expenditure on pulses in Rupees
- CONSUMPTION_MILK_EXPENDITURE_RS | DOUBLE PRECISION | MEASURE | Expenditure on milk in Rupees
- CONSUMPTION_VEGETABLES_EXPENDITURE_RS | DOUBLE PRECISION | MEASURE | Expenditure on vegetables in Rupees
- CONSUMPTION_EDIBLE_OILS_EXPENDITURE_RS | DOUBLE PRECISION | MEASURE | Expenditure on edible oils in Rupees

Foreign Keys:
- HOUSEHOLD_ID → DIM_HOUSEHOLD.HOUSEHOLD_ID

Natural Language Synonyms:
- Food spending
- Grocery expenditure
- Staple food costs
- Dietary consumption facts
- Food item expenses

--------------------------------------------------

==================================================
# JOIN PATHS
- DIM_VILLAGE_INFRASTRUCTURE joins DIM_GEOGRAPHY_HIERARCHY via GEOGRAPHY_ID
- DIM_HOUSEHOLD_SOCIAL joins DIM_HOUSEHOLD via HOUSEHOLD_ID
- DIM_HOUSEHOLD_MARITAL joins DIM_HOUSEHOLD via HOUSEHOLD_ID
- DIM_EDUCATION_ADULT joins DIM_HOUSEHOLD via HOUSEHOLD_ID
- DIM_EDUCATION_CHILD joins DIM_HOUSEHOLD via HOUSEHOLD_ID
- DIM_HEALTH_CHILD joins DIM_HOUSEHOLD via HOUSEHOLD_ID
- DIM_LIVELIHOOD_INCOME joins DIM_HOUSEHOLD via HOUSEHOLD_ID
- DIM_HOUSEHOLD_CHORES joins DIM_HOUSEHOLD via HOUSEHOLD_ID
- FACT_HOUSEHOLD_CONSUMPTION joins DIM_HOUSEHOLD via HOUSEHOLD_ID
- FACT_HOUSEHOLD_CONSUMPTION joins DIM_GEOGRAPHY_HIERARCHY via GEOGRAPHY_ID
- FACT_UTILITY_CONSUMPTION joins DIM_HOUSEHOLD via HOUSEHOLD_ID
- FACT_FOOD_CONSUMPTION joins DIM_HOUSEHOLD via HOUSEHOLD_ID

==================================================
# BUSINESS RULES (Only if derivable from schema)
- All dimension tables related to household attributes (Social, Marital, Education, Health, Income, Chores) must have a valid HOUSEHOLD_ID that exists in DIM_HOUSEHOLD.
- A village's infrastructure details (DIM_VILLAGE_INFRASTRUCTURE) are defined for a specific geographic unit (GEOGRAPHY_ID) in DIM_GEOGRAPHY_HIERARCHY.
- The survey design weight (SURVEY_DESIGN_WEIGHT) is stored at the household level within the utility consumption fact (FACT_UTILITY_CONSUMPTION) and should be applied for population-level estimates.
- Consumption facts (FACT_HOUSEHOLD_CONSUMPTION, FACT_FOOD_CONSUMPTION, FACT_UTILITY_CONSUMPTION) are recorded at the household level.
- The household assets index (HOUSEHOLD_ASSETS_INDEX) is a composite wealth measure stored in the social dimension.

==================================================
# COMMON QUERY PATTERNS
- Calculate average monthly per capita consumption by state and urban/rural classification.
- Analyze the correlation between household assets index and total consumption expenditure.
- Compare food expenditure patterns (e.g., rice vs. wheat) across different caste/religion groups.
- Assess child immunization rates (BCG, DPT1) against the availability of village health facilities.
- Evaluate the relationship between time spent on water collection and household income levels.
- Aggregate total outstanding loans by district and business activity subtype.
- Filter households where women's literacy status is true and analyze their non-food expenditure.

==================================================
# KNOWN PITFALLS
- The column `HOUSEHOLD_HAS_HEALTH_INSURANCE_INDICATOR` is stored in DIM_HEALTH_CHILD, which may imply it's child-specific, but the name suggests it's a household-level attribute. Interpretation requires caution.
- Joining all household-related dimensions (Social, Marital, Education, etc.) to the fact tables requires multiple joins through DIM_HOUSEHOLD, which can lead to fan-out if not carefully managed.
- The `CENSUS_2001_URBAN_RURAL` classification is based on the 2001 census and may not reflect current urban/rural boundaries.
- The `SURVEY_DESIGN_WEIGHT` is only present in FACT_UTILITY_CONSUMPTION. For weighted analyses using other facts, this weight must be joined via HOUSEHOLD_ID.
- Time allocation columns (fuel and water collection) are in different units (hours vs. minutes), requiring unit conversion for combined analysis.

==================================================
# CONFIDENCE SCORE
95