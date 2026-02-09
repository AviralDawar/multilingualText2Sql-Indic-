-- Complexity: Level 1 (Simple)
-- Category: Caste Demographics
SELECT SUM(SC_POPULATION_TOTAL) AS TOTAL_SC,
       SUM(ST_POPULATION_TOTAL) AS TOTAL_ST
FROM CENSUS_CASTE;
