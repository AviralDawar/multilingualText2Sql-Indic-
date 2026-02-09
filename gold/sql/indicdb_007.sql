-- Complexity: Level 1 (Simple)
-- Category: Population Analytics
SELECT CENSUS_YEAR, COUNT(*) AS RECORD_COUNT
FROM CENSUS_POPULATION
GROUP BY CENSUS_YEAR;
