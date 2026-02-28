SELECT
    country_code,
    country_name,
    indicator_code,
    indicator_name,
    year,
    value
FROM dataset
WHERE year >= {{min_year}}
ORDER BY country_code, year
