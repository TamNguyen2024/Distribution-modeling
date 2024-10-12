WITH cte_date_mapping AS
(
    SELECT 
        d.d_date,
        is_last_week,
        d.year, 
        d.month, 
        d.eomonth,
        d.week,
        d.eoweek,
        CASE 
            WHEN d.is_last_week = 1 THEN CAST(d.week - 1 AS int) 
            ELSE d.week
        END AS map_week, 
        d.day_week,
        CASE 
            WHEN d.is_last_week = 1 THEN 6 
            ELSE d.day_week 
        END AS map_day
    FROM {{ ref('dim_date') }} d
)
SELECT DISTINCT
    d.year,
    d.month,
    d.eomonth,
    d.week,
    d.eoweek,
    d.d_date,
    d.is_last_week,
    CAST(d1.d_date AS DATE) AS SOL_date
FROM cte_date_mapping d
LEFT JOIN  cte_date_mapping d1
    ON d.year = d1.year
    AND d.month = d1.month
    AND d.map_week = d1.week
    AND d.map_day = d1.day_week



