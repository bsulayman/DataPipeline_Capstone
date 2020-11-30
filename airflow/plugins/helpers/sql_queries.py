class SqlQueries:
    state_temperature_table_insert = ("""
        INSERT INTO state_temperature (state, month, avg_temperature)
        SELECT DISTINCT state, month, AVG(avg_temp) AS avg_temperature
        FROM staging_state_temperature 
        WHERE year <= (SELECT MAX(year) FROM staging_state_temperature) AND year > (SELECT MAX(year) FROM staging_state_temperature)-10
        GROUP BY state, month 
        ORDER BY state, month ASC
    """)

    date_time_table_insert = ("""
        INSERT INTO date_time (date, day, week, month, year, weekday)
        SELECT DISTINCT arrival_date, EXTRACT(DAY from arrival_date), EXTRACT(WEEK from arrival_date), 
               EXTRACT(MONTH from arrival_date), EXTRACT(YEAR from arrival_date), EXTRACT(DAYOFWEEK from arrival_date)
        FROM fact_immigration
    """)