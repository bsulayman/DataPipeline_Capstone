CREATE TABLE IF NOT EXISTS staging_state_temperature(
    dt DATE NOT NULL,
    state VARCHAR(256) NOT NULL,
    avg_temp FLOAT4,
    "month" INT4 NOT NULL,
    "year" INT4 NOT NULL,
    CONSTRAINT state_dt PRIMARY KEY (dt, state)
);

CREATE TABLE IF NOT EXISTS fact_immigration (
    cic_id INT8 PRIMARY KEY NOT NULL,
    born_country INT4,
    res_country INT4,
    i94_mode INT4,
    arr_state VARCHAR(3),
    age INT4,
    i94_visa INT4,
    birth_year INT4,
    gender VARCHAR(1),
    visa_type VARCHAR(3),
    arrival_date DATE NOT NULL,
);

CREATE TABLE IF NOT EXISTS city_demographics (
    city VARCHAR(256) NOT NULL,
    state VARCHAR(256) NOT NULL,
    med_age FLOAT4,
    male_pop INT8,
    female_pop INT8,
    tot_pop INT8,
    num_veterans INT8,
    foreign_born INT8,
    avg_household FLOAT4,
    race VARCHAR(256),
    "count" INT8,
    CONSTRAINT city_state PRIMARY KEY (city, state)
);

CREATE TABLE IF NOT EXISTS date_time(
    "date" DATE PRIMARY KEY NOT NULL,
    "day" INT4,
    "week" INT4,
    "month" VARCHAR(20),
    "year" INT4,
    "weekday" VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS country_code(
    code INT4 PRIMARY KEY NOT NULL,
    country VARCHAR(256)
);

CREATE TABLE IF NOT EXISTS state_temperature(
    state VARCHAR(256) PRIMARY KEY NOT NULL,
    "month" INT4 NOT NULL,
    avg_temperature FLOAT4
);