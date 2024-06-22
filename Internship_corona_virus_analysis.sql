CREATE DATABASE corona_virus;
USE corona_virus;
CREATE TABLE corona_virus_data (
    Province VARCHAR(255),
    Country_Region VARCHAR(255),
    Latitude FLOAT,
    Longitude FLOAT,
    Date DATE,
    Confirmed INT,
    Deaths INT,
    Recovered INT
);
SHOW VARIABLES LIKE 'secure_file_priv';
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Corona Virus Dataset .csv'
INTO TABLE corona_virus_data
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(@Province, @Country_Region, @Latitude, @Longitude, @Date, @Confirmed, @Deaths, @Recovered)
SET
    Province = @Province,
    Country_Region = @Country_Region,
    Latitude = @Latitude,
    Longitude = @Longitude,
    Date = STR_TO_DATE(@Date, '%d-%m-%Y'),
    Confirmed = @Confirmed,
    Deaths = @Deaths,
    Recovered = @Recovered;
SELECT
    SUM(CASE WHEN Province IS NULL THEN 1 ELSE 0 END) AS null_province,
    SUM(CASE WHEN Country_Region IS NULL THEN 1 ELSE 0 END) AS null_country_region,
    SUM(CASE WHEN Latitude IS NULL THEN 1 ELSE 0 END) AS null_latitude,
    SUM(CASE WHEN Longitude IS NULL THEN 1 ELSE 0 END) AS null_longitude,
    SUM(CASE WHEN Date IS NULL THEN 1 ELSE 0 END) AS null_date,
    SUM(CASE WHEN Confirmed IS NULL THEN 1 ELSE 0 END) AS null_confirmed,
    SUM(CASE WHEN Deaths IS NULL THEN 1 ELSE 0 END) AS null_deaths,
    SUM(CASE WHEN Recovered IS NULL THEN 1 ELSE 0 END) AS null_recovered
FROM corona_virus_data;
SET SQL_SAFE_UPDATES = 0;
UPDATE corona_virus_data
SET
    Province = IFNULL(Province, 'Unknown'),
    Country_Region = IFNULL(Country_Region, 'Unknown'),
    Latitude = IFNULL(Latitude, 0),
    Longitude = IFNULL(Longitude, 0),
    Confirmed = IFNULL(Confirmed, 0),
    Deaths = IFNULL(Deaths, 0),
    Recovered = IFNULL(Recovered, 0);
SELECT COUNT(*) AS total_rows FROM corona_virus_data;
SELECT
    MIN(Date) AS start_date,
    MAX(Date) AS end_date
FROM corona_virus_data;
SELECT COUNT(DISTINCT DATE_FORMAT(Date, '%Y-%m')) AS number_of_months FROM corona_virus_data;
SELECT
    DATE_FORMAT(Date, '%Y-%m') AS month,
    AVG(Confirmed) AS avg_confirmed,
    AVG(Deaths) AS avg_deaths,
    AVG(Recovered) AS avg_recovered
FROM corona_virus_data
GROUP BY DATE_FORMAT(Date, '%Y-%m');
SELECT
    month,
    MAX(confirmed) AS most_frequent_confirmed,
    MAX(deaths) AS most_frequent_deaths,
    MAX(recovered) AS most_frequent_recovered
FROM (
    SELECT
        DATE_FORMAT(Date, '%Y-%m') AS month,
        Confirmed,
        Deaths,
        Recovered,
        ROW_NUMBER() OVER (PARTITION BY DATE_FORMAT(Date, '%Y-%m') ORDER BY COUNT(*) DESC) AS row_num
    FROM corona_virus_data
    GROUP BY DATE_FORMAT(Date, '%Y-%m'), Confirmed, Deaths, Recovered
) AS subquery
WHERE row_num = 1
GROUP BY month;
SELECT
    DATE_FORMAT(Date, '%Y') AS year,
    MIN(Confirmed) AS min_confirmed,
    MIN(Deaths) AS min_deaths,
    MIN(Recovered) AS min_recovered
FROM corona_virus_data
GROUP BY DATE_FORMAT(Date, '%Y');
SELECT
    DATE_FORMAT(Date, '%Y') AS year,
    MAX(Confirmed) AS max_confirmed,
    MAX(Deaths) AS max_deaths,
    MAX(Recovered) AS max_recovered
FROM corona_virus_data
GROUP BY DATE_FORMAT(Date, '%Y');
SELECT
    DATE_FORMAT(Date, '%Y-%m') AS month,
    SUM(Confirmed) AS total_confirmed,
    SUM(Deaths) AS total_deaths,
    SUM(Recovered) AS total_recovered
FROM corona_virus_data
GROUP BY DATE_FORMAT(Date, '%Y-%m');
SELECT
    DATE_FORMAT(Date, '%Y-%m') AS month,
    SUM(Confirmed) AS total_confirmed,
    AVG(Confirmed) AS avg_confirmed,
    VAR_POP(Confirmed) AS variance_confirmed,
    STDDEV_POP(Confirmed) AS stdev_confirmed
FROM corona_virus_data
GROUP BY DATE_FORMAT(Date, '%Y-%m');
SELECT
    DATE_FORMAT(Date, '%Y-%m') AS month,
    SUM(Deaths) AS total_deaths,
    AVG(Deaths) AS avg_deaths,
    VAR_POP(Deaths) AS variance_deaths,
    STDDEV_POP(Deaths) AS stdev_deaths
FROM corona_virus_data
GROUP BY DATE_FORMAT(Date, '%Y-%m');
SELECT
    DATE_FORMAT(Date, '%Y-%m') AS month,
    SUM(Recovered) AS total_recovered,
    AVG(Recovered) AS avg_recovered,
    VAR_POP(Recovered) AS variance_recovered,
    STDDEV_POP(Recovered) AS stdev_recovered
FROM corona_virus_data
GROUP BY DATE_FORMAT(Date, '%Y-%m');
SELECT
    Country_Region,
    SUM(Confirmed) AS total_confirmed
FROM corona_virus_data
GROUP BY Country_Region
ORDER BY total_confirmed DESC
LIMIT 1;
SELECT
    Country_Region,
    SUM(Deaths) AS total_deaths
FROM corona_virus_data
GROUP BY Country_Region
ORDER BY total_deaths ASC
LIMIT 1;
SELECT
    Country_Region,
    SUM(Recovered) AS total_recovered
FROM corona_virus_data
GROUP BY Country_Region
ORDER BY total_recovered DESC
LIMIT 5;










