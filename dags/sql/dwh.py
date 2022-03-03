from airflow.models import Variable

# Global configuration
DAG_ID = 'data-warehouse-dag'
config = Variable.get(DAG_ID, deserialize_json=True)
bigquery = config['bq']
PROJECT_ID = bigquery['project_id']
STAGING = bigquery['staging_dataset']
DWH = bigquery['dwh_dataset']



DWH_IMMIGRATION = f"""  
    CREATE OR REPLACE TABLE {DWH}.f_immigration_data
    OPTIONS(
      description="Fact table IMMIGRATION_DATA"
    ) AS
    SELECT
      cicid,
      visapost AS country_of_origin,
      i94port AS port_name,
      DATE_ADD(DATE "1960-01-01", INTERVAL CAST(arrdate AS INT) day) AS arrival_date,
      CAST(i94mode AS STRING) AS arrival_mode,
      i94addr AS destination_state,
      DATE_ADD(DATE "1960-01-01", INTERVAL CAST(depdate AS INT) day) AS departure_date,
      CAST(i94bir AS numeric) AS age,
      CAST(i94visa AS string) AS visa_category,
      gender,
      CAST(biryear AS numeric) AS birth_year,
      visatype AS visa_type
    FROM {PROJECT_ID}.{STAGING}.immigration_data_sample;
"""

DWH_COUNTRY = f"""
    CREATE OR REPLACE TABLE {DWH}.d_country
    OPTIONS(
      description="Dimension table COUNTRY"
    ) AS
    SELECT
      CAST(code AS STRING) AS country_id,
      i94ctry AS country_name
    FROM {PROJECT_ID}.{STAGING}.uscountry;
"""

DWH_TIME = f"""
    CREATE OR REPLACE TABLE {DWH}.d_time
    OPTIONS(
      description="Dimension table TIME"
    ) AS
    SELECT
      date,
      EXTRACT(DAY FROM date)AS day,
      EXTRACT(MONTH FROM date) AS month,
      EXTRACT(YEAR FROM date) AS year,
      EXTRACT(QUARTER FROM date) AS quarter,
      EXTRACT(DAYOFWEEK FROM date) AS dayofweek,
      EXTRACT(WEEK FROM date) AS weekofyear
    FROM (
      SELECT 
        distinct(arrival_date) AS date 
      FROM {PROJECT_ID}.{DWH}.f_immigration_data
    );
"""

DWH_PORT = f"""
    CREATE OR REPLACE TABLE {DWH}.d_port
    OPTIONS(
      description="Dimension table PORT"
    ) AS
    SELECT
      id AS port_id,
      port AS port_name
    FROM {PROJECT_ID}.{STAGING}.usport;
"""

DWH_STATE = f"""
    CREATE OR REPLACE TABLE {DWH}.d_state
    OPTIONS(
      description="Dimension table STATE"
    ) AS
    SELECT
      code AS state_id,
      state AS state_name
    FROM {PROJECT_ID}.{STAGING}.usstate;
"""

DWH_WEATHER = f"""
    CREATE OR REPLACE TABLE {DWH}.d_weather
    OPTIONS(
      description="Dimension table WEATHER"
    ) AS
    SELECT
      year,
      city,
      country,
      latitude,
      longitude,
      AVG(averagetemperature) AS avg_temperature
    FROM
      {PROJECT_ID}.{STAGING}.globallandtemperaturesbycity
    WHERE
      country='United States'
    GROUP BY
      1,2,3,4,5
    ORDER BY
      1,2;
"""

DWH_DEMO = f"""
    CREATE OR REPLACE TABLE {DWH}.d_city_demo
    OPTIONS( 
      description="Dimension table CITY_DEMO" 
    ) AS
    SELECT
    *
    FROM (
      SELECT
          city AS city_name,
          state AS state_name,
          statecode AS state_id,
          medianage AS median_age,
          malepopulation AS male_population,
          femalepopulation AS female_population,
          foreignborn AS foreign_born,
          count,
          CASE
          WHEN race = 'White' THEN 'white_population'
          WHEN race = 'Black or African-American' THEN 'black_population'
          WHEN race = 'Asian' THEN 'asian_population'
          WHEN race = 'Hispanic or Latino' THEN 'latino_population'
          WHEN race = 'American Indian and Alaska Native' THEN 'native_population'
      END
          AS population
      FROM
          {PROJECT_ID}.{STAGING}.us_cities_demographics
    )
    PIVOT(
        SUM(count) FOR population IN (
        'white_population',
        'black_population',
        'asian_population',
        'latino_population',
        'native_population'
        )
    );
"""


