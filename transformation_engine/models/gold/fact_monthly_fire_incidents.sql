{{ config(schema='gold') }}

SELECT
  YEAR(incident_date) AS year,
  MONTH(incident_date) AS month,
  neighborhood_district AS district,
  battalion,
  COUNT(DISTINCT id) AS number_incidents,
  IFNULL(SUM(fire_fatalities), 0) AS fire_fatalities,
  IFNULL(SUM(fire_injuries), 0) AS fire_injuries,
  IFNULL(SUM(civilian_fatalities), 0) AS civilian_fatalities,
  IFNULL(SUM(civilian_injuries), 0) AS civilian_injuries,
  IFNULL(SUM(estimated_property_loss), 0) AS estimated_property_loss,
  IFNULL(SUM(estimated_contents_loss), 0) AS estimated_contents_loss
FROM
  {{ ref('san_francisco_fire_incidents') }}
GROUP BY
  1, 2, 3, 4