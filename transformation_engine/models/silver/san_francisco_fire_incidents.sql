{{ config(schema='silver') }}

SELECT   
    CASE WHEN "ID" IN ('None', '-') THEN NULL ELSE "ID" END AS id,  
    "Incident Number" AS incident_number,  
    "Exposure Number" AS exposure_number,  
    -- Address-related columns  
    CASE WHEN "Address" IN ('None', '-') THEN NULL ELSE "Address" END AS address,  
    CASE WHEN "City" IN ('None', '-') THEN NULL ELSE "City" END AS city,  
    "zipcode" AS zipcode,  
    "Supervisor District" AS supervisor_district,  
    IFNULL(CASE WHEN "neighborhood_district" IN ('None', '-') THEN NULL ELSE "neighborhood_district" END, 'NOT_INFORMED') AS neighborhood_district,
    CASE WHEN "Battalion" IN ('None', '-') THEN NULL ELSE "Battalion" END AS battalion,   
    "point" AS point,  
    -- Date-related columns  
    CASE WHEN "Incident Date" IN ('None', '-') THEN NULL ELSE "Incident Date" END AS incident_date,  
    CASE WHEN "Alarm DtTm" IN ('None', '-') THEN NULL ELSE "Alarm DtTm" END AS alarm_dttm,  
    CASE WHEN "Arrival DtTm" IN ('None', '-') THEN NULL ELSE "Arrival DtTm" END AS arrival_dttm,  
    CASE WHEN "Close DtTm" IN ('None', '-') THEN NULL ELSE "Close DtTm" END AS close_dttm,  
    -- Detector-related columns  
    CASE WHEN "Detectors Present" IN ('None', '-') THEN NULL ELSE "Detectors Present" END AS detectors_present,  
    CASE WHEN "Detector Type" IN ('None', '-') THEN NULL ELSE "Detector Type" END AS detector_type,  
    CASE WHEN "Detector Operation" IN ('None', '-') THEN NULL ELSE "Detector Operation" END AS detector_operation,  
    CASE WHEN "Detector Effectiveness" IN ('None', '-') THEN NULL ELSE "Detector Effectiveness" END AS detector_effectiveness,  
    CASE WHEN "Detector Failure Reason" IN ('None', '-') THEN NULL ELSE "Detector Failure Reason" END AS detector_failure_reason,  
    CASE WHEN "Automatic Extinguishing System Present" IN ('None', '-') THEN NULL ELSE "Automatic Extinguishing System Present" END AS automatic_extinguishing_system_present,  
    CASE WHEN "Automatic Extinguishing Sytem Type" IN ('None', '-') THEN NULL ELSE "Automatic Extinguishing Sytem Type" END AS automatic_extinguishing_sytem_type,  
    CASE WHEN "Automatic Extinguishing Sytem Perfomance" IN ('None', '-') THEN NULL ELSE "Automatic Extinguishing Sytem Perfomance" END AS automatic_extinguishing_sytem_perfomance,  
    CASE WHEN "Automatic Extinguishing Sytem Failure Reason" IN ('None', '-') THEN NULL ELSE "Automatic Extinguishing Sytem Failure Reason" END AS automatic_extinguishing_sytem_failure_reason,  
    "Number of Sprinkler Heads Operating" AS number_of_sprinkler_heads_operating,  
    -- Unit-related columns  
    "Suppression Units" AS suppression_units,  
    "Suppression Personnel" AS suppression_personnel,  
    "EMS Units" AS ems_units,  
    "EMS Personnel" AS ems_personnel,  
    "Other Units" AS other_units,  
    "Other Personnel" AS other_personnel,  
    CASE WHEN "First Unit On Scene" IN ('None', '-') THEN NULL ELSE "First Unit On Scene" END AS first_unit_on_scene,  
    -- Casualty-related columns  
    "Fire Fatalities" AS fire_fatalities,  
    "Fire Injuries" AS fire_injuries,  
    "Civilian Fatalities" AS civilian_fatalities,  
    "Civilian Injuries" AS civilian_injuries,  
    -- Alarm and Action-related columns  
    "Call Number" AS call_number,  
    "Number of Alarms" AS number_of_alarms,  
    CASE WHEN "Primary Situation" IN ('None', '-') THEN NULL ELSE "Primary Situation" END AS primary_situation,  
    CASE WHEN "Mutual Aid" IN ('None', '-') THEN NULL ELSE "Mutual Aid" END AS mutual_aid,  
    CASE WHEN "Action Taken Primary" IN ('None', '-') THEN NULL ELSE "Action Taken Primary" END AS action_taken_primary,  
    CASE WHEN "Action Taken Secondary" IN ('None', '-') THEN NULL ELSE "Action Taken Secondary" END AS action_taken_secondary,  
    CASE WHEN "Action Taken Other" IN ('None', '-') THEN NULL ELSE "Action Taken Other" END AS action_taken_other,  
    -- Fire-related columns  
    "Estimated Contents Loss" AS estimated_contents_loss,  
    "Estimated Property Loss" AS estimated_property_loss,  
    CASE WHEN "Area of Fire Origin" IN ('None', '-') THEN NULL ELSE "Area of Fire Origin" END AS area_of_fire_origin,  
    CASE WHEN "Ignition Cause" IN ('None', '-') THEN NULL ELSE "Ignition Cause" END AS ignition_cause,  
    CASE WHEN "Ignition Factor Primary" IN ('None', '-') THEN NULL ELSE "Ignition Factor Primary" END AS ignition_factor_primary,  
    CASE WHEN "Ignition Factor Secondary" IN ('None', '-') THEN NULL ELSE "Ignition Factor Secondary" END AS ignition_factor_secondary,  
    CASE WHEN "Heat Source" IN ('None', '-') THEN NULL ELSE "Heat Source" END AS heat_source,  
    CASE WHEN "Item First Ignited" IN ('None', '-') THEN NULL ELSE "Item First Ignited" END AS item_first_ignited,  
    CASE WHEN "Human Factors Associated with Ignition" IN ('None', '-') THEN NULL ELSE "Human Factors Associated with Ignition" END AS human_factors_associated_with_ignition,  
    -- Structure-related columns  
    CASE WHEN "Structure Type" IN ('None', '-') THEN NULL ELSE "Structure Type" END AS structure_type,  
    CASE WHEN "Structure Status" IN ('None', '-') THEN NULL ELSE "Structure Status" END AS structure_status,  
    "Floor of Fire Origin" AS floor_of_fire_origin,  
    CASE WHEN "Fire Spread" IN ('None', '-') THEN NULL ELSE "Fire Spread" END AS fire_spread,  
    "No Flame Spead" AS no_flame_spead,  
    "Number of floors with minimum damage" AS number_of_floors_with_minimum_damage,  
    "Number of floors with significant damage" AS number_of_floors_with_significant_damage,  
    "Number of floors with heavy damage" AS number_of_floors_with_heavy_damage,  
    "Number of floors with extreme damage" AS number_of_floors_with_extreme_damage  
FROM 
    {{ source('bronze', 'san_francisco_fire_incidents') }}