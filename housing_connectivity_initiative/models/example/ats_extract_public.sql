{{ config(
    materialized='table',
    schema='housing_raw_schema',
    unique_key=['project_id', 'application_id']
) }}


SELECT 'FOR' ministrycode
	     , authorizations.business_area
	     , 'ATS' source_system_acronym
	     , authorizations.permit_type
	     , projects.project_id project_id
	     , authorizations.AUTHORIZATION_ID application_id
	     , projects.project_name
	     , projects.project_description
	     , projects.project_location
	     , NULL utm_easting
	     , NULL utm_northing
	     , authorizations.RECEIVED_DATE
	     , authorizations.ACCEPTED_DATE 
	     , authorizations.adjudication_date 
	     , authorizations.rejected_date
	     , authorizations.AMENDMENT_RENEWAL_DATE
	     , authorizations.TECH_REVIEW_COMPLETION_DATE
	     , authorizations.fn_consultn_start_date
	     , authorizations.fn_consultn_completion_date
	     , NULL fn_consultn_comment
	     , projects.region_name
	     ,   CASE
    WHEN array_length(string_to_array(projects.project_description, ','), 1) >= 2
    THEN CASE
      WHEN (string_to_array(projects.project_description, ','))[3] = 'Not_IL' THEN 'N'
      WHEN (string_to_array(projects.project_description, ','))[3] = 'IL' THEN 'Y'
      ELSE 'Unknown'
    END
    ELSE NULL
  END indigenous_led_ind
	     , CASE
    WHEN array_length(string_to_array(projects.project_description, ','), 1) >= 2
    THEN CASE
      WHEN (string_to_array(projects.project_description, ','))[4] = 'Rental' THEN 'Y'
      WHEN (string_to_array(projects.project_description, ','))[4] = 'Not_Rental' THEN 'N'
      ELSE 'Unknown'
    END
    ELSE NULL
  END rental_license_ind      
	     , CASE
    WHEN array_length(string_to_array(projects.project_description, ','), 1) >= 2
    THEN CASE
      WHEN (string_to_array(projects.project_description, ','))[4] = 'SH' THEN 'Y'
      WHEN (string_to_array(projects.project_description, ','))[4] = 'Not_SH' THEN 'N'
      ELSE 'Unknown'
    END
    ELSE NULL
  END  social_housing_ind,
COALESCE(
REPLACE(
REPLACE(
REPLACE(
REPLACE(case WHEN position(',' in projects.project_description) > 0 THEN
            substring(projects.project_description, 1, position(',' in projects.project_description) - 1)
        else NULL end ,'Unknown_Units','Unknown'),'SF_','Single-Family '),
        'SF','Single-Family'),
        'MF_','Multi-Family '),
        'Unknown') AS housing_type
        
	     , 
    CASE
    WHEN array_length(string_to_array(projects.project_description, ','), 1) >= 5
    THEN
        NULLIF(
            CASE
                WHEN regexp_replace((string_to_array(projects.project_description, ','))[5], E'^\\d+$', '') = ''
                THEN (string_to_array(projects.project_description, ','))[5]
                ELSE NULL
            END,
            ''
        )
    ELSE NULL
END AS estimated_housing
	     , NULL application_status
	     , authorizations.FILE_NUMBER
	     , 'ATS_EXTRACT_JOB' AS RECORD_CREATED_BY
	     , CURRENT_TIMESTAMP(0) AS RECORD_CREATED_DTTM
FROM
(SELECT
    prj.project_id,
    prj.project_name,
    COALESCE(REPLACE(REPLACE(prj.description, E'\n', ''), E'\r', ''), '') AS project_description,
    COALESCE(REPLACE(REPLACE(prj."LOCATION", E'\n', ''), E'\r', ''), '') AS project_location,
    psc."NAME" AS project_status,
    ptc."NAME" AS project_type,
    amfr.region_name,
    aso.subregional_office_name,
    aso2.secondary_office_name,
    prj.who_created,
    prj.when_created,
    prj.who_updated,
    prj.when_updated
FROM
    public.ats_projects prj
JOIN
    public.ats_proj_proj_type_code_xref pt ON prj.project_id = pt.project_id
JOIN
    public.ats_project_status_codes psc ON prj.project_status_code = psc.project_status_code
JOIN
    public.ats_project_type_codes ptc ON pt.project_type_code = ptc.project_type_code
JOIN
    public.ats_managing_fcbc_regions amfr ON prj.managing_fcbc_region_id = amfr.managing_fcbc_region_id
JOIN
    public.ats_subregional_offices aso ON prj.subregional_office_id = aso.subregional_office_id
LEFT JOIN
    public.ats_secondary_offices aso2 ON prj.secondary_office_id = aso2.secondary_office_id
WHERE
    prj.project_status_code = '1'
    AND pt.project_type_code = '681')projects
    JOIN
(SELECT
    auth.project_id,
    auth.authorization_id,
    aai.authorization_instrument_name AS permit_type,
    apa.agency_name AS business_area,
    auth.application_accepted_date AS accepted_date,
    auth.application_received_date AS received_date,
    auth.application_rejected_date AS rejected_date,
    auth.authorization_due_date,
    auth.amendment_renewal_date,
    auth.target_days,
    auth.tech_review_completion_date,
    auth.first_nation_start_date AS fn_consultn_start_date,
    auth.first_nation_completion_date AS fn_consultn_completion_date,
    auth.first_nation_comment AS fn_consultn_comment,
    auth.adjudication_date,
    aasc."NAME" AS auth_status,
    aasc.authorization_status_code AS authorization_status_code,
    aacrc."NAME" AS close_reason,
    auth.file_number
FROM
    ats_authorizations auth
LEFT JOIN
    public.ats_authorization_status_codes aasc ON auth.authorization_status_code = aasc.authorization_status_code
LEFT JOIN
    public.ats_athn_close_reason_codes aacrc ON auth.athn_close_reason_code = aacrc.athn_close_reason_code
LEFT JOIN
    public.ats_managing_fcbc_regions amfr ON auth.managing_fcbc_region_id = amfr.managing_fcbc_region_id
LEFT JOIN
    public.ats_regional_users aru ON auth.regional_user_id = aru.regional_user_id
LEFT JOIN
    public.ats_authorization_instruments aai ON auth.authorization_instrument_id = aai.authorization_instrument_id
JOIN
    public.ats_partner_agencies apa ON aai.partner_agency_id = apa.partner_agency_id
WHERE
    aasc.authorization_status_code = '1')authorizations
    ON projects.project_id = authorizations.project_id

    union all 

    SELECT 'FOR' ministrycode
	     , authorizations.business_area
	     , 'ATS' source_system_acronym
	     , authorizations.permit_type
	     , projects.project_id project_id
	     , authorizations.AUTHORIZATION_ID application_id
	     , projects.project_name
	     , projects.project_description
	     , projects.project_location
	     , NULL utm_easting
	     , NULL utm_northing
	     , authorizations.RECEIVED_DATE
	     , authorizations.ACCEPTED_DATE 
	     , authorizations.adjudication_date 
	     , authorizations.rejected_date
	     , authorizations.AMENDMENT_RENEWAL_DATE
	     , authorizations.TECH_REVIEW_COMPLETION_DATE
	     , authorizations.fn_consultn_start_date
	     , authorizations.fn_consultn_completion_date
	     , NULL fn_consultn_comment
	     , projects.region_name
	     ,   CASE
    WHEN array_length(string_to_array(projects.project_description, ','), 1) >= 2
    THEN CASE
      WHEN (string_to_array(projects.project_description, ','))[3] = 'Not_IL' THEN 'N'
      WHEN (string_to_array(projects.project_description, ','))[3] = 'IL' THEN 'Y'
      ELSE 'Unknown'
    END
    ELSE NULL
  END indigenous_led_ind
	     , CASE
    WHEN array_length(string_to_array(projects.project_description, ','), 1) >= 2
    THEN CASE
      WHEN (string_to_array(projects.project_description, ','))[4] = 'Rental' THEN 'Y'
      WHEN (string_to_array(projects.project_description, ','))[4] = 'Not_Rental' THEN 'N'
      ELSE 'Unknown'
    END
    ELSE NULL
  END rental_license_ind      
	     , CASE
    WHEN array_length(string_to_array(projects.project_description, ','), 1) >= 2
    THEN CASE
      WHEN (string_to_array(projects.project_description, ','))[4] = 'SH' THEN 'Y'
      WHEN (string_to_array(projects.project_description, ','))[4] = 'Not_SH' THEN 'N'
      ELSE 'Unknown'
    END
    ELSE NULL
  END  social_housing_ind,
COALESCE(
REPLACE(
REPLACE(
REPLACE(
REPLACE(case WHEN position(',' in projects.project_description) > 0 THEN
            substring(projects.project_description, 1, position(',' in projects.project_description) - 1)
        else NULL end ,'Unknown_Units','Unknown'),'SF_','Single-Family '),
        'SF','Single-Family'),
        'MF_','Multi-Family '),
        'Unknown') AS housing_type
        
	     , 
    CASE
    WHEN array_length(string_to_array(projects.project_description, ','), 1) >= 5
    THEN
        NULLIF(
            CASE
                WHEN regexp_replace((string_to_array(projects.project_description, ','))[5], E'^\\d+$', '') = ''
                THEN (string_to_array(projects.project_description, ','))[5]
                ELSE NULL
            END,
            ''
        )
    ELSE NULL
END AS estimated_housing
	     , NULL application_status
	     , authorizations.FILE_NUMBER
	     , 'ATS_EXTRACT_JOB' AS RECORD_CREATED_BY
	     , CURRENT_TIMESTAMP(0) AS 
FROM
(SELECT
    prj.project_id,
    prj.project_name,
    COALESCE(REPLACE(REPLACE(prj.description, E'\n', ''), E'\r', ''), '') AS project_description,
    COALESCE(REPLACE(REPLACE(prj."LOCATION", E'\n', ''), E'\r', ''), '') AS project_location,
    psc."NAME" AS project_status,
    ptc."NAME" AS project_type,
    amfr.region_name,
    aso.subregional_office_name,
    aso2.secondary_office_name,
    prj.who_created,
    prj.when_created,
    prj.who_updated,
    prj.when_updated
FROM
    public.ats_projects prj
JOIN
    public.ats_proj_proj_type_code_xref pt ON prj.project_id = pt.project_id
JOIN
    public.ats_project_status_codes psc ON prj.project_status_code = psc.project_status_code
JOIN
    public.ats_project_type_codes ptc ON pt.project_type_code = ptc.project_type_code
JOIN
    public.ats_managing_fcbc_regions amfr ON prj.managing_fcbc_region_id = amfr.managing_fcbc_region_id
JOIN
    public.ats_subregional_offices aso ON prj.subregional_office_id = aso.subregional_office_id
LEFT JOIN
    public.ats_secondary_offices aso2 ON prj.secondary_office_id = aso2.secondary_office_id
WHERE
     pt.project_type_code = '681')projects
    JOIN
(SELECT
    auth.project_id,
    auth.authorization_id,
    aai.authorization_instrument_name AS permit_type,
    apa.agency_name AS business_area,
    auth.application_accepted_date AS accepted_date,
    auth.application_received_date AS received_date,
    auth.application_rejected_date AS rejected_date,
    auth.authorization_due_date,
    auth.amendment_renewal_date,
    auth.target_days,
    auth.tech_review_completion_date,
    auth.first_nation_start_date AS fn_consultn_start_date,
    auth.first_nation_completion_date AS fn_consultn_completion_date,
    auth.first_nation_comment AS fn_consultn_comment,
    auth.adjudication_date,
    aasc."NAME" AS auth_status,
    aasc.authorization_status_code AS authorization_status_code,
    aacrc."NAME" AS close_reason,
    auth.file_number
FROM
    ats_authorizations auth
LEFT JOIN
    public.ats_authorization_status_codes aasc ON auth.authorization_status_code = aasc.authorization_status_code
LEFT JOIN
    public.ats_athn_close_reason_codes aacrc ON auth.athn_close_reason_code = aacrc.athn_close_reason_code
LEFT JOIN
    public.ats_managing_fcbc_regions amfr ON auth.managing_fcbc_region_id = amfr.managing_fcbc_region_id
LEFT JOIN
    public.ats_regional_users aru ON auth.regional_user_id = aru.regional_user_id
LEFT JOIN
    public.ats_authorization_instruments aai ON auth.authorization_instrument_id = aai.authorization_instrument_id
JOIN
    public.ats_partner_agencies apa ON aai.partner_agency_id = apa.partner_agency_id
    )authorizations
    ON projects.project_id = authorizations.project_id
    where 
 TO_TIMESTAMP(authorizations.adjudication_date, 'YYYY-MM-DD"T"HH24:MI:SS') > '2023-04-01 00:00:00'::timestamp
    OR TO_TIMESTAMP(authorizations.adjudication_date, 'YYYY-MM-DD"T"HH24:MI:SS.US') > '2023-04-01 00:00:00'::timestamp
