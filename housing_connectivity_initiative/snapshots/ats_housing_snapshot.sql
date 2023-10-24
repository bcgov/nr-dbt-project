{% snapshot ats_housing_hist %}

    {{
        config(
          target_schema='dlh_housing_cur',
          strategy='check',
          unique_key='pi_hash_key',
          check_cols='all',
        )
    }}

    WITH scd_data AS (
  SELECT
    raw_data.*,
    -- Combine multiple keys into a single composite_key
    md5(coalesce(cast(ministrycode					 		as varchar),'~') || '|'|| 
			coalesce(cast(business_area 				  	as varchar),'~') || '|'|| 
			coalesce(cast(permit_type		             		as varchar),'~') || '|'|| 
			coalesce(cast(project_id 	                  	as varchar),'~') || '|'|| 
			coalesce(cast(application_id		         		as varchar),'~') || '|'|| 
			coalesce(cast(source_system_acronym					 		as varchar),'~')) as pi_hash_key
  FROM dlh_housing_raw.ats_housing_extract raw_data
)

SELECT *
FROM scd_data

{% endsnapshot %}