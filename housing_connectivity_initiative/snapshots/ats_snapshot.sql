{% snapshot ats_snapshot %}

    {{
        config(
          target_schema='ats_cur_schema',
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
  FROM public_housing_raw_schema.ats_extract raw_data
)

SELECT *
FROM scd_data

{% endsnapshot %}