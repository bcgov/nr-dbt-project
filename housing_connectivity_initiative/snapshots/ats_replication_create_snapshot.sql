{% snapshot ats_athn_close_reason_codes_hist %}

    {{
        config(
          target_schema='ats_replication_hist',
          strategy='check',
          unique_key='athn_close_reason_code',
          check_cols='all',
        )
    }}

SELECT * FROM ats_replication.ats_athn_close_reason_codes

{% endsnapshot %}

{% snapshot ats_authorization_instruments_hist %}

    {{
        config(
          target_schema='ats_replication_hist',
          strategy='check',
          unique_key='authorization_instrument_id',
          check_cols='all',
        )
    }}

SELECT * FROM ats_replication.ats_authorization_instruments

{% endsnapshot %}

{% snapshot ats_authorization_status_codes_hist %}

    {{
        config(
          target_schema='ats_replication_hist',
          strategy='check',
          unique_key='authorization_status_code',
          check_cols='all',
        )
    }}

SELECT * FROM ats_replication.ats_authorization_status_codes

{% endsnapshot %}

{% snapshot ats_authorizations_hist %}

    {{
        config(
          target_schema='ats_replication_hist',
          strategy='check',
          unique_key='authorization_id',
          check_cols='all',
        )
    }}

SELECT * FROM ats_replication.ats_authorizations

{% endsnapshot %}

{% snapshot ats_managing_fcbc_regions_hist %}

    {{
        config(
          target_schema='ats_replication_hist',
          strategy='check',
          unique_key='managing_fcbc_region_id',
          check_cols='all',
        )
    }}

SELECT * FROM ats_replication.ats_managing_fcbc_regions

{% endsnapshot %}

{% snapshot ats_partner_agencies_hist %}

    {{
        config(
          target_schema='ats_replication_hist',
          strategy='check',
          unique_key='partner_agency_id',
          check_cols='all',
        )
    }}

SELECT * FROM ats_replication.ats_partner_agencies

{% endsnapshot %}

{% snapshot ats_proj_proj_type_code_xref_hist %}

    {{
        config(
          target_schema='ats_replication_hist',
          strategy='check',
          unique_key='pi_hash_key',
          check_cols='all',
        )
    }}

WITH scd_data AS (
  SELECT
    raw_data.*,
    -- Combine multiple keys into a single composite_key
    md5(coalesce(cast(project_id					 		as varchar),'~') || '|'||  
			coalesce(cast(project_type_code					 		as varchar),'~')) as pi_hash_key
  FROM ats_replication.ats_proj_proj_type_code_xref raw_data
)

SELECT *
FROM scd_data


{% endsnapshot %}

{% snapshot ats_project_status_codes_hist %}

    {{
        config(
          target_schema='ats_replication_hist',
          strategy='check',
          unique_key='project_status_code',
          check_cols='all',
        )
    }}

SELECT * FROM ats_replication.ats_project_status_codes

{% endsnapshot %}

{% snapshot ats_project_type_codes_hist %}

    {{
        config(
          target_schema='ats_replication_hist',
          strategy='check',
          unique_key='project_type_code',
          check_cols='all',
        )
    }}

SELECT * FROM ats_replication.ats_project_type_codes

{% endsnapshot %}

{% snapshot ats_projects_hist %}

    {{
        config(
          target_schema='ats_replication_hist',
          strategy='check',
          unique_key='project_id',
          check_cols='all',
        )
    }}

SELECT * FROM ats_replication.ats_projects

{% endsnapshot %}

{% snapshot ats_regional_users_hist %}

    {{
        config(
          target_schema='ats_replication_hist',
          strategy='check',
          unique_key='regional_user_id',
          check_cols='all',
        )
    }}

SELECT * FROM ats_replication.ats_regional_users

{% endsnapshot %}

{% snapshot ats_secondary_offices_hist %}

    {{
        config(
          target_schema='ats_replication_hist',
          strategy='check',
          unique_key='secondary_office_id',
          check_cols='all',
        )
    }}

SELECT * FROM ats_replication.ats_secondary_offices

{% endsnapshot %}

{% snapshot ats_subregional_offices_hist %}

    {{
        config(
          target_schema='ats_replication_hist',
          strategy='check',
          unique_key='subregional_office_id',
          check_cols='all',
        )
    }}

SELECT * FROM ats_replication.ats_subregional_offices

{% endsnapshot %}