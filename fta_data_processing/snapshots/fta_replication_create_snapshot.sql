{% snapshot resource_road_tenure_hist %}

    {{
        config(
          target_schema='rrs_replication_hist',
          strategy='timestamp',
          unique_key='resource_road_tenure_guid',
          updated_at='update_date',
        )
    }}

SELECT * FROM app_rrs1.resource_road_tenure

{% endsnapshot %}

{% snapshot road_appl_map_feature_hist %}

    {{
        config(
          target_schema='rrs_replication_hist',
          strategy='timestamp',
          unique_key='road_appl_map_feature_guid',
          updated_at='update_date',
        )
    }}

SELECT * FROM app_rrs1.road_appl_map_feature

{% endsnapshot %}

{% snapshot road_application_hist %}

    {{
        config(
          target_schema='rrs_replication_hist',
          strategy='timestamp',
          unique_key='road_application_guid',
          updated_at='update_date',
        )
    }}

SELECT * FROM app_rrs1.road_application

{% endsnapshot %}

{% snapshot road_application_status_code_hist %}

    {{
        config(
          target_schema='rrs_replication_hist',
          strategy='timestamp',
          unique_key='road_application_status_code',
          updated_at='update_date',
        )
    }}

SELECT * FROM app_rrs1.road_application_status_code

{% endsnapshot %}

{% snapshot road_feature_class_sdw_hist %}

    {{
        config(
          target_schema='rrs_replication_hist',
          strategy='timestamp',
          unique_key='road_feature_class_sdw_guid',
          updated_at='update_date',
        )
    }}

SELECT * FROM app_rrs1.road_feature_class_sdw

{% endsnapshot %}

{% snapshot road_org_unit_sdw_hist %}

    {{
        config(
          target_schema='rrs_replication_hist',
          strategy='timestamp',
          unique_key='road_org_unit_sdw_guid',
          updated_at='update_date',
        )
    }}

SELECT * FROM app_rrs1.road_org_unit_sdw

{% endsnapshot %}

{% snapshot road_section_hist %}

    {{
        config(
          target_schema='rrs_replication_hist',
          strategy='timestamp',
          unique_key='road_section_guid',
          updated_at='update_date',
        )
    }}

SELECT * FROM app_rrs1.road_section

{% endsnapshot %}

{% snapshot road_section_status_code_hist %}

    {{
        config(
          target_schema='rrs_replication_hist',
          strategy='timestamp',
          unique_key='road_section_status_code',
          updated_at='update_date',
        )
    }}

SELECT * FROM app_rrs1.road_section_status_code

{% endsnapshot %}

{% snapshot road_submission_hist %}

    {{
        config(
          target_schema='rrs_replication_hist',
          strategy='timestamp',
          unique_key='road_submission_guid',
          updated_at='update_date',
        )
    }}

SELECT * FROM app_rrs1.road_submission

{% endsnapshot %}

{% snapshot road_tenure_status_code_hist %}

    {{
        config(
          target_schema='rrs_replication_hist',
          strategy='timestamp',
          unique_key='road_tenure_status_code',
          updated_at='update_date',
        )
    }}

SELECT * FROM app_rrs1.road_tenure_status_code

{% endsnapshot %}

{% snapshot road_tenure_type_code_hist %}

    {{
        config(
          target_schema='rrs_replication_hist',
          strategy='timestamp',
          unique_key='road_tenure_type_code',
          updated_at='update_date',
        )
    }}

SELECT * FROM app_rrs1.road_tenure_type_code

{% endsnapshot %}

{% snapshot submission_status_code_hist %}

    {{
        config(
          target_schema='rrs_replication_hist',
          strategy='timestamp',
          unique_key='submission_status_code',
          updated_at='update_date',
        )
    }}

SELECT * FROM app_rrs1.submission_status_code

{% endsnapshot %}