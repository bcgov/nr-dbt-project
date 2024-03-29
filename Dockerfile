FROM ghcr.io/dbt-labs/dbt-postgres

RUN chmod -R g+rwX /usr

COPY /ods_dlh_data_processing /usr/app/dbt/

# usually would be: COPY profiles.yml /root/.dbt/profiles.yml

COPY ods_dlh_data_processing/profiles.yml /usr/app/dbt/.dbt/profiles.yml

COPY ods_dlh_data_processing/dbt_project.yml /app/dbt_project.yml

WORKDIR /usr/app/dbt/

# dbt is already initiated

CMD ["--help"]
