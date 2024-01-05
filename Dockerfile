# Make sure to have .profiles.yml under the project folder (e.g. fta_data_processing)

FROM ghcr.io/dbt-labs/dbt-postgres

RUN chmod -R g+rwX /usr

COPY /fta_data_processing /usr/app/dbt/

# usually would be: COPY profiles.yml /root/.dbt/profiles.yml

COPY fta_data_processing/profiles.yml /usr/app/dbt/.dbt/profiles.yml

COPY fta_data_processing/dbt_project.yml /app/dbt_project.yml

WORKDIR /usr/app/dbt/

# dbt is already initiated

CMD ["--help"]
