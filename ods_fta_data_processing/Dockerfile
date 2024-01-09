# Make sure to have .profiles.yml under the project folder (e.g. fta_data_processing)

FROM ghcr.io/dbt-labs/dbt-postgres

RUN chmod -R g+rwX /usr

COPY .  /usr/app/dbt/

# usually would be: COPY profiles.yml /root/.dbt/profiles.yml


COPY profiles.yml /usr/app/dbt/.dbt/profiles.yml

COPY dbt_project.yml /app/dbt_project.yml

WORKDIR /usr/app/dbt/

# dbt is already initiated

CMD ["--help"]
