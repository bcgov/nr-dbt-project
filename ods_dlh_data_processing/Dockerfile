FROM ghcr.io/dbt-labs/dbt-postgres

RUN chmod -R g+rwX /usr

COPY .  /usr/app/dbt/

COPY profiles.yml /usr/app/dbt/.dbt/profiles.yml

COPY dbt_project.yml /app/dbt_project.yml

WORKDIR /usr/app/dbt/

# dbt is already initiated

CMD ["--help"]