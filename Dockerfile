##
#  Generic dockerfile for dbt image building.
#  See README for operational details
##

# Top level build args
ARG build_for=linux/amd64

##
# base image (abstract)
##
# Please do not upgrade beyond python3.10.7 currently as dbt-spark does not support
# 3.11py and images do not get made properly
FROM --platform=$build_for python:3.10.7-slim-bullseye as base

# N.B. The refs updated automagically every release via bumpversion
# N.B. dbt-postgres is currently found in the core codebase so a value of dbt-core@<some_version> is correct

ARG dbt_core_ref=dbt-core@v1.8.0a1
ARG dbt_postgres_ref=dbt-core@v1.8.0a1
ARG dbt_redshift_ref=dbt-redshift@v1.8.0a1
ARG dbt_bigquery_ref=dbt-bigquery@v1.8.0a1
ARG dbt_snowflake_ref=dbt-snowflake@v1.8.0a1
ARG dbt_spark_ref=dbt-spark@v1.8.0a1
# special case args
ARG dbt_spark_version=all
ARG dbt_third_party

# System setup
RUN apt-get update \
  && apt-get dist-upgrade -y \
  && apt-get install -y --no-install-recommends \
    git \
    ssh-client \
    software-properties-common \
    make \
    build-essential \
    ca-certificates \
    libpq-dev \
  && apt-get clean \
  && rm -rf \
    /var/lib/apt/lists/* \
    /tmp/* \
    /var/tmp/*

# Env vars
ENV PYTHONIOENCODING=utf-8
ENV LANG=C.UTF-8

# Update python
RUN python -m pip install --upgrade pip setuptools wheel --no-cache-dir

# Set docker basics

ENTRYPOINT ["dbt"]

##
# dbt-postgres
##
FROM base as dbt-postgres
RUN python -m pip install --no-cache-dir "git+https://github.com/dbt-labs/${dbt_postgres_ref}#egg=dbt-postgres&subdirectory=plugins/postgres"

