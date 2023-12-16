# Make sure to have .profiles.yml under the folder you create this in

FROM python:3.11.4-slim-buster

RUN apt-get update \
    && apt-get -y install libpq-dev gcc 

COPY requirements.txt requirements.txt

RUN pip3 install -r requirements.txt

COPY /fta_data_processing /usr/app/dbt/

COPY profiles.yml /root/.dbt/profiles.yml

COPY dbt_project.yml /app/dbt_project.yml

WORKDIR /usr/app/dbt/

CMD ["dbt", "snapshot"]


