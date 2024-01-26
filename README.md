# nr-dbt-project
This repository contains Data build tools(DBT) data models and other artifacts

#  Use of GHCR
Container images are built automatically and pushed to the GHCR any time there is a push or PR to the **main** branch. Images are named according to the file path and tagged with the branch name. Use the image name in an Airflow DAG to create a job using the DBT container. See Airflow example here: [dbt_example.py](https://github.com/bcgov/nr-airflow/blob/e45c83f933d1f96e479a36a3e765dabd61e1ff2e/dags/dbt_example.py#L19)

Usage example: 
```sh
docker pull ghcr.io/bcgov/nr-dbt-project-ods-dlh:main
```

Alternatively, there is this manual workflow: 
```sh
docker build -t image-registry.apps.emerald.devops.gov.bc.ca/a1b9b0-dev/<image_name>:<tag> -f <Dockerfile_Name> .
```
```sh
docker push image-registry.apps.emerald.devops.gov.bc.ca/a1b9b0-dev/<image_name>:<tag>
```

