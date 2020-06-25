#!/usr/bin/env bash

airflow connections --add \
  --conn_id 'prod_redshift' \
  --conn_type 'Postgres' \
  --conn_uri 'REPLACE_ME' \
  --conn_schema 'REPLACE_ME' \
  --conn_login 'REPLACE_ME' \
  --conn_password 'REPLACE_ME' \
  --conn_port '5439'

airflow connections --add \
  --conn_id 'prod_s3' \
  --conn_type 's3' \
  --conn_extra '{'aws_access_key_id':'REPLACE_ME', 'aws_secret_access_key': 'REPLACE_ME'}'
