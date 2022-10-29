#!/bin/bash 

airflow connections add 'postgres_stage' \
    --conn-type 'postgres' \
    --conn-login 'bmk' \
    --conn-password 'bmk' \
    --conn-host 'postgres_database' \
    --conn-port '5433' \
    --conn-schema 'stage'
    
airflow connections add 'postgres_warehouse' \
    --conn-type 'postgres' \
    --conn-login 'bmk' \
    --conn-password 'bmk' \
    --conn-host 'postgres_database' \
    --conn-port '5433' \
    --conn-schema 'warehouse'
    
airflow connections add 'telegram' \
    --conn-type 'http' 
    --conn-password '$TELEGRAM_API_TOKEN'
