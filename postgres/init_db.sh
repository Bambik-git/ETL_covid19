#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "postgres" <<-EOSQL
	CREATE DATABASE warehouse;
	CREATE DATABASE stage;
	GRANT ALL PRIVILEGES ON DATABASE warehouse, stage TO $POSTGRES_USER;
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "stage" <<-EOSQL
	CREATE TABLE covid19_stage (
	id serial,
	date_of_data date,
	iso_country varchar(5),
	json_data jsonb
	);
EOSQL
	
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "warehouse" <<-EOSQL
	CREATE TABLE covid19_warehouse (
	id serial,
	date_day date,
	confirmed int,
	confirmed_diff int,
	deaths int,
	deaths_diff int,
	recovered int,
	recovered_diff int,
	active int,
	active_diff int,
	fatality_rate float,
	country varchar(30),
	province varchar(50),
	region_lat float,
	region_long float
	);
EOSQL
