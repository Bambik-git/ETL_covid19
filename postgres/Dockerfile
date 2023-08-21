FROM postgres:13

ENV POSTGRES_USER postgres
ENV POSTGRES_PASSWORD 'postgres'

COPY pg_init_covid19.sql /docker-entrypoint-initdb.d/pg_init_covid19.sql


