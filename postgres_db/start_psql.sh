docker run -d --rm \
--publish 5432:5432 \
-e POSTGRES_USER=postgres \
-e POSTGRES_PASSWORD=secret \
--name acosom_psql \
postgres:15-alpine

sleep 5

docker exec acosom_psql psql -d postgres -U postgres -c 'create database acosom_assessment;'

docker exec acosom_psql psql -d acosom_assessment -U postgres -c 'create table aggregated_sales (seller_id VARCHAR(10), window_start TIMESTAMP, window_end TIMESTAMP, aggregated_amount INTEGER);'