CREATE DATABASE crypto_etl_db;

CREATE USER crypto_etl_user WITH ENCRYPTED PASSWORD 'crypto_etl';

REVOKE ALL PRIVILEGES ON DATABASE crypto_etl_db FROM crypto_etl_user;
