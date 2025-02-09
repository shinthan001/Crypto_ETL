CREATE DATABASE crypto_etl_db;

CREATE USER crypto_etl_user WITH ENCRYPTED PASSWORD 'crypto_etl';

GRANT ALL PRIVILEGES ON DATABASE crypto_etl_db TO crypto_etl_user;