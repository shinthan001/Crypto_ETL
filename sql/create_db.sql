-- run command 1 by 1
-- postgres database creation
CREATE DATABASE crypto_etl_db;

-- create username, password
CREATE USER crypto_etl_user WITH ENCRYPTED PASSWORD 'crypto_etl';

-- grant db privileges to user  
GRANT ALL ON DATABASE crypto_etl_db TO crypto_etl_user;