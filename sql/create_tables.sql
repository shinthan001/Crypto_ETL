
DROP TABLE  IF EXISTS coins;
CREATE TABLE coins (
    coin_id VARCHAR(255) NOT NULL,
    coin_symbol VARCHAR(255) NOT NULL,
    coin_name VARCHAR(255) NOT NULL,
    PRIMARY KEY(coin_id)
);

DROP TABLE  IF EXISTS contracts;
CREATE TABLE contracts (
    contract_id SERIAL NOT NULL,
    contract_addr VARCHAR(255) NOT NULL,
    coin_id VARCHAR(255) NOT NULL,
    contract_platform VARCHAR(255) NOT NULL,
    PRIMARY KEY(contract_id)
);

DROP TABLE  IF EXISTS market_assets;
CREATE TABLE market_assets (
    market_id VARCHAR(255) NOT NULL,
    coin_id VARCHAR(255) NOT NULL,
    last_updated VARCHAR(255) NOT NULL,
    market_cap DECIMAL(24,2) NOT NULL,
    market_cap_rank INT NOT NULL,
    current_price DECIMAL(24,8) NOT NULL,
    total_supply DECIMAL(24,2) NOT NULL,
    max_supply DECIMAL(24,2) NOT NULL,
    circulation_supply DECIMAL(24,2) NOT NULL,
    all_time_high DECIMAL(24,8) NOT NULL,
    all_time_high_date TIMESTAMP NOT NULL,
    all_time_low DECIMAL(24,8) NOT NULL,
    all_time_low_date TIMESTAMP NOT NULL,
    PRIMARY KEY(market_id)
);



DROP TABLE  IF EXISTS candlesticks;
CREATE TABLE candlesticks (
    candlestick_id SERIAL NOT NULL,
    coin_id VARCHAR(255) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    open_price DECIMAL(24,8) NOT NULL,
	high_price DECIMAL(24,8) NOT NULL,
	low_price DECIMAL(24,8) NOT NULL,
    close_price DECIMAL(24,8) NOT NULL,
    article_id INT NOT NULL,
    PRIMARY KEY(candlestick_id)
);

DROP TABLE IF EXISTS time;
CREATE TABLE time (
    timestamp TIMESTAMP NOT NULL,
    hour TIMESTAMP NOT NULL,
    day TIMESTAMP NOT NULL,
    month TIMESTAMP NOT NULL,
    year TIMESTAMP NOT NULL,
    weekday TIMESTAMP NOT NULL,
    PRIMARY KEY(timestamp)
);

DROP TABLE IF EXISTS news_platforms;
CREATE TABLE news_platforms (
    news_platform_id SERIAL NOT NULL,
    news_platforms_name VARCHAR(255) NOT NULL,
    PRIMARY KEY(news_platform_id)
);

DROP TABLE IF EXISTS articles;
CREATE TABLE articles (
    article_id SERIAL NOT NULL,
    coin_symbol VARCHAR(255) NOT NULL,
	news_platform_id VARCHAR(255) NOT NULL,
	author VARCHAR(255) NOT NULL,
	title VARCHAR(255) NOT NULL,
	description VARCHAR(255) NOT NULL,
	content VARCHAR(255) NOT NULL,
	published_date VARCHAR(255) NOT NULL,
	polarity_score VARCHAR(255) NOT NULL,
	subjectivity_score VARCHAR(255) NOT NULL,
    PRIMARY KEY(article_id)
);