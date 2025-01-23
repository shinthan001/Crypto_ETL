
DROP TABLE  IF EXISTS coins;
CREATE TABLE coins (
    coin_id VARCHAR(255) NOT NULL,
    coin_symbol VARCHAR(255) NOT NULL,
    coin_name VARCHAR(255) NOT NULL,
    current_price DECIMAL(24,8) NOT NULL,
    market_cap DECIMAL(24,2) NOT NULL,
    market_cap_rank INT NOT NULL,
    total_supply DECIMAL(24,2) NOT NULL,
    circulating_supply DECIMAL(24,2) NOT NULL,
    all_time_high DECIMAL(24,8) NOT NULL,
    all_time_high_date TIMESTAMP NOT NULL,
    all_time_low DECIMAL(24,8) NOT NULL,
    all_time_low_date TIMESTAMP NOT NULL,
    last_updated VARCHAR(255) NOT NULL,
    PRIMARY KEY(coin_id)
);


DROP TABLE  IF EXISTS candlesticks;
CREATE TABLE candlesticks (
    candlestick_id SERIAL NOT NULL,
    coin_id VARCHAR(255) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    open DECIMAL(24,8) NOT NULL,
	high DECIMAL(24,8) NOT NULL,
	low DECIMAL(24,8) NOT NULL,
    close DECIMAL(24,8) NOT NULL,
    PRIMARY KEY(candlestick_id)
);

DROP TABLE IF EXISTS time;
CREATE TABLE time (
    timestamp TIMESTAMP NOT NULL,
    hour INT NOT NULL,
    day INT NOT NULL,
    month INT NOT NULL,
    year INT NOT NULL,
    weekday INT NOT NULL,
    PRIMARY KEY(timestamp)
);

DROP TABLE IF EXISTS news;
CREATE TABLE news (
	news_id SERIAL NOT NULL,
    coin_id VARCHAR(255) NOT NULL,
    month INT NOT NULL,
    year INT NOT NULL,
    agg_polarity_score DECIMAL(24,8) NOT NULL,
    number_of_articles INT NOT NULL,
    number_of_sources INT NOT NULL,
    PRIMARY KEY(news_id)
);
