DROP TABLE IF EXISTS soop.popular_live_with_rank;
DROP TABLE IF EXISTS soop.weekly_viewers_count;
DROP TABLE IF EXISTS soop.channel_name_latest;
DROP TABLE IF EXISTS soop.peak_channel;
DROP TABLE IF EXISTS soop.channel_live_viewers_count;
DROP TABLE IF EXISTS soop.channel_peak_live;


CREATE TABLE IF NOT EXISTS soop.popular_live_with_rank (
    execution_ts TIMESTAMP NOT NULL,
    live_id VARCHAR(255) NOT NULL,
    live_title VARCHAR(255),
    viewers_count INT,
    channel_id VARCHAR(255) NOT NULL,
    channel_name VARCHAR(255),
    category VARCHAR(255),
    is_adult BOOLEAN,
    rank INT
);
CREATE TABLE IF NOT EXISTS soop.weekly_viewers_count (
    weekday VARCHAR(20) NOT NULL,
    max_viewers_count INT,
    avg_viewers_count INT,
    total_viewers_count INT
);
CREATE TABLE IF NOT EXISTS soop.channel_name_latest (
    channel_id VARCHAR(255) PRIMARY KEY,
    channel_name VARCHAR(255),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS soop.peak_channel (
    date DATE,
    channel_name VARCHAR(255),
    peak_time VARCHAR(10),
    peak_viewers_count INT
);
CREATE TABLE IF NOT EXISTS soop.channel_live_viewers_count (
    channel_name VARCHAR(255),
    live_id VARCHAR(255),
    viewers_count INT,
    execution_ts TIMESTAMP
);
CREATE TABLE IF NOT EXISTS soop.channel_peak_live (
    channel_name VARCHAR(255),
    live_id VARCHAR(255),
    peak_viewers_count INT,
    peak_live_title VARCHAR(255),
    peak_category VARCHAR(255),
    peak_is_adult BOOLEAN,
    peak_ts TIMESTAMP
);