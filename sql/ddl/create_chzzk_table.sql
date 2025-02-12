DROP TABLE IF EXISTS chzzk.popular_live_with_rank;
DROP TABLE IF EXISTS chzzk.weekly_viewers_count;
DROP TABLE IF EXISTS chzzk.channel_name_latest;
DROP TABLE IF EXISTS chzzk.peak_channel;
DROP TABLE IF EXISTS chzzk.live_time_to_peak;
DROP TABLE IF EXISTS chzzk.channel_live_viewers_count;
DROP TABLE IF EXISTS chzzk.channel_peak_live;


CREATE TABLE IF NOT EXISTS chzzk.popular_live_with_rank (
    execution_ts TIMESTAMP NOT NULL,
    live_id VARCHAR(255) NOT NULL,
    live_title VARCHAR(255),
    viewers_count INT,
    channel_id VARCHAR(255) NOT NULL,
    category_type VARCHAR(255),
    category_id VARCHAR(255),
    category_name VARCHAR(255),
    is_adult BOOLEAN,
    open_ts TIMESTAMP,
    rank INT
);
CREATE TABLE IF NOT EXISTS chzzk.weekly_viewers_count (
    weekday VARCHAR(20) NOT NULL,
    max_viewers_count INT,
    avg_viewers_count INT,
    total_viewers_count INT
);
CREATE TABLE IF NOT EXISTS chzzk.channel_name_latest (
    channel_id VARCHAR(255) PRIMARY KEY,
    channel_name VARCHAR(255),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS chzzk.peak_channel (
    date DATE,
    channel_name VARCHAR(255),
    peak_time VARCHAR(10),
    peak_viewers_count INT
);
CREATE TABLE IF NOT EXISTS chzzk.live_time_to_peak (
    live_id VARCHAR(255) PRIMARY KEY,
    channel_name VARCHAR(255),
    time_to_peak INTERVAL
);
CREATE TABLE IF NOT EXISTS chzzk.channel_live_viewers_count (
    channel_name VARCHAR(255),
    live_id VARCHAR(255),
    viewers_count INT,
    execution_ts TIMESTAMP
);
CREATE TABLE IF NOT EXISTS chzzk.channel_peak_live (
    channel_name VARCHAR(255),
    live_id VARCHAR(255),
    peak_viewers_count INT,
    peak_live_title VARCHAR(255),
    peak_category_id VARCHAR(255),
    peak_is_adult BOOLEAN,
    peak_ts TIMESTAMP
);