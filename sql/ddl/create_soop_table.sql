-- 모든 테이블 삭제
DROP TABLE IF EXISTS soop.soop_hourly_viewers_count;
DROP TABLE IF EXISTS soop.soop_daily_viewers_count;
DROP TABLE IF EXISTS soop.soop_weekly_viewers_count;
DROP TABLE IF EXISTS soop.soop_top_10_live_hourly_viewers_count;
DROP TABLE IF EXISTS soop.soop_hourly_peak_channel;
DROP TABLE IF EXISTS soop.soop_daily_peak_channel;
DROP TABLE IF EXISTS soop.soop_channel_peak_live;
DROP TABLE IF EXISTS soop.soop_channel_viewers_count;
DROP TABLE IF EXISTS soop.soop_category_hourly_viewers_count;
DROP TABLE IF EXISTS soop.soop_category_daily_viewers_count;

-- 테이블 생성
CREATE TABLE IF NOT EXISTS soop.soop_hourly_viewers_count (
    id SERIAL,
    ts TIMESTAMP NOT NULL,
    max_viewers_count INT,
    avg_viewers_count INT,
    total_viewers_count INT,
    PRIMARY KEY (id, ts)
);

CREATE TABLE IF NOT EXISTS soop.soop_daily_viewers_count (
    id SERIAL,
    date DATE NOT NULL,
    max_viewers_count INT,
    avg_viewers_count INT,
    total_viewers_count INT,
    PRIMARY KEY (id, date)
);

CREATE TABLE IF NOT EXISTS soop.soop_weekly_viewers_count (
    id SERIAL,
    weekday VARCHAR(20) NOT NULL,
    max_viewers_count INT,
    avg_viewers_count INT,
    total_viewers_count INT,
    PRIMARY KEY (id, weekday)
);

CREATE TABLE IF NOT EXISTS soop.soop_top_10_live_hourly_viewers_count (
    id SERIAL,
    live_id VARCHAR(255) NOT NULL,
    ts TIMESTAMP NOT NULL,
    rank INT,
    viewers_count INT,
    is_adult BOOLEAN,
    PRIMARY KEY (id, live_id, ts)
);

CREATE TABLE IF NOT EXISTS soop.soop_hourly_peak_channel (
    id SERIAL,
    ts TIMESTAMP NOT NULL,
    channel_id VARCHAR(255),
    channel_name VARCHAR(255),
    peak_viewers_count INT,
    PRIMARY KEY (id, ts)
);

CREATE TABLE IF NOT EXISTS soop.soop_daily_peak_channel (
    id SERIAL,
    date DATE NOT NULL,
    channel_id VARCHAR(255),
    channel_name VARCHAR(255),
    peak_viewers_count INT,
    PRIMARY KEY (id, date)
);

CREATE TABLE IF NOT EXISTS soop.soop_channel_peak_live (
    id SERIAL,
    channel_id VARCHAR(255) NOT NULL,
    live_id VARCHAR(255) NOT NULL,
    channel_name VARCHAR(255),
    peak_ts TIMESTAMP,
    peak_viewers_count INT,
    live_title VARCHAR(255),
    category_id VARCHAR(255),
    is_adult BOOLEAN,
    PRIMARY KEY (id, channel_id, live_id)
);

CREATE TABLE IF NOT EXISTS soop.soop_channel_viewers_count (
    id SERIAL,
    channel_id VARCHAR(255) NOT NULL,
    live_id VARCHAR(255) NOT NULL,
    channel_name VARCHAR(255),
    max_viewers_count INT,
    avg_viewers_count INT,
    PRIMARY KEY (id, channel_id, live_id)
);

CREATE TABLE IF NOT EXISTS soop.soop_category_hourly_viewers_count (
    id SERIAL,
    category_id VARCHAR(255) NOT NULL,
    ts TIMESTAMP NOT NULL,
    max_viewers_count INT,
    avg_viewers_count INT,
    total_viewers_count INT,
    PRIMARY KEY (id, category_id, ts)
);

CREATE TABLE IF NOT EXISTS soop.soop_category_daily_viewers_count (
    id SERIAL,
    category_id VARCHAR(255) NOT NULL,
    date TIMESTAMP NOT NULL,
    max_viewers_count INT,
    avg_viewers_count INT,
    total_viewers_count INT,
    PRIMARY KEY (id, category_id, date)
);