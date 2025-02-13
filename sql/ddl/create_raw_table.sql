-- 테이블 삭제
DROP TABLE IF EXISTS raw.chzzk_popular_lives;
DROP TABLE IF EXISTS raw.chzzk_popular_channels;
DROP TABLE IF EXISTS raw.soop_popular_lives;
DROP TABLE IF EXISTS raw.youtube_trending_game_videos;
DROP TABLE IF EXISTS raw.youtube_trending_latest_videos;
DROP TABLE IF EXISTS raw.youtube_trending_game_ranks;
DROP TABLE IF EXISTS raw.youtube_trending_latest_ranks;

-- 테이블 생성
CREATE TABLE IF NOT EXISTS raw.chzzk_popular_lives (
    id SERIAL,
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
    PRIMARY KEY (id, live_id, execution_ts)
);

CREATE TABLE IF NOT EXISTS raw.chzzk_popular_channels (
    id SERIAL,
    execution_ts TIMESTAMP NOT NULL,
    channel_id VARCHAR(255) NOT NULL,
    channel_name VARCHAR(255),
    followers_count INT,
    is_verified BOOLEAN,
    channel_type VARCHAR(255),
    channel_description TEXT,
    PRIMARY KEY (id, channel_id, execution_ts)
);

CREATE TABLE IF NOT EXISTS raw.soop_popular_lives (
    id SERIAL,
    execution_ts TIMESTAMP NOT NULL,
    live_id VARCHAR(255) NOT NULL,
    live_title VARCHAR(255),
    viewers_count INT,
    channel_id VARCHAR(255) NOT NULL,
    channel_name VARCHAR(255),
    category VARCHAR(255),
    is_adult BOOLEAN,
    PRIMARY KEY (id, live_id, execution_ts)
);

CREATE TABLE IF NOT EXISTS raw.youtube_trending_game_videos (
    id SERIAL,
    link VARCHAR(255) NOT NULL,
    title VARCHAR(255),
    views_count INT,
    uploaded_at TIMESTAMP,
    -- thumbsup_count INT,
    thumbnail_img VARCHAR(255),
    video_text TEXT,
    channel_link VARCHAR(255),
    channel_name VARCHAR(255),
    subscribers_count INT,
    channel_img VARCHAR(255),
    execution_ts TIMESTAMP NOT NULL,
    PRIMARY KEY (id, link, execution_ts)
);

CREATE TABLE IF NOT EXISTS raw.youtube_trending_latest_videos (
    id SERIAL,
    link VARCHAR(255) NOT NULL,
    title VARCHAR(255),
    views_count INT,
    uploaded_at TIMESTAMP,
    -- thumbsup_count INT,
    thumbnail_img VARCHAR(255),
    video_text TEXT,
    channel_link VARCHAR(255),
    channel_name VARCHAR(255),
    subscribers_count INT,
    channel_img VARCHAR(255),
    execution_ts TIMESTAMP NOT NULL,
    PRIMARY KEY (id, link, execution_ts)
);

CREATE TABLE IF NOT EXISTS raw.youtube_trending_game_ranks (
    id SERIAL,
    rk INT,
    link VARCHAR(255) NOT NULL,
    execution_ts TIMESTAMP NOT NULL,
    PRIMARY KEY (id, link, execution_ts)
);

CREATE TABLE IF NOT EXISTS raw.youtube_trending_latest_ranks (
    id SERIAL,
    rk INT,
    link VARCHAR(255) NOT NULL,
    execution_ts TIMESTAMP NOT NULL,
    PRIMARY KEY (id, link, execution_ts)
);
