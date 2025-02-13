DROP TABLE IF EXISTS youtube.game_video_with_rank;
DROP TABLE IF EXISTS youtube.game_weekly_views_count;
DROP TABLE IF EXISTS youtube.game_channel_name_latest;
DROP TABLE IF EXISTS youtube.game_channel_video;
DROP TABLE IF EXISTS youtube.game_channel_live_time_to_trending;


CREATE TABLE IF NOT EXISTS youtube.game_video_with_rank (
    rank INT,
    link VARCHAR(255) NOT NULL,
    title VARCHAR(255),
    views_count INT,
    uploaded_at TIMESTAMP,
    thumbnail_img VARCHAR(255),
    video_text TEXT,
    channel_link VARCHAR(255),
    channel_name VARCHAR(255),
    subscribers_count INT,
    channel_img VARCHAR(255),
    execution_ts TIMESTAMP NOT NULL,
    PRIMARY KEY (link, execution_ts)
);
CREATE TABLE IF NOT EXISTS youtube.game_weekly_views_count (
    weekday VARCHAR(20) NOT NULL,
    max_views_count INT,
    min_views_count INT,
    avg_views_count INT,
    PRIMARY KEY (weekday)
);
CREATE TABLE IF NOT EXISTS youtube.game_channel_name_latest (
    channel_link VARCHAR(255) PRIMARY KEY,
    channel_name VARCHAR(255),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS youtube.game_channel_video (
    channel_name VARCHAR(255),
    video_link VARCHAR(255),
    views_count INT,
    rank INT,
    subscribers_count INT,
    execution_ts TIMESTAMP,
    PRIMARY KEY (channel_name, video_link, execution_ts)
);
CREATE TABLE IF NOT EXISTS youtube.game_channel_live_time_to_trending (
    channel_name VARCHAR(255),
    video_link VARCHAR(255),
    time_to_trending INTERVAL,
    PRIMARY KEY (channel_name, video_link)
);