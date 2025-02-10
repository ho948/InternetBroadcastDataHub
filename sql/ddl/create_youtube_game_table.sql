-- 테이블 삭제
DROP TABLE IF EXISTS youtube.youtube_trending_game_video_with_rank;
DROP TABLE IF EXISTS youtube.youtube_trending_game_hourly_all_videos_views_count;
DROP TABLE IF EXISTS youtube.youtube_trending_game_hourly_top_10_videos_views_count;
DROP TABLE IF EXISTS youtube.youtube_trending_game_hourly_top_1_video_views_count;
DROP TABLE IF EXISTS youtube.youtube_trending_game_daily_all_videos_views_count;
DROP TABLE IF EXISTS youtube.youtube_trending_game_daily_top_10_videos_views_count;
DROP TABLE IF EXISTS youtube.youtube_trending_game_daily_top_1_videos_views_count;
DROP TABLE IF EXISTS youtube.youtube_trending_game_weekly_all_videos_views_count;
DROP TABLE IF EXISTS youtube.youtube_trending_game_weekly_top_10_videos_views_count;
DROP TABLE IF EXISTS youtube.youtube_trending_game_weekly_top_1_videos_views_count;
DROP TABLE IF EXISTS youtube.youtube_trending_game_hourly_all_channels_subscribers_count;
DROP TABLE IF EXISTS youtube.youtube_trending_game_hourly_top_10_channels_subscribers_count;
DROP TABLE IF EXISTS youtube.youtube_trending_game_hourly_top_1_channel_subscribers_count;
DROP TABLE IF EXISTS youtube.youtube_trending_game_daily_all_channels_subscribers_count;
DROP TABLE IF EXISTS youtube.youtube_trending_game_daily_top_10_channels_subscribers_count;
DROP TABLE IF EXISTS youtube.youtube_trending_game_daily_top_1_channels_subscribers_count;
DROP TABLE IF EXISTS youtube.youtube_trending_game_weekly_all_channels_subscribers_count;
DROP TABLE IF EXISTS youtube.youtube_trending_game_weekly_top_10_channels_subscribers_count;
DROP TABLE IF EXISTS youtube.youtube_trending_game_weekly_top_1_channels_subscribers_count;
DROP TABLE IF EXISTS youtube.youtube_trending_game_top_1_video_views_count;
DROP TABLE IF EXISTS youtube.youtube_trending_game_all_video_time_to_trending;
DROP TABLE IF EXISTS youtube.youtube_trending_game_top_10_video_time_to_trending;
DROP TABLE IF EXISTS youtube.youtube_trending_game_top_1_video_time_to_trending;
DROP TABLE IF EXISTS youtube.youtube_trending_game_channel_subscribers_count;
DROP TABLE IF EXISTS youtube.youtube_trending_game_channel_trending_ts;
DROP TABLE IF EXISTS youtube.youtube_trending_game_channel_videos_views_count;
DROP TABLE IF EXISTS youtube.youtube_trending_game_channel_videos_time_to_trending;
DROP TABLE IF EXISTS youtube.youtube_trending_game_channel_videos_rank;
DROP TABLE IF EXISTS youtube.youtube_trending_game_channel_trending_count;

-- 순위가 포함된 쿼리용 테이블 작성
CREATE TABLE IF NOT EXISTS youtube.youtube_trending_game_video_with_rank (
    id SERIAL,
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
    PRIMARY KEY (id, link, execution_ts)
);

-- 분석용 테이블 생성
-- 시간대별 모든 게임 인기 급상승 영상들의 최고/최저/평균 조회수 테이블 생성
CREATE TABLE IF NOT EXISTS youtube.youtube_trending_game_hourly_all_videos_views_count (
    id SERIAL,
    ts TIMESTAMP NOT NULL,
    max_views_count INT,
    min_views_count INT,
    avg_views_count INT,
    PRIMARY KEY (id, ts)
);
-- 시간대별 게임 Top 10 인기 급상승 영상들의 최고/최저/평균 조회수 테이블 생성
CREATE TABLE IF NOT EXISTS youtube.youtube_trending_game_hourly_top_10_videos_views_count (
    id SERIAL,
    ts TIMESTAMP NOT NULL,
    max_views_count INT,
    min_views_count INT,
    avg_views_count INT,
    PRIMARY KEY (id, ts)
);
-- 시간대별 게임 Top 1 인기 급상승 영상 조회수 테이블 생성
CREATE TABLE IF NOT EXISTS youtube.youtube_trending_game_hourly_top_1_video_views_count (
    id SERIAL,
    ts TIMESTAMP NOT NULL,
    views_count INT,
    PRIMARY KEY (id, ts)
);
-- 일별 게임 모든 인기 급상승 영상들의 최고/최저/평균 조회수 테이블 생성
CREATE TABLE IF NOT EXISTS youtube.youtube_trending_game_daily_all_videos_views_count (
    id SERIAL,
    date DATE NOT NULL,
    max_views_count INT,
    min_views_count INT,
    avg_views_count INT,
    PRIMARY KEY (id, date)
);
-- 일별 게임 Top 10 인기 급상승 영상들의 최고/최저/평균 조회수 테이블 생성
CREATE TABLE IF NOT EXISTS youtube.youtube_trending_game_daily_top_10_videos_views_count (
    id SERIAL,
    date DATE NOT NULL,
    max_views_count INT,
    min_views_count INT,
    avg_views_count INT,
    PRIMARY KEY (id, date)
);
-- 일별 게임 Top 1 인기 급상승 영상들의 최고/최저/평균 조회수 테이블 생성
CREATE TABLE IF NOT EXISTS youtube.youtube_trending_game_daily_top_1_videos_views_count (
    id SERIAL,
    date DATE NOT NULL,
    max_views_count INT,
    min_views_count INT,
    avg_views_count INT,
    PRIMARY KEY (id, date)
);
-- 요일별 게임 모든 인기 급상승 영상들의 최고/최저/평균 조회수 테이블 생성
CREATE TABLE IF NOT EXISTS youtube.youtube_trending_game_weekly_all_videos_views_count (
    id SERIAL,
    weekday VARCHAR(20) NOT NULL,
    max_views_count INT,
    min_views_count INT,
    avg_views_count INT,
    PRIMARY KEY (id, weekday)
);
-- 요일별 게임 Top 10 인기 급상승 영상들의 최고/최저/평균 조회수 테이블 생성
CREATE TABLE IF NOT EXISTS youtube.youtube_trending_game_weekly_top_10_videos_views_count (
    id SERIAL,
    weekday VARCHAR(20) NOT NULL,
    max_views_count INT,
    min_views_count INT,
    avg_views_count INT,
    PRIMARY KEY (id, weekday)
);
-- 요일별 게임 Top 1 인기 급상승 영상들의 최고/최저/평균 조회수 테이블 생성
CREATE TABLE IF NOT EXISTS youtube.youtube_trending_game_weekly_top_1_videos_views_count (
    id SERIAL,
    weekday VARCHAR(20) NOT NULL,
    max_views_count INT,
    min_views_count INT,
    avg_views_count INT,
    PRIMARY KEY (id, weekday)
);
-- 시간대별 게임 모든 인기 급상승에 오른 채널들의 최고/최저/평균 구독자 수 테이블 생성
CREATE TABLE IF NOT EXISTS youtube.youtube_trending_game_hourly_all_channels_subscribers_count (
    id SERIAL,
    ts TIMESTAMP NOT NULL,
    max_subscribers_count INT,
    min_subscribers_count INT,
    avg_subscribers_count INT,
    PRIMARY KEY (id, ts)
);
-- 시간대별 게임 Top 10 인기 급상승에 오른 채널들의 최고/최저/평균 구독자 수 테이블 생성
CREATE TABLE IF NOT EXISTS youtube.youtube_trending_game_hourly_top_10_channels_subscribers_count (
    id SERIAL,
    ts TIMESTAMP NOT NULL,
    max_subscribers_count INT,
    min_subscribers_count INT,
    avg_subscribers_count INT,
    PRIMARY KEY (id, ts)
);
-- 시간대별 게임 Top 1 인기 급상승에 오른 채널의 구독자 수 테이블 생성
CREATE TABLE IF NOT EXISTS youtube.youtube_trending_game_hourly_top_1_channel_subscribers_count (
    id SERIAL,
    ts TIMESTAMP NOT NULL,
    subscribers_count INT,
    PRIMARY KEY (id, ts)
);
-- 일별 게임 모든 인기 급상승에 오른 채널들의 최고/최저/평균 구독자 수 테이블 생성
CREATE TABLE IF NOT EXISTS youtube.youtube_trending_game_daily_all_channels_subscribers_count (
    id SERIAL,
    date DATE NOT NULL,
    max_subscribers_count INT,
    min_subscribers_count INT,
    avg_subscribers_count INT,
    PRIMARY KEY (id, date)
);
-- 일별 게임 Top 10 인기 급상승에 오른 채널들의 최고/최저/평균 구독자 수 테이블 생성
CREATE TABLE IF NOT EXISTS youtube.youtube_trending_game_daily_top_10_channels_subscribers_count (
    id SERIAL,
    date DATE NOT NULL,
    max_subscribers_count INT,
    min_subscribers_count INT,
    avg_subscribers_count INT,
    PRIMARY KEY (id, date)
);
-- 일별 게임 Top 1 인기 급상승에 오른 채널들의 최고/최저/평균 구독자 수 테이블 생성
CREATE TABLE IF NOT EXISTS youtube.youtube_trending_game_daily_top_1_channels_subscribers_count (
    id SERIAL,
    date DATE NOT NULL,
    max_subscribers_count INT,
    min_subscribers_count INT,
    avg_subscribers_count INT,
    PRIMARY KEY (id, date)
);
-- 요일별 모든 인기 급상승에 오른 채널들의 최고/최저/평균 구독자 수 테이블 생성
CREATE TABLE IF NOT EXISTS youtube.youtube_trending_game_weekly_all_channels_subscribers_count (
    id SERIAL,
    weekday VARCHAR(20) NOT NULL,
    max_subscribers_count INT,
    min_subscribers_count INT,
    avg_subscribers_count INT,
    PRIMARY KEY (id, weekday)
);
-- 요일별 Top 10 인기 급상승에 오른 채널들의 최고/최저/평균 구독자 수 테이블 생성
CREATE TABLE IF NOT EXISTS youtube.youtube_trending_game_weekly_top_10_channels_subscribers_count (
    id SERIAL,
    weekday VARCHAR(20) NOT NULL,
    max_subscribers_count INT,
    min_subscribers_count INT,
    avg_subscribers_count INT,
    PRIMARY KEY (id, weekday)
);
-- 요일별 Top 1 인기 급상승에 오른 채널들의 최고/최저/평균 구독자 수 테이블 생성
CREATE TABLE IF NOT EXISTS youtube.youtube_trending_game_weekly_top_1_channels_subscribers_count (
    id SERIAL,
    weekday VARCHAR(20) NOT NULL,
    max_subscribers_count INT,
    min_subscribers_count INT,
    avg_subscribers_count INT,
    PRIMARY KEY (id, weekday)
);
-- 1위에 등극한 적이 있는 게임 인기 급상승 영상의 최고/최저/평균 조회수 테이블 생성
CREATE TABLE IF NOT EXISTS youtube.youtube_trending_game_top_1_video_views_count (
    id SERIAL,
    video_link VARCHAR(255) NOT NULL,
    max_views_count INT,
    min_views_count INT,
    avg_views_count INT,
    PRIMARY KEY (id, video_link)
);
-- 업로드 후 게임 모든 인기 급상승에 오른 적이 있는 영상의 모든에 도달까지 걸린 시간 테이블 생성
CREATE TABLE IF NOT EXISTS youtube.youtube_trending_game_all_video_time_to_trending (
    id SERIAL,
    video_link VARCHAR(255) NOT NULL,
    time_to_trending INTERVAL,
    PRIMARY KEY (id, video_link)
);
-- 업로드 후 게임 Top 10 인기 급상승에 오른 적이 있는 영상의 Top 10 도달까지 걸린 시간 테이블 생성
CREATE TABLE IF NOT EXISTS youtube.youtube_trending_game_top_10_video_time_to_trending (
    id SERIAL,
    video_link VARCHAR(255) NOT NULL,
    time_to_trending INTERVAL,
    PRIMARY KEY (id, video_link)
);
-- 업로드 후 게임 Top 1 인기 급상승에 오른 적이 있는 영상의 Top 1 도달까지 걸린 시간 테이블 생성
CREATE TABLE IF NOT EXISTS youtube.youtube_trending_game_top_1_video_time_to_trending (
    id SERIAL,
    video_link VARCHAR(255) NOT NULL,
    time_to_trending INTERVAL,
    PRIMARY KEY (id, video_link)
);
-- 채널별 구독자 수 테이블 생성
CREATE TABLE IF NOT EXISTS youtube.youtube_trending_game_channel_subscribers_count (
    id SERIAL,
    channel_link VARCHAR(255) NOT NULL,
    channel_name VARCHAR(255),
    ts TIMESTAMP NOT NULL,
    subscribers_count INT,
    PRIMARY KEY (id, channel_link, ts)
);
-- 채널별 게임 인기 급상승 시각 테이블 생성
CREATE TABLE IF NOT EXISTS youtube.youtube_trending_game_channel_trending_ts (
    id SERIAL,
    channel_link VARCHAR(255) NOT NULL,
    channel_name VARCHAR(255),
    ts TIMESTAMP NOT NULL,
    PRIMARY KEY (id, channel_link, ts)
);
-- 채널별 게임 인기 급상승에 오른 영상들의 최고/최저/평균 조회수 테이블 생성
CREATE TABLE IF NOT EXISTS youtube.youtube_trending_game_channel_videos_views_count (
    id SERIAL,
    channel_link VARCHAR(255) NOT NULL,
    channel_name VARCHAR(255),
    max_views_count INT,
    min_views_count INT,
    avg_views_count INT,
    PRIMARY KEY (id, channel_link)
);
-- 채널별 게임 인기 급상승에 오른 영상들의 도달까지 걸린 최고/최저/평균 소요 시간 테이블 생성
CREATE TABLE IF NOT EXISTS youtube.youtube_trending_game_channel_videos_time_to_trending (
    id SERIAL,
    channel_link VARCHAR(255) NOT NULL,
    channel_name VARCHAR(255),
    max_time_to_trending INTERVAL,
    min_time_to_trending INTERVAL,
    avg_time_to_trending INTERVAL,
    PRIMARY KEY (id, channel_link)
);
-- 채널별 게임 인기 급상승 영상들의 최고/최저/평균 순위 테이블 생성
CREATE TABLE IF NOT EXISTS youtube.youtube_trending_game_channel_videos_rank (
    id SERIAL,
    channel_link VARCHAR(255) NOT NULL,
    channel_name VARCHAR(255),
    high_rank INT,
    low_rank INT,
    avg_rank FLOAT,
    PRIMARY KEY (id, channel_link)
);
-- 채널별 게임 인기 급상승 등극 영상의 수 테이블 생성
CREATE TABLE IF NOT EXISTS youtube.youtube_trending_game_channel_trending_count (
    id SERIAL,
    channel_link VARCHAR(255) NOT NULL,
    channel_name VARCHAR(255),
    trending_count INT,
    PRIMARY KEY (id, channel_link)
);