-- youtube_trending_game_video_with_rank(유튜브 개임 인기 급상승 동영상 정보 및 순위) 중복 제거 및 데이터 삽입
WITH RankedGameVideos AS (
    SELECT
        ytgr.rk, ytgv.link, ytgv.title, ytgv.views_count, ytgv.uploaded_at::timestamp, 
        ytgv.thumbnail_img, ytgv.video_text, ytgv.channel_link, ytgv.channel_name, 
        ytgv.subscribers_count, ytgv.channel_img, ytgv.execution_ts::timestamp,
        ROW_NUMBER() OVER (PARTITION BY ytgv.link, ytgv.execution_ts::timestamp ORDER BY ytgr.rk) AS row_num
    FROM raw.youtube_trending_game_videos ytgv
    JOIN raw.youtube_trending_game_ranks ytgr 
      ON ytgv.link = ytgr.link AND ytgv.execution_ts::timestamp = ytgr.execution_ts::timestamp AND ytgv.views_count > 10000
)
INSERT INTO youtube.youtube_trending_game_video_with_rank (rank, link, title, views_count, uploaded_at, thumbnail_img, video_text, channel_link, channel_name, subscribers_count, channel_img, execution_ts)
SELECT
    rk, link, title, views_count, uploaded_at::timestamp, thumbnail_img, video_text, channel_link, channel_name, subscribers_count, channel_img, execution_ts::timestamp
FROM RankedGameVideos
WHERE row_num = 1;

-- 분석용 데이터 삽입

INSERT INTO youtube.youtube_trending_game_hourly_all_videos_views_count (ts, max_views_count, min_views_count, avg_views_count)
SELECT
    DATE_TRUNC('hour', execution_ts::timestamp) AS ts,
    MAX(views_count) AS max_views_count,
    MIN(views_count) AS min_views_count,
    CAST(AVG(views_count) AS INT) AS avg_views_count
FROM youtube.youtube_trending_game_video_with_rank
GROUP BY DATE_TRUNC('hour', execution_ts::timestamp);

INSERT INTO youtube.youtube_trending_game_hourly_top_10_videos_views_count (ts, max_views_count, min_views_count, avg_views_count)
SELECT
    DATE_TRUNC('hour', execution_ts::timestamp) AS ts,
    MAX(views_count) AS max_views_count,
    MIN(views_count) AS min_views_count,
    CAST(AVG(views_count) AS INT) AS avg_views_count
FROM youtube.youtube_trending_game_video_with_rank
WHERE rank <= 10
GROUP BY DATE_TRUNC('hour', execution_ts::timestamp);

INSERT INTO youtube.youtube_trending_game_hourly_top_1_video_views_count (ts, views_count)
SELECT
    DATE_TRUNC('hour', execution_ts::timestamp) AS ts,
    views_count
FROM youtube.youtube_trending_game_video_with_rank
WHERE rank = 1;

INSERT INTO youtube.youtube_trending_game_daily_all_videos_views_count (date, max_views_count, min_views_count, avg_views_count)
SELECT
    DATE_TRUNC('day', execution_ts::timestamp) AS date,
    MAX(views_count) AS max_views_count,
    MIN(views_count) AS min_views_count,
    CAST(AVG(views_count) AS INT) AS avg_views_count
FROM youtube.youtube_trending_game_video_with_rank
GROUP BY DATE_TRUNC('day', execution_ts::timestamp);

INSERT INTO youtube.youtube_trending_game_daily_top_10_videos_views_count (date, max_views_count, min_views_count, avg_views_count)
SELECT
    DATE_TRUNC('day', execution_ts::timestamp) AS date,
    MAX(views_count) AS max_views_count,
    MIN(views_count) AS min_views_count,
    CAST(AVG(views_count) AS INT) AS avg_views_count
FROM youtube.youtube_trending_game_video_with_rank
WHERE rank <= 10
GROUP BY DATE_TRUNC('day', execution_ts::timestamp);

INSERT INTO youtube.youtube_trending_game_daily_top_1_videos_views_count (date, max_views_count, min_views_count, avg_views_count)
SELECT
    DATE_TRUNC('day', execution_ts::timestamp) AS date,
    MAX(views_count) AS max_views_count,
    MIN(views_count) AS min_views_count,
    CAST(AVG(views_count) AS INT) AS avg_views_count
FROM youtube.youtube_trending_game_video_with_rank
WHERE rank = 1
GROUP BY DATE_TRUNC('day', execution_ts::timestamp);

INSERT INTO youtube.youtube_trending_game_weekly_all_videos_views_count (weekday, max_views_count, min_views_count, avg_views_count)
SELECT
    TO_CHAR(execution_ts::timestamp, 'Day') AS weekday,
    MAX(views_count) AS max_views_count,
    MIN(views_count) AS min_views_count,
    CAST(AVG(views_count) AS INT) AS avg_views_count
FROM youtube.youtube_trending_game_video_with_rank
WHERE execution_ts::timestamp BETWEEN '2024-11-01' AND '2024-11-28'
GROUP BY TO_CHAR(execution_ts::timestamp, 'Day');

INSERT INTO youtube.youtube_trending_game_weekly_top_10_videos_views_count (weekday, max_views_count, min_views_count, avg_views_count)
SELECT
    TO_CHAR(execution_ts::timestamp, 'Day') AS weekday,
    MAX(views_count) AS max_views_count,
    MIN(views_count) AS min_views_count,
    CAST(AVG(views_count) AS INT) AS avg_views_count
FROM youtube.youtube_trending_game_video_with_rank
WHERE rank <= 10 AND execution_ts::timestamp BETWEEN '2024-11-01' AND '2024-11-28'
GROUP BY TO_CHAR(execution_ts::timestamp, 'Day');

INSERT INTO youtube.youtube_trending_game_weekly_top_1_videos_views_count (weekday, max_views_count, min_views_count, avg_views_count)
SELECT
    TO_CHAR(execution_ts::timestamp, 'Day') AS weekday,
    MAX(views_count) AS max_views_count,
    MIN(views_count) AS min_views_count,
    CAST(AVG(views_count) AS INT) AS avg_views_count
FROM youtube.youtube_trending_game_video_with_rank
WHERE rank = 1 AND execution_ts::timestamp BETWEEN '2024-11-01' AND '2024-11-28'
GROUP BY TO_CHAR(execution_ts::timestamp, 'Day');

INSERT INTO youtube.youtube_trending_game_hourly_all_channels_subscribers_count (ts, max_subscribers_count, min_subscribers_count, avg_subscribers_count)
SELECT 
    DATE_TRUNC('hour', execution_ts::timestamp) AS ts,
    MAX(subscribers_count) AS max_subscribers_count,
    MIN(subscribers_count) AS min_subscribers_count,
    ROUND(AVG(subscribers_count)) AS avg_subscribers_count
FROM youtube.youtube_trending_game_video_with_rank
GROUP BY DATE_TRUNC('hour', execution_ts::timestamp);

INSERT INTO youtube.youtube_trending_game_hourly_top_10_channels_subscribers_count (ts, max_subscribers_count, min_subscribers_count, avg_subscribers_count)
SELECT 
    DATE_TRUNC('hour', execution_ts::timestamp) AS ts,
    MAX(subscribers_count) AS max_subscribers_count,
    MIN(subscribers_count) AS min_subscribers_count,
    ROUND(AVG(subscribers_count)) AS avg_subscribers_count
FROM youtube.youtube_trending_game_video_with_rank
WHERE rank <= 10
GROUP BY DATE_TRUNC('hour', execution_ts::timestamp);

INSERT INTO youtube.youtube_trending_game_hourly_top_1_channel_subscribers_count (ts, subscribers_count)
SELECT 
    DATE_TRUNC('hour', execution_ts::timestamp) AS ts,
    subscribers_count
FROM youtube.youtube_trending_game_video_with_rank
WHERE rank = 1;

INSERT INTO youtube.youtube_trending_game_daily_all_channels_subscribers_count (date, max_subscribers_count, min_subscribers_count, avg_subscribers_count)
SELECT 
    DATE_TRUNC('day', execution_ts::timestamp) AS date,
    MAX(subscribers_count) AS max_subscribers_count,
    MIN(subscribers_count) AS min_subscribers_count,
    ROUND(AVG(subscribers_count)) AS avg_subscribers_count
FROM youtube.youtube_trending_game_video_with_rank
GROUP BY DATE_TRUNC('day', execution_ts::timestamp);

INSERT INTO youtube.youtube_trending_game_daily_top_10_channels_subscribers_count (date, max_subscribers_count, min_subscribers_count, avg_subscribers_count)
SELECT 
    DATE_TRUNC('day', execution_ts::timestamp) AS date,
    MAX(subscribers_count) AS max_subscribers_count,
    MIN(subscribers_count) AS min_subscribers_count,
    ROUND(AVG(subscribers_count)) AS avg_subscribers_count
FROM youtube.youtube_trending_game_video_with_rank
WHERE rank <= 10
GROUP BY DATE_TRUNC('day', execution_ts::timestamp);

INSERT INTO youtube.youtube_trending_game_daily_top_1_channels_subscribers_count (date, max_subscribers_count, min_subscribers_count, avg_subscribers_count)
SELECT 
    DATE_TRUNC('day', execution_ts::timestamp) AS date,
    MAX(subscribers_count) AS max_subscribers_count,
    MIN(subscribers_count) AS min_subscribers_count,
    ROUND(AVG(subscribers_count)) AS avg_subscribers_count
FROM youtube.youtube_trending_game_video_with_rank
WHERE rank = 1
GROUP BY DATE_TRUNC('day', execution_ts::timestamp);

INSERT INTO youtube.youtube_trending_game_weekly_all_channels_subscribers_count (weekday, max_subscribers_count, min_subscribers_count, avg_subscribers_count)
SELECT
    TO_CHAR(execution_ts::timestamp, 'Day') AS weekday,
    MAX(subscribers_count) AS max_subscribers_count,
    MIN(subscribers_count) AS min_subscribers_count,
    ROUND(AVG(subscribers_count)) AS avg_subscribers_count
FROM youtube.youtube_trending_game_video_with_rank
WHERE execution_ts::timestamp BETWEEN '2024-11-01' AND '2024-11-28'
GROUP BY TO_CHAR(execution_ts::timestamp, 'Day');

INSERT INTO youtube.youtube_trending_game_weekly_top_10_channels_subscribers_count (weekday, max_subscribers_count, min_subscribers_count, avg_subscribers_count)
SELECT
    TO_CHAR(execution_ts::timestamp, 'Day') AS weekday,
    MAX(subscribers_count) AS max_subscribers_count,
    MIN(subscribers_count) AS min_subscribers_count,
    ROUND(AVG(subscribers_count)) AS avg_subscribers_count
FROM youtube.youtube_trending_game_video_with_rank
WHERE rank <= 10 AND execution_ts::timestamp BETWEEN '2024-11-01' AND '2024-11-28'
GROUP BY TO_CHAR(execution_ts::timestamp, 'Day');

INSERT INTO youtube.youtube_trending_game_weekly_top_1_channels_subscribers_count (weekday, max_subscribers_count, min_subscribers_count, avg_subscribers_count)
SELECT
    TO_CHAR(execution_ts::timestamp, 'Day') AS weekday,
    MAX(subscribers_count) AS max_subscribers_count,
    MIN(subscribers_count) AS min_subscribers_count,
    ROUND(AVG(subscribers_count)) AS avg_subscribers_count
FROM youtube.youtube_trending_game_video_with_rank
WHERE rank = 1 AND execution_ts::timestamp BETWEEN '2024-11-01' AND '2024-11-28'
GROUP BY TO_CHAR(execution_ts::timestamp, 'Day');

INSERT INTO youtube.youtube_trending_game_top_1_video_views_count (video_link, max_views_count, min_views_count, avg_views_count)
SELECT 
    link,
    MAX(views_count) AS max_views_count,
    MIN(views_count) AS min_views_count,
    CAST(AVG(views_count) AS INT) AS avg_views_count
FROM youtube.youtube_trending_game_video_with_rank
WHERE rank = 1
GROUP BY link;

INSERT INTO youtube.youtube_trending_game_all_video_time_to_trending (video_link, time_to_trending)
SELECT
    link AS video_link,
    MIN(execution_ts::timestamp - uploaded_at::timestamp) AS time_to_trending
FROM youtube.youtube_trending_game_video_with_rank
GROUP BY link;

INSERT INTO youtube.youtube_trending_game_top_10_video_time_to_trending (video_link, time_to_trending)
SELECT
    link AS video_link,
    MIN(execution_ts::timestamp - uploaded_at::timestamp) AS time_to_trending
FROM youtube.youtube_trending_game_video_with_rank
WHERE rank <= 10
GROUP BY link;

INSERT INTO youtube.youtube_trending_game_top_1_video_time_to_trending (video_link, time_to_trending)
SELECT
    link AS video_link,
    MIN(execution_ts::timestamp - uploaded_at::timestamp) AS time_to_trending
FROM youtube.youtube_trending_game_video_with_rank
WHERE rank = 1
GROUP BY link;

INSERT INTO youtube.youtube_trending_game_channel_subscribers_count (channel_link, channel_name, ts, subscribers_count)
SELECT 
    channel_link,
    channel_name,
    execution_ts::timestamp AS ts,
    subscribers_count
FROM youtube.youtube_trending_game_video_with_rank;

INSERT INTO youtube.youtube_trending_game_channel_trending_ts (channel_link, channel_name, ts)
SELECT 
    channel_link,
    channel_name,
    execution_ts::timestamp AS ts
FROM youtube.youtube_trending_game_video_with_rank;

INSERT INTO youtube.youtube_trending_game_channel_videos_views_count (channel_link, channel_name, max_views_count, min_views_count, avg_views_count)
SELECT
    channel_link,
    channel_name,
    MAX(views_count) AS max_views_count,
    MIN(views_count) AS min_views_count,
    CAST(AVG(views_count) AS INT) AS avg_views_count
FROM youtube.youtube_trending_game_video_with_rank
GROUP BY channel_link, channel_name;

INSERT INTO youtube.youtube_trending_game_channel_videos_time_to_trending (channel_link, channel_name, max_time_to_trending, min_time_to_trending, avg_time_to_trending)
SELECT
    channel_link,
    channel_name,
    MAX(execution_ts::timestamp - uploaded_at::timestamp) AS max_time_to_trending,
    MIN(execution_ts::timestamp - uploaded_at::timestamp) AS min_time_to_trending,
    AVG(execution_ts::timestamp - uploaded_at::timestamp) AS avg_time_to_trending
FROM youtube.youtube_trending_game_video_with_rank
GROUP BY channel_link, channel_name;

INSERT INTO youtube.youtube_trending_game_channel_videos_rank (channel_link, channel_name, high_rank, low_rank, avg_rank)
SELECT
    channel_link,
    channel_name,
    MIN(rank) AS high_rank,
    MAX(rank) AS low_rank,
    ROUND(AVG(rank), 1) AS avg_rank
FROM youtube.youtube_trending_game_video_with_rank
GROUP BY channel_link, channel_name;

INSERT INTO youtube.youtube_trending_game_channel_trending_count (channel_link, channel_name, trending_count)
SELECT 
    channel_link,
    channel_name,
    COUNT(DISTINCT link) AS trending_count
FROM youtube.youtube_trending_game_video_with_rank
GROUP BY channel_link, channel_name;