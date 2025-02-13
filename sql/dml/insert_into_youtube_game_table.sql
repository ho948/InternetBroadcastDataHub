INSERT INTO youtube.game_video_with_rank 
    (rank, link, title, views_count, uploaded_at, thumbnail_img, video_text, channel_link, channel_name, subscribers_count, channel_img, execution_ts)
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
SELECT
    rk, link, title, views_count, uploaded_at::timestamp, thumbnail_img, video_text, channel_link, channel_name, subscribers_count, channel_img, execution_ts::timestamp
FROM RankedGameVideos
WHERE row_num = 1;

INSERT INTO youtube.game_weekly_views_count (weekday, max_views_count, min_views_count, avg_views_count)
SELECT
    TO_CHAR(execution_ts::timestamp, 'Day') AS weekday,
    MAX(views_count) AS max_views_count,
    MIN(views_count) AS min_views_count,
    CAST(AVG(views_count) AS INT) AS avg_views_count
FROM youtube.game_video_with_rank
WHERE execution_ts::timestamp BETWEEN '2024-11-01' AND '2024-11-28'
GROUP BY TO_CHAR(execution_ts::timestamp, 'Day');

INSERT INTO youtube.game_channel_name_latest (channel_link, channel_name, created_at, updated_at)
SELECT DISTINCT
    c.channel_link, 
    c.channel_name,
    NOW() AS created_at,
    NOW() AS updated_at
FROM 
    youtube.game_video_with_rank c,
    (
    SELECT channel_link, MAX(execution_ts) AS execution_ts
    FROM youtube.game_video_with_rank
    GROUP BY 1
    ) c2
WHERE c.channel_link = c2.channel_link AND c.execution_ts = c2.execution_ts
ON CONFLICT (channel_link)
DO UPDATE SET 
    channel_name = EXCLUDED.channel_name,
    updated_at = NOW();

INSERT INTO youtube.game_channel_video (channel_name, video_link, views_count, rank, subscribers_count, execution_ts)
SELECT
    c.channel_name,
    v.link,
    v.views_count,
    v.rank,
    v.subscribers_count,
    v.execution_ts
FROM youtube.game_video_with_rank v
JOIN youtube.game_channel_name_latest c
ON v.channel_link = c.channel_link;

INSERT INTO youtube.game_channel_live_time_to_trending (channel_name, video_link, time_to_trending)
WITH video_first AS (
    SELECT link, MIN(execution_ts) AS execution_ts
    FROM youtube.game_video_with_rank
    GROUP BY 1
    )
SELECT
    c.channel_name,
    v.link,
    (vf.execution_ts - v.uploaded_at) AS time_to_trending
FROM youtube.game_video_with_rank v
JOIN video_first vf ON v.link = vf.link AND v.execution_ts = vf.execution_ts
JOIN youtube.game_channel_name_latest c ON v.channel_link = c.channel_link;