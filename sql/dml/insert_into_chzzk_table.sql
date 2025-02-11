-- chzzk_hourly_viewers_count(치지직 시간대별 시청자 수) 데이터 삽입
INSERT INTO chzzk.chzzk_hourly_viewers_count (ts, max_viewers_count, avg_viewers_count, total_viewers_count)
SELECT 
    DATE_TRUNC('hour', execution_ts::timestamp) AS ts,
    MAX(viewers_count) AS max_viewers_count,
    ROUND(AVG(viewers_count), 1) AS avg_viewers_count,
    SUM(viewers_count) AS total_viewers_count
FROM raw.chzzk_popular_lives
GROUP BY DATE_TRUNC('hour', execution_ts::timestamp);

-- chzzk_daily_viewers_count(치지직 일별 시청자 수) 데이터 삽입
INSERT INTO chzzk.chzzk_daily_viewers_count (date, max_viewers_count, avg_viewers_count, total_viewers_count)
SELECT 
    DATE(execution_ts::timestamp) AS date,
    MAX(viewers_count) AS max_viewers_count,
    ROUND(AVG(viewers_count), 1) AS avg_viewers_count,
    SUM(viewers_count) AS total_viewers_count
FROM raw.chzzk_popular_lives
GROUP BY DATE(execution_ts::timestamp);

-- chzzk_weekly_viewers_count(치지직 요일별 시청자 수) 데이터 삽입
INSERT INTO chzzk.chzzk_weekly_viewers_count (weekday, max_viewers_count, avg_viewers_count, total_viewers_count)
SELECT 
    TO_CHAR(execution_ts::timestamp, 'Day') AS weekday,
    MAX(viewers_count) AS max_viewers_count,
    ROUND(AVG(viewers_count), 1) AS avg_viewers_count,
    SUM(viewers_count) AS total_viewers_count
FROM raw.chzzk_popular_lives
WHERE execution_ts::timestamp BETWEEN '2024-11-06' AND '2024-12-03'
GROUP BY TO_CHAR(execution_ts::timestamp, 'Day');

-- chzzk_top_10_live_hourly_viewers_count(치지직 시간대별 탑10 라이브 시청자 수) 데이터 삽입
INSERT INTO chzzk.chzzk_top_10_live_hourly_viewers_count (live_id, ts, rank, viewers_count, is_adult)
WITH ranked_lives AS (
    SELECT 
        live_id, 
        DATE_TRUNC('hour', execution_ts::timestamp) AS ts,
        ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('hour', execution_ts::timestamp) ORDER BY viewers_count DESC) AS rank,
        viewers_count,
        is_adult
    FROM raw.chzzk_popular_lives
)
SELECT live_id, ts, rank, viewers_count, is_adult
FROM ranked_lives
WHERE rank <= 10;

-- chzzk_hourly_peak_channel(치지직 시간대별 최고 시청자 수 채널) 데이터 삽입
INSERT INTO chzzk.chzzk_hourly_peak_channel (ts, channel_id, channel_name, peak_viewers_count)
WITH ranked_channels AS (
    SELECT 
        date_trunc('hour', l.execution_ts::timestamp) AS ts,
        l.channel_id,
        c.channel_name,
        MAX(l.viewers_count) AS peak_viewers_count,
        RANK() OVER (PARTITION BY date_trunc('hour', l.execution_ts::timestamp) ORDER BY MAX(l.viewers_count) DESC) AS rank
    FROM raw.chzzk_popular_lives l
    JOIN raw.chzzk_popular_channels c
    ON l.channel_id = c.channel_id
    GROUP BY date_trunc('hour', l.execution_ts::timestamp), l.channel_id, c.channel_name
)
SELECT ts, channel_id, channel_name, peak_viewers_count
FROM ranked_channels
WHERE rank = 1;

-- chzzk_daily_peak_channel(치지직 일별 최고 시청자 수 채널) 데이터 삽입
INSERT INTO chzzk.chzzk_daily_peak_channel (date, channel_id, channel_name, peak_viewers_count)
WITH ranked_channels AS (
    SELECT 
        date_trunc('day', l.execution_ts::timestamp) AS date,
        l.channel_id,
        c.channel_name,
        MAX(l.viewers_count) AS peak_viewers_count,
        RANK() OVER (PARTITION BY date_trunc('day', l.execution_ts::timestamp) ORDER BY MAX(l.viewers_count) DESC) AS rank
    FROM raw.chzzk_popular_lives l
    JOIN raw.chzzk_popular_channels c
    ON l.channel_id = c.channel_id
    GROUP BY date_trunc('day', l.execution_ts::timestamp), l.channel_id, c.channel_name
)
SELECT date, channel_id, channel_name, peak_viewers_count
FROM ranked_channels
WHERE rank = 1;

-- chzzk_live_reach_time_to_peak(치지직 라이브별 최대 시청자 수 도달 시간) 데이터 삽입
INSERT INTO chzzk.chzzk_live_reach_time_to_peak (live_id, open_ts, time_to_peak)
SELECT 
    l.live_id,
    l.open_ts::timestamp,
    (l.execution_ts::timestamp - l.open_ts::timestamp) AS time_to_peak
FROM raw.chzzk_popular_lives l
JOIN (
    SELECT live_id, MAX(viewers_count) AS max_viewers_count
    FROM raw.chzzk_popular_lives
    GROUP BY live_id
) max_viewers
ON l.live_id = max_viewers.live_id
AND l.viewers_count = max_viewers.max_viewers_count;

-- chzzk_channel_reach_time_to_peak(치지직 채널별 각 라이브의 최대 시청자 수 도달 시간) 데이터 삽입
INSERT INTO chzzk.chzzk_channel_reach_time_to_peak (channel_id, live_id, channel_name, open_ts, time_to_peak)
SELECT DISTINCT
    l.channel_id,
    l.live_id,
    c.channel_name,
    l.open_ts::timestamp,
    (l.execution_ts::timestamp - l.open_ts::timestamp) AS time_to_peak
FROM raw.chzzk_popular_lives l
JOIN raw.chzzk_popular_channels c
    ON l.channel_id = c.channel_id
JOIN (
    SELECT live_id, MAX(viewers_count) AS max_viewers_count
    FROM raw.chzzk_popular_lives
    GROUP BY live_id
) max_viewers
    ON l.live_id = max_viewers.live_id
    AND l.viewers_count = max_viewers.max_viewers_count;
    
-- chzzk_channel_peak_live(치지직 채널별 최대 시청자 수 도달 시점의 라이브 정보)
INSERT INTO chzzk.chzzk_channel_peak_live (channel_id, live_id, channel_name, peak_ts, peak_viewers_count, live_title, category_id, is_adult)
SELECT DISTINCT
    l.channel_id,
    l.live_id,
    c.channel_name,
    l.execution_ts::timestamp AS peak_ts,
    l.viewers_count AS peak_viewers_count,
    l.live_title,
    l.category_id,
    l.is_adult
FROM raw.chzzk_popular_lives l
JOIN raw.chzzk_popular_channels c ON l.channel_id = c.channel_id
WHERE l.viewers_count = (
     SELECT MAX(viewers_count)
     FROM raw.chzzk_popular_lives 
     WHERE live_id = l.live_id
    );

-- chzzk_channel_followers_count(치지직 채널별 팔로워 수) 데이터 삽입
INSERT INTO chzzk.chzzk_channel_followers_count (channel_id, ts, channel_name, followers_count)
SELECT 
    channel_id,
    execution_ts::timestamp AS ts,
    channel_name,
    followers_count
FROM raw.chzzk_popular_channels;

-- chzzk_channel_viewers_count(치지직 채널별 각 라이브 방송의 시청자 수) 데이터 삽입
INSERT INTO chzzk.chzzk_channel_viewers_count (channel_id, live_id, channel_name, open_ts, max_viewers_count, avg_viewers_count)
SELECT 
    l.channel_id,
    l.live_id,
    c.channel_name,
    l.open_ts::timestamp,
    MAX(l.viewers_count) AS max_viewers_count,
    ROUND(AVG(l.viewers_count), 1) AS avg_viewers_count
FROM raw.chzzk_popular_lives l
JOIN raw.chzzk_popular_channels c
    ON l.channel_id = c.channel_id
GROUP BY l.channel_id, l.live_id, c.channel_name, l.open_ts::timestamp;

-- chzzk_category_hourly_viewers_count(치지직 카테고리별 각 시간대의 시청자 수) 데이터 삽입
INSERT INTO chzzk.chzzk_category_hourly_viewers_count (category_id, ts, max_viewers_count, avg_viewers_count, total_viewers_count)
SELECT 
    COALESCE(l.category_id, 'None'),
    DATE_TRUNC('hour', l.execution_ts::timestamp) AS ts,
    MAX(l.viewers_count) AS max_viewers_count,
    ROUND(AVG(l.viewers_count), 1) AS avg_viewers_count,
    SUM(l.viewers_count) AS total_viewers_count
FROM raw.chzzk_popular_lives l
GROUP BY l.category_id, DATE_TRUNC('hour', l.execution_ts::timestamp);

-- chzzk_category_daily_viewers_count(치지직 카테고리별 각 일자의 시청자 수) 데이터 삽입
INSERT INTO chzzk.chzzk_category_daily_viewers_count (category_id, date, max_viewers_count, avg_viewers_count, total_viewers_count)
SELECT 
    COALESCE(l.category_id, 'None'),
    DATE_TRUNC('day', l.execution_ts::timestamp) AS date,
    MAX(l.viewers_count) AS max_viewers_count,
    ROUND(AVG(l.viewers_count), 1) AS avg_viewers_count,
    SUM(l.viewers_count) AS total_viewers_count
FROM raw.chzzk_popular_lives l
GROUP BY l.category_id, DATE_TRUNC('day', l.execution_ts::timestamp);