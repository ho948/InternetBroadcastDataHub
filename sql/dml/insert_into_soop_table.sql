-- soop_hourly_viewers_count(soop 시간대별 시청자 수) 데이터 삽입
INSERT INTO soop.soop_hourly_viewers_count (ts, max_viewers_count, avg_viewers_count, total_viewers_count)
SELECT 
    DATE_TRUNC('hour', execution_ts::timestamp) AS ts,
    MAX(viewers_count) AS max_viewers_count,
    ROUND(AVG(viewers_count), 1) AS avg_viewers_count,
    SUM(viewers_count) AS total_viewers_count
FROM raw.soop_popular_lives
GROUP BY DATE_TRUNC('hour', execution_ts::timestamp);

-- soop_daily_viewers_count(soop 일별 시청자 수) 데이터 삽입
INSERT INTO soop.soop_daily_viewers_count (date, max_viewers_count, avg_viewers_count, total_viewers_count)
SELECT 
    DATE(execution_ts::timestamp) AS date,
    MAX(viewers_count) AS max_viewers_count,
    ROUND(AVG(viewers_count), 1) AS avg_viewers_count,
    SUM(viewers_count) AS total_viewers_count
FROM raw.soop_popular_lives
GROUP BY DATE(execution_ts::timestamp);

-- soop_weekly_viewers_count(soop 요일별 시청자 수) 데이터 삽입
INSERT INTO soop.soop_weekly_viewers_count (weekday, max_viewers_count, avg_viewers_count, total_viewers_count)
SELECT 
    TO_CHAR(execution_ts::timestamp, 'Day') AS weekday,
    MAX(viewers_count) AS max_viewers_count,
    ROUND(AVG(viewers_count), 1) AS avg_viewers_count,
    SUM(viewers_count) AS total_viewers_count
FROM raw.soop_popular_lives
WHERE execution_ts::timestamp BETWEEN '2024-11-06' AND '2024-12-03'
GROUP BY TO_CHAR(execution_ts::timestamp, 'Day');

-- soop_top_10_live_hourly_viewers_count(soop 시간대별 탑10 라이브 시청자 수) 데이터 삽입
INSERT INTO soop.soop_top_10_live_hourly_viewers_count (live_id, ts, rank, viewers_count, is_adult)
WITH ranked_lives AS (
    SELECT 
        live_id, 
        DATE_TRUNC('hour', execution_ts::timestamp) AS ts,
        ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('hour', execution_ts::timestamp) ORDER BY viewers_count DESC) AS rank,
        viewers_count,
        is_adult
    FROM raw.soop_popular_lives
)
SELECT live_id, ts, rank, viewers_count, is_adult
FROM ranked_lives
WHERE rank <= 10;

-- soop_hourly_peak_channel(soop 시간대별 최고 시청자 수 채널) 데이터 삽입
INSERT INTO soop.soop_hourly_peak_channel (ts, channel_id, channel_name, peak_viewers_count)
WITH ranked_channels AS (
    SELECT 
        DATE_TRUNC('hour', execution_ts::timestamp) AS ts,
        channel_id,
        channel_name,
        MAX(viewers_count) AS peak_viewers_count,
        RANK() OVER (PARTITION BY DATE_TRUNC('hour', execution_ts::timestamp) ORDER BY MAX(viewers_count) DESC) AS rank
    FROM raw.soop_popular_lives
    GROUP BY DATE_TRUNC('hour', execution_ts::timestamp), channel_id, channel_name
)
SELECT ts, channel_id, channel_name, peak_viewers_count
FROM ranked_channels
WHERE rank = 1;

-- soop_daily_peak_channel(soop 일별 최고 시청자 수 채널) 데이터 삽입
INSERT INTO soop.soop_daily_peak_channel (date, channel_id, channel_name, peak_viewers_count)
WITH ranked_channels AS (
    SELECT 
        DATE_TRUNC('day', execution_ts::timestamp) AS date,
        channel_id,
        channel_name,
        MAX(viewers_count) AS peak_viewers_count,
        RANK() OVER (PARTITION BY DATE_TRUNC('day', execution_ts::timestamp) ORDER BY MAX(viewers_count) DESC) AS rank
    FROM raw.soop_popular_lives
    GROUP BY DATE_TRUNC('day', execution_ts::timestamp), channel_id, channel_name
)
SELECT date, channel_id, channel_name, peak_viewers_count
FROM ranked_channels
WHERE rank = 1;

-- soop_channel_peak_live(soop 채널별 최대 시청자 수 도달 시점의 라이브 정보)
INSERT INTO soop.soop_channel_peak_live (channel_id, live_id, channel_name, peak_ts, peak_viewers_count, live_title, category_id, is_adult)
SELECT DISTINCT
    l.channel_id,
    l.live_id,
    l.channel_name,
    l.execution_ts::timestamp AS peak_ts,
    l.viewers_count AS peak_viewers_count,
    l.live_title,
    l.category,
    l.is_adult
FROM raw.soop_popular_lives l
WHERE viewers_count = (
     SELECT MAX(viewers_count)
     FROM raw.soop_popular_lives 
     WHERE live_id = l.live_id
    );

-- soop_channel_viewers_count(soop 채널별 각 라이브 방송의 시청자 수) 데이터 삽입
INSERT INTO soop.soop_channel_viewers_count (channel_id, live_id, channel_name, max_viewers_count, avg_viewers_count)
SELECT 
    channel_id,
    live_id,
    channel_name,
    MAX(viewers_count) AS max_viewers_count,
    ROUND(AVG(viewers_count), 1) AS avg_viewers_count
FROM raw.soop_popular_lives
GROUP BY channel_id, live_id, channel_name;

-- soop_category_hourly_viewers_count(soop 카테고리별 각 시간대의 시청자 수) 데이터 삽입
INSERT INTO soop.soop_category_hourly_viewers_count (category_id, ts, max_viewers_count, avg_viewers_count, total_viewers_count)
SELECT 
    category,
    DATE_TRUNC('hour', execution_ts::timestamp) AS ts,
    MAX(viewers_count) AS max_viewers_count,
    ROUND(AVG(viewers_count), 1) AS avg_viewers_count,
    SUM(viewers_count) AS total_viewers_count
FROM raw.soop_popular_lives
GROUP BY category, DATE_TRUNC('hour', execution_ts::timestamp);

-- soop_category_daily_viewers_count(soop 카테고리별 각 일자의 시청자 수) 데이터 삽입
INSERT INTO soop.soop_category_daily_viewers_count (category_id, date, max_viewers_count, avg_viewers_count, total_viewers_count)
SELECT 
    category,
    DATE(execution_ts::timestamp) AS date,
    MAX(viewers_count) AS max_viewers_count,
    ROUND(AVG(viewers_count), 1) AS avg_viewers_count,
    SUM(viewers_count) AS total_viewers_count
FROM raw.soop_popular_lives
GROUP BY category, DATE(execution_ts::timestamp);