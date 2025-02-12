INSERT INTO chzzk.popular_live_with_rank 
    (execution_ts, live_id, live_title, viewers_count, channel_id, category_type, category_id, category_name, is_adult, open_ts, rank)
    SELECT
        execution_ts::timestamp,
        live_id,
        live_title,
        viewers_count,
        channel_id,
        category_type,
        COALESCE(category_id, 'None'),
        category_name,
        is_adult,
        open_ts::timestamp,
        ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('hour', execution_ts::timestamp) ORDER BY viewers_count DESC) AS rank
    FROM raw.chzzk_popular_lives;

INSERT INTO chzzk.channel_name_latest (channel_id, channel_name, created_at, updated_at)
    SELECT 
        c.channel_id, 
        c.channel_name,
        NOW() AS created_at,
        NOW() AS updated_at
    FROM 
        raw.chzzk_popular_channels c,
        (
            SELECT channel_id, MAX(execution_ts) AS execution_ts
            FROM raw.chzzk_popular_channels
            GROUP BY 1
        ) c2
    WHERE c.channel_id = c2.channel_id AND c.execution_ts = c2.execution_ts
    ON CONFLICT (channel_id)
    DO UPDATE SET 
        channel_name = EXCLUDED.channel_name,
        updated_at = NOW();

INSERT INTO chzzk.peak_channel (date, channel_name, peak_time, peak_viewers_count)
    WITH ranked_data AS (
        SELECT 
            DATE(execution_ts) AS date,
            channel_id,
            TO_CHAR(execution_ts, 'HH24:MI') AS peak_time,
            viewers_count AS peak_viewers_count,
            RANK() OVER (PARTITION BY DATE(execution_ts) ORDER BY viewers_count DESC) AS rnk
        FROM chzzk.popular_live_with_rank
    )
    SELECT 
        r.date AS date,
        c.channel_name AS channel_name,
        r.peak_time AS peak_time,
        r.peak_viewers_count AS peak_viewers_count
    FROM ranked_data r
    JOIN chzzk.channel_name_latest c ON r.channel_id = c.channel_id
    WHERE rnk = 1;

INSERT INTO chzzk.live_time_to_peak (live_id, channel_name, time_to_peak)
    WITH live_max_viewers AS (
        SELECT live_id, MAX(viewers_count) AS max_viewers_count
        FROM chzzk.popular_live_with_rank
        GROUP BY 1
    )
    SELECT l.live_id, channel_name, (MIN(execution_ts) - open_ts) AS time_to_peak
    FROM chzzk.popular_live_with_rank l
    JOIN live_max_viewers lm ON l.live_id = lm.live_id AND l.viewers_count = lm.max_viewers_count
    JOIN chzzk.channel_name_latest c ON l.channel_id = c.channel_id
    WHERE open_ts < execution_ts
    GROUP BY l.live_id, channel_name, open_ts;

INSERT INTO chzzk.channel_live_viewers_count (channel_name, live_id, viewers_count, execution_ts)
    SELECT c.channel_name, live_id, viewers_count, execution_ts
    FROM chzzk.popular_live_with_rank l
    JOIN chzzk.channel_name_latest c ON l.channel_id = c.channel_id
    ORDER BY execution_ts;

INSERT INTO chzzk.channel_peak_live 
    (channel_name, live_id, peak_viewers_count, peak_live_title, peak_category_id, peak_is_adult, peak_ts)
  SELECT 
      cn.channel_name,
      pl.live_id,
      pl.viewers_count AS peak_viewers_count,
      pl.live_title AS peak_live_title,
      pl.category_id AS peak_category_id,
      pl.is_adult AS peak_is_adult,
      MIN(pl.execution_ts) AS peak_ts
  FROM (
      SELECT 
          channel_id,
          live_id, 
          viewers_count, 
          live_title, 
          category_id, 
          is_adult, 
          execution_ts
      FROM chzzk.popular_live_with_rank pl
      WHERE (pl.live_id, pl.viewers_count) IN (
          SELECT 
              live_id, 
              MAX(viewers_count)
          FROM chzzk.popular_live_with_rank
          GROUP BY live_id
      )
  ) pl
  JOIN chzzk.channel_name_latest cn ON pl.channel_id = cn.channel_id
  GROUP BY 1, 2, 3, 4, 5, 6;