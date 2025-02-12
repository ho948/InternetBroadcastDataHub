INSERT INTO soop.popular_live_with_rank 
    (execution_ts, live_id, live_title, viewers_count, channel_id, channel_name, category, is_adult, rank)
    SELECT
        execution_ts::timestamp,
        live_id,
        live_title,
        viewers_count,
        channel_id,
        channel_name,
        category,
        is_adult,
        ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('hour', execution_ts::timestamp) ORDER BY viewers_count DESC) AS rank
    FROM raw.soop_popular_lives;

INSERT INTO soop.channel_name_latest (channel_id, channel_name, created_at, updated_at)
    SELECT 
        c.channel_id, 
        c.channel_name,
        NOW() AS created_at,
        NOW() AS updated_at
    FROM 
        raw.soop_popular_lives c,
        (
            SELECT channel_id, MAX(execution_ts) AS execution_ts
            FROM raw.soop_popular_lives
            GROUP BY 1
        ) c2
    WHERE c.channel_id = c2.channel_id AND c.execution_ts = c2.execution_ts
    ON CONFLICT (channel_id)
    DO UPDATE SET 
        channel_name = EXCLUDED.channel_name,
        updated_at = NOW();

INSERT INTO soop.peak_channel (date, channel_name, peak_time, peak_viewers_count)
    WITH ranked_data AS (
        SELECT 
            DATE(execution_ts) AS date,
            channel_id,
            TO_CHAR(execution_ts, 'HH24:MI') AS peak_time,
            viewers_count AS peak_viewers_count,
            RANK() OVER (PARTITION BY DATE(execution_ts) ORDER BY viewers_count DESC) AS rnk
        FROM soop.popular_live_with_rank
    )
    SELECT 
        r.date AS date,
        c.channel_name AS channel_name,
        r.peak_time AS peak_time,
        r.peak_viewers_count AS peak_viewers_count
    FROM ranked_data r
    JOIN soop.channel_name_latest c ON r.channel_id = c.channel_id
    WHERE rnk = 1;

INSERT INTO soop.channel_live_viewers_count (channel_name, live_id, viewers_count, execution_ts)
    SELECT c.channel_name, live_id, viewers_count, execution_ts
    FROM soop.popular_live_with_rank l
    JOIN soop.channel_name_latest c ON l.channel_id = c.channel_id
    ORDER BY execution_ts;

INSERT INTO soop.channel_peak_live 
    (channel_name, live_id, peak_viewers_count, peak_live_title, peak_category, peak_is_adult, peak_ts)
  SELECT 
      cn.channel_name,
      pl.live_id,
      pl.viewers_count AS peak_viewers_count,
      pl.live_title AS peak_live_title,
      pl.category AS peak_category,
      pl.is_adult AS peak_is_adult,
      MIN(pl.execution_ts) AS peak_ts
  FROM (
      SELECT 
          channel_id,
          live_id, 
          viewers_count, 
          live_title, 
          category, 
          is_adult, 
          execution_ts
      FROM soop.popular_live_with_rank pl
      WHERE (pl.live_id, pl.viewers_count) IN (
          SELECT 
              live_id, 
              MAX(viewers_count)
          FROM soop.popular_live_with_rank
          GROUP BY live_id
      )
  ) pl
  JOIN soop.channel_name_latest cn ON pl.channel_id = cn.channel_id
  GROUP BY 1, 2, 3, 4, 5, 6;