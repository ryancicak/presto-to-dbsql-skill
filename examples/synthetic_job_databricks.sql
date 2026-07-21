/* Synthetic Presto/Trino job for a fictional video-streaming service. */
/* Reproduces, in miniature, every conversion-relevant pattern observed in large */
/* production Presto analytics jobs: deep CTE chains, CROSS JOIN UNNEST in both alias */
/* forms, ARRAY_AGG over typed ROW casts (with positional renames and type coercions), */
/* higher-order lambdas with nested TRY(), epoch-window FILTERs, anonymous-ROW */
/* array_intersect pair matching, nested structs, ordinal GROUP BY, and */
/* key-normalization joins. All tables, columns, and values are fictional. */
WITH vw_page_views AS (
  SELECT DISTINCT
    REGEXP_REPLACE(TRIM(LOWER(viewer.device_agent_id)), '-', '') AS viewer_key,
    REGEXP_REPLACE(TRIM(LOWER(stream_request.stream_id)), '-', '') AS stream_key,
    stream_request.started_by,
    promo.promo_entry_bool,
    event_date
  FROM media_prod_events.playback_session_event
  WHERE
    event.event_name = 'catalog.viewed'
    AND event.event_category = 'browse'
    AND event.event_type = 'Page View'
    AND UPPER(storefront.brand_name) IN ('ACMEFLIX', 'ACMEFLIX PLUS')
    AND event_date BETWEEN CAST('2024-05-01' AS DATE) AND CAST('2024-05-01' AS DATE)
    AND experience.page_line IN ('Movies', 'Bundles')
), vw_impressions AS (
  SELECT
    event_date,
    viewer_key,
    stream_key,
    COLLECT_LIST(
      CAST(STRUCT(slot_number, item_id, min_ts, max_ts, shown) AS STRUCT<slot_number: INT, item_id: STRING, min_ts: BIGINT, max_ts: BIGINT, shown: INT>)
    ) AS views_array,
    MAX(slot_number) AS max_shown_slot
  FROM (
    SELECT
      REGEXP_REPLACE(TRIM(LOWER(viewer.device_agent_id)), '-', '') AS viewer_key,
      REGEXP_REPLACE(TRIM(LOWER(stream_request.stream_id)), '-', '') AS stream_key,
      t.item_id,
      t.slot_number,
      event_date,
      MIN(ts_info.event_ts) AS min_ts,
      MAX(ts_info.event_ts) AS max_ts,
      MAX(1) AS shown
    FROM media_prod_events.playback_session_event
    LATERAL VIEW
    EXPLODE(item_list) t_lv AS t
    WHERE
      event.event_name = 'tile.presented'
      AND event.event_category = 'rail'
      AND event.event_type = 'Impression'
      AND NOT stream_request.stream_id IS NULL
      AND stream_request.stream_id <> ''
      AND event_date BETWEEN CAST('2024-05-01' AS DATE) AND CAST('2024-05-01' AS DATE)
    GROUP BY
      1,
      2,
      3,
      4,
      5
  )
  WHERE
    NOT item_id IS NULL
  GROUP BY
    1,
    2,
    3
), vw_purchases AS (
  SELECT
    buy_event_date,
    buy_viewer_key,
    buy_stream_key,
    COLLECT_LIST(
      CAST(STRUCT(buy_item_id, buy_slot, event_date, buy_epoch, buy_datetime_utc, buy_amount, buy_margin) AS STRUCT<
        item_id: STRING,
        purchase_slot: INT,
        purchase_event_date: DATE,
        buy_epoch: BIGINT,
        buy_datetime_utc: TIMESTAMP,
        amount: DOUBLE,
        margin: DOUBLE
      >)
    ) AS purchase_array
  FROM media_prod_metrics.watch_attr_msr
  WHERE
    buy_event_date >= CAST('2024-05-01' AS DATE)
    AND buy_event_date <= CAST('2024-05-01' AS DATE)
    AND event_date <= DATE_ADD(CAST('2024-05-01' AS DATE), 8)
  GROUP BY
    1,
    2,
    3
), vw_base AS (
  SELECT
    CAST(event_date_utc AS DATE) AS event_date,
    event_epoch,
    next_event_epoch,
    REGEXP_REPLACE(TRIM(LOWER(viewer.device_agent_id)), '-', '') AS viewer_key,
    REGEXP_REPLACE(TRIM(LOWER(rec_attributes_struct.stream_id)), '-', '') AS stream_key,
    rec_attributes_struct.prefs.sort_type AS sort_type,
    IF(SIZE(rec_attributes_struct.prefs.applied_filters) > 0, TRUE, FALSE) AS is_filtered,
    CAST(STRUCT(rec_attributes_struct.geo.region_system, rec_attributes_struct.geo.region_name) AS STRUCT<region_system: STRING, region_name: STRING>) AS region_info,
    storefront.brand_name,
    page.category_name,
    TRANSFORM(
      COALESCE(item_list, ARRAY()),
      p -> CAST(STRUCT(
        CAST(p.item_id AS STRING),
        p.slot_number,
        COALESCE(p.candidate_info.promoted_bool, FALSE),
        p.candidate_info.unit_price,
        p.candidate_info.badge_id
      ) AS STRUCT<
        item_id: STRING,
        slot_number: INT,
        promoted_bool: BOOLEAN,
        unit_price: DOUBLE,
        badge_id: BIGINT
      >)
    ) AS item_list,
    CASE
      WHEN SIZE(
        FILTER(
          candidate_items,
          x -> (
            x.event_epoch - event_epoch <= 10800000
          )
          AND (
            x.event_epoch < next_event_epoch OR next_event_epoch IS NULL
          )
          AND x.category_name = page.category_name
        )
      ) = 0
      THEN NULL
      ELSE FILTER(
        candidate_items,
        x -> (
          x.event_epoch - event_epoch <= 10800000
        )
        AND (
          x.event_epoch < next_event_epoch OR next_event_epoch IS NULL
        )
        AND x.category_name = page.category_name
      )
    END AS recent_items_array
  FROM media_prod_facts.engagement_daily_fact
  WHERE
    event_type = 'browse'
    AND UPPER(storefront.brand_name) IN ('ACMEFLIX', 'ACMEFLIX PLUS')
    AND page.category_name IN ('Movies', 'Bundles')
    AND DATE(event_date_utc) BETWEEN CAST('2024-05-01' AS DATE) AND CAST('2024-05-01' AS DATE)
), vw_joined AS (
  SELECT
    b.*,
    pv.started_by,
    pv.promo_entry_bool,
    imp.views_array,
    imp.max_shown_slot,
    pur.purchase_array
  FROM vw_base AS b
  LEFT JOIN vw_page_views AS pv
    ON b.event_date = pv.event_date
    AND b.viewer_key = pv.viewer_key
    AND b.stream_key = pv.stream_key
  LEFT JOIN vw_impressions AS imp
    ON b.event_date = imp.event_date
    AND b.viewer_key = imp.viewer_key
    AND b.stream_key = imp.stream_key
  LEFT JOIN vw_purchases AS pur
    ON b.event_date = pur.buy_event_date
    AND b.viewer_key = pur.buy_viewer_key
    AND b.stream_key = pur.buy_stream_key
), vw_enriched AS (
  SELECT
    event_date,
    viewer_key,
    stream_key,
    started_by,
    promo_entry_bool,
    sort_type,
    is_filtered,
    region_info,
    max_shown_slot,
    TRANSFORM(
      item_list,
      x -> CAST(STRUCT(
        x.item_id,
        x.slot_number,
        x.promoted_bool,
        IF(
          SIZE(
            ARRAY_INTERSECT(
              TRANSFORM(
                COALESCE(views_array, ARRAY()),
                y -> STRUCT(y.item_id AS _f0, y.slot_number AS _f1)
              ),
              TRANSFORM(item_list, y -> STRUCT(y.item_id AS _f0, y.slot_number AS _f1))
            )
          ) > 0
          AND SIZE(
            FILTER(
              COALESCE(views_array, ARRAY()),
              y -> TRY_CAST(y.item_id AS STRING) = x.item_id AND y.slot_number = x.slot_number
            )
          ) > 0,
          1,
          0
        ),
        SIZE(
          FILTER(
            COALESCE(recent_items_array, ARRAY()),
            y -> TRY_CAST(y.item_ref.item_id AS STRING) = TRY_CAST(x.item_id AS STRING)
          )
        ),
        SIZE(
          FILTER(
            COALESCE(purchase_array, ARRAY()),
            y -> TRY_CAST(y.item_id AS STRING) = TRY_CAST(x.item_id AS STRING)
            AND y.buy_epoch - 1714557600000 <= 86400000
          )
        )
      ) AS STRUCT<
        item_id: STRING,
        slot_number: INT,
        promoted_bool: BOOLEAN,
        seen: INT,
        recent_views: INT,
        purchases_24h: INT
      >)
    ) AS enriched_list
  FROM vw_joined
), vw_final AS (
  SELECT
    e.event_date,
    e.viewer_key,
    e.stream_key,
    e.started_by,
    e.promo_entry_bool,
    e.sort_type,
    e.is_filtered,
    e.region_info,
    e.max_shown_slot,
    p.item_id,
    p.slot_number,
    p.promoted_bool,
    p.seen,
    p.recent_views,
    p.purchases_24h
  FROM vw_enriched AS e
  LATERAL VIEW
  INLINE(e.enriched_list) p AS item_id, slot_number, promoted_bool, seen, recent_views, purchases_24h
)
SELECT
  event_date,
  viewer_key,
  stream_key,
  started_by,
  promo_entry_bool,
  sort_type,
  is_filtered,
  region_info,
  max_shown_slot,
  item_id,
  slot_number,
  promoted_bool,
  COALESCE(seen, 0) AS seen,
  recent_views,
  purchases_24h
FROM vw_final;
