-- Shadow schemas for examples/synthetic_job_databricks.sql (all fictional).
-- Lets you validate the converted job end-to-end on a Databricks SQL warehouse
-- with zero real data. Expected output with the seed rows below: 2 rows -
--   item A1 slot 1: seen=1, recent_views=1, purchases_24h=1
--   item B2 slot 2: seen=0, recent_views=0, purchases_24h=0
CREATE SCHEMA IF NOT EXISTS media_prod_events;
CREATE SCHEMA IF NOT EXISTS media_prod_facts;
CREATE SCHEMA IF NOT EXISTS media_prod_metrics;

CREATE OR REPLACE TABLE media_prod_events.playback_session_event (
  event_date DATE,
  viewer STRUCT<device_agent_id: STRING>,
  stream_request STRUCT<stream_id: STRING, started_by: STRING>,
  event STRUCT<event_name: STRING, event_category: STRING, event_type: STRING>,
  storefront STRUCT<brand_name: STRING>,
  experience STRUCT<page_line: STRING>,
  ts_info STRUCT<event_ts: BIGINT>,
  promo STRUCT<promo_entry_bool: BOOLEAN>,
  item_list ARRAY<STRUCT<item_id: STRING, slot_number: INT>>
);

CREATE OR REPLACE TABLE media_prod_facts.engagement_daily_fact (
  event_type STRING,
  event_date_utc DATE,
  event_epoch BIGINT,
  next_event_epoch BIGINT,
  viewer STRUCT<device_agent_id: STRING>,
  rec_attributes_struct STRUCT<
    stream_id: STRING,
    prefs: STRUCT<sort_type: STRING, applied_filters: ARRAY<STRING>>,
    geo: STRUCT<region_system: STRING, region_name: STRING>>,
  storefront STRUCT<brand_name: STRING>,
  page STRUCT<category_name: STRING>,
  item_list ARRAY<STRUCT<item_id: STRING, slot_number: INT,
    candidate_info: STRUCT<promoted_bool: BOOLEAN, nightly_rate: DOUBLE, badge_id: STRING>>>,
  candidate_items ARRAY<STRUCT<event_epoch: BIGINT, category_name: STRING,
    item_ref: STRUCT<item_id: STRING>>>
);

CREATE OR REPLACE TABLE media_prod_metrics.watch_attr_msr (
  buy_event_date DATE,
  buy_viewer_key STRING,
  buy_stream_key STRING,
  event_date DATE,
  buy_item_id STRING,
  buy_slot INT,
  buy_epoch BIGINT,
  buy_datetime_utc STRING,
  buy_amount DOUBLE,
  buy_margin DOUBLE
);

-- Seed rows. Base search epoch: 1714557600000 (2024-05-01).
INSERT INTO media_prod_facts.engagement_daily_fact
SELECT
  'browse', DATE'2024-05-01', 1714557600000, NULL,
  named_struct('device_agent_id', 'UA-1'),
  named_struct('stream_id', 'S-1',
    'prefs', named_struct('sort_type', 'popular', 'applied_filters', array('hd')),
    'geo', named_struct('region_system', 'geo1', 'region_name', 'north')),
  named_struct('brand_name', 'AcmeFlix'),
  named_struct('category_name', 'Movies'),
  array(
    named_struct('item_id', 'A1', 'slot_number', 1,
      'candidate_info', named_struct('promoted_bool', TRUE, 'nightly_rate', 9.99, 'badge_id', '5')),
    named_struct('item_id', 'B2', 'slot_number', 2,
      'candidate_info', named_struct('promoted_bool', FALSE, 'nightly_rate', 4.99, 'badge_id', '6'))),
  array(
    named_struct('event_epoch', 1714558600000, 'category_name', 'Movies',
      'item_ref', named_struct('item_id', 'A1')),
    named_struct('event_epoch', 1999999999999, 'category_name', 'Movies',
      'item_ref', named_struct('item_id', 'A1')));

INSERT INTO media_prod_events.playback_session_event
SELECT DATE'2024-05-01',
  named_struct('device_agent_id', 'UA-1'),
  named_struct('stream_id', 'S-1', 'started_by', 'menu'),
  named_struct('event_name', 'catalog.viewed', 'event_category', 'browse', 'event_type', 'Page View'),
  named_struct('brand_name', 'AcmeFlix'), named_struct('page_line', 'Movies'),
  named_struct('event_ts', 1714557600000), named_struct('promo_entry_bool', TRUE), NULL;

INSERT INTO media_prod_events.playback_session_event
SELECT DATE'2024-05-01',
  named_struct('device_agent_id', 'UA-1'),
  named_struct('stream_id', 'S-1', 'started_by', 'menu'),
  named_struct('event_name', 'tile.presented', 'event_category', 'rail', 'event_type', 'Impression'),
  named_struct('brand_name', 'AcmeFlix'), named_struct('page_line', 'Movies'),
  named_struct('event_ts', 1714557700000), named_struct('promo_entry_bool', FALSE),
  array(named_struct('item_id', 'A1', 'slot_number', 1));

INSERT INTO media_prod_metrics.watch_attr_msr VALUES
  (DATE'2024-05-01', 'ua1', 's1', DATE'2024-05-01', 'A1', 1, 1714561200000,
   '2024-05-01 11:00:00', 19.99, 4.00),
  (DATE'2024-05-01', 'ua1', 's1', DATE'2024-05-02', 'A1', 1, 1714647700000,
   '2024-05-02 11:01:40', 29.99, 6.00);
