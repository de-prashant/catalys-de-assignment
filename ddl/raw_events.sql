CREATE OR REPLACE TABLE RAW.EVENTS (
    event_id STRING,
    user_id STRING,
    event_type STRING,
    timestamp TIMESTAMP,
    raw VARIANT
);
