CREATE TABLE IF NOT EXISTS transfers (
  block_number UInt64,
  transaction_index UInt64,
  log_index UInt64,
  transaction_hash String,
  address String,
  topic0 String,
  topic1 String,
  topic2 String,
  topic3 String,
  "from" String,
  "to" String,
  amount Decimal128(38, 0),
  "timestamp" Int64
);

ALTER TABLE transfers ADD INDEX transfers_address_index("address") TYPE bloom_filter(0.01) GRANULARITY 8;
ALTER TABLE transfers ADD INDEX transfers_from_index("from") TYPE bloom_filter(0.01) GRANULARITY 8;
ALTER TABLE transfers ADD INDEX transfers_to_index("to") TYPE bloom_filter(0.01) GRANULARITY 8;
ALTER TABLE transfers ADD INDEX transfers_timestamp_index("timestamp") TYPE minmax GRANULARITY 8;
