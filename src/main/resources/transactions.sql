CREATE TABLE transactions (
    account_id VARCHAR,
    transaction_time TIMESTAMP(3),
    amount DOUBLE
) WITH (
    'connector' = 'kafka',
    'topic' = 'transactions',
    'properties.bootstrap.servers' = 'localhost:9092',
    'format' = 'json'
)
