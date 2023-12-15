CREATE TABLE account_aggregation (
    account_id VARCHAR,
    window_time TIMESTAMP(3),
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),
    total_number BIGINT,
    total_amount DOUBLE,
    average_amount DOUBLE,
    PRIMARY KEY (account_id, window_time) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://mysql:3306/statistics',
    'table-name' = 'account_aggregation',
    'driver' = 'com.mysql.jdbc.Driver',
    'username' = 'root',
    'password' = '123456'
)
