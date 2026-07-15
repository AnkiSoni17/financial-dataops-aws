CREATE TABLE "financial-dataops-dev-gold-db"."customer_card_analysis"
WITH (
    format = 'PARQUET',
    parquet_compression = 'SNAPPY',
    external_location = 's3://financial-dataops-dev-49e514ea/gold/customer-card-analysis/'
) AS
SELECT
    card_brand,
    card_type,
    COUNT(*) AS transaction_count,
    ROUND(SUM(ABS(amount)), 2) AS total_transaction_value,
    ROUND(AVG(ABS(amount)), 2) AS average_transaction_value,
    SUM(
        CASE
            WHEN transaction_method = 'Online Transaction' THEN 1
            ELSE 0
        END
    ) AS online_transaction_count,
    SUM(
        CASE
            WHEN right_is_fraud = true THEN 1
            ELSE 0
        END
    ) AS fraud_transaction_count
FROM "financial-dataops-dev-gold-db"."gold" 
GROUP BY
    card_brand,
    card_type;