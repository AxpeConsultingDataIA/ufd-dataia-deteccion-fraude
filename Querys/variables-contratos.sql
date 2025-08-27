WITH base AS (
    SELECT
        cups,
        MIN(creation_date) AS first_contract_date,
        MAX(end_date) AS last_contract_date,
        MAX(last_bill_date) AS last_bill_date,
        MAX(modification_date) AS last_modification_date,
        MAX(drop_date) AS last_drop_date,
        MAX(fare_modification_date) AS last_fare_modification_date,
        MAX(contracted_power_control) AS current_contracted_power,
        COUNT(DISTINCT cod_modification_type) AS num_distinct_mod_types,
        COUNT( distinct cod_contract) AS num_contracts,
        SUM(CASE WHEN cod_drop_reason_type IS NOT NULL THEN 1 ELSE 0 END) AS num_contract_drops
    FROM archived_zele.gccon_contracted_service
    GROUP BY cups
)
SELECT
    cups,
    first_contract_date,
    last_contract_date,
    last_bill_date,
    last_modification_date,
    last_drop_date,
    last_fare_modification_date,
    current_contracted_power,
    num_contracts,
    num_distinct_mod_types,
    num_contract_drops,
 
    -- Variables derivadas temporales
    date_diff('day', last_modification_date, current_date) AS days_since_last_modification,
    date_diff('day', last_drop_date, current_date) AS days_since_last_drop,
    date_diff('day', last_fare_modification_date, current_date) AS days_since_last_fare_change,
    date_diff('day', first_contract_date, current_date) AS contract_lifetime_days,
    date_diff('day', last_bill_date, current_date) AS days_since_last_bill,
 
    -- Flags de eventos recientes
    CASE WHEN last_modification_date >= date_add('week', -1, current_date) THEN 1 ELSE 0 END AS mod_last_week,
    CASE WHEN last_modification_date >= date_add('month', -1, current_date) THEN 1 ELSE 0 END AS mod_last_month,
    CASE WHEN last_modification_date >= date_add('quarter', -1, current_date) THEN 1 ELSE 0 END AS mod_last_quarter,
    CASE WHEN last_modification_date >= date_add('year', -1, current_date) THEN 1 ELSE 0 END AS mod_last_year,
    CASE WHEN last_bill_date >= date_add('month', -3, current_date) THEN 1 ELSE 0 END AS billed_last_3m

FROM base;