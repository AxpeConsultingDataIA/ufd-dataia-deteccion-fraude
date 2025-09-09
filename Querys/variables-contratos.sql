WITH 
deduplicated AS (
    SELECT
        g.*,
        -- in order to take the most recent snapshot
        ROW_NUMBER() OVER (
            PARTITION BY cups, cod_contract
            ORDER BY snapshot DESC
        ) AS rownum
    FROM archived_zele.gccon_contracted_service g
),
base AS (
    SELECT
        cups,
        MIN(creation_date) AS first_contract_date,
        MAX(end_date) AS last_contract_end_date,
        MAX(last_bill_date) AS last_bill_date,
        MAX(from_date) AS last_contract_initial_date,
        -- MAX(contracted_power_control) AS current_contracted_power,
        COUNT(DISTINCT cod_contract) AS num_contracts
    FROM deduplicated
    -- in order to take the most recent snapshot
    WHERE rownum = 1  
    GROUP BY cups
)
SELECT
    cups,
    first_contract_date,
    last_contract_end_date,
    last_bill_date,
    last_contract_initial_date,
    -- current_contracted_power,
    num_contracts,
 
    -- Variables derivadas temporales
    date_diff('day', last_contract_initial_date, current_date) AS days_since_last_contract_initial_date,
    date_diff('day', first_contract_date, current_date) AS contract_lifetime_days,
    date_diff('day', last_bill_date, current_date) AS days_since_last_bill,
 
    -- Recent events flags
    CASE WHEN last_bill_date >= date_add('month', -3, current_date) THEN 1 ELSE 0 END AS billed_last_3m
 
FROM base;