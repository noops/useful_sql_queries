WITH experiment_group AS (
    SELECT
        experiment_id,
        group_variation,
        cx_merchant_id,
        enrollment_date
    FROM growth_experiments.experiment_enrollment
    WHERE experiment_id = 'GROW-238.1'
),
durations AS (
    SELECT
        experiment_group.experiment_id,
        experiment_group.group_variation,
        experiment_group.cx_merchant_id,
        CASE
            WHEN DATEDIFF(DAYS, enrollment_date, fact_sellers.cc_entry_date) < 0 THEN 0
            ELSE COALESCE(DATEDIFF(DAYS, enrollment_date, fact_sellers.cc_entry_date),
                          DATEDIFF(DAYS, enrollment_date, CURRENT_DATE))
        END AS elapsed_time,
        CASE WHEN fact_sellers.cc_entry_date IS NULL THEN 0 ELSE 1 END AS event
    FROM auctane_cx_reporting.fact_sellers
        INNER JOIN experiment_group ON fact_sellers.cx_merchant_id = experiment_group.cx_merchant_id
),
num_users AS (
    SELECT
        experiment_id,
        group_variation,
        COUNT(1) AS num_users
    FROM durations
    GROUP BY 1, 2
),
daily_tally AS (
    SELECT
        experiment_id,
        group_variation,
        elapsed_time,
        COUNT(*) AS num_obs,
        SUM(event) AS events
    FROM durations
    GROUP BY 1, 2, 3
),
cumulative_tally AS (
    SELECT
        elapsed_time,
        num_obs,
        events,
        nu.experiment_id,
        nu.group_variation,
        nu.num_users - COALESCE(SUM(num_obs)
                                OVER ( PARTITION BY group_variation ORDER BY elapsed_time ASC ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
                                0) AS n_users
    FROM daily_tally
        LEFT JOIN num_users nu USING (group_variation)
),
exp_conversion_at_tp AS (
    SELECT
        experiment_id,
        group_variation,
        elapsed_time,
        events,
        n_users,
        (SUM(events / n_users::float)
         OVER ( PARTITION BY group_variation ORDER BY elapsed_time ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) AS conv_prc
    FROM cumulative_tally
),
summary_stats AS (
    SELECT
        elapsed_time,
        SUM(CASE WHEN group_variation = 'Variant 1' THEN events ELSE 0 END) AS variant1_events,
        SUM(CASE WHEN group_variation = 'Variant 2' THEN events ELSE 0 END) AS variant2_events,
        SUM(CASE WHEN group_variation = 'Variant 1' THEN n_users ELSE 0 END) AS variant1_n_users,
        SUM(CASE WHEN group_variation = 'Variant 2' THEN n_users ELSE 0 END) AS variant2_n_users
    FROM exp_conversion_at_tp
    GROUP BY elapsed_time
)
,chi_squared AS (
    SELECT
        elapsed_time,
        ((variant1_events * variant2_n_users) - (variant2_events * variant1_n_users))^2 /
        ((variant1_events + variant2_events) * (variant1_n_users + variant2_n_users) * variant1_n_users * variant2_n_users) AS chi_squared_value
    FROM summary_stats
    WHERE ((variant1_events + variant2_events) * (variant1_n_users + variant2_n_users) * variant1_n_users * variant2_n_users) != 0
)
SELECT
    exp_conversion_at_tp.group_variation
    , exp_conversion_at_tp.elapsed_time
    , COALESCE(SUM(exp_conversion_at_tp.conv_prc ), 0) as conversion_at_tp
    , chi_squared.chi_squared_value
FROM exp_conversion_at_tp
JOIN chi_squared ON exp_conversion_at_tp.elapsed_time = chi_squared.elapsed_time
GROUP BY
1,
2,
4
ORDER BY
1,
2