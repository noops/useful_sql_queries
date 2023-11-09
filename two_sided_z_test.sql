WITH experiment_data AS (
    SELECT DISTINCT
        experiment_enrollment.experiment_id
        , experiment_enrollment.group_variation
        , experiment_enrollment.cx_merchant_id
        , enrollment_control.is_control
        , CASE WHEN fact_sellers.cc_entry_date IS NULL OR DATEDIFF( DAYS, enrollment_date, cc_entry_date ) > 30 THEN 0
               ELSE 1 END AS event
        FROM growth_experiments.experiment_enrollment
            LEFT JOIN growth_experiments.enrollment_control ON experiment_enrollment.experiment_id = enrollment_control.experiment_id
                AND experiment_enrollment.group_variation = enrollment_control.group_variation
            LEFT JOIN auctane_cx_reporting.fact_sellers ON experiment_enrollment.cx_merchant_id = fact_sellers.cx_merchant_id
        WHERE experiment_enrollment.experiment_id = 'GROW-218'
    )
    , num_users AS (
    SELECT
        experiment_id
        , group_variation
        , is_control
        , COUNT( 1 ) AS n_count
        FROM experiment_data
        GROUP BY 1
            , 2
            , 3
    )
    , num_events AS (
    SELECT
        experiment_id
        , group_variation
        , is_control
        , COUNT( * )::FLOAT AS n_count
        , SUM( event )::FLOAT AS events
        FROM experiment_data
        GROUP BY 1
            , 2
            , 3
    )
    , gross_conversion AS (
    SELECT
        num_events.n_count
        , num_events.events
        , nu.experiment_id
        , nu.group_variation
        , nu.is_control
        FROM num_events
            LEFT JOIN num_users AS nu USING (group_variation)
    )
    , experiment_control AS (
    SELECT
        experiment_id
        , 1 AS sortorder
        , group_variation
        , events
        , n_count AS n_count
        , CAST( ROUND( ((events * 1.0) / n_count), 4 ) AS DECIMAL(8, 4) ) AS conv_prc
        FROM gross_conversion
        WHERE is_control = true
        GROUP BY 1
            , 2
            , 3
            , 4
            , 5
        LIMIT 1
    )
    , test_groups AS (
    SELECT
        experiment_id
        , 1 + ROW_NUMBER( ) OVER (ORDER BY group_variation) AS sortorder
        , group_variation
        , events
        , n_count AS n_count
        , CAST( ROUND( ((events * 1.0) / n_count), 4 ) AS DECIMAL(8, 4) ) AS conv_prc
        FROM gross_conversion
        WHERE group_variation NOT IN (
            SELECT
                group_variation
                FROM experiment_control
            )
        GROUP BY 1
            , 3
            , 4
            , 5
    )
    , experiment AS (
    SELECT
        *
        FROM (
            SELECT
                sortorder
                , experiment_id
                , group_variation
                , events
                , n_count
                , conv_prc
                FROM experiment_control
            UNION ALL
            SELECT
                sortorder
                , experiment_id
                , group_variation
                , events
                , n_count
                , conv_prc
                FROM test_groups
            )
        ORDER BY sortorder
    )
, unfavorable_events AS (
    SELECT
        experiment.sortorder
        , experiment.experiment_id
        , experiment.group_variation
        , (n_count - events) AS events_unfavorable
    FROM experiment
    ORDER BY sortorder
    )
, p_hat AS (
    SELECT
        experiment.sortorder
        , experiment.experiment_id
        , experiment.group_variation
        , CASE WHEN sortorder = 1 THEN NULL
               ELSE (events + (FIRST_VALUE( events ) OVER (ORDER BY sortorder ROWS UNBOUNDED PRECEDING))) /
                    (n_count + (FIRST_VALUE( n_count ) OVER (ORDER BY sortorder ROWS UNBOUNDED PRECEDING))) END AS p_hat
        FROM experiment
        ORDER BY sortorder
)
, se_pooled AS (
    SELECT
         experiment.sortorder
        , experiment.experiment_id
        , experiment.group_variation
        , CASE WHEN experiment.sortorder = 1 THEN NULL
               ELSE SQRT( (p_hat.p_hat * (1 - p_hat.p_hat)) * (1 / FIRST_VALUE( experiment.n_count ) OVER (ORDER BY experiment.sortorder ROWS UNBOUNDED PRECEDING) + 1 / experiment.n_count) ) end AS se
        FROM experiment
        LEFT JOIN p_hat on experiment.sortorder = p_hat.sortorder
        ORDER BY SORTORDER
)
, z_exact AS (
    SELECT
        experiment.sortorder
        , experiment.experiment_id
        , experiment.group_variation
        , CASE WHEN experiment.sortorder = 1 THEN NULL
               ELSE ((FIRST_VALUE( experiment.conv_prc ) OVER (ORDER BY experiment.sortorder ROWS UNBOUNDED PRECEDING) - experiment.conv_prc) / se_pooled.se ) end AS z
        FROM experiment
        LEFT JOIN se_pooled on experiment.sortorder = se_pooled.sortorder
        ORDER BY SORTORDER
    )
, erf as (
SELECT
   z_exact.sortorder
   , z_exact.experiment_id
   , z_exact.group_variation
   , z_exact.z
   , power(z_exact.z,2) as x2
   , ((8*(3.1415927 - 3))/(3*3.1415927*(4 - 3.1415927))) * x2 as ax2
   , (4/3.1415927) + ax2 as num
   , 1 + ax2 as denom
   , (-x2)*num/denom as inside
   , 1 - exp(inside) as erf2
   , sqrt(erf2) as erf
FROM z_exact
ORDER BY SORTORDER
    )
, cdf as (
SELECT
    erf.sortorder
    , erf.experiment_id
    , erf.group_variation
    , erf.z
    , erf.erf
    , 1 / (1 + .2315419 * abs(erf.z)) as t
    , .3989423 * exp( ((erf.z * -1) * erf.z) / 2) as d
    , round(d * t * (.3193815 + t * ( -.3565638 + t * (1.781478 + t * (-1.821256 + t * 1.330274)))), 4) as CDF
    , CASE WHEN erf.z > 0 THEN 1 - CDF ELSE CDF END AS PROB
    FROM erf
ORDER BY SORTORDER
    )
    SELECT
        experiment.sortorder
        , experiment.experiment_id
        , experiment.group_variation
        , experiment.events
        , experiment.n_count
        , experiment.conv_prc
        , CASE WHEN experiment.sortorder = 1 THEN NULL
               ELSE experiment.conv_prc - FIRST_VALUE( experiment.conv_prc ) OVER (ORDER BY experiment.sortorder ROWS UNBOUNDED PRECEDING) END AS delta_p
        , CASE WHEN experiment.sortorder = 1 THEN NULL
               ELSE (experiment.conv_prc - FIRST_VALUE( experiment.conv_prc ) OVER (ORDER BY experiment.sortorder ROWS UNBOUNDED PRECEDING)) /
                    FIRST_VALUE( experiment.conv_prc ) OVER (ORDER BY experiment.sortorder ROWS UNBOUNDED PRECEDING) END AS lift
        , unfavorable_events.events_unfavorable
        , p_hat.p_hat
        , se_pooled.se
        , z_exact.z
        , erf.erf
        , cdf.cdf
        , cdf.prob
        FROM experiment
        LEFT JOIN unfavorable_events on experiment.group_variation = unfavorable_events.group_variation
        LEFT JOIN p_hat on experiment.group_variation = p_hat.group_variation
        LEFT JOIN se_pooled on experiment.group_variation = se_pooled.group_variation
        LEFT JOIN z_exact on experiment.group_variation = z_exact.group_variation
        LEFT JOIN erf on erf.group_variation = experiment.group_variation
        LEFT JOIN cdf on cdf.group_variation = experiment.group_variation
        ORDER BY experiment.sortorder