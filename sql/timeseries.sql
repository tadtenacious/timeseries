WITH initial AS (
  SELECT
    mnth,
    passengers,
    AVG(CAST(Passengers AS NUMERIC)) 
      OVER(
        ORDER BY mnth 
        ROWS BETWEEN 6 PRECEDING AND 5 FOLLOWING
      ) AS trend
FROM
  `timeseries.airpassengers`),
trend AS (
  SELECT
    a.mnth
    ,a.passengers
    ,a.trend
  FROM initial a
  WHERE EXISTS (
                SELECT 1 FROM initial x
                WHERE a.mnth = DATE_SUB(x.mnth, INTERVAL 5 MONTH)
                )
  AND EXISTS (
              SELECT 1 FROM initial x
                WHERE a.mnth = DATE_ADD(x.mnth, INTERVAL 6 MONTH)
                )
), detrend AS (              
  SELECT
    t.mnth
    ,t.passengers
    ,t.trend
    ,CAST(t.passengers AS NUMERIC) / t.trend AS detrend
  FROM trend t
), seasonality AS (
SELECT
  d.mnth
  ,d.passengers
  ,d.trend
  ,d.detrend
  ,AVG(d.detrend) OVER (PARTITION BY EXTRACT(MONTH FROM d.mnth)) AS avg_seasonality
FROM detrend d
), time_series AS (
  SELECT
    s.mnth
    ,s.Passengers
    ,s.trend
    ,s.detrend
    ,s.avg_seasonality
    ,s.Passengers / (s.avg_seasonality * s.trend) AS random_noise
  FROM seasonality s
)
SELECT
  a.mnth AS Month
  ,a.Passengers 
  ,t.trend
  ,t.detrend
  ,t.avg_seasonality
  ,t.random_noise
  ,CAST(ROUND(t.trend * t.avg_seasonality * t.random_noise,0) AS INT64) AS recontructed
FROM `timeseries.airpassengers` a
LEFT JOIN time_series t ON a.mnth= t.mnth
ORDER BY 1