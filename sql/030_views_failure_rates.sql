DROP VIEW IF EXISTS public.v_top_failure_reasons;
DROP VIEW IF EXISTS public.v_failure_rate_by_year;
DROP VIEW IF EXISTS public.v_failure_rate_by_model;

-- then recreate (no casts needed)
CREATE VIEW public.v_failure_rate_by_model AS
SELECT
  make,
  model,
  COUNT(*)                                     AS tests,   -- bigint
  SUM((test_result='FAIL')::int)               AS fails,   -- integer
  ROUND(100.0 * SUM((test_result='FAIL')::int) / NULLIF(COUNT(*),0), 2) AS fail_rate_pct
FROM public.mot_tests
GROUP BY 1,2
ORDER BY tests DESC;

CREATE VIEW public.v_failure_rate_by_year AS
SELECT
  COALESCE(EXTRACT(YEAR FROM first_use_date)::int, 0) AS first_use_year,
  COUNT(*)                                            AS tests,  -- bigint
  SUM((test_result='FAIL')::int)                      AS fails,
  ROUND(100.0 * SUM((test_result='FAIL')::int) / NULLIF(COUNT(*),0), 2) AS fail_rate_pct
FROM public.mot_tests
GROUP BY 1
ORDER BY 1;

CREATE VIEW public.v_top_failure_reasons AS
SELECT
  i.rfr_id,
  COALESCE(c.description, 'Unknown reason') AS description,
  COUNT(*)                                   AS occurrences   -- bigint
FROM public.mot_test_items i
LEFT JOIN public.mot_failure_codes c ON c.code = i.rfr_id
GROUP BY 1,2
ORDER BY occurrences DESC
LIMIT 100;
