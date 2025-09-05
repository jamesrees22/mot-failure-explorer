create or replace view v_failure_rate_by_model as
select
  make,
  model,
  count(*)::int as tests,
  sum(case when result='FAIL' then 1 else 0 end)::int as fails,
  round(100.0 * avg(case when result='FAIL' then 1 else 0 end), 2) as fail_rate_pct
from mot_tests
group by 1,2
having count(*) >= 200
order by fail_rate_pct desc;

create or replace view v_failure_rate_by_year as
select
  first_use_year,
  count(*)::int as tests,
  round(100.0 * avg(case when result='FAIL' then 1 else 0 end), 2) as fail_rate_pct
from mot_tests
where first_use_year is not null
group by 1
order by first_use_year;

create or replace view v_top_failure_reasons as
with exploded as (
  select unnest(failure_reasons) as code
  from mot_tests
  where result='FAIL'
)
select
  e.code,
  c.description,
  count(*)::int as occurrences
from exploded e
left join mot_failure_codes c on c.code = e.code
group by 1,2
order by occurrences desc
limit 25;
