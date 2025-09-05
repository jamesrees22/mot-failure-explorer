create table if not exists mot_tests (
  id bigserial primary key,
  test_date date not null,
  make text not null,
  model text not null,
  fuel_type text,
  first_use_year int,
  odometer int,
  result text check (result in ('PASS','FAIL')) not null,
  station_postcode text,
  failure_reasons text[] default '{}'::text[]
);

create table if not exists mot_failure_codes (
  code text primary key,
  description text not null
);

create index if not exists idx_mot_tests_model on mot_tests(make, model);
create index if not exists idx_mot_tests_result on mot_tests(result);
create index if not exists idx_mot_tests_year on mot_tests(first_use_year);
create index if not exists idx_mot_tests_date on mot_tests(test_date);
