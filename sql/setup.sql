-- Fresh schema for MOT 2024 aggregated dataset
create table if not exists public.aggregated_failures_2024 (
  id bigserial primary key,
  make text not null,
  model text not null,
  mileage_bucket text not null,       -- e.g. '25k to 50k'
  failure_category text not null,     -- e.g. 'Brakes'
  month_year text not null,           -- '2024-06'
  vehicle_type text not null,         -- 'Car' etc.
  failure_count integer not null
);

create index if not exists idx_mot24_make on public.aggregated_failures_2024 (make);
create index if not exists idx_mot24_model on public.aggregated_failures_2024 (model);
create index if not exists idx_mot24_category on public.aggregated_failures_2024 (failure_category);
create index if not exists idx_mot24_mileage on public.aggregated_failures_2024 (mileage_bucket);
create index if not exists idx_mot24_month on public.aggregated_failures_2024 (month_year);

alter table public.aggregated_failures_2024 enable row level security;

create policy "public read" on public.aggregated_failures_2024
for select using (true);
