# MOT Failure Explorer

Automotive-themed dashboard (Next.js + Tailwind + Supabase + Recharts).

## 1) Supabase

1. Create a new Supabase project.
2. Run `sql/setup.sql` in the SQL editor to create the main `aggregated_failures_2024` table and supporting filter materialized views.
3. Import your CSV into table `aggregated_failures_2024` with columns:
   - make, model, mileage_bucket, failure_category, month_year, vehicle_type, failure_count
4. Refresh the filter materialized views whenever data is reloaded:
   - Run `sql/refresh_filters.sql` in the Supabase SQL Editor, or  
   - From the CLI:  
     ```bash
     psql <connection-string> -f sql/refresh_filters.sql
     ```
5. (Optional) Set up a Supabase scheduled job to refresh nightly:
   ```sql
   create extension if not exists pg_cron;
   select cron.schedule(
     'refresh-mv-mot24-daily',
     '0 2 * * *',
     $$
       refresh materialized view concurrently mv_mot24_makes;
       refresh materialized view concurrently mv_mot24_models;
       refresh materialized view concurrently mv_mot24_categories;
       refresh materialized view concurrently mv_mot24_mileage;
       refresh materialized view concurrently mv_mot24_months;
     $$
   );
