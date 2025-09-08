# MOT Failure Explorer

Cyberpunk-styled dashboard (Next.js + Tailwind + Supabase + Recharts).

## 1) Supabase
1. Create a new Supabase project.
2. Run `sql/setup.sql` in the SQL editor.
3. Import your CSV into table `aggregated_failures_2024` with columns:
   - make, model, mileage_bucket, failure_category, month_year, vehicle_type, failure_count
4. (Optional) Turn off RLS if you want private access only (we default to public read).

## 2) Env Vars
Create `.env.local` (and set in Vercel project settings):
