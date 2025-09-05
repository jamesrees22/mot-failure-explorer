# MOT Failure Explorer

Open-data demo: explore MOT failure rates by make/model and year.

## Stack
- Supabase (Postgres) for storage + views
- Next.js (Vercel) for UI
- GitHub Actions + Python ETL

## Quick start
1) Create a Supabase project → run `sql/001_schema.sql`, `sql/010_views.sql`, `sql/002_policies.sql`.
2) In GitHub → Settings → Secrets and variables → Actions → add:
   - `SUPABASE_URL`
   - `SUPABASE_SERVICE_ROLE_KEY`
3) Push repo → Actions tab → run “MOT Refresh” (or wait for schedule). This seeds sample data.
4) In Vercel, set env vars:
   - `NEXT_PUBLIC_SUPABASE_URL`
   - `NEXT_PUBLIC_SUPABASE_ANON_KEY`
   Deploy the app at `apps/web`.

## ETL notes
- `etl/mot_refresh.py` seeds from `etl/sample_mot_rows.csv` and `etl/failure_codes.csv`.
- Replace with DVSA bulk load when ready; keep Supabase size reasonable by filtering to recent years.

## Roadmap
- Add filters (year, fuel), small charts (fail rate vs. age)
- Enable RLS with read-only policy; restrict writes to service role
- Load real DVSA data in batches via Actions
