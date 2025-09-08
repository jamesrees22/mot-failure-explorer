-- Refresh materialized views for dropdowns
refresh materialized view concurrently mv_mot24_makes;
refresh materialized view concurrently mv_mot24_models;
refresh materialized view concurrently mv_mot24_categories;
refresh materialized view concurrently mv_mot24_mileage;
refresh materialized view concurrently mv_mot24_months;
